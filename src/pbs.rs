//! Talking to PBS and to the ABCI-Q account commands.
//!
//! Everything comes from `qstat -f -F json`. Note we never pass `-u`: the
//! `qstat` first in `PATH` on ABCI-Q is a site wrapper that rejects it. We ask
//! for every job the server will show us and filter by owner in-process, which
//! works with both the wrapper and the stock `/opt/pbs/bin/qstat`.

use crate::fmt;
use serde_json::Value;
use std::collections::HashMap;
use std::process::Command;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum State {
    Running,
    Queued,
    Held,
    Waiting,
    Exiting,
    Finished,
    Suspended,
    Other,
}

impl State {
    fn from_code(c: &str) -> State {
        match c {
            "R" => State::Running,
            "Q" => State::Queued,
            "H" => State::Held,
            "W" => State::Waiting,
            "E" => State::Exiting,
            "F" => State::Finished,
            "S" | "T" | "U" => State::Suspended,
            _ => State::Other,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            State::Running => "R",
            State::Queued => "Q",
            State::Held => "H",
            State::Waiting => "W",
            State::Exiting => "E",
            State::Finished => "F",
            State::Suspended => "S",
            State::Other => "?",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            State::Running => "running",
            State::Queued => "queued",
            State::Held => "held",
            State::Waiting => "waiting",
            State::Exiting => "exiting",
            State::Finished => "finished",
            State::Suspended => "suspended",
            State::Other => "unknown",
        }
    }

    /// ANSI colour for this state.
    pub fn color(&self) -> &'static str {
        match self {
            State::Running => "32",
            State::Queued => "33",
            State::Held => "35",
            State::Waiting => "34",
            State::Exiting => "36",
            State::Finished => "90",
            State::Suspended => "31",
            State::Other => "37",
        }
    }

    /// Sort key: active work first, then things that are waiting, then history.
    pub fn rank(&self) -> u8 {
        match self {
            State::Running => 0,
            State::Exiting => 1,
            State::Queued => 2,
            State::Waiting => 3,
            State::Held => 4,
            State::Suspended => 5,
            State::Finished => 6,
            State::Other => 7,
        }
    }
}

pub struct Job {
    pub id: String,
    pub short_id: String,
    pub name: String,
    pub owner: String,
    pub state: State,
    pub queue: String,
    pub group: String,
    pub service: String,
    pub rtype: Option<String>,
    pub nodect: u64,
    pub ncpus: u64,
    pub ngpus: u64,
    pub mem_req: String,
    pub walltime_req: Option<u64>,
    pub walltime_used: Option<u64>,
    pub cpupercent: Option<f64>,
    pub mem_used_kb: Option<u64>,
    pub nodes: Vec<String>,
    pub log_path: Option<String>,
    pub error_path: Option<String>,
    pub join_oe: bool,
    pub qtime: Option<i64>,
    pub stime: Option<i64>,
    pub est_start: Option<String>,
    pub comment: Option<String>,
    pub exit_status: Option<i64>,
    pub submit_args: Option<String>,
}

impl Job {
    /// Fraction of the requested walltime consumed, for running jobs.
    pub fn walltime_frac(&self) -> Option<f64> {
        match (self.walltime_used, self.walltime_req) {
            (Some(u), Some(r)) if r > 0 => Some(u as f64 / r as f64),
            _ => None,
        }
    }

    pub fn walltime_left(&self) -> Option<u64> {
        match (self.walltime_used, self.walltime_req) {
            (Some(u), Some(r)) => Some(r.saturating_sub(u)),
            _ => None,
        }
    }

    /// Points per hour for the whole allocation (rate card × node count).
    pub fn point_rate(&self, rates: &HashMap<String, f64>) -> Option<f64> {
        let t = self.rtype.as_ref()?;
        let per_node = rates.get(t)?;
        Some(per_node * self.nodect.max(1) as f64)
    }

    /// Points consumed so far.
    pub fn points_spent(&self, rates: &HashMap<String, f64>) -> Option<f64> {
        let rate = self.point_rate(rates)?;
        let used = self.walltime_used?;
        Some(rate * used as f64 / 3600.0)
    }

    /// Points reserved up-front by the walltime limit.
    pub fn points_reserved(&self, rates: &HashMap<String, f64>) -> Option<f64> {
        let rate = self.point_rate(rates)?;
        let req = self.walltime_req?;
        Some(rate * req as f64 / 3600.0)
    }

    /// How long a not-yet-started job has been waiting.
    pub fn wait_secs(&self, now: i64) -> Option<u64> {
        let q = self.qtime?;
        if self.stime.is_some() {
            return None;
        }
        Some((now - q).max(0) as u64)
    }

    pub fn resource_label(&self) -> String {
        match &self.rtype {
            Some(t) if self.nodect > 1 => format!("{}×{}", t, self.nodect),
            Some(t) => t.clone(),
            None => "-".to_string(),
        }
    }
}

fn s(v: &Value, key: &str) -> Option<String> {
    match v.get(key) {
        Some(Value::String(x)) => Some(x.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

fn u(v: &Value, key: &str) -> Option<u64> {
    match v.get(key) {
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(x)) => x.parse().ok(),
        _ => None,
    }
}

fn f(v: &Value, key: &str) -> Option<f64> {
    match v.get(key) {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(x)) => x.parse().ok(),
        _ => None,
    }
}

/// The `rt_*` key inside `Resource_List` is the resource type the job asked for.
fn resource_type(rl: &Value) -> Option<String> {
    let obj = rl.as_object()?;
    obj.keys()
        .find(|k| k.starts_with("rt_") && !k.starts_with("spot_") && !k.starts_with("ondemand_"))
        .cloned()
}

/// `"(qh250[0]:ngpus=2:...+qh250[1]:ngpus=2:...)"` -> `["qh250"]`.
fn parse_nodes(exec_vnode: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for chunk in exec_vnode.trim_matches(['(', ')'].as_ref()).split('+') {
        let host = chunk
            .split(':')
            .next()
            .unwrap_or("")
            .split('[')
            .next()
            .unwrap_or("")
            .trim();
        if !host.is_empty() && !out.iter().any(|h| h == host) {
            out.push(host.to_string());
        }
    }
    out
}

/// `"qes04:/groups/.../x.sh.o190456"` -> drop the submitting-host prefix.
fn strip_host(p: String) -> String {
    match p.split_once(':') {
        Some((_, path)) if path.starts_with('/') => path.to_string(),
        _ => p,
    }
}

pub fn qstat_bin() -> String {
    // Prefer the stock binary: the /usr/local/bin wrapper is fine for -f -F json
    // but the stock one is what the JSON schema is documented against.
    let stock = "/opt/pbs/bin/qstat";
    if std::path::Path::new(stock).exists() {
        stock.to_string()
    } else {
        "qstat".to_string()
    }
}

pub struct Snapshot {
    pub server: String,
    pub timestamp: i64,
    pub jobs: Vec<Job>,
}

pub fn fetch(history: bool, ids: &[String], tz: i64) -> Result<Snapshot, String> {
    let mut cmd = Command::new(qstat_bin());
    cmd.args(["-f", "-F", "json"]);
    if history {
        cmd.arg("-x");
    }
    for id in ids {
        cmd.arg(id);
    }
    let out = cmd
        .output()
        .map_err(|e| format!("could not run {}: {e}", qstat_bin()))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    if stdout.trim().is_empty() {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        if err.is_empty() {
            return Err("qstat returned nothing".to_string());
        }
        return Err(err);
    }

    let root: Value = serde_json::from_str(&stdout).map_err(|e| {
        format!(
            "could not parse qstat JSON ({e}). PBS emits invalid JSON when a job \
             attribute contains a quote or backslash; try naming the job ids \
             explicitly, e.g. `qrich <jobid>`."
        )
    })?;

    let server = root
        .get("pbs_server")
        .and_then(|v| v.as_str())
        .unwrap_or("pbs")
        .to_string();
    let timestamp = root
        .get("timestamp")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let mut jobs = Vec::new();
    if let Some(map) = root.get("Jobs").and_then(|v| v.as_object()) {
        for (id, j) in map {
            jobs.push(parse_job(id, j, tz));
        }
    }

    // qstat still prints a JSON envelope when it rejects a job id, so an empty
    // job list plus a complaint on stderr is a real error, not "no jobs".
    if jobs.is_empty() {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        if !err.is_empty() {
            let hint = if !ids.is_empty() && !history {
                "\n       finished jobs need history: re-run with -x"
            } else {
                ""
            };
            return Err(format!("{err}{hint}"));
        }
    }

    Ok(Snapshot {
        server,
        timestamp,
        jobs,
    })
}

fn parse_job(id: &str, j: &Value, tz: i64) -> Job {
    let empty = Value::Null;
    let rl = j.get("Resource_List").unwrap_or(&empty);
    let ru = j.get("resources_used").unwrap_or(&empty);
    let vars = j.get("Variable_List").unwrap_or(&empty);

    let owner = s(j, "Job_Owner")
        .and_then(|o| o.split('@').next().map(|x| x.to_string()))
        .unwrap_or_default();

    let short_id = id.split('.').next().unwrap_or(id).to_string();

    let log_path = s(j, "Output_Path").map(strip_host);
    let error_path = s(j, "Error_Path").map(strip_host);
    // "oe"/"eo" fold stderr into the output file; "n" keeps a separate .e file.
    let join_oe = matches!(s(j, "Join_Path").as_deref(), Some("oe") | Some("eo"));

    let nodes = s(j, "exec_vnode")
        .map(|v| parse_nodes(&v))
        .or_else(|| s(j, "exec_host").map(|v| parse_nodes(&v)))
        .unwrap_or_default();

    Job {
        id: id.to_string(),
        short_id,
        name: s(j, "Job_Name").unwrap_or_else(|| "-".into()),
        owner,
        state: State::from_code(&s(j, "job_state").unwrap_or_default()),
        queue: s(j, "queue").unwrap_or_default(),
        group: s(j, "group_list")
            .or_else(|| s(vars, "GROUP_NAME"))
            .unwrap_or_default(),
        service: s(vars, "SERVICE_TYPE").unwrap_or_default(),
        rtype: resource_type(rl),
        nodect: u(rl, "nodect").unwrap_or(1),
        ncpus: u(rl, "ncpus").unwrap_or(0),
        ngpus: u(rl, "ngpus").unwrap_or(0),
        mem_req: s(rl, "mem").unwrap_or_default(),
        walltime_req: s(rl, "walltime").and_then(|w| fmt::parse_hms(&w)),
        walltime_used: s(ru, "walltime").and_then(|w| fmt::parse_hms(&w)),
        cpupercent: f(ru, "cpupercent"),
        mem_used_kb: s(ru, "mem").and_then(|m| fmt::parse_size_kb(&m)),
        nodes,
        log_path,
        error_path,
        join_oe,
        qtime: s(j, "qtime").and_then(|d| fmt::parse_pbs_date(&d, tz)),
        stime: s(j, "stime").and_then(|d| fmt::parse_pbs_date(&d, tz)),
        est_start: j
            .get("estimated")
            .and_then(|e| e.get("start_time"))
            .and_then(|v| v.as_str())
            .map(|x| x.to_string()),
        comment: s(j, "comment"),
        exit_status: j.get("Exit_status").and_then(|v| v.as_i64()),
        submit_args: s(j, "Submit_arguments"),
    }
}

/// Point rates per node-hour, from the 特別支援課題利用 (Special Support) card.
/// Other usage categories have their own card — override with
/// `ABCIQ_POINT_RATES="rt_QF=5,rt_QG=2,rt_QC=1"`.
pub fn point_rates() -> HashMap<String, f64> {
    let mut m = HashMap::new();
    for (k, v) in [
        ("rt_QF", 5.0),
        ("rt_QG", 2.0),
        ("rt_QC", 1.0),
        ("rt_QD", 10.0),
        ("rt_QS", 10.0),
        ("rt_QA", 10.0),
    ] {
        m.insert(k.to_string(), v);
    }
    if let Ok(spec) = std::env::var("ABCIQ_POINT_RATES") {
        for pair in spec.split(',') {
            if let Some((k, v)) = pair.split_once('=') {
                if let Ok(rate) = v.trim().parse::<f64>() {
                    m.insert(k.trim().to_string(), rate);
                }
            }
        }
    }
    m
}

pub struct GroupPoints {
    pub group: String,
    pub used: f64,
    pub granted: f64,
}

impl GroupPoints {
    pub fn left(&self) -> f64 {
        (self.granted - self.used).max(0.0)
    }
}

fn strip_commas(s: &str) -> String {
    s.replace(',', "")
}

/// Parse `show_point`. Group rows start at column 0; per-user rows are indented
/// with `|-` / `` `- `` and are skipped.
pub fn show_point() -> Vec<GroupPoints> {
    let out = match Command::new("show_point").output() {
        Ok(o) => o,
        Err(_) => return Vec::new(),
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut groups = Vec::new();
    for line in text.lines() {
        if line.starts_with(char::is_whitespace) || line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 6 || f[0] == "Group" {
            continue;
        }
        let used: f64 = match strip_commas(f[3]).parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let granted: f64 = match strip_commas(f[4]).parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        groups.push(GroupPoints {
            group: f[0].to_string(),
            used,
            granted,
        });
    }
    groups
}

/// Local UTC offset in seconds, via `date +%z` (PBS prints local timestamps and
/// std has no local-time support).
pub fn tz_offset() -> i64 {
    let out = match Command::new("date").arg("+%z").output() {
        Ok(o) => o,
        Err(_) => return 0,
    };
    let z = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if z.len() < 5 {
        return 0;
    }
    let sign = if z.starts_with('-') { -1 } else { 1 };
    let h: i64 = z[1..3].parse().unwrap_or(0);
    let m: i64 = z[3..5].parse().unwrap_or(0);
    sign * (h * 3600 + m * 60)
}

/// Terminal width, via `stty size` (falls back to 100 columns).
pub fn term_width() -> usize {
    let out = Command::new("stty")
        .arg("size")
        .stdin(std::process::Stdio::inherit())
        .output();
    if let Ok(o) = out {
        let text = String::from_utf8_lossy(&o.stdout);
        if let Some(cols) = text.split_whitespace().nth(1) {
            if let Ok(w) = cols.parse::<usize>() {
                if w >= 40 {
                    return w;
                }
            }
        }
    }
    100
}
