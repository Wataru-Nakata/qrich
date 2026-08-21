//! Cluster-wide capacity: what is free right now, and what would start.
//!
//! Node data comes from the **stock** `/opt/pbs/bin/pbsnodes` — the
//! `/usr/local/bin` wrapper refuses to run on the login node. Two traps make a
//! raw node count wrong, and both are handled here:
//!
//! 1. A node only runs your job if its `resources_available.Qlist` contains the
//!    queue you submit to (`abciq`), so nodes dedicated elsewhere are excluded.
//! 2. The server's `resources_assigned.ngpus` reads 0 because GPUs are tracked
//!    per vnode — the per-node sum is the authority.

use crate::fmt;
use serde_json::Value;
use std::process::Command;

pub struct Resv {
    pub name: String,
    pub start: i64,
    pub end: i64,
    pub nodect: u64,
}

pub struct Cluster {
    pub server: String,
    pub queue: String,
    pub timestamp: i64,
    pub pool: usize,
    pub offline: usize,
    pub gpus_total: u64,
    pub gpus_free: u64,
    pub cpus_total: u64,
    pub cpus_free: u64,
    pub nodes_idle: usize,
    pub nodes_busy: usize,
    pub free_gpus_on_busy: u64,
    pub running: u64,
    pub queued: u64,
    pub held: u64,
    pub limit_user_nodes: Option<u64>,
    pub limit_cluster_nodect: Option<u64>,
    pub next_resv: Option<Resv>,
}

impl Cluster {
    pub fn usable(&self) -> usize {
        self.pool.saturating_sub(self.offline)
    }
}

fn pbsnodes_bin() -> String {
    let stock = "/opt/pbs/bin/pbsnodes";
    if std::path::Path::new(stock).exists() {
        stock.to_string()
    } else {
        "pbsnodes".to_string()
    }
}

fn json_cmd(bin: &str, args: &[&str]) -> Result<Value, String> {
    let out = Command::new(bin)
        .args(args)
        .output()
        .map_err(|e| format!("could not run {bin}: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    if stdout.trim().is_empty() {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        return Err(if err.is_empty() {
            format!("{bin} returned nothing")
        } else {
            err
        });
    }
    serde_json::from_str(&stdout).map_err(|e| format!("could not parse {bin} JSON: {e}"))
}

/// Sub-object lookup that tolerates PBS's `<various>` string placeholders.
fn num(v: &Value, group: &str, key: &str) -> u64 {
    v.get(group)
        .and_then(|g| g.get(key))
        .and_then(|x| x.as_u64())
        .unwrap_or(0)
}

/// `"Transit:0 Queued:0 Held:22 Waiting:1 Running:68 ..."` -> one field.
fn state_count(s: &str, key: &str) -> u64 {
    for field in s.split_whitespace() {
        if let Some((k, v)) = field.split_once(':') {
            if k == key {
                return v.parse().unwrap_or(0);
            }
        }
    }
    0
}

/// `"[u:PBS_GENERIC=100]"` / `"[o:PBS_ALL=300]"` -> 100 / 300.
fn limit_value(s: &str) -> Option<u64> {
    let after = s.rsplit_once('=')?.1;
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// The next reservation that has not started yet, from `pbs_rstat -F`.
fn next_reservation(now: i64, tz: i64) -> Option<Resv> {
    let out = Command::new("/opt/pbs/bin/pbs_rstat").arg("-F").output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut best: Option<Resv> = None;
    let (mut name, mut start, mut end, mut nodect) = (String::new(), 0i64, 0i64, 0u64);
    let flush = |name: &mut String, start: &mut i64, end: &mut i64, nodect: &mut u64, best: &mut Option<Resv>| {
        if *start > now && (best.is_none() || *start < best.as_ref().unwrap().start) {
            *best = Some(Resv {
                name: std::mem::take(name),
                start: *start,
                end: *end,
                nodect: *nodect,
            });
        }
        name.clear();
        *start = 0;
        *end = 0;
        *nodect = 0;
    };

    for line in text.lines() {
        let line = line.trim();
        if line.starts_with("Resv ID:") {
            flush(&mut name, &mut start, &mut end, &mut nodect, &mut best);
        } else if let Some((k, v)) = line.split_once('=') {
            let (k, v) = (k.trim(), v.trim());
            match k {
                "Reserve_Name" => name = v.to_string(),
                "reserve_start" => start = fmt::parse_pbs_date(v, tz).unwrap_or(0),
                "reserve_end" => end = fmt::parse_pbs_date(v, tz).unwrap_or(0),
                "Resource_List.nodect" => nodect = v.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    flush(&mut name, &mut start, &mut end, &mut nodect, &mut best);
    best
}

pub fn fetch(tz: i64) -> Result<Cluster, String> {
    let qstat = crate::pbs::qstat_bin();

    // --- server: name, the queue jobs default to, and the job backlog --------
    let sv = json_cmd(&qstat, &["-B", "-f", "-F", "json"])?;
    let server_name = sv
        .get("pbs_server")
        .and_then(|v| v.as_str())
        .unwrap_or("pbs")
        .to_string();
    let timestamp = sv.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
    let server = sv
        .get("Server")
        .and_then(|s| s.as_object())
        .and_then(|m| m.values().next().cloned())
        .unwrap_or(Value::Null);
    let queue = server
        .get("default_queue")
        .and_then(|v| v.as_str())
        .unwrap_or("abciq")
        .to_string();
    let counts = server
        .get("state_count")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // --- nodes: the authority on free capacity -------------------------------
    let nv = json_cmd(&pbsnodes_bin(), &["-a", "-F", "json"])?;
    let nodes = nv
        .get("nodes")
        .and_then(|n| n.as_object())
        .ok_or("pbsnodes returned no nodes")?;

    let mut c = Cluster {
        server: server_name,
        queue: queue.clone(),
        timestamp,
        pool: 0,
        offline: 0,
        gpus_total: 0,
        gpus_free: 0,
        cpus_total: 0,
        cpus_free: 0,
        nodes_idle: 0,
        nodes_busy: 0,
        free_gpus_on_busy: 0,
        running: state_count(counts, "Running"),
        queued: state_count(counts, "Queued"),
        held: state_count(counts, "Held"),
        limit_user_nodes: None,
        limit_cluster_nodect: None,
        next_resv: None,
    };

    for node in nodes.values() {
        let qlist = node
            .get("resources_available")
            .and_then(|r| r.get("Qlist"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !qlist.split(',').any(|q| q.trim() == queue) {
            continue; // dedicated to some other queue — not yours to use
        }
        c.pool += 1;

        let state = node.get("state").and_then(|v| v.as_str()).unwrap_or("");
        if state.contains("offline") || state.contains("down") {
            c.offline += 1;
            continue;
        }

        let av_g = num(node, "resources_available", "ngpus");
        let av_c = num(node, "resources_available", "ncpus");
        let as_g = num(node, "resources_assigned", "ngpus");
        let as_c = num(node, "resources_assigned", "ncpus");

        c.gpus_total += av_g;
        c.gpus_free += av_g.saturating_sub(as_g);
        c.cpus_total += av_c;
        c.cpus_free += av_c.saturating_sub(as_c);

        if as_g == 0 && as_c == 0 {
            c.nodes_idle += 1;
        } else {
            c.nodes_busy += 1;
            c.free_gpus_on_busy += av_g.saturating_sub(as_g);
        }
    }

    // --- optional extras: never fail the whole view over these ---------------
    if let Ok(qv) = json_cmd(&qstat, &["-Q", "-f", &queue, "-F", "json"]) {
        if let Some(q) = qv
            .get("Queue")
            .and_then(|s| s.as_object())
            .and_then(|m| m.values().next())
        {
            let mrr = q.get("max_run_res");
            c.limit_cluster_nodect = mrr
                .and_then(|m| m.get("nodect"))
                .and_then(|v| v.as_str())
                .and_then(limit_value);
            c.limit_user_nodes = mrr
                .and_then(|m| m.get("spot_rt_QF"))
                .and_then(|v| v.as_str())
                .and_then(limit_value);
        }
    }
    c.next_resv = next_reservation(timestamp, tz);

    Ok(c)
}
