//! Terminal rendering: a responsive table with walltime bars, or one card per
//! job in long mode.

use crate::fmt::{bar, commas, ellipsize, hm, hms, pad, points, rpad, short_datetime, size};
use crate::pbs::{GroupPoints, Job, Snapshot, State};
use std::collections::HashMap;

pub struct View<'a> {
    pub color: bool,
    pub ascii: bool,
    pub width: usize,
    pub long: bool,
    pub show_points: bool,
    pub rates: &'a HashMap<String, f64>,
    pub tz: i64,
    pub now: i64,
}

impl View<'_> {
    fn c(&self, code: &str, s: &str) -> String {
        if self.color && !s.is_empty() {
            format!("\x1b[{code}m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }

    fn dim(&self, s: &str) -> String {
        self.c("2", s)
    }

    fn bullet(&self) -> &'static str {
        if self.ascii {
            "*"
        } else {
            "●"
        }
    }

    fn warn_sign(&self) -> &'static str {
        if self.ascii {
            "!"
        } else {
            "⚠"
        }
    }

    fn sep(&self) -> String {
        self.dim(if self.ascii { " | " } else { " · " })
    }

    /// Colour a walltime bar by how close the job is to being killed.
    fn bar_color(&self, frac: f64) -> &'static str {
        if frac >= 0.9 {
            "31"
        } else if frac >= 0.7 {
            "33"
        } else {
            "32"
        }
    }

    fn bar_cell(&self, frac: Option<f64>, width: usize) -> String {
        let (open, close) = if self.ascii { ("[", "]") } else { ("▕", "▏") };
        match frac {
            Some(f) => format!(
                "{}{}{}",
                self.dim(open),
                self.c(self.bar_color(f), &bar(f, width, self.ascii)),
                self.dim(close)
            ),
            None => format!(
                "{}{}{}",
                self.dim(open),
                self.dim(&bar(0.0, width, self.ascii)),
                self.dim(close)
            ),
        }
    }
}

/// Which optional columns fit at the current terminal width.
struct Cols {
    gpu: bool,
    bar: bool,
    time: bool,
    points: bool,
    node: bool,
}

const W_ID: usize = 8;
const W_ST: usize = 2;
const W_RES: usize = 9;
const W_GPU: usize = 4;
const W_BAR: usize = 14;
const W_PCT: usize = 5;
const W_TIME: usize = 14;
const W_PTS: usize = 12;
const W_NODE: usize = 9;
const MIN_NAME: usize = 14;

impl Cols {
    fn fixed(&self) -> usize {
        // Every column carries one trailing space.
        let mut w = (W_ID + 1) + (W_ST + 1) + (W_RES + 1) + (W_PCT + 1);
        if self.gpu {
            w += W_GPU + 1;
        }
        if self.bar {
            w += W_BAR + 2 + 1;
        }
        if self.time {
            w += W_TIME + 1;
        }
        if self.points {
            w += W_PTS + 1;
        }
        if self.node {
            w += W_NODE + 1;
        }
        w + 1 // leading indent
    }
}

fn fit(width: usize) -> (Cols, usize) {
    let mut cols = Cols {
        gpu: true,
        bar: true,
        time: true,
        points: true,
        node: true,
    };
    // Drop columns least-first until the name column can breathe.
    for step in 0..5 {
        if cols.fixed() + MIN_NAME <= width {
            break;
        }
        match step {
            0 => cols.node = false,
            1 => cols.points = false,
            2 => cols.gpu = false,
            3 => cols.bar = false,
            _ => cols.time = false,
        }
    }
    let name = width.saturating_sub(cols.fixed()).max(MIN_NAME);
    (cols, name)
}

pub fn render(snap: &Snapshot, jobs: &[&Job], view: &View, who: &str) -> String {
    let mut out = String::new();
    out.push_str(&header(snap, jobs, view, who));
    if !view.long {
        out.push('\n');
    }

    if jobs.is_empty() {
        out.push_str(&format!(
            "  {}\n",
            view.dim("no jobs — submit one with `qsub script.sh`")
        ));
        return out;
    }

    if view.long {
        for j in jobs {
            out.push('\n');
            out.push_str(&card(j, view));
        }
    } else {
        let (cols, name_w) = fit(view.width);
        out.push_str(&table_header(&cols, name_w, view));
        for j in jobs {
            out.push_str(&row(j, &cols, name_w, view));
        }
    }

    out.push_str(&footer(jobs, view));
    out
}

fn header(snap: &Snapshot, jobs: &[&Job], view: &View, who: &str) -> String {
    let mut counts: Vec<(State, usize)> = Vec::new();
    for j in jobs {
        match counts.iter_mut().find(|(s, _)| *s == j.state) {
            Some((_, n)) => *n += 1,
            None => counts.push((j.state, 1)),
        }
    }
    counts.sort_by_key(|(s, _)| s.rank());

    let gpus: u64 = jobs
        .iter()
        .filter(|j| j.state == State::Running)
        .map(|j| j.ngpus)
        .sum();
    let burn: f64 = jobs
        .iter()
        .filter(|j| j.state == State::Running)
        .filter_map(|j| j.point_rate(view.rates))
        .sum();

    let mut parts = vec![
        view.c("1", &format!("ABCI-Q {}", snap.server)),
        view.c("36", who),
    ];
    for (s, n) in &counts {
        parts.push(view.c(s.color(), &format!("{n} {}", s.label())));
    }
    if gpus > 0 {
        parts.push(format!("{gpus} GPU"));
    }
    if burn > 0.0 {
        parts.push(format!("{} pt/h", trim_num(burn)));
    }
    parts.push(view.dim(&short_datetime(view.now, view.tz)));

    format!(" {}\n", parts.join(&view.sep()))
}

fn table_header(cols: &Cols, name_w: usize, view: &View) -> String {
    let mut line = format!(" {} {} {} {} ", pad("ID", W_ID), pad("", W_ST), pad("NAME", name_w), pad("TYPE", W_RES));
    if cols.gpu {
        line.push_str(&format!("{} ", rpad("GPU", W_GPU)));
    }
    if cols.bar {
        line.push_str(&format!("{} ", pad("WALLTIME", W_BAR + 2)));
    }
    line.push_str(&format!("{} ", rpad("%", W_PCT)));
    if cols.time {
        line.push_str(&format!("{} ", pad("USED/REQ", W_TIME)));
    }
    if cols.points {
        line.push_str(&format!("{} ", pad("POINTS", W_PTS)));
    }
    if cols.node {
        line.push_str(&format!("{} ", pad("NODE", W_NODE)));
    }
    format!("{}\n", view.dim(line.trim_end()))
}

fn row(j: &Job, cols: &Cols, name_w: usize, view: &View) -> String {
    let frac = j.walltime_frac();

    let mut line = String::from(" ");
    line.push_str(&view.c(j.state.color(), &pad(&ellipsize(&j.short_id, W_ID), W_ID)));
    line.push(' ');
    line.push_str(&view.c(
        j.state.color(),
        &format!("{}{}", view.bullet(), j.state.code()),
    ));
    line.push(' ');
    line.push_str(&pad(&ellipsize(&j.name, name_w), name_w));
    line.push(' ');
    line.push_str(&pad(&ellipsize(&j.resource_label(), W_RES), W_RES));
    line.push(' ');

    if cols.gpu {
        let g = if j.ngpus > 0 {
            j.ngpus.to_string()
        } else {
            "-".to_string()
        };
        line.push_str(&rpad(&g, W_GPU));
        line.push(' ');
    }
    if cols.bar {
        match frac {
            Some(_) => line.push_str(&view.bar_cell(frac, W_BAR)),
            // An empty bar says nothing; the time spent waiting does.
            None => {
                let label = match j.wait_secs(view.now) {
                    Some(w) => format!("  {} {}", j.state.label(), hm(w)),
                    None => format!("  {}", j.state.label()),
                };
                line.push_str(&view.dim(&pad(&ellipsize(&label, W_BAR + 2), W_BAR + 2)));
            }
        }
        line.push(' ');
    }

    let pct = match frac {
        Some(f) => format!("{:.0}%", f * 100.0),
        None => "-".to_string(),
    };
    match frac {
        Some(f) => line.push_str(&view.c(view.bar_color(f), &rpad(&pct, W_PCT))),
        None => line.push_str(&view.dim(&rpad(&pct, W_PCT))),
    }
    line.push(' ');

    if cols.time {
        let t = match (j.walltime_used, j.walltime_req) {
            (Some(u), Some(r)) => format!("{}/{}", hm(u), hm(r)),
            (None, Some(r)) => format!("-/{}", hm(r)),
            _ => "-".to_string(),
        };
        line.push_str(&pad(&ellipsize(&t, W_TIME), W_TIME));
        line.push(' ');
    }

    if cols.points {
        let p = match (j.points_spent(view.rates), j.points_reserved(view.rates)) {
            (Some(sp), Some(rv)) => format!("{}/{}", points(sp), points(rv)),
            (None, Some(rv)) => format!("0/{}", points(rv)),
            _ => "-".to_string(),
        };
        line.push_str(&pad(&ellipsize(&p, W_PTS), W_PTS));
        line.push(' ');
    }

    if cols.node {
        let n = if j.nodes.is_empty() {
            "-".to_string()
        } else if j.nodes.len() == 1 {
            j.nodes[0].clone()
        } else {
            format!("{}+{}", j.nodes[0], j.nodes.len() - 1)
        };
        line.push_str(&view.dim(&pad(&ellipsize(&n, W_NODE), W_NODE)));
    }

    format!("{}\n", line.trim_end())
}

fn card(j: &Job, view: &View) -> String {
    let mut out = String::new();
    let frac = j.walltime_frac();

    out.push_str(&format!(
        " {} {}  {}{}\n",
        view.c(j.state.color(), view.bullet()),
        view.c("1", &j.id),
        view.c("1", &ellipsize(&j.name, view.width.saturating_sub(38))),
        view.dim(&format!(
            "   {} · {}{}",
            j.state.label(),
            j.queue,
            if j.service.is_empty() {
                String::new()
            } else {
                format!(" · {}", j.service)
            }
        )),
    ));

    let mut spec = vec![j.resource_label()];
    if j.ncpus > 0 {
        spec.push(format!("{} CPU", j.ncpus));
    }
    if j.ngpus > 0 {
        spec.push(format!("{} GPU", j.ngpus));
    }
    if !j.mem_req.is_empty() {
        spec.push(j.mem_req.clone());
    }
    if !j.nodes.is_empty() {
        spec.push(j.nodes.join(","));
    }
    if !j.group.is_empty() {
        spec.push(format!("charged {}", j.group));
    }
    out.push_str(&format!("   {}\n", spec.join(&view.sep())));

    if let (Some(f), Some(u), Some(r)) = (frac, j.walltime_used, j.walltime_req) {
        out.push_str(&format!(
            "   {} {} {}  {} used of {}{}\n",
            view.dim("walltime"),
            view.bar_cell(Some(f), 24),
            view.c(view.bar_color(f), &format!("{:.1}%", f * 100.0)),
            hms(u),
            hms(r),
            match j.walltime_left() {
                Some(l) if j.state == State::Running =>
                    format!("{}{} left", view.sep(), hms(l)),
                _ => String::new(),
            }
        ));
    } else if let Some(w) = j.wait_secs(view.now) {
        out.push_str(&format!(
            "   {} {} {}{}{}\n",
            view.dim("walltime"),
            j.state.label(),
            hms(w),
            view.sep(),
            match j.walltime_req {
                Some(r) => format!("{} requested", hms(r)),
                None => "no limit".to_string(),
            }
        ));
    }

    // For anything not running, PBS's comment is the scheduler explaining
    // itself ("Not Running: Insufficient amount of resource ...").
    if j.state != State::Running {
        if let Some(c) = &j.comment {
            out.push_str(&format!(
                "   {} {}\n",
                view.dim("why     "),
                ellipsize(c, view.width.saturating_sub(13))
            ));
        }
    }

    let mut usage = Vec::new();
    if let Some(cp) = j.cpupercent {
        let cores = cp / 100.0;
        usage.push(format!(
            "cpu {:.0}% ({:.1} of {} cores)",
            cp, cores, j.ncpus
        ));
    }
    if let Some(m) = j.mem_used_kb {
        usage.push(format!("mem {}", size(m)));
    }
    if !usage.is_empty() {
        out.push_str(&format!(
            "   {} {}\n",
            view.dim("usage   "),
            usage.join(&view.sep())
        ));
    }

    if let (Some(sp), Some(rv)) = (j.points_spent(view.rates), j.points_reserved(view.rates)) {
        out.push_str(&format!(
            "   {} {} spent of {} reserved{}{}\n",
            view.dim("points  "),
            points(sp),
            points(rv),
            view.sep(),
            format!("{} pt/h", trim_num(j.point_rate(view.rates).unwrap_or(0.0)))
        ));
    }

    if let Some(st) = j.stime {
        out.push_str(&format!(
            "   {} {}{}\n",
            view.dim("started "),
            short_datetime(st, view.tz),
            match j.qtime {
                Some(q) => format!("{}submitted {}", view.sep(), short_datetime(q, view.tz)),
                None => String::new(),
            }
        ));
    } else if let Some(q) = j.qtime {
        out.push_str(&format!(
            "   {} {}{}\n",
            view.dim("queued  "),
            short_datetime(q, view.tz),
            match &j.est_start {
                Some(e) => format!("{}estimated start {}", view.sep(), e),
                None => String::new(),
            }
        ));
    }

    if let Some(code) = j.exit_status {
        let style = if code == 0 { "32" } else { "31" };
        out.push_str(&format!(
            "   {} {}\n",
            view.dim("exit    "),
            view.c(style, &code.to_string())
        ));
    }

    if let Some(p) = &j.log_path {
        out.push_str(&format!("   {} {}\n", view.dim("log     "), p));
    }
    if let Some(a) = &j.submit_args {
        out.push_str(&format!("   {} {}\n", view.dim("submit  "), a));
    }

    out
}

fn footer(jobs: &[&Job], view: &View) -> String {
    let mut out = String::new();

    let spent: f64 = jobs.iter().filter_map(|j| j.points_spent(view.rates)).sum();
    let reserved: f64 = jobs
        .iter()
        .filter(|j| j.state != State::Finished)
        .filter_map(|j| j.points_reserved(view.rates))
        .sum();

    if reserved > 0.0 || spent > 0.0 {
        out.push_str(&format!(
            "\n {}\n",
            view.dim(&format!(
                "{} pt spent so far · {} pt reserved by these jobs",
                points(spent),
                points(reserved)
            ))
        ));
    }

    // Jobs about to be killed by their own walltime limit.
    let mut tight: Vec<&&Job> = jobs
        .iter()
        .filter(|j| j.state == State::Running && j.walltime_frac().unwrap_or(0.0) >= 0.9)
        .collect();
    tight.sort_by(|a, b| {
        b.walltime_frac()
            .unwrap_or(0.0)
            .partial_cmp(&a.walltime_frac().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for j in tight {
        out.push_str(&format!(
            " {} {} {} at {:.0}% of walltime — {} left\n",
            view.c("31", view.warn_sign()),
            view.c("1", &j.short_id),
            ellipsize(&j.name, 28),
            j.walltime_frac().unwrap_or(0.0) * 100.0,
            hms(j.walltime_left().unwrap_or(0))
        ));
    }

    if view.show_points {
        let mut groups: Vec<&String> = jobs
            .iter()
            .filter(|j| !j.group.is_empty())
            .map(|j| &j.group)
            .collect();
        groups.sort();
        groups.dedup();
        if !groups.is_empty() {
            let balances: Vec<GroupPoints> = crate::pbs::show_point();
            for g in groups {
                if let Some(b) = balances.iter().find(|b| &b.group == g) {
                    let used_frac = if b.granted > 0.0 {
                        b.used / b.granted
                    } else {
                        0.0
                    };
                    let committed: f64 = jobs
                        .iter()
                        .filter(|j| &j.group == g && j.state != State::Finished)
                        .filter_map(|j| j.points_reserved(view.rates))
                        .sum();
                    let over = committed > b.left();
                    out.push_str(&format!(
                        " {} {} {} {} left of {}{}\n",
                        pad(g, 12),
                        view.bar_cell(Some(used_frac), 12),
                        view.c(
                            view.bar_color(used_frac),
                            &rpad(&format!("{:.0}%", used_frac * 100.0), 4)
                        ),
                        commas(b.left()),
                        commas(b.granted),
                        if over {
                            view.c(
                                "31",
                                &format!(
                                    "  {} {} pt reserved exceeds the balance",
                                    view.warn_sign(),
                                    commas(committed)
                                ),
                            )
                        } else {
                            String::new()
                        }
                    ));
                }
            }
        }
    }

    out
}

/// `5` not `5.0`, but keep a decimal when there is one.
fn trim_num(v: f64) -> String {
    if (v - v.round()).abs() < 0.05 {
        format!("{:.0}", v)
    } else {
        format!("{:.1}", v)
    }
}
