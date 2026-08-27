//! qlog — stream and search PBS job logs on ABCI-Q.
//!
//! Companion binary to qrich, sharing its PBS plumbing. Jobs are addressed by
//! id: each job's own Output_Path / Error_Path attribute says where its log
//! lives, so you never hunt for paths. Modes: an index of every job's log
//! file (a j/k picker on a terminal), a paged full-screen live view with one
//! page per job, and a substring search across all logs.

#![allow(dead_code)]

#[path = "../fmt.rs"]
mod fmt;
#[path = "../pbs.rs"]
mod pbs;

use pbs::Job;
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
use std::process::{Command, ExitCode, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

const USAGE: &str = "\
qlog — stream and search PBS job logs on ABCI-Q (companion to qrich)

USAGE:
    qlog [OPTIONS] [JOBID...]

MODES:
    (default)               index of every job's log file; on a terminal, j/k
                            moves a cursor and Enter opens that job's page
                            (piped output stays a plain table)
    -f, --follow            full-screen live view: one page per job plus an
                            \"all\" page, each with a title bar and a tab bar.
                            1-9 page · j/k or n/p cycle · a all · u/d scroll
                            g oldest · G tail · q quit
                            (* on a tab = output arrived on a hidden page)
                            Piped, it degrades to a plain [jobid]-prefixed
                            stream.
    -g, --grep PATTERN      search the logs (plain substring, no regex)
    -p, --paths             print log paths only (for less/vim/scp)

OPTIONS:
    -i, --ignore-case       case-insensitive search (ASCII)
    -C, --context N         context lines around matches (default 0)
    -n, --tail N            backlog lines per log (default 10 piped, a full
                            page in the live view)
    -x, --history           include recently finished jobs
    -a, --all               every user's jobs, not just your own
    -u, --user USER         jobs owned by USER
        --color WHEN        always | never | auto (default auto)
        --width COLS        override the detected terminal width
    -h, --help              this message
    -V, --version           print the version

NOTES:
    A running job with no log on disk usually lacks `#PBS -k oed` — its output
    stays spooled on the compute node until the job ends.
    `qlog -f -g PATTERN` streams only matching lines.
    Search exits 0 if something matched, 1 if nothing did.
";

const JOB_COLORS: [&str; 6] = ["36", "35", "33", "32", "94", "95"];

struct Opts {
    follow: bool,
    grep: Option<String>,
    icase: bool,
    context: usize,
    tail: usize,
    tail_set: bool,
    history: bool,
    all_users: bool,
    user: Option<String>,
    paths: bool,
    color: Option<bool>,
    width: Option<usize>,
    bar_preview: bool,
    page_preview: bool,
    ids: Vec<String>,
}

fn parse_args() -> Result<Option<Opts>, String> {
    let mut o = Opts {
        follow: false,
        grep: None,
        icase: false,
        context: 0,
        tail: 10,
        tail_set: false,
        history: false,
        all_users: false,
        user: None,
        paths: false,
        color: None,
        width: None,
        bar_preview: false,
        page_preview: false,
        ids: Vec::new(),
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        match a {
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(None);
            }
            "-V" | "--version" => {
                println!("qlog {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }
            "-f" | "--follow" => o.follow = true,
            "-p" | "--paths" => o.paths = true,
            "-i" | "--ignore-case" => o.icase = true,
            "-x" | "--history" => o.history = true,
            "-a" | "--all" => o.all_users = true,
            "--bar-preview" => o.bar_preview = true, // hidden: render the tab bar once
            "--page-preview" => o.page_preview = true, // hidden: render one page frame
            "-g" | "--grep" => {
                i += 1;
                o.grep = Some(args.get(i).ok_or("--grep needs a pattern")?.clone());
            }
            "-u" | "--user" => {
                i += 1;
                o.user = Some(args.get(i).ok_or("--user needs a username")?.clone());
            }
            "-C" | "--context" => {
                i += 1;
                o.context = args
                    .get(i)
                    .ok_or("--context needs a number")?
                    .parse()
                    .map_err(|_| "--context needs a number")?;
            }
            "-n" | "--tail" => {
                i += 1;
                o.tail = args
                    .get(i)
                    .ok_or("--tail needs a number")?
                    .parse()
                    .map_err(|_| "--tail needs a number")?;
                o.tail_set = true;
            }
            "--color" => {
                i += 1;
                o.color = match args.get(i).map(|s| s.as_str()) {
                    Some("always") => Some(true),
                    Some("never") => Some(false),
                    Some("auto") => None,
                    _ => return Err("--color takes always, never, or auto".into()),
                };
            }
            "--width" => {
                i += 1;
                o.width = Some(
                    args.get(i)
                        .ok_or("--width needs a number")?
                        .parse()
                        .map_err(|_| "--width needs a number")?,
                );
            }
            _ if a.starts_with('-') => return Err(format!("unknown option: {a}")),
            _ => o.ids.push(a.to_string()),
        }
        i += 1;
    }
    Ok(Some(o))
}

fn current_user() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .unwrap_or_default()
}

#[derive(Clone, Copy)]
struct Pal {
    on: bool,
}

impl Pal {
    fn c(&self, code: &str, s: &str) -> String {
        if self.on && !s.is_empty() {
            format!("\x1b[{code}m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
    fn dim(&self, s: &str) -> String {
        self.c("2", s)
    }
}

/// Write to stdout, exiting quietly if the reader went away (e.g. `| head`).
fn put(s: &str) {
    let mut out = std::io::stdout().lock();
    if out.write_all(s.as_bytes()).and_then(|_| out.flush()).is_err() {
        std::process::exit(0);
    }
}

/// One log file belonging to one job (`.e` suffix marks a separate stderr).
struct Target<'a> {
    job: &'a Job,
    jidx: usize,
    label: String,
    path: String,
    color: &'static str,
}

fn build_targets<'a>(jobs: &'a [&'a Job]) -> Vec<Target<'a>> {
    let mut ts = Vec::new();
    for (jidx, j) in jobs.iter().enumerate() {
        let color = JOB_COLORS[jidx % JOB_COLORS.len()];
        if let Some(p) = &j.log_path {
            if p != "/dev/null" {
                ts.push(Target {
                    job: j,
                    jidx,
                    label: j.short_id.clone(),
                    path: p.clone(),
                    color,
                });
            }
        }
        // Only jobs without -j oe have a live separate stderr file.
        if !j.join_oe {
            if let Some(e) = &j.error_path {
                if e != "/dev/null" && j.log_path.as_deref() != Some(e.as_str()) {
                    ts.push(Target {
                        job: j,
                        jidx,
                        label: format!("{}.e", j.short_id),
                        path: e.clone(),
                        color,
                    });
                }
            }
        }
    }
    ts
}

// ---------------------------------------------------------------------------
// text helpers
// ---------------------------------------------------------------------------

/// Substring find; `icase` is ASCII-insensitive. Returns a byte offset that is
/// always a char boundary (lead bytes and ASCII never collide with
/// continuation bytes).
fn find_ci(hay: &[u8], needle: &[u8], icase: bool) -> Option<usize> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    if icase {
        hay.windows(needle.len())
            .position(|w| w.eq_ignore_ascii_case(needle))
    } else {
        hay.windows(needle.len()).position(|w| w == needle)
    }
}

fn highlight(pal: Pal, line: &str, pat: &str, icase: bool) -> String {
    if !pal.on {
        return line.to_string();
    }
    let mut out = String::new();
    let mut rest = line;
    while let Some(i) = find_ci(rest.as_bytes(), pat.as_bytes(), icase) {
        let end = i + pat.len();
        match (rest.get(..i), rest.get(i..end), rest.get(end..)) {
            (Some(a), Some(b), Some(c)) => {
                out.push_str(a);
                out.push_str(&pal.c("1;31", b));
                rest = c;
            }
            _ => break,
        }
    }
    out.push_str(rest);
    out
}

fn after_last_cr(b: &[u8]) -> &[u8] {
    match b.iter().rposition(|c| *c == b'\r') {
        Some(i) => &b[i + 1..],
        None => b,
    }
}

/// Raw line bytes -> what the terminal would show: trailing CR stripped, only
/// the text after the last `\r` (progress bars rewrite the line), capped.
fn display_bytes(mut b: &[u8]) -> String {
    while b.last() == Some(&b'\r') {
        b = &b[..b.len() - 1];
    }
    let b = after_last_cr(b);
    let s = String::from_utf8_lossy(b);
    if s.chars().count() > 4000 {
        let t: String = s.chars().take(4000).collect();
        format!("{t}…")
    } else {
        s.into_owned()
    }
}

/// Display width of one char: CJK and fullwidth count as 2 columns.
fn char_width(c: char) -> usize {
    match c as u32 {
        0x1100..=0x115F        // Hangul Jamo
        | 0x2E80..=0xA4CF      // CJK radicals .. Yi
        | 0xAC00..=0xD7A3      // Hangul syllables
        | 0xF900..=0xFAFF      // CJK compat ideographs
        | 0xFE30..=0xFE4F      // CJK compat forms
        | 0xFF00..=0xFF60      // fullwidth forms
        | 0xFFE0..=0xFFE6
        | 0x20000..=0x3FFFD => 2,
        _ => 1,
    }
}

fn disp_width(s: &str) -> usize {
    s.chars().map(char_width).sum()
}

/// Truncate to at most `max` display columns, marking elision.
fn truncate_w(s: &str, max: usize) -> String {
    if disp_width(s) <= max {
        return s.to_string();
    }
    let mut out = String::new();
    let mut w = 0;
    for c in s.chars() {
        let cw = char_width(c);
        if w + cw > max.saturating_sub(1) {
            break;
        }
        out.push(c);
        w += cw;
    }
    out.push('…');
    out
}

/// Pad with spaces to exactly `width` display columns (truncating if over).
fn pad_w(s: &str, width: usize) -> String {
    let t = truncate_w(s, width);
    let w = disp_width(&t);
    format!("{}{}", t, " ".repeat(width.saturating_sub(w)))
}

/// Path shortened from the front — the filename end is the part that matters.
fn ellipsize_front(s: &str, max: usize) -> String {
    let n = s.chars().count();
    if n <= max {
        return s.to_string();
    }
    let tail: String = s.chars().skip(n - (max - 1)).collect();
    format!("…{tail}")
}

fn human_bytes(b: u64) -> String {
    if b < 1024 {
        format!("{b} B")
    } else {
        fmt::size(b / 1024)
    }
}

fn ago(m: SystemTime) -> String {
    match SystemTime::now().duration_since(m) {
        Ok(d) if d.as_secs() < 60 => format!("{}s", d.as_secs()),
        Ok(d) => fmt::until(d.as_secs() as i64),
        Err(_) => "0s".to_string(),
    }
}

/// Terminal (rows, cols) via `stty size`; honors a --width override for cols.
fn term_size(width_override: Option<usize>) -> (usize, usize) {
    let mut rows = 30usize;
    let mut cols = 100usize;
    if let Ok(out) = Command::new("stty")
        .arg("size")
        .stdin(Stdio::inherit())
        .output()
    {
        let t = String::from_utf8_lossy(&out.stdout);
        let mut it = t.split_whitespace();
        if let (Some(r), Some(c)) = (it.next(), it.next()) {
            rows = r.parse().unwrap_or(30);
            cols = c.parse().unwrap_or(100);
        }
    }
    if let Some(w) = width_override {
        cols = w;
    }
    (rows.max(5), cols.max(40))
}

// ---------------------------------------------------------------------------
// index table, list mode, picker
// ---------------------------------------------------------------------------

/// The index table: header plus, per target, a colored row and a plain
/// (uncolored) twin the picker can reverse-video as its cursor. The leading
/// digit is the job's key in the picker and the live view's tab bar.
fn build_rows(targets: &[Target], pal: Pal, width: usize) -> (String, Vec<(String, String)>) {
    let label_w = targets
        .iter()
        .map(|t| t.label.chars().count())
        .max()
        .unwrap_or(6)
        .max(6);
    const NAME_W: usize = 22;
    const SIZE_W: usize = 9;
    const AGE_W: usize = 7;
    let fixed = 1 + 2 + label_w + 1 + 2 + 1 + NAME_W + 1 + SIZE_W + 1 + AGE_W + 1;
    let path_w = width.saturating_sub(fixed).max(24);

    let header = pal.dim(&format!(
        " {} {} {} {} {} {} {}",
        " ",
        fmt::pad("ID", label_w),
        "S ",
        fmt::pad("NAME", NAME_W),
        fmt::rpad("SIZE", SIZE_W),
        fmt::rpad("WRITE", AGE_W),
        "PATH"
    ));

    let row = |t: &Target, key: &str, p: Pal| -> String {
        let (size_s, age_s) = match std::fs::metadata(&t.path) {
            Ok(m) => (
                human_bytes(m.len()),
                m.modified().map(ago).unwrap_or_else(|_| "-".into()),
            ),
            Err(_) => ("-".to_string(), "-".to_string()),
        };
        format!(
            " {} {} {} {} {} {} {}",
            p.c("1", &fmt::pad(key, 1)),
            p.c(t.color, &fmt::pad(&t.label, label_w)),
            p.c(t.job.state.color(), &fmt::pad(t.job.state.code(), 2)),
            fmt::pad(&fmt::ellipsize(&t.job.name, NAME_W), NAME_W),
            fmt::rpad(&size_s, SIZE_W),
            fmt::rpad(&age_s, AGE_W),
            p.dim(&ellipsize_front(&t.path, path_w)),
        )
    };

    let plain = Pal { on: false };
    let mut rows = Vec::new();
    let mut seen_jidx = usize::MAX;
    for t in targets {
        // Number only the job's first row; a .e row belongs to the same key.
        let key = if t.jidx != seen_jidx && t.jidx < 9 {
            seen_jidx = t.jidx;
            (t.jidx + 1).to_string()
        } else {
            seen_jidx = t.jidx;
            " ".to_string()
        };
        rows.push((row(t, &key, pal), row(t, &key, plain)));
    }
    (header, rows)
}

fn list(targets: &[Target], pal: Pal, width: usize) {
    let (header, rows) = build_rows(targets, pal, width);
    put(&format!("{header}\n"));
    for (colored, _) in &rows {
        put(&format!("{colored}\n"));
    }
    if targets.iter().any(|t| std::fs::metadata(&t.path).is_err()) {
        put(&format!(
            " {}\n",
            pal.dim("- = no file yet: job not started, or output spooled without #PBS -k oed")
        ));
    }
    put(&format!(
        " {}\n",
        pal.dim("follow: qlog -f · search: qlog -g PATTERN · open: less $(qlog -p <jobid>)")
    ));
}

/// Interactive index: j/k moves a cursor over the rows, Enter opens that job's
/// page, 1-9 opens a job directly, f opens the "all" page.
fn picker(targets: &[Target], nj: usize, pal: Pal, width: usize, keys: &Keys) -> Option<usize> {
    let (header, rows) = build_rows(targets, pal, width);
    put(&format!("{header}\n"));
    let hint = pal.dim(" j/k move · Enter open · 1-9 open · f follow all · q quit");
    let mut cursor = 0usize;

    let paint = |cursor: usize, first: bool| {
        let mut out = String::new();
        if !first {
            out.push_str(&format!("\x1b[{}A\r", rows.len() + 1));
        }
        for (i, (colored, plain)) in rows.iter().enumerate() {
            out.push_str("\x1b[2K");
            if i == cursor {
                if pal.on {
                    out.push_str(&pal.c("7", plain));
                } else {
                    out.push_str(&format!(">{}", &plain[1..]));
                }
            } else {
                out.push_str(colored);
            }
            out.push('\n');
        }
        out.push_str(&format!("\x1b[2K{hint}\n"));
        put(&out);
    };
    paint(cursor, true);

    loop {
        match keys.rx.recv_timeout(Duration::from_millis(300)) {
            Ok(k) => match k {
                b'q' | 3 | 4 | 27 => return None,
                b'j' | b'n' => {
                    if cursor + 1 < rows.len() {
                        cursor += 1;
                        paint(cursor, false);
                    }
                }
                b'k' | b'p' => {
                    if cursor > 0 {
                        cursor -= 1;
                        paint(cursor, false);
                    }
                }
                b'\r' | b'\n' => return Some(targets[cursor].jidx),
                b'f' | b'a' => return Some(usize::MAX), // the "all" page
                b'1'..=b'9' => {
                    let j = (k - b'1') as usize;
                    if j < nj {
                        return Some(j);
                    }
                }
                _ => {}
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(_) => return None,
        }
    }
}

// ---------------------------------------------------------------------------
// search mode
// ---------------------------------------------------------------------------

fn search_mode(targets: &[Target], pat: &str, o: &Opts, pal: Pal) -> ExitCode {
    if pat.is_empty() {
        eprintln!("qlog: empty pattern");
        return ExitCode::from(2);
    }
    if targets.is_empty() {
        eprintln!("qlog: no logs to search");
        return ExitCode::FAILURE;
    }
    let label_w = targets
        .iter()
        .map(|t| t.label.chars().count())
        .max()
        .unwrap_or(6);
    let (mut total, mut files_hit, mut scanned, mut unreadable) = (0usize, 0usize, 0usize, 0usize);
    for t in targets {
        match search_file(t, pat, o, pal, label_w) {
            Ok(n) => {
                scanned += 1;
                if n > 0 {
                    files_hit += 1;
                    total += n;
                }
            }
            Err(_) => unreadable += 1,
        }
    }
    let note = if unreadable > 0 {
        format!(" · {unreadable} unreadable/missing")
    } else {
        String::new()
    };
    put(&format!(
        " {}\n",
        pal.dim(&format!(
            "{total} matching line{} in {files_hit} of {scanned} logs{note}",
            if total == 1 { "" } else { "s" }
        ))
    ));
    if total > 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn search_file(
    t: &Target,
    pat: &str,
    o: &Opts,
    pal: Pal,
    label_w: usize,
) -> std::io::Result<usize> {
    let f = std::fs::File::open(&t.path)?;
    let mut r = BufReader::with_capacity(1 << 18, f);
    let mut raw: Vec<u8> = Vec::new();
    let mut lineno = 0u64;
    let mut before: VecDeque<(u64, String)> = VecDeque::new();
    let mut after = 0usize;
    let mut last_printed = 0u64;
    let mut n = 0usize;

    let ctx = |ln: u64, text: &str| {
        put(&format!(
            "{}{}{}\n",
            pal.c(t.color, &fmt::pad(&t.label, label_w)),
            pal.dim(&format!("-{ln}- ")),
            text
        ));
    };

    loop {
        raw.clear();
        if r.read_until(b'\n', &mut raw)? == 0 {
            break;
        }
        lineno += 1;
        while raw.last().map_or(false, |c| *c == b'\n') {
            raw.pop();
        }
        let disp = display_bytes(&raw);
        if find_ci(disp.as_bytes(), pat.as_bytes(), o.icase).is_some() {
            n += 1;
            if o.context > 0
                && last_printed > 0
                && before.front().map_or(lineno, |x| x.0) > last_printed + 1
            {
                put(&format!("{}\n", pal.dim("--")));
            }
            for (bn, bl) in before.drain(..) {
                ctx(bn, &bl);
                last_printed = bn;
            }
            put(&format!(
                "{}{}{}\n",
                pal.c(t.color, &fmt::pad(&t.label, label_w)),
                pal.dim(&format!(":{lineno}: ")),
                highlight(pal, &disp, pat, o.icase)
            ));
            if o.context > 0 {
                last_printed = lineno;
            }
            after = o.context;
        } else if after > 0 {
            ctx(lineno, &disp);
            last_printed = lineno;
            after -= 1;
        } else if o.context > 0 {
            before.push_back((lineno, disp));
            if before.len() > o.context {
                before.pop_front();
            }
        }
    }
    Ok(n)
}

// ---------------------------------------------------------------------------
// raw terminal plumbing
// ---------------------------------------------------------------------------

/// Restores the terminal with the settings saved at construction.
struct RawGuard {
    saved: Option<String>,
}

impl RawGuard {
    fn new() -> RawGuard {
        let saved = Command::new("stty")
            .arg("-g")
            .stdin(Stdio::inherit())
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
        if saved.is_some() {
            // cbreak without echo; -isig so Ctrl-C arrives as a byte and we
            // restore the terminal ourselves instead of dying mid-raw.
            let _ = Command::new("stty")
                .args(["-echo", "-icanon", "-isig", "min", "1", "time", "0"])
                .stdin(Stdio::inherit())
                .status();
        }
        RawGuard { saved }
    }
}

impl Drop for RawGuard {
    fn drop(&mut self) {
        if let Some(s) = &self.saved {
            let _ = Command::new("stty").arg(s).stdin(Stdio::inherit()).status();
        }
    }
}

/// Alternate screen + hidden cursor; the previous screen (and scrollback)
/// comes back untouched on drop.
struct ScreenGuard;

impl ScreenGuard {
    fn new() -> ScreenGuard {
        put("\x1b[?1049h\x1b[?25l\x1b[2J\x1b[H");
        ScreenGuard
    }
}

impl Drop for ScreenGuard {
    fn drop(&mut self) {
        put("\x1b[?25h\x1b[?1049l");
    }
}

/// One raw-mode stdin session shared by the picker and the live view — a
/// single reader thread for the whole process, so handing off between modes
/// never leaves a second reader stealing keystrokes.
struct Keys {
    rx: mpsc::Receiver<u8>,
    interactive: bool,
    _tx: mpsc::Sender<u8>, // keeps recv_timeout ticking when non-interactive
    _guard: Option<RawGuard>,
}

impl Keys {
    fn new(interactive: bool) -> Keys {
        let (tx, rx) = mpsc::channel::<u8>();
        let guard = if interactive {
            let g = RawGuard::new();
            let txk = tx.clone();
            std::thread::spawn(move || {
                let mut b = [0u8; 1];
                let mut si = std::io::stdin();
                loop {
                    match si.read(&mut b) {
                        Ok(n) if n > 0 => {
                            if txk.send(b[0]).is_err() {
                                break;
                            }
                        }
                        _ => break,
                    }
                }
            });
            Some(g)
        } else {
            None
        };
        Keys {
            rx,
            interactive,
            _tx: tx,
            _guard: guard,
        }
    }
}

// ---------------------------------------------------------------------------
// log streaming
// ---------------------------------------------------------------------------

struct Stream<'a> {
    t: &'a Target<'a>,
    offset: u64,
    buf: Vec<u8>,
    opened: bool,
    wait_announced: bool,
    last_partial: Instant,
}

fn make_streams<'a>(targets: &'a [Target<'a>]) -> Vec<Stream<'a>> {
    targets
        .iter()
        .map(|t| Stream {
            t,
            offset: 0,
            buf: Vec::new(),
            opened: false,
            wait_announced: false,
            last_partial: Instant::now(),
        })
        .collect()
}

const TAIL_SCAN: u64 = 1 << 18; // how far back the initial backlog looks
const MAX_READ: usize = 1 << 21; // per stream per poll

/// New display lines from one log; `.1` marks status lines (shown dim).
fn poll_stream(s: &mut Stream, tail_n: usize) -> Vec<(String, bool)> {
    let mut out: Vec<(String, bool)> = Vec::new();
    let meta = match std::fs::metadata(&s.t.path) {
        Ok(m) => m,
        Err(_) => {
            if !s.wait_announced && !s.opened {
                out.push((
                    "waiting for log file (job not started, or output spooled without #PBS -k oed)"
                        .into(),
                    true,
                ));
                s.wait_announced = true;
            }
            return out;
        }
    };
    let size = meta.len();

    if !s.opened {
        s.opened = true;
        if s.wait_announced {
            out.push(("log file appeared".into(), true));
        }
        let start = size.saturating_sub(TAIL_SCAN);
        if let Ok(mut f) = std::fs::File::open(&s.t.path) {
            if f.seek(SeekFrom::Start(start)).is_ok() {
                let mut chunk = Vec::new();
                let _ = f.take(size - start).read_to_end(&mut chunk);
                let mut segs: Vec<&[u8]> = chunk.split(|b| *b == b'\n').collect();
                s.buf = segs.pop().map(|x| x.to_vec()).unwrap_or_default();
                let segs = if start > 0 && !segs.is_empty() {
                    &segs[1..] // first segment is a partial line's tail
                } else {
                    &segs[..]
                };
                for seg in segs.iter().skip(segs.len().saturating_sub(tail_n)) {
                    out.push((display_bytes(seg), false));
                }
            }
        }
        s.offset = size;
        return out;
    }

    if size < s.offset {
        s.offset = 0;
        s.buf.clear();
        out.push(("log truncated — following from the start".into(), true));
    }
    if size > s.offset {
        if let Ok(mut f) = std::fs::File::open(&s.t.path) {
            if f.seek(SeekFrom::Start(s.offset)).is_ok() {
                let want = ((size - s.offset) as usize).min(MAX_READ);
                let mut chunk = vec![0u8; want];
                if let Ok(n) = f.read(&mut chunk) {
                    chunk.truncate(n);
                    s.offset += n as u64;
                    s.buf.extend_from_slice(&chunk);
                }
                while let Some(pos) = s.buf.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = s.buf.drain(..=pos).collect();
                    out.push((display_bytes(&line[..line.len() - 1]), false));
                    s.last_partial = Instant::now();
                }
            }
        }
    }
    // A \r-rewriting progress bar may not emit \n for minutes; surface its
    // current state as a snapshot line every few seconds.
    if !s.buf.is_empty()
        && s.buf.contains(&b'\r')
        && s.last_partial.elapsed() >= Duration::from_secs(5)
    {
        let snap = display_bytes(&s.buf);
        if !snap.trim().is_empty() {
            out.push((snap, false));
        }
        s.buf.clear();
        s.last_partial = Instant::now();
    }
    out
}

// ---------------------------------------------------------------------------
// live view (paged TUI)
// ---------------------------------------------------------------------------

/// The tab bar: which key is which job, which page is focused, `*` on tabs
/// whose job printed while hidden. Fits `width` by shrinking names, then
/// dropping the hint, then dropping trailing tabs — never the focused one.
fn tab_bar(pal: Pal, jobs: &[&Job], solo: Option<usize>, activity: &[bool], width: usize) -> String {
    let n = jobs.len();
    let key_of = |i: usize| {
        if i < 9 {
            ((b'1' + i as u8) as char).to_string()
        } else {
            "·".to_string()
        }
    };
    let hint = "j/k page · u/d scroll · q quit";

    let plain_len = |i: usize, name_w: usize| {
        let mut l = key_of(i).chars().count() + 1 + jobs[i].short_id.chars().count();
        if activity[i] {
            l += 1;
        }
        if name_w > 0 {
            l += 1 + jobs[i].name.chars().count().min(name_w);
        }
        if solo == Some(i) {
            l += 2; // side padding of the highlighted block
        }
        l
    };
    let total_len = |name_w: usize, hint_on: bool| {
        let mut l = 1 + 5 + if solo.is_none() { 2 } else { 0 }; // " a:all" (+padding)
        for i in 0..n {
            l += 2 + plain_len(i, name_w);
        }
        if hint_on {
            l += 3 + hint.chars().count();
        }
        l
    };

    let mut name_w = 14usize;
    let mut hint_on = true;
    while total_len(name_w, hint_on) > width && name_w > 0 {
        name_w = name_w.saturating_sub(4);
    }
    if total_len(name_w, hint_on) > width {
        hint_on = false;
    }

    let tab = |i: usize| -> String {
        let color = JOB_COLORS[i % JOB_COLORS.len()];
        let star = if activity[i] { "*" } else { "" };
        let name = if name_w > 0 {
            format!(" {}", fmt::ellipsize(&jobs[i].name, name_w))
        } else {
            String::new()
        };
        if solo == Some(i) {
            let raw = format!("{}:{}{}{}", key_of(i), jobs[i].short_id, star, name);
            if pal.on {
                pal.c(&format!("7;1;{color}"), &format!(" {raw} "))
            } else {
                format!("[{raw}]")
            }
        } else {
            format!(
                "{}{}{}",
                pal.c(color, &format!("{}:{}", key_of(i), jobs[i].short_id)),
                pal.c("1;33", star),
                pal.dim(&name)
            )
        }
    };

    let mut out = String::from(" ");
    if solo.is_none() {
        out.push_str(&if pal.on {
            pal.c("7;1", " a:all ")
        } else {
            "[a:all]".to_string()
        });
    } else {
        out.push_str(&pal.dim("a:all"));
    }
    let mut used = 1 + 5 + if solo.is_none() { 2 } else { 0 };
    let budget = if hint_on {
        width.saturating_sub(3 + hint.chars().count())
    } else {
        width
    };
    let mut skipped = 0usize;
    for i in 0..n {
        let l = 2 + plain_len(i, name_w);
        // Never drop the focused tab; drop trailing ones that no longer fit.
        if used + l + 4 > budget && solo != Some(i) {
            skipped += 1;
            continue;
        }
        out.push_str("  ");
        out.push_str(&tab(i));
        used += l;
    }
    if skipped > 0 {
        out.push_str(&pal.dim(&format!(" +{skipped}")));
    }
    if hint_on {
        out.push_str(&format!("   {}", pal.dim(hint)));
    }
    out
}

/// A buffered line: which target produced it, its text, and whether it is a
/// status message rather than log content.
type Entry = (usize, String, bool);

/// Full-width title bar for a page: identity on the left, live job facts
/// beside it, scroll state when not tailing.
fn title_bar(
    pal: Pal,
    jobs: &[&Job],
    targets: &[Target],
    page: usize,
    scroll: usize,
    new_below: usize,
    cols: usize,
    rates: &HashMap<String, f64>,
) -> String {
    let nj = jobs.len();
    let scroll_note = if scroll > 0 {
        let below = if new_below > 0 {
            format!(" · {new_below} new below")
        } else {
            String::new()
        };
        format!("  ↑{scroll}{below} — G to tail")
    } else {
        String::new()
    };

    let (plain, color) = if page >= nj {
        (
            format!(
                " ◆ all logs — {} job{} · {} file{}{}",
                nj,
                if nj == 1 { "" } else { "s" },
                targets.len(),
                if targets.len() == 1 { "" } else { "s" },
                scroll_note
            ),
            "7;1".to_string(),
        )
    } else {
        let j = jobs[page];
        let gpus = if j.ngpus > 0 {
            format!(" · {} GPU", j.ngpus)
        } else {
            String::new()
        };
        let wall = match (j.walltime_frac(), j.walltime_used, j.walltime_req) {
            (Some(f), Some(u), Some(r)) => {
                format!(" · wall {:.0}% ({}/{})", f * 100.0, fmt::hm(u), fmt::hm(r))
            }
            _ => String::new(),
        };
        let pts = match (j.points_spent(rates), j.points_reserved(rates)) {
            (Some(sp), Some(rv)) => format!(" · {}/{} pt", fmt::points(sp), fmt::points(rv)),
            _ => String::new(),
        };
        let node = if j.nodes.is_empty() {
            String::new()
        } else if j.nodes.len() == 1 {
            format!(" · {}", j.nodes[0])
        } else {
            format!(" · {}+{}", j.nodes[0], j.nodes.len() - 1)
        };
        (
            format!(
                " ● {} {} — {} · {}{}{}{}{}{}",
                j.short_id,
                j.name,
                j.state.label(),
                j.resource_label(),
                gpus,
                wall,
                pts,
                node,
                scroll_note
            ),
            format!("7;1;{}", JOB_COLORS[page % JOB_COLORS.len()]),
        )
    };
    pal.c(&color, &pad_w(&plain, cols))
}

/// Render one buffered entry into at most `cols` columns.
fn render_entry(
    pal: Pal,
    targets: &[Target],
    entry: &Entry,
    all_page: bool,
    label_w: usize,
    job_multi: &[bool],
    cols: usize,
    grep: Option<(&str, bool)>,
) -> String {
    let (ti, line, sys) = entry;
    let t = &targets[*ti];
    let (prefix, avail) = if all_page {
        (
            format!(
                "{} ",
                pal.c(t.color, &format!("[{}]", fmt::pad(&t.label, label_w)))
            ),
            cols.saturating_sub(label_w + 3).max(10),
        )
    } else if job_multi[t.jidx] && t.label.ends_with(".e") {
        (pal.c("31", "▎"), cols.saturating_sub(1).max(10))
    } else {
        (String::new(), cols)
    };
    let body = truncate_w(line, avail);
    let body = if *sys {
        pal.dim(&body)
    } else if let Some((p, ic)) = grep {
        highlight(pal, &body, p, ic)
    } else {
        body
    };
    format!("{prefix}{body}")
}

/// One full frame: title bar, content window, tab bar.
#[allow(clippy::too_many_arguments)]
fn paint_frame(
    pal: Pal,
    jobs: &[&Job],
    targets: &[Target],
    bufs: &[VecDeque<Entry>],
    page: usize,
    scroll: usize,
    new_below: usize,
    activity: &[bool],
    rows: usize,
    cols: usize,
    grep: Option<(&str, bool)>,
    rates: &HashMap<String, f64>,
) -> String {
    let nj = jobs.len();
    let content_h = rows.saturating_sub(2).max(1);
    let label_w = targets
        .iter()
        .map(|t| t.label.chars().count())
        .max()
        .unwrap_or(6);
    let mut multi = vec![0usize; nj];
    for t in targets {
        multi[t.jidx] += 1;
    }
    let job_multi: Vec<bool> = multi.iter().map(|c| *c > 1).collect();

    let buf = &bufs[page.min(nj)];
    let total = buf.len();
    let max_scroll = total.saturating_sub(content_h);
    let scroll = scroll.min(max_scroll);
    let end = total - scroll;
    let start = end.saturating_sub(content_h);

    let mut out = String::from("\x1b[H");
    out.push_str(&title_bar(
        pal, jobs, targets, page, scroll, new_below, cols, rates,
    ));
    out.push_str("\r\n");
    for entry in buf.iter().skip(start).take(end - start) {
        out.push_str("\x1b[2K");
        out.push_str(&render_entry(
            pal,
            targets,
            entry,
            page >= nj,
            label_w,
            &job_multi,
            cols,
            grep,
        ));
        out.push_str("\r\n");
    }
    for _ in (end - start)..content_h {
        out.push_str("\x1b[2K\r\n");
    }
    out.push_str(&format!("\x1b[{rows};1H\x1b[2K"));
    let solo = if page < nj { Some(page) } else { None };
    out.push_str(&tab_bar(pal, jobs, solo, activity, cols));
    out
}

const BUF_CAP_JOB: usize = 4000;
const BUF_CAP_ALL: usize = 8000;

fn push_capped(buf: &mut VecDeque<Entry>, e: Entry, cap: usize) {
    buf.push_back(e);
    if buf.len() > cap {
        buf.pop_front();
    }
}

/// The paged live view: alt screen, one page per job plus an "all" page.
fn follow_tui(
    jobs: &[&Job],
    targets: &[Target],
    o: &Opts,
    pal: Pal,
    initial_solo: Option<usize>,
    keys: &Keys,
) {
    let nj = jobs.len();
    let all_page = nj;
    let mut page = initial_solo.unwrap_or(all_page);
    let mut scroll = 0usize;
    let mut new_below = 0usize;
    let mut activity = vec![false; nj];
    let mut bufs: Vec<VecDeque<Entry>> = (0..=nj).map(|_| VecDeque::new()).collect();
    let mut streams = make_streams(targets);
    let grep = o.grep.as_deref().map(|p| (p, o.icase));
    let rates = pbs::point_rates();

    let (mut rows, mut cols) = term_size(o.width);
    let mut last_size = Instant::now();
    // Seed enough backlog to fill a page unless the user chose a depth.
    let tail_n = if o.tail_set {
        o.tail
    } else {
        (rows.saturating_sub(2)).max(o.tail) * 2
    };

    let _screen = ScreenGuard::new();
    let mut dirty = true;

    'outer: loop {
        let content_h = rows.saturating_sub(2).max(1);
        match keys.rx.recv_timeout(Duration::from_millis(400)) {
            Ok(first) => {
                let mut pressed = vec![first];
                while let Ok(b) = keys.rx.try_recv() {
                    pressed.push(b);
                }
                for k in pressed {
                    let max_scroll = bufs[page].len().saturating_sub(content_h);
                    match k {
                        b'q' | 3 | 4 | 27 => break 'outer, // q, ^C, ^D, Esc
                        b'a' | b'0' => {
                            page = all_page;
                            scroll = 0;
                            new_below = 0;
                            dirty = true;
                        }
                        b'1'..=b'9' => {
                            let j = (k - b'1') as usize;
                            if j < nj {
                                page = j;
                                scroll = 0;
                                new_below = 0;
                                dirty = true;
                            }
                        }
                        b'j' | b'n' => {
                            page = (page + 1) % (nj + 1);
                            scroll = 0;
                            new_below = 0;
                            dirty = true;
                        }
                        b'k' | b'p' => {
                            page = (page + nj) % (nj + 1);
                            scroll = 0;
                            new_below = 0;
                            dirty = true;
                        }
                        b'u' => {
                            scroll = (scroll + content_h / 2).min(max_scroll);
                            dirty = true;
                        }
                        b'd' => {
                            scroll = scroll.saturating_sub(content_h / 2);
                            if scroll == 0 {
                                new_below = 0;
                            }
                            dirty = true;
                        }
                        b'g' => {
                            scroll = max_scroll;
                            dirty = true;
                        }
                        b'G' => {
                            scroll = 0;
                            new_below = 0;
                            dirty = true;
                        }
                        _ => {}
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                std::thread::sleep(Duration::from_millis(400));
            }
        }

        for i in 0..streams.len() {
            let jidx = streams[i].t.jidx;
            for (line, sys) in poll_stream(&mut streams[i], tail_n) {
                if !sys {
                    if let Some((p, ic)) = grep {
                        if find_ci(line.as_bytes(), p.as_bytes(), ic).is_none() {
                            continue;
                        }
                    }
                }
                push_capped(&mut bufs[jidx], (i, line.clone(), sys), BUF_CAP_JOB);
                push_capped(&mut bufs[all_page], (i, line, sys), BUF_CAP_ALL);
                let visible = page == all_page || page == jidx;
                if visible {
                    if scroll == 0 {
                        dirty = true;
                    } else {
                        new_below += 1;
                        dirty = true; // title shows the growing count
                    }
                } else if !sys && !activity[jidx] {
                    activity[jidx] = true;
                    dirty = true;
                }
            }
        }

        if last_size.elapsed() >= Duration::from_secs(5) {
            let ns = term_size(o.width);
            if ns != (rows, cols) {
                (rows, cols) = ns;
                dirty = true;
            }
            last_size = Instant::now();
        }

        if dirty {
            if page < nj {
                activity[page] = false;
            } else {
                activity.iter_mut().for_each(|a| *a = false);
            }
            put(&paint_frame(
                pal, jobs, targets, &bufs, page, scroll, new_below, &activity, rows, cols, grep,
                &rates,
            ));
            dirty = false;
        }
    }
    // _screen drop restores the previous screen; Keys' RawGuard restores stty.
}

/// Piped follow: a plain multiplexed stream, no screen control at all.
fn follow_pipe(targets: &[Target], o: &Opts, pal: Pal, keys: &Keys) {
    let label_w = targets
        .iter()
        .map(|t| t.label.chars().count())
        .max()
        .unwrap_or(6);
    let prefix_on = targets.len() > 1;
    let grep = o.grep.as_deref();
    let mut streams = make_streams(targets);

    put(&format!(
        " {}\n",
        pal.dim(&format!(
            "following {} log{} · Ctrl-C to stop",
            targets.len(),
            if targets.len() == 1 { "" } else { "s" }
        ))
    ));

    loop {
        // The channel never disconnects (Keys holds a sender), so this is a
        // plain 400ms tick.
        match keys.rx.recv_timeout(Duration::from_millis(400)) {
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                std::thread::sleep(Duration::from_millis(400));
            }
        }
        let mut batch = String::new();
        for s in streams.iter_mut() {
            let t = s.t;
            for (line, sys) in poll_stream(s, o.tail) {
                if !sys {
                    if let Some(p) = grep {
                        if find_ci(line.as_bytes(), p.as_bytes(), o.icase).is_none() {
                            continue;
                        }
                    }
                }
                let body = if sys {
                    pal.dim(&line)
                } else if let Some(p) = grep {
                    highlight(pal, &line, p, o.icase)
                } else {
                    line
                };
                if prefix_on {
                    batch.push_str(&format!(
                        "{} {}\n",
                        pal.c(t.color, &format!("[{}]", fmt::pad(&t.label, label_w))),
                        body
                    ));
                } else {
                    batch.push_str(&format!("{body}\n"));
                }
            }
        }
        if !batch.is_empty() {
            put(&batch);
        }
    }
}

fn follow(
    jobs: &[&Job],
    targets: &[Target],
    o: &Opts,
    pal: Pal,
    initial_solo: Option<usize>,
    keys: &Keys,
) {
    if targets.is_empty() {
        eprintln!("qlog: no logs to follow (no jobs, or all logs at /dev/null)");
        return;
    }
    if keys.interactive {
        follow_tui(jobs, targets, o, pal, initial_solo, keys);
    } else {
        follow_pipe(targets, o, pal, keys);
    }
}

// ---------------------------------------------------------------------------

fn main() -> ExitCode {
    let o = match parse_args() {
        Ok(Some(o)) => o,
        Ok(None) => return ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("qlog: {e}");
            eprintln!("try `qlog --help`");
            return ExitCode::from(2);
        }
    };

    let tz = pbs::tz_offset();
    let snap = match pbs::fetch(o.history, &o.ids, tz) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("qlog: {e}");
            return ExitCode::FAILURE;
        }
    };

    let filter_owner = if o.all_users || !o.ids.is_empty() {
        None
    } else {
        Some(o.user.clone().unwrap_or_else(current_user))
    };
    let mut jobs: Vec<&Job> = snap
        .jobs
        .iter()
        .filter(|j| match &filter_owner {
            Some(u) => &j.owner == u,
            None => true,
        })
        .collect();
    jobs.sort_by(|a, b| {
        a.state
            .rank()
            .cmp(&b.state.rank())
            .then_with(|| a.short_id.cmp(&b.short_id))
    });

    let pal = Pal {
        on: o.color.unwrap_or_else(|| {
            std::io::stdout().is_terminal() && std::env::var("NO_COLOR").is_err()
        }),
    };
    let targets = build_targets(&jobs);

    if o.bar_preview {
        // Hidden test hook: render the tab bar in a few states without a TTY.
        if jobs.is_empty() {
            eprintln!("qlog: no jobs");
            return ExitCode::FAILURE;
        }
        let width = o.width.unwrap_or_else(pbs::term_width);
        let mut activity = vec![false; jobs.len()];
        if jobs.len() > 2 {
            activity[2] = true;
        }
        put(&format!(
            "{}\n{}\n{}\n",
            tab_bar(pal, &jobs, None, &activity, width),
            tab_bar(pal, &jobs, Some(0), &activity, width),
            tab_bar(pal, &jobs, Some(jobs.len() - 1), &activity, width),
        ));
        return ExitCode::SUCCESS;
    }
    if o.page_preview {
        // Hidden test hook: seed buffers from the logs' tails and print one
        // frame per page kind, with cursor addressing stripped.
        if targets.is_empty() {
            eprintln!("qlog: no logs");
            return ExitCode::FAILURE;
        }
        let (rows, cols) = (20usize, o.width.unwrap_or(100));
        let nj = jobs.len();
        let mut bufs: Vec<VecDeque<Entry>> = (0..=nj).map(|_| VecDeque::new()).collect();
        let mut streams = make_streams(&targets);
        for i in 0..streams.len() {
            let jidx = streams[i].t.jidx;
            for (line, sys) in poll_stream(&mut streams[i], rows) {
                push_capped(&mut bufs[jidx], (i, line.clone(), sys), BUF_CAP_JOB);
                push_capped(&mut bufs[nj], (i, line, sys), BUF_CAP_ALL);
            }
        }
        let activity = vec![false; nj];
        let rates = pbs::point_rates();
        for page in [0usize, nj] {
            let frame = paint_frame(
                pal, &jobs, &targets, &bufs, page, 0, 0, &activity, rows, cols, None, &rates,
            );
            let clean = frame
                .replace("\x1b[H", "")
                .replace("\x1b[2K", "")
                .replace(&format!("\x1b[{rows};1H"), "")
                .replace("\r\n", "\n");
            put(&format!("{clean}\n{}\n", "═".repeat(cols.min(60))));
        }
        return ExitCode::SUCCESS;
    }
    let tty = std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
    if o.follow {
        let keys = Keys::new(tty);
        follow(&jobs, &targets, &o, pal, None, &keys);
        return ExitCode::SUCCESS;
    }
    if let Some(p) = o.grep.clone() {
        return search_mode(&targets, &p, &o, pal);
    }
    if targets.is_empty() {
        put(&format!("{}\n", pal.dim(" no jobs with log files")));
        return ExitCode::SUCCESS;
    }
    let width = o.width.unwrap_or_else(pbs::term_width);
    if tty {
        let keys = Keys::new(true);
        match picker(&targets, jobs.len(), pal, width, &keys) {
            Some(usize::MAX) => follow(&jobs, &targets, &o, pal, None, &keys),
            Some(j) => follow(&jobs, &targets, &o, pal, Some(j), &keys),
            None => {}
        }
    } else {
        list(&targets, pal, width);
    }
    ExitCode::SUCCESS
}
