//! qrich — a richer `qstat` for ABCI-Q.
//!
//! Reads `qstat -f -F json` and renders what the plain table leaves out: GPU
//! count, a walltime progress bar, point burn against the group's balance, the
//! node the job landed on, and the log path to tail.

mod fmt;
mod pbs;
mod render;

use std::io::{IsTerminal, Write};
use std::process::ExitCode;

const USAGE: &str = "\
qrich — rich, visualized qstat for ABCI-Q

USAGE:
    qrich [OPTIONS] [JOBID...]

OPTIONS:
    -l, --long          one detailed card per job (usage, points, log path)
    -a, --all           every user's jobs, not just your own
    -u, --user USER     jobs owned by USER
    -x, --history       include recently finished jobs
    -w, --watch [SECS]  refresh continuously (default 10s, Ctrl-C to stop)
        --no-points     skip the show_point group balance lookup
        --color WHEN    always | never | auto (default auto)
        --ascii         plain ASCII bars, for terminals without box drawing
        --width COLS    override the detected terminal width
    -h, --help          this message
    -V, --version       print the version

NOTES:
    Point figures use the 特別支援課題利用 (Special Support) rate card:
    rt_QF 5, rt_QG 2, rt_QC 1, rt_QD/QS/QA 10 points per node-hour. Other usage
    categories differ — override with, e.g.:
        export ABCIQ_POINT_RATES=\"rt_QF=7,rt_QG=3\"
";

struct Opts {
    long: bool,
    all_users: bool,
    user: Option<String>,
    history: bool,
    watch: Option<u64>,
    points: bool,
    color: Option<bool>,
    ascii: bool,
    width: Option<usize>,
    ids: Vec<String>,
}

fn parse_args() -> Result<Option<Opts>, String> {
    let mut o = Opts {
        long: false,
        all_users: false,
        user: None,
        history: false,
        watch: None,
        points: true,
        color: None,
        ascii: false,
        width: None,
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
                println!("qrich {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }
            "-l" | "--long" => o.long = true,
            "-a" | "--all" => o.all_users = true,
            "-x" | "--history" => o.history = true,
            "--no-points" => o.points = false,
            "--ascii" => o.ascii = true,
            "-u" | "--user" => {
                i += 1;
                o.user = Some(args.get(i).ok_or("--user needs a username")?.clone());
            }
            "-w" | "--watch" => {
                // The interval is optional: `-w` alone means every 10s.
                let next = args.get(i + 1).and_then(|v| v.parse::<u64>().ok());
                match next {
                    Some(secs) => {
                        o.watch = Some(secs.max(1));
                        i += 1;
                    }
                    None => o.watch = Some(10),
                }
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

fn draw(o: &Opts, tz: i64, rates: &std::collections::HashMap<String, f64>) -> Result<String, String> {
    let snap = pbs::fetch(o.history, &o.ids, tz)?;

    // An explicit job id means "show me this job", whoever owns it.
    let filter_owner = if o.all_users || !o.ids.is_empty() {
        None
    } else {
        Some(o.user.clone().unwrap_or_else(current_user))
    };

    let mut jobs: Vec<&pbs::Job> = snap
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
            .then_with(|| {
                // Within running jobs, closest to its walltime limit first.
                b.walltime_frac()
                    .unwrap_or(-1.0)
                    .partial_cmp(&a.walltime_frac().unwrap_or(-1.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.short_id.cmp(&b.short_id))
    });

    let now = if snap.timestamp > 0 {
        snap.timestamp
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    };

    let color = o
        .color
        .unwrap_or_else(|| std::io::stdout().is_terminal() && std::env::var("NO_COLOR").is_err());

    let who = match (&filter_owner, o.ids.is_empty()) {
        (Some(u), _) => u.clone(),
        (None, false) => "selected jobs".to_string(),
        (None, true) => "all users".to_string(),
    };

    let view = render::View {
        color,
        ascii: o.ascii,
        width: o.width.unwrap_or_else(pbs::term_width),
        long: o.long,
        show_points: o.points,
        rates,
        tz,
        now,
    };

    Ok(render::render(&snap, &jobs, &view, &who))
}

fn main() -> ExitCode {
    let opts = match parse_args() {
        Ok(Some(o)) => o,
        Ok(None) => return ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("qrich: {e}");
            eprintln!("try `qrich --help`");
            return ExitCode::from(2);
        }
    };

    let tz = pbs::tz_offset();
    let rates = pbs::point_rates();

    match opts.watch {
        None => match draw(&opts, tz, &rates) {
            Ok(s) => {
                print!("{s}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("qrich: {e}");
                ExitCode::FAILURE
            }
        },
        Some(secs) => {
            loop {
                match draw(&opts, tz, &rates) {
                    // Home, then clear-to-end: repaints without the flicker of
                    // a full clear, and leaves scrollback intact.
                    Ok(s) => print!("\x1b[H\x1b[J{s}"),
                    Err(e) => print!("\x1b[H\x1b[Jqrich: {e}\n"),
                }
                let _ = std::io::stdout().flush();
                std::thread::sleep(std::time::Duration::from_secs(secs));
            }
        }
    }
}
