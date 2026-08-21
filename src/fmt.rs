//! Parsing and formatting helpers for the shapes PBS emits:
//! `HH:MM:SS` durations (hours may exceed 24), `123456kb` / `1000gb` sizes,
//! and `Thu Aug 20 10:57:05 2026` timestamps.

/// `"397:58:40"` -> seconds. Also accepts `MM:SS` and a bare second count.
pub fn parse_hms(s: &str) -> Option<u64> {
    let mut total: u64 = 0;
    let mut parts = 0;
    for field in s.trim().split(':') {
        let v: u64 = field.trim().parse().ok()?;
        total = total * 60 + v;
        parts += 1;
    }
    if parts == 0 || parts > 3 {
        return None;
    }
    Some(total)
}

/// Seconds -> `H:MM`, keeping hours uncapped (`397:58`).
pub fn hm(secs: u64) -> String {
    format!("{}:{:02}", secs / 3600, (secs % 3600) / 60)
}

/// Seconds -> `H:MM:SS`.
pub fn hms(secs: u64) -> String {
    format!(
        "{}:{:02}:{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

/// `"189946940kb"`, `"1000gb"`, `"524288000kb"` -> KiB.
pub fn parse_size_kb(s: &str) -> Option<u64> {
    let s = s.trim().to_ascii_lowercase();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().ok()?;
    let kb = match unit.trim() {
        "" | "b" => n / 1024,
        "kb" | "k" => n,
        "mb" | "m" => n * 1024,
        "gb" | "g" => n * 1024 * 1024,
        "tb" | "t" => n * 1024 * 1024 * 1024,
        _ => return None,
    };
    Some(kb)
}

/// KiB -> a short human size (`181 GiB`).
pub fn size(kb: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    let mut v = kb as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if v >= 100.0 || u == 0 {
        format!("{:.0} {}", v, UNITS[u])
    } else {
        format!("{:.1} {}", v, UNITS[u])
    }
}

/// Thousands separators for point counts.
pub fn commas(v: f64) -> String {
    let neg = v < 0.0;
    let whole = v.abs().round() as u64;
    let digits = whole.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if neg {
        format!("-{out}")
    } else {
        out
    }
}

/// Point counts: keep a decimal below 10 pt (a short rt_QC job costs 0.5 pt),
/// thousands separators above it.
pub fn points(v: f64) -> String {
    if v > 0.0 && v < 10.0 {
        let s = format!("{:.1}", v);
        s.trim_end_matches(".0").to_string()
    } else {
        commas(v)
    }
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Days since the Unix epoch for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// `"Thu Aug 20 10:57:05 2026"` -> Unix epoch, given the local UTC offset.
pub fn parse_pbs_date(s: &str, tz_offset_secs: i64) -> Option<i64> {
    let f: Vec<&str> = s.split_whitespace().collect();
    if f.len() < 5 {
        return None;
    }
    let month = MONTHS.iter().position(|m| *m == f[1])? as i64 + 1;
    let day: i64 = f[2].parse().ok()?;
    let year: i64 = f[4].parse().ok()?;
    let clock: Vec<i64> = f[3].split(':').filter_map(|p| p.parse().ok()).collect();
    if clock.len() != 3 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days * 86400 + clock[0] * 3600 + clock[1] * 60 + clock[2] - tz_offset_secs)
}

/// Epoch -> `20 Aug 10:57` in local time.
pub fn short_datetime(epoch: i64, tz_offset_secs: i64) -> String {
    let local = epoch + tz_offset_secs;
    let days = local.div_euclid(86400);
    let secs = local.rem_euclid(86400);
    // Inverse of days_from_civil.
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!(
        "{} {} {:02}:{:02}",
        d,
        MONTHS[(m - 1) as usize],
        secs / 3600,
        (secs % 3600) / 60
    )
}

/// Coarse distance in time: `16d 11h`, `3h 20m`, `45m`.
pub fn until(secs: i64) -> String {
    let s = secs.max(0);
    let (d, h, m) = (s / 86400, (s % 86400) / 3600, (s % 3600) / 60);
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else {
        format!("{m}m")
    }
}

const PARTIALS: [char; 8] = [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉'];

/// A proportional bar `width` cells wide, using eighth-blocks for the remainder.
pub fn bar(frac: f64, width: usize, ascii: bool) -> String {
    let frac = frac.clamp(0.0, 1.0);
    if ascii {
        let filled = ((frac * width as f64).round() as usize).min(width);
        return format!("{}{}", "#".repeat(filled), "-".repeat(width - filled));
    }
    let eighths = (frac * (width * 8) as f64).round() as usize;
    let full = (eighths / 8).min(width);
    let rem = eighths % 8;
    let mut s: String = "█".repeat(full);
    let mut used = full;
    if rem > 0 && full < width {
        s.push(PARTIALS[rem]);
        used += 1;
    }
    s.push_str(&"░".repeat(width.saturating_sub(used)));
    s
}

/// Truncate to `max` display cells, marking elision.
pub fn ellipsize(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        return s.to_string();
    }
    if max <= 1 {
        return "…".to_string();
    }
    let mut out: String = s.chars().take(max - 1).collect();
    out.push('…');
    out
}

/// Pad to `width` display cells (no-op if already wider).
pub fn pad(s: &str, width: usize) -> String {
    let count = s.chars().count();
    if count >= width {
        s.to_string()
    } else {
        format!("{}{}", s, " ".repeat(width - count))
    }
}

/// Right-align within `width` cells.
pub fn rpad(s: &str, width: usize) -> String {
    let count = s.chars().count();
    if count >= width {
        s.to_string()
    } else {
        format!("{}{}", " ".repeat(width - count), s)
    }
}
