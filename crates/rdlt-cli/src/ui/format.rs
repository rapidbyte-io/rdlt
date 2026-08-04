//! Human-scale number rendering, shared by the live display and the
//! summary so the two can never disagree about what "1.2M" means.

/// Plain digits up to 9,999; `10.0k`, `1.23M`, `4.56B` beyond.
pub fn count(n: u64) -> String {
    match n {
        0..=9_999 => n.to_string(),
        10_000..=999_999 => format!("{:.1}k", n as f64 / 1_000.0),
        1_000_000..=999_999_999 => format!("{:.2}M", n as f64 / 1_000_000.0),
        _ => format!("{:.2}B", n as f64 / 1_000_000_000.0),
    }
}

/// Bytes with binary-ish familiarity but decimal units, matching what
/// `ls -l`-adjacent tooling shows: `96.4 MB`.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "kB", "MB", "GB", "TB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1_000.0 && unit < UNITS.len() - 1 {
        value /= 1_000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// A rate, already per-second: `861k/s` — and `0.4/s` below one,
/// because a slow pipeline and a stalled one must not read the same.
pub fn rate(per_sec: f64) -> String {
    if per_sec > 0.0 && per_sec < 1.0 {
        format!("{per_sec:.1}/s")
    } else {
        format!("{}/s", count(per_sec.round() as u64))
    }
}

/// `1 commit`, `2 commits` — the difference between a tool and a
/// prototype is that the tool conjugates.
pub fn commits(n: u64) -> String {
    if n == 1 {
        "1 commit".to_owned()
    } else {
        format!("{n} commits")
    }
}

/// A duration for humans: `1.2s`, `450ms`, `3m12s`.
pub fn duration(d: std::time::Duration) -> String {
    let ms = d.as_millis();
    if ms < 1_000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        format!("{:.1}s", d.as_secs_f64())
    } else {
        let secs = d.as_secs();
        format!("{}m{:02}s", secs / 60, secs % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The boundaries, pinned — renderers key layouts on these widths.
    #[test]
    fn scales_switch_where_documented() {
        assert_eq!(count(9_999), "9999");
        assert_eq!(count(10_000), "10.0k");
        assert_eq!(count(1_351), "1351");
        assert_eq!(count(3_210_000), "3.21M");
        assert_eq!(bytes(999), "999 B");
        assert_eq!(bytes(96_400_000), "96.4 MB");
        assert_eq!(rate(861_000.4), "861.0k/s");
        assert_eq!(rate(0.4), "0.4/s");
        assert_eq!(rate(0.0), "0/s");
        assert_eq!(commits(1), "1 commit");
        assert_eq!(commits(3), "3 commits");
        assert_eq!(duration(std::time::Duration::from_millis(450)), "450ms");
        assert_eq!(duration(std::time::Duration::from_secs(192)), "3m12s");
    }
}
