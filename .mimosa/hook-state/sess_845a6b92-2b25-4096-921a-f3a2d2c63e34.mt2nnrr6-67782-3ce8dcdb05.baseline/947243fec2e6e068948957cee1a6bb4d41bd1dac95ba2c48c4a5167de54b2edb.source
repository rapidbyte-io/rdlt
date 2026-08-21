//! The ONE self-reported summary-line convention every competitor arm
//! answers to: a run may print noise, then one JSON object line LAST on
//! stdout whose `seconds` is the in-process self-timed measurement, with an
//! optional `peak_rss_kb` (getrusage ru_maxrss) and an optional `extra{}`
//! object carried verbatim into the record.

/// The summary line's fields the harness reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Summary {
    pub(crate) seconds: f64,
    pub(crate) peak_rss_kb: Option<u64>,
    pub(crate) extra: Option<serde_json::Value>,
}

impl Summary {
    /// `peak_rss_kb` in bytes.
    pub(crate) fn peak_rss_bytes(&self) -> Option<u64> {
        self.peak_rss_kb.map(|kb| kb * 1024)
    }
}

/// The last stdout line that parses as a JSON object carrying a numeric
/// `seconds`; `None` when no line does.
pub(crate) fn parse(stdout: &str) -> Option<Summary> {
    stdout.lines().rev().find_map(|line| {
        let object: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
        let seconds = object.get("seconds")?.as_f64()?;
        Some(Summary {
            seconds,
            peak_rss_kb: object.get("peak_rss_kb").and_then(|v| v.as_u64()),
            extra: object.get("extra").cloned(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_json_line_carrying_seconds_wins() {
        let stdout =
            "noise\n{\"rows\": 10, \"seconds\": 1.5}\n{\"seconds\": 2.5, \"rows_per_s\": 4}\n";
        assert_eq!(parse(stdout).unwrap().seconds, 2.5);
        assert!(parse("no json here").is_none());
        assert!(parse("{\"rows\": 10}").is_none());
    }

    #[test]
    fn rss_and_extra_read_off_the_same_line() {
        let stdout = "{\"rows\": 10, \"seconds\": 1.5, \"peak_rss_kb\": 2048, \"extra\": {\"sync_s\": 40.0}}\n";
        let summary = parse(stdout).unwrap();
        assert_eq!(summary.peak_rss_bytes(), Some(2048 * 1024));
        assert_eq!(summary.extra.unwrap()["sync_s"], 40.0);
        let bare = parse("{\"seconds\": 1.0}").unwrap();
        assert_eq!(bare.peak_rss_bytes(), None);
        assert!(bare.extra.is_none());
    }
}
