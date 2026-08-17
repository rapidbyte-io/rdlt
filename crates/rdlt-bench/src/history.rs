//! The append-only history feed (`benches/history.jsonl`): one recorded data
//! point per cell×variant per recorded invocation, and the Trends table that
//! renders the latest two per key. The file is the Trends section's whole
//! memory; the line shape is FROZEN.

use std::path::Path;

use crate::artifact::{Artifact, CompetitorSide};
use crate::cell;
use crate::error::{Error, Result};

/// One recorded data point per cell×variant. `ts` is taken from the artifact's
/// own timestamp — the feed introduces no new clock source.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Line {
    pub(crate) ts: String,
    pub(crate) cell: String,
    pub(crate) variant: String,
    pub(crate) median_ms: f64,
    pub(crate) rows: Option<u64>,
    /// Recorded on a machine that failed the quiet guard, so a forced median
    /// in the trends table is never mistaken for a recorded-session one.
    /// Optional so existing history files still read.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) forced: bool,
}

/// Append one line per cell×variant for this recorded invocation (rdlt plus
/// every competitor that produced a median). Every arm of a cell delivers
/// the SAME declared stream set — the delivered-vs-declared check enforces
/// it — so the cell's declared total is each competitor's row count too,
/// which is what lets the trends guard fire on a competitor row.
pub(crate) fn append(path: &Path, artifact: &Artifact) -> Result<()> {
    let mut lines = vec![Line {
        ts: artifact.recorded_at.clone(),
        cell: artifact.cell_id.clone(),
        variant: "rdlt".into(),
        median_ms: artifact.rdlt.median_ms,
        rows: artifact.rdlt.rows,
        forced: artifact.forced,
    }];
    let declared: Option<u64> = artifact.verify.as_ref().map(|v| v.values().copied().sum());
    for (variant, side) in &artifact.competitors {
        if let CompetitorSide::Ok { median_ms, .. } = side {
            lines.push(Line {
                ts: artifact.recorded_at.clone(),
                cell: artifact.cell_id.clone(),
                variant: variant.clone(),
                median_ms: *median_ms,
                rows: declared,
                forced: artifact.forced,
            });
        }
    }
    let mut body = String::new();
    for line in &lines {
        let json = serde_json::to_string(line)
            .map_err(|e| Error(format!("serializing history line: {e}")))?;
        body.push_str(&json);
        body.push('\n');
    }
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| Error(format!("opening {}: {e}", path.display())))?;
    file.write_all(body.as_bytes())
        .map_err(|e| Error(format!("appending {}: {e}", path.display())))?;
    Ok(())
}

/// Every line of the feed; a feed that does not exist yet reads as empty.
pub(crate) fn read(path: &Path) -> Result<Vec<Line>> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    let mut lines = Vec::new();
    for (n, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: Line = serde_json::from_str(line)
            .map_err(|e| Error(format!("{}:{}: {e}", path.display(), n + 1)))?;
        lines.push(parsed);
    }
    Ok(lines)
}

/// Trends: the latest two recorded medians per cell×variant, and the delta
/// between them — the "is it drifting?" view. Selftest lines are filtered by
/// the same rule the matrix uses: harness machinery is never a product row.
pub(crate) fn trends(history: &[Line]) -> String {
    use std::collections::BTreeMap;
    // Preserve append order (chronological) per key; keep the last two.
    let mut by_key: BTreeMap<(String, String), Vec<&Line>> = BTreeMap::new();
    for line in history.iter().filter(|l| !cell::is_selftest(&l.cell)) {
        by_key
            .entry((line.cell.clone(), line.variant.clone()))
            .or_default()
            .push(line);
    }
    let mut out = String::new();
    out.push_str("| Cell | Variant | Latest | Previous | Δ |\n");
    out.push_str("|---|---|---|---|---|\n");
    for ((cell, variant), points) in &by_key {
        let latest = points.last().expect("non-empty by construction");
        let prev = points.iter().rev().nth(1);
        // A percentage is only meaningful between runs that moved the same
        // volume. When the row counts differ the two points measured different
        // work, so render the counts instead — a cell whose scope was corrected
        // would otherwise publish the correction as a speedup.
        let delta = prev.map_or_else(
            || "—".to_owned(),
            |p| match (latest.rows, p.rows) {
                (Some(now), Some(before)) if now != before => {
                    format!("rows {before} → {now}")
                }
                _ => {
                    let pct = (latest.median_ms - p.median_ms) / p.median_ms * 100.0;
                    format!("{pct:+.1}%")
                }
            },
        );
        out.push_str(&format!(
            "| {cell} | {variant} | {} | {} | {delta} |\n",
            crate::report::fmt_ms(latest.median_ms),
            prev.map_or_else(|| "—".to_owned(), |p| crate::report::fmt_ms(p.median_ms)),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_round_trips_and_trends_show_the_delta() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let mut a1 = crate::artifact::tests::minimal("pg-to-pg-1m");
        a1.recorded_at = "2026-07-24".into();
        a1.rdlt.median_ms = 1000.0;
        append(&path, &a1).unwrap();
        let mut a2 = crate::artifact::tests::minimal("pg-to-pg-1m");
        a2.recorded_at = "2026-07-25".into();
        a2.rdlt.median_ms = 1100.0;
        append(&path, &a2).unwrap();

        let history = read(&path).unwrap();
        assert_eq!(history.len(), 2);
        let table = trends(&history);
        assert!(table.contains("pg-to-pg-1m"), "{table}");
        assert!(table.contains("rdlt"), "{table}");
        assert!(table.contains("+10.0%"), "{table}");
    }

    /// The line shape on disk: the frozen keys in order, `forced` present
    /// only when true.
    #[test]
    fn a_line_is_written_with_the_frozen_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let mut artifact = crate::artifact::tests::minimal("pg-to-pg-1m");
        artifact.recorded_at = "2026-08-13".into();
        artifact.rdlt.median_ms = 1000.5;
        append(&path, &artifact).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"ts\":\"2026-08-13\",\"cell\":\"pg-to-pg-1m\",\"variant\":\"rdlt\",\"median_ms\":1000.5,\"rows\":100}\n"
        );
    }

    /// Selftest history lines never reach the Trends table — the matrix
    /// filters harness machinery and Trends filters by the SAME rule; a
    /// feed carrying a selftest line renders only the product rows.
    #[test]
    fn selftest_history_lines_never_reach_the_trends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let mut product = crate::artifact::tests::minimal("pg-to-pg-1m");
        product.rdlt.median_ms = 1000.0;
        append(&path, &product).unwrap();
        let mut machinery = crate::artifact::tests::minimal("selftest-protocol");
        machinery.rdlt.median_ms = 22.0;
        append(&path, &machinery).unwrap();

        let table = trends(&read(&path).unwrap());
        assert!(table.contains("pg-to-pg-1m"), "{table}");
        assert!(!table.contains("selftest"), "{table}");
    }

    /// A cell whose scope is corrected moves fewer rows than it did before, so
    /// its wall time drops for a reason that is not a speedup. Publishing that
    /// drop as a percentage would advertise the correction as an improvement.
    #[test]
    fn a_delta_across_different_row_counts_shows_the_rows_not_a_percentage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let mut before = crate::artifact::tests::minimal("pg-to-pg-dedup-1m");
        before.recorded_at = "2026-07-24".into();
        before.rdlt.median_ms = 14_784.0;
        before.rdlt.rows = Some(3_000_000);
        append(&path, &before).unwrap();
        let mut after = crate::artifact::tests::minimal("pg-to-pg-dedup-1m");
        after.recorded_at = "2026-07-25".into();
        after.rdlt.median_ms = 5_028.0;
        after.rdlt.rows = Some(1_000_000);
        append(&path, &after).unwrap();

        let table = trends(&read(&path).unwrap());
        assert!(table.contains("rows 3000000 → 1000000"), "{table}");
        assert!(
            !table.contains('%'),
            "a scope change is not a speedup: {table}"
        );
    }
}
