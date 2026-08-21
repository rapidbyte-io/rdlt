//! The append-only history feed (`benches/history.jsonl`): one recorded data
//! point per cell×variant per recorded invocation. The file is the Trends
//! table's whole memory (the table itself is rendered by `report`); the line
//! shape is FROZEN.

use std::path::Path;

use crate::artifact::{Artifact, CompetitorSide};
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
