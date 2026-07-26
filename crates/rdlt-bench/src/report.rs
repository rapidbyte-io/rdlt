//! RESULTS.md generation: the matrix and trends tables regenerate from
//! artifacts (and the append-only history feed) between explicit markers; the
//! hand-written narrative outside the markers is preserved byte-for-byte. No
//! markers → refusal, never guessing.

use std::path::Path;

use crate::artifact::{Artifact, CompetitorSide};
use crate::cells::{Bar, BarKind, Cell};
use crate::{BenchError, Result};

/// Cells whose artifacts are harness machinery, never a product row.
fn is_selftest(id: &str) -> bool {
    id.starts_with("selftest")
}

pub(crate) fn begin_marker(section: &str) -> String {
    format!("<!-- rdlt-bench:BEGIN {section} -->")
}
pub(crate) fn end_marker(section: &str) -> String {
    format!("<!-- rdlt-bench:END {section} -->")
}

fn fmt_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2} s", ms / 1000.0)
    } else {
        format!("{ms:.1} ms")
    }
}

fn fmt_opt<T>(v: Option<T>, f: impl Fn(T) -> String) -> String {
    v.map_or_else(|| "—".to_owned(), f)
}

/// Spread across the counted runs, as a percent of the median — the honest
/// jitter band behind a single median number.
fn spread_pct(runs_ms: &[f64], median_ms: f64) -> Option<f64> {
    if runs_ms.len() < 2 || median_ms <= 0.0 {
        return None;
    }
    let min = runs_ms.iter().copied().fold(f64::INFINITY, f64::min);
    let max = runs_ms.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    Some((max - min) / median_ms * 100.0)
}

fn rdlt_cell(artifact: &Artifact) -> String {
    match spread_pct(&artifact.rdlt.runs_ms, artifact.rdlt.median_ms) {
        Some(pct) => format!("{} (±{pct:.0}%)", fmt_ms(artifact.rdlt.median_ms)),
        None => fmt_ms(artifact.rdlt.median_ms),
    }
}

fn bar_target(bar: Option<&Bar>) -> String {
    match bar.map(|b| &b.kind) {
        Some(BarKind::RatioVs { min_ratio, .. }) => format!("≥ {min_ratio}×"),
        Some(BarKind::AbsoluteMs { max_ms }) => format!("≤ {max_ms} ms abs"),
        Some(BarKind::RssRatioVs { max_rss_ratio, .. }) => {
            format!("≤ 1/{:.0}", 1.0 / max_rss_ratio)
        }
        None => "—".into(),
    }
}

/// The bar verdict for a cell, or `—` when no bar governs it (measurement-first:
/// most cells carry no bar).
fn status(artifact: &Artifact, bar: Option<&Bar>) -> String {
    match bar {
        Some(bar) => {
            if crate::gate::evaluate(bar, artifact).passed() {
                "PASS".into()
            } else {
                "FAIL".into()
            }
        }
        None => "—".into(),
    }
}

const HEADERS: [&str; 8] = [
    "Cell",
    "rdlt median",
    "vs baseline",
    "Target",
    "Status",
    "rows/s",
    "MB/s",
    "peak RSS",
];

/// The shared ROW builder — both renderers (markdown for RESULTS.md, aligned
/// text for the run summary) consume these cells, so the two views can never
/// drift. Emphasis (`**`) is markdown-layer decoration, added there only.
fn rows_for(artifacts: &[&Artifact], bars: &[Bar]) -> Vec<[String; 8]> {
    let mut rows = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let bar = bars.iter().find(|b| b.cell == artifact.cell_id);
        // Every recorded competitor's ratio, LEAST flattering first (that one
        // gets the bold headline), Missing entries last — a single "vs
        // baseline" pick would hide a pairing, and the matrix is three-way.
        let mut entries: Vec<(&String, &CompetitorSide)> = artifact.competitors.iter().collect();
        entries.sort_by(|(_, a), (_, b)| {
            let ratio = |side: &CompetitorSide| match side {
                CompetitorSide::Ok {
                    median_ms,
                    ratio_vs_rdlt,
                    ..
                } => ratio_vs_rdlt.unwrap_or(median_ms / artifact.rdlt.median_ms),
                CompetitorSide::Missing { .. } => f64::INFINITY,
            };
            ratio(a).total_cmp(&ratio(b))
        });
        let ratios: Vec<String> = entries
            .into_iter()
            .map(|(id, side)| match side {
                CompetitorSide::Ok {
                    median_ms,
                    ratio_vs_rdlt,
                    ..
                } => format!(
                    "{:.1}× ({id}: {})",
                    ratio_vs_rdlt.unwrap_or(median_ms / artifact.rdlt.median_ms),
                    fmt_ms(*median_ms)
                ),
                CompetitorSide::Missing { reason } => format!("{id}: MISSING ({reason})"),
            })
            .collect();
        rows.push([
            artifact.cell_id.clone(),
            rdlt_cell(artifact),
            if ratios.is_empty() {
                "—".into()
            } else {
                ratios.join("; ")
            },
            bar_target(bar),
            status(artifact, bar),
            fmt_opt(artifact.rdlt.rows_per_s, |v| format!("{v:.0}")),
            fmt_opt(artifact.rdlt.mb_per_s, |v| format!("{v:.1}")),
            fmt_opt(artifact.rdlt.rss.peak_bytes, |v| format!("{} MB", v >> 20)),
        ]);
    }
    rows
}

/// Markdown rendering for the RESULTS.md matrix section. Always emits the
/// header row so the region is a coherent (possibly empty) table.
fn table_for(artifacts: &[&Artifact], bars: &[Bar]) -> String {
    let mut out = String::new();
    out.push_str(&format!("| {} |\n", HEADERS.join(" | ")));
    out.push_str("|---|---|---|---|---|---|---|---|\n");
    for row in rows_for(artifacts, bars) {
        let mut cells = row;
        // Bold the headline ratio, exactly as the hand tables always did.
        if !cells[2].starts_with("MISSING")
            && cells[2] != "—"
            && let Some(space) = cells[2].find(' ')
        {
            cells[2] = format!("**{}** {}", &cells[2][..space], &cells[2][space + 1..]);
        }
        out.push_str(&format!("| {} |\n", cells.join(" | ")));
    }
    out
}

/// Aligned text rendering for the terminal (stdout run summary). Widths are
/// char-counted — the table stays a table even with ×/≥/— in the cells.
fn terminal_table(artifacts: &[&Artifact], bars: &[Bar]) -> String {
    let rows = rows_for(artifacts, bars);
    let mut widths: [usize; 8] = HEADERS.map(str::len);
    for row in &rows {
        for (w, cell) in widths.iter_mut().zip(row.iter()) {
            *w = (*w).max(cell.chars().count());
        }
    }
    let pad = |cell: &str, width: usize, right: bool| {
        let fill = width - cell.chars().count();
        if right {
            format!("{}{}", " ".repeat(fill), cell)
        } else {
            format!("{}{}", cell, " ".repeat(fill))
        }
    };
    // Numeric columns read best right-aligned.
    const RIGHT: [bool; 8] = [false, true, false, false, false, true, true, true];
    let render = |cells: &[String; 8]| -> String {
        let mut line = String::new();
        for i in 0..8 {
            if i > 0 {
                line.push_str("  ");
            }
            line.push_str(&pad(&cells[i], widths[i], RIGHT[i]));
        }
        format!("{}\n", line.trim_end())
    };
    let mut out = render(&HEADERS.map(str::to_owned));
    out.push_str(&format!(
        "{}\n",
        "─".repeat(widths.iter().sum::<usize>() + 2 * 7)
    ));
    for row in &rows {
        out.push_str(&render(row));
    }
    out
}

/// The italic provenance footer: `source` says where the rows came from
/// ("committed artifacts" for RESULTS.md, "this invocation" for the run
/// summary) — the table renderer itself stays shared.
fn provenance(artifacts: &[&Artifact], source: &str) -> String {
    if artifacts.is_empty() {
        return format!("\n_{source} (no recorded sessions yet)._\n");
    }
    let dates: std::collections::BTreeSet<&str> =
        artifacts.iter().map(|a| a.recorded_at.as_str()).collect();
    let pins: std::collections::BTreeSet<&str> = artifacts
        .iter()
        .flat_map(|a| a.fingerprint.competitor_pins.values().map(String::as_str))
        .collect();
    format!(
        "\n_{source} (recorded {}{})._\n",
        dates.into_iter().collect::<Vec<_>>().join(", "),
        if pins.is_empty() {
            String::new()
        } else {
            format!("; {}", pins.into_iter().collect::<Vec<_>>().join(", "))
        }
    )
}

/// The run-summary rendering: the same rows as the RESULTS.md matrix, aligned
/// for a terminal instead of markdown.
pub fn summary_table(artifacts: &[&Artifact], bars: &[Bar]) -> String {
    let footer = provenance(artifacts, "Measured by this invocation");
    format!(
        "{}{}",
        terminal_table(artifacts, bars),
        footer.replace('_', "") // no markdown italics on a terminal
    )
}

// ---------------------------------------------------------------------------
// History feed (Trends)
// ---------------------------------------------------------------------------

/// One recorded data point per cell×variant. `ts` is taken from the artifact's
/// own timestamp — the feed introduces no new clock source.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HistoryLine {
    pub ts: String,
    pub cell: String,
    pub variant: String,
    pub median_ms: f64,
    pub rows: Option<u64>,
    /// Recorded on a machine that failed the quiet guard.
    ///
    /// The artifact carries this flag precisely so a forced number is never
    /// mistaken for evidence; dropping it here let a forced median into the
    /// trends table indistinguishable from a recorded-session one — the same
    /// confusion, one layer over. Optional so existing history files still read.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub forced: bool,
}

/// Append one line per cell×variant for this recorded invocation (rdlt plus
/// every competitor that produced a median). Append-only: the file is the
/// Trends section's whole memory.
pub fn append_history(path: &Path, artifact: &Artifact) -> Result<()> {
    let mut lines = vec![HistoryLine {
        ts: artifact.recorded_at.clone(),
        cell: artifact.cell_id.clone(),
        variant: "rdlt".into(),
        median_ms: artifact.rdlt.median_ms,
        rows: artifact.rdlt.rows,
        forced: artifact.forced,
    }];
    // Every arm of a cell delivers the SAME declared stream set — that is what
    // the delivered-vs-declared check enforces — so the cell's declared total
    // is each competitor's row count too.
    //
    // Recording it is what lets the trends guard fire on a competitor row. It
    // was `None` here, and the guard compares before/now row counts, so a
    // scope correction was rendered as a plain percentage for every arm except
    // rdlt's — precisely the misleading output the guard exists to prevent,
    // just on the other side of the comparison.
    let declared: Option<u64> = artifact.verify.as_ref().map(|v| v.values().copied().sum());
    for (variant, side) in &artifact.competitors {
        if let CompetitorSide::Ok { median_ms, .. } = side {
            lines.push(HistoryLine {
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
            .map_err(|e| BenchError(format!("serializing history line: {e}")))?;
        body.push_str(&json);
        body.push('\n');
    }
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| BenchError(format!("opening {}: {e}", path.display())))?;
    file.write_all(body.as_bytes())
        .map_err(|e| BenchError(format!("appending {}: {e}", path.display())))?;
    Ok(())
}

fn read_history(path: &Path) -> Result<Vec<HistoryLine>> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Ok(Vec::new()); // no feed yet → empty Trends
    };
    let mut lines = Vec::new();
    for (n, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: HistoryLine = serde_json::from_str(line)
            .map_err(|e| BenchError(format!("{}:{}: {e}", path.display(), n + 1)))?;
        lines.push(parsed);
    }
    Ok(lines)
}

/// Trends: the latest two recorded medians per cell×variant, and the delta
/// between them — the "is it drifting?" view fed only by the history feed.
fn trends_table(history: &[HistoryLine]) -> String {
    use std::collections::BTreeMap;
    // Preserve append order (chronological) per key; keep the last two.
    let mut by_key: BTreeMap<(String, String), Vec<&HistoryLine>> = BTreeMap::new();
    for line in history {
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
            fmt_ms(latest.median_ms),
            prev.map_or_else(|| "—".to_owned(), |p| fmt_ms(p.median_ms)),
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Splice + regenerate
// ---------------------------------------------------------------------------

/// Splice `body` between the section's markers. Everything outside the
/// markers — the narrative — is preserved byte-for-byte.
pub fn splice(results_md: &str, section: &str, body: &str) -> Result<String> {
    let begin = begin_marker(section);
    let end = end_marker(section);
    let start = results_md.find(&begin).ok_or_else(|| {
        BenchError(format!(
            "RESULTS.md has no `{begin}` marker — insert the marker pair where the {section} table belongs"
        ))
    })?;
    let after_begin = start + begin.len();
    let end_at = results_md[after_begin..]
        .find(&end)
        .map(|i| after_begin + i)
        .ok_or_else(|| BenchError(format!("RESULTS.md has `{begin}` but no `{end}`")))?;
    let mut out = String::with_capacity(results_md.len() + body.len());
    out.push_str(&results_md[..after_begin]);
    out.push('\n');
    out.push_str(body.trim_end());
    out.push('\n');
    out.push_str(&results_md[end_at..]);
    Ok(out)
}

/// Regenerate the two generated regions — `matrix` (from committed artifacts)
/// and `trends` (from the history feed) — from disk. The hand-written sections
/// are preserved byte-for-byte. A cell with an artifact file that cannot be
/// read (corrupt, format-version mismatch) fails loudly.
pub fn regenerate(
    results_md_path: &Path,
    cells: &[Cell],
    bars: &[Bar],
    results_dir: &Path,
) -> Result<Vec<String>> {
    let mut content = std::fs::read_to_string(results_md_path)
        .map_err(|e| BenchError(format!("reading {}: {e}", results_md_path.display())))?;

    let mut artifacts: Vec<Artifact> = Vec::new();
    for cell in cells.iter().filter(|c| !is_selftest(&c.id)) {
        if !results_dir.join(format!("{}.json", cell.id)).is_file() {
            continue; // not-yet-measured: simply absent from the matrix
        }
        artifacts.push(crate::artifact::read(results_dir, &cell.id)?);
    }
    let refs: Vec<&Artifact> = artifacts.iter().collect();

    let matrix_body = format!(
        "{}{}",
        table_for(&refs, bars),
        provenance(
            &refs,
            "Generated by `rdlt-bench report` from committed artifacts"
        )
    );
    content = splice(&content, "matrix", &matrix_body)?;

    let history = read_history(&results_md_path.with_file_name("history.jsonl"))?;
    content = splice(&content, "trends", &trends_table(&history))?;

    std::fs::write(results_md_path, content)?;
    Ok(vec!["matrix".to_owned(), "trends".to_owned()])
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOC: &str = "# Results\n\nNarrative ABOVE — every byte sacred.\n\n<!-- rdlt-bench:BEGIN matrix -->\nold table\n<!-- rdlt-bench:END matrix -->\n\nNarrative BELOW — history, policy notes.\n";

    #[test]
    fn splice_replaces_only_between_markers() {
        let out = splice(DOC, "matrix", "| new |").unwrap();
        assert!(out.contains("Narrative ABOVE — every byte sacred."));
        assert!(out.contains("Narrative BELOW — history, policy notes."));
        assert!(out.contains("| new |"));
        assert!(!out.contains("old table"));
        // idempotent: splicing the same body again changes nothing
        assert_eq!(splice(&out, "matrix", "| new |").unwrap(), out);
    }

    #[test]
    fn narrative_is_byte_identical_outside_markers() {
        let out = splice(DOC, "matrix", "X").unwrap();
        let begin = begin_marker("matrix");
        let end = end_marker("matrix");
        let (before_in, after_in) = (
            DOC.split(&begin).next().unwrap(),
            DOC.split(&end).nth(1).unwrap(),
        );
        let (before_out, after_out) = (
            out.split(&begin).next().unwrap(),
            out.split(&end).nth(1).unwrap(),
        );
        assert_eq!(before_in, before_out);
        assert_eq!(after_in, after_out);
    }

    #[test]
    fn missing_markers_are_a_refusal_not_a_guess() {
        let err = splice("no markers here", "matrix", "X")
            .unwrap_err()
            .to_string();
        assert!(err.contains("BEGIN matrix"), "{err}");
        let err2 = splice("<!-- rdlt-bench:BEGIN matrix -->", "matrix", "X")
            .unwrap_err()
            .to_string();
        assert!(
            err2.contains("no `<!-- rdlt-bench:END matrix -->`"),
            "{err2}"
        );
    }

    #[test]
    fn table_includes_missing_baselines_loudly() {
        let mut artifact = crate::artifact::tests::minimal("loud-cell");
        artifact.competitors.insert(
            "dlt".into(),
            CompetitorSide::Missing {
                reason: "image not built".into(),
            },
        );
        let table = table_for(&[&artifact], &[]);
        assert!(table.contains("MISSING (image not built)"), "{table}");
        assert!(table.contains("loud-cell"), "{table}");
    }

    #[test]
    fn empty_matrix_is_a_coherent_header_only_table() {
        let table = table_for(&[], &[]);
        assert!(table.contains("| Cell |"), "{table}");
        // header + separator only, no data rows
        assert_eq!(table.lines().count(), 2, "{table}");
    }

    #[test]
    fn history_round_trips_and_trends_show_the_delta() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let mut a1 = crate::artifact::tests::minimal("pg-to-pg-1m");
        a1.recorded_at = "2026-07-24".into();
        a1.rdlt.median_ms = 1000.0;
        append_history(&path, &a1).unwrap();
        let mut a2 = crate::artifact::tests::minimal("pg-to-pg-1m");
        a2.recorded_at = "2026-07-25".into();
        a2.rdlt.median_ms = 1100.0;
        append_history(&path, &a2).unwrap();

        let history = read_history(&path).unwrap();
        assert_eq!(history.len(), 2);
        let table = trends_table(&history);
        assert!(table.contains("pg-to-pg-1m"), "{table}");
        assert!(table.contains("rdlt"), "{table}");
        assert!(table.contains("+10.0%"), "{table}");
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
        append_history(&path, &before).unwrap();
        let mut after = crate::artifact::tests::minimal("pg-to-pg-dedup-1m");
        after.recorded_at = "2026-07-25".into();
        after.rdlt.median_ms = 5_028.0;
        after.rdlt.rows = Some(1_000_000);
        append_history(&path, &after).unwrap();

        let table = trends_table(&read_history(&path).unwrap());
        assert!(table.contains("rows 3000000 → 1000000"), "{table}");
        assert!(
            !table.contains('%'),
            "a scope change is not a speedup: {table}"
        );
    }
}
