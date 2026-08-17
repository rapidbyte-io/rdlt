//! RESULTS.md generation: the matrix and trends tables regenerate from the
//! recorded artifacts and the history feed between explicit markers; the
//! hand-written narrative outside the markers is preserved byte-for-byte.
//! No markers → refusal, never guessing. The same row builder renders the
//! terminal run summary, so the two views can never drift.

use crate::artifact::{Artifact, CompetitorSide};
use crate::bar::{self, Bar, Kind};
use crate::cell::{self, Cell};
use crate::error::{Error, Result};
use crate::paths::Paths;
use crate::{artifact, history};

pub(crate) fn begin_marker(section: &str) -> String {
    format!("<!-- rdlt-bench:BEGIN {section} -->")
}
pub(crate) fn end_marker(section: &str) -> String {
    format!("<!-- rdlt-bench:END {section} -->")
}

pub(crate) fn fmt_ms(ms: f64) -> String {
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
        Some(Kind::RatioVs { min_ratio, .. }) => format!("≥ {min_ratio}×"),
        Some(Kind::AbsoluteMs { max_ms }) => format!("≤ {max_ms} ms abs"),
        Some(Kind::RssRatioVs { max_rss_ratio, .. }) => {
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
            if bar::evaluate(bar, artifact).passed() {
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
/// text for the run summary) consume these cells. Emphasis (`**`) is
/// markdown-layer decoration, added there only.
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
pub(crate) fn summary_table(artifacts: &[&Artifact], bars: &[Bar]) -> String {
    let footer = provenance(artifacts, "Measured by this invocation");
    format!(
        "{}{}",
        terminal_table(artifacts, bars),
        footer.replace('_', "") // no markdown italics on a terminal
    )
}

/// Splice `body` between the section's markers. Everything outside the
/// markers — the narrative — is preserved byte-for-byte.
pub(crate) fn splice(results_md: &str, section: &str, body: &str) -> Result<String> {
    let begin = begin_marker(section);
    let end = end_marker(section);
    let start = results_md.find(&begin).ok_or_else(|| {
        Error(format!(
            "RESULTS.md has no `{begin}` marker — insert the marker pair where the {section} table belongs"
        ))
    })?;
    let after_begin = start + begin.len();
    let end_at = results_md[after_begin..]
        .find(&end)
        .map(|i| after_begin + i)
        .ok_or_else(|| Error(format!("RESULTS.md has `{begin}` but no `{end}`")))?;
    let mut out = String::with_capacity(results_md.len() + body.len());
    out.push_str(&results_md[..after_begin]);
    out.push('\n');
    out.push_str(body.trim_end());
    out.push('\n');
    out.push_str(&results_md[end_at..]);
    Ok(out)
}

/// Regenerate the two generated regions of `paths.results_md` — `matrix`
/// from the recorded artifacts, `trends` from the history feed — with the
/// hand-written sections preserved byte-for-byte. A cell with an artifact
/// file that cannot be read (corrupt, format-version mismatch) fails loudly;
/// a not-yet-measured cell is simply absent from the matrix.
pub(crate) fn regenerate(paths: &Paths, cells: &[Cell], bars: &[Bar]) -> Result<Vec<String>> {
    let mut content = std::fs::read_to_string(&paths.results_md)
        .map_err(|e| Error(format!("reading {}: {e}", paths.results_md.display())))?;

    let mut artifacts: Vec<Artifact> = Vec::new();
    for cell in cells.iter().filter(|c| !cell::is_selftest(&c.id)) {
        if !paths.results.join(format!("{}.json", cell.id)).is_file() {
            continue;
        }
        artifacts.push(artifact::read(&paths.results, &cell.id)?);
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

    let history = history::read(&paths.history)?;
    content = splice(&content, "trends", &history::trends(&history))?;

    std::fs::write(&paths.results_md, content)?;
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

    /// Regenerate renders the artifacts and history feed of the ONE ledger
    /// the paths name — the same recorded artifacts the gate's bars bind
    /// against — so a report run re-renders the tables the bars cite.
    #[test]
    fn regenerate_renders_the_ledgers_artifacts_and_history() {
        let root = tempfile::tempdir().unwrap();
        let paths = Paths::rooted(root.path().to_path_buf(), root.path().join("target"));
        let mut artifact = crate::artifact::tests::minimal("pg-cell");
        artifact.rdlt.median_ms = 1234.0;
        crate::artifact::write(&paths.results, &artifact).unwrap();
        history::append(&paths.history, &artifact).unwrap();

        std::fs::write(
            &paths.results_md,
            "# Results\n\n<!-- rdlt-bench:BEGIN matrix -->\nstale\n<!-- rdlt-bench:END matrix -->\n\
             \n<!-- rdlt-bench:BEGIN trends -->\nstale\n<!-- rdlt-bench:END trends -->\n",
        )
        .unwrap();
        let cell: Cell = toml::from_str("id = \"pg-cell\"\nfixtures = []").unwrap();

        let updated = regenerate(&paths, &[cell], &[]).unwrap();
        assert_eq!(updated, ["matrix", "trends"]);
        let out = std::fs::read_to_string(&paths.results_md).unwrap();
        assert!(
            out.matches("| pg-cell |").count() >= 2,
            "both the matrix and the trends table carry the recorded cell: {out}"
        );
        assert!(out.contains("1.23 s"), "{out}");
        assert!(!out.contains("stale"), "{out}");
    }
}
