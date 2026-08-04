//! The final summary, rendered from the RunReport — the exactly-once
//! numbers, never the live fold. Both renderers end with this; only
//! quiet mode suppresses it.

use std::time::Duration;

use console::style;
use rdlt::prelude::{ResumedFrom, RunReport};

use super::format;

/// Render the summary block to a string (stderr's, ultimately).
/// Styling degrades to plain text when the stream is not a terminal
/// or color is disabled — `console` handles both.
pub fn render(report: &RunReport) -> String {
    let elapsed = Duration::from_millis(report.elapsed_ms);
    let total_rows: u64 = report.tables.values().map(|t| t.rows).sum();
    let total_bytes: u64 = report.tables.values().map(|t| t.bytes).sum();
    let mut out = String::new();

    let resumed = match &report.resumed_from {
        ResumedFrom::Fresh => "fresh".to_owned(),
        ResumedFrom::Cursor => "resumed from cursor".to_owned(),
        ResumedFrom::Wal { replayed_batches } => {
            format!("resumed from WAL ({replayed_batches} batches replayed)")
        }
        // `#[non_exhaustive]` upstream: an unknown resume kind still
        // ran — say so without guessing.
        _ => "resumed".to_owned(),
    };
    out.push_str(&format!(
        "\n  {} {} · {} · {} · {} · {resumed}\n",
        style("✔").green().bold(),
        style(report.pipeline.as_str()).bold(),
        format::duration(elapsed),
        style("exactly-once").dim(),
        format::commits(report.commits),
    ));

    // The per-table block, aligned by hand; the `output` column
    // appears only when some table wrote files — a faked zero column
    // on SQL destinations would imply a measurement that never
    // happened.
    let any_output = report.tables.values().any(|t| t.output_bytes > 0);
    let name_width = report
        .tables
        .keys()
        .map(|t| t.as_str().len())
        .max()
        .unwrap_or(5)
        .max("table".len());
    let output_header = if any_output {
        format!("  {:>10}", style("output").dim())
    } else {
        String::new()
    };
    out.push_str(&format!(
        "\n  {:<name_width$}  {:>10}  {:>10}{output_header}  {:>10}\n",
        style("table").dim(),
        style("rows").dim(),
        style("bytes").dim(),
        style("rate").dim(),
    ));
    let elapsed_secs = elapsed.as_secs_f64();
    for (table, totals) in &report.tables {
        let rate = if elapsed_secs > f64::EPSILON && totals.rows > 0 {
            format::rate(totals.rows as f64 / elapsed_secs)
        } else {
            "—".to_owned()
        };
        let output = if any_output {
            format!("  {:>10}", format::bytes(totals.output_bytes))
        } else {
            String::new()
        };
        out.push_str(&format!(
            "  {:<name_width$}  {:>10}  {:>10}{output}  {:>10}\n",
            table.as_str(),
            format::count(totals.rows),
            format::bytes(totals.bytes),
            rate,
        ));
        if totals.discarded_rows > 0 || totals.discarded_values > 0 {
            out.push_str(&format!(
                "  {}\n",
                style(format!(
                    "! discarded {} rows / {} values",
                    totals.discarded_rows, totals.discarded_values
                ))
                .yellow()
            ));
        }
    }

    // The precomputed average — the report's own number, not a local
    // re-derivation.
    let avg = report
        .rows_per_sec_avg
        .map(|r| format!(" · {} avg", format::rate(r)))
        .unwrap_or_default();
    let total_output: u64 = report.tables.values().map(|t| t.output_bytes).sum();
    let output = if total_output > 0 {
        format!(" · output {}", format::bytes(total_output))
    } else {
        String::new()
    };
    out.push_str(&format!(
        "\n  total {} rows · {} in-mem{output}{avg}\n",
        format::count(total_rows),
        format::bytes(total_bytes),
    ));
    let mut facts: Vec<String> = Vec::new();
    if !report.schema_migrations.is_empty() {
        facts.push(format!(
            "schema: {} migrations",
            report.schema_migrations.len()
        ));
    }
    if report.retries > 0 {
        facts.push(format!("retries: {}", report.retries));
    }
    if !facts.is_empty() {
        out.push_str(&format!("  {}\n", style(facts.join(" · ")).dim()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The summary reads its numbers from the REPORT — a synthetic one
    /// renders every section deterministically (styling off under
    /// test: console detects the non-terminal).
    #[test]
    fn a_report_renders_all_sections() {
        let mut report = RunReport::new(
            rdlt::prelude::StreamName::new("p").as_str().into(),
            rdlt::sdk::spi::core::LoadId::new("load-1"),
        );
        report.commits = 2;
        report.elapsed_ms = 2_000;
        let rendered = render(&report);
        assert!(rendered.contains("exactly-once"), "{rendered}");
        assert!(rendered.contains("2 commits"), "{rendered}");
        assert!(!rendered.contains("1 commits"), "{rendered}");
        assert!(rendered.contains("fresh"), "{rendered}");
        assert!(rendered.contains("total 0 rows"), "{rendered}");
    }
}
