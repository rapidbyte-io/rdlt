//! The live terminal display: one line per stream, a totals line, all
//! redrawn in place at 10 Hz over the shared `Metrics` fold. Honest by
//! construction: everything shown is LIVE and approximate; the final
//! summary re-renders from the run report, the exactly-once record.
//!
//! indicatif rather than a full-screen TUI, deliberately: a batch
//! tool must compose with scrollback, pipes and CI logs, and leave
//! the terminal exactly as found — a full-screen takeover erases the
//! run's visible history on exit.

use std::collections::BTreeMap;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use rdlt::event::PipelineEvent;
use rdlt::id::StreamName;
use rdlt::metrics::Metrics;

use crate::render::{format, stderr};

/// How often the display redraws — the driver ticks at this cadence
/// between events. Coarser than the terminal could take, deliberately:
/// the numbers move faster than eyes read.
pub(crate) const REDRAW_EVERY: Duration = Duration::from_millis(100);

pub(crate) struct Pretty {
    metrics: Metrics,
    multi: MultiProgress,
    header: ProgressBar,
    totals: ProgressBar,
    streams: BTreeMap<StreamName, ProgressBar>,
    finished: BTreeMap<StreamName, bool>,
}

impl Pretty {
    pub(crate) fn new(pipeline: &str) -> Self {
        Self::with_target(pipeline, ProgressDrawTarget::stderr_with_hz(10))
    }

    /// The display over any draw target — the tests render into an
    /// in-memory terminal through this seam.
    fn with_target(pipeline: &str, target: ProgressDrawTarget) -> Self {
        // Every row is `{wide_msg}`: truncated to the terminal's width,
        // never wrapped, so the region is always exactly one row per bar
        // and a redraw's cursor arithmetic cannot disagree with the
        // terminal about how many rows a long line took.
        let multi = MultiProgress::with_draw_target(target);
        let header = multi.add(ProgressBar::no_length());
        header.set_style(ProgressStyle::with_template("  {wide_msg}").expect("static template"));
        header.set_message(format!(
            "pipeline {}",
            stderr::sanitize_identifier(pipeline)
        ));
        let totals = multi.add(ProgressBar::no_length());
        totals.set_style(ProgressStyle::with_template("  {wide_msg}").expect("static template"));
        Self {
            metrics: Metrics::new(),
            multi,
            header,
            totals,
            streams: BTreeMap::new(),
            finished: BTreeMap::new(),
        }
    }

    /// Fold one event and update whatever rows it touches.
    pub(crate) fn apply(&mut self, event: &PipelineEvent) {
        self.metrics.apply(event);
        match event {
            PipelineEvent::RunStarted {
                load_id,
                resumed_from,
            } => {
                let resumed = format::resumed_from(resumed_from);
                let current = self.header.message();
                self.header
                    .set_message(format!("{current} · load {load_id} · {resumed}"));
            }
            PipelineEvent::StreamStarted { stream, .. } => {
                let bar = self
                    .multi
                    .insert_before(&self.totals, ProgressBar::no_length());
                bar.set_style(
                    ProgressStyle::with_template("  {spinner:.cyan} {wide_msg}")
                        .expect("static template"),
                );
                // The declared stream name is connector-controlled
                // text and indicatif writes messages straight to the
                // terminal, so it is escaped here with the IDENTIFIER
                // predicate: an admitted joiner cannot render the name
                // invisibly, and a line break cannot forge display
                // lines.
                bar.set_message(stderr::sanitize_identifier(stream.as_str()));
                self.streams.insert(stream.clone(), bar);
                self.finished.insert(stream.clone(), false);
            }
            PipelineEvent::StreamFinished { stream } => {
                self.finished.insert(stream.clone(), true);
            }
            _ => {}
        }
        self.redraw();
    }

    /// Recompute every row from the fold — also the periodic tick.
    pub(crate) fn redraw(&mut self) {
        let snap = self.metrics.snapshot();
        for (stream, bar) in &self.streams {
            let read = snap.streams.get(stream).copied().unwrap_or_default();
            // The written side joins through the mapping StreamStarted
            // ANNOUNCED — never by string equality, which breaks the
            // moment the destination's normalization touches a name.
            // Child tables shredded out of nested payloads have no
            // stream row and surface in the totals.
            let written = snap
                .stream_tables
                .get(stream)
                .and_then(|table| snap.tables.get(table))
                .copied()
                .unwrap_or_default();
            let done = self.finished.get(stream).copied().unwrap_or(false);
            // Escaped like the announcement above — same string, same
            // boundary.
            let name = stderr::sanitize_identifier(stream.as_str());
            let mut line = format!(
                "{name:<14} read {:<8} written {:<8} {:<9}",
                format::count(read.rows_read),
                format::count(written.rows_written),
                format::bytes(written.bytes_written),
            );
            if written.output_bytes > 0 {
                line.push_str(&format!(" out {:<9}", format::bytes(written.output_bytes)));
            }
            if done {
                // Style first so the ✔ template is in effect when the
                // message renders; set — never finish — the message, so
                // the row stays clearable (see `clear`).
                bar.set_style(
                    ProgressStyle::with_template("  ✔ {wide_msg}").expect("static template"),
                );
                line.push_str(" done");
                bar.set_message(line);
            } else {
                // The feed loop is the one driver: this tick advances the
                // spinner, so no per-stream ticker thread exists to race
                // the teardown.
                bar.tick();
                bar.set_message(line);
            }
        }
        let mut totals = format::totals(snap.rows_written, snap.bytes_written);
        if let Some(rate) = snap.rows_per_sec {
            totals.push_str(&format!(" · {}", format::rate(rate)));
        }
        totals.push_str(&format!(" · {}", format::commits(snap.commits)));
        if snap.committing {
            totals.push_str(" (committing…)");
        } else if let Some(ago) = snap.since_last_commit {
            totals.push_str(&format!(" (last {} ago)", format::duration(ago)));
        }
        self.totals.set_message(totals);
    }

    /// Tear the ENTIRE live display down — header, every stream row,
    /// totals — leaving nothing behind: the summary or the error text
    /// that follows is the durable record. Every row stays clearable
    /// because `redraw` never `finish`es a bar (indicatif keeps a
    /// finished row as committed output beyond a clear's reach). The
    /// target goes hidden after the clear so dropping the bar handles —
    /// which reaps rows and would redraw the rest — draws nothing more.
    pub(crate) fn clear(self) {
        let _ = self.multi.clear();
        self.multi.set_draw_target(ProgressDrawTarget::hidden());
    }
}

#[cfg(test)]
mod tests {
    use indicatif::InMemoryTerm;
    use rdlt::id::{StreamName, TableName};
    use rdlt::report::ResumedFrom;
    use rdlt::sdk::spi::core::id::LoadId;

    use super::*;

    fn rows(term: &InMemoryTerm) -> usize {
        term.contents().lines().count()
    }

    /// Redraw after the draw-rate limiter has refilled, so the assertions
    /// read the display's settled contents rather than a dropped frame.
    fn settle(display: &mut Pretty) {
        std::thread::sleep(Duration::from_millis(120));
        display.redraw();
    }

    /// The region's height is fixed from the moment the streams are
    /// announced: batches, ticks, a stream finishing (the ✔ row) and a
    /// commit change text, never the row count — a row-count change is
    /// what shifts the display on a terminal. `clear()` leaves nothing
    /// behind, and nothing is drawn after it.
    #[test]
    fn the_display_keeps_its_height_and_clears_without_residue() {
        let term = InMemoryTerm::new(24, 100);
        let target = ProgressDrawTarget::term_like_with_hz(Box::new(term.clone()), 200);
        let mut display = Pretty::with_target("p", target);
        display.apply(&PipelineEvent::RunStarted {
            load_id: LoadId::new("load-1"),
            resumed_from: ResumedFrom::Fresh,
        });
        for stream in ["a", "b"] {
            display.apply(&PipelineEvent::StreamStarted {
                stream: StreamName::new(stream),
                table: TableName::new(stream),
            });
        }
        settle(&mut display);
        // header + two stream rows + totals
        assert_eq!(rows(&term), 4, "{}", term.contents());
        assert!(term.contents().contains("load load-1 · fresh"));

        display.apply(&PipelineEvent::BatchLoaded {
            table: TableName::new("a"),
            rows: 40,
            bytes: 400,
        });
        display.redraw();
        display.apply(&PipelineEvent::StreamFinished {
            stream: StreamName::new("a"),
        });
        display.apply(&PipelineEvent::Committed {
            commit_seq: 1,
            cursors: Default::default(),
        });
        settle(&mut display);
        assert_eq!(rows(&term), 4, "{}", term.contents());
        assert!(term.contents().contains("✔ a"), "{}", term.contents());
        assert!(term.contents().contains("1 commit"), "{}", term.contents());

        let _ = term.moves_since_last_check();
        display.clear();
        assert_eq!(term.contents(), "", "the display leaves no residue");
        let moves = term.moves_since_last_check();
        assert!(
            !moves.contains("Str("),
            "nothing is drawn after the clear:\n{moves}"
        );
        // No ticker thread exists to draw late: a redraw interval later
        // the terminal is still blank and untouched.
        std::thread::sleep(REDRAW_EVERY * 2);
        assert_eq!(term.contents(), "");
        assert_eq!(term.moves_since_last_check(), "");
    }

    /// A narrow terminal truncates rows, never wraps them: the region
    /// stays one row per bar however long the pipeline name, load id
    /// or stream row runs.
    #[test]
    fn long_rows_truncate_instead_of_wrapping() {
        let term = InMemoryTerm::new(24, 40);
        let target = ProgressDrawTarget::term_like_with_hz(Box::new(term.clone()), 200);
        let mut display = Pretty::with_target("a-rather-long-pipeline-name", target);
        display.apply(&PipelineEvent::RunStarted {
            load_id: LoadId::new("1a011417aec-4a05a-0-a911350ec4463e1c"),
            resumed_from: ResumedFrom::Wal {
                replayed_batches: 3,
            },
        });
        display.apply(&PipelineEvent::StreamStarted {
            stream: StreamName::new("a-stream-name-past-the-column"),
            table: TableName::new("t"),
        });
        std::thread::sleep(Duration::from_millis(120));
        display.redraw();
        let contents = term.contents();
        assert_eq!(contents.lines().count(), 3, "{contents}");
        assert!(
            contents
                .lines()
                .all(|line| console::measure_text_width(line) <= 40),
            "{contents}"
        );
        display.clear();
        assert_eq!(term.contents(), "");
    }
}
