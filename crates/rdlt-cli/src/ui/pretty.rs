//! The live terminal display: one line per stream, a totals line, all
//! redrawn in place at 10 Hz over the shared `Metrics` fold. Honest by
//! construction: everything shown is LIVE and approximate; the final
//! summary re-renders from the RunReport, the exactly-once record.
//!
//! indicatif rather than a full-screen TUI, deliberately: a batch
//! tool must compose with scrollback, pipes and CI logs, and leave
//! the terminal exactly as found — a full-screen takeover erases the
//! run's visible history on exit.

use std::collections::BTreeMap;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use rdlt::prelude::{Metrics, PipelineEvent, ResumedFrom, StreamName};

use super::format;

/// How often the display redraws. Coarser than the terminal could
/// take, deliberately: the numbers move faster than eyes read.
const REDRAW: Duration = Duration::from_millis(100);

pub struct Pretty {
    metrics: Metrics,
    multi: MultiProgress,
    header: ProgressBar,
    totals: ProgressBar,
    streams: BTreeMap<StreamName, ProgressBar>,
    finished: BTreeMap<StreamName, bool>,
}

impl Pretty {
    pub fn new(pipeline: &str) -> Self {
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(10));
        let header = multi.add(ProgressBar::no_length());
        header.set_style(ProgressStyle::with_template("  {msg}").expect("static template"));
        header.set_message(format!("pipeline {}", super::sanitize_identifier(pipeline)));
        let totals = multi.add(ProgressBar::no_length());
        totals.set_style(ProgressStyle::with_template("  {msg}").expect("static template"));
        Self {
            metrics: Metrics::new(),
            multi,
            header,
            totals,
            streams: BTreeMap::new(),
            finished: BTreeMap::new(),
        }
    }

    /// The redraw cadence, exposed so the driver can tick between
    /// events.
    pub fn redraw_every() -> Duration {
        REDRAW
    }

    /// Fold one event and update whatever rows it touches.
    pub fn apply(&mut self, event: &PipelineEvent) {
        self.metrics.apply(event);
        match event {
            PipelineEvent::RunStarted {
                load_id,
                resumed_from,
            } => {
                let resumed = match resumed_from {
                    ResumedFrom::Fresh => "fresh".to_owned(),
                    ResumedFrom::Cursor => "resumed from cursor".to_owned(),
                    ResumedFrom::Wal { replayed_batches } => {
                        format!("resumed from WAL ({replayed_batches} replayed)")
                    }
                    // `#[non_exhaustive]` upstream: an unknown resume
                    // kind still ran — say so without guessing.
                    _ => "resumed".to_owned(),
                };
                let current = self.header.message();
                self.header
                    .set_message(format!("{current} · load {load_id} · {resumed}"));
            }
            PipelineEvent::StreamStarted { stream, .. } => {
                let bar = self
                    .multi
                    .insert_before(&self.totals, ProgressBar::no_length());
                bar.set_style(
                    ProgressStyle::with_template("  {spinner:.cyan} {msg}")
                        .expect("static template"),
                );
                bar.enable_steady_tick(REDRAW);
                // The declared stream name is connector-controlled
                // text; indicatif writes messages straight to the
                // terminal, so it is escaped at this boundary exactly
                // like the plain renderer's lines are at stderr_line —
                // and with the IDENTIFIER predicate (5L14), so even an
                // admitted joiner can't render the name invisibly or a
                // line break forge display lines.
                bar.set_message(super::sanitize_identifier(stream.as_str()));
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
    pub fn redraw(&mut self) {
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
            let name = super::sanitize_identifier(stream.as_str());
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
                // live-but-still, NOT finished: `finish_with_message` commits
                // the row to scrollback beyond `clear()`'s reach (indicatif
                // treats a finished bar as permanent output, not a
                // redrawable line), which is exactly the one-✔-row-per-
                // stream residue `clear()` is supposed to remove. Style
                // first so the ✔ template is in effect when the message
                // renders, disable the steady tick so a done bar stops
                // ticking (nothing left to animate), then set — never
                // finish — the message.
                bar.set_style(ProgressStyle::with_template("  ✔ {msg}").expect("static template"));
                bar.disable_steady_tick();
                line.push_str(" done");
                bar.set_message(line);
            } else {
                bar.set_message(line);
            }
        }
        let mut totals = format!(
            "total {} rows · {} in-mem",
            format::count(snap.rows_written),
            format::bytes(snap.bytes_written),
        );
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

    /// Tear the ENTIRE live display down — header, every stream row, totals —
    /// leaving nothing behind. The summary (on success) or the error text
    /// (on failure) is the durable record; the live rows exist only to
    /// animate a run in progress and are ephemeral by design, so every row
    /// must stay CLEARABLE for as long as this display lives. That is why
    /// `redraw`'s done branch never calls `finish_with_message`: indicatif
    /// treats a finished bar as committed output beyond a `MultiProgress`
    /// clear's reach (it survives on screen forever, one line per stream,
    /// duplicating what the summary's per-table rows already say) — a
    /// done row must stay merely `set_message`d, live-but-still, so this
    /// `clear()` actually removes it.
    pub fn clear(self) {
        let _ = self.multi.clear();
    }
}
