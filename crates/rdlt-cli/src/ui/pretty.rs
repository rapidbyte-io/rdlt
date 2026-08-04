//! The live terminal display: one line per stream, a totals line, all
//! redrawn in place at 10 Hz over the shared `Metrics` fold. Honest by
//! construction: everything shown is LIVE and approximate; the final
//! summary re-renders from the RunReport, the exactly-once record.
//!
//! indicatif rather than a full-screen TUI, deliberately (plan D4): a
//! batch tool must compose with scrollback, pipes and CI logs, and
//! leave the terminal exactly as found.

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
        header.set_message(format!("pipeline {pipeline}"));
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
            PipelineEvent::StreamStarted { stream } => {
                let bar = self
                    .multi
                    .insert_before(&self.totals, ProgressBar::no_length());
                bar.set_style(
                    ProgressStyle::with_template("  {spinner:.cyan} {msg}")
                        .expect("static template"),
                );
                bar.enable_steady_tick(REDRAW);
                bar.set_message(format!("{stream}"));
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
            // The written side joins by NAME where a table matches this
            // stream (the overwhelmingly common case); tables that
            // match no stream surface in the totals regardless.
            let written = snap
                .tables
                .iter()
                .find(|(table, _)| table.as_str() == stream.as_str())
                .map(|(_, t)| *t)
                .unwrap_or_default();
            let done = self.finished.get(stream).copied().unwrap_or(false);
            let mut line = format!(
                "{stream:<14} read {:<8} written {:<8} {:<9}",
                format::count(read.rows_read),
                format::count(written.rows_written),
                format::bytes(written.bytes_written),
            );
            if written.output_bytes > 0 {
                line.push_str(&format!(" out {:<9}", format::bytes(written.output_bytes)));
            }
            if done {
                bar.set_style(ProgressStyle::with_template("  ✔ {msg}").expect("static template"));
                line.push_str(" done");
                bar.finish_with_message(line);
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

    /// Tear the live rows down (the summary replaces them). Clearing —
    /// not finishing — so scrollback keeps the run's real history: the
    /// summary that follows carries the durable numbers.
    pub fn clear(self) {
        let _ = self.multi.clear();
    }
}
