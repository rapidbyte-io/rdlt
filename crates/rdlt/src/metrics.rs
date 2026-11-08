//! A run's metrics through the `metrics` crate: counters and histograms from its events, gauges
//! sampled from its status; labelled by pipeline and stream alone.

#[cfg(test)]
mod tests;

use metrics::{counter, gauge, histogram};
use rdlt_engine::{Event, EventKind, Scope, Status};
use tokio::sync::broadcast;

/// Records every event of a run of `pipeline` that `metered` holds, and counts those it fell
/// behind on; the metrics lag no engine task.
pub(crate) fn drained(pipeline: &str, metered: &mut broadcast::Receiver<Event>) {
    loop {
        match metered.try_recv() {
            Ok(event) => recorded(pipeline, &event),
            Err(broadcast::error::TryRecvError::Lagged(dropped)) => unmetered(pipeline, dropped),
            Err(_) => break,
        }
    }
}

/// Records what `event` of a run of `pipeline` tells.
pub(crate) fn recorded(pipeline: &str, event: &Event) {
    let pipeline = pipeline.to_owned();
    match &event.kind {
        EventKind::Committed {
            took,
            phases,
            streams,
            ..
        } => {
            counter!("rdlt_commits_total", "pipeline" => pipeline.clone()).increment(1);
            histogram!("rdlt_commit_seconds", "pipeline" => pipeline.clone())
                .record(took.as_secs_f64());
            histogram!("rdlt_commit_flush_seconds", "pipeline" => pipeline.clone())
                .record(phases.flush.as_secs_f64());
            histogram!("rdlt_commit_publish_seconds", "pipeline" => pipeline.clone())
                .record(phases.commit.as_secs_f64());
            histogram!("rdlt_commit_ack_seconds", "pipeline" => pipeline.clone())
                .record(phases.ack.as_secs_f64());
            for (stream, report) in streams {
                let labels = [("pipeline", pipeline.clone()), ("stream", stream.clone())];
                counter!("rdlt_rows_committed_total", &labels).increment(report.rows);
                counter!("rdlt_bytes_committed_total", &labels).increment(report.bytes);
                counter!("rdlt_rows_discarded_total", &labels).increment(report.discarded_rows);
                counter!("rdlt_values_discarded_total", &labels).increment(report.discarded_values);
            }
        }
        EventKind::Credited { rows, .. } => {
            counter!("rdlt_rows_credited_total", "pipeline" => pipeline).increment(*rows);
        }
        EventKind::Retrying {
            scope: Scope::Attempt,
            ..
        } => counter!("rdlt_attempt_retries_total", "pipeline" => pipeline).increment(1),
        EventKind::Retrying {
            scope: Scope::Commit,
            ..
        } => counter!("rdlt_commit_retries_total", "pipeline" => pipeline).increment(1),
        EventKind::SchemaChanged { .. } => {
            counter!("rdlt_schema_changes_total", "pipeline" => pipeline).increment(1);
        }
        EventKind::AttemptStarted
        | EventKind::RetentionReset { .. }
        | EventKind::Forgotten { .. }
        | EventKind::RunEnded { .. } => {}
    }
}

/// Counts the events of a run of `pipeline` its metrics fell `dropped` behind on.
fn unmetered(pipeline: &str, dropped: u64) {
    counter!("rdlt_events_unmetered_total", "pipeline" => pipeline.to_owned()).increment(dropped);
}

/// Sets the gauges of a run of `pipeline` from its `status`.
pub(crate) fn sampled(pipeline: &str, status: &Status) {
    let held = status.memory;
    for (name, bytes) in [
        ("rdlt_memory_held_bytes_intake", held.intake),
        ("rdlt_memory_held_bytes_work", held.work),
        ("rdlt_memory_held_bytes_cursors", held.cursors),
        ("rdlt_memory_held_bytes_tables", held.tables),
        ("rdlt_memory_held_bytes_reads", held.reads),
        ("rdlt_memory_held_bytes_control", held.control),
    ] {
        gauge!(name, "pipeline" => pipeline.to_owned()).set(approx(bytes));
    }
    let queued = status.writers.iter().map(|writer| writer.queued).sum();
    gauge!("rdlt_writer_queued_writes", "pipeline" => pipeline.to_owned()).set(approx(queued));
    gauge!("rdlt_partitions_reading", "pipeline" => pipeline.to_owned())
        .set(approx(status.partitions.reading));
    gauge!("rdlt_partitions_waiting", "pipeline" => pipeline.to_owned())
        .set(approx(status.partitions.waiting));
    for (stream, freshness) in &status.streams {
        let labels = [
            ("pipeline", pipeline.to_owned()),
            ("stream", stream.clone()),
        ];
        gauge!("rdlt_stream_behind_records", &labels).set(approx(freshness.behind.unwrap_or(0)));
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a gauge is a measure, which 53 bits of mantissa hold closely enough"
)]
fn approx(value: u64) -> f64 {
    value as f64
}
