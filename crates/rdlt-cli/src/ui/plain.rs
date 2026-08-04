//! The line-per-event renderer: CI logs, pipes, and anyone who reads
//! scrollback. The default until the pretty renderer lands, and the
//! permanent form off-TTY.

use rdlt::prelude::PipelineEvent;

use crate::args::Verbosity;

/// Render one event as its stderr line, or `None` for silence at this
/// verbosity. The NORMAL lines are the pre-036 CLI's spellings,
/// unchanged; the VERBOSE additions cover the 036 telemetry.
pub fn line(event: &PipelineEvent, verbosity: Verbosity) -> Option<String> {
    if verbosity == Verbosity::Quiet {
        return None;
    }
    let verbose = verbosity >= Verbosity::Verbose;
    match event {
        PipelineEvent::StreamStarted { stream } => Some(format!("-> stream {stream} started")),
        PipelineEvent::StreamFinished { stream } => Some(format!("-> stream {stream} finished")),
        PipelineEvent::BatchLoaded { table, rows, .. } => Some(format!("  {table}: +{rows} rows")),
        PipelineEvent::SchemaEvolved { delta } => Some(format!(
            "  schema: {} -> {} changes",
            delta.table,
            delta.changes.len()
        )),
        PipelineEvent::Committed { commit_seq, .. } => Some(format!("commit {commit_seq} ok")),
        PipelineEvent::Discarded {
            table,
            rows,
            values,
            ..
        } => Some(format!(
            "! {table}: discarded {rows} rows / {values} values"
        )),
        PipelineEvent::Retried { stream, attempt } => {
            Some(format!("! retry attempt {attempt} ({stream:?})"))
        }
        PipelineEvent::BatchRead {
            stream,
            rows,
            bytes,
        } if verbose => Some(format!("  {stream}: read {rows} rows ({bytes} B)")),
        PipelineEvent::CommitStarted { commit_seq } if verbose => {
            Some(format!("commit {commit_seq} starting"))
        }
        PipelineEvent::PartClosed {
            table,
            encoded_bytes,
            reason,
        } if verbose => Some(format!(
            "  {table}: part closed ({encoded_bytes} B, {reason:?})"
        )),
        // Heartbeats are for machines; quiet events stay quiet below
        // their verbosity.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt::prelude::TableName;

    /// The NORMAL spellings are the pre-036 CLI's, byte for byte — the
    /// compatibility contract's stderr half.
    #[test]
    fn the_frozen_lines_render_unchanged() {
        let event = PipelineEvent::BatchLoaded {
            table: TableName::new("events"),
            rows: 40,
            bytes: 1,
        };
        assert_eq!(
            line(&event, Verbosity::Normal).as_deref(),
            Some("  events: +40 rows")
        );
        let event = PipelineEvent::Committed {
            commit_seq: 3,
            cursors: Default::default(),
        };
        assert_eq!(
            line(&event, Verbosity::Normal).as_deref(),
            Some("commit 3 ok")
        );
    }

    /// Quiet silences everything; verbose reveals the 036 lines.
    #[test]
    fn verbosity_gates_hold() {
        let read = PipelineEvent::BatchRead {
            stream: rdlt::prelude::StreamName::new("s"),
            rows: 10,
            bytes: 100,
        };
        assert_eq!(line(&read, Verbosity::Quiet), None);
        assert_eq!(line(&read, Verbosity::Normal), None);
        assert_eq!(
            line(&read, Verbosity::Verbose).as_deref(),
            Some("  s: read 10 rows (100 B)")
        );
    }
}
