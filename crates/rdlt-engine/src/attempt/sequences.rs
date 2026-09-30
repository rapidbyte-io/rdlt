//! Who made the sequences of a stream's table, and whether its load may compare them (ADR 0023).

#[cfg(test)]
mod tests;

use rdlt_connector::{Sequences, TableState};

use crate::error::Error;
use crate::plan::StreamPlan;

/// The sequences state must record for `plan`'s table, which state records as `recorded`, before
/// its load, with whether the table keeps history; `None` when state records them already.
///
/// A change stream matched by key compares its source's positions with the stored rows' across
/// commits, so every other stream records that the engine made its table's sequences. A history
/// table holds every version of each key, which only a history stream keeps.
///
/// # Errors
///
/// A change merge into a table whose rows the engine sequenced, or that a load created before
/// state recorded its sequences, is `table_sequences_mismatch`, a Config error: the source's
/// positions cannot order those rows, and merging would leave them stale. A history stream into a
/// table created without history, or any other stream into a history table, is
/// `table_history_mismatch`: the table's rows have no versions to close, or a merge would take
/// its versions for one row.
pub(super) fn to_record(
    plan: &StreamPlan,
    recorded: Option<&TableState>,
) -> Result<Option<(Sequences, bool)>, Error> {
    let written = if plan.merges_changes() {
        Sequences::Source
    } else {
        Sequences::Engine
    };
    let history = plan.keeps_history();
    let held = recorded.and_then(|table| table.sequences);
    let kept = recorded.is_some_and(|table| table.history);
    let created = recorded.is_some_and(|table| table.schema.is_some());
    let name = plan.name();
    let refuse = |code: &str, detail: &str| {
        Error::config(format!("stream {name}: {detail}"))
            .with_code(code)
            .with_stream(name)
    };
    if written == Sequences::Source && created && held != Some(Sequences::Source) {
        return Err(refuse(
            "table_sequences_mismatch",
            "its table holds rows the engine merged, which a change stream's positions cannot \
             order; load the change stream into a table of its own",
        ));
    }
    if created && kept != history {
        let detail = if history {
            "its table was created without history, whose rows have no versions to close; keep \
             the stream's history in a table of its own"
        } else {
            "its table keeps the history of another stream, whose versions only a history stream \
             keeps"
        };
        return Err(refuse("table_history_mismatch", detail));
    }
    Ok((held != Some(written) || kept != history).then_some((written, history)))
}
