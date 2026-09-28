//! Who made the sequences of a stream's table, and whether its load may compare them (ADR 0023).

#[cfg(test)]
mod tests;

use rdlt_connector::{Sequences, TableState};

use crate::error::Error;
use crate::plan::StreamPlan;

/// The sequences state must record for `plan`'s table, which state records as `recorded`, before
/// its load; `None` when state records them already.
///
/// A change stream merged by key compares its source's positions with the stored rows' across
/// commits, so every other stream records that the engine made its table's sequences.
///
/// # Errors
///
/// A change merge into a table whose rows the engine sequenced, or that a load created before
/// state recorded its sequences, is `table_sequences_mismatch`, a Config error: the source's
/// positions cannot order those rows, and merging would leave them stale.
pub(super) fn to_record(
    plan: &StreamPlan,
    recorded: Option<&TableState>,
) -> Result<Option<Sequences>, Error> {
    let written = if plan.merges_changes() {
        Sequences::Source
    } else {
        Sequences::Engine
    };
    let held = recorded.and_then(|table| table.sequences);
    let created = recorded.is_some_and(|table| table.schema.is_some());
    if written == Sequences::Source && created && held != Some(Sequences::Source) {
        let name = plan.name();
        return Err(Error::config(format!(
            "stream {name}: its table holds rows the engine merged, which a change stream's \
             positions cannot order; load the change stream into a table of its own"
        ))
        .with_code("table_sequences_mismatch")
        .with_stream(name));
    }
    Ok((held != Some(written)).then_some(written))
}
