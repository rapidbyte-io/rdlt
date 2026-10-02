//! How a stream's table sequences and matches its rows, and whether its load may go on doing so
//! (ADR 0023, ADR 0042).

#[cfg(test)]
mod tests;

use rdlt_connector::{ColumnPath, Sequences, TableState};

use crate::error::Error;
use crate::plan::StreamPlan;

/// What state records of how a table's rows are sequenced and matched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Keying {
    /// Who made the sequences of the rows the table holds.
    pub(crate) sequences: Sequences,
    /// Whether the table keeps every version of each key.
    pub(crate) history: bool,
    /// The columns the table's rows were merged by; none for a table never merged.
    pub(crate) key: Vec<ColumnPath>,
    /// The column a history table's versions begin at.
    pub(crate) change_time: Option<ColumnPath>,
}

/// What state must record of how `plan`'s table sequences and matches its rows, which state
/// records as `recorded`, before its load merges by `key` and, keeping history, begins versions at
/// `change_time`; `None` when state records it already.
///
/// A table merged by a key keeps it across loads that merge nothing, so a later merge is held to
/// the key its rows hold.
///
/// # Errors
///
/// All are Config errors, refused before the table changes:
/// - A change merge into a table whose rows the engine sequenced, or that a load created before
///   state recorded its sequences, is `table_sequences_mismatch`: the source's positions cannot
///   order those rows, and merging would leave them stale.
/// - A history stream into a table created without history, or any other stream into a history
///   table, is `table_history_mismatch`: the table's rows have no versions to close, or a merge
///   would take its versions for one row.
/// - A merge by a key other than the key the table's rows were merged by is
///   `table_key_mismatch`: it would replace rows of another key, and keep rows of its own twice.
/// - A history stream whose versions begin at another column than the table's did, or at none
///   where they did at one, is `table_change_time_mismatch`: every row would hash anew, open a
///   version, and close one at a time of another basis.
pub(super) fn to_record(
    plan: &StreamPlan,
    recorded: Option<&TableState>,
    key: &[ColumnPath],
    change_time: Option<&ColumnPath>,
) -> Result<Option<Keying>, Error> {
    let history = plan.keeps_history();
    let keying = Keying {
        sequences: if plan.merges_changes() {
            Sequences::Source
        } else {
            Sequences::Engine
        },
        history,
        key: key.to_vec(),
        change_time: change_time.filter(|_| history).cloned(),
    };
    let Some(table) = recorded else {
        return Ok(Some(keying));
    };
    if table.schema.is_some() {
        refuse_change(plan, table, &keying)?;
    }
    let keying = Keying {
        key: if keying.key.is_empty() {
            table.key.clone()
        } else {
            keying.key
        },
        ..keying
    };
    let unchanged = table.sequences == Some(keying.sequences)
        && table.history == keying.history
        && table.key == keying.key
        && table.change_time == keying.change_time;
    Ok((!unchanged).then_some(keying))
}

/// Refuses to load `plan`'s stream as `keying` says into its created table, which state records
/// as `table`, where that would change how its rows are sequenced or matched.
fn refuse_change(plan: &StreamPlan, table: &TableState, keying: &Keying) -> Result<(), Error> {
    let name = plan.name();
    let refuse = |code: &str, detail: &str| {
        Err(Error::config(format!("stream {name}: {detail}"))
            .with_code(code)
            .with_stream(name))
    };
    if keying.sequences == Sequences::Source && table.sequences != Some(Sequences::Source) {
        return refuse(
            "table_sequences_mismatch",
            "its table holds rows the engine merged, which a change stream's positions cannot \
             order; load the change stream into a table of its own",
        );
    }
    if table.history != keying.history {
        let detail = if keying.history {
            "its table was created without history, whose rows have no versions to close; keep \
             the stream's history in a table of its own"
        } else {
            "its table keeps the history of another stream, whose versions only a history stream \
             keeps"
        };
        return refuse("table_history_mismatch", detail);
    }
    if !keying.key.is_empty() && !table.key.is_empty() && table.key != keying.key {
        return refuse(
            "table_key_mismatch",
            "its table's rows were merged by another key; reset the stream's tables to merge by \
             this one",
        );
    }
    if keying.history && table.change_time != keying.change_time {
        return refuse(
            "table_change_time_mismatch",
            "its table's versions began at another change time; reset the stream's tables to \
             begin them at this one",
        );
    }
    Ok(())
}
