//! What a stream's policies discard from a row: its values, or the row and the child rows it
//! would have added, where the pushes the engine shreds together may decide it.

use rdlt_engine::SchemaPolicy;

use super::{changed, child_rows, counted, drift_type, node, policy};
use crate::workload::{Drift, Row, SimStream};

/// Whether a policy discards something, where the pushes the engine shreds together decide it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::oracle) enum Chance {
    /// It does not.
    Never,
    /// It does in some ways the engine may gather pushes, and not in others.
    Perhaps,
    /// It does.
    Surely,
}

/// The rows and values a policy discards from a row, each as the least and most it may be.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::oracle) struct Discards {
    pub(in crate::oracle) rows: (u64, u64),
    pub(in crate::oracle) values: (u64, u64),
}

/// Whether `stream`'s policies drop `row`: a column that lands and discards rows changes in it
/// with a value.
pub(in crate::oracle) fn dropped(stream: &SimStream, row: &Row) -> Chance {
    landing(stream)
        .filter(|(column, _)| policy(stream, *column) == SchemaPolicy::DiscardRow)
        .filter_map(|(column, drift)| {
            let extra = row.extras[column].as_ref()?;
            let node = node(stream, drift_type(row, drift), extra);
            let max_depth = stream.max_depth().filter(|_| !stream.whole(column));
            (counted(vec![(drift.name.clone(), node)], max_depth) > 0)
                .then(|| changed(stream, row, column))
        })
        .max()
        .unwrap_or(Chance::Never)
}

/// Whether `row` goes before its batch's schema is resolved: in a stream that normalizes, it holds
/// an item of a new array whose column drops rows, and the child table that would take the item
/// drops its parent row with it.
pub(in crate::oracle) fn pruned(stream: &SimStream, row: &Row) -> bool {
    let Some(max_depth) = stream.max_depth() else {
        return false;
    };
    landing(stream).any(|(column, drift)| {
        let Some(extra) = &row.extras[column] else {
            return false;
        };
        policy(stream, column) == SchemaPolicy::DiscardRow
            && !stream.whole(column)
            && child_rows(
                &drift.name,
                node(stream, drift_type(row, drift), extra),
                max_depth,
            ) > 0
    })
}

/// The rows and values `stream`'s policies discard from `row`, each as the least and most it may
/// be: none where its filter drops it, before any policy meets it.
///
/// Where a column that discards rows changes in it with a value, that is the row, and the rows its
/// values in child tables that take them would have added; otherwise each value of a column that
/// discards values and changes in it. Where the stream normalizes, a value counts each value
/// column and array item it normalizes into, so an empty array or an object of nulls counts none.
pub(in crate::oracle) fn discards(stream: &SimStream, row: &Row) -> Discards {
    if !stream.reshaping.keeps(row) {
        return Discards::default();
    }
    let (mut surely, mut perhaps) = (0, 0);
    for (column, drift) in landing(stream) {
        let Some(extra) = &row.extras[column] else {
            continue;
        };
        if policy(stream, column) != SchemaPolicy::DiscardValue {
            continue;
        }
        let node = node(stream, drift_type(row, drift), extra);
        let max_depth = stream.max_depth().filter(|_| !stream.whole(column));
        let count = counted(vec![(drift.name.clone(), node)], max_depth);
        match changed(stream, row, column) {
            Chance::Surely => surely += count,
            Chance::Perhaps => perhaps += count,
            Chance::Never => {}
        }
    }
    let rows = 1 + children(stream, row);
    match dropped(stream, row) {
        Chance::Never => Discards {
            rows: (0, 0),
            values: (surely, surely + perhaps),
        },
        Chance::Perhaps => Discards {
            rows: (0, rows),
            values: (0, surely + perhaps),
        },
        Chance::Surely => Discards {
            rows: (rows, rows),
            values: (0, 0),
        },
    }
}

/// The rows `row`'s values would add to the child tables that take them.
fn children(stream: &SimStream, row: &Row) -> u64 {
    landing(stream)
        .filter(|(column, _)| {
            !matches!(
                policy(stream, *column),
                SchemaPolicy::DiscardRow | SchemaPolicy::DiscardValue
            ) && !stream.whole(*column)
        })
        .filter_map(|(column, drift)| {
            let extra = row.extras[column].as_ref()?;
            let max_depth = stream.max_depth()?;
            let node = node(stream, drift_type(row, drift), extra);
            Some(child_rows(&drift.name, node, max_depth))
        })
        .sum()
}

/// `stream`'s drift columns that land, by their places: none its allow-list leaves out reaches a
/// policy.
pub(super) fn landing(stream: &SimStream) -> impl Iterator<Item = (usize, &Drift)> {
    let drift = stream.drift.iter().enumerate();
    drift.filter(|(_, drift)| stream.lands(&drift.name))
}
