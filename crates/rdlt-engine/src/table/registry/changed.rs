//! The changes that take a table from one view to the next, as the destination applies them
//! and as the run tells of them.

use rdlt_connector::{ColumnKey, Field, LogicalType, TableChange};

use super::super::resolve::Change;
use super::TableView;
use crate::error::shown;
use crate::event::{ColumnChange, EventKind, Listed};

/// The event telling that the destination applied `changes`, which made `next`: a table it
/// created lists its columns as added.
///
/// Only the columns the event names are shown.
pub(super) fn changed(next: &TableView, changes: &[TableChange]) -> EventKind {
    let named = changes.iter().flat_map(|change| {
        let (added, widened): (&[Field], _) = match change {
            TableChange::Create { .. } => (&next.physical[..next.model.columns.len()], None),
            TableChange::AddColumn { field, .. } => (std::slice::from_ref(field), None),
            TableChange::Widen {
                column, from, to, ..
            } => {
                let column = &**column;
                (&[], Some(Named::Widened { column, from, to }))
            }
        };
        added.iter().map(Named::Added).chain(widened)
    });
    let shown_change = |named: Named<'_>| match named {
        Named::Added(field) => ColumnChange::Added {
            column: shown(field.name()),
            logical_type: shown(field.logical_type()),
            variant: matches!(
                next.model.names.owner(field.name()),
                Some(ColumnKey::Variant { .. })
            ),
        },
        Named::Widened { column, from, to } => ColumnChange::Widened {
            column: shown(column),
            from: shown(from),
            to: shown(to),
        },
    };
    EventKind::SchemaChanged {
        table: shown(&next.table.name),
        version: next.model.version,
        changes: Listed::first(named).map(shown_change),
    }
}

/// A column a change names, before the event shows it.
enum Named<'a> {
    Added(&'a Field),
    Widened {
        column: &'a str,
        from: &'a LogicalType,
        to: &'a LogicalType,
    },
}

/// The destination changes that take `before` to `after`: the whole table for its first version,
/// then each added column and each widening that changes how the destination stores a column.
pub(super) fn table_changes(
    before: &TableView,
    after: &TableView,
    changes: &[Change],
) -> Vec<TableChange> {
    let table = after.table.clone();
    if !before.model.created() {
        return vec![TableChange::Create {
            table,
            schema: after.physical_schema(),
        }];
    }
    let positions = after.model.positions();
    changes
        .iter()
        .filter_map(|change| match change {
            Change::Add { key } => {
                let index = *positions.get(after.model.names.get(key)?)?;
                Some(TableChange::AddColumn {
                    table: table.clone(),
                    field: after.physical[index].clone(),
                })
            }
            Change::Widen { column, .. } => {
                let (from, to) = (&before.lowered[*column], &after.lowered[*column]);
                (from != to).then(|| TableChange::Widen {
                    table: table.clone(),
                    column: after.model.columns[*column].name().into(),
                    from: from.clone(),
                    to: to.clone(),
                })
            }
        })
        .collect()
}
