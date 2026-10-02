//! What a table must hold before a writer's rows or a schema change are taken.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{ConnectorErrorKind, SegmentId, TableSchema};

use super::{Location, Shared};
use crate::columns::changed;
use crate::files::manifest::{self, Listed};
use crate::files::tables;
use crate::merge;

/// Refuses the `buffered` batches of `table` where it is a merge table and one of them holds a
/// row its merge cannot take, under the code the merge would refuse it with, so nothing of a
/// flush is staged that its commit would fail on.
pub(super) fn admitted(
    location: &Location,
    table: &TableRef,
    buffered: &[(SegmentId, RecordBatch)],
) -> Result<()> {
    let Some(key) = &table.merge else {
        return Ok(());
    };
    let stored = tables::read(&location.rdlt, &table.name)?;
    let stored = stored.map(|schema| Arc::new(schema.to_arrow()));
    for (_, batch) in buffered {
        merge::admitted(batch, stored.as_ref(), key)
            .map_err(|error| merge::failed("staging rows", &error))?;
    }
    Ok(())
}

/// The code of an error for a table that changed while its rows were checked against a change.
pub(super) const TABLE_CHANGED: &str = "table_changed";

/// What a change of a table's schema was checked against: nothing, where it changes no column's
/// type, or else the schema, the manifest and the staged files the table had.
#[derive(Debug, Default)]
pub(super) struct Checked {
    held: Option<(TableSchema, Option<u64>, usize)>,
}

/// Whether a column of `current` has another type in `next`.
fn retyped(current: &TableSchema, next: &TableSchema) -> bool {
    let (held, taken) = (current.to_arrow(), next.to_arrow());
    held.fields().iter().any(|field| {
        let now = taken.field_with_name(field.name());
        now.is_ok_and(|now| now.data_type() != field.data_type())
    })
}

/// The files of `table` the session staged and has not committed.
fn staged(shared: &Mutex<Shared>, table: &str) -> Vec<Listed> {
    let shared = shared.lock();
    let of_table = shared
        .staged
        .iter()
        .filter(|file| *file.table.name == *table);
    of_table.map(|file| file.file.clone()).collect()
}

/// Checks `change` against what its table holds, where it changes a column's type: every row
/// the pipeline publishes of the table, keeps in a generation or among its tombstones, or the
/// session staged for it, must fit the type its column would take.
///
/// A change one of them does not fit is a `Data` error coded `schema_conflict`, and leaves the
/// table as it was: a table that took it would merge no more. A change of no column's type
/// reads no file.
///
/// The table's lock is not held here, since this reads the whole table: the change is taken
/// under the lock only where [`Checked::stands`] finds the table as it was checked.
pub(super) fn fits(
    location: &Location,
    shared: &Mutex<Shared>,
    change: &TableChange,
) -> Result<Checked> {
    let name = &change.table().name;
    let Some(current) = tables::read(&location.rdlt, name)? else {
        return Ok(Checked::default());
    };
    // A change the schema refuses is refused where it is taken.
    let next = match changed(Some(&current), change) {
        Ok(next) if retyped(&current, &next) => next,
        _ => return Ok(Checked::default()),
    };
    let (held, taken) = (Arc::new(current.to_arrow()), Arc::new(next.to_arrow()));
    let manifest = manifest::latest(&location.dir)?;
    let version = manifest.as_ref().map(|manifest| manifest.version);
    let table = manifest.and_then(|mut manifest| manifest.tables.remove(&**name));
    let table = table.unwrap_or_default();
    let conflict = |error: arrow_schema::ArrowError| {
        ConnectorError::data(format!(
            "table {name} holds a row the change does not fit: {error}"
        ))
        .with_code("schema_conflict")
    };
    for file in table
        .files
        .iter()
        .chain(table.generations.values().flatten())
    {
        let rows = manifest::read_held(&location.dir, &file.path, &held)?;
        merge::holds(&rows, &taken).map_err(conflict)?;
    }
    let staged = staged(shared, name);
    for file in &staged {
        let rows = manifest::read(&location.dir, &file.path, &held)?;
        merge::holds(&rows, &taken).map_err(conflict)?;
    }
    if let Some(key) = &change.table().merge {
        // A tombstone holds its key under the table's types and its sequence as it compares.
        let reading = |error| merge::failed("reading tombstones", &error);
        let buried = merge::tombstone_schema(&held, key).map_err(reading)?;
        let kept = merge::tombstone_schema(&taken, key).map_err(reading)?;
        for file in &table.tombstones {
            let rows = manifest::read(&location.dir, &file.path, &buried)?;
            merge::holds(&rows, &kept).map_err(conflict)?;
        }
    }
    Ok(Checked {
        held: Some((current, version, staged.len())),
    })
}

impl Checked {
    /// Checks, under the table's lock, that a change taking the table's schema from `current`
    /// to `next` is one this check covers: it changes no column's type, or the table is as it
    /// was when its rows were read.
    ///
    /// A table that changed since is a `Transient` error coded `table_changed`.
    pub(super) fn stands(
        &self,
        location: &Location,
        shared: &Mutex<Shared>,
        (name, current, next): (&str, &TableSchema, &TableSchema),
    ) -> Result<()> {
        if !retyped(current, next) {
            return Ok(());
        }
        let version = manifest::latest(&location.dir)?.map(|manifest| manifest.version);
        let found = (current, version, staged(shared, name).len());
        let checked = self.held.as_ref();
        if checked.is_some_and(|(schema, version, staged)| (schema, *version, *staged) == found) {
            return Ok(());
        }
        let message = format!("table {name} changed while its rows were checked against a change");
        Err(ConnectorError::new(ConnectorErrorKind::Transient, message).with_code(TABLE_CHANGED))
    }
}
