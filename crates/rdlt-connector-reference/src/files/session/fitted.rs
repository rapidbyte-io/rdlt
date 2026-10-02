//! What a table must hold before a writer's rows or a schema change are taken.

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::{SegmentId, TableSchema};

use super::Location;
use crate::files::manifest;
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

/// Refuses `change`, which takes its table's schema from `current` to `next`, where a column
/// changes its type and a row the pipeline publishes of the table, keeps in a generation or
/// among its tombstones, does not fit the type the column would take.
///
/// Such a change is a `Data` error coded `schema_conflict`, and leaves the table as it was: a
/// table that took it would merge no more. A change of no column's type reads no file.
pub(super) fn fits(
    location: &Location,
    change: &TableChange,
    current: &TableSchema,
    next: &TableSchema,
) -> Result<()> {
    let (held, taken) = (Arc::new(current.to_arrow()), Arc::new(next.to_arrow()));
    let retyped = held.fields().iter().any(|field| {
        let now = taken.field_with_name(field.name());
        now.is_ok_and(|now| now.data_type() != field.data_type())
    });
    let name = &change.table().name;
    let table = match manifest::latest(&location.dir)? {
        Some(mut manifest) if retyped => manifest.tables.remove(&**name),
        _ => None,
    };
    let Some(table) = table else {
        return Ok(());
    };
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
    if let Some(key) = &change.table().merge {
        let buried = merge::tombstone_schema(&held, key)
            .map_err(|error| merge::failed("reading tombstones", &error))?;
        for file in &table.tombstones {
            let rows = manifest::read(&location.dir, &file.path, &buried)?;
            merge::holds(&rows, &taken).map_err(conflict)?;
        }
    }
    Ok(())
}
