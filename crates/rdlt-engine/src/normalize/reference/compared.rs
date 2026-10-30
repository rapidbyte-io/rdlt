//! The Arrow normalizer's tables as the reference writes them, each row known by following its
//! lineage, and the comparison of the two.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, FieldRef};
use rdlt_connector::{Epoch, SegmentId};
use serde_json::Value as Json;

use super::{Record, Row, Tables, canonical_text, json, normalize, sorted, step};
use crate::normalize::{Part, PartSchemas, Received, Shape};

/// The tables `pieces` normalize into, each a batch of the stream's rows in order normalized as
/// `shape` and received where its stamp says, the schemas of their parts kept from piece to piece
/// as a partition keeps them, as the reference writes them, each row known by following its
/// lineage, as [`walked`] follows it.
///
/// # Errors
///
/// Why a piece does not normalize, or [`walked`] refuses its parts.
pub(crate) fn tables_of(
    pieces: &[(RecordBatch, Received)],
    shape: &Shape,
) -> Result<Tables, Disagreement> {
    let mut normalized = Vec::with_capacity(pieces.len());
    let mut schemas = PartSchemas::default();
    for (batch, received) in pieces {
        let parts = crate::normalize::normalize(batch, shape, &mut schemas);
        let parts = parts.map_err(Disagreement::from)?;
        normalized.push((parts, *received));
    }
    walked(&normalized, !shape.key.is_empty())
}

/// The tables of `pieces`' parts, each piece's received where its stamp says, as the reference
/// writes them, each row known by following its lineage: its parent's id to the row it names, in
/// a table its own extends, and its root's id to the root row it comes from; a root's key is its
/// id where `keyed`.
///
/// # Errors
///
/// Where an id names two rows, or a row's lineage names no row, or one it does not come from. A
/// merge stream's roots are known by their keys, which must be distinct.
pub(crate) fn walked(
    pieces: &[(Vec<Part>, Received)],
    keyed: bool,
) -> Result<Tables, Disagreement> {
    let mut named: BTreeMap<Vec<u8>, (Vec<String>, String)> = BTreeMap::new();
    let mut tables = Tables::new();
    let mut first = 0;
    for (parts, received) in pieces {
        for part in parts {
            let table: Vec<String> = part.path.iter().map(ToString::to_string).collect();
            let (ids, parent) = part.lineage.columns(received)?;
            let ids = ids.as_binary::<i32>();
            for row in 0..part.batch.num_rows() {
                let at = match &parent {
                    None => format!("#{}", first + row),
                    Some([parents, roots, idx]) => {
                        let found = |ids: &ArrayRef, what: &str| {
                            named.get(ids.as_binary::<i32>().value(row)).ok_or_else(|| {
                                Disagreement(format!("a row of {table:?} names no {what}"))
                            })
                        };
                        let (parent_table, parent_at) = found(parents, "parent")?;
                        let (root_table, root_at) = found(roots, "root")?;
                        let extends =
                            table.len() > parent_table.len() && table.starts_with(parent_table);
                        let position = idx.as_primitive::<Int32Type>().value(row);
                        let at = step(parent_at, &table, position);
                        let rooted = at.starts_with(&format!("{root_at}/"));
                        if !extends || !root_table.is_empty() || !rooted {
                            let named = format!("{at} names {parent_at} and {root_at}");
                            return Err(Disagreement(named));
                        }
                        at
                    }
                };
                let id = ids.value(row).to_vec();
                let key = (keyed && table.is_empty()).then(|| id.clone());
                if named.insert(id, (table.clone(), at.clone())).is_some() {
                    return Err(Disagreement(format!(
                        "the id of {at} names another row too"
                    )));
                }
                let columns = columns_of(part, row)?;
                tables
                    .entry(table.clone())
                    .or_default()
                    .push(Row { at, key, columns });
            }
        }
        first += parts.first().map_or(0, |root| root.batch.num_rows());
    }
    Ok(sorted(tables))
}

/// The non-null columns of `part`'s row `row`, as the reference writes them.
fn columns_of(part: &Part, row: usize) -> Result<BTreeMap<Vec<String>, String>, Disagreement> {
    let schema = part.batch.schema();
    let mut columns = BTreeMap::new();
    for (path, (field, column)) in part
        .columns
        .iter()
        .zip(schema.fields().iter().zip(part.batch.columns()))
    {
        let value = json_of(field, column, row)?;
        if !value.is_null() {
            let path = path.segments().map(str::to_owned).collect();
            columns.insert(path, canonical_text(&value));
        }
    }
    Ok(columns)
}

/// The records `batch`'s rows are: each an object of its columns' values.
///
/// # Errors
///
/// Where a column is of a type with no JSON form here.
fn records_of(batch: &RecordBatch) -> Result<Vec<Json>, Disagreement> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|row| {
            let fields = schema.fields().iter().zip(batch.columns());
            let object = fields
                .map(|(field, column)| Ok((field.name().clone(), json_of(field, column, row)?)))
                .collect::<Result<_, Disagreement>>()?;
            Ok(Json::Object(object))
        })
        .collect()
}

/// The value at `row` of `array`, of `field`, as JSON: objects without their null fields, a float
/// JSON cannot hold as null, and a column of JSON, at any depth, as the value its text holds.
fn json_of(field: &FieldRef, array: &ArrayRef, row: usize) -> Result<Json, Disagreement> {
    if array.is_null(row) {
        return Ok(Json::Null);
    }
    Ok(match array.data_type() {
        DataType::Null => Json::Null,
        DataType::Boolean => Json::from(array.as_boolean().value(row)),
        DataType::Int64 => Json::from(array.as_primitive::<Int64Type>().value(row)),
        DataType::Float64 => {
            let value = array.as_primitive::<Float64Type>().value(row);
            serde_json::Number::from_f64(value).map_or(Json::Null, Json::Number)
        }
        DataType::Utf8 if json(Some(field)) => {
            let text = array.as_string::<i32>().value(row);
            serde_json::from_str(text).map_err(|error| Disagreement(error.to_string()))?
        }
        DataType::Utf8 => Json::from(array.as_string::<i32>().value(row)),
        DataType::Struct(fields) => {
            let object = array.as_struct();
            let mut members = serde_json::Map::new();
            for (field, column) in fields.iter().zip(object.columns()) {
                let value = json_of(field, column, row)?;
                if !value.is_null() {
                    members.insert(field.name().clone(), value);
                }
            }
            Json::Object(members)
        }
        DataType::List(item) => {
            let items = array.as_list::<i32>().value(row);
            let items = (0..items.len()).map(|at| json_of(item, &items, at));
            Json::Array(items.collect::<Result<_, _>>()?)
        }
        other => return Err(Disagreement(format!("no JSON form for {other}"))),
    })
}

/// Why normalizing and the reference disagree, or one could not be read.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
#[cfg_attr(
    not(feature = "bench"),
    expect(
        unreachable_pub,
        reason = "the fuzz targets reach it through the bench feature"
    )
)]
pub struct Disagreement(String);

impl From<arrow_schema::ArrowError> for Disagreement {
    fn from(error: arrow_schema::ArrowError) -> Self {
        Self(error.to_string())
    }
}

/// Where a piece of a stream's rows begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    not(feature = "bench"),
    expect(
        unreachable_pub,
        reason = "the fuzz targets reach it through the bench feature"
    )
)]
pub enum Cut {
    /// After the piece before it, in its segment.
    Piece,
    /// In a new segment of the session.
    Segment,
    /// In a new session's first segment.
    Session,
}

/// Normalizes `pieces`, one stream's rows in order, each beginning where its cut says, as a
/// stream normalized to `max_depth` and merged by `key` where it names a column, and compares
/// the tables they make with what the reference normalizer makes of the rows they hold.
///
/// # Errors
///
/// Why they differ: a row one holds and the other does not, a row's lineage naming no row or one
/// it does not come from, an id naming two rows, or a piece that does not normalize or holds a
/// value of a type the comparison reads no JSON from. A merge's keys must be distinct.
#[cfg_attr(
    not(feature = "bench"),
    expect(
        unreachable_pub,
        reason = "the fuzz targets reach it through the bench feature"
    )
)]
pub fn normalized_against_reference(
    pieces: &[(RecordBatch, Cut)],
    max_depth: u8,
    key: Option<&str>,
) -> Result<(), Disagreement> {
    let shape = Shape {
        max_depth,
        whole: BTreeSet::new(),
        key: key.map(Arc::from).into_iter().collect(),
    };
    let actual = tables_of(&stamped(pieces), &shape)?;
    let records = pieces
        .iter()
        .map(|(batch, _)| records_of(batch))
        .collect::<Result<Vec<_>, _>>()?;
    let held: Vec<Record<'_>> = pieces
        .iter()
        .zip(&records)
        .flat_map(|((batch, _), records)| {
            let fields = batch.schema_ref().fields();
            records.iter().map(move |value| Record { value, fields })
        })
        .collect();
    let key: Vec<&str> = key.into_iter().collect();
    let expected = normalize(&held, max_depth, &key, &[]);
    if actual == expected {
        Ok(())
    } else {
        Err(Disagreement(format!(
            "normalizing made {actual:?}, the reference {expected:?}"
        )))
    }
}

/// Each of `pieces`, received where its cut says: the first in segment 1 of epoch 1.
pub(crate) fn stamped(pieces: &[(RecordBatch, Cut)]) -> Vec<(RecordBatch, Received)> {
    let mut received = Received {
        epoch: Epoch(1),
        segment: SegmentId(1),
        first_row: 0,
    };
    let mut stamped = Vec::with_capacity(pieces.len());
    for (batch, cut) in pieces {
        match cut {
            Cut::Piece => {}
            Cut::Segment => (received.segment.0, received.first_row) = (received.segment.0 + 1, 0),
            Cut::Session => {
                received.epoch.0 += 1;
                (received.segment, received.first_row) = (SegmentId(1), 0);
            }
        }
        stamped.push((batch.clone(), received));
        received.first_row += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    }
    stamped
}
