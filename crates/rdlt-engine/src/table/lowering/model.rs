//! A batch's model columns, converted to the types the table holds and lowered as it stores
//! them.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, new_null_array};
use arrow_schema::DataType;
use rdlt_connector::{Field, LogicalType, StreamName};

use super::split::Fitted;
use super::unheld::Converted;
use super::{LoweringPlan, Source, lower_array};
use crate::error::{Error, codes};
use crate::table::convert::convert;

impl LoweringPlan {
    /// The model's columns of `batch`, each from where the plan routes it, converted and lowered
    /// as its column stores it; a column the batch lacks is null, one array of nulls a type.
    ///
    /// A history table's columns come also as the model's types hold them, which its versions
    /// are hashed by.
    pub(super) fn model_columns(
        &self,
        batch: &RecordBatch,
        fitted: &Fitted,
        converted: &Converted,
    ) -> Result<Columns, Error> {
        let view = &self.view;
        let mut columns = Vec::with_capacity(view.physical.len());
        let mut held = view.meta.history.as_ref().map(|_| Vec::new());
        let mut nulls = Nulls::new(batch.num_rows());
        for ((column, lowered), source) in view
            .model
            .columns
            .iter()
            .zip(&view.lowered)
            .zip(&self.sources)
        {
            let converted = match source {
                Source::Incoming(index, from) if !source.is_null() => match converted.get(*index) {
                    Some(converted) => Arc::clone(converted),
                    None => held_as(&self.stream, batch.column(*index), from, column)?,
                },
                Source::Read(index) => {
                    let own = fitted.own(*index, column.logical_type());
                    let own = own.map_err(|error| self.unread(column, error))?;
                    held_as(&self.stream, &own, column.logical_type(), column)?
                }
                Source::Rest(index) => {
                    let rest = fitted.rest(*index);
                    let rest = rest.map_err(|error| self.unread(column, error))?;
                    held_as(&self.stream, &rest, &LogicalType::Json, column)?
                }
                // Nulls are built as the destination stores them, never as the wider type the
                // column holds them in; a hash leaves a null column out.
                _ => {
                    columns.push(nulls.of(lowered.to_arrow()));
                    if let Some(held) = &mut held {
                        held.push(nulls.of(DataType::Null));
                    }
                    continue;
                }
            };
            columns.push(stored_as(&self.stream, &converted, column, lowered)?);
            if let Some(held) = &mut held {
                held.push(converted);
            }
        }
        Ok((columns, held))
    }
}

/// A batch's model columns as the destination stores them, and for a history table as the
/// model's types hold them.
pub(super) type Columns = (Vec<ArrayRef>, Option<Vec<ArrayRef>>);

/// Arrays of nulls as long as one batch, one of each type, which every column of that type the
/// batch holds nothing in shares.
struct Nulls {
    rows: usize,
    built: BTreeMap<DataType, ArrayRef>,
}

impl Nulls {
    fn new(rows: usize) -> Self {
        Self {
            rows,
            built: BTreeMap::new(),
        }
    }

    /// The array of nulls of `data_type`, built the first time it is asked for.
    fn of(&mut self, data_type: DataType) -> ArrayRef {
        let rows = self.rows;
        let built = self
            .built
            .entry(data_type)
            .or_insert_with_key(|data_type| new_null_array(data_type, rows));
        Arc::clone(built)
    }
}

/// `array`, of type `from`, as `column` holds it; a value the column cannot hold fails the
/// batch.
fn held_as(
    stream: &StreamName,
    array: &ArrayRef,
    from: &LogicalType,
    column: &Field,
) -> Result<ArrayRef, Error> {
    convert(array, from, column.logical_type())
        .map_err(|error| unrepresentable(stream, column, &error))
}

/// `array`, as `column` holds it, as `lowered` stores it.
fn stored_as(
    stream: &StreamName,
    array: &ArrayRef,
    column: &Field,
    lowered: &LogicalType,
) -> Result<ArrayRef, Error> {
    lower_array(array, column.logical_type(), lowered)
        .map_err(|error| unrepresentable(stream, column, &error))
}

/// The error for a value of `stream`'s batch `column` cannot hold.
fn unrepresentable(stream: &StreamName, column: &Field, error: &arrow_schema::ArrowError) -> Error {
    let detail = format!(
        "stream {stream}: column {} cannot hold a value of the batch: {error}",
        column.name()
    );
    Error::schema(detail)
        .with_code(codes::VALUE_UNREPRESENTABLE)
        .with_stream(stream)
}
