//! A batch's model columns, converted to the types the table holds and lowered as it stores
//! them.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, new_null_array};
use rdlt_connector::{Field, LogicalType, StreamName};

use super::split::Fitted;
use super::unheld::Converted;
use super::{LoweringPlan, Source, lower_array};
use crate::error::Error;
use crate::table::convert::convert;

impl LoweringPlan {
    /// The model's columns of `batch`, each from where the plan routes it, converted and lowered
    /// as its column stores it; a column the batch lacks is null.
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
        for ((column, lowered), source) in view
            .model
            .columns
            .iter()
            .zip(&view.lowered)
            .zip(&self.sources)
        {
            let converted = match source {
                Source::Incoming(index, from) if !source.is_null() => {
                    match converted.iter().find(|(converted, _)| converted == index) {
                        Some((_, converted)) => Arc::clone(converted),
                        None => held_as(&self.stream, batch.column(*index), from, column)?,
                    }
                }
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
                    let rows = batch.num_rows();
                    columns.push(new_null_array(&lowered.to_arrow(), rows));
                    if let Some(held) = &mut held {
                        held.push(new_null_array(&arrow_schema::DataType::Null, rows));
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
        .with_code("value_unrepresentable")
        .with_stream(stream)
}
