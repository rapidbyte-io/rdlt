//! Arrow values into SQLite rows, and SQLite tables back into Arrow batches.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::sqlgen::{SqlDialect, SqlValue, Statement};
use rdlt_connector::{ConnectorError, Result};
use rusqlite::Connection;
use rusqlite::types::Value;

use super::database::{columns, failed};

/// Runs `statement` once for each row of `batch`, binding the row's values after the statement's
/// own parameters.
pub(super) fn stage(
    connection: &Connection,
    statement: &Statement,
    batch: &RecordBatch,
) -> Result<()> {
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(column, field)| cells(column).map_err(|error| error.in_column(field.name())))
        .collect::<Result<Vec<_>>>()?;
    let mut prepared = connection
        .prepare_cached(&statement.sql)
        .map_err(failed("preparing to stage rows"))?;
    let fixed: Vec<Value> = statement.params.iter().map(fixed).collect();
    for row in 0..batch.num_rows() {
        let values = fixed
            .iter()
            .cloned()
            .chain(columns.iter().map(|column| column[row].clone()));
        prepared
            .execute(rusqlite::params_from_iter(values))
            .map_err(failed("staging a row"))?;
    }
    Ok(())
}

fn fixed(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(integer) => Value::Integer(*integer),
        SqlValue::Text(text) => Value::Text(text.clone()),
        SqlValue::Blob(blob) => Value::Blob(blob.clone()),
    }
}

/// A type the destination cannot store, in the column it was found in.
struct Unstorable(DataType);

impl Unstorable {
    fn in_column(self, column: &str) -> ConnectorError {
        ConnectorError::data(format!(
            "column {column} is {:?}, which the sqlite destination does not store",
            self.0
        ))
    }
}

/// The values of `array`, one per row.
fn cells(array: &ArrayRef) -> std::result::Result<Vec<Value>, Unstorable> {
    let integers = |values: Vec<Option<i64>>| {
        values
            .into_iter()
            .map(|value| value.map_or(Value::Null, Value::Integer))
            .collect()
    };
    let reals = |values: Vec<Option<f64>>| {
        values
            .into_iter()
            .map(|value| value.map_or(Value::Null, Value::Real))
            .collect()
    };
    let values = match array.data_type() {
        // A column the engine sends as a dictionary is stored as the values it encodes.
        DataType::Dictionary(_, value) => {
            let decoded = arrow_cast::cast(array, value)
                .map_err(|_| Unstorable(array.data_type().clone()))?;
            return cells(&decoded);
        }
        DataType::Null => vec![Value::Null; array.len()],
        DataType::Boolean => integers(
            array
                .as_boolean()
                .iter()
                .map(|v| v.map(i64::from))
                .collect(),
        ),
        DataType::Int8 => integers(widen::<Int8Type>(array)),
        DataType::Int16 => integers(widen::<Int16Type>(array)),
        DataType::Int32 => integers(widen::<Int32Type>(array)),
        DataType::Int64 => integers(widen::<Int64Type>(array)),
        DataType::UInt8 => integers(widen::<UInt8Type>(array)),
        DataType::UInt16 => integers(widen::<UInt16Type>(array)),
        DataType::UInt32 => integers(widen::<UInt32Type>(array)),
        DataType::Float32 => reals(
            array
                .as_primitive::<Float32Type>()
                .iter()
                .map(|v| v.map(f64::from))
                .collect(),
        ),
        DataType::Float64 => reals(array.as_primitive::<Float64Type>().iter().collect()),
        DataType::Utf8 => texts(array.as_string::<i32>().iter()),
        DataType::LargeUtf8 => texts(array.as_string::<i64>().iter()),
        DataType::Binary => blobs(array.as_binary::<i32>().iter()),
        DataType::LargeBinary => blobs(array.as_binary::<i64>().iter()),
        DataType::FixedSizeBinary(_) => blobs(array.as_fixed_size_binary().iter()),
        other => return Err(Unstorable(other.clone())),
    };
    Ok(values)
}

fn widen<T>(array: &ArrayRef) -> Vec<Option<i64>>
where
    T: arrow_array::ArrowPrimitiveType,
    T::Native: Into<i64>,
{
    array
        .as_primitive::<T>()
        .iter()
        .map(|value| value.map(Into::into))
        .collect()
}

fn texts<'a>(values: impl Iterator<Item = Option<&'a str>>) -> Vec<Value> {
    values
        .map(|value| value.map_or(Value::Null, |text| Value::Text(text.to_owned())))
        .collect()
}

fn blobs<'a>(values: impl Iterator<Item = Option<&'a [u8]>>) -> Vec<Value> {
    values
        .map(|value| value.map_or(Value::Null, |blob| Value::Blob(blob.to_vec())))
        .collect()
}

/// Every row of `table` as one batch, each column as its storage class; none when the table is
/// missing.
pub(super) fn read_table(
    connection: &Connection,
    dialect: &impl SqlDialect,
    table: &str,
) -> Result<Vec<RecordBatch>> {
    let columns = columns(connection, dialect, table)?;
    if columns.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!("SELECT * FROM {}", dialect.quote(table));
    let mut prepared = connection
        .prepare(&sql)
        .map_err(failed("reading a table"))?;
    let rows: Vec<Vec<Value>> = prepared
        .query_map([], |row| {
            (0..columns.len()).map(|index| row.get(index)).collect()
        })
        .map_err(failed("reading a table"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(failed("reading a table's rows"))?;
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (index, column) in columns.iter().enumerate() {
        let values = rows.iter().map(|row| &row[index]);
        let (data_type, array): (DataType, ArrayRef) = match column.declared.as_str() {
            "BOOLEAN" => (
                DataType::Boolean,
                Arc::new(
                    values
                        .map(|value| integer(value).map(|v| v != 0))
                        .collect::<BooleanArray>(),
                ),
            ),
            "INTEGER" => (
                DataType::Int64,
                Arc::new(values.map(integer).collect::<Int64Array>()),
            ),
            "REAL" => (
                DataType::Float64,
                Arc::new(values.map(real).collect::<Float64Array>()),
            ),
            "BLOB" => (
                DataType::Binary,
                Arc::new(values.map(blob).collect::<BinaryArray>()),
            ),
            _ => (
                DataType::Utf8,
                Arc::new(values.map(text).collect::<StringArray>()),
            ),
        };
        fields.push(Field::new(&column.name, data_type, true));
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| ConnectorError::internal(format!("reading table {table}: {error}")))?;
    Ok(vec![batch])
}

fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(integer) => Some(*integer),
        _ => None,
    }
}

fn real(value: &Value) -> Option<f64> {
    match value {
        Value::Real(real) => Some(*real),
        #[expect(
            clippy::cast_precision_loss,
            reason = "SQLite keeps integral reals as integers"
        )]
        Value::Integer(integer) => Some(*integer as f64),
        _ => None,
    }
}

fn text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(text) => Some(text),
        _ => None,
    }
}

fn blob(value: &Value) -> Option<&[u8]> {
    match value {
        Value::Blob(blob) => Some(blob),
        _ => None,
    }
}
