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
use rdlt_connector::sqlgen::{Column, SqlDialect, SqlValue, Statement};
use rdlt_connector::{ConnectorError, Result};
use rusqlite::Connection;
use rusqlite::types::{ToSqlOutput, Value, ValueRef};

use super::database::{columns, failed};

#[cfg(test)]
mod tests;

/// Runs `statement` once for each row of `batch`, binding the row's values after the statement's
/// own parameters, each straight from its Arrow array: only the row being bound is held.
///
/// A column of a type the destination does not store is refused before any row runs; a float
/// SQLite would store as another value, `NaN` or negative zero, where its row is bound: a `Data`
/// error coded `float_unstorable`.
pub(super) fn stage(
    connection: &Connection,
    statement: &Statement,
    batch: &RecordBatch,
) -> Result<()> {
    let schema = batch.schema();
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            let cells = Cells::of(column.as_ref())
                .map_err(|unstorable| unstorable.in_column(field.name()))?;
            Ok((field.name().as_str(), cells))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut prepared = connection
        .prepare_cached(&statement.sql)
        .map_err(failed("preparing to stage rows"))?;
    let fixed: Vec<Value> = statement.params.iter().map(fixed).collect();
    let mut bound: Vec<ToSqlOutput<'_>> = Vec::with_capacity(fixed.len() + columns.len());
    for row in 0..batch.num_rows() {
        bound.clear();
        bound.extend(
            fixed
                .iter()
                .map(|value| ToSqlOutput::Borrowed(value.into())),
        );
        for (name, cells) in &columns {
            let value = cells.value(row).map_err(|float| float.in_column(name))?;
            bound.push(ToSqlOutput::Borrowed(value));
        }
        prepared
            .execute(rusqlite::params_from_iter(bound.iter()))
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

/// A float SQLite stores as another value: `NaN` as a null, negative zero as zero.
struct Changed(f64);

impl Changed {
    fn in_column(self, column: &str) -> ConnectorError {
        ConnectorError::data(format!(
            "column {column} holds {:?}, which sqlite would store as another value",
            self.0
        ))
        .with_code("float_unstorable")
    }
}

/// One column's values, read from its array a row at a time.
enum Cells<'a> {
    /// Every value is null.
    Null,
    Integer(&'a dyn Array, Box<dyn Fn(usize) -> i64 + 'a>),
    Real(&'a dyn Array, Box<dyn Fn(usize) -> f64 + 'a>),
    Text(&'a dyn Array, Box<dyn Fn(usize) -> &'a str + 'a>),
    Blob(&'a dyn Array, Box<dyn Fn(usize) -> &'a [u8] + 'a>),
    /// A column the engine sends as a dictionary is stored as the values its keys name.
    Dictionary(&'a dyn Array, Vec<usize>, Box<Cells<'a>>),
}

impl<'a> Cells<'a> {
    /// The cells of `array`, where the destination stores its type.
    fn of(array: &'a dyn Array) -> std::result::Result<Self, Unstorable> {
        fn integers<T>(array: &dyn Array) -> Cells<'_>
        where
            T: arrow_array::ArrowPrimitiveType,
            T::Native: Into<i64>,
        {
            let values = array.as_primitive::<T>();
            Cells::Integer(array, Box::new(|row| values.value(row).into()))
        }
        Ok(match array.data_type() {
            DataType::Null => Self::Null,
            DataType::Boolean => {
                let values = array.as_boolean();
                Self::Integer(array, Box::new(|row| i64::from(values.value(row))))
            }
            DataType::Int8 => integers::<Int8Type>(array),
            DataType::Int16 => integers::<Int16Type>(array),
            DataType::Int32 => integers::<Int32Type>(array),
            DataType::Int64 => integers::<Int64Type>(array),
            DataType::UInt8 => integers::<UInt8Type>(array),
            DataType::UInt16 => integers::<UInt16Type>(array),
            DataType::UInt32 => integers::<UInt32Type>(array),
            DataType::Float32 => {
                let values = array.as_primitive::<Float32Type>();
                Self::Real(array, Box::new(|row| f64::from(values.value(row))))
            }
            DataType::Float64 => {
                let values = array.as_primitive::<Float64Type>();
                Self::Real(array, Box::new(|row| values.value(row)))
            }
            DataType::Utf8 => {
                let values = array.as_string::<i32>();
                Self::Text(array, Box::new(|row| values.value(row)))
            }
            DataType::LargeUtf8 => {
                let values = array.as_string::<i64>();
                Self::Text(array, Box::new(|row| values.value(row)))
            }
            DataType::Binary => {
                let values = array.as_binary::<i32>();
                Self::Blob(array, Box::new(|row| values.value(row)))
            }
            DataType::LargeBinary => {
                let values = array.as_binary::<i64>();
                Self::Blob(array, Box::new(|row| values.value(row)))
            }
            DataType::FixedSizeBinary(_) => {
                let values = array.as_fixed_size_binary();
                Self::Blob(array, Box::new(|row| values.value(row)))
            }
            DataType::Dictionary(..) => {
                let encoded = array
                    .as_any_dictionary_opt()
                    .ok_or_else(|| Unstorable(array.data_type().clone()))?;
                let values = Self::of(encoded.values().as_ref())
                    .map_err(|_| Unstorable(array.data_type().clone()))?;
                Self::Dictionary(array, encoded.normalized_keys(), Box::new(values))
            }
            other => return Err(Unstorable(other.clone())),
        })
    }

    /// The value at `row`, as it is bound.
    fn value(&self, row: usize) -> std::result::Result<ValueRef<'_>, Changed> {
        let null = |array: &dyn Array| array.is_null(row);
        Ok(match self {
            Self::Integer(array, value) if !null(*array) => ValueRef::Integer(value(row)),
            Self::Real(array, value) if !null(*array) => {
                let real = value(row);
                if real.is_nan() || (real == 0.0 && real.is_sign_negative()) {
                    return Err(Changed(real));
                }
                ValueRef::Real(real)
            }
            Self::Text(array, value) if !null(*array) => ValueRef::Text(value(row).as_bytes()),
            Self::Blob(array, value) if !null(*array) => ValueRef::Blob(value(row)),
            Self::Dictionary(array, keys, values) if !null(*array) => {
                return values.value(keys[row]);
            }
            _ => ValueRef::Null,
        })
    }
}

/// Rows: the most one batch of a table read back holds.
const BATCH_ROWS: usize = 1024;

/// Bytes: the text and blob values at which a batch of a table read back ends, though it holds
/// fewer rows.
const BATCH_BYTES: usize = 1 << 20;

/// Every row of `table`, in batches of bounded size, each column as its storage class; none when
/// the table is missing, and one empty batch when it holds no row.
pub(super) fn read_table(
    connection: &Connection,
    dialect: &impl SqlDialect,
    table: &str,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    read_table_each(connection, dialect, table, &mut |batch| {
        batches.push(batch);
        Ok(())
    })?;
    Ok(batches)
}

/// Gives `each` every batch [`read_table`] answers, as it is read: no more than one batch of the
/// table is held at a time.
pub(super) fn read_table_each(
    connection: &Connection,
    dialect: &impl SqlDialect,
    table: &str,
    each: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let columns = columns(connection, dialect, table)?;
    if columns.is_empty() {
        return Ok(());
    }
    let sql = format!("SELECT * FROM {}", dialect.quote(table));
    let mut prepared = connection
        .prepare(&sql)
        .map_err(failed("reading a table"))?;
    let mut read = prepared.query([]).map_err(failed("reading a table"))?;
    let (mut rows, mut bytes, mut batches) = (Vec::new(), 0_usize, 0_usize);
    while let Some(row) = read.next().map_err(failed("reading a table's rows"))? {
        let values = (0..columns.len())
            .map(|index| row.get::<_, Value>(index))
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(failed("reading a table's rows"))?;
        bytes = values.iter().fold(bytes, |bytes, value| match value {
            Value::Text(text) => bytes.saturating_add(text.len()),
            Value::Blob(blob) => bytes.saturating_add(blob.len()),
            _ => bytes,
        });
        rows.push(values);
        if rows.len() >= BATCH_ROWS || bytes >= BATCH_BYTES {
            each(batch(table, &columns, &std::mem::take(&mut rows))?)?;
            (bytes, batches) = (0, batches + 1);
        }
    }
    if !rows.is_empty() || batches == 0 {
        each(batch(table, &columns, &rows)?)?;
    }
    Ok(())
}

/// `rows` of `table`, whose columns are `columns`, as a batch.
fn batch(table: &str, columns: &[Column], rows: &[Vec<Value>]) -> Result<RecordBatch> {
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
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| ConnectorError::internal(format!("reading table {table}: {error}")))
}

fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(integer) => Some(*integer),
        _ => None,
    }
}

fn real(value: &Value) -> Option<f64> {
    match value {
        // A column of real type reads back as reals, integral values too.
        Value::Real(real) => Some(*real),
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
