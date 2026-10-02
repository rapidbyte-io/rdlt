//! What a probe reads back, admitted before any clause uses it: a destination chooses the rows,
//! types and encodings of what it reads back, so each is bounded and checked here, once.

#[cfg(test)]
mod tests;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;

use super::Bench;
use crate::cost::Rendering;
use crate::destination::TableRef;
use crate::testing::limits::{PUBLISHED_BYTES, PUBLISHED_ROWS};
use crate::testing::{Reason, Violation, bounded_call};

/// The batches a destination read back of one table, within certification's limits.
#[derive(Debug)]
pub(super) struct Published(Vec<RecordBatch>);

impl Bench<'_> {
    /// What `table` publishes, read through the probe and admitted.
    pub(super) async fn read(&self, table: &TableRef) -> Result<Published, Violation> {
        let batches = bounded_call("probe", self.probe.published(table)).await?;
        Published::admit(batches)
    }
}

impl Published {
    /// `batches`, when they hold at most [`PUBLISHED_ROWS`] rows in columns of the kinds
    /// certification writes, which expand to at most [`PUBLISHED_BYTES`] once each row holds its
    /// own value.
    pub(super) fn admit(batches: Vec<RecordBatch>) -> Result<Self, Violation> {
        let mut rows = 0_usize;
        for batch in &batches {
            rows = rows.saturating_add(batch.num_rows());
        }
        if rows > PUBLISHED_ROWS {
            return Err(Violation::from(format_args!(
                "the destination read back {rows} rows, more than the {PUBLISHED_ROWS} \
                 certification reads of a table"
            )));
        }
        // What the rows become once each holds its own value, as the cost model measures it.
        let (rendering, limit) = (Rendering::native(), count(PUBLISHED_BYTES));
        let mut bytes = 0_u64;
        for batch in &batches {
            let schema = batch.schema();
            for (field, column) in schema.fields().iter().zip(batch.columns()) {
                if !flat(column.as_ref()) {
                    return Err(Violation::from(format_args!(
                        "column `{}` read back as {}, which no column certification writes is",
                        field.name(),
                        column.data_type()
                    )));
                }
            }
            let expanded = rendering.expanded(batch, 0..batch.num_rows(), limit);
            bytes = bytes.saturating_add(expanded);
            if bytes > limit {
                return Err(Violation::from(format_args!(
                    "the destination read back rows that expand to more than the \
                     {PUBLISHED_BYTES} bytes certification reads of a table"
                )));
            }
        }
        Ok(Self(batches))
    }

    /// The batches, in the order read.
    pub(super) fn batches(&self) -> impl Iterator<Item = Read<'_>> {
        self.0.iter().map(Read)
    }

    /// How many rows were read back.
    pub(super) fn rows(&self) -> usize {
        self.0.iter().map(RecordBatch::num_rows).sum()
    }
}

/// The whole numbers of the column `name`, matched whatever its case, of `batches`, what a
/// destination read back of a table: admitted as every read-back a clause reads is, read as
/// integers are, and none of them null.
///
/// # Errors
///
/// Why the read-back is not admitted, or its column not such integers.
pub fn read_back_integers(batches: Vec<RecordBatch>, name: &str) -> Result<Vec<i64>, Reason> {
    let integers = || -> Result<Vec<i64>, Violation> {
        let published = Published::admit(batches)?;
        let mut integers = Vec::with_capacity(published.rows());
        for batch in published.batches() {
            let schema = batch.0.schema();
            let named = schema
                .fields()
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    Violation::from(format_args!("a published batch has no `{name}` column"))
                })?;
            let column = batch.required(named.name(), &DataType::Int64)?;
            integers.extend(column.as_primitive::<Int64Type>().values().iter().copied());
        }
        Ok(integers)
    };
    integers().map_err(|violation| violation.reason)
}

/// One batch read back.
#[derive(Clone, Copy)]
pub(super) struct Read<'a>(&'a RecordBatch);

impl Read<'_> {
    /// How many rows the batch holds.
    pub(super) fn rows(self) -> usize {
        self.0.num_rows()
    }

    /// Whether the batch has a column `name`.
    pub(super) fn has(self, name: &str) -> bool {
        self.0.column_by_name(name).is_some()
    }

    /// The column `name` as `to`, a value per row; none where the batch has no such column.
    pub(super) fn optional(self, name: &str, to: &DataType) -> Result<Option<ArrayRef>, Violation> {
        let Some(column) = self.0.column_by_name(name) else {
            return Ok(None);
        };
        // Only the casts that compute nothing on what a destination chose: a column is read as
        // what a column of its kind is written as, or not at all.
        if !reads(column.data_type(), to) {
            return Err(Violation::from(format_args!(
                "column `{name}` read back as {}, which no column written as {to} is",
                column.data_type()
            )));
        }
        let cast = arrow_cast::cast(column, to).map_err(|error| {
            Violation::from(format_args!("column `{name}` read back as {error}"))
        })?;
        whole(name, cast, self.rows()).map(Some)
    }

    /// The column `name` as `to`, which may hold nulls.
    pub(super) fn nullable(self, name: &str, to: &DataType) -> Result<ArrayRef, Violation> {
        self.optional(name, to)?.ok_or_else(|| {
            Violation::from(format_args!("a published batch has no `{name}` column"))
        })
    }

    /// The column `name` as `to`, which was created holding no null: a null in it is a violation.
    pub(super) fn required(self, name: &str, to: &DataType) -> Result<ArrayRef, Violation> {
        let column = self.nullable(name, to)?;
        if column.logical_null_count() > 0 {
            return Err(Violation::from(format_args!(
                "a published row has no `{name}`"
            )));
        }
        Ok(column)
    }
}

/// `cast`, when it holds a value for each of the batch's `rows`: a cast may answer with an array
/// of another length, which no row index may reach into.
fn whole(name: &str, cast: ArrayRef, rows: usize) -> Result<ArrayRef, Violation> {
    if cast.len() == rows {
        Ok(cast)
    } else {
        Err(Violation::from(format_args!(
            "column `{name}` read back as {} values for {rows} rows",
            cast.len()
        )))
    }
}

/// Whether a column of `from`, plain or encoded once, is read as `to`.
///
/// A column is read from the kinds one written as `to` is stored as: integers and instants as
/// integers, text as text, bytes as bytes, flags as flags, instants and whole numbers as
/// instants. Those casts copy, narrow with a check or change an instant's unit with a check;
/// none renders through a calendar or multiplies unchecked, as casts between kinds do.
fn reads(from: &DataType, to: &DataType) -> bool {
    let from = match from {
        DataType::Dictionary(_, values) => values.as_ref(),
        DataType::RunEndEncoded(_, values) => values.data_type(),
        plain => plain,
    };
    let bytes = matches!(
        from,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    );
    let instant = matches!(from, DataType::Timestamp(..));
    match to {
        _ if *from == DataType::Null => true,
        DataType::Int64 => from.is_integer() || instant,
        DataType::Utf8 => matches!(
            from,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        DataType::Binary | DataType::FixedSizeBinary(_) => bytes,
        DataType::Timestamp(..) => instant || *from == DataType::Int64,
        to => from == to,
    }
}

/// Whether `column` is of a kind certification writes: a scalar, or scalars one dictionary or
/// one run of ends encodes; not a nested column, nor binary values of no width.
fn flat(column: &dyn Array) -> bool {
    match column.data_type() {
        DataType::Dictionary(_, values) => scalar(values),
        DataType::RunEndEncoded(_, values) => scalar(values.data_type()),
        plain => scalar(plain),
    }
}

/// Whether values of `kind` are scalars certification writes.
fn scalar(kind: &DataType) -> bool {
    match kind {
        DataType::Null
        | DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8View
        | DataType::BinaryView => true,
        DataType::FixedSizeBinary(width) => *width > 0,
        kind => kind.primitive_width().is_some(),
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
