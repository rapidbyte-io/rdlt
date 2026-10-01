//! What a probe reads back, admitted before any clause uses it: a destination chooses the rows,
//! types and encodings of what it reads back, so each is bounded and checked here, once.

#[cfg(test)]
mod tests;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    BinaryViewType, ByteArrayType, ByteViewType, GenericBinaryType, GenericStringType,
    StringViewType,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;

use super::Bench;
use crate::destination::TableRef;
use crate::testing::limits::{PUBLISHED_BYTES, PUBLISHED_ROWS};
use crate::testing::{Violation, bounded_call};

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
    /// certification writes, which expand to at most [`PUBLISHED_BYTES`].
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
        let mut bytes = 0_usize;
        for batch in &batches {
            let schema = batch.schema();
            for (field, column) in schema.fields().iter().zip(batch.columns()) {
                let Some(widest) = widest(column.as_ref()) else {
                    return Err(Violation::from(format_args!(
                        "column `{}` read back as {}, which no column certification writes is",
                        field.name(),
                        column.data_type()
                    )));
                };
                bytes = bytes.saturating_add(widest.saturating_mul(batch.num_rows()));
            }
        }
        if bytes > PUBLISHED_BYTES {
            return Err(Violation::from(format_args!(
                "the destination read back rows that expand to {bytes} bytes, more than the \
                 {PUBLISHED_BYTES} certification reads of a table"
            )));
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
        // Arrow renders an instant as text through a calendar, which holds no instant near its
        // ends: a column written as text reads back as no instant.
        if text(to) && temporal(column.data_type()) {
            return Err(Violation::from(format_args!(
                "column `{name}` read back as {}, not as text",
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

fn text(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Whether `kind` holds instants, dates, times or spans, encoded or not.
fn temporal(kind: &DataType) -> bool {
    match kind {
        DataType::Dictionary(_, values) => temporal(values),
        DataType::RunEndEncoded(_, values) => temporal(values.data_type()),
        kind => kind.is_temporal(),
    }
}

/// The bytes the widest value of `column` takes, where `column` is of a kind certification
/// writes: a scalar, or scalars one dictionary or one run of ends encodes; none otherwise, as for
/// nested columns and binary values of no width.
fn widest(column: &dyn Array) -> Option<usize> {
    match column.data_type() {
        DataType::Dictionary(_, _) => scalar(column.as_any_dictionary().values().as_ref()),
        DataType::RunEndEncoded(_, _) => {
            let values = column.to_data().child_data().get(1).cloned()?;
            scalar(arrow_array::make_array(values).as_ref())
        }
        _ => scalar(column),
    }
}

/// The bytes the widest value of `values` takes, where it holds scalars.
fn scalar(values: &dyn Array) -> Option<usize> {
    let kind = values.data_type();
    if let Some(width) = kind.primitive_width() {
        return Some(width);
    }
    match kind {
        DataType::Null | DataType::Boolean => Some(1),
        DataType::FixedSizeBinary(width) => usize::try_from(*width).ok().filter(|width| *width > 0),
        DataType::Utf8 => Some(longest::<GenericStringType<i32>>(values)),
        DataType::LargeUtf8 => Some(longest::<GenericStringType<i64>>(values)),
        DataType::Binary => Some(longest::<GenericBinaryType<i32>>(values)),
        DataType::LargeBinary => Some(longest::<GenericBinaryType<i64>>(values)),
        DataType::Utf8View => Some(longest_view::<StringViewType>(values)),
        DataType::BinaryView => Some(longest_view::<BinaryViewType>(values)),
        _ => None,
    }
}

/// The length of the longest value of `values`, strings or bytes held by offsets.
fn longest<T: ByteArrayType>(values: &dyn Array) -> usize {
    let lengths = values.as_bytes::<T>().offsets().lengths();
    lengths.max().unwrap_or_default()
}

/// The length of the longest value of `values`, strings or bytes held by views.
fn longest_view<T: ByteViewType>(values: &dyn Array) -> usize {
    let lengths = values.as_byte_view::<T>().lengths();
    let longest = lengths.max().unwrap_or_default();
    usize::try_from(longest).unwrap_or(usize::MAX)
}
