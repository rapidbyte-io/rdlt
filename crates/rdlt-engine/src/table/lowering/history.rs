//! A history table's columns: when each version begins, its end, whether it is
//! current, and the hash of its data.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use rdlt_connector::StreamName;

use super::{ChangeRows, LoweringPlan, Source, Stamp, lower_array};
use crate::error::Error;
use crate::normalize::identity::{unread, version_hashes};
use crate::table::convert::decoded;
use crate::table::lower::loaded_at_type;

impl LoweringPlan {
    /// The history columns of `batch`'s rows, lowered, where the plan's table keeps history:
    /// `columns` starts with the rows' data columns as stored, and `changes` says which rows
    /// delete; none for another table.
    pub(super) fn history(
        &self,
        batch: &RecordBatch,
        columns: &[ArrayRef],
        stamp: &Stamp,
        changes: Option<&ChangeRows>,
    ) -> Result<Vec<ArrayRef>, Error> {
        let view = &self.view;
        let Some(names) = &view.meta.history else {
            return Ok(Vec::new());
        };
        let stream = &self.stream;
        let failed = |error: arrow_schema::ArrowError| {
            Error::internal(format!(
                "stream {stream}: lowering history columns: {error}"
            ))
        };
        let data = self
            .data(batch.num_rows(), columns, names.change_time.as_deref())
            .map_err(failed)?;
        let from = match &names.change_time {
            Some(column) => Some(batch.column_by_name(column).ok_or_else(|| {
                Error::schema(format!(
                    "stream {stream}: a batch has no change time {column}"
                ))
                .with_code("change_time_missing")
                .with_stream(stream)
            })?),
            None => None,
        };
        let deleting = |row: usize| {
            changes.is_some_and(|changes| {
                matches!(
                    changes.op(row),
                    Some(rdlt_connector::ChangeOp::Delete | rdlt_connector::ChangeOp::Truncate)
                )
            })
        };
        let history = history_columns(stream, &data, from, stamp.received_at, &deleting)?;
        let first = columns.len();
        let logical = [
            loaded_at_type(),
            loaded_at_type(),
            rdlt_connector::LogicalType::Bool,
            rdlt_connector::LogicalType::Binary,
        ];
        history
            .iter()
            .zip(&logical)
            .enumerate()
            .map(|(index, (array, logical))| {
                let lowered = view.physical[first + index].logical_type();
                lower_array(array, logical, lowered).map_err(failed)
            })
            .collect()
    }

    /// The rows' data columns, from `columns`, as a batch of `rows` rows, without the column the
    /// stream's `change_time` fills: when a change happened is not what it changed.
    fn data(
        &self,
        rows: usize,
        columns: &[ArrayRef],
        change_time: Option<&str>,
    ) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let timed = |source: &Source| match source {
            Source::Incoming(index, _) => self
                .incoming
                .schema
                .fields()
                .iter()
                .nth(*index)
                .is_some_and(|field| Some(field.name()) == change_time),
            Source::Read(_) | Source::Rest(_) | Source::Nulls => false,
        };
        // Each column keeps its field's metadata, which says, for one, that its text is JSON.
        let data: Vec<_> = self
            .view
            .schema
            .fields()
            .iter()
            .zip(columns)
            .zip(&self.sources)
            .filter(|(_, source)| !timed(source))
            .map(|((field, array), _)| (field, array))
            .collect();
        let fields: Vec<_> = data
            .iter()
            .map(|(field, array)| {
                arrow_schema::Field::new(field.name(), array.data_type().clone(), true)
                    .with_metadata(field.metadata().clone())
            })
            .collect();
        let arrays = data.iter().map(|(_, array)| Arc::clone(array)).collect();
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows));
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        RecordBatch::try_new_with_options(schema, arrays, &options)
    }
}

/// The history columns of rows holding `data`, the table's data columns as stored: when each
/// version begins, from `from`, the stream's change time, or `received`, when its batch arrived,
/// where the stream names none; no end; current; and the hash of its data, null where `deleting`
/// says the row deletes.
///
/// A row's hash encodes its data as one object of its non-null columns by name, so a column
/// added since, null in the row, a wider type or another encoding of the same values hashes
/// alike, and only a change of value opens a version.
///
/// # Errors
///
/// A change time of another type than a date or timestamp, or beyond what microseconds since the
/// epoch hold, is `change_time_invalid`, and a row without one `change_time_null`, Schema errors:
/// no version can begin at an unknown time.
pub(super) fn history_columns(
    stream: &StreamName,
    data: &RecordBatch,
    from: Option<&ArrayRef>,
    received: SystemTime,
    deleting: &dyn Fn(usize) -> bool,
) -> Result<[ArrayRef; 4], Error> {
    let rows = data.num_rows();
    let micros = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let valid_from: ArrayRef = if let Some(from) = from {
        begins(stream, from, &micros)?
    } else {
        let since = received.duration_since(UNIX_EPOCH).unwrap_or_default();
        let at = i64::try_from(since.as_micros()).unwrap_or(i64::MAX);
        Arc::new(TimestampMicrosecondArray::from(vec![at; rows]).with_timezone("UTC"))
    };
    let valid_to = arrow_array::new_null_array(&micros, rows);
    let current = Arc::new(BooleanArray::from(vec![true; rows]));
    let hashes = version_hashes(data)
        .map_err(|error| unread(stream, "preparing history columns", &error))?;
    let hashes: BinaryArray = hashes
        .iter()
        .enumerate()
        .map(|(row, hash)| hash.filter(|_| !deleting(row)))
        .collect();
    Ok([valid_from, valid_to, current, Arc::new(hashes)])
}

/// When the version of each row whose change time `from` holds begins, in microseconds.
fn begins(stream: &StreamName, from: &ArrayRef, micros: &DataType) -> Result<ArrayRef, Error> {
    let refuse = |code: &str, detail: String| {
        Err(Error::schema(format!("stream {stream}: {detail}"))
            .with_code(code)
            .with_stream(stream))
    };
    // A dictionary or run-end encoding holds its rows' times, and its rows' nulls, in its values.
    let from = decoded(from).or_else(|error| {
        refuse(
            "change_time_invalid",
            format!("its change time cannot be read: {error}"),
        )
    })?;
    let time = matches!(
        from.data_type(),
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
    );
    if !time {
        let held = from.data_type();
        return refuse(
            "change_time_invalid",
            format!("its change time holds {held}, not a time"),
        );
    }
    if from.null_count() > 0 {
        let detail = "a change has no change time, when its version begins".to_owned();
        return refuse("change_time_null", detail);
    }
    // A strict cast: a time microseconds cannot hold fails rather than turning null.
    let strict = arrow_cast::CastOptions {
        safe: false,
        ..arrow_cast::CastOptions::default()
    };
    arrow_cast::cast_with_options(&from, micros, &strict).or_else(|error| {
        refuse(
            "change_time_invalid",
            format!("its change time holds a time out of range: {error}"),
        )
    })
}
