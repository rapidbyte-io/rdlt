//! A history table's columns: when each version begins, its end, whether it is
//! current, and the hash of its data.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, TimestampMicrosecondArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, TimeUnit};
use rdlt_connector::StreamName;

use super::{ChangeRows, LoweringPlan, Source, Stamp, lower_array};
use crate::error::Error;
use crate::normalize::identity::{unread, version_hashes};
use crate::policy::SchemaPolicy;
use crate::table::convert::decoded;
use crate::table::lower::loaded_at_type;
use crate::table::temporal;

impl LoweringPlan {
    /// The history columns of `batch`'s rows, lowered, the first the table's column at `first`,
    /// where the plan's table keeps history: `held` holds the rows' data columns as the model's
    /// types hold them, and `changes` says which rows delete; none for another table.
    pub(super) fn history(
        &self,
        batch: &RecordBatch,
        (held, first): (&[ArrayRef], usize),
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
            .data(batch.num_rows(), held, names.change_time.as_deref())
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
        // A change time whose policy discards values begins its version when its batch arrived
        // where it holds none, or one no version can begin at, which `nulled` took.
        let fallback = self.change_time_policy() == SchemaPolicy::DiscardValue;
        let begun = Begins {
            from,
            received: stamp.received_at,
            fallback,
        };
        let history = history_columns(stream, &data, begun, &deleting)?;
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
                // Validity a destination stores as microseconds is the count the timestamp is.
                if *lowered == rdlt_connector::LogicalType::Int64 {
                    return arrow_cast::cast(array, &DataType::Int64).map_err(failed);
                }
                lower_array(array, logical, lowered).map_err(failed)
            })
            .collect()
    }

    /// The rows' data columns, from `held`, as the model's types hold them, as a batch of `rows`
    /// rows, without the column the stream's `change_time` fills: when a change happened is not
    /// what it changed.
    fn data(
        &self,
        rows: usize,
        held: &[ArrayRef],
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
        // Each column keeps its type's metadata, which says, for one, that its text is JSON.
        let data: Vec<_> = self
            .view
            .model
            .columns
            .iter()
            .zip(held)
            .zip(&self.sources)
            .filter(|(_, source)| !timed(source))
            .map(|((column, array), _)| (column, array))
            .collect();
        let fields: Vec<_> = data
            .iter()
            .map(|(column, array)| {
                arrow_schema::Field::new(column.name(), array.data_type().clone(), true)
                    .with_metadata(column.to_arrow().metadata().clone())
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
    begun: Begins<'_>,
    deleting: &dyn Fn(usize) -> bool,
) -> Result<[ArrayRef; 4], Error> {
    let rows = data.num_rows();
    let micros = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let arrived = || {
        micros_of(begun.received).ok_or_else(|| {
            Error::internal(format!(
                "stream {stream}: the clock reads a time microseconds since the epoch cannot hold"
            ))
        })
    };
    let valid_from: ArrayRef = match begun.from {
        Some(from) => {
            let fallback = if begun.fallback {
                Some(arrived()?)
            } else {
                None
            };
            begins(stream, from, fallback)?
        }
        None => {
            Arc::new(TimestampMicrosecondArray::from(vec![arrived()?; rows]).with_timezone("UTC"))
        }
    };
    let valid_to = arrow_array::new_null_array(&micros, rows);
    let current = Arc::new(BooleanArray::from(vec![true; rows]));
    let hashes = version_hashes(data)
        .map_err(|error| unread(stream, "preparing history columns", &error))?;
    let (offsets, values, nulls) = hashes.into_parts();
    let kept = NullBuffer::new(BooleanBuffer::collect_bool(rows, |row| !deleting(row)));
    let nulls =
        NullBuffer::union(nulls.as_ref(), Some(&kept)).filter(|nulls| nulls.null_count() > 0);
    let hashes = BinaryArray::try_new(offsets, values, nulls)
        .map_err(|error| unread(stream, "preparing history columns", &error))?;
    Ok([valid_from, valid_to, current, Arc::new(hashes)])
}

/// Where a history table's versions begin: at the stream's change time `from`, or where it names
/// none at `received`, when their batch arrived; and with `fallback`, a row without a change time
/// at `received` too, rather than refused.
#[derive(Clone, Copy, Debug)]
pub(super) struct Begins<'a> {
    pub(super) from: Option<&'a ArrayRef>,
    pub(super) received: SystemTime,
    pub(super) fallback: bool,
}

/// The microseconds since the epoch of `time`, before it negative, a time between two the
/// earlier; `None` beyond what an `i64` holds.
fn micros_of(time: SystemTime) -> Option<i64> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(since) => i64::try_from(since.as_micros()).ok(),
        Err(before) => {
            let before = before.duration();
            let part = u128::from(!before.subsec_nanos().is_multiple_of(1_000));
            let micros = i64::try_from(before.as_micros() + part).ok()?;
            Some(-micros)
        }
    }
}

/// When the version of each row whose change time `from` holds begins, in microseconds: a
/// date's midnight in UTC, and an instant between two microseconds the earlier; a row without
/// one at `fallback`, where there is one.
fn begins(stream: &StreamName, from: &ArrayRef, fallback: Option<i64>) -> Result<ArrayRef, Error> {
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
    if from.null_count() > 0 && fallback.is_none() {
        let detail = "a change has no change time, when its version begins".to_owned();
        return refuse("change_time_null", detail);
    }
    let begun: Option<Vec<i64>> = (0..from.len())
        .map(|row| match fallback {
            Some(fallback) if from.is_null(row) => Some(fallback),
            _ => temporal::micros_at(from.as_ref(), row),
        })
        .collect();
    let Some(begun) = begun else {
        let detail = "its change time holds a time microseconds since the epoch cannot hold";
        return refuse("change_time_invalid", detail.to_owned());
    };
    Ok(Arc::new(
        TimestampMicrosecondArray::from(begun).with_timezone("UTC"),
    ))
}
