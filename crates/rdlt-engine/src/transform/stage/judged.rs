//! A change batch's rows a transform judges and computes from: its inserts and updates alone, a
//! cell an update flags unchanged read as null where its column may hold one.
//!
//! A delete needs only its key and a truncate none, so neither reaches a filter or a transform
//! that computes a column, and the projection after a filter does not compute from an upsert the
//! filter turned into a delete; a transform that says it passes every column on runs on the part
//! as it arrived, its output the input's columns. Such a row leaves each transform with the columns the transform passes on and the columns the
//! stream keeps, as they arrived, and null in the columns it computes. What a transform computes
//! from a placeholder an update sent for a column it left unchanged is carried as unchanged and
//! never stored, so the placeholder is read as null where its column may hold one.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::{
    Array, BooleanArray, OffsetSizeTrait, RecordBatch, RecordBatchOptions, UInt64Array,
    new_null_array,
};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{ArrowError, DataType, FieldRef, Schema, SchemaRef};
use arrow_select::{filter, nullif, take};
use rdlt_connector::cost;

use super::Refusal;
use super::changes::Changes;
use super::contract::{Declared, applied, kept};
use crate::transform::op::ARRAY_ROOM;
use crate::transform::{Transform, column_bytes};

/// A step as a change part's judging reads it: its transform, what it declares, the chain's
/// schema for its output, the columns the stream keeps, and the bytes the part asks so far, of
/// `free`.
pub(super) struct Judging<'a> {
    pub(super) transform: &'a dyn Transform,
    pub(super) declared: &'a Declared,
    pub(super) schema: &'a SchemaRef,
    pub(super) kept: &'a BTreeSet<String>,
    pub(super) asked: u64,
    pub(super) free: u64,
}

/// `output`, a transform's declared output, as a change stream's chain holds it: a column the
/// transform does not pass on, nor the stream keeps by name, may hold null, as the rows it does not
/// judge do.
pub(super) fn relaxed(
    output: &SchemaRef,
    passes: Option<&[Option<usize>]>,
    kept: &BTreeSet<String>,
) -> SchemaRef {
    let fields = output.fields().iter().enumerate().map(|(index, field)| {
        let said = passes.is_some_and(|passes| passes.get(index).copied().flatten().is_some());
        if said || kept.contains(field.name()) {
            Arc::clone(field)
        } else {
            Arc::new(field.as_ref().clone().with_nullable(true))
        }
    });
    let fields: Vec<FieldRef> = fields.collect();
    Arc::new(Schema::new_with_metadata(fields, output.metadata().clone()))
}

/// What the step's filter keeps of `data`, of a change part every row but an insert or update it
/// judges and does not keep, its `changes` set aside; or the bytes the part asks, where the copy
/// of its inserts and updates the filter judges does not fit.
pub(super) fn keep(
    step: &Judging<'_>,
    data: &RecordBatch,
    changes: Option<&Changes>,
) -> Result<ControlFlow<u64, BooleanArray>, Refusal> {
    let bounded = step.declared.bound.is_some();
    let Some(upserts) = changes.and_then(Changes::upserts) else {
        return kept(step.transform, bounded, data).map(ControlFlow::Continue);
    };
    if upserts.true_count() == 0 {
        let every = BooleanBuffer::new_set(data.num_rows());
        return Ok(ControlFlow::Continue(BooleanArray::new(every, None)));
    }
    let asked = step.asked.saturating_add(copied(data, &upserts));
    if asked > step.free {
        return Ok(ControlFlow::Break(asked));
    }
    let keep = kept(step.transform, bounded, &judged(data, &upserts)?)?;
    let mut answers = keep.values().iter();
    let rows = upserts.values().iter();
    let widened = rows.map(|judged| Some(!judged || answers.next().unwrap_or(false)));
    Ok(ControlFlow::Continue(widened.collect()))
}

/// What the step's transform makes of `data`, for every row, of a change part computed from its
/// inserts and updates alone, its `changes` set aside; or the bytes the part asks, where what that
/// takes does not fit.
pub(super) fn output(
    step: &Judging<'_>,
    data: &RecordBatch,
    changes: Option<&Changes>,
) -> Result<ControlFlow<u64, RecordBatch>, Refusal> {
    let Some(changes) = changes else {
        let output = applied(step.transform, step.declared, data, step.schema)?;
        return Ok(ControlFlow::Continue(output));
    };
    let passes = step.declared.passes.as_deref();
    let computes = passes.is_none_or(|passes| passes.iter().any(Option::is_none));
    let upserts = changes.upserts();
    let nullable = nullable(data);
    let rows = data.num_rows();
    // A transform that computes nothing, or one of inserts and updates that send every value it
    // may read as null, runs on the part as it is.
    if !computes || (upserts.is_none() && !changes.upserts_flag(Some(&nullable))) {
        let output = applied(step.transform, step.declared, data, step.schema)?;
        return Ok(ControlFlow::Continue(output));
    }
    let flagged = flagged(changes, &nullable, data.num_columns());
    let unjudged = upserts
        .as_ref()
        .is_some_and(|upserts| upserts.true_count() == 0);
    let nulls = computed(step).map(|field| cost::nulls(field.data_type(), rows));
    let nulls = nulls.fold(0, u64::saturating_add);
    // Where it says what it passes on, a part of deletes and truncates is made without it.
    if unjudged && passes.is_some() {
        let asked = step.asked.saturating_add(nulls);
        if asked > step.free {
            return Ok(ControlFlow::Break(asked));
        }
        return restored(None, data, data, upserts.as_ref(), step).map(ControlFlow::Continue);
    }
    let room = room(data, upserts.as_ref(), &flagged, step, nulls);
    let asked = step.asked.saturating_add(room);
    if asked > step.free {
        return Ok(ControlFlow::Break(asked));
    }
    let mut columns = data.columns().to_vec();
    for (place, cells) in flagged {
        let cells = BooleanArray::new(cells, None);
        let nulled = nullif::nullif(columns[place].as_ref(), &cells);
        columns[place] = nulled.map_err(|error| failed("nulling the flagged cells of", &error))?;
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    let input = RecordBatch::try_new_with_options(data.schema(), columns, &options)
        .map_err(|error| failed("nulling the flagged cells of", &error))?;
    let input = match &upserts {
        Some(upserts) => judged(&input, upserts)?,
        None => input,
    };
    let made = applied(step.transform, step.declared, &input, step.schema)?;
    restored(Some(&made), &input, data, upserts.as_ref(), step).map(ControlFlow::Continue)
}

/// The fields of the step's output it computes: those it does not say it passes on.
fn computed<'a>(step: &'a Judging<'_>) -> impl Iterator<Item = &'a FieldRef> {
    let passes = step.declared.passes.as_deref();
    let fields = step.schema.fields().iter().enumerate();
    fields
        .filter(move |(index, _)| passes.is_none_or(|passes| passes[*index].is_none()))
        .map(|(_, field)| field)
}

/// `data`'s rows that `upserts` marks.
fn judged(data: &RecordBatch, upserts: &BooleanArray) -> Result<RecordBatch, Refusal> {
    filter::filter_record_batch(data, upserts).map_err(|error| failed("judging", &error))
}

/// The places of `data`'s columns that may hold null.
fn nullable(data: &RecordBatch) -> Vec<usize> {
    let fields = data.schema_ref().fields().iter().enumerate();
    let nullable =
        fields.filter(|(_, field)| field.is_nullable() && field.data_type() != &DataType::Null);
    nullable.map(|(place, _)| place).collect()
}

/// For each of `columns` data columns whose place `nullable` names, the inserts and updates of
/// `changes` that flag it unchanged, where one does.
fn flagged(changes: &Changes, nullable: &[usize], columns: usize) -> Vec<(usize, BooleanBuffer)> {
    let mut cells = changes.unchanged_cells(columns);
    let flagged = nullable
        .iter()
        .filter_map(|place| Some((*place, cells[*place].take()?)));
    flagged.collect()
}

/// `computed`, a transform's output for `input`, `data` as the transform saw it, for every row of
/// `data`, where `upserts` marks the rows `input` holds: a column the transform passes on, says
/// it does or, saying nothing, hands back as it was handed, or the stream keeps by name, is
/// `data`'s, and another holds null on the rows it did not judge, every one where it computed
/// nothing.
fn restored(
    computed: Option<&RecordBatch>,
    input: &RecordBatch,
    data: &RecordBatch,
    upserts: Option<&BooleanArray>,
    step: &Judging<'_>,
) -> Result<RecordBatch, Refusal> {
    let places = upserts.map(|upserts| {
        let mut taken = 0_u64;
        let places = upserts.values().iter().map(|upsert| {
            upsert.then(|| {
                let place = taken;
                taken += 1;
                place
            })
        });
        places.collect::<UInt64Array>()
    });
    let passes = step.declared.passes.as_deref();
    let rows = data.num_rows();
    let mut columns = Vec::with_capacity(step.schema.fields().len());
    for (index, field) in step.schema.fields().iter().enumerate() {
        let column = computed.map(|computed| computed.column(index));
        let handed = || {
            let column = column?;
            input
                .columns()
                .iter()
                .position(|arrived| Arc::ptr_eq(arrived, column))
        };
        let arrived = match passes {
            Some(passes) => passes[index],
            None => handed(),
        };
        let arrived = arrived.or_else(|| {
            let named = step.kept.contains(field.name());
            named.then(|| data.schema_ref().index_of(field.name()).ok())?
        });
        let restored = match (arrived, column, &places) {
            (Some(place), ..) => Arc::clone(data.column(place)),
            (None, None, _) => new_null_array(field.data_type(), rows),
            (None, Some(column), None) => Arc::clone(column),
            (None, Some(column), Some(places)) => take::take(column.as_ref(), places, None)
                .map_err(|error| failed("restoring", &error))?,
        };
        columns.push(restored);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::clone(step.schema), columns, &options)
        .map_err(|error| failed("restoring", &error))
}

/// Bytes: what computing from `data`'s inserts and updates holds beside the step's room: a null
/// bitmap for each column `flagged` names, and where `upserts` marks them apart, their copy, each
/// row's place among them, and the computed columns taken back to every row, `nulls` beside what
/// the copy and the transform's bound hold.
fn room(
    data: &RecordBatch,
    upserts: Option<&BooleanArray>,
    flagged: &[(usize, BooleanBuffer)],
    step: &Judging<'_>,
    nulls: u64,
) -> u64 {
    let rows = u64::try_from(data.num_rows()).unwrap_or(u64::MAX);
    let bitmap = rows.div_ceil(8).saturating_add(ARRAY_ROOM);
    let flagged = u64::try_from(flagged.len()).unwrap_or(u64::MAX);
    let nulling = bitmap.saturating_mul(flagged);
    let Some(upserts) = upserts else {
        return nulling;
    };
    let copy = copied(data, upserts);
    let places = rows.saturating_mul(8).saturating_add(bitmap);
    let bound = step.declared.bound.as_ref();
    let taken = bound
        .map_or(0, |bound| bound.of(data))
        .saturating_add(copy)
        .saturating_add(nulls);
    nulling
        .saturating_add(copy)
        .saturating_add(places)
        .saturating_add(taken)
}

/// Bytes: a copy of `data`'s rows that `upserts` marks.
fn copied(data: &RecordBatch, upserts: &BooleanArray) -> u64 {
    let rows = upserts.true_count();
    let columns = data.columns().iter();
    let copies = columns.map(|column| selected(column.as_ref(), upserts, rows));
    copies.fold(0, u64::saturating_add)
}

/// Bytes: a copy of `column`'s `rows` rows that `upserts` marks, their values or views and their
/// validity; the whole column's bytes for a type a copy takes otherwise; an empty array's where
/// it marks none.
fn selected(column: &dyn Array, upserts: &BooleanArray, rows: usize) -> u64 {
    let count = u64::try_from(rows).unwrap_or(u64::MAX);
    let around = count.div_ceil(8).saturating_add(ARRAY_ROOM);
    if rows == 0 {
        return around;
    }
    let marked = || upserts.values().set_indices();
    let values = match column.data_type() {
        DataType::Boolean => count.div_ceil(8),
        DataType::Utf8 => texts(column.as_string::<i32>().value_offsets(), marked()),
        DataType::LargeUtf8 => texts(column.as_string::<i64>().value_offsets(), marked()),
        DataType::Binary => texts(column.as_binary::<i32>().value_offsets(), marked()),
        DataType::LargeBinary => texts(column.as_binary::<i64>().value_offsets(), marked()),
        DataType::Utf8View | DataType::BinaryView => count.saturating_mul(16),
        DataType::FixedSizeBinary(width) => {
            let width = u64::try_from(*width).unwrap_or(u64::MAX);
            count.saturating_mul(width)
        }
        other => match other.primitive_width() {
            Some(width) => count.saturating_mul(u64::try_from(width).unwrap_or(u64::MAX)),
            None => return column_bytes(column),
        },
    };
    values.saturating_add(around)
}

/// Bytes: the offsets and values of the rows `marked` names, of an array of text or bytes whose
/// offsets are `offsets`.
fn texts<O: OffsetSizeTrait>(offsets: &[O], marked: impl Iterator<Item = usize>) -> u64 {
    let width = u64::try_from(size_of::<O>()).unwrap_or(u64::MAX);
    let (mut rows, mut bytes) = (1_u64, 0_u64);
    for row in marked {
        let length = offsets[row + 1]
            .as_usize()
            .saturating_sub(offsets[row].as_usize());
        rows = rows.saturating_add(1);
        bytes = bytes.saturating_add(u64::try_from(length).unwrap_or(u64::MAX));
    }
    rows.saturating_mul(width).saturating_add(bytes)
}

fn failed(what: &str, error: &ArrowError) -> Refusal {
    Refusal::Internal(format!(
        "{what} the inserts and updates of a change batch: {error}"
    ))
}
