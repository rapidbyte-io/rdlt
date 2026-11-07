//! A change batch through the stage: its change columns set aside while transforms see its
//! data, the filter rule on its ops, and its unchanged flags carried along each transform's
//! lineage.

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray as _;
use arrow_array::types::Int8Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, RecordBatch, RecordBatchOptions,
};
use arrow_schema::{ArrowError, FieldRef, Schema, SchemaRef};
use arrow_select::filter;
use rdlt_connector::cost::Allocations;
use rdlt_connector::{
    ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN, UnchangedFlags, remap_unchanged,
};

use super::{Chain, Refusal};
use crate::transform::column_bytes;
use crate::transform::op::ARRAY_ROOM;

/// Where a change batch's change columns sit among its fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ChangePlaces {
    op: usize,
    seq: usize,
    flags: Option<usize>,
}

impl ChangePlaces {
    fn holds(self, place: usize) -> bool {
        place == self.op || place == self.seq || Some(place) == self.flags
    }
}

/// A change batch's change columns, set aside while transforms see its data; its flags over
/// the data columns as the last transform left them.
pub(super) struct Changes {
    op: Int8Array,
    seq: ArrayRef,
    flags: Option<BinaryArray>,
}

/// The places of `input`'s data columns, and of its change columns where `changes` says its
/// batches carry them; a declared schema carries none.
pub(super) fn places(input: &Schema, changes: bool) -> (Vec<usize>, Option<ChangePlaces>) {
    let index = |name| input.index_of(name).ok();
    let places = match (changes, index(OP_COLUMN), index(SEQ_COLUMN)) {
        (true, Some(op), Some(seq)) => Some(ChangePlaces {
            op,
            seq,
            flags: index(UNCHANGED_COLUMN),
        }),
        _ => None,
    };
    let data = (0..input.fields().len())
        .filter(|place| places.is_none_or(|places| !places.holds(*place)))
        .collect();
    (data, places)
}

/// The stage's output schema: `data`, then `input`'s change columns.
pub(super) fn output(data: &SchemaRef, input: &Schema, places: Option<&ChangePlaces>) -> SchemaRef {
    let Some(places) = places else {
        return Arc::clone(data);
    };
    let mut fields: Vec<FieldRef> = data.fields().iter().cloned().collect();
    let changed = [places.op, places.seq].into_iter().chain(places.flags);
    fields.extend(changed.map(|place| Arc::clone(&input.fields()[place])));
    Arc::new(Schema::new_with_metadata(fields, data.metadata().clone()))
}

/// Bytes: flags over `columns` columns for `rows` rows, built anew: each row's bitmap, its offset
/// and its validity, and the rounding of their buffers.
fn flags_bytes(rows: usize, columns: usize) -> u64 {
    let rows = u64::try_from(rows).unwrap_or(u64::MAX);
    let width = u64::try_from(columns.div_ceil(8)).unwrap_or(u64::MAX);
    rows.saturating_mul(width.saturating_add(4))
        .saturating_add(rows.div_ceil(8))
        .saturating_add(ARRAY_ROOM)
}

/// Bytes: what moving the flags of `rows` rows of the chain's input to the kept columns' places
/// allocates; nothing where its batches carry no flags.
pub(super) fn remapping(chain: &Chain, rows: usize) -> u64 {
    let flagged = chain.places.is_some_and(|places| places.flags.is_some());
    if flagged {
        flags_bytes(rows, chain.kept.len())
    } else {
        0
    }
}

/// `batch`'s kept data columns, shared, as the chain's transforms take them, and its change
/// columns set aside, its flags moved to the kept columns' places.
pub(super) fn split(
    batch: &RecordBatch,
    chain: &Chain,
) -> Result<(RecordBatch, Option<Changes>), Refusal> {
    let columns = chain
        .kept
        .iter()
        .map(|place| Arc::clone(batch.column(*place)))
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    let data = RecordBatch::try_new_with_options(Arc::clone(&chain.schemas[0]), columns, &options)
        .map_err(|error| Refusal::Internal(format!("keeping a batch's columns: {error}")))?;
    let Some(places) = chain.places else {
        return Ok((data, None));
    };
    let unexpected =
        || Refusal::Internal("a change batch's change columns are not of their types".to_owned());
    let op = batch
        .column(places.op)
        .as_primitive_opt::<Int8Type>()
        .ok_or_else(unexpected)?;
    let flags = match places.flags {
        Some(place) => {
            let flags = batch
                .column(place)
                .as_binary_opt::<i32>()
                .ok_or_else(unexpected)?;
            Some(remap_unchanged(flags, &chain.kept_places))
        }
        None => None,
    };
    let changes = Changes {
        op: op.clone(),
        seq: Arc::clone(batch.column(places.seq)),
        flags,
    };
    Ok((data, Some(changes)))
}

impl Changes {
    /// Bytes: the change columns' buffers, as a copy of every row takes them.
    pub(super) fn bytes(&self) -> u64 {
        let flags = self.flags.as_ref().map_or(0, |flags| column_bytes(flags));
        column_bytes(&self.op)
            .saturating_add(column_bytes(self.seq.as_ref()))
            .saturating_add(flags)
    }

    /// Bytes: the ops of every row, rewritten.
    pub(super) fn ops_bytes(&self) -> u64 {
        let rows = u64::try_from(self.op.len()).unwrap_or(u64::MAX);
        rows.saturating_add(ARRAY_ROOM)
    }

    /// Bytes: the flags rebuilt over `columns` output columns; nothing where there are none.
    pub(super) fn relined_bytes(&self, columns: usize) -> u64 {
        self.flags
            .as_ref()
            .map_or(0, |flags| flags_bytes(flags.len(), columns))
    }

    /// Bytes: the buffers the change columns hold beyond `part`'s, the batch they came from.
    pub(super) fn beyond(&self, part: &RecordBatch) -> u64 {
        let input = Allocations::of(part);
        let mut made = Allocations::default();
        let ops = made.add_array_beyond(&self.op, &input);
        let seqs = made.add_array_beyond(self.seq.as_ref(), &input);
        let flags = self
            .flags
            .as_ref()
            .map_or(0, |flags| made.add_array_beyond(flags, &input));
        ops.saturating_add(seqs).saturating_add(flags)
    }

    /// Whether a row flags unchanged a column of `reads`, by its place among the data columns,
    /// or any column where `reads` is none.
    pub(super) fn flags_any_of(&self, reads: Option<&[usize]>) -> bool {
        let Some(flags) = &self.flags else {
            return false;
        };
        let mut bitmaps = flags.iter().flatten();
        match reads {
            None => bitmaps.any(|bitmap| bitmap.iter().any(|byte| *byte != 0)),
            Some(reads) => bitmaps.any(|bitmap| {
                let unchanged = UnchangedFlags::new(bitmap);
                reads.iter().any(|column| unchanged.contains(*column))
            }),
        }
    }

    /// The flags over a transform's output, whose `lineage` gives each output column's input
    /// columns: an output column is unchanged where it reads a column and every column it reads
    /// is unchanged.
    pub(super) fn relined(self, lineage: &[Vec<usize>]) -> Self {
        let Some(flags) = &self.flags else {
            return self;
        };
        let width = lineage.len().div_ceil(8);
        let mut relined =
            BinaryBuilder::with_capacity(flags.len(), flags.len().saturating_mul(width));
        let mut bytes = vec![0_u8; width];
        for bitmap in flags {
            let Some(bitmap) = bitmap else {
                relined.append_null();
                continue;
            };
            let unchanged = UnchangedFlags::new(bitmap);
            bytes.fill(0);
            for (column, reads) in lineage.iter().enumerate() {
                if !reads.is_empty() && reads.iter().all(|input| unchanged.contains(*input)) {
                    bytes[column / 8] |= 1 << (column % 8);
                }
            }
            relined.append_value(&bytes);
        }
        Self {
            flags: Some(relined.finish()),
            ..self
        }
    }

    /// The change columns with each insert or update `keep` does not keep turned into a delete
    /// of its key, and how many were.
    pub(super) fn deleting(self, keep: &BooleanArray) -> (Self, u64) {
        let mut ops = Vec::with_capacity(self.op.len());
        let mut deleted = 0;
        let judged = self.op.values().iter().zip(keep.values().iter());
        for (op, kept) in judged {
            if !kept && upsert(*op) {
                deleted += 1;
                ops.push(ChangeOp::Delete.code());
            } else {
                ops.push(*op);
            }
        }
        let op = Int8Array::new(ops.into(), self.op.nulls().cloned());
        (Self { op, ..self }, deleted)
    }

    /// Which rows pass `keep`: those it keeps, and every delete and truncate, which hold no
    /// values to judge.
    pub(super) fn passing(&self, keep: &BooleanArray) -> BooleanArray {
        let ops = self.op.values().iter().zip(keep.values().iter());
        ops.map(|(op, kept)| Some(kept || !upsert(*op))).collect()
    }

    /// The rows `keep` keeps.
    pub(super) fn filter(self, keep: &BooleanArray) -> Result<Self, ArrowError> {
        let op = filter::filter(&self.op, keep)?
            .as_primitive::<Int8Type>()
            .clone();
        let seq = filter::filter(self.seq.as_ref(), keep)?;
        let flags = match self.flags {
            Some(flags) => Some(filter::filter(&flags, keep)?.as_binary::<i32>().clone()),
            None => None,
        };
        Ok(Self { op, seq, flags })
    }
}

fn upsert(code: i8) -> bool {
    matches!(
        ChangeOp::from_code(code),
        Some(ChangeOp::Insert | ChangeOp::Update)
    )
}

/// `data`, the transforms' output, with `changes` after it: its flags lie over the output's
/// data columns, which come first.
pub(super) fn join(
    data: RecordBatch,
    changes: Option<Changes>,
    chain: &Chain,
) -> Result<RecordBatch, Refusal> {
    let Some(changes) = changes else {
        return Ok(data);
    };
    let rows = data.num_rows();
    let mut columns = data.columns().to_vec();
    columns.push(Arc::new(changes.op));
    columns.push(changes.seq);
    if let Some(flags) = changes.flags {
        columns.push(Arc::new(flags));
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::clone(&chain.output), columns, &options)
        .map_err(|error| Refusal::Internal(format!("joining a change batch: {error}")))
}
