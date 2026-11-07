//! The transform stage: what a stream's include and transforms make of each batch, each
//! transform held to what it declares.

mod changes;
mod contract;
mod decoding;
mod refused;
mod room;
#[cfg(test)]
mod tests;

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use arrow_select::filter;
use rdlt_connector::cost::arrays;
use rdlt_connector::{ColumnPath, ReadMode, UNCHANGED_COLUMN};

use self::changes::{ChangePlaces, Changes};
use self::contract::{Declared, applied, carried, declares, kept};
use self::decoding::Encoded;
pub(crate) use self::refused::{Breach, Refusal, refused};
use super::{Transform, TransformStep, allocated_beyond};
use crate::plan::StreamPlan;

/// What a filter does with a row it does not keep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilterRule {
    /// Drops it: a stream that appends or replaces, or keeps a log of its changes.
    Drop,
    /// Turns its insert or update into a delete of its key: a change stream matched by key.
    Delete,
}

/// A stream's include and transforms, as every partition of it runs them on each batch.
#[derive(Debug)]
pub(crate) struct Stage {
    steps: Arc<[TransformStep]>,
    /// The top-level columns kept, where the stream lists them: those it includes, and those
    /// every transform passes on.
    included: Option<BTreeSet<String>>,
    /// The top-level columns every transform passes on as they arrive: the stream's key and
    /// change time.
    kept: BTreeSet<String>,
    rule: FilterRule,
    /// Whether the stream's batches carry change columns.
    changes: bool,
}

/// What the stage makes of batches of one schema.
#[derive(Debug)]
pub(crate) struct Chain {
    input: SchemaRef,
    /// The data columns kept, by their places in the input.
    kept: Vec<usize>,
    /// For each input field, its place among the kept data columns: where its flags move.
    kept_places: Vec<Option<usize>>,
    /// Where a change batch's change columns sit in the input.
    places: Option<ChangePlaces>,
    /// The data columns kept, as they arrive.
    arrived: SchemaRef,
    /// The kept data columns that arrive encoded, which a stage that transforms decodes first.
    encoded: Encoded,
    /// Each transform's input schema, then the transforms' output; data columns alone, the
    /// encoded ones decoded.
    schemas: Vec<SchemaRef>,
    /// What each transform declares of its input.
    declared: Vec<Declared>,
    /// The transforms' output, then the change columns.
    output: SchemaRef,
}

/// What the stage made of a batch: the batches left, in order, what they hold beyond it, and
/// what its filters did.
#[derive(Debug, Default)]
pub(crate) struct Ran {
    pub(crate) batches: Vec<RecordBatch>,
    /// Bytes: the buffers the batches hold beyond the input's, and their arrays.
    held: u64,
    /// Rows a filter dropped.
    pub(crate) dropped: u64,
    /// Inserts and updates a filter turned into deletes of their keys.
    pub(crate) deleted: u64,
}

/// A part of a batch run through the chain: done, or to be cut in two first.
enum Part {
    Done(Ran),
    Split,
}

impl Stage {
    /// The stage of `plan`'s stream, matched by `key` and timed by the source's `change_time`;
    /// none where the stream includes every column and transforms nothing.
    pub(crate) fn new(
        plan: &StreamPlan,
        key: &[ColumnPath],
        change_time: Option<&ColumnPath>,
    ) -> Option<Self> {
        if plan.included().is_none() && plan.transforms().is_empty() {
            return None;
        }
        let root = |column: &ColumnPath| column.segments().next().map(str::to_owned);
        let kept: BTreeSet<String> = key.iter().chain(change_time).filter_map(root).collect();
        let included = plan.included().map(|columns| {
            let listed = columns.iter().filter_map(root);
            listed.chain(kept.iter().cloned()).collect()
        });
        let rule = if plan.merges_changes() {
            FilterRule::Delete
        } else {
            FilterRule::Drop
        };
        Some(Self {
            steps: plan.transforms().into(),
            included,
            kept,
            rule,
            changes: plan.read_mode() == ReadMode::Cdc,
        })
    }

    /// Whether the stage allocates as it runs on batches of `input`, as its transforms do and as
    /// moving a change batch's unchanged flags to the kept columns' places does, and so holds
    /// lowering's bytes.
    pub(crate) fn reserves(&self, input: &Schema) -> bool {
        let flagged = self.changes && input.index_of(UNCHANGED_COLUMN).is_ok();
        !self.steps.is_empty() || flagged
    }

    /// The name of the transform whose output the stage leaves, where it transforms.
    pub(crate) fn last_transform(&self) -> Option<&str> {
        let last = self.steps.last();
        last.map(|step| step.as_transform().name())
    }

    /// The chain the stage makes of batches of `input`.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] where a transform refuses its input, declares a lineage or the columns its
    /// filter reads naming columns its input lacks, or the transforms do not pass a column the
    /// stream keeps on as it arrives.
    pub(crate) fn chain(&self, input: &SchemaRef) -> Result<Chain, Refusal> {
        let (data, places) = changes::places(input, self.changes);
        let kept: Vec<usize> = data
            .into_iter()
            .filter(|index| self.includes(input.field(*index).name()))
            .collect();
        let mut kept_places = vec![None; input.fields().len()];
        for (at, place) in kept.iter().enumerate() {
            kept_places[*place] = Some(at);
        }
        let arrived = input
            .project(&kept)
            .map_err(|error| internal("keeping a batch's columns", &error))?;
        let arrived = Arc::new(arrived);
        let (first, encoded) = if self.steps.is_empty() {
            (Arc::clone(&arrived), Encoded::default())
        } else {
            decoding::decoded_schema(&arrived)?
        };
        let mut schemas = vec![first];
        let mut declared = Vec::with_capacity(self.steps.len());
        for step in self.steps.iter() {
            let transform = step.as_transform();
            let current = Arc::clone(schemas.last().expect("a chain starts at its input"));
            let output = transform
                .output(&current)
                .map_err(|error| Refusal::Schema {
                    transform: transform.name().to_owned(),
                    error,
                })?;
            declared.push(declares(transform, &current, &output)?);
            schemas.push(output);
        }
        let data = Arc::clone(schemas.last().expect("a chain starts at its input"));
        self.check_kept(&schemas[0], &data)?;
        let output = changes::output(&data, input, places.as_ref());
        Ok(Chain {
            input: Arc::clone(input),
            kept,
            kept_places,
            places,
            arrived,
            encoded,
            schemas,
            declared,
            output,
        })
    }

    /// The chain the stage makes of `declared`, a stream's declared schema, refusing an op that
    /// names a column it lacks as the op meets it.
    ///
    /// # Errors
    ///
    /// As [`Stage::chain`], and [`Refusal::Absent`] for such an op.
    pub(crate) fn declared(&self, declared: &SchemaRef) -> Result<Chain, Refusal> {
        let chain = self.chain(declared)?;
        for (index, step) in self.steps.iter().enumerate() {
            if let TransformStep::Op(op) = step
                && let Some(column) = op.lacks(&chain.schemas[index])
            {
                let transform = op.name().to_owned();
                return Err(Refusal::Absent { transform, column });
            }
        }
        Ok(chain)
    }

    /// `batch`, of the chain's input schema, through the stage, its transforms holding at most
    /// `free` bytes beyond it: a part whose bound passes what is left is cut in halves first.
    ///
    /// A batch of no rows leaves one of no rows, of the stage's output schema; a batch whose rows
    /// a filter drops every one of leaves none.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] where a transform fails, breaks its contract or its bound, meets a batch
    /// flagging columns unchanged that it cannot carry, or a row alone takes more than `free`
    /// leaves.
    pub(crate) fn run(&self, chain: &Chain, batch: RecordBatch, free: u64) -> Result<Ran, Refusal> {
        let arrived_empty = batch.num_rows() == 0;
        let mut pending = VecDeque::from([batch]);
        let mut ran = Ran::default();
        while let Some(part) = pending.pop_front() {
            match self.part(chain, &part, free.saturating_sub(ran.held))? {
                Part::Done(done) => {
                    ran.held = ran.held.saturating_add(done.held);
                    ran.dropped += done.dropped;
                    ran.deleted += done.deleted;
                    let left = done
                        .batches
                        .into_iter()
                        .filter(|batch| arrived_empty || batch.num_rows() > 0);
                    ran.batches.extend(left);
                }
                Part::Split => {
                    let half = part.num_rows() / 2;
                    pending.push_front(part.slice(half, part.num_rows() - half));
                    pending.push_front(part.slice(0, half));
                }
            }
        }
        Ok(ran)
    }

    fn includes(&self, name: &str) -> bool {
        self.included
            .as_ref()
            .is_none_or(|included| included.contains(name))
    }

    /// Refuses `output` where it lacks a column the stream keeps as `input` holds it, or holds
    /// it as another type.
    fn check_kept(&self, input: &Schema, output: &Schema) -> Result<(), Refusal> {
        for name in &self.kept {
            let Ok(arrived) = input.field_with_name(name) else {
                continue;
            };
            match output.field_with_name(name) {
                Ok(left) if left.data_type() == arrived.data_type() => {}
                _ => {
                    return Err(Refusal::Kept {
                        column: name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// `part` through the chain within `free` bytes, or [`Part::Split`] where it does not fit.
    fn part(&self, chain: &Chain, part: &RecordBatch, free: u64) -> Result<Part, Refusal> {
        let remapping = changes::remapping(chain, part.num_rows());
        if remapping > free {
            return too_large(None, part, remapping, free);
        }
        let (mut data, mut changes) = changes::split(part, chain)?;
        let mut held = changes.as_ref().map_or(0, |changes| changes.beyond(part));
        if !chain.encoded.is_empty() {
            let room = decoding::room(&data, &chain.encoded, free.saturating_sub(held))?;
            if held.saturating_add(room) > free {
                return too_large(None, part, held.saturating_add(room), free);
            }
            data = decoding::decode(&data, &chain.encoded, &chain.schemas[0])?;
            held = room::held(part, &data, changes.as_ref());
        }
        let mut done = Ran::default();
        for (index, step) in self.steps.iter().enumerate() {
            let transform = step.as_transform();
            let (declared, schema) = (&chain.declared[index], &chain.schemas[index + 1]);
            let bounded = declared.bound.is_some();
            let stepping = (transform, declared, self.rule);
            let room = room::step_room(stepping, &data, changes.as_ref(), schema);
            if bounded && held.saturating_add(room) > free {
                return too_large(Some(transform), part, held.saturating_add(room), free);
            }
            if let Some(changes) = &changes {
                carried(transform, declared, changes)?;
            }
            if transform.filters() {
                let keep = kept(transform, bounded, &data)?;
                (data, changes) = self.filtered(data, changes, &keep, &mut done)?;
            }
            let output = applied(transform, declared, &data, schema)?;
            changes = match (changes, &declared.lineage) {
                (Some(changes), Some(lineage)) if changes.flags_any_of(None) => {
                    let mixed = || Refusal::Mixed {
                        transform: transform.name().to_owned(),
                    };
                    Some(changes.relined(lineage).ok_or_else(mixed)?)
                }
                (changes, _) => changes,
            };
            data = output;
            held = room::held(part, &data, changes.as_ref());
            if !bounded && held > free {
                return too_large(Some(transform), part, held, free);
            }
        }
        let batch = changes::join(data, changes, chain)?;
        done.held =
            allocated_beyond(&batch, part).saturating_add(arrays(batch.schema_ref().fields()));
        done.batches.push(batch);
        Ok(Part::Done(done))
    }

    /// `data` and its `changes` as the stream's filter rule leaves the rows `keep` does not
    /// keep, counted in `done`.
    fn filtered(
        &self,
        data: RecordBatch,
        changes: Option<Changes>,
        keep: &BooleanArray,
        done: &mut Ran,
    ) -> Result<(RecordBatch, Option<Changes>), Refusal> {
        if keep.true_count() == keep.len() {
            return Ok((data, changes));
        }
        let failed = |error: ArrowError| internal("filtering a batch", &error);
        let rows = u64::try_from(data.num_rows()).unwrap_or(u64::MAX);
        let left = |passing: &BooleanArray| u64::try_from(passing.true_count()).unwrap_or(rows);
        match (self.rule, changes) {
            (FilterRule::Delete, Some(changes)) => {
                let (changes, deleted) = changes.deleting(keep);
                done.deleted += deleted;
                Ok((data, Some(changes)))
            }
            (FilterRule::Drop, Some(changes)) => {
                let passing = changes.passing(keep);
                done.dropped += rows - left(&passing);
                let data = filter::filter_record_batch(&data, &passing).map_err(failed)?;
                Ok((data, Some(changes.filter(&passing).map_err(failed)?)))
            }
            (FilterRule::Delete | FilterRule::Drop, None) => {
                done.dropped += rows - left(keep);
                Ok((
                    filter::filter_record_batch(&data, keep).map_err(failed)?,
                    None,
                ))
            }
        }
    }
}

impl Chain {
    /// The input schema the chain was made for.
    pub(crate) fn input(&self) -> &SchemaRef {
        &self.input
    }

    /// The transforms' output, data columns alone.
    pub(crate) fn data_output(&self) -> &SchemaRef {
        self.schemas.last().expect("a chain starts at its input")
    }
}

fn internal(what: &str, error: &ArrowError) -> Refusal {
    Refusal::Internal(format!("{what}: {error}"))
}

/// A part that does not fit in `free` bytes, as `transform`, or the stage's own work where that
/// is none, asks `asked`: cut in two, or refused where it holds one row.
fn too_large(
    transform: Option<&dyn Transform>,
    part: &RecordBatch,
    asked: u64,
    free: u64,
) -> Result<Part, Refusal> {
    if part.num_rows() > 1 {
        return Ok(Part::Split);
    }
    let transform = transform.map(|transform| transform.name().to_owned());
    Err(Refusal::Row {
        transform,
        asked,
        free,
    })
}
