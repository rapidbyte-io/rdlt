//! Merging a history table's rows: each key keeps every version, and a change closes its key's
//! current version where the change begins, or leaves it as it is (SCD2).

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_row::{RowConverter, Rows};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use rdlt_connector::{ChangeOp, Deletion, HistoryColumns, MergeKey};

use super::aligned::{Nulls, aligned, concat, interleaved};
use super::changes::{nullable, stored};
use super::retype::retyped;
use super::tombstones::{self, Tombstones};
use super::written::ops;
use super::{binary, converter, held, key_columns};

/// A row a version's values come from.
#[derive(Clone, Copy, Debug)]
enum Row {
    /// The published row at this index.
    Published(usize),
    /// Row `.1` of incoming batch `.0`.
    Incoming(usize, usize),
}

impl Row {
    /// Where the row sits among the sources: the published batch, then each incoming batch.
    fn slot(self) -> (usize, usize) {
        match self {
            Self::Published(row) => (0, row),
            Self::Incoming(batch, row) => (batch + 1, row),
        }
    }
}

/// One version of a key, by the rows its values come from.
#[derive(Clone, Copy, Debug)]
struct Version {
    /// The row holding its data, key and hash.
    data: Row,
    /// The row that published it, holding its sequence, `valid_from` and deletion time: `data`,
    /// or the delete of a version a soft delete kept.
    opened: Row,
    /// A published row, whose `valid_to` it keeps, or the row that closed it, whose `valid_from`
    /// it takes; none while it is open.
    until: Option<Row>,
    current: bool,
}

/// A key's newest version's row, which guards a change stream's key, its current version, and
/// how many of the table's truncates it has met.
#[derive(Debug)]
struct Key {
    newest: Row,
    current: Option<usize>,
    met: usize,
}

/// The sequences, hashes and deletion times of the published batch and each incoming one.
struct Sources {
    seqs: Vec<ArrayRef>,
    hashes: Vec<ArrayRef>,
    /// Where deletes are soft, the deletion times.
    at: Option<Vec<ArrayRef>>,
}

impl Sources {
    fn seq(&self, row: Row) -> &[u8] {
        let (batch, row) = row.slot();
        self.seqs[batch].as_binary::<i32>().value(row)
    }

    fn hash(&self, row: Row) -> Option<&[u8]> {
        let (batch, row) = row.slot();
        let hashes = self.hashes[batch].as_binary::<i32>();
        hashes.is_valid(row).then(|| hashes.value(row))
    }

    /// Whether `row` deleted what it holds.
    fn deleted(&self, row: Row) -> bool {
        let (batch, row) = row.slot();
        self.at.as_ref().is_some_and(|at| at[batch].is_valid(row))
    }
}

/// A history table's versions, and the rows they come from.
struct Versions {
    list: Vec<Version>,
    sources: Sources,
}

impl Versions {
    /// Closes the version at `index` where `by` begins.
    fn close(&mut self, index: usize, by: Row) {
        let version = &mut self.list[index];
        version.until = Some(by);
        version.current = false;
    }

    /// Publishes a current version of `data` that `opened` published as `key`'s.
    fn open(&mut self, key: &mut Key, data: Row, opened: Row) {
        key.current = Some(self.list.len());
        self.list.push(Version {
            data,
            opened,
            until: None,
            current: true,
        });
        // A change stream opens a version only past its key's newest; a plain table never asks.
        key.newest = opened;
    }

    /// Applies the upsert `by` to `key`: unless its hash is the current version's, which is not
    /// deleted, it closes that version and becomes the current one.
    fn upsert(&mut self, key: &mut Key, by: Row) {
        if let Some(index) = key.current {
            let version = self.list[index];
            let sources = &self.sources;
            if !sources.deleted(version.opened) && sources.hash(version.data) == sources.hash(by) {
                return;
            }
            self.close(index, by);
        }
        self.open(key, by, by);
    }

    /// Applies the delete `by` to `key`'s current version: closes it, and where deletes are
    /// soft, keeps its data in a deleted current version, unless it is deleted already.
    fn remove(&mut self, key: &mut Key, by: Row) {
        let Some(index) = key.current else {
            return;
        };
        let version = self.list[index];
        if self.sources.at.is_none() {
            self.close(index, by);
            key.current = None;
        } else if !self.sources.deleted(version.opened) {
            self.close(index, by);
            self.open(key, version.data, by);
        }
    }
}

/// A history table while its changes apply: its versions, each key's, and the truncates applied
/// so far, in sequence order, which a key meets when it is next touched.
struct History {
    versions: Versions,
    keys: BTreeMap<Vec<u8>, Key>,
    truncates: Vec<Row>,
}

/// Applies to `state` the truncates it has not met: the first sequenced past its current version
/// does to it what a delete does, and those after it find it removed or deleted already.
fn meet(versions: &mut Versions, truncates: &[Row], state: &mut Key) {
    let pending = &truncates[state.met..];
    state.met = truncates.len();
    let Some(index) = state.current else {
        return;
    };
    let sources = &versions.sources;
    let opened = sources.seq(versions.list[index].opened);
    let first = pending.partition_point(|by| sources.seq(*by) <= opened);
    if let Some(by) = pending.get(first) {
        versions.remove(state, *by);
    }
}

impl History {
    /// The versions `published` holds, keyed as `converter` encodes `key`.
    fn load(
        published: &RecordBatch,
        converter: &RowConverter,
        (key, history): (&MergeKey, &HistoryColumns),
        sources: Sources,
    ) -> Result<Self, ArrowError> {
        let keys = converter.convert_columns(&key_columns(published, key)?)?;
        let flags = published
            .column_by_name(&history.is_current)
            .ok_or_else(|| ArrowError::SchemaError(format!("no column {}", history.is_current)))?;
        let flags = retyped(flags, &DataType::Boolean)?;
        let flags = flags.as_boolean();
        let mut loaded = Self {
            versions: Versions {
                list: Vec::with_capacity(published.num_rows()),
                sources,
            },
            keys: BTreeMap::new(),
            truncates: Vec::new(),
        };
        for row in 0..published.num_rows() {
            let at = Row::Published(row);
            let current = flags.is_valid(row) && flags.value(row);
            let index = loaded.versions.list.len();
            loaded.versions.list.push(Version {
                data: at,
                opened: at,
                until: Some(at),
                current,
            });
            let sources = &loaded.versions.sources;
            let key = keys.row(row);
            if let Some(state) = loaded.keys.get_mut(key.as_ref()) {
                state.newest = [state.newest, at]
                    .into_iter()
                    .max_by_key(|row| sources.seq(*row))
                    .unwrap_or(at);
                if current {
                    state.current = Some(index);
                }
            } else {
                let current = current.then_some(index);
                let state = Key {
                    newest: at,
                    current,
                    met: 0,
                };
                loaded.keys.insert(key.as_ref().to_vec(), state);
            }
        }
        Ok(loaded)
    }

    /// Whether a change of `key` at `by` is sequenced past the key's newest version, once the key
    /// has met the truncates so far.
    fn past(&mut self, key: &[u8], by: Row) -> bool {
        let Some(state) = self.keys.get_mut(key) else {
            return true;
        };
        meet(&mut self.versions, &self.truncates, state);
        let sources = &self.versions.sources;
        sources.seq(state.newest) < sources.seq(by)
    }

    fn upsert(&mut self, key: &[u8], by: Row) {
        if let Some(state) = self.keys.get_mut(key) {
            meet(&mut self.versions, &self.truncates, state);
            self.versions.upsert(state, by);
        } else {
            let mut state = Key {
                newest: by,
                current: None,
                met: self.truncates.len(),
            };
            self.versions.upsert(&mut state, by);
            self.keys.insert(key.to_vec(), state);
        }
    }

    fn delete(&mut self, key: &[u8], by: Row) {
        if let Some(state) = self.keys.get_mut(key) {
            meet(&mut self.versions, &self.truncates, state);
            self.versions.remove(state, by);
        }
    }

    /// Records the truncate `by`, which does to every current version sequenced before it what
    /// a delete does, when its key next meets the table's truncates.
    fn truncate(&mut self, by: Row) {
        self.truncates.push(by);
    }

    /// The table's versions once every key has met every truncate.
    fn finish(mut self) -> Versions {
        for state in self.keys.values_mut() {
            meet(&mut self.versions, &self.truncates, state);
        }
        self.versions
    }
}

/// One incoming row, and what it does.
struct Change {
    batch: usize,
    row: usize,
    op: ChangeOp,
}

/// The published versions of a history table, and its tombstones, once `incoming` applies, row
/// by row in sequence order, to `published` and, for a change stream's, `buried`.
pub(crate) fn merge_history(
    schema: &SchemaRef,
    published: &[RecordBatch],
    buried: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
    history: &HistoryColumns,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>), ArrowError> {
    let schema = key
        .changes
        .as_ref()
        .map_or_else(|| Arc::clone(schema), |changes| stored(schema, changes));
    let converter = converter(&schema, key)?;
    let mut nulls = Nulls::default();
    let published = concat(published, &schema, &mut nulls)?;
    let nullable = nullable(&schema);
    let aligned = incoming
        .iter()
        .map(|batch| aligned(batch, &nullable, &mut nulls))
        .collect::<Result<Vec<_>, _>>()?;
    let keys = aligned
        .iter()
        .map(|batch| converter.convert_columns(&key_columns(batch, key)?))
        .collect::<Result<Vec<Rows>, _>>()?;
    let sources = sources(&published, &aligned, (key, history))?;
    let mut table = History::load(&published, &converter, (key, history), sources)?;
    let tombstone_schema = tombstones::schema(&schema, key)?;
    let buried = if key.changes.is_some() { buried } else { &[] };
    let mut tombstones = Tombstones::load(buried, &tombstone_schema, &converter, key)?;
    for change in changes(incoming, key, &table.versions.sources)? {
        apply(&mut table, &mut tombstones, &keys, &change, key);
    }
    let versions = table.finish();
    let merged = assemble(
        &schema,
        [std::slice::from_ref(&published), &aligned],
        &versions,
        (key, history),
        &mut nulls,
    )?;
    let buried = tombstones.assemble(&tombstone_schema, &aligned, key)?;
    Ok((held([merged]), buried))
}

/// Applies `change` to `table` and `tombstones`, where it is sequenced past its key's newest
/// version, tombstone and the bound, or `key` names no change stream, which guards nothing.
fn apply(
    table: &mut History,
    tombstones: &mut Tombstones,
    keys: &[Rows],
    change: &Change,
    key: &MergeKey,
) {
    let by = Row::Incoming(change.batch, change.row);
    let seq = table.versions.sources.seq(by).to_vec();
    let hard = table.versions.sources.at.is_none();
    if change.op == ChangeOp::Truncate {
        if tombstones.admits(None, &seq) {
            table.truncate(by);
            if hard {
                tombstones.raise(seq);
            }
        }
        return;
    }
    let row_key = keys[change.batch].row(change.row);
    let row_key = row_key.as_ref();
    let guarded = key.changes.is_some();
    if guarded && !(tombstones.admits(Some(row_key), &seq) && table.past(row_key, by)) {
        return;
    }
    if change.op == ChangeOp::Delete {
        table.delete(row_key, by);
        if hard {
            tombstones.bury(row_key.to_vec(), seq, change.batch, change.row);
        }
    } else {
        table.upsert(row_key, by);
    }
}

/// The column a change stream whose deletes are soft records deletion times in.
fn deleted_at(key: &MergeKey) -> Option<&str> {
    match key.changes.as_ref().map(|changes| &changes.deletion) {
        Some(Deletion::Soft { at }) => Some(at),
        _ => None,
    }
}

/// The sequences, hashes and, where deletes are soft, deletion times of `published` and
/// `aligned`.
fn sources(
    published: &RecordBatch,
    aligned: &[RecordBatch],
    (key, history): (&MergeKey, &HistoryColumns),
) -> Result<Sources, ArrowError> {
    let batches = || std::iter::once(published).chain(aligned);
    let column = |batch: &RecordBatch, name: &str| {
        batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| ArrowError::SchemaError(format!("no column {name}")))
    };
    Ok(Sources {
        seqs: batches()
            .map(|batch| binary(batch, &key.seq))
            .collect::<Result<_, _>>()?,
        hashes: batches()
            .map(|batch| binary(batch, &history.row_hash))
            .collect::<Result<_, _>>()?,
        at: deleted_at(key)
            .map(|at| batches().map(|batch| column(batch, at)).collect())
            .transpose()?,
    })
}

/// Every row of `incoming`, in sequence order, with its op: a change stream's names it, and
/// every other row is an upsert.
fn changes(
    incoming: &[RecordBatch],
    key: &MergeKey,
    sources: &Sources,
) -> Result<Vec<Change>, ArrowError> {
    let mut rows = Vec::new();
    for (index, batch) in incoming.iter().enumerate() {
        let ops = match &key.changes {
            Some(changes) => ops(batch, changes)?,
            None => vec![ChangeOp::Update; batch.num_rows()],
        };
        rows.extend(ops.into_iter().enumerate().map(|(row, op)| Change {
            batch: index,
            row,
            op,
        }));
    }
    // A stable sort keeps rows of one sequence in the order they were written.
    rows.sort_by(|left, right| {
        let seq = |change: &Change| sources.seq(Row::Incoming(change.batch, change.row));
        seq(left).cmp(seq(right))
    });
    Ok(rows)
}

/// The versions as one batch of `schema`: each column from the rows its versions' values come
/// from, of the published batch and the incoming ones, `valid_to` from the rows that closed them
/// and `is_current` from their flags.
fn assemble(
    schema: &SchemaRef,
    [published, aligned]: [&[RecordBatch]; 2],
    versions: &Versions,
    (key, history): (&MergeKey, &HistoryColumns),
    nulls: &mut Nulls,
) -> Result<RecordBatch, ArrowError> {
    let list = &versions.list;
    let opened = [Some(&*key.seq), Some(&*history.valid_from), deleted_at(key)];
    let from = schema.index_of(&history.valid_from)?;
    let batches = || published.iter().chain(aligned);
    let absent = published.len() + aligned.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (column, field) in schema.fields().iter().enumerate() {
        let name = field.name().as_str();
        if name == &*history.is_current {
            let flags: BooleanArray = list.iter().map(|version| Some(version.current)).collect();
            columns.push(Arc::new(flags));
            continue;
        }
        let null = nulls.of(field.data_type(), 1);
        let mut values: Vec<ArrayRef> = batches()
            .map(|batch| Arc::clone(batch.column(column)))
            .collect();
        let rows: Vec<Option<Row>> = if name == &*history.valid_to {
            // A version ends where the row that closed it begins; a published one where it did.
            for (value, batch) in values.iter_mut().zip(batches()).skip(published.len()) {
                *value = retyped(batch.column(from), field.data_type())?;
            }
            list.iter().map(|version| version.until).collect()
        } else if opened.contains(&Some(name)) {
            list.iter().map(|version| Some(version.opened)).collect()
        } else {
            list.iter().map(|version| Some(version.data)).collect()
        };
        let mut sources: Vec<&dyn Array> = values.iter().map(AsRef::as_ref).collect();
        sources.push(null.as_ref());
        let indices: Vec<(usize, usize)> = rows
            .into_iter()
            .map(|row| row.map_or((absent, 0), Row::slot))
            .collect();
        columns.push(interleaved(&sources, &indices, field.data_type(), nulls)?);
    }
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(list.len())),
    )
}
