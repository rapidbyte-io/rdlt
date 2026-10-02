//! Merging a history table's rows: each key keeps every version, and a change closes its key's
//! current version where the change begins, or leaves it as it is (SCD2).

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_row::Rows;
use arrow_schema::{ArrowError, DataType, SchemaRef};
use rdlt_connector::{ChangeOp, Deletion, HistoryColumns, MergeKey};

use super::changes::stored;
use super::retype::retyped;
use super::sparse::{self, At, Base, Nulls, Pick, Sources};
use super::tombstones::{self, Tombstones};
use super::written::ops;
use super::{converter, source_bytes, source_keys, source_seqs, sources};

/// A row a version's values come from, of the merge's sources.
type Row = At;

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
    /// Whether the merge opened or closed it: a version it left is its published row whole.
    touched: bool,
}

/// A key's newest version's row, which guards a change stream's key, its current version, how
/// many of the table's truncates it has met, and whether one of them opened its newest version.
#[derive(Debug)]
struct Key {
    newest: Row,
    current: Option<usize>,
    met: usize,
    /// A version a truncate opened carries the truncate's sequence, and a change of its key at
    /// that sequence is not before the truncate: it applies.
    truncated: bool,
}

/// The sequences, hashes and deletion times of each of the merge's sources.
struct Guards {
    seqs: Vec<ArrayRef>,
    hashes: Vec<ArrayRef>,
    /// Where deletes are soft, the deletion times.
    at: Option<Vec<ArrayRef>>,
}

impl Guards {
    fn seq(&self, row: Row) -> &[u8] {
        self.seqs[row.source].as_binary::<i32>().value(row.row)
    }

    fn hash(&self, row: Row) -> Option<&[u8]> {
        let hashes = self.hashes[row.source].as_binary::<i32>();
        hashes.is_valid(row.row).then(|| hashes.value(row.row))
    }

    /// Whether `row` deleted what it holds.
    fn deleted(&self, row: Row) -> bool {
        self.at
            .as_ref()
            .is_some_and(|at| at[row.source].is_valid(row.row))
    }
}

/// A history table's versions, and the rows they come from.
struct Versions {
    list: Vec<Version>,
    sources: Guards,
}

impl Versions {
    /// Closes the version at `index` where `by` begins.
    fn close(&mut self, index: usize, by: Row) {
        let version = &mut self.list[index];
        version.until = Some(by);
        version.current = false;
        version.touched = true;
    }

    /// Publishes a current version of `data` that `opened` published as `key`'s.
    fn open(&mut self, key: &mut Key, data: Row, opened: Row) {
        key.current = Some(self.list.len());
        self.list.push(Version {
            data,
            opened,
            until: None,
            current: true,
            touched: true,
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
        state.truncated = true;
    }
}

impl History {
    /// The versions the first `published` of `sources` hold, keyed by `keys`, each source's.
    fn load(
        sources: &Sources,
        published: usize,
        keys: &[Rows],
        (history, guards): (&HistoryColumns, Guards),
        nulls: &mut Nulls,
    ) -> Result<Self, ArrowError> {
        let is_current = sources.schema().index_of(&history.is_current)?;
        let mut loaded = Self {
            versions: Versions {
                list: Vec::new(),
                sources: guards,
            },
            keys: BTreeMap::new(),
            truncates: Vec::new(),
        };
        for (source, keys) in keys.iter().enumerate().take(published) {
            let flags = sources.dense(source, is_current, nulls);
            let flags = retyped(&flags, &DataType::Boolean)?;
            let flags = flags.as_boolean();
            for row in 0..sources.rows(source) {
                let at = At { source, row };
                let current = flags.is_valid(row) && flags.value(row);
                loaded.hold(keys.row(row).as_ref(), at, current);
            }
        }
        Ok(loaded)
    }

    /// Adds the published version at `at`, of `key`, which is its key's current one or not.
    fn hold(&mut self, key: &[u8], at: Row, current: bool) {
        let index = self.versions.list.len();
        self.versions.list.push(Version {
            data: at,
            opened: at,
            until: Some(at),
            current,
            touched: false,
        });
        let sources = &self.versions.sources;
        if let Some(state) = self.keys.get_mut(key) {
            if sources.seq(at) > sources.seq(state.newest) {
                state.newest = at;
            }
            if current {
                state.current = Some(index);
            }
        } else {
            let state = Key {
                newest: at,
                current: current.then_some(index),
                met: 0,
                truncated: false,
            };
            self.keys.insert(key.to_vec(), state);
        }
    }

    /// Whether a change of `key` at `by` is sequenced past the key's newest version, once the key
    /// has met the truncates so far.
    fn past(&mut self, key: &[u8], by: Row) -> bool {
        let Some(state) = self.keys.get_mut(key) else {
            return true;
        };
        meet(&mut self.versions, &self.truncates, state);
        let sources = &self.versions.sources;
        match sources.seq(state.newest).cmp(sources.seq(by)) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => state.truncated,
            std::cmp::Ordering::Greater => false,
        }
    }

    fn upsert(&mut self, key: &[u8], by: Row) {
        if let Some(state) = self.keys.get_mut(key) {
            meet(&mut self.versions, &self.truncates, state);
            self.versions.upsert(state, by);
            state.truncated = false;
        } else {
            let mut state = Key {
                newest: by,
                current: None,
                met: self.truncates.len(),
                truncated: false,
            };
            self.versions.upsert(&mut state, by);
            self.keys.insert(key.to_vec(), state);
        }
    }

    fn delete(&mut self, key: &[u8], by: Row) {
        if let Some(state) = self.keys.get_mut(key) {
            meet(&mut self.versions, &self.truncates, state);
            self.versions.remove(state, by);
            state.truncated = false;
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
    at: Row,
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
    let (mut sources, held) = sources(&schema, published, incoming)?;
    let keys = source_keys(&sources, &converter, key, &mut nulls)?;
    let guards = guards(&sources, (key, history), &mut nulls)?;
    let mut table = History::load(&sources, held, &keys, (history, guards), &mut nulls)?;
    let tombstone_schema = tombstones::schema(&schema, key)?;
    let buried = if key.changes.is_some() { buried } else { &[] };
    let mut tombstones = Tombstones::load(buried, &tombstone_schema, &converter, key)?;
    for change in changes(incoming, held, key, &table.versions.sources)? {
        apply(&mut table, &mut tombstones, &keys, &change, key);
    }
    let versions = table.finish();
    let buried = tombstones.assemble(&tombstone_schema, &sources, key, &mut nulls)?;
    let merged = assemble(&mut sources, held, &versions, (key, history))?;
    Ok((merged, buried))
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
    let by = change.at;
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
    let row_key = keys[by.source].row(by.row);
    let row_key = row_key.as_ref();
    let guarded = key.changes.is_some();
    if guarded && !(tombstones.admits(Some(row_key), &seq) && table.past(row_key, by)) {
        return;
    }
    if change.op == ChangeOp::Delete {
        table.delete(row_key, by);
        if hard {
            tombstones.bury(row_key.to_vec(), seq, by);
        }
    } else {
        table.upsert(row_key, by);
        // A version sequenced past the key's tombstone holds the key now.
        tombstones.lift(row_key);
    }
}

/// The column a change stream whose deletes are soft records deletion times in.
fn deleted_at(key: &MergeKey) -> Option<&str> {
    match key.changes.as_ref().map(|changes| &changes.deletion) {
        Some(Deletion::Soft { at }) => Some(at),
        _ => None,
    }
}

/// The sequences, hashes and, where deletes are soft, deletion times of each of `sources`.
fn guards(
    sources: &Sources,
    (key, history): (&MergeKey, &HistoryColumns),
    nulls: &mut Nulls,
) -> Result<Guards, ArrowError> {
    let times = match deleted_at(key) {
        Some(at) => {
            let column = sources.schema().index_of(at)?;
            let times = (0..sources.len()).map(|source| sources.dense(source, column, nulls));
            Some(times.collect())
        }
        None => None,
    };
    Ok(Guards {
        seqs: source_seqs(sources, key, nulls)?,
        hashes: source_bytes(sources, &history.row_hash, nulls)?,
        at: times,
    })
}

/// Every row of `incoming`, the sources from `first` on, in sequence order, with its op: a
/// change stream's names it, and every other row is an upsert.
fn changes(
    incoming: &[RecordBatch],
    first: usize,
    key: &MergeKey,
    sources: &Guards,
) -> Result<Vec<Change>, ArrowError> {
    let mut rows = Vec::new();
    for (index, batch) in incoming.iter().enumerate() {
        let ops = match &key.changes {
            Some(changes) => ops(batch, changes)?,
            None => vec![ChangeOp::Update; batch.num_rows()],
        };
        let source = first + index;
        rows.extend(ops.into_iter().enumerate().map(|(row, op)| Change {
            at: At { source, row },
            op,
        }));
    }
    // A truncate closes what is sequenced before it, and a key's change at its own sequence
    // is not before it: the truncate applies first, wherever it was written. A stable sort
    // keeps the other rows of one sequence in the order they were written.
    rows.sort_by(|left, right| {
        let placed = |change: &Change| (sources.seq(change.at), change.op != ChangeOp::Truncate);
        placed(left).cmp(&placed(right))
    });
    Ok(rows)
}

/// The versions as batches of the columns they hold: a version the merge left is its published
/// row whole; of another, each column comes from the rows its values come from, `valid_to` from
/// the row that closed it and `is_current` from its flag.
fn assemble(
    sources: &mut Sources,
    published: usize,
    versions: &Versions,
    (key, history): (&MergeKey, &HistoryColumns),
) -> Result<Vec<RecordBatch>, ArrowError> {
    let schema = Arc::clone(sources.schema());
    let column = |name: &str| schema.index_of(name);
    let (from, to) = (column(&history.valid_from)?, column(&history.valid_to)?);
    let current = column(&history.is_current)?;
    let mut opened = vec![column(&key.seq)?, from];
    if let Some(at) = deleted_at(key) {
        opened.push(column(at)?);
    }
    // A version ends where the row that closed it begins: each incoming source's `valid_from`
    // stands as a source of `valid_to`, and a source of two rows holds the two flags.
    let mut closing: BTreeMap<usize, usize> = BTreeMap::new();
    for source in published..sources.len() {
        if let Some(begins) = sources.column(source, from) {
            let ends = retyped(begins, schema.field(to).data_type())?;
            let rows = sources.rows(source);
            closing.insert(source, sources.add_held(rows, vec![(to, ends)]));
        }
    }
    let flags: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));
    let flag = sources.add_held(2, vec![(current, flags)]);
    let over: Vec<Vec<(usize, Option<At>)>> = versions
        .list
        .iter()
        .map(|version| {
            if !version.touched {
                return Vec::new();
            }
            // A published row closed it and it keeps that row's own end, or an incoming row did
            // and it ends where that begins; open, it has no end.
            let until = version
                .until
                .and_then(|until| match closing.get(&until.source) {
                    Some(shadow) => Some(At {
                        source: *shadow,
                        row: until.row,
                    }),
                    None if until.source < published => Some(until),
                    None => None,
                });
            let mut over: Vec<(usize, Option<At>)> = opened
                .iter()
                .map(|column| (*column, Some(version.opened)))
                .collect();
            over.push((to, until));
            let flagged = At {
                source: flag,
                row: usize::from(version.current),
            };
            over.push((current, Some(flagged)));
            over.sort_by_key(|(column, _)| *column);
            over
        })
        .collect();
    let picks = versions.list.iter().zip(&over).map(|(version, over)| Pick {
        base: Base::Row(version.data),
        over,
    });
    sparse::assemble(sources, picks)
}
