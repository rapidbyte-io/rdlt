//! What the oracle reads from the store: published tables, committed cursors and completed full
//! reads.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{
    NameMap, PartitionId, PartitionState, StateEntry, StateKey, StateRecord, StreamName, TablePath,
};

use super::Stored;
use crate::source::SimCursor;
use crate::world::World;

/// What a table holds: its rows, and the name map naming its columns.
#[derive(Debug)]
pub(crate) struct Published {
    /// The table's identifier.
    pub(crate) physical: String,
    /// Each source column's identifiers, and its variants'.
    pub(crate) names: NameMap,
    /// Its rows.
    pub(crate) rows: Vec<Stored>,
}

/// What the table at `path` holds, if the destination has it.
pub(crate) fn published_table(world: &World, path: &TablePath) -> Option<Published> {
    let store = world.store.lock();
    let (physical, names) = names(&store.state, path)?;
    let rows = store.tables.get(physical.as_str())?.published.clone();
    Some(Published {
        physical,
        names,
        rows,
    })
}

/// The paths of every table the destination holds for `stream`: its own and its child tables.
pub(crate) fn table_paths(world: &World, stream: &str) -> Vec<Vec<String>> {
    let store = world.store.lock();
    store
        .names
        .keys()
        .map(|path| {
            path.segments()
                .map(ToOwned::to_owned)
                .collect::<Vec<String>>()
        })
        .filter(|segments| segments.first().map(String::as_str) == Some(stream))
        .collect()
}

/// The identifier and name map state records for the table at `path`.
pub(super) fn names(
    state: &BTreeMap<String, StateRecord>,
    path: &TablePath,
) -> Option<(String, NameMap)> {
    let record = state.get(&StateKey::Names(path.clone()).encode())?;
    match StateEntry::from_record(record).ok()? {
        StateEntry::Names {
            physical, names, ..
        } => Some((physical.to_string(), names)),
        _ => None,
    }
}

/// Whether state records a full read in progress.
pub(crate) fn reads_in_progress(world: &World) -> bool {
    let store = world.store.lock();
    store
        .state
        .keys()
        .any(|key| matches!(StateKey::parse(key), Ok(StateKey::Generation(_))))
}

/// Distinct full reads of `stream` completed in `phase`.
pub fn completions(world: &World, stream: &str, phase: usize) -> usize {
    let store = world.store.lock();
    store
        .completions
        .get(&(stream.to_owned(), phase))
        .map_or(0, BTreeSet::len)
}

/// The committed resume offset of a partition: `u64::MAX` once it is done.
pub(crate) fn committed_next(
    world: &World,
    stream: &StreamName,
    partition: &PartitionId,
) -> Option<u64> {
    let store = world.store.lock();
    next_offset(&store.state, stream, partition)
}

pub(super) fn next_offset(
    state: &BTreeMap<String, StateRecord>,
    stream: &StreamName,
    partition: &PartitionId,
) -> Option<u64> {
    let key = StateKey::Partition(stream.clone(), partition.clone()).encode();
    let entry = StateEntry::from_record(state.get(&key)?).ok()?;
    match entry {
        StateEntry::Partition {
            state: PartitionState::Cursor(cursor),
            ..
        } => cursor.decode::<SimCursor>(1).ok().map(|cursor| cursor.next),
        StateEntry::Partition {
            state: PartitionState::Done,
            ..
        } => Some(u64::MAX),
        _ => None,
    }
}
