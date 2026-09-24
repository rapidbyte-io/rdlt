//! What the oracle reads from the store: published rows by source column, committed cursors and
//! completed full reads.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{
    LogicalType, NameMap, PartitionId, PartitionState, StateEntry, StateKey, StateRecord,
    StreamName, TablePath,
};
use serde_json::{Map, Value};

use super::cells;
use crate::source::SimCursor;
use crate::world::World;

/// Every row published for `stream`, with each source column's value gathered from its column
/// and variant columns through the committed name map: the rows as the source sent them.
///
/// JSON a destination stored as text, having no JSON type, is read back through the committed
/// schema's types. A value found in two columns of one source column is a violation.
pub fn published(world: &World, stream: &str) -> Vec<Map<String, Value>> {
    let store = world.store.lock();
    let Ok(path) = TablePath::new([stream]) else {
        return Vec::new();
    };
    let Some((physical, names)) = names(&store.state, &path) else {
        return Vec::new();
    };
    let Some(table) = store.tables.get(physical.as_str()) else {
        return Vec::new();
    };
    let types = logical_types(&store.state, &path);
    table
        .published
        .iter()
        .map(|row| {
            cells::unlowered(row, &types, &table.columns)
                .and_then(|row| cells::source_row(&row, &names))
                .unwrap_or_else(|finding| {
                    world.violation(format!("stream {stream}: {finding}"));
                    Map::new()
                })
        })
        .collect()
}

/// The logical type of each column of the table at `path`, by identifier, as state records it.
fn logical_types(
    state: &BTreeMap<String, StateRecord>,
    path: &TablePath,
) -> BTreeMap<String, LogicalType> {
    let record = state.get(&StateKey::Schema(path.clone()).encode());
    match record.and_then(|record| StateEntry::from_record(record).ok()) {
        Some(StateEntry::Schema { schema, .. }) => schema
            .fields()
            .iter()
            .map(|field| (field.name().to_owned(), field.logical_type().clone()))
            .collect(),
        _ => BTreeMap::new(),
    }
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
