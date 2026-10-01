//! The model of a stream's changes: what each change does, and the table they leave.

use std::collections::BTreeMap;

use super::ChangedStream;
use crate::generator::mix;

/// What change `position` of a stream does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// Sets the key's row.
    Upsert {
        /// The key.
        id: i64,
        /// Its value; `None` leaves the value unchanged.
        value: Option<String>,
        /// Its counter: the change's position.
        n: i64,
    },
    /// Removes the key's row.
    Delete {
        /// The key.
        id: i64,
    },
    /// Removes every row.
    Truncate,
}

/// Change `position` (from 1) of `stream` under `seed`.
pub fn change(seed: u64, stream: &ChangedStream, position: u64) -> Change {
    if stream.truncates.contains(&position) {
        return Change::Truncate;
    }
    let draw = mix(seed ^ position.wrapping_mul(0x9E37_79B9));
    // Half again as many keys as the snapshot holds, so changes insert keys too.
    let span = stream
        .keys
        .saturating_add(stream.keys / 2)
        .saturating_add(1);
    let id = i64::try_from(draw % span).unwrap_or(i64::MAX);
    let n = i64::try_from(position).unwrap_or(i64::MAX);
    match (draw >> 32) % 10 {
        0 | 1 => Change::Delete { id },
        2 if stream.partial => Change::Upsert { id, value: None, n },
        _ => Change::Upsert {
            id,
            value: Some(format!("v{position}")),
            n,
        },
    }
}

/// One row of the table a stream's changes leave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    /// Its value.
    pub value: Option<String>,
    /// Its counter.
    pub n: i64,
}

/// The table `stream` holds, by key, once its snapshot and every change apply with deletes
/// removing rows.
pub fn expected(seed: u64, stream: &ChangedStream) -> BTreeMap<i64, Row> {
    let mut table = snapshot(seed, stream);
    let captured = usize::try_from(stream.captured).unwrap_or(usize::MAX);
    for position in (1..=stream.changes).skip(captured) {
        apply(&mut table, change(seed, stream, position));
    }
    table
}

/// The table `stream`'s snapshot holds, by key: its `keys` rows once the first `captured`
/// changes applied.
pub fn snapshot(seed: u64, stream: &ChangedStream) -> BTreeMap<i64, Row> {
    let mut table: BTreeMap<i64, Row> = (0..stream.keys)
        .map(|key| {
            let id = i64::try_from(key).unwrap_or(i64::MAX);
            (id, snapshot_row(id))
        })
        .collect();
    for position in 1..=stream.captured.min(stream.changes) {
        apply(&mut table, change(seed, stream, position));
    }
    table
}

/// Applies `change` to `table`, deletes removing rows.
fn apply(table: &mut BTreeMap<i64, Row>, change: Change) {
    match change {
        Change::Upsert { id, value, n } => {
            let value = value.or_else(|| table.get(&id).and_then(|row| row.value.clone()));
            table.insert(id, Row { value, n });
        }
        Change::Delete { id } => {
            table.remove(&id);
        }
        Change::Truncate => table.clear(),
    }
}

fn snapshot_row(id: i64) -> Row {
    Row {
        value: Some(format!("s{id}")),
        n: 0,
    }
}
