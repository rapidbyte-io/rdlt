//! The history a timed stream of whole rows leaves in a history table.

use std::collections::BTreeMap;

use super::{Change, ChangedStream, change, snapshot};

/// One version of a key in a history table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    /// The key.
    pub id: i64,
    /// When the version began: its change's position, as its change time holds it in microseconds.
    pub from: u64,
    /// Its value.
    pub value: Option<String>,
    /// Its counter.
    pub n: i64,
    /// When a later change closed it; `None` while it is current.
    pub to: Option<u64>,
    /// Whether it is its key's current version.
    pub current: bool,
    /// Whether a soft delete opened it, keeping the data it closed.
    pub deleted: bool,
}

/// Every version `stream`, timed and of whole rows, leaves under `seed`, sorted, where deletes
/// and truncates are `soft` or hard: its snapshot's rows at the captured position, then each
/// change after it, a change equal to its key's live version changing nothing.
pub fn history(seed: u64, stream: &ChangedStream, soft: bool) -> Vec<Version> {
    let mut versions: Vec<Version> = snapshot(seed, stream)
        .into_iter()
        .map(|(id, row)| Version {
            id,
            from: stream.captured,
            value: row.value,
            n: row.n,
            to: None,
            current: true,
            deleted: false,
        })
        .collect();
    let mut current: BTreeMap<i64, usize> = versions
        .iter()
        .enumerate()
        .map(|(index, version)| (version.id, index))
        .collect();
    for position in stream.captured + 1..=stream.changes {
        let removed: Vec<i64> = match change(seed, stream, position) {
            Change::Upsert { id, value, n } => {
                let live = current.get(&id).map(|index| &versions[*index]);
                if live.is_some_and(|live| !live.deleted && live.value == value && live.n == n) {
                    continue;
                }
                if let Some(index) = current.remove(&id) {
                    close(&mut versions[index], position);
                }
                current.insert(id, versions.len());
                versions.push(Version {
                    id,
                    from: position,
                    value,
                    n,
                    to: None,
                    current: true,
                    deleted: false,
                });
                continue;
            }
            Change::Delete { id } => vec![id],
            Change::Truncate => current.keys().copied().collect(),
        };
        for id in removed {
            remove(&mut versions, &mut current, id, position, soft);
        }
    }
    versions.sort();
    versions
}

/// Removes `id`'s live version at `position`: closes it, and where deletes are `soft` opens a
/// deleted version keeping its data, unless it is deleted already.
fn remove(
    versions: &mut Vec<Version>,
    current: &mut BTreeMap<i64, usize>,
    id: i64,
    position: u64,
    soft: bool,
) {
    let Some(index) = current.get(&id).copied() else {
        return;
    };
    if versions[index].deleted {
        return;
    }
    close(&mut versions[index], position);
    current.remove(&id);
    if soft {
        let kept = Version {
            from: position,
            to: None,
            current: true,
            deleted: true,
            ..versions[index].clone()
        };
        current.insert(id, versions.len());
        versions.push(kept);
    }
}

fn close(version: &mut Version, at: u64) {
    version.to = Some(at);
    version.current = false;
}
