//! The rows a stream's table must hold after a phase, as groups: a merge stream's table holds one
//! row of each key, which may be the last of any partition's rows of that key.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::ReadMode;
use rdlt_engine::WriteMode;

use super::expected::{self, Chance};
use crate::destination::completions;
use crate::seed::Seed;
use crate::workload::{Row, SimStream};
use crate::world::World;

/// Rows a table must hold `count` times in all, any of `rows` each time, or where `at_most`, no
/// more often.
#[derive(Clone, Debug)]
pub(super) struct Group {
    pub(super) rows: Vec<Row>,
    pub(super) count: usize,
    pub(super) at_most: bool,
}

/// The groups of rows `stream`'s table holds after `phase`, as the model has them, each held at
/// most as often where the phase `stopped` short: for a merge stream, one row of each key the
/// policy keeps; otherwise each row the policy keeps, as often as the table holds it.
///
/// A row the policy perhaps drops is held at most as often.
pub(super) fn groups(
    world: &World,
    stream: &SimStream,
    phase: usize,
    stopped: bool,
    seed: Seed,
) -> Vec<Group> {
    if stream.write == WriteMode::Merge {
        return merged(stream, phase, stopped);
    }
    let mut counts: BTreeMap<String, Group> = BTreeMap::new();
    for row in loaded(world, stream, phase, stopped, seed) {
        let dropped = expected::dropped(stream, &row);
        if dropped != Chance::Surely {
            let group = counts.entry(expected::ident(&row)).or_insert(Group {
                rows: vec![row],
                count: 0,
                at_most: stopped || dropped == Chance::Perhaps,
            });
            group.count += 1;
        }
    }
    counts.into_values().collect()
}

/// The rows a stream that does not merge loaded by `phase`, each as often as its table holds it:
/// where the phase `stopped` short, a full read may be in progress, and a replaced table may
/// still hold an earlier phase's rows.
fn loaded(world: &World, stream: &SimStream, phase: usize, stopped: bool, seed: Seed) -> Vec<Row> {
    match (stream.read, stream.write) {
        (ReadMode::Full, WriteMode::Append) => (0..=phase)
            .flat_map(|done| {
                let copies =
                    completions(world, &stream.name, done) + usize::from(stopped && done == phase);
                assert!(
                    copies > 0 || done < phase,
                    "seed {seed}: stream {} never completed",
                    stream.name
                );
                std::iter::repeat_n(stream.all_rows(done), copies).flatten()
            })
            .collect(),
        (ReadMode::Full, WriteMode::Replace) if stopped => {
            (0..=phase).flat_map(|done| stream.all_rows(done)).collect()
        }
        _ => stream.all_rows(phase),
    }
}

/// A merge key's identity: its key and, for a key spanning two columns, its tag.
type Identity = (i64, Option<String>);

/// The last phase that delivered rows of a key, and each partition's last of them.
struct Latest {
    phase: usize,
    /// By partition.
    rows: BTreeMap<i64, Row>,
}

/// One group for each key of a merge stream: the last row the policy keeps of each partition's
/// rows of the key delivered in the last phase that delivered any, as partitions race to merge
/// a key they share.
///
/// Where the phase `stopped` short, or the policy perhaps drops a row of the key, it is any row
/// of the key delivered so far, at most once.
fn merged(stream: &SimStream, phase: usize, stopped: bool) -> Vec<Group> {
    let mut keys: BTreeMap<Identity, Latest> = BTreeMap::new();
    let mut every: BTreeMap<Identity, Vec<Row>> = BTreeMap::new();
    let mut uncertain: BTreeSet<Identity> = BTreeSet::new();
    for delivered in 0..=phase {
        for row in stream.all_rows(delivered) {
            let dropped = expected::dropped(stream, &row);
            if row.delivered != delivered || dropped == Chance::Surely {
                continue;
            }
            let Some(key) = row.key else { continue };
            let identity = (key, row.tag.clone());
            if dropped == Chance::Perhaps {
                uncertain.insert(identity.clone());
            }
            every.entry(identity.clone()).or_default().push(row.clone());
            let latest = keys.entry(identity).or_insert(Latest {
                phase: delivered,
                rows: BTreeMap::new(),
            });
            if latest.phase != delivered {
                latest.phase = delivered;
                latest.rows.clear();
            }
            latest.rows.insert(row.partition, row);
        }
    }
    keys.into_iter()
        .map(|(identity, latest)| {
            if stopped || uncertain.contains(&identity) {
                Group {
                    rows: every.remove(&identity).unwrap_or_default(),
                    count: 1,
                    at_most: true,
                }
            } else {
                Group {
                    rows: latest.rows.into_values().collect(),
                    count: 1,
                    at_most: false,
                }
            }
        })
        .collect()
}
