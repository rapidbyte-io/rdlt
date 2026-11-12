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
use crate::workload::{Relaxed, Row, SimStream};
use crate::world::World;

/// Rows a table must hold `count` times in all, any of `rows` each time, or where `at_most`, no
/// more often.
#[derive(Clone, Debug)]
pub(super) struct Group {
    pub(super) rows: Vec<Row>,
    pub(super) count: usize,
    pub(super) at_most: bool,
}

/// The groups of rows `stream`'s table holds after `phase`, as the model has them and as an
/// operator left the stream (`relaxed`), each held at most as often where the phase `stopped`
/// short: for a merge stream, one row of each key the policy keeps; otherwise each row the
/// filter and the policy keep, as often as the table holds it.
///
/// A row the policy perhaps drops is held at most as often.
pub(super) fn groups(
    world: &World,
    (stream, relaxed): (&SimStream, Relaxed),
    phase: usize,
    stopped: bool,
    seed: Seed,
) -> Vec<Group> {
    if stream.write == WriteMode::Merge {
        return merged(stream, relaxed, phase, stopped);
    }
    let mut counts: BTreeMap<String, Group> = BTreeMap::new();
    let loaded = loaded(world, (stream, relaxed), phase, stopped, seed);
    for row in loaded.into_iter().filter(|row| stream.reshaping.keeps(row)) {
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
/// each phase's that it reads in full, as often as its full reads completed, and the rows each
/// phase that it reads incrementally seals, once; none before a reset with its tables.
///
/// Where the phase `stopped` short, a full read may be in progress, and a replaced table may still
/// hold an earlier phase's rows.
fn loaded(
    world: &World,
    (stream, relaxed): (&SimStream, Relaxed),
    phase: usize,
    stopped: bool,
    seed: Seed,
) -> Vec<Row> {
    if stream.write == WriteMode::Replace {
        return if stopped {
            (0..=phase).flat_map(|done| stream.all_rows(done)).collect()
        } else {
            stream.all_rows(phase)
        };
    }
    let mut loaded = Vec::new();
    for done in relaxed.reset.unwrap_or(0)..=phase {
        let rows = landing(stream, relaxed, done);
        if stream.reads[done] != ReadMode::Full {
            loaded.extend(rows);
            continue;
        }
        let copies = completions(world, &stream.name, done) + usize::from(stopped && done == phase);
        assert!(
            copies > 0 || done < phase,
            "seed {seed}: stream {} never completed",
            stream.name
        );
        loaded.extend(std::iter::repeat_n(rows, copies).flatten());
    }
    loaded
}

/// A merge key's identity: its key and, for a key spanning two columns, its tag.
type Identity = (i64, Option<String>);

/// The last phase in which rows of a key landed, and each partition's last of them.
struct Latest {
    phase: usize,
    /// By partition.
    rows: BTreeMap<i64, Row>,
}

/// What had landed of a workload's streams in a phase: each as an operator had left it, and how
/// many of its full reads had completed.
#[derive(Clone, Debug)]
pub(super) struct Landed {
    relaxed: Vec<Relaxed>,
    completed: Vec<usize>,
}

impl Landed {
    /// What had landed of `world`'s streams in `phase` by now, as `relaxed` leaves them.
    pub(super) fn now(world: &World, relaxed: &[Relaxed], phase: usize) -> Self {
        let streams = world.workload.streams.iter();
        Self {
            relaxed: relaxed.to_vec(),
            completed: streams
                .map(|stream| completions(world, &stream.name, phase))
                .collect(),
        }
    }
}

/// The rows of `world`'s stream `index` that landed in `phase` since `before`, or in all of it
/// where there is none, as an operator left it (`relaxed`), each as often: every row of each full
/// read completed since, and those its incremental reads sealed since.
///
/// Where the phase `stopped` short, a full read may be in progress too.
pub(super) fn landed(
    world: &World,
    (index, relaxed): (usize, Relaxed),
    phase: usize,
    stopped: bool,
    before: Option<&Landed>,
) -> Vec<Row> {
    let stream = &world.workload.streams[index];
    if stream.reads[phase] == ReadMode::Full {
        let completed = before.map_or(0, |landed| landed.completed[index]);
        let copies = completions(world, &stream.name, phase).saturating_sub(completed);
        let copies = copies + usize::from(stopped);
        return std::iter::repeat_n(stream.all_rows(phase), copies)
            .flatten()
            .collect();
    }
    let Some(before) = before else {
        return landing(stream, relaxed, phase);
    };
    let partitions = 0..stream.partitions.len();
    partitions
        .flat_map(|partition| {
            let from = stream.sealed(partition, phase, before.relaxed[index]);
            let to = stream.sealed(partition, phase, relaxed);
            let rows = stream
                .rows(partition, phase)
                .get(from..to)
                .unwrap_or_default();
            rows.iter().cloned()
        })
        .collect()
}

/// The rows of `stream` that land in `phase`, as an operator left it (`relaxed`), each once:
/// every row of a phase read in full, and those its incremental reads seal.
pub(super) fn landing(stream: &SimStream, relaxed: Relaxed, phase: usize) -> Vec<Row> {
    let partitions = 0..stream.partitions.len();
    partitions
        .flat_map(|partition| stream.landing(partition, phase, relaxed).iter().cloned())
        .collect()
}

/// One group for each key of a merge stream: the last row the policy keeps of each partition's
/// rows of the key that landed in the last phase that landed any, as partitions race to merge a
/// key they share; none before a reset with its tables.
///
/// Where the phase `stopped` short, or the policy perhaps drops a row of the key, it is any row
/// of the key landed so far, at most once.
fn merged(stream: &SimStream, relaxed: Relaxed, phase: usize, stopped: bool) -> Vec<Group> {
    let mut keys: BTreeMap<Identity, Latest> = BTreeMap::new();
    let mut every: BTreeMap<Identity, Vec<Row>> = BTreeMap::new();
    let mut uncertain: BTreeSet<Identity> = BTreeSet::new();
    for landed in relaxed.reset.unwrap_or(0)..=phase {
        for row in landing(stream, relaxed, landed) {
            let dropped = expected::dropped(stream, &row);
            if dropped == Chance::Surely {
                continue;
            }
            let Some(key) = row.key else { continue };
            let identity = (key, row.tag.clone());
            if dropped == Chance::Perhaps {
                uncertain.insert(identity.clone());
            }
            every.entry(identity.clone()).or_default().push(row.clone());
            let latest = keys.entry(identity).or_insert(Latest {
                phase: landed,
                rows: BTreeMap::new(),
            });
            if latest.phase != landed {
                latest.phase = landed;
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
