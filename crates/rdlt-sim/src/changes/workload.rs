//! Seeded change workloads: keyed tables a source snapshots, then changes round after round, and
//! the tables and logs their changes leave.

use std::collections::BTreeMap;

use rdlt_connector::ChangeOp;
use rdlt_engine::{DeleteMode, OnTruncate, WriteMode};

use crate::rng::SplitMix64;

/// How many rounds a change simulation runs; the source holds more changes each round.
pub(crate) const ROUNDS: usize = 2;

/// Everything a simulated change source serves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangeWorkload {
    /// The streams.
    pub streams: Vec<ChangeStream>,
}

/// One change stream: a table of `keys` rows, changed; the source snapshots it once `captured`
/// changes have applied, and serves the changes after them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeStream {
    /// The stream's name.
    pub name: String,
    /// The keys the table holds when the snapshot is taken: `0..keys`.
    pub keys: u64,
    /// The partitions the snapshot is read in: key `k` belongs to partition `k % partitions`.
    pub partitions: u64,
    /// Rows per pushed batch; a checkpoint follows each batch.
    pub batch_rows: u64,
    /// How the pipeline writes it: merged by key, or appended as a log.
    pub write: WriteMode,
    /// What its deletes do to a merged table.
    pub deletes: DeleteMode,
    /// What its truncates do to a merged table.
    pub truncates: OnTruncate,
    /// Every change, in order: change `i` is at position `i + 1`.
    pub events: Vec<Event>,
    /// How many changes the snapshot holds: it is taken at position `captured`, its rows carry
    /// that position, and the changes after it are read.
    pub captured: usize,
    /// How many changes the source holds in each round.
    pub rounds: [usize; ROUNDS],
}

/// One change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// What it does.
    pub op: ChangeOp,
    /// The key; none for a truncate.
    pub key: Option<i64>,
    /// The value it sets; `None` for a delete or truncate, or an update leaving it unchanged.
    pub value: Option<String>,
    /// The counter it sets: its position; none for a delete or truncate.
    pub n: Option<i64>,
    /// Whether it leaves `value` unchanged.
    pub partial: bool,
}

impl Event {
    /// The change at `position` drawn from `rng`, of a key below `span`: now and then a truncate,
    /// otherwise a delete, an update leaving `value` unchanged, or an insert or update.
    fn draw(rng: &mut SplitMix64, span: i64, position: i64) -> Self {
        if rng.chance(15) {
            return Self {
                op: ChangeOp::Truncate,
                key: None,
                value: None,
                n: None,
                partial: false,
            };
        }
        let key = Some(i64::try_from(rng.below(span.unsigned_abs())).unwrap_or(0));
        match rng.below(10) {
            0 | 1 => Self {
                op: ChangeOp::Delete,
                key,
                value: None,
                n: None,
                partial: false,
            },
            2 => Self {
                op: ChangeOp::Update,
                key,
                value: None,
                n: Some(position),
                partial: true,
            },
            kind => Self {
                op: if kind == 3 {
                    ChangeOp::Insert
                } else {
                    ChangeOp::Update
                },
                key,
                value: Some(format!("v{position}")),
                n: Some(position),
                partial: false,
            },
        }
    }
}

impl ChangeWorkload {
    /// A workload drawn from `rng`: one to three streams, merged or logged, with every delete and
    /// truncate mode.
    pub(crate) fn generate(rng: &mut SplitMix64) -> Self {
        let streams = (0..=rng.below(3))
            .map(|index| ChangeStream::generate(rng, format!("c{index}")))
            .collect();
        Self { streams }
    }
}

impl ChangeStream {
    fn generate(rng: &mut SplitMix64, name: String) -> Self {
        let keys = 1 + rng.below(40);
        let write = if rng.chance(750) {
            WriteMode::Merge
        } else {
            WriteMode::Append
        };
        let deletes = match rng.below(4) {
            0 => DeleteMode::Soft,
            1 => DeleteMode::Ignore,
            _ => DeleteMode::Hard,
        };
        let truncates = if rng.chance(250) {
            OnTruncate::Ignore
        } else {
            OnTruncate::Apply
        };
        let total = to_usize(rng.below(120));
        let span = i64::try_from(keys + keys / 2 + 1).unwrap_or(i64::MAX);
        let events = (0..total)
            .map(|index| Event::draw(rng, span, i64::try_from(index + 1).unwrap_or(i64::MAX)))
            .collect();
        let first = to_usize(rng.below(u64::try_from(total).unwrap_or(0) + 1));
        let captured = to_usize(rng.below(u64::try_from(first).unwrap_or(0) + 1));
        Self {
            name,
            keys,
            partitions: 1 + rng.below(3),
            batch_rows: 1 + rng.below(12),
            write,
            deletes,
            truncates,
            events,
            captured,
            rounds: [first, total],
        }
    }

    /// The snapshot's rows, by key: the table once the first `captured` changes applied, each
    /// removing what it deletes.
    pub(crate) fn snapshot(&self) -> BTreeMap<i64, Merged> {
        let mut table: BTreeMap<i64, Merged> = (0..self.keys)
            .map(|key| {
                let key = i64::try_from(key).unwrap_or(i64::MAX);
                let row = Merged {
                    value: Some(format!("s{key}")),
                    n: 0,
                    deleted: false,
                };
                (key, row)
            })
            .collect();
        for event in &self.events[..self.captured] {
            apply(&mut table, event, DeleteMode::Hard, OnTruncate::Apply);
        }
        table
    }

    /// The changes read after the snapshot, as the source holds them in `round`.
    fn read(&self, round: usize) -> &[Event] {
        &self.events[self.captured..self.rounds[round]]
    }

    /// The table a merge leaves once the changes of `round` apply, by key: each row's value,
    /// counter, and whether a soft delete marked it deleted.
    pub(crate) fn merged(&self, round: usize) -> BTreeMap<i64, Merged> {
        let mut table = self.snapshot();
        for event in self.read(round) {
            apply(&mut table, event, self.deletes, self.truncates);
        }
        table
    }

    /// The log appending leaves once the changes of `round` are read: every snapshot row and
    /// change, as its op, key, value and counter, sorted.
    pub(crate) fn log(&self, round: usize) -> Vec<Logged> {
        let mut log: Vec<Logged> = self
            .snapshot()
            .into_iter()
            .map(|(key, row)| (ChangeOp::Insert.code(), Some(key), row.value, Some(row.n)))
            .collect();
        log.extend(
            self.read(round)
                .iter()
                .map(|event| (event.op.code(), event.key, event.value.clone(), event.n)),
        );
        log.sort();
        log
    }
}

/// Applies `event` to `table`, its deletes and truncates as `deletes` and `truncates` say.
fn apply(
    table: &mut BTreeMap<i64, Merged>,
    event: &Event,
    deletes: DeleteMode,
    truncates: OnTruncate,
) {
    match (event.op, event.key) {
        (ChangeOp::Truncate, _) if truncates == OnTruncate::Ignore => {}
        (ChangeOp::Truncate, _) if deletes == DeleteMode::Soft => {
            for row in table.values_mut() {
                row.deleted = true;
            }
        }
        (ChangeOp::Truncate, _) => table.clear(),
        (ChangeOp::Delete, _) if deletes == DeleteMode::Ignore => {}
        (ChangeOp::Delete, Some(key)) if deletes == DeleteMode::Soft => {
            if let Some(row) = table.get_mut(&key) {
                row.deleted = true;
            }
        }
        (ChangeOp::Delete, Some(key)) => {
            table.remove(&key);
        }
        (_, Some(key)) => {
            // A soft-deleted row keeps its values, so an update keeping one keeps it.
            let kept = table.get(&key).and_then(|row| row.value.clone());
            let value = if event.partial {
                kept
            } else {
                event.value.clone()
            };
            let row = Merged {
                value,
                n: event.n.unwrap_or(0),
                deleted: false,
            };
            table.insert(key, row);
        }
        _ => {}
    }
}

/// One row of a merged table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Merged {
    pub(crate) value: Option<String>,
    pub(crate) n: i64,
    pub(crate) deleted: bool,
}

/// One row of a log: its op code, key, value and counter.
pub(crate) type Logged = (i8, Option<i64>, Option<String>, Option<i64>);

fn to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}
