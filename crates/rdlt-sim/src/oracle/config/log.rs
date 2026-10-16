//! The least log the engine promises a world's loads: the frames its partitions hold unsealed,
//! the frames of a commit and what the chunks holding them write beside them take at most three
//! quarters of it, as ADR 0045 claims.

use rdlt_connector::LogicalType;
use rdlt_testkit::drawn::Shape;
use rdlt_wire::limits::count;

use crate::source::json_push;
use crate::workload::{PHASES, SimStream, Workload};

/// Bytes: what each table's batch frame of a push takes at most beside its columns and values:
/// its head, header and Arrow messages, the engine's own columns among them.
const FRAME: u64 = 5 << 10;

/// Bytes: what each leaf column of a push takes at most in its frame beside its values: its
/// field in the schema message and its buffers, each padded.
const COLUMN: u64 = 512;

/// Bytes: what a table's schema frame takes at most beside its columns.
const SCHEMA: u64 = 1 << 10;

/// Bytes: what each column of a table takes at most in its schema frame.
const SCHEMA_COLUMN: u64 = 128;

/// Bytes: what a commit's seal, phase and commit frames take at most beside its partitions'.
const COMMIT: u64 = 8 << 10;

/// Bytes: what each partition of a load takes at most in a commit's frames: a seal, and its
/// cursor and name in the commit.
const COMMIT_PARTITION: u64 = 2 << 10;

/// Bytes: a chunk's header frame at most, naming its pipeline, load and destination.
const HEADER: u64 = 512;

/// Bytes: the least log the engine promises the loads of `workload`, `partitions` of whose
/// partitions are read at once, each holding up to a checkpoint's pushes unsealed.
pub(super) fn least(workload: &Workload, partitions: usize) -> u64 {
    let streams: Vec<Pushes> = workload.streams.iter().map(pushes).collect();
    let held = |part: fn(&Pushes) -> u64| {
        let mut held: Vec<u64> = workload
            .streams
            .iter()
            .zip(&streams)
            .flat_map(|(stream, pushes)| {
                let held = stream.checkpoint_every.saturating_mul(part(pushes));
                std::iter::repeat_n(held, stream.partitions.len())
            })
            .collect();
        held.sort_unstable_by(|a, b| b.cmp(a));
        held.iter().take(partitions).sum::<u64>()
    };
    let open = Open {
        bytes: held(|pushes| pushes.bytes),
        frames: held(|pushes| pushes.tables),
    };
    let schema = streams
        .iter()
        .map(|pushes| pushes.versions * pushes.tables * (SCHEMA + SCHEMA_COLUMN * pushes.leaves))
        .sum();
    let partitions: u64 = workload
        .streams
        .iter()
        .map(|stream| count(stream.partitions.len()))
        .sum();
    let commit = COMMIT + COMMIT_PARTITION * partitions;
    let mut limit = 16 << 10;
    while !fits(limit, open, schema, commit) {
        limit += limit / 8;
    }
    limit
}

/// What a load's partitions hold unsealed at most: bytes of batch frames, and frames.
#[derive(Clone, Copy)]
struct Open {
    bytes: u64,
    frames: u64,
}

/// The most what one push of a stream logs takes: bytes and frames, the leaf columns of one of
/// its tables, and the versions of its tables its pushes may make.
struct Pushes {
    bytes: u64,
    tables: u64,
    leaves: u64,
    versions: u64,
}

/// What the pushes of `stream` log at most: a push whose rows were delivered in several phases
/// logs the tables of each phase's shapes.
fn pushes(stream: &SimStream) -> Pushes {
    let mut most = Pushes {
        bytes: 0,
        tables: 1,
        leaves: 0,
        versions: 0,
    };
    let mut versions: Vec<Vec<Option<&LogicalType>>> = Vec::new();
    for partition in 0..stream.partitions.len() {
        for phase in 0..PHASES {
            let version = stream.drift.iter().map(|drift| {
                drift.shapes[partition][phase]
                    .as_ref()
                    .map(|shape| &shape.logical)
            });
            let version = std::iter::once(Some(&stream.key_types[partition][phase]))
                .chain(version)
                .collect();
            if !versions.contains(&version) {
                versions.push(version);
            }
            let rows = stream.rows(partition, phase);
            for batch in stream.batches(partition, phase) {
                let rows = &rows[batch];
                let mut delivered: Vec<usize> = rows.iter().map(|row| row.delivered).collect();
                delivered.sort_unstable();
                delivered.dedup();
                let mut push = Push::default();
                for delivered in delivered {
                    let tables = Tables::of(stream, partition, delivered);
                    push.frames += tables.tables;
                    push.bytes += tables.tables * FRAME + tables.leaves * COLUMN;
                    most.leaves = most.leaves.max(tables.leaves);
                }
                let text = count(json_push(stream, rows, false).len());
                most.bytes = most.bytes.max(push.bytes + 2 * text);
                most.tables = most.tables.max(push.frames);
            }
        }
    }
    most.versions = count(versions.len()).max(1);
    most
}

/// What one push logs beside its values: bytes and frames.
#[derive(Default)]
struct Push {
    bytes: u64,
    frames: u64,
}

/// The tables rows of a stream's partition delivered in a phase land in, and the leaf columns
/// of each.
struct Tables {
    tables: u64,
    leaves: u64,
}

impl Tables {
    fn of(stream: &SimStream, partition: usize, delivered: usize) -> Self {
        let mut counted = Counted::default();
        let shapes = stream
            .drift
            .iter()
            .filter_map(|drift| drift.shapes[partition][delivered].as_ref());
        shapes.for_each(|shape| counted.walk(shape));
        let keys = u64::from(stream.keys > 0) + u64::from(stream.keys > 0 && stream.composite);
        Self {
            tables: 1 + if stream.normalized() {
                counted.lists
            } else {
                0
            },
            leaves: 4 + keys + counted.leaves,
        }
    }
}

/// The leaf columns and arrays of a value's shape.
#[derive(Default)]
struct Counted {
    leaves: u64,
    lists: u64,
}

impl Counted {
    fn walk(&mut self, shape: &Shape) {
        match shape.logical {
            LogicalType::List(_) => self.lists += 1,
            LogicalType::Struct(_) => {}
            _ => self.leaves += 1,
        }
        shape.children.iter().for_each(|child| self.walk(child));
    }
}

/// Whether `open` frames, of tables whose schema frames take `schema` bytes, fit a log of `limit`
/// bytes beside a commit of `commit`.
///
/// They, the commit's frames and what the chunks holding them write beside them take at most
/// three quarters of it. A chunk holds an eighth of the log less what it writes beside its batch
/// frames, and every chunk holding open frames but two holds more than half of that, or a frame
/// alone.
fn fits(limit: u64, open: Open, schema: u64, commit: u64) -> bool {
    let mut chunks = 1;
    let beside = loop {
        let beside = framing(chunks + 2) + schema;
        let holds = (limit / 8).saturating_sub(beside);
        let needed = if holds == 0 {
            open.frames
        } else {
            open.frames.min((2 * open.bytes).div_ceil(holds) + 1)
        };
        if needed <= chunks {
            break beside;
        }
        chunks = needed;
    };
    open.bytes + commit + chunks * beside <= limit * 3 / 4
}

/// Bytes: what a chunk writes beside its batch and schema frames where its end names `chunks`
/// chunks and as many commits: a 14-byte preamble, its header, a 9-byte relief frame, and its end
/// of 9 bytes beside its JSON, each number of up to 20 digits.
fn framing(chunks: u64) -> u64 {
    let names = 2 + 21 * chunks;
    14 + HEADER + 9 + 9 + 22 + 2 * names
}
