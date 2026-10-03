//! Logs no load writes, as damage or a hand that can write the log's directory makes them: each
//! refused, never replayed in part.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use rdlt_connector::{
    BoxFuture, DroppedTable, Epoch, LoadId, PartitionId, PartitionState, PipelineId, SchemaVersion,
    SegmentId, StreamName, TablePath, TableRef, TableSchema,
};

use super::{FRAME_BYTES, load, meta, pipeline, published, scan, seq};
use crate::wal::frame::{self, Batch, Committing, End, Fence, Frame, Header, Seal, Table};
use crate::wal::memory::MemoryWal;
use crate::wal::store::{Chunk, StagedChunk, WalStore};

fn header(number: u64, epoch: u64) -> Frame {
    Frame::Header(Header {
        pipeline: pipeline(),
        load: load(),
        chunk: number,
        epoch: Epoch(epoch),
        opened: None,
    })
}

fn table(index: u32, version: u32) -> Frame {
    let batch = super::ids(0);
    Frame::Schema(Table {
        index,
        table: TableRef {
            path: TablePath::new(["t"]).expect("a valid path"),
            name: "t".into(),
            version: SchemaVersion(version),
            generation: None,
            merge: None,
        },
        schema: TableSchema::from_arrow(&batch.schema()).expect("a schema"),
    })
}

fn batch(segment: u64, ordinal: u64) -> Frame {
    Frame::Batch(Batch {
        segment: SegmentId(segment),
        table: 0,
        ordinal,
        batch: super::ids(0),
    })
}

fn seal(segment: u64, batches: u64, rows: u64) -> Frame {
    Frame::Seal(Seal {
        segment: SegmentId(segment),
        stream: StreamName::new("orders").expect("a valid stream"),
        partition: PartitionId::parse(format!("p{segment}")).expect("a valid partition"),
        replayable: false,
        phase: 0,
        from: None,
        state: PartitionState::Done,
        batches,
        rows,
    })
}

fn commit(number: u64, segments: &[u64], seals: u32) -> Frame {
    Frame::Commit(Box::new(Committing {
        meta: meta(number, segments),
        seals,
        phases: 0,
    }))
}

fn end(live: &[u64], received: &[u64]) -> Frame {
    Frame::End(End {
        live: live.to_vec(),
        received: received.iter().map(|number| seq(*number)).collect(),
    })
}

/// A chunk's bytes: its preamble, then `frames`.
fn chunk(frames: &[Frame]) -> Vec<u8> {
    let mut bytes = frame::preamble().to_vec();
    for frame in frames {
        bytes.extend_from_slice(&frame.encode().expect("the frame encodes"));
    }
    bytes
}

/// A load's chunk 0 holding commit 1 of segment 1, whole.
fn whole() -> Vec<Frame> {
    vec![
        header(0, 1),
        table(0, 1),
        batch(1, 0),
        seal(1, 1, 2),
        commit(1, &[1], 1),
        end(&[], &[]),
    ]
}

/// What a scan of a log of `chunks` does, by number, each of `frames`.
async fn scanned(chunks: Vec<(u64, Vec<Frame>)>) -> Result<usize, crate::Error> {
    let store = MemoryWal::default();
    for (number, frames) in chunks {
        let at = Chunk {
            load: load(),
            number,
        };
        published(&store, &pipeline(), at, chunk(&frames)).await;
    }
    scan(&store, &pipeline(), load(), FRAME_BYTES)
        .await
        .map(|scanned| scanned.pending().count())
}

/// Checks a log of `chunks` is refused as one no load wrote, as `case` says.
async fn refused(case: &str, chunks: Vec<(u64, Vec<Frame>)>) {
    let error = scanned(chunks).await.expect_err(case);
    assert_eq!(error.code(), Some("wal_unreadable"), "{case}: {error}");
}

/// `whole()` with the frame at `at` replaced by `frames`.
fn replaced(at: usize, frames: &[Frame]) -> Vec<Frame> {
    let mut whole = whole();
    whole.splice(at..=at, frames.iter().cloned());
    whole
}

#[tokio::test]
async fn a_chunk_shaped_as_a_load_writes_one_reads_back() {
    assert_eq!(scanned(vec![(0, whole())]).await.expect("it reads"), 1);
}

#[tokio::test]
async fn a_chunk_not_shaped_as_a_load_writes_one_is_refused() {
    let without_end = whole()[..5].to_vec();
    let mut end_first = whole();
    end_first.insert(5, batch(1, 7));
    let mut two_commits = whole();
    two_commits.insert(5, commit(2, &[], 0));
    let mut headless = whole();
    headless.remove(0);
    let mut two_headers = whole();
    two_headers.insert(1, header(0, 1));
    for (case, frames) in [
        ("no end", without_end),
        (
            "an end before a frame",
            replaced(5, &[end(&[], &[]), batch(1, 7)]),
        ),
        ("a frame between the commit and the end", end_first),
        ("two commits in one chunk", two_commits),
        ("no header", headless),
        ("two headers", two_headers),
        ("the number of another chunk", replaced(0, &[header(3, 1)])),
        ("seals no commit follows", replaced(4, &[Frame::Closed])),
        ("a fence holding frames", {
            let fence = Frame::Fence(Fence {
                pipeline: pipeline(),
                load: load(),
                chunk: 0,
            });
            replaced(0, &[fence])
        }),
    ] {
        refused(case, vec![(0, frames)]).await;
    }
}

#[tokio::test]
async fn a_commit_missing_anything_it_names_is_refused() {
    for (case, frames) in [
        ("its seal", replaced(3, &[])),
        ("a seal it counts", replaced(4, &[commit(1, &[1], 2)])),
        (
            "a seal of a segment it publishes",
            replaced(4, &[commit(1, &[1, 4], 1)]),
        ),
        ("a batch its seal counts", replaced(3, &[seal(1, 2, 4)])),
        ("rows its seal counts", replaced(3, &[seal(1, 1, 3)])),
        (
            "one batch logged twice",
            replaced(2, &[batch(1, 0), batch(1, 0)]),
        ),
        (
            "a segment sealed twice",
            replaced(3, &[seal(1, 1, 2), seal(1, 1, 2)]),
        ),
    ] {
        refused(case, vec![(0, frames)]).await;
    }
    // A segment logged twice under one ordinal but sealed for both is refused all the same.
    let doubled = replaced(2, &[batch(1, 0), batch(1, 0)]);
    let doubled = {
        let mut frames = doubled;
        frames[4] = seal(1, 2, 4);
        frames
    };
    refused("an ordinal twice", vec![(0, doubled)]).await;
}

#[tokio::test]
async fn a_commit_its_load_could_not_have_written_is_refused() {
    let mut other_load = meta(1, &[1]);
    other_load.load_id = LoadId::from_parts(std::time::UNIX_EPOCH, 99);
    let mut other_epoch = meta(1, &[1]);
    other_epoch.epoch = Epoch(9);
    let mut dropping = meta(1, &[1]);
    dropping.drop_tables = vec![DroppedTable {
        path: TablePath::new(["t"]).expect("a valid path"),
        name: "t".into(),
    }];
    for (case, meta) in [
        ("another load's", other_load),
        ("another session's", other_epoch),
        ("dropping a table", dropping),
    ] {
        let forged = Frame::Commit(Box::new(Committing {
            meta,
            seals: 1,
            phases: 0,
        }));
        refused(case, vec![(0, replaced(4, &[forged]))]).await;
    }
    // Commits in the order of no load.
    let second = vec![
        header(1, 1),
        table(0, 1),
        batch(2, 1),
        seal(2, 1, 2),
        commit(1, &[2], 1),
        end(&[0], &[]),
    ];
    let mut first = whole();
    first[4] = commit(2, &[1], 1);
    refused("commits out of order", vec![(0, first), (1, second)]).await;
}

#[tokio::test]
async fn chunks_that_disagree_with_one_another_are_refused() {
    let second = |header: Frame, table: Frame| {
        vec![
            header,
            table,
            batch(2, 1),
            seal(2, 1, 2),
            commit(2, &[2], 1),
            end(&[0], &[]),
        ]
    };
    assert_eq!(
        scanned(vec![(0, whole()), (1, second(header(1, 1), table(0, 1)))])
            .await
            .expect("two chunks of one load read"),
        2
    );
    refused(
        "another session's header",
        vec![(0, whole()), (1, second(header(1, 2), table(0, 1)))],
    )
    .await;
    refused(
        "a table described anew",
        vec![(0, whole()), (1, second(header(1, 1), table(0, 2)))],
    )
    .await;
}

#[tokio::test]
async fn what_a_received_commit_lacks_is_no_matter() {
    // Commit 1 has its receipt: its segment's frames may have gone with its settled chunk.
    let frames = replaced(2, &[]);
    let received = {
        let mut frames = frames;
        frames[4] = end(&[], &[1]);
        frames
    };
    let second = vec![
        header(1, 1),
        table(0, 1),
        batch(2, 1),
        seal(2, 1, 2),
        commit(2, &[2], 1),
        end(&[0], &[1]),
    ];
    assert_eq!(
        scanned(vec![(0, received), (1, second)])
            .await
            .expect("it reads"),
        1
    );
}

/// A store over a [`MemoryWal`] that notes the most it was asked to read at once.
#[derive(Debug, Default)]
struct Measured {
    inner: MemoryWal,
    largest: AtomicU64,
}

impl WalStore for Measured {
    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        self.inner.stage(pipeline, chunk)
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        self.inner.loads(pipeline)
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        self.inner.chunks(pipeline, load)
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        self.largest.fetch_max(len, Ordering::Relaxed);
        self.inner.read(pipeline, chunk, offset, len)
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.remove(pipeline, chunk)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.remove_log(pipeline, load)
    }
}

#[tokio::test]
async fn a_frame_announcing_more_than_a_frame_may_hold_is_refused_before_it_is_read() {
    for (announced, present) in [
        (FRAME_BYTES + 1, FRAME_BYTES + 1),
        (u64::from(u32::MAX), 64),
        (FRAME_BYTES, 64),
    ] {
        let mut bytes = chunk(&[header(0, 1)]);
        bytes.push(3);
        bytes.extend_from_slice(&u32::try_from(announced).expect("fits").to_le_bytes());
        bytes.extend_from_slice(&[0; 4]);
        bytes.resize(bytes.len() + usize::try_from(present).expect("fits"), 0);
        let store = Measured::default();
        let at = Chunk {
            load: load(),
            number: 0,
        };
        published(&store.inner, &pipeline(), at, bytes).await;
        let error = scan(&store, &pipeline(), load(), FRAME_BYTES)
            .await
            .expect_err("refused");
        assert_eq!(error.code(), Some("wal_unreadable"), "{announced}");
        let largest = store.largest.load(Ordering::Relaxed);
        assert!(
            largest < FRAME_BYTES.min(announced),
            "{announced}: read {largest}"
        );
    }
}
