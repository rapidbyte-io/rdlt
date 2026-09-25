#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, CommitMeta, ConnectorError, DestinationSession, DestinationWriter, PartitionId,
    Receipt, Result, SchemaVersion, SegmentId, TableChange, TablePath, TableRef, WriteStats,
};
use tokio_util::sync::CancellationToken;

use super::{Lanes, Write};
use crate::budget::MemoryBudget;
use crate::error::ErrorKind;
use crate::table::{Model, SharedSession, Tables, testing::resolver};

type Log = Arc<Mutex<Vec<String>>>;

struct Recording {
    table: usize,
    version: u32,
    log: Log,
    fail_write: bool,
    fail_flush: bool,
}

impl DestinationWriter for Recording {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if self.fail_write {
                return Err(ConnectorError::data("write refused"));
            }
            let entry = format!(
                "t{} v{} s{} r{}",
                self.table,
                self.version,
                segment.0,
                batch.num_rows()
            );
            self.log.lock().push(entry);
            Ok(())
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async move {
            if self.fail_flush {
                return Err(ConnectorError::data("flush refused"));
            }
            let entry = format!("t{} v{} flush", self.table, self.version);
            self.log.lock().push(entry);
            Ok(WriteStats::default())
        })
    }
}

/// A session whose writers record what they stage, each named after its table: `t<index>`.
struct Session {
    log: Log,
    fail_open: bool,
    fail_write: bool,
    fail_flush: bool,
}

impl DestinationSession for Session {
    fn apply_schema<'a>(&'a mut self, _change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        if self.fail_open {
            return Box::pin(async { Err(ConnectorError::data("no writer")) });
        }
        let writer: Box<dyn DestinationWriter> = Box::new(Recording {
            table: table.name[1..].parse().unwrap(),
            version: table.version.0,
            log: Arc::clone(&self.log),
            fail_write: self.fail_write,
            fail_flush: self.fail_flush,
        });
        let entry = format!("open {} v{}", table.name, table.version.0);
        self.log.lock().push(entry);
        Box::pin(async move { Ok(writer) })
    }

    fn commit<'a>(&'a mut self, _meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async { Err(ConnectorError::internal("no commits here")) })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// `count` lanes over `tables` tables, `t0` onwards, whose writers log to `log`.
fn lanes(
    count: usize,
    tables: usize,
    log: &Log,
    [fail_open, fail_write, fail_flush]: [bool; 3],
    window: usize,
) -> (Lanes, Vec<super::Lane>) {
    let session = Session {
        log: Arc::clone(log),
        fail_open,
        fail_write,
        fail_flush,
    };
    let all = Tables::new(SharedSession::new(Box::new(session)));
    for table in 0..tables {
        let name = format!("t{table}");
        let table = TableRef {
            path: TablePath::new([name.as_str()]).unwrap(),
            name: name.clone().into(),
            version: SchemaVersion(0),
            generation: None,
            merge: None,
        };
        all.add(resolver(&name), &table, Model::default());
    }
    Lanes::new(
        NonZeroUsize::new(count).unwrap(),
        &Arc::new(all),
        NonZeroUsize::new(window).unwrap(),
    )
}

fn rows(count: i64) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from_iter_values(0..count)) as _)])
        .unwrap()
}

async fn write(
    lanes: &Lanes,
    budget: &MemoryBudget,
    lane: usize,
    table: usize,
    segment: u64,
    count: i64,
) -> Result<(), crate::Error> {
    write_at(
        lanes,
        budget,
        (lane, table, SchemaVersion(0)),
        segment,
        count,
    )
    .await
}

/// Writes `count` rows of segment `segment` to `table` on `lane`, lowered for `version`.
async fn write_at(
    lanes: &Lanes,
    budget: &MemoryBudget,
    (lane, table, version): (usize, usize, SchemaVersion),
    segment: u64,
    count: i64,
) -> Result<(), crate::Error> {
    let write = Write {
        table,
        version,
        segment: SegmentId(segment),
        batch: rows(count),
        reservation: Box::new(budget.acquire(10).await),
    };
    lanes.write(lane, write).await
}

#[test]
fn routing_is_stable_and_spreads_partitions_across_lanes() {
    let log = Log::default();
    let (lanes, _) = lanes(4, 1, &log, [false, false, false], 1);
    let partition = |id: usize| PartitionId::parse(format!("p{id}")).unwrap();
    let routes: Vec<usize> = (0..64).map(|id| lanes.route(0, &partition(id))).collect();
    assert_eq!(
        routes,
        (0..64)
            .map(|id| lanes.route(0, &partition(id)))
            .collect::<Vec<_>>()
    );
    assert!(routes.iter().all(|lane| *lane < 4));
    let used: std::collections::BTreeSet<usize> = routes.iter().copied().collect();
    assert_eq!(used.len(), 4);
    assert_ne!(
        (0..8)
            .map(|id| lanes.route(0, &partition(id)))
            .collect::<Vec<_>>(),
        (0..8)
            .map(|id| lanes.route(1, &partition(id)))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_lane_stages_writes_in_order_and_flushes_every_table_before_answering() {
    let log = Log::default();
    let budget = MemoryBudget::new(1_000);
    let (lanes, mut tasks) = lanes(1, 2, &log, [false, false, false], 8);
    let lane = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&lanes, &budget, 0, 1, 1, 3).await.unwrap();
    write(&lanes, &budget, 0, 0, 2, 4).await.unwrap();
    lanes.flush().await.unwrap();
    assert_eq!(
        *log.lock(),
        [
            "open t1 v0",
            "t1 v0 s1 r3",
            "open t0 v0",
            "t0 v0 s2 r4",
            "t0 v0 flush",
            "t1 v0 flush"
        ],
        "a writer opens on its table's first write"
    );
    assert_eq!(
        budget.reserved(),
        0,
        "staged writes release their reservations"
    );
    drop(lanes);
    lane.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_failed_write_ends_the_lane_with_a_destination_error() {
    let log = Log::default();
    let budget = MemoryBudget::new(1_000);
    let (lanes, mut tasks) = lanes(1, 1, &log, [false, true, false], 1);
    let lane = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&lanes, &budget, 0, 0, 1, 1).await.unwrap();
    let error = lane.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Destination);
    let after = write(&lanes, &budget, 0, 0, 2, 1).await.unwrap_err();
    assert_eq!(after.kind(), ErrorKind::Cancelled);
    assert_eq!(
        lanes.flush().await.unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn a_failed_flush_ends_the_lane_and_the_flush_is_not_answered() {
    let log = Log::default();
    let budget = MemoryBudget::new(1_000);
    let (lanes, mut tasks) = lanes(1, 1, &log, [false, false, true], 1);
    let lane = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&lanes, &budget, 0, 0, 1, 1).await.unwrap();
    assert_eq!(
        lanes.flush().await.unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(
        lane.await.unwrap().unwrap_err().kind(),
        ErrorKind::Destination
    );
}

#[tokio::test]
async fn cancelling_a_lane_ends_it() {
    let log = Log::default();
    let cancel = CancellationToken::new();
    let (_lanes, mut tasks) = lanes(1, 1, &log, [false, false, false], 1);
    let lane = tokio::spawn(tasks.remove(0).run(cancel.clone()));
    cancel.cancel();
    assert_eq!(
        lane.await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn a_lane_that_cannot_open_a_writer_ends_with_a_destination_error() {
    let log = Log::default();
    let budget = MemoryBudget::new(1_000);
    let (lanes, mut tasks) = lanes(1, 1, &log, [true, false, false], 1);
    let lane = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&lanes, &budget, 0, 0, 1, 1).await.unwrap();
    let error = lane.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Destination);
    assert!(
        error.to_string().contains("creating a writer for table t0"),
        "{error}"
    );
}

#[tokio::test]
async fn each_write_goes_through_a_writer_of_the_schema_version_it_was_lowered_for() {
    let log = Log::default();
    let budget = MemoryBudget::new(1_000);
    let (lanes, mut tasks) = lanes(1, 1, &log, [false, false, false], 8);
    let lane = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    let at = |version| (0, 0, SchemaVersion(version));
    write_at(&lanes, &budget, at(1), 1, 2).await.unwrap();
    write_at(&lanes, &budget, at(2), 1, 3).await.unwrap();
    // A partition still lowering for the older version writes after the table changed.
    write_at(&lanes, &budget, at(1), 2, 4).await.unwrap();
    lanes.flush().await.unwrap();
    assert_eq!(
        *log.lock(),
        [
            "open t0 v1",
            "t0 v1 s1 r2",
            "open t0 v2",
            "t0 v2 s1 r3",
            "t0 v1 s2 r4",
            "t0 v1 flush",
            "t0 v2 flush"
        ]
    );
    drop(lanes);
    lane.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_flush_reaches_only_the_writers_written_since_the_last() {
    let log = Log::default();
    let budget = MemoryBudget::new(1_000);
    let (lanes, mut tasks) = lanes(1, 1, &log, [false, false, false], 8);
    let lane = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    let at = |version| (0, 0, SchemaVersion(version));
    write_at(&lanes, &budget, at(1), 1, 2).await.unwrap();
    write_at(&lanes, &budget, at(2), 1, 3).await.unwrap();
    lanes.flush().await.unwrap();
    log.lock().clear();
    // The table has moved on to version 2: its older writer has nothing left to flush.
    write_at(&lanes, &budget, at(2), 2, 4).await.unwrap();
    lanes.flush().await.unwrap();
    assert_eq!(*log.lock(), ["t0 v2 s2 r4", "t0 v2 flush"]);
    drop(lanes);
    lane.await.unwrap().unwrap();
}
