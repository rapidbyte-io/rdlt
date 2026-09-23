#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, ConnectorError, DestinationWriter, PartitionId, Result, SegmentId, WriteStats,
};
use tokio_util::sync::CancellationToken;

use super::{Lanes, Write};
use crate::budget::MemoryBudget;
use crate::error::ErrorKind;

type Log = Arc<Mutex<Vec<String>>>;

struct Recording {
    table: usize,
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
            let entry = format!("t{} s{} r{}", self.table, segment.0, batch.num_rows());
            self.log.lock().push(entry);
            Ok(())
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async move {
            if self.fail_flush {
                return Err(ConnectorError::data("flush refused"));
            }
            self.log.lock().push(format!("t{} flush", self.table));
            Ok(WriteStats::default())
        })
    }
}

fn writers(
    lanes: usize,
    tables: usize,
    log: &Log,
    fail_write: bool,
    fail_flush: bool,
) -> Vec<Vec<Box<dyn DestinationWriter>>> {
    (0..lanes)
        .map(|_| {
            (0..tables)
                .map(|table| {
                    let writer: Box<dyn DestinationWriter> = Box::new(Recording {
                        table,
                        log: Arc::clone(log),
                        fail_write,
                        fail_flush,
                    });
                    writer
                })
                .collect()
        })
        .collect()
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
    let write = Write {
        table,
        segment: SegmentId(segment),
        batch: rows(count),
        reservation: Box::new(budget.acquire(10).await),
    };
    lanes.write(lane, write).await
}

#[test]
fn routing_is_stable_and_spreads_partitions_across_lanes() {
    let log = Log::default();
    let (lanes, _) = Lanes::new(writers(4, 1, &log, false, false), NonZeroUsize::MIN);
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
    let (lanes, mut tasks) = Lanes::new(
        writers(1, 2, &log, false, false),
        NonZeroUsize::new(8).unwrap(),
    );
    let lane = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&lanes, &budget, 0, 1, 1, 3).await.unwrap();
    write(&lanes, &budget, 0, 0, 2, 4).await.unwrap();
    lanes.flush().await.unwrap();
    assert_eq!(
        *log.lock(),
        ["t1 s1 r3", "t0 s2 r4", "t0 flush", "t1 flush"]
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
    let (lanes, mut tasks) = Lanes::new(writers(1, 1, &log, true, false), NonZeroUsize::MIN);
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
    let (lanes, mut tasks) = Lanes::new(writers(1, 1, &log, false, true), NonZeroUsize::MIN);
    let lane = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
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
    let (_lanes, mut tasks) = Lanes::new(writers(1, 1, &log, false, false), NonZeroUsize::MIN);
    let lane = tokio::spawn(tasks.remove(0).run(cancel.clone()));
    cancel.cancel();
    assert_eq!(
        lane.await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}
