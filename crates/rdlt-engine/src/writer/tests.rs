#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, CommitMeta, ConnectorError, DestinationSession, DestinationWriter, Receipt, Result,
    SchemaVersion, SegmentId, TableChange, TablePath, TableRef, WriteStats,
};
use tokio_util::sync::CancellationToken;

use super::{Write, Writers};
use crate::budget::MemoryBudget;
use crate::env::{Env, InlineEnv};
use crate::error::ErrorKind;
use crate::report::{Flushes, Tally, WriterCounters};
use crate::table::{Model, SharedSession, Tables, testing::resolver};

type Log = Arc<Mutex<Vec<String>>>;

struct Recording {
    table: usize,
    version: u32,
    log: Log,
    fail_write: bool,
    fail_flush: bool,
    /// How long a write takes; a flush takes twice as long.
    pace: Duration,
}

impl DestinationWriter for Recording {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            tokio::time::sleep(self.pace).await;
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
            tokio::time::sleep(self.pace * 2).await;
            if self.fail_flush {
                return Err(ConnectorError::data("flush refused"));
            }
            let entry = format!("t{} v{} flush", self.table, self.version);
            self.log.lock().push(entry);
            Ok(WriteStats::default())
        })
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        let entry = format!("close t{} v{}", self.table, self.version);
        self.log.lock().push(entry);
    }
}

/// A session whose table writers record what they stage, each named after its table: `t<index>`.
struct Session {
    log: Log,
    fail_open: bool,
    fail_write: bool,
    fail_flush: bool,
    pace: Duration,
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
            return Box::pin(async { Err(ConnectorError::data("no table writer")) });
        }
        let writer: Box<dyn DestinationWriter> = Box::new(Recording {
            table: table.name[1..].parse().unwrap(),
            version: table.version.0,
            log: Arc::clone(&self.log),
            fail_write: self.fail_write,
            fail_flush: self.fail_flush,
            pace: self.pace,
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

/// `count` writers over `tables` tables, `t0` onwards, whose table writers log to `log`.
fn writers(
    count: usize,
    tables: usize,
    log: &Log,
    fails: [bool; 3],
    window: usize,
) -> (Writers, Vec<super::Writer>) {
    budgeted(
        count,
        tables,
        log,
        fails,
        window,
        &crate::budget::budget(1 << 30),
    )
}

/// [`writers`] under `budget`.
fn budgeted(
    count: usize,
    tables: usize,
    log: &Log,
    [fail_open, fail_write, fail_flush]: [bool; 3],
    window: usize,
    budget: &MemoryBudget,
) -> (Writers, Vec<super::Writer>) {
    let session = Session {
        log: Arc::clone(log),
        fail_open,
        fail_write,
        fail_flush,
        pace: Duration::ZERO,
    };
    over(session, (count, tables, window), budget, &Arc::default())
}

/// `count` writers over `tables` tables of `session`, queueing `window` writes each, counting into
/// `tally`.
fn over(
    session: Session,
    (count, tables, window): (usize, usize, usize),
    budget: &MemoryBudget,
    tally: &Arc<Tally>,
) -> (Writers, Vec<super::Writer>) {
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
        all.add(resolver(&name), &table, Model::default()).unwrap();
    }
    let writers = crate::config::GrowthLimits::default().table_writers();
    let env: Arc<dyn Env> = Arc::new(InlineEnv);
    Writers::new(
        (NonZeroUsize::new(count).unwrap(), writers),
        &Arc::new(all),
        NonZeroUsize::new(window).unwrap(),
        budget,
        (&env, tally),
    )
}

fn rows(count: i64) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from_iter_values(0..count)) as _)])
        .unwrap()
}

async fn write(
    writers: &Writers,
    budget: &MemoryBudget,
    writer: usize,
    table: usize,
    segment: u64,
    count: i64,
) -> Result<(), crate::Error> {
    write_at(
        writers,
        budget,
        (writer, table, SchemaVersion(0)),
        segment,
        count,
    )
    .await
}

/// Writes `count` rows of segment `segment` to `table` on `writer`, lowered for `version`.
async fn write_at(
    writers: &Writers,
    budget: &MemoryBudget,
    (writer, table, version): (usize, usize, SchemaVersion),
    segment: u64,
    count: i64,
) -> Result<(), crate::Error> {
    let write = Write {
        table,
        version,
        segment: SegmentId(segment),
        batch: rows(count),
        reservation: Box::new(budget.acquire(10).await.expect("the budget has room")),
    };
    writers.write(writer, write).await
}

#[test]
fn places_up_to_the_writers_take_a_writer_each_and_more_wrap_evenly() {
    let log = Log::default();
    let (four, _) = writers(4, 1, &log, [false, false, false], 1);
    let routes = |writers: &Writers, table: usize, places: usize| {
        (0..places)
            .map(|place| writers.route(table, place))
            .collect::<Vec<_>>()
    };
    assert_eq!(routes(&four, 0, 4), [0, 1, 2, 3]);
    assert_eq!(
        routes(&four, 1, 4),
        [1, 2, 3, 0],
        "a table starts a writer further on"
    );
    let mut taken = [0_usize; 4];
    for writer in routes(&four, 0, 16) {
        taken[writer] += 1;
    }
    assert_eq!(
        taken, [4; 4],
        "sixteen places on four writers take four each"
    );
    let (sixteen, _) = writers(16, 1, &log, [false, false, false], 1);
    let used: std::collections::BTreeSet<usize> = routes(&sixteen, 0, 16).into_iter().collect();
    assert_eq!(used.len(), 16);
}

proptest::proptest! {
    /// Distinct places of one table take distinct writers while there are writers for them, and
    /// every route names a writer.
    #[test]
    fn places_of_a_table_take_distinct_writers_up_to_the_writers(
        count in 1_usize..=64,
        table in 0_usize..8,
        first in 0_usize..1_000,
    ) {
        let log = Log::default();
        let (writers, _) = writers(count, table + 1, &log, [false, false, false], 1);
        let routed: Vec<usize> = (first..first + count)
            .map(|place| writers.route(table, place))
            .collect();
        proptest::prop_assert!(routed.iter().all(|writer| *writer < count));
        let distinct: std::collections::BTreeSet<_> = routed.iter().collect();
        proptest::prop_assert_eq!(distinct.len(), count);
    }
}

#[tokio::test]
async fn a_writer_stages_writes_in_order_and_flushes_every_table_before_answering() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let (writers, mut tasks) = writers(1, 2, &log, [false, false, false], 8);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&writers, &budget, 0, 1, 1, 3).await.unwrap();
    write(&writers, &budget, 0, 0, 2, 4).await.unwrap();
    writers.flush().await.unwrap();
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
        "a table writer opens on its table's first write"
    );
    assert_eq!(
        budget.reserved(),
        0,
        "staged writes release their reservations"
    );
    drop(writers);
    writer.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_failed_write_ends_the_writer_with_a_destination_error() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let (writers, mut tasks) = writers(1, 1, &log, [false, true, false], 1);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&writers, &budget, 0, 0, 1, 1).await.unwrap();
    let error = writer.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Destination);
    let after = write(&writers, &budget, 0, 0, 2, 1).await.unwrap_err();
    assert_eq!(after.kind(), ErrorKind::Cancelled);
    assert_eq!(
        writers.flush().await.unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn a_failed_flush_ends_the_writer_and_the_flush_is_not_answered() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let (writers, mut tasks) = writers(1, 1, &log, [false, false, true], 1);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&writers, &budget, 0, 0, 1, 1).await.unwrap();
    assert_eq!(
        writers.flush().await.unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(
        writer.await.unwrap().unwrap_err().kind(),
        ErrorKind::Destination
    );
}

#[tokio::test]
async fn cancelling_a_writer_ends_it() {
    let log = Log::default();
    let cancel = CancellationToken::new();
    let (_writers, mut tasks) = writers(1, 1, &log, [false, false, false], 1);
    let writer = tokio::spawn(tasks.remove(0).run(cancel.clone()));
    cancel.cancel();
    assert_eq!(
        writer.await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn a_writer_that_cannot_open_a_table_writer_ends_with_a_destination_error() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let (writers, mut tasks) = writers(1, 1, &log, [true, false, false], 1);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&writers, &budget, 0, 0, 1, 1).await.unwrap();
    let error = writer.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Destination);
    assert!(
        error
            .to_string()
            .contains("creating a table writer for table t0"),
        "{error}"
    );
}

#[tokio::test]
async fn each_write_goes_through_a_table_writer_of_the_schema_version_it_was_lowered_for() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let (writers, mut tasks) = writers(1, 1, &log, [false, false, false], 8);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    let at = |version| (0, 0, SchemaVersion(version));
    write_at(&writers, &budget, at(1), 1, 2).await.unwrap();
    write_at(&writers, &budget, at(2), 1, 3).await.unwrap();
    // A partition still lowering for the older version writes after the table changed: the
    // older version's table writer, retired by the newer's first write, opens again.
    write_at(&writers, &budget, at(1), 2, 4).await.unwrap();
    writers.flush().await.unwrap();
    assert_eq!(
        *log.lock(),
        [
            "open t0 v1",
            "t0 v1 s1 r2",
            "t0 v1 flush",
            "close t0 v1",
            "open t0 v2",
            "t0 v2 s1 r3",
            "open t0 v1",
            "t0 v1 s2 r4",
            "t0 v1 flush",
            "t0 v2 flush"
        ]
    );
    drop(writers);
    writer.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_flush_reaches_only_the_table_writers_written_since_the_last() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let (writers, mut tasks) = writers(1, 1, &log, [false, false, false], 8);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    let at = |version| (0, 0, SchemaVersion(version));
    write_at(&writers, &budget, at(1), 1, 2).await.unwrap();
    write_at(&writers, &budget, at(2), 1, 3).await.unwrap();
    writers.flush().await.unwrap();
    log.lock().clear();
    // The table has moved on to version 2: its older table writer has nothing left to flush.
    write_at(&writers, &budget, at(2), 2, 4).await.unwrap();
    writers.flush().await.unwrap();
    assert_eq!(*log.lock(), ["t0 v2 s2 r4", "t0 v2 flush"]);
    drop(writers);
    writer.await.unwrap().unwrap();
}

/// Waits until `log` holds `entry`.
async fn logged(log: &Log, entry: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !log.lock().iter().any(|logged| logged == entry) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the entry is logged");
}

#[tokio::test]
async fn a_write_holds_its_bytes_until_its_table_writer_flushes_them() {
    // A table writer may buffer what it stages until it flushes, so the bytes stay charged till
    // then.
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let (writers, mut tasks) = budgeted(1, 1, &log, [false, false, false], 8, &budget);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&writers, &budget, 0, 0, 1, 3).await.unwrap();
    logged(&log, "t0 v0 s1 r3").await;
    assert_eq!(budget.reserved(), 10);
    writers.flush().await.unwrap();
    assert_eq!(budget.reserved(), 0);
    drop(writers);
    writer.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_writer_flushes_its_table_writers_once_the_budget_is_pressed() {
    let log = Log::default();
    // Pushes may take 27 bytes of this budget: two writes of ten and no third.
    let budget = crate::budget::budget(64);
    let (writers, mut tasks) = budgeted(1, 1, &log, [false, false, false], 8, &budget);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&writers, &budget, 0, 0, 1, 3).await.unwrap();
    write(&writers, &budget, 0, 0, 1, 4).await.unwrap();
    logged(&log, "t0 v0 s1 r4").await;
    // A request that waits for the written bytes gets them once the writer flushes on its own.
    let waiting = tokio::time::timeout(Duration::from_secs(5), budget.acquire(10))
        .await
        .expect("the writer releases what it flushed");
    assert!(log.lock().iter().any(|entry| entry == "t0 v0 flush"));
    drop(waiting);
    drop(writers);
    writer.await.unwrap().unwrap();
}

/// How many table writers `log` says are open.
fn open(log: &Log) -> usize {
    let log = log.lock();
    let opened = log.iter().filter(|entry| entry.starts_with("open")).count();
    let closed = log
        .iter()
        .filter(|entry| entry.starts_with("close"))
        .count();
    opened - closed
}

#[tokio::test]
async fn a_writer_retires_the_table_writer_of_a_superseded_version() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000_000);
    let (writers, mut tasks) = writers(1, 1, &log, [false, false, false], 8);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    for version in 1..=50 {
        write_at(&writers, &budget, (0, 0, SchemaVersion(version)), 1, 1)
            .await
            .unwrap();
    }
    writers.flush().await.unwrap();
    assert_eq!(open(&log), 1);
    // What a retired table writer staged was flushed before it closed.
    let retired = log.lock().iter().position(|entry| entry == "close t0 v1");
    let flushed = log.lock().iter().position(|entry| entry == "t0 v1 flush");
    assert!(flushed.is_some() && flushed < retired, "{:?}", log.lock());
    drop(writers);
    writer.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_writer_holds_no_more_table_writers_than_its_share() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000_000);
    let tables = 200;
    let (writers, mut tasks) = writers(1, tables, &log, [false, false, false], 8);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    let share = crate::config::GrowthLimits::default().table_writers().get();
    for table in 0..tables {
        write(&writers, &budget, 0, table, 1, 1).await.unwrap();
        assert!(open(&log) <= share, "{} table writers open", open(&log));
    }
    // Every table's writes reach the destination, flushed before a table writer closes.
    writers.flush().await.unwrap();
    for table in 0..tables {
        let flushed = format!("t{table} v0 flush");
        assert!(log.lock().contains(&flushed), "{flushed}");
    }
    drop(writers);
    writer.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_writer_counts_the_time_a_full_queue_blocks_and_its_writes_and_flushes_take() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let tally = Arc::new(Tally::default());
    let session = Session {
        log: Arc::clone(&log),
        fail_open: false,
        fail_write: false,
        fail_flush: false,
        pace: Duration::from_secs(1),
    };
    let (writers, tasks) = over(session, (2, 1, 1), &budget, &tally);
    let running: Vec<_> = tasks
        .into_iter()
        .map(|writer| tokio::spawn(writer.run(CancellationToken::new())))
        .collect();
    // The queue holds one write: the third waits while the first is written, a second.
    for segment in 1..=3 {
        write(&writers, &budget, 1, 0, segment, 1).await.unwrap();
    }
    writers.flush().await.unwrap();
    drop(writers);
    for writer in running {
        writer.await.unwrap().unwrap();
    }
    let second = WriterCounters {
        blocked: Duration::from_secs(1),
        writing: Duration::from_secs(3),
        flushing: Duration::from_secs(2),
        flushes: Flushes {
            commit: 1,
            ..Flushes::default()
        },
    };
    assert_eq!(
        tally.counters().writers,
        [WriterCounters::default(), second]
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_writer_adds_what_it_counted() {
    let log = Log::default();
    let budget = crate::budget::budget(1_000);
    let tally = Arc::new(Tally::default());
    let session = Session {
        log: Arc::clone(&log),
        fail_open: false,
        fail_write: false,
        fail_flush: false,
        pace: Duration::from_secs(1),
    };
    let cancel = CancellationToken::new();
    let (writers, mut tasks) = over(session, (1, 1, 1), &budget, &tally);
    let writer = tokio::spawn(tasks.remove(0).run(cancel.clone()));
    for segment in 1..=2 {
        write(&writers, &budget, 0, 0, segment, 1).await.unwrap();
    }
    writers.flush().await.unwrap();
    // The third write is cancelled halfway, so its time is not counted.
    write(&writers, &budget, 0, 0, 3, 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    let ended = tokio::time::timeout(Duration::from_secs(60), writer)
        .await
        .expect("a cancelled writer ends");
    assert_eq!(ended.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
    let counted = WriterCounters {
        blocked: Duration::ZERO,
        writing: Duration::from_secs(2),
        flushing: Duration::from_secs(2),
        flushes: Flushes {
            commit: 1,
            ..Flushes::default()
        },
    };
    assert_eq!(tally.counters().writers, [counted]);
}

#[tokio::test(start_paused = true)]
async fn a_failed_write_or_flush_counts_its_time() {
    // A write takes one second and a flush two; the writer fails at the refused call.
    for (fail_write, fail_flush, counted) in [(true, false, (1, 0)), (false, true, (1, 2))] {
        let budget = crate::budget::budget(1_000);
        let tally = Arc::new(Tally::default());
        let session = Session {
            log: Log::default(),
            fail_open: false,
            fail_write,
            fail_flush,
            pace: Duration::from_secs(1),
        };
        let (writers, mut tasks) = over(session, (1, 1, 1), &budget, &tally);
        let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
        write(&writers, &budget, 0, 0, 1, 1).await.unwrap();
        if fail_flush {
            let flushed = tokio::time::timeout(Duration::from_secs(60), writers.flush()).await;
            assert!(flushed.expect("a refused flush ends").is_err());
        }
        let ended = tokio::time::timeout(Duration::from_secs(60), writer)
            .await
            .expect("a failed writer ends");
        assert!(ended.unwrap().is_err());
        let (writing, flushing) = counted;
        let counted = WriterCounters {
            blocked: Duration::ZERO,
            writing: Duration::from_secs(writing),
            flushing: Duration::from_secs(flushing),
            flushes: Flushes {
                commit: u64::from(fail_flush),
                ..Flushes::default()
            },
        };
        assert_eq!(
            tally.counters().writers,
            [counted],
            "fail_write {fail_write}"
        );
    }
}

#[tokio::test]
async fn a_writer_started_under_pressure_flushes_what_it_holds_at_once() {
    let log = Log::default();
    let budget = crate::budget::budget(64);
    let (writers, mut tasks) = budgeted(1, 1, &log, [false, false, false], 8, &budget);
    // Pressure is on before the writer first waits for it.
    let pressing = budget.pressing();
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    write(&writers, &budget, 0, 0, 1, 3).await.unwrap();
    logged(&log, "t0 v0 flush").await;
    assert_eq!(budget.reserved(), 0, "the write's bytes were released");
    drop(pressing);
    drop(writers);
    writer.await.unwrap().unwrap();
}

/// What the writer's tally counted of its table writers' flushes, once `writers` and the writer are
/// gone.
async fn flushes_of(
    writers: Writers,
    writer: tokio::task::JoinHandle<Result<(), crate::Error>>,
    tally: &Tally,
) -> Flushes {
    drop(writers);
    let ended = tokio::time::timeout(Duration::from_secs(5), writer).await;
    ended
        .expect("the writer ends once its senders are gone")
        .unwrap()
        .unwrap();
    tally.counters().writers[0].flushes
}

#[tokio::test]
async fn a_writer_counts_each_table_writer_flush_by_what_asked_for_it() {
    let log = Log::default();
    // Pushes may take 27 bytes of this budget: two writes of ten and no third.
    let budget = crate::budget::budget(64);
    let tally = Arc::new(Tally::default());
    let session = Session {
        log: Arc::clone(&log),
        fail_open: false,
        fail_write: false,
        fail_flush: false,
        pace: Duration::ZERO,
    };
    let (writers, mut tasks) = over(session, (1, 2, 8), &budget, &tally);
    let writer = tokio::spawn(tasks.remove(0).run(CancellationToken::new()));
    // A newer version retires the older's table writer, which flushes what it was written; a commit
    // then flushes the newer.
    write_at(&writers, &budget, (0, 0, SchemaVersion(1)), 1, 1)
        .await
        .unwrap();
    write_at(&writers, &budget, (0, 0, SchemaVersion(2)), 1, 1)
        .await
        .unwrap();
    writers.flush().await.unwrap();
    // A commit flushes each table writer written since the last flush: two tables'.
    write_at(&writers, &budget, (0, 0, SchemaVersion(2)), 2, 1)
        .await
        .unwrap();
    write(&writers, &budget, 0, 1, 2, 1).await.unwrap();
    writers.flush().await.unwrap();
    // A request waiting for the bytes two staged writes hold presses the writer to flush both.
    write_at(&writers, &budget, (0, 0, SchemaVersion(2)), 3, 1)
        .await
        .unwrap();
    write(&writers, &budget, 0, 1, 3, 1).await.unwrap();
    logged(&log, "t1 v0 s3 r1").await;
    let admitted = tokio::time::timeout(Duration::from_secs(5), budget.acquire(10))
        .await
        .expect("the writer releases what it flushed");
    drop(admitted);
    let expected = Flushes {
        pressure: 2,
        commit: 3,
        retired: 1,
    };
    assert_eq!(
        flushes_of(writers, writer, &tally).await,
        expected,
        "{:?}",
        log.lock()
    );
}
