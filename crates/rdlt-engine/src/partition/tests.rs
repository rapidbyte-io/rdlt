use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, UNIX_EPOCH};

use rdlt_connector::{
    Capabilities, ConnectContext, Cursor, OpenContext, Partition, PartitionId, PartitionState,
    PipelineId, SegmentId, StreamName, destination_factory, source_factory,
};
use rdlt_connector_reference::{GeneratorSource, MemoryDestination};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::coalesce::Coalescer;
use super::{
    CursorHold, Ingested, LoadClock, OpenSegment, PartitionContext, PartitionJob, Progress, Slots,
    end_state,
};
use crate::budget::MemoryBudget;
use crate::compute::Pool;
use crate::config::BatchPolicy;
use crate::env::{Env, InlineEnv};
use crate::lane::Lanes;
use crate::table::{SharedSession, Tables};
use crate::watch;

/// Bounds every wait of a check; a request waits on the budget far longer.
const WAIT: Duration = Duration::from_secs(1);

/// A read that ended with `received` rows after its last cursor, `written` of them written.
fn received(received: u64, written: u64, cursor: Option<u64>) -> Ingested {
    Ingested {
        open: OpenSegment {
            id: SegmentId(9),
            rows: written,
            received,
            ..OpenSegment::default()
        },
        last_cursor: cursor.map(|next| Cursor::encode(1, &next).unwrap()),
        sealed_rows: 0,
        stopped: false,
        coalescer: Coalescer::new(BatchPolicy::default()),
        schemas: super::write::Schemas::default(),
    }
}

fn ingested(rows: u64, cursor: Option<u64>) -> Ingested {
    received(rows, rows, cursor)
}

#[test]
fn a_partition_ends_at_its_last_cursor_unless_rows_follow_it() {
    let cursor = Cursor::encode(1, &5u64).unwrap();
    assert_eq!(
        end_state(&ingested(0, Some(5)), false),
        Some(PartitionState::Cursor(cursor))
    );
    assert_eq!(
        end_state(&ingested(3, Some(5)), false),
        Some(PartitionState::Done)
    );
    assert_eq!(
        end_state(&ingested(3, None), false),
        Some(PartitionState::Done)
    );
}

#[test]
fn rows_after_the_last_cursor_its_policy_discarded_all_of_end_the_partition() {
    assert_eq!(
        end_state(&received(2, 0, Some(5)), false),
        Some(PartitionState::Done)
    );
    assert_eq!(
        end_state(&received(2, 0, None), false),
        Some(PartitionState::Done)
    );
}

#[test]
fn an_unbounded_partition_ends_only_at_its_last_cursor_and_rows_after_it_are_read_again() {
    let cursor = Cursor::encode(1, &5u64).unwrap();
    assert_eq!(
        end_state(&ingested(0, Some(5)), true),
        Some(PartitionState::Cursor(cursor))
    );
    assert_eq!(end_state(&ingested(3, Some(5)), true), None);
    assert_eq!(end_state(&ingested(3, None), true), None);
    assert_eq!(end_state(&received(2, 0, Some(5)), true), None);
}

#[test]
fn a_partition_that_read_nothing_and_never_checkpointed_records_no_position() {
    assert_eq!(end_state(&ingested(0, None), false), None);
}

#[test]
fn a_sealed_segment_carries_its_rows_state_and_discards() {
    let mut discards = crate::report::ColumnDiscards::default();
    let (table, column): (Arc<str>, Arc<str>) = (Arc::from("events"), Arc::from("d"));
    let reason = crate::report::DiscardReason::OutOfRange;
    discards.add((&table, &column), reason, (2, 1));
    let open = OpenSegment {
        id: SegmentId(4),
        rows: 7,
        bytes: 70,
        received: 9,
        discarded_rows: 2,
        discarded_values: 1,
        discards: discards.clone(),
        deletes_ignored: 0,
        truncates_ignored: 0,
        shred: crate::report::ShredCounts::default(),
    };
    let seal = open.seal(2, PartitionState::Done, Some(3), CursorHold::default());
    assert_eq!(
        (
            seal.partition,
            seal.segment,
            seal.rows,
            seal.bytes,
            seal.answers
        ),
        (2, SegmentId(4), 7, 70, Some(3))
    );
    assert_eq!(seal.state, PartitionState::Done);
    assert_eq!((seal.discarded_rows, seal.discarded_values), (2, 1));
    assert_eq!(seal.discards, discards);
}

#[tokio::test]
async fn a_cursors_hold_shows_the_bytes_it_holds() {
    let budget = crate::budget::budget(1 << 20);
    let cursor = Cursor::new(1, b"abc").unwrap();
    let cancel = CancellationToken::new();
    let held = CursorHold::reserve(&budget, &cancel, &PartitionState::Cursor(cursor))
        .await
        .unwrap();
    assert_eq!(format!("{held:?}"), "CursorHold(3)");
    assert_eq!(format!("{:?}", CursorHold::default()), "CursorHold(0)");
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::disallowed_methods,
    reason = "the test bounds its wait on tokio's paused clock directly"
)]
async fn a_read_ending_at_its_last_cursor_holds_that_cursor_in_the_cursors_share() {
    let budget = MemoryBudget::new(64 << 20, Arc::new(InlineEnv), Duration::from_secs(60));
    let source = source_factory::<GeneratorSource>()
        .connect(
            json!({ "seed": 1, "streams": [{ "name": "events", "rows": 10 }] }),
            ConnectContext::new(),
        )
        .await
        .expect("the generator connects");
    let (progress, mut told) = tokio::sync::mpsc::unbounded_channel();
    let context = PartitionContext {
        source: Arc::from(source),
        progress,
        ..on_pool(budget.clone(), Pool::inline()).await
    };
    // Resumed past the partition's last row, the read sends nothing and ends where it resumed.
    let cursor = Cursor::encode(1, &json!({ "next": 10 })).unwrap();
    let job = PartitionJob {
        index: 0,
        stream: StreamName::new("events").unwrap(),
        table: 0,
        partition: Partition::new(PartitionId::parse("0").unwrap()),
        cursor: Some(cursor.clone()),
        on_demand: false,
        changes: None,
        stop: CancellationToken::new(),
        follow: false,
        reset_retention: false,
        exclude: rdlt_connector::Exclusion::default(),
    };
    let ran = tokio::time::timeout(WAIT, super::run(job, Arc::new(context))).await;
    ran.expect("the read ends in time")
        .expect("the read succeeds");
    let seal = std::iter::from_fn(|| told.try_recv().ok())
        .find_map(|progress| match progress {
            Progress::Sealed(seal) => Some(seal),
            _ => None,
        })
        .expect("the read seals where it ended");
    assert_eq!(seal.state, PartitionState::Cursor(cursor.clone()));
    let bytes = u64::try_from(cursor.bytes().len()).unwrap();
    assert_eq!((seal.held.bytes(), budget.reserved()), (bytes, bytes));
    drop(seal);
    assert_eq!(budget.reserved(), 0);
}

/// A partition's context over `budget` whose jobs run on `pool`, its source never called and its
/// lanes ended.
pub(super) async fn on_pool(budget: MemoryBudget, pool: Pool) -> PartitionContext {
    let env: Arc<dyn Env> = Arc::new(InlineEnv);
    let source = source_factory::<GeneratorSource>()
        .connect(json!({ "seed": 1, "streams": [] }), ConnectContext::new())
        .await
        .expect("the generator connects");
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "units" }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let open = OpenContext {
        pipeline: PipelineId::parse("units").expect("a valid pipeline id"),
    };
    let opened = destination.open(&open).await.expect("the session opens");
    let tables = Arc::new(Tables::new(SharedSession::new(opened.session)));
    let writers = crate::config::GrowthLimits::default().writers();
    let (lanes, _) = Lanes::new(
        (NonZeroUsize::MIN, writers),
        &tables,
        NonZeroUsize::MIN,
        &budget,
        (&env, &Arc::default()),
    );
    let (progress, _) = tokio::sync::mpsc::unbounded_channel();
    let (_, barrier) = watch::channel(0);
    let slots = Slots::new(NonZeroUsize::MIN, None, &budget);
    PartitionContext {
        source: Arc::from(source),
        lanes,
        tables,
        budget,
        rendering: Arc::new(crate::cost::rendering(&Capabilities::minimal())),
        progress,
        latest: Arc::default(),
        barrier,
        stop: CancellationToken::new(),
        cancel: CancellationToken::new(),
        slots,
        segments: Arc::new(AtomicU64::new(1)),
        buffer: NonZeroUsize::MIN,
        epoch: rdlt_connector::Epoch(1),
        clock: Arc::new(LoadClock::new(UNIX_EPOCH)),
        env: Arc::clone(&env),
        pool,
        batch: BatchPolicy::default(),
        stop_wait: Duration::from_secs(60),
    }
}

/// A clock reading what a list says, in turn, then its last reading.
struct Readings(
    parking_lot::Mutex<Vec<std::time::SystemTime>>,
    crate::env::SystemEnv,
);

impl Env for Readings {
    fn now(&self) -> std::time::SystemTime {
        let mut readings = self.0.lock();
        if readings.len() > 1 {
            readings.remove(0)
        } else {
            readings[0]
        }
    }

    fn instant(&self) -> std::time::Instant {
        self.1.instant()
    }

    fn sleep(&self, duration: Duration) -> crate::env::Sleep {
        self.1.sleep(duration)
    }

    fn random(&self) -> u64 {
        self.1.random()
    }

    fn compute(&self) -> &dyn crate::compute::ComputePool {
        self.1.compute()
    }

    fn cores(&self) -> NonZeroUsize {
        self.1.cores()
    }
}

#[test]
fn a_load_s_batches_are_received_in_order_whatever_the_clock_reads() {
    let at = |seconds: u64| UNIX_EPOCH + Duration::from_secs(seconds);
    let readings = [at(5), at(20), at(10), at(30)];
    let env = Readings(
        parking_lot::Mutex::new(readings.to_vec()),
        crate::env::SystemEnv::one_core(),
    );
    let clock = LoadClock::new(at(10));
    let received: Vec<_> = (0..4).map(|_| clock.received(&env)).collect();
    assert_eq!(received, [at(10), at(20), at(20), at(30)]);
    assert_eq!(clock.started(), at(10));
}
