//! A write let go before it ends frees what each of its charges covers before the charge is
//! released, so nothing else may take those bytes while they are still held.
#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock directly"
)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_buffer::Buffer;
use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{Permit, SchemaVersion, TablePath, TableRef};

use super::super::super::coalesce::{Flushed, Unit};
use super::super::super::{OpenSegment, PartitionContext};
use super::super::{Schemas, write_flushed};
use super::job;
use crate::budget::{MemoryBudget, Reservation};
use crate::compute::{ComputePool, Job, Pool};
use crate::env::{Env, InlineEnv, Sleep};
use crate::partition::tests::on_pool;
use crate::table::{Model, testing};

/// Bounds every wait of a check; a request waits on the budget far longer.
const WAIT: Duration = Duration::from_secs(1);

/// Whether what a charge covers was freed as the charge was released; nothing while it is held.
type Freed = Arc<Mutex<Option<bool>>>;

/// A charge for `buffers` that notes, as it is released, whether nothing but it held them.
struct Charge {
    buffers: Vec<Buffer>,
    freed: Freed,
}

impl Drop for Charge {
    fn drop(&mut self) {
        let alone = self.buffers.iter().all(|buffer| buffer.strong_count() == 1);
        *self.freed.lock() = Some(alone);
    }
}

/// A batch of `rows` integers from `first`.
fn ids(first: i64, rows: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(first..first + rows));
    RecordBatch::try_from_iter([("id", ids)]).unwrap()
}

/// A charge for `batches`, every buffer of their columns and their children, and what it notes
/// once released.
fn charge(batches: &[RecordBatch]) -> (Permit, Freed) {
    let mut buffers = Vec::new();
    let mut pending: Vec<_> = batches
        .iter()
        .flat_map(|batch| batch.columns().iter().map(|column| column.to_data()))
        .collect();
    while let Some(data) = pending.pop() {
        buffers.extend(data.buffers().iter().cloned());
        pending.extend(data.child_data().iter().cloned());
    }
    let freed = Freed::default();
    let charge = Charge {
        buffers,
        freed: Arc::clone(&freed),
    };
    (Box::new(charge), freed)
}

/// What `batches` are written as, held by `permit`.
fn flushed(batches: Vec<RecordBatch>, permit: Permit) -> Flushed {
    Flushed {
        unit: Unit::Arrow(batches),
        permits: vec![permit],
        bytes: 0,
    }
}

/// A budget whose requests wait far longer than a check.
fn budget() -> MemoryBudget {
    MemoryBudget::new(64 << 20, Arc::new(InlineEnv), Duration::from_secs(60))
}

/// Every byte lowering may take of `budget`, so the next request waits.
fn filled(budget: &MemoryBudget) -> Vec<Reservation> {
    let mut held = Vec::new();
    let mut bytes = budget.shares().request;
    while bytes > 0 {
        match budget.try_acquire_working(bytes) {
            Some(reserved) => held.push(reserved),
            None => bytes /= 2,
        }
    }
    held
}

/// A partition's context over `budget` with a table to write, normalized as `shape` where one
/// is given, its jobs run on `pool`.
async fn context(
    budget: MemoryBudget,
    pool: Pool,
    shape: Option<crate::normalize::Shape>,
) -> PartitionContext {
    let context = on_pool(budget, pool).await;
    let table = TableRef {
        path: TablePath::new(["events"]).unwrap(),
        name: "events".into(),
        version: SchemaVersion(0),
        generation: None,
        merge: None,
    };
    let resolver = testing::resolver("events");
    let added = context
        .tables
        .add_normalized(resolver, &table, Model::default(), shape);
    assert_eq!(added.unwrap(), 0);
    context
}

/// Writes `flushed` until it has waited a check's bound, and then lets it go.
async fn given_up(context: &PartitionContext, flushed: Flushed) {
    let (job, mut open, mut schemas) = (job(), OpenSegment::default(), Schemas::default());
    let write = write_flushed(&job, context, &mut open, &mut schemas, flushed);
    let waited = tokio::time::timeout(WAIT, write).await;
    assert!(waited.is_err(), "the write ended: {waited:?}");
}

#[tokio::test(start_paused = true)]
async fn a_write_given_up_waiting_for_room_frees_its_batches_before_their_charge() {
    let budget = budget();
    let _others = filled(&budget);
    let context = context(budget, Pool::inline(), None).await;
    let batch = ids(0, 10_000);
    let (permit, freed) = charge(std::slice::from_ref(&batch));
    given_up(&context, flushed(vec![batch], permit)).await;
    assert_eq!(*freed.lock(), Some(true));
}

#[tokio::test(start_paused = true)]
async fn a_normalized_write_given_up_waiting_for_room_frees_its_piece_before_its_charge() {
    let budget = budget();
    let _others = filled(&budget);
    let shape = crate::normalize::Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let context = context(budget, Pool::inline(), Some(shape)).await;
    let batch = ids(0, 10_000);
    let (permit, freed) = charge(std::slice::from_ref(&batch));
    given_up(&context, flushed(vec![batch], permit)).await;
    assert_eq!(*freed.lock(), Some(true));
}

#[tokio::test(start_paused = true)]
async fn a_unit_s_charge_is_released_once_the_last_of_its_pieces_is_let_go() {
    // Pieces of a sixty-fourth of a mebibyte: the unit is cut into many, and its lane, which
    // queues one write and is never run, takes the first and leaves the next waiting.
    let budget = MemoryBudget::new(1 << 20, Arc::new(InlineEnv), Duration::from_secs(60));
    let mut context = context(budget, Pool::inline(), None).await;
    let env: Arc<dyn Env> = Arc::new(InlineEnv);
    let writers = crate::config::GrowthLimits::default().writers();
    let (lanes, ends) = crate::lane::Lanes::new(
        (NonZeroUsize::MIN, writers),
        &context.tables,
        NonZeroUsize::MIN,
        &context.budget,
        (&env, &Arc::default()),
    );
    context.lanes = lanes;
    let batch = ids(0, 100_000);
    let (permit, freed) = charge(std::slice::from_ref(&batch));
    given_up(&context, flushed(vec![batch], permit)).await;
    assert_eq!(
        *freed.lock(),
        None,
        "a piece on its lane still holds the unit"
    );
    drop((ends, context));
    assert_eq!(*freed.lock(), Some(true));
}

/// A context over `budget` normalizing its stream, writing to two lanes that are never run, and
/// the lanes, each holding what is queued on it until dropped: a partition's rows go to the
/// first, its array's items to the second.
async fn normalizing(budget: MemoryBudget) -> (PartitionContext, Vec<crate::lane::Lane>) {
    let shape = crate::normalize::Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let mut context = context(budget, Pool::inline(), Some(shape)).await;
    let env: Arc<dyn Env> = Arc::new(InlineEnv);
    let writers = crate::config::GrowthLimits::default().writers();
    let window = NonZeroUsize::new(1_024).unwrap();
    let (lanes, ends) = crate::lane::Lanes::new(
        (NonZeroUsize::new(2).unwrap(), writers),
        &context.tables,
        window,
        &context.budget,
        (&env, &Arc::default()),
    );
    context.lanes = lanes;
    (context, ends)
}

#[tokio::test(start_paused = true)]
async fn a_normalized_unit_s_charge_is_released_once_the_last_of_its_pieces_on_any_lane_goes() {
    let budget = budget();
    let (context, mut ends) = normalizing(budget.clone()).await;
    // Rows holding arrays are cut into several pieces before the split, and the rows after them,
    // holding empty arrays, into more: the last pieces go all to the first lane. Each item's
    // text is long enough that lowering a piece's items takes no more than its allowance holds.
    let count = 88_000_i64;
    let mut lists =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    for row in 0..count {
        if row < 8_000 {
            for _ in 0..3 {
                lists.values().append_value("x".repeat(32));
            }
        }
        lists.append(true);
    }
    let lists = lists.finish();
    let rows: ArrayRef = Arc::new(Int64Array::from_iter_values(0..count));
    let batch =
        RecordBatch::try_from_iter([("id", rows), ("items", Arc::new(lists) as ArrayRef)]).unwrap();
    let (permit, freed) = charge(std::slice::from_ref(&batch));
    let (job, mut open, mut schemas) = (job(), OpenSegment::default(), Schemas::default());
    let written = write_flushed(
        &job,
        &context,
        &mut open,
        &mut schemas,
        flushed(vec![batch], permit),
    );
    written.await.unwrap();
    drop(context);
    // The first lane lets go of its writes, the last piece's among them: the second still holds
    // the items of the pieces before it.
    let second = ends.pop();
    drop(ends);
    assert_eq!(
        *freed.lock(),
        None,
        "the unit's charge went with its last piece"
    );
    assert!(
        budget.reserved() > 0,
        "its pieces hold what lowering them took"
    );
    drop(second);
    assert_eq!(*freed.lock(), Some(true));
    assert_eq!(budget.reserved(), 0, "every piece gave back what it took");
}

#[tokio::test(start_paused = true)]
async fn a_normalized_json_write_gives_back_all_it_reserved_once_its_pieces_go() {
    let budget = budget();
    let (context, ends) = normalizing(budget.clone()).await;
    // Rows whose arrays' items go to a table of their own, lowered inside what was reserved for
    // the flush.
    let records: Vec<String> = (0..2_000)
        .map(|row| {
            format!(
                "{{\"id\":{row},\"items\":[\"{}\",\"{row}\"]}}",
                "x".repeat(32)
            )
        })
        .collect();
    let text = Bytes::from(records.join("\n"));
    let permit = super::json_text(&budget, text.clone()).await;
    let flushed = Flushed {
        unit: Unit::Json(vec![text]),
        permits: vec![permit],
        bytes: 0,
    };
    let (job, mut open, mut schemas) = (job(), OpenSegment::default(), Schemas::default());
    let written = write_flushed(&job, &context, &mut open, &mut schemas, flushed);
    written.await.unwrap();
    drop(context);
    assert!(
        budget.reserved() > 0,
        "its pieces hold what lowering them took"
    );
    drop(ends);
    assert_eq!(budget.reserved(), 0, "every piece gave back what it took");
}

/// Jobs kept until they are run, as a pool whose threads are all busy keeps them.
#[derive(Default)]
struct Deferred(Mutex<Vec<Job>>);

impl ComputePool for Deferred {
    fn execute(&self, job: Job) {
        self.0.lock().push(job);
    }
}

/// An environment whose compute jobs wait in [`Deferred`] until they are run.
#[derive(Default)]
struct Deferring(Deferred);

impl Deferring {
    /// Runs every job kept so far.
    fn run(&self) {
        let jobs = std::mem::take(&mut *self.0.0.lock());
        for job in jobs {
            job();
        }
    }
}

impl Env for Deferring {
    fn now(&self) -> SystemTime {
        InlineEnv.now()
    }

    fn instant(&self) -> Instant {
        InlineEnv.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        InlineEnv.sleep(duration)
    }

    fn random(&self) -> u64 {
        InlineEnv.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        &self.0
    }

    fn cores(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_given_up_while_its_piece_waits_for_the_pool_lets_the_job_go_first() {
    let deferring = Arc::new(Deferring::default());
    let pool = Pool::new(Arc::clone(&deferring) as Arc<dyn Env>, Arc::default());
    let context = context(budget(), pool, None).await;
    // Two batches are one piece, which concatenating them copies: it is lowered on the pool.
    let batches = vec![ids(0, 1_000), ids(1_000, 1_000)];
    let (permit, freed) = charge(&batches);
    given_up(&context, flushed(batches, permit)).await;
    assert_eq!(*freed.lock(), Some(true));
    // The pool finds the job let go, and runs nothing.
    deferring.run();
}

/// A JSON push's text that notes in `order` when it is freed.
struct Text {
    text: Vec<u8>,
    order: Arc<Mutex<Vec<&'static str>>>,
}

impl AsRef<[u8]> for Text {
    fn as_ref(&self) -> &[u8] {
        &self.text
    }
}

impl Drop for Text {
    fn drop(&mut self) {
        self.order.lock().push("text");
    }
}

/// A charge that notes in `order` when it is released.
struct Noted(Arc<Mutex<Vec<&'static str>>>);

impl Drop for Noted {
    fn drop(&mut self) {
        self.0.lock().push("charge");
    }
}

#[tokio::test(start_paused = true)]
async fn a_json_write_given_up_waiting_for_room_frees_its_text_before_its_charge() {
    let budget = budget();
    let _others = filled(&budget);
    let context = context(budget, Pool::inline(), None).await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let text = Text {
        text: b"{\"a\":1}\n{\"a\":2}\n".to_vec(),
        order: Arc::clone(&order),
    };
    let flushed = Flushed {
        unit: Unit::Json(vec![Bytes::from_owner(text)]),
        permits: vec![Box::new(Noted(Arc::clone(&order)))],
        bytes: 0,
    };
    given_up(&context, flushed).await;
    assert_eq!(*order.lock(), ["text", "charge"]);
}
