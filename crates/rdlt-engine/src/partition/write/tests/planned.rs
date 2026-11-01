//! What a normalized piece holds once its parts are planned is within what it keeps charged,
//! until the last of their pieces is written, whatever rows the cascade dropped and copied; and a
//! JSON flush that leaves its pieces a narrow room loads such rows where they fit it.
#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and run a lane directly"
)]

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{ArrayRef, Decimal256Array, Int8Array, Int64Array, RecordBatch};
use arrow_buffer::i256;
use bytes::Bytes;
use rdlt_connector::cost::batch_charge;
use rdlt_connector::{Exclusion, Permit, SchemaVersion, TablePath, TableRef};
use tokio_util::sync::CancellationToken;

use super::super::super::coalesce::{Flushed, Unit};
use super::super::super::{OpenSegment, PartitionContext};
use super::super::{Schemas, write_flushed};
use super::job;
use crate::budget::MemoryBudget;
use crate::compute::Pool;
use crate::config::BatchPolicy;
use crate::cost::tests::HEAP;
use crate::env::{Env, InlineEnv};
use crate::error::{Error, ErrorKind, codes};
use crate::lane::{Lane, Lanes};
use crate::limits::REQUEST_SHARE;
use crate::normalize::Shape;
use crate::partition::tests::on_pool;
use crate::policy::{SchemaPolicy, SchemaSettings};
use crate::shred::{self, ShredLimits};
use crate::table::{Model, testing};

/// Bounds every write: one that waits for room its lanes never give back fails, rather than
/// hangs.
const WAIT: Duration = Duration::from_secs(1);

/// Columns of small integers each row holds, which its table stores as 256-bit decimals: the
/// cascade copies a byte of each, and lowering makes thirty-two.
const NUMBERS: usize = 64;

/// A partition's context over `budget` with a table to write, normalized, whose pipeline drops
/// the rows holding a value its table cannot take, or a new array once the table is created.
async fn dropping(budget: MemoryBudget) -> PartitionContext {
    let context = on_pool(budget, Pool::inline()).await;
    let table = TableRef {
        path: TablePath::new(["events"]).unwrap(),
        name: "events".into(),
        version: SchemaVersion(0),
        generation: None,
        merge: None,
    };
    let mut resolver = testing::resolver("events");
    let dropping = SchemaSettings::new().policy(SchemaPolicy::DiscardRow);
    resolver.settings.stream = resolver.settings.stream.column("extra", dropping);
    let shape = Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let added = context
        .tables
        .add_normalized(resolver, &table, Model::default(), Some(shape));
    assert_eq!(added.unwrap(), 0);
    context
}

/// Gives `context` one lane, which holds what is queued on it until it is run or dropped.
fn lane(context: &mut PartitionContext) -> Vec<Lane> {
    let env: Arc<dyn Env> = Arc::new(InlineEnv);
    let writers = crate::config::GrowthLimits::default().writers();
    let (lanes, ends) = Lanes::new(
        (NonZeroUsize::MIN, writers),
        &context.tables,
        NonZeroUsize::new(1_024).unwrap(),
        &context.budget,
        (&env, &Arc::default()),
    );
    context.lanes = lanes;
    ends
}

/// A row of an id and `NUMBERS` 256-bit decimals, which make its table's columns of them.
fn decimals() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![0]));
    let wide = Decimal256Array::from(vec![i256::from_i128(1)]);
    let wide: ArrayRef = Arc::new(wide.with_precision_and_scale(76, 0).unwrap());
    let mut columns = vec![("id".to_owned(), ids)];
    columns.extend((0..NUMBERS).map(|column| (format!("n{column}"), Arc::clone(&wide))));
    RecordBatch::try_from_iter(columns).unwrap()
}

/// `rows` rows of an id and `NUMBERS` small integers, the first also holding an array `extra`,
/// the rest none.
fn small(rows: usize) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
        0..i64::try_from(rows).unwrap(),
    ));
    let mut columns = vec![("id".to_owned(), ids)];
    let numbers: ArrayRef = Arc::new(Int8Array::from(vec![1_i8; rows]));
    columns.extend((0..NUMBERS).map(|column| (format!("n{column}"), Arc::clone(&numbers))));
    let mut extras = ListBuilder::new(Int64Builder::new());
    extras.values().append_value(7);
    extras.append(true);
    for _ in 1..rows {
        extras.append(false);
    }
    columns.push(("extra".to_owned(), Arc::new(extras.finish())));
    RecordBatch::try_from_iter(columns).unwrap()
}

/// What `batch`, charged `permit`, is written as.
fn flushed(batch: RecordBatch, permit: Permit) -> Flushed {
    Flushed {
        unit: Unit::Arrow(vec![batch]),
        permits: vec![permit],
        bytes: 0,
    }
}

#[tokio::test(start_paused = true)]
async fn a_normalized_piece_holds_the_rows_its_cascade_copied_within_what_it_reserved() {
    let budget = MemoryBudget::new(64 << 20, Arc::new(InlineEnv), Duration::from_secs(60));
    let mut context = dropping(budget.clone()).await;
    let (job, mut open, mut schemas) = (job(), OpenSegment::default(), Schemas::default());
    // A first flush creates the table; none of it stays queued.
    let first = lane(&mut context);
    let created = flushed(decimals(), Box::new(()));
    let write = write_flushed(&job, &context, &mut open, &mut schemas, created);
    let written = tokio::time::timeout(WAIT, write).await;
    written.expect("the first flush is written").unwrap();
    let _lane = lane(&mut context);
    drop(first);
    // The first row holds a new array, which drops it: the cascade copies the rest of the
    // stream's part, slices of the unit. Lowered, they take more than an allowance of two
    // pieces, so the partition waits for its first pieces to be written, which hold all of it.
    let (before, reserved) = (HEAP.current_usage(), budget.reserved());
    let batch = small(20_000);
    let charged = budget.try_acquire_working(batch_charge(&batch)).unwrap();
    let (held, charged) = {
        let unit = flushed(batch, Box::new(charged));
        let write = write_flushed(&job, &context, &mut open, &mut schemas, unit);
        let mut write = std::pin::pin!(write);
        let waited = tokio::time::timeout(WAIT, write.as_mut()).await;
        assert!(
            waited.is_err(),
            "the write waits for its pieces: {waited:?}"
        );
        let held = u64::try_from(HEAP.current_usage().saturating_sub(before)).unwrap();
        (held, budget.reserved().saturating_sub(reserved))
    };
    assert!(held <= charged, "{charged} charged, {held} held");
    assert_eq!(open.discarded_rows, 1);
}

/// Records of an id and `NUMBERS` integers each, more than a narrow room's first piece holds,
/// the first holding a new array that drops it.
const DENSE: usize = 100;

/// Records of an array of `ITEMS` small integers each, whose batches take more than twice their
/// text, so a flush of them narrows the room lent to its pieces.
const ARRAYS: usize = 90;

/// Items of each array record.
const ITEMS: usize = 500;

/// A JSON flush's pushes: `dense` records of an id and `NUMBERS` integers, the first also holding
/// a new array `extra` where `extra` says so, then `arrays` records of an array of `ITEMS` small
/// integers, thirty a push.
fn records(dense: usize, arrays: usize, extra: bool) -> Vec<Bytes> {
    let mut numbers = String::new();
    for column in 0..NUMBERS {
        write!(numbers, ",\"n{column}\":1").expect("writing to a string fails nothing");
    }
    let dense: Vec<String> = (0..dense)
        .map(|row| {
            let new = if extra && row == 0 {
                ",\"extra\":[1]"
            } else {
                ""
            };
            format!("{{\"id\":{row}{numbers}{new}}}")
        })
        .collect();
    let mut pushes = vec![Bytes::from(dense.join("\n"))];
    let array = format!("{{\"items\":[{}]}}", vec!["1"; ITEMS].join(","));
    let arrays = vec![array; arrays];
    pushes.extend(
        arrays
            .chunks(30)
            .map(|records| Bytes::from(records.join("\n"))),
    );
    pushes
}

/// Bytes: what building the batches of `pushes` takes beyond twice their text.
async fn excess(pushes: &[Bytes]) -> u64 {
    let limits = ShredLimits::new(10_000);
    let chunk_bytes = BatchPolicy::default().chunk_bytes().get();
    let pool = Pool::inline();
    let excluded = Exclusion::default();
    let observed = shred::observe(&pool, pushes.to_vec(), chunk_bytes, limits, excluded);
    observed.await.unwrap().excess()
}

/// Writes `records(DENSE, ARRAYS, true)` after a flush that creates its table, through a
/// partition whose budget's request leaves its splits `narrow` bytes beside what building its
/// batches takes, its lane writing as it goes; how the second write ended.
async fn narrowed(narrow: u64) -> Result<OpenSegment, Error> {
    let pushes = records(DENSE, ARRAYS, true);
    let capacity = (excess(&pushes).await + narrow) * REQUEST_SHARE;
    let budget = MemoryBudget::new(capacity, Arc::new(InlineEnv), Duration::from_secs(60));
    let mut context = dropping(budget.clone()).await;
    let lane = lane(&mut context).remove(0);
    let writing = tokio::spawn(lane.run(CancellationToken::new()));
    let (job, mut open, mut schemas) = (job(), OpenSegment::default(), Schemas::default());
    let mut ended = Ok(());
    for pushes in [records(1, 0, false), pushes] {
        let mut permits = Vec::with_capacity(pushes.len());
        for push in &pushes {
            permits.push(super::json_text(&budget, push.clone()).await);
        }
        let unit = Flushed {
            unit: Unit::Json(pushes),
            permits,
            bytes: 0,
        };
        let write = write_flushed(&job, &context, &mut open, &mut schemas, unit);
        ended = tokio::time::timeout(WAIT, write)
            .await
            .expect("the write ends");
    }
    drop(context);
    writing.await.unwrap().unwrap();
    assert_eq!(budget.reserved(), 0, "every charge went back");
    ended.map(|()| open)
}

#[tokio::test(start_paused = true)]
async fn a_normalized_json_flush_leaving_its_splits_a_narrow_room_loads_the_rows_it_copied() {
    let open = narrowed(130_000).await.expect("the flush loads");
    assert_eq!(open.discarded_rows, 1);
    // The first flush's row, and of the second every item and every row but its first.
    let rows = DENSE + ARRAYS + ARRAYS * ITEMS;
    assert_eq!(open.rows, u64::try_from(rows).unwrap());
}

#[tokio::test(start_paused = true)]
async fn a_normalized_json_flush_whose_planned_rows_pass_its_narrow_room_is_refused() {
    let Err(refused) = narrowed(80_000).await else {
        panic!("a row's lowering fits beside the split");
    };
    assert_eq!(
        (refused.kind(), refused.code()),
        (ErrorKind::Source, Some(codes::ROW_EXCEEDS_BUDGET))
    );
    // What the row takes, the room left to it, the request, what building the batches took
    // beyond their text, and what the piece's split holds, as the refusal names them.
    let said = refused.to_string();
    let counts: Vec<u64> = said
        .split(|character: char| !character.is_ascii_digit())
        .filter_map(|count| count.parse().ok())
        .collect();
    let [needed, room, request, built, held] = counts[..] else {
        panic!("the refusal names five counts: {said}");
    };
    assert!(held > 0, "the split holds nothing: {said}");
    assert!(needed > room, "the row fits its room: {said}");
    assert_eq!(room, request - built - held, "{said}");
}
