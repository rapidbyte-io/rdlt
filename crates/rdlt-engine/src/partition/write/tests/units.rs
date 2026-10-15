//! What a partition holds while it waits to build its JSON pushes.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use rdlt_connector::{
    Capabilities, ConnectContext, LoadId, OpenContext, PipelineId, destination_factory,
    source_factory,
};
use rdlt_connector_reference::{GeneratorSource, MemoryDestination};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::super::units::{Beside, built};
use super::job;
use crate::budget::{MemoryBudget, Reservation};
use crate::compute::Pool;
use crate::config::BatchPolicy;
use crate::env::{Env, InlineEnv};
use crate::error::Error;
use crate::lane::Lanes;
use crate::partition::{LoadClock, PartitionContext, Slots};
use crate::shred::{self, ShredLimits};
use crate::table::{SharedSession, Tables};
use crate::watch;

/// A partition's context over `budget`, its pool running jobs on the calling thread, its
/// connectors never called.
async fn context(budget: MemoryBudget) -> PartitionContext {
    let env: Arc<dyn Env> = Arc::new(InlineEnv);
    let source = source_factory::<GeneratorSource>()
        .connect(json!({ "seed": 1, "streams": [] }), ConnectContext::new())
        .await
        .expect("the generator connects");
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "units" }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let load_id = LoadId::from_parts(UNIX_EPOCH, 1);
    let open = OpenContext {
        pipeline: PipelineId::parse("units").expect("a valid pipeline id"),
        load_id,
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
        slots: Slots::new(NonZeroUsize::MIN),
        segments: Arc::new(AtomicU64::new(1)),
        buffer: NonZeroUsize::MIN,
        load_id,
        clock: Arc::new(LoadClock::new(UNIX_EPOCH)),
        env: Arc::clone(&env),
        pool: Pool::inline(),
        batch: BatchPolicy::default(),
        stop_wait: Duration::from_secs(60),
        wal: None,
    }
}

/// Reserves `bytes` of what lowering holds, a request's share at a time.
fn held(budget: &MemoryBudget, mut bytes: u64) -> Vec<Reservation> {
    let mut held = Vec::new();
    while bytes > 0 {
        let taken = bytes.min(budget.shares().request);
        held.push(
            budget
                .try_acquire_working(taken)
                .expect("the budget has room"),
        );
        bytes -= taken;
    }
    held
}

#[tokio::test(start_paused = true)]
async fn a_partition_waiting_to_build_its_json_holds_none_of_what_observing_it_took() {
    let budget = MemoryBudget::new(64 << 20, Arc::new(InlineEnv), Duration::from_secs(60));
    // Six hundred records of a key each, whose nulls take building more than observing them may
    // hold.
    let records: Vec<String> = (0..600)
        .map(|record| format!("{{\"k{record}\":{record}}}"))
        .collect();
    let push = Bytes::from(records.join("\n"));
    let _permit = super::json_text(&budget, push.clone()).await;
    let limits = ShredLimits::new(budget.limits().schema_columns);
    let chunk_bytes = BatchPolicy::default().chunk_bytes().get();
    let observed = shred::observe(&Pool::inline(), vec![push.clone()], chunk_bytes, limits)
        .await
        .expect("the push is observed");
    let (observing, building) = (limits.beyond_bytes(), observed.excess());
    assert!(
        building > observing,
        "{building} bytes to build, {observing} to observe"
    );
    // The rest is held, as partitions waiting behind this one hold it: building fits only once
    // observing's room goes back, so a partition that waited holding it would wait for good.
    let free = budget.shares().data - budget.reserved();
    let _others = held(&budget, free - building - observing / 2);
    let context = context(budget).await;
    let built = built(&job(), &context, &[push], Beside::Bytes(0))
        .await
        .expect("the push is built without waiting for what it holds itself");
    assert_eq!(built.batches.len(), 1, "a chunk's batch");
    assert_eq!(built.beyond.map(|beyond| beyond.bytes()), Some(building));
}

#[tokio::test(start_paused = true)]
async fn json_whose_build_and_what_is_asked_beside_take_all_a_request_may_is_built() {
    // A thousand records of a key each: what their nulls take building beyond their text, and a
    // byte beside, are a request of the budget.
    let records: Vec<String> = (0..1_000)
        .map(|record| format!("{{\"k{record}\":{record}}}"))
        .collect();
    let push = Bytes::from(records.join("\n"));
    let chunk_bytes = BatchPolicy::default().chunk_bytes().get();
    let limits = ShredLimits::new(10_000);
    let observed = shred::observe(&Pool::inline(), vec![push.clone()], chunk_bytes, limits)
        .await
        .expect("the push is observed");
    let request = observed.excess() + 1;
    let capacity = request * crate::limits::REQUEST_SHARE;
    let budget = MemoryBudget::new(capacity, Arc::new(InlineEnv), Duration::from_secs(60));
    assert_eq!(budget.shares().request, request);
    let _permit = super::json_text(&budget, push.clone()).await;
    let context = context(budget).await;
    let pushes = [push];
    let built = built(&job(), &context, &pushes, Beside::Bytes(1))
        .await
        .expect("a build within a request is built");
    let beside = built.beside.as_ref().map(Reservation::bytes);
    assert_eq!((built.batches.len(), beside), (1, Some(1)));
    drop(built);
    let refused = built_err(&context, &pushes, Beside::Bytes(2)).await;
    assert_eq!(refused.code(), Some(crate::limits::JSON_EXCEEDS_BUDGET));
}

/// The error building `pushes` with `beside` ends with.
async fn built_err(context: &PartitionContext, pushes: &[Bytes], beside: Beside) -> Error {
    match built(&job(), context, pushes, beside).await {
        Ok(_) => panic!("the build fits"),
        Err(error) => error,
    }
}
