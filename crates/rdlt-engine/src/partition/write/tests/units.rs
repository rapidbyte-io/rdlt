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

use super::job;
use crate::budget::{MemoryBudget, Reservation};
use crate::compute::Pool;
use crate::config::BatchPolicy;
use crate::env::{Env, InlineEnv};
use crate::lane::Lanes;
use crate::partition::coalesce::{Flushed, Unit};
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
    let push_bytes = u64::try_from(push.len()).expect("a push's length fits");
    let permit = super::json_text(&budget, push.clone()).await;
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
    let flushed = Flushed {
        unit: Unit::Json(vec![push]),
        permits: vec![permit],
        bytes: rdlt_connector::cost::JSON_CHARGE * push_bytes,
    };
    let (units, _) = super::super::units::of(&job(), &context, flushed)
        .await
        .expect("the push is built without waiting for what it holds itself");
    assert_eq!(units.len(), 1, "a chunk's batch");
}
