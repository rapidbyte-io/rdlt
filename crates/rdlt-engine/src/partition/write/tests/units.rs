//! What a partition holds while it waits to build its JSON pushes.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::super::units::{Beside, built};
use super::job;
use crate::budget::{MemoryBudget, Reservation};
use crate::compute::Pool;
use crate::config::BatchPolicy;
use crate::env::InlineEnv;
use crate::error::Error;
use crate::partition::PartitionContext;
use crate::partition::tests::on_pool;
use crate::shred::{self, ShredLimits};

/// A partition's context over `budget`, its pool running jobs on the calling thread, its
/// connectors never called.
async fn context(budget: MemoryBudget) -> PartitionContext {
    on_pool(budget, Pool::inline()).await
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

#[tokio::test(start_paused = true)]
async fn json_built_for_the_rest_of_a_request_holds_it_though_its_batches_take_no_more() {
    let budget = MemoryBudget::new(64 << 20, Arc::new(InlineEnv), Duration::from_secs(60));
    // Records of one long string, whose batches take no more than their text was admitted for.
    let record = format!("{{\"a\":\"{}\"}}", "x".repeat(1_000));
    let push = Bytes::from(vec![record; 100].join("\n"));
    let _permit = super::json_text(&budget, push.clone()).await;
    let request = budget.shares().request;
    let context = context(budget).await;
    let built = built(&job(), &context, &[push], Beside::Rest)
        .await
        .expect("the push is built");
    let held = |reserved: Option<Reservation>| reserved.map_or(0, |reserved| reserved.bytes());
    assert_eq!(built.excess, 0);
    assert_eq!(held(built.beyond), 0);
    assert_eq!(held(built.beside), request);
}

#[tokio::test(start_paused = true)]
async fn json_built_for_its_lowering_asks_what_its_batches_take_again_within_a_request() {
    let budget = MemoryBudget::new(64 << 20, Arc::new(InlineEnv), Duration::from_secs(60));
    let records: Vec<String> = (0..600)
        .map(|record| format!("{{\"k{record}\":{record}}}"))
        .collect();
    let push = Bytes::from(records.join("\n"));
    let text = u64::try_from(push.len()).expect("a push's length fits");
    let _permit = super::json_text(&budget, push.clone()).await;
    let request = budget.shares().request;
    let context = context(budget).await;
    let built = built(&job(), &context, &[push], Beside::Lowering)
        .await
        .expect("the push is built");
    let excess = built.excess;
    let beside = built.beside.as_ref().map(Reservation::bytes);
    let lowering = (excess + 2 * text).min(request - excess);
    assert!(
        excess > 0 && lowering < request - excess,
        "{excess} beside {request}"
    );
    assert_eq!(beside, Some(lowering));
}

/// The error building `pushes` with `beside` ends with.
async fn built_err(context: &PartitionContext, pushes: &[Bytes], beside: Beside) -> Error {
    match built(&job(), context, pushes, beside).await {
        Ok(_) => panic!("the build fits"),
        Err(error) => error,
    }
}
