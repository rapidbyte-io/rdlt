#![expect(
    clippy::disallowed_methods,
    reason = "the tests wait on tokio's paused clock, as the budget's do"
)]

use std::sync::Arc;
use std::time::Duration;

use rdlt_wire::bounded::Charge;
use rdlt_wire::limits::Class;
use rdlt_wire::tonic::Code;

use super::Decoding;
use crate::budget::MemoryBudget;
use crate::env::SystemEnv;

const BUDGET: u64 = 6_400;

fn budget() -> MemoryBudget {
    MemoryBudget::new(BUDGET).within(Arc::new(SystemEnv::one_core()), Duration::from_secs(3_600))
}

#[tokio::test(start_paused = true)]
async fn a_frame_is_charged_to_what_pushes_may_take_and_any_other_answer_to_its_own_share() {
    let budget = budget();
    let shares = budget.shares();
    let decoding = Decoding(budget.clone());
    let bytes = |share: u64| usize::try_from(share).unwrap();
    // A frame as large as pushes may take, which the share of answers could not hold.
    assert!(shares.intake > shares.control);
    let frame = decoding
        .charge(Class::Data, bytes(shares.intake))
        .await
        .unwrap();
    assert_eq!(budget.reserved(), shares.intake);
    drop(frame);
    for class in [Class::Catalog, Class::State, Class::Control] {
        let answer = decoding.charge(class, bytes(shares.control)).await.unwrap();
        assert_eq!(budget.reserved(), shares.control, "{class:?}");
        drop(answer);
        let large = decoding
            .charge(class, bytes(shares.control) + 1)
            .await
            .err()
            .unwrap();
        assert_eq!(large.code(), Code::OutOfRange, "{class:?}");
    }
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn an_answer_that_waits_until_the_deadline_fails_its_call_as_exhausted() {
    let budget = budget();
    let control = usize::try_from(budget.shares().control).unwrap();
    let decoding = Decoding(budget.clone());
    let held = decoding.charge(Class::Catalog, control).await.unwrap();
    let started = tokio::time::Instant::now();
    let refused = decoding.charge(Class::State, 1).await.err().unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(3_600));
    assert_eq!(refused.code(), Code::ResourceExhausted);
    assert!(
        refused.message().contains("decoding an answer"),
        "{refused:?}"
    );
    drop(held);
}
