//! A served read's push is admitted within what its frame was charged before it was decoded.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::cost::push_charge;
use rdlt_connector::wire::MALFORMED_FRAME;
use rdlt_connector::{Admission, ConnectorErrorKind, Permit, Push};
use rdlt_wire::bounded::Charge;
use rdlt_wire::limits::Class;

use super::{charging, pushed_json};
use crate::budget::Decoding;
use crate::cost::Admitted;

/// A push of `bytes` of JSON text.
fn json(bytes: usize) -> Push {
    Push::Json(bytes::Bytes::from(vec![b' '; bytes]))
}

/// A push of a batch of `rows` ids.
fn ids(rows: i64) -> Push {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    Push::Arrow(RecordBatch::try_from_iter([("id", ids)]).unwrap())
}

#[tokio::test]
async fn a_frame_s_charge_shrinks_to_its_push_s_and_asks_for_nothing_more() {
    let (budget, admission) = charging(1 << 30, 1);
    let decoding = Decoding(budget.clone());
    for push in [ids(1_000), json(1_000)] {
        let charged = push_charge(&push);
        let mut charge: Permit = decoding.charge(Class::Data, 100_000).await.unwrap();
        assert_eq!(budget.reserved(), 100_000);
        admission.admit_charged(&push, &mut charge).unwrap();
        assert_eq!(budget.reserved(), charged, "{push:?}");
        let admitted = Admitted::of(charge).expect("the engine's permit");
        assert_eq!(admitted.bytes, charged);
        drop(admitted);
        assert_eq!(budget.reserved(), 0);
    }
    // A push charged as much as its frame is admitted within it.
    let push = ids(1_000);
    let exact = usize::try_from(push_charge(&push)).unwrap();
    let mut charge: Permit = decoding.charge(Class::Data, exact).await.unwrap();
    admission.admit_charged(&push, &mut charge).unwrap();
    assert_eq!(budget.waited().intake.count, 0, "no push waited");
}

#[tokio::test(start_paused = true)]
async fn a_push_admitted_within_its_frame_s_charge_forgets_a_wait_that_failed_before() {
    let (budget, admission) = charging(6_400, 1);
    let held = admission.admit(&pushed_json(748)).await.unwrap();
    admission.admit(&pushed_json(1)).await.err().unwrap();
    assert!(admission.exhausted().is_some(), "the wait is remembered");
    drop(held);
    let mut charge: Permit = Decoding(budget.clone())
        .charge(Class::Data, 2_000)
        .await
        .unwrap();
    admission.admit_charged(&json(10), &mut charge).unwrap();
    assert_eq!(admission.exhausted(), None);
}

#[tokio::test]
async fn a_push_charged_more_than_its_frame_is_refused_as_a_malformed_frame() {
    let (budget, admission) = charging(1 << 30, 1);
    let decoding = Decoding(budget.clone());
    let push = ids(1_000);
    let short = usize::try_from(push_charge(&push) - 1).unwrap();
    let mut charge: Permit = decoding.charge(Class::Data, short).await.unwrap();
    let refused = admission.admit_charged(&push, &mut charge).unwrap_err();
    assert_eq!(refused.code(), Some(MALFORMED_FRAME));
    assert_eq!(refused.kind(), ConnectorErrorKind::Internal);
    assert_eq!(
        budget.reserved(),
        u64::try_from(short).unwrap(),
        "the charge is its holder's to let go once the push is gone"
    );
    drop(charge);
    assert_eq!(budget.reserved(), 0);
    // A charge the engine did not make admits nothing.
    let mut foreign: Permit = Box::new(u64::MAX);
    let refused = admission.admit_charged(&push, &mut foreign).unwrap_err();
    assert_eq!(refused.kind(), ConnectorErrorKind::Internal);
}
