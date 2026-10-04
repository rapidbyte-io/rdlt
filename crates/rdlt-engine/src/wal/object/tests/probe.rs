//! The probe a store meets before a log is kept there.

use std::sync::Arc;

use rdlt_testkit::objects::{Call, Fault, Op, Plan, faultless};

use super::{always, keys, objects, options, tries};
use crate::env::SystemClock;
use crate::wal::object::ObjectStoreWal;
use crate::{ErrorKind, ObjectStoreOptions};

/// The code of the error opening a log in a store faulted as `plan` says, with `options`.
async fn refused(plan: Plan, options: ObjectStoreOptions) -> (Option<String>, bool, Vec<String>) {
    let objects = objects(plan);
    let opened = ObjectStoreWal::open(
        Arc::clone(&objects) as _,
        "logs",
        Arc::new(SystemClock),
        options,
    )
    .await;
    let error = opened.expect_err("the probe refuses the store");
    let code = error.code().map(str::to_owned);
    assert_ne!(error.kind(), ErrorKind::Internal, "{error:?}");
    (code, error.is_retryable(), keys(&objects).await)
}

fn creates(call: &Call) -> bool {
    call.op == Op::Put { create: true }
}

fn lists(call: &Call) -> bool {
    call.op == Op::List
}

fn deletes(call: &Call) -> bool {
    call.op == Op::Delete
}

fn begins(call: &Call) -> bool {
    call.op == Op::Begin
}

fn reads(call: &Call) -> bool {
    call.op == Op::Get
}

#[tokio::test]
async fn a_store_that_takes_a_second_create_of_one_name_is_refused() {
    let (code, retryable, left) =
        refused(always(Fault::Overwrite, creates), options(1 << 20)).await;
    assert_eq!(code.as_deref(), Some("wal_storage_unsupported"));
    assert!(!retryable);
    assert_eq!(
        left,
        Vec::<String>::new(),
        "the probe's markers are deleted"
    );
}

#[tokio::test]
async fn a_store_that_implements_no_conditional_create_is_refused() {
    let (code, retryable, _) = refused(always(Fault::Unsupported, creates), options(1 << 20)).await;
    assert_eq!(code.as_deref(), Some("wal_storage_unsupported"));
    assert!(!retryable);
}

#[tokio::test]
async fn a_store_whose_listing_misses_a_fresh_object_is_refused() {
    let (code, retryable, left) = refused(always(Fault::Stale, lists), options(1 << 20)).await;
    assert_eq!(code.as_deref(), Some("wal_storage_unsupported"));
    assert!(!retryable);
    assert_eq!(left, Vec::<String>::new());
}

#[tokio::test]
async fn a_store_that_takes_no_upload_in_parts_is_refused() {
    let (code, retryable, _) = refused(always(Fault::Unsupported, begins), options(1 << 20)).await;
    assert_eq!(code.as_deref(), Some("wal_storage_unsupported"));
    assert!(!retryable);
}

#[tokio::test]
async fn a_store_that_keeps_what_it_says_it_deleted_is_refused() {
    let (code, retryable, _) = refused(always(Fault::Ignored, deletes), options(1 << 20)).await;
    assert_eq!(code.as_deref(), Some("wal_storage_unsupported"));
    assert!(!retryable);
}

#[tokio::test]
async fn a_store_that_refuses_the_credentials_is_refused_for_good() {
    let (code, retryable, _) = refused(always(Fault::Denied, reads), options(1 << 20)).await;
    assert_eq!(code.as_deref(), Some("wal_storage_denied"));
    assert!(!retryable);
}

#[tokio::test(start_paused = true)]
async fn a_store_that_never_answers_is_refused_retryably() {
    let (code, retryable, _) = refused(
        always(Fault::Hang, creates),
        tries(2, std::time::Duration::from_secs(1)),
    )
    .await;
    assert_eq!(code.as_deref(), Some("wal_storage_unavailable"));
    assert!(retryable);
}

#[tokio::test]
async fn a_store_that_does_what_a_log_needs_is_left_as_it_was() {
    let objects = objects(faultless());
    ObjectStoreWal::open(
        Arc::clone(&objects) as _,
        "logs",
        Arc::new(SystemClock),
        options(1 << 20),
    )
    .await
    .expect("the probe passes");
    assert_eq!(keys(&objects).await, Vec::<String>::new());
}
