//! State a destination answers an open with, held to what the engine could have recorded: one
//! record a key, and table names its rules admit, one table a name.

use std::sync::Arc;

use rdlt_connector::{Destination, StateEntry, StateRecord};
use rdlt_engine::{ErrorKind, ResetScope, RunOutcome, RunStatus};

use crate::support::destinations::{Restate, limited, restated};
use crate::support::{commit_every, engine, generator, memory, pipeline, stream};

/// The memory destination at `store`, reserving table names that begin `rdlt_`.
async fn reserving(store: &str) -> Arc<dyn Destination> {
    limited(memory(store).await, |capabilities| {
        capabilities.identifiers.reserved_table_prefixes = ["rdlt_".to_owned()].into();
    })
}

/// Loads `orders` and `items` into `store` as pipeline `name`, then loads them again through a
/// destination answering with the state `restate` rewrote.
async fn loaded_again(name: &str, store: &str, restate: Restate) -> RunOutcome {
    let source = generator(&[("orders", 20, 1, 7), ("items", 20, 1, 7)]).await;
    let plan = || pipeline(name, [stream("orders"), stream("items")]);
    let first = engine(commit_every(10))
        .run(plan(), Arc::clone(&source), reserving(store).await)
        .await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    let destination = restated(reserving(store).await, restate);
    engine(commit_every(10))
        .run(plan(), source, destination)
        .await
}

/// Checks that `outcome` failed on the state its destination answered with, loading nothing.
fn refused(outcome: &RunOutcome) {
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.as_ref().expect("the run fails");
    assert_eq!(
        (error.kind(), error.code(), error.is_retryable()),
        (ErrorKind::Destination, Some("state_invalid"), false),
        "{error}"
    );
    assert_eq!(outcome.report.rows, 0);
}

/// Renames every table `records` record to the name `rename` gives its present one.
fn renamed(records: &mut [StateRecord], rename: impl Fn(&str) -> String) {
    for record in records {
        let Ok(StateEntry::Names {
            table,
            physical,
            names,
        }) = StateEntry::from_record(record)
        else {
            continue;
        };
        let entry = StateEntry::Names {
            table,
            physical: Arc::from(rename(&physical)),
            names,
        };
        *record = entry.to_record();
    }
}

#[tokio::test(start_paused = true)]
async fn state_answering_a_key_twice_is_refused() {
    let repeat: Restate = |records| {
        let first = records.first().cloned().expect("a record");
        records.push(first);
    };
    refused(&loaded_again("repeated", "state_repeated", repeat).await);
}

#[tokio::test(start_paused = true)]
async fn a_recorded_table_name_the_destination_reserves_is_refused() {
    let reserved: Restate = |records| renamed(records, |name| format!("rdlt_{name}"));
    refused(&loaded_again("reserved", "state_reserved", reserved).await);
}

#[tokio::test(start_paused = true)]
async fn two_tables_recorded_under_one_name_are_refused() {
    let alike: Restate = |records| renamed(records, |_| "orders".to_owned());
    refused(&loaded_again("alike", "state_alike", alike).await);
}

#[tokio::test(start_paused = true)]
async fn a_recorded_table_name_outside_the_destination_s_rules_is_refused() {
    let unruly: Restate = |records| renamed(records, |name| format!("{name}\u{0}"));
    refused(&loaded_again("unruly", "state_unruly", unruly).await);
    let long: Restate = |records| renamed(records, |name| format!("{name}{}", "x".repeat(4096)));
    refused(&loaded_again("long", "state_long", long).await);
}

#[tokio::test(start_paused = true)]
async fn a_reset_refuses_a_recorded_table_name_the_destination_reserves() {
    let source = generator(&[("orders", 20, 1, 7)]).await;
    let plan = pipeline("reset_reserved", [stream("orders")]);
    let first = engine(commit_every(10))
        .run(plan, Arc::clone(&source), reserving("reset_reserved").await)
        .await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    let reserved: Restate = |records| renamed(records, |_| "rdlt_epochs".to_owned());
    let destination = restated(reserving("reset_reserved").await, reserved);
    let error = engine(commit_every(10))
        .reset(
            "reset_reserved",
            &["orders"],
            ResetScope::Tables,
            source,
            destination,
        )
        .await
        .expect_err("the reset is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Destination, Some("state_invalid"))
    );
}
