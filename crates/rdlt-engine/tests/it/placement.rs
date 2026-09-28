//! The destination-facing scenarios against every reference destination spawned in a process of
//! its own, and listening on the network reached over mutual TLS: the placement matrix's second
//! and third legs (§20.9).
//!
//! They run on the real clock: a spawned connector's heartbeat and deadlines do, and a paused
//! clock would expire them while the connector's process works.

use rdlt_engine::RunStatus;

use crate::support::targets::Target;
use crate::support::{commit_every, each, engine, every_id, pipeline, spawned_generator, stream};
use crate::{changes, destinations, merge, normalized};

#[tokio::test]
async fn publishes_every_row_once() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::publishes_every_row_once,
    )
    .await;
}

#[tokio::test]
async fn resumes_an_incremental_read_where_it_committed() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::resumes_an_incremental_read_where_it_committed,
    )
    .await;
}

#[tokio::test]
async fn appends_a_fresh_copy_every_full_run_and_replaces_one() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::appends_a_fresh_copy_every_full_run_and_replaces_one,
    )
    .await;
}

#[tokio::test]
async fn keeps_a_stopped_replace_hidden_until_it_completes() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::keeps_a_stopped_replace_hidden_until_it_completes,
    )
    .await;
}

#[tokio::test]
async fn merges_the_newest_row_of_each_key_across_runs() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::merges_the_newest_row_of_each_key_across_runs,
    )
    .await;
}

#[tokio::test]
async fn publishes_a_commit_whose_response_was_lost_once() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::publishes_a_commit_whose_response_was_lost_once,
    )
    .await;
}

#[tokio::test]
async fn fences_an_older_run() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::fences_an_older_run,
    )
    .await;
}

#[tokio::test]
async fn evolves_a_table_as_its_batches_change() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::evolves_a_table_as_its_batches_change,
    )
    .await;
}

#[tokio::test]
async fn stores_nested_values_natively_or_as_json_text() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::stores_nested_values_natively_or_as_json_text,
    )
    .await;
}

#[tokio::test]
async fn loads_streams_named_like_its_own_tables() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        destinations::loads_streams_named_like_its_own_tables,
    )
    .await;
}

#[tokio::test]
async fn takes_tables_of_arrays_that_hold_only_arrays() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        normalized::takes_tables_of_arrays_that_hold_only_arrays,
    )
    .await;
}

#[tokio::test]
async fn takes_normalize_turned_on_for_an_existing_stream() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        normalized::takes_normalize_turned_on_for_an_existing_stream,
    )
    .await;
}

#[tokio::test]
async fn replaces_a_roots_children_when_it_merges() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        normalized::replaces_a_roots_children_when_it_merges,
    )
    .await;
}

#[tokio::test]
async fn keeps_the_children_of_a_keys_last_row_in_a_commit() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        normalized::keeps_the_children_of_a_keys_last_row_in_a_commit,
    )
    .await;
}

#[tokio::test]
async fn keeps_the_children_of_rows_after_a_dropped_one() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        normalized::keeps_the_children_of_rows_after_a_dropped_one,
    )
    .await;
}

#[tokio::test]
async fn drops_only_the_rows_carrying_a_change_among_rows_sharing_a_key() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        normalized::drops_only_the_rows_carrying_a_change_among_rows_sharing_a_key,
    )
    .await;
}

#[tokio::test]
async fn merges_into_a_table_it_appended_to_and_appends_again() {
    each(
        Target::SPAWNED.into_iter().chain(Target::REMOTE),
        merge::merges_into_a_table_it_appended_to_and_appends_again,
    )
    .await;
}

#[tokio::test]
async fn a_spawned_source_loads_every_row_once() {
    for target in [Target::Memory, Target::SpawnedSqlite, Target::RemoteSqlite] {
        let source = spawned_generator(&[("orders", 1000, 4, 37)]).await;
        let outcome = engine(commit_every(250))
            .run(
                pipeline("spawned-source", [stream("orders")]),
                source,
                target.destination("spawned_source").await,
            )
            .await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{target:?}");
        assert_eq!(
            target.ids("spawned_source", "orders"),
            every_id(1000),
            "{target:?}"
        );
    }
}

#[tokio::test]
async fn merges_changes_read_from_a_spawned_source() {
    each(
        [
            Target::SpawnedJsonl,
            Target::SpawnedArrow,
            Target::RemoteJsonl,
            Target::RemoteArrow,
        ],
        |target| async move {
            let source = changes::spawned_changes(8, &changes::orders(&[90])).await;
            changes::merges_changes(target, source).await;
        },
    )
    .await;
}
