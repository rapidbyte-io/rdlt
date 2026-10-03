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
            Target::SpawnedSqlite,
            Target::SpawnedJsonl,
            Target::SpawnedArrow,
            Target::RemoteSqlite,
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

#[tokio::test]
async fn pushes_joined_past_the_destination_s_frame_limit_are_written_as_several_frames() {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use rdlt_host::Provider as _;
    use rdlt_wire::limits::MIN_FRAME_BYTES;

    use crate::support::batches::{BatchStream, batches};
    // The host sends frames of the least size a peer may take; a budget this large joins the two
    // pushes, each within that size, into one batch beyond it, which the host cuts as it writes.
    let options = rdlt_host::Options {
        limits: rdlt_wire::Limits {
            frame_bytes: MIN_FRAME_BYTES,
            ..rdlt_wire::Limits::default()
        },
        ..rdlt_host::Options::default()
    };
    let store = Target::SpawnedJsonl.name("joined_frames");
    let root = tempfile::tempdir().expect("a temporary directory");
    let config = serde_json::json!({ "root": root.path(), "format": "jsonl" });
    let id = rdlt_connector::ConnectorId::parse("io.rapidbyte.files").expect("a valid id");
    let reference = rdlt_host::ConnectorRef::new(id).path(crate::support::example("serve_files"));
    let placed = crate::support::local()
        .options(options)
        .destination(&reference, &config)
        .await
        .expect("the destination starts");
    let push = |from: i64, rows: usize| {
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(from..from + 3000));
        let text: ArrayRef = Arc::new(StringArray::from(vec!["t".repeat(1000); rows]));
        RecordBatch::try_from_iter([("id", ids), ("text", text)]).expect("a batch")
    };
    let pushes = vec![push(0, 3000), push(3000, 3000)];
    let source = batches(
        &store,
        vec![BatchStream::new("events", pushes).one_segment()],
    )
    .await;
    let outcome = engine(commit_every(1_000_000).memory(2 << 30))
        .run(
            pipeline(&store, [stream("events")]),
            source,
            Arc::from(placed.connector),
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 6000);
}
