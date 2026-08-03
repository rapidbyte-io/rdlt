//! The commit protocol under redelivery and interleaving: receipts
//! forever, once-per-load truncation guarded durably, state landing
//! with the receipt LAST.

use rdlt_connector_file::destination::{self, DestFormat};
use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector_sdk::spi::{Destination, OpenContext};
use rdlt_testkit::{batch_of, commit_meta_for, schema_for};

use super::common::local_dest;

async fn run_load(
    config: &destination::Config,
    pipeline: &PipelineId,
    load: &str,
    seq: u64,
    mode: &WriteMode,
    rows: &[i64],
) {
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let load_id = LoadId::new(load);
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load_id.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), mode)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(rows))
        .await
        .expect("write");
    s.commit(commit_meta_for(pipeline, &load_id, seq))
        .await
        .expect("commit");
}

/// A redelivered (load, seq) is recognised even after LATER loads have
/// run — receipts are retained for the life of the destination.
#[tokio::test]
async fn a_redelivered_commit_is_recognised_after_later_loads_have_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("later-loads");
    run_load(&config, &pipeline, "load-1", 1, &WriteMode::Append, &[1, 2]).await;
    run_load(&config, &pipeline, "load-2", 1, &WriteMode::Append, &[3]).await;
    // Redeliver load-1 seq 1: recognised, nothing republished.
    run_load(&config, &pipeline, "load-1", 1, &WriteMode::Append, &[1, 2]).await;
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3,
        "the redelivered load republished nothing"
    );
}

/// Replace truncation runs ONCE per load, read from the receipt log: a
/// second commit of the SAME load must not delete what the first
/// commit of that load published.
#[tokio::test]
async fn replace_truncates_once_per_load_guarded_by_the_receipt_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("replace-guard");
    // A predecessor load's data.
    run_load(
        &config,
        &pipeline,
        "load-old",
        1,
        &WriteMode::Replace,
        &[9, 9, 9],
    )
    .await;

    // A new Replace load, TWO commits through one session.
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let load = LoadId::new("load-new");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("first commit");
    s.write(&TableName::new("events"), batch_of(&[2]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("second commit");

    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        2,
        "the predecessor is gone; BOTH of this load's commits survive"
    );
}

/// The state document lands with the receipt and reads back for its
/// OWN pipeline only — a sibling sharing the output stays invisible.
#[tokio::test]
async fn state_reads_back_scoped_to_its_own_pipeline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("state-owner");
    let sibling = PipelineId::new("state-sibling");
    run_load(&config, &pipeline, "load-1", 1, &WriteMode::Append, &[1]).await;

    let dest = destination::Shell::new(config.clone()).expect("valid");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), LoadId::new("load-2")))
        .await
        .expect("open");
    let state = s.read_state(&pipeline).await.expect("read");
    assert_eq!(state.expect("the owner sees its state").pipeline, pipeline);
    let none = s.read_state(&sibling).await.expect("read");
    assert!(none.is_none(), "a sibling pipeline sees nothing");
}

/// Opening a session reclaims only ITS OWN pipeline's staging: a
/// sibling's staged parts and state survive.
#[tokio::test]
async fn open_does_not_destroy_another_pipelines_staging_or_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let ours = PipelineId::new("pipeline-a");
    let theirs = PipelineId::new("pipeline-b");

    // The sibling stages a part but never commits (a live session).
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let their_load = LoadId::new("their-load");
    let mut their_session = dest
        .open(OpenContext::new(theirs.clone(), their_load.clone()))
        .await
        .expect("open");
    their_session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    their_session
        .write(&TableName::new("events"), batch_of(&[7]))
        .await
        .expect("write");

    // Our session opens: the sibling's staged file must survive.
    let _our_session = dest
        .open(OpenContext::new(ours.clone(), LoadId::new("our-load")))
        .await
        .expect("open");
    their_session
        .commit(commit_meta_for(&theirs, &their_load, 1))
        .await
        .expect("the sibling's staged part is still there to publish");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        1
    );
}

/// A future MANIFEST version refuses the same way — planted beside
/// the commit-log pin because the two gates must never drift. Review
/// round 3 caught this path unpinned, hiding a garbled spelling (a
/// run of literal spaces baked into the message).
#[tokio::test]
async fn a_future_manifest_version_refuses_upgrade_not_reset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("future-manifest");
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline.as_str(), 12);
    let file = format!("_rdlt_manifest.{scope}.json");
    std::fs::write(
        dir.path().join(&file),
        r#"{"format_version": 99, "load_id": "x", "commit_seq": 1, "names": []}"#,
    )
    .expect("plant");

    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-1");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains(&format!(
            "manifest `{file}` format v99 is newer than this build supports \
             (v1); upgrade rdlt instead of resetting"
        )),
        "{err}"
    );
}

/// A future commit-log version refuses as an upgrade prompt with the
/// frozen spelling — never a reset.
#[tokio::test]
async fn a_future_commit_log_version_refuses_upgrade_not_reset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("future-log");
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline.as_str(), 12);
    let file = format!("_rdlt_commits.{scope}.json");
    std::fs::write(
        dir.path().join(&file),
        r#"{"format_version": 99, "receipts": []}"#,
    )
    .expect("plant");

    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-1");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains(&format!(
            "commit log `{file}` format v99 is newer than this build supports \
             (v1); upgrade rdlt instead of resetting"
        )),
        "{err}"
    );
}

/// Merge is refused at ensure with the frozen spelling.
#[tokio::test]
async fn merge_is_refused_at_ensure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = destination::Shell::new(local_dest(dir.path())).expect("valid");
    let mut s = dest
        .open(OpenContext::new(
            PipelineId::new("merge-refusal"),
            LoadId::new("load-1"),
        ))
        .await
        .expect("open");
    let err = s
        .ensure_table(
            &schema_for("events"),
            &WriteMode::Merge {
                key: vec!["id".into()],
            },
        )
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("file destination does not support Merge (capabilities.merge = false)"),
        "{err}"
    );
}

/// Jsonl parts publish under the same protocol (the format matters to
/// the encoder, never to the commit).
#[tokio::test]
async fn jsonl_output_moves_through_the_same_protocol() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path()).with_format(DestFormat::Jsonl);
    let pipeline = PipelineId::new("jsonl-out");
    run_load(
        &config,
        &pipeline,
        "load-1",
        1,
        &WriteMode::Append,
        &[1, 2, 3],
    )
    .await;
    assert!(
        dir.path().join("events/part-load-1-1-0.jsonl").is_file(),
        "the jsonl part carries the same frozen name shape"
    );
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );
}
