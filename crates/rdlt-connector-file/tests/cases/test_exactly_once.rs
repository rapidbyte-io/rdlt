//! The commit protocol under redelivery and interleaving: receipts
//! forever, once-per-load truncation guarded durably, state landing
//! with the receipt LAST.

use rdlt_connector_file::destination::{self, DestFormat};
use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector_sdk::spi::{Destination, OpenContext};
use rdlt_testkit::{batch_of, commit_meta_for, schema_for};

use super::common::local_dest;

/// Open one session against a FRESH `File` connector — a distinct
/// instance every call, so this mints a distinct owner token each time
/// (037 US2 T7's mandatory rule: the owner is process-unique, never
/// derived from config/pipeline/path). This is the shape two separate
/// `rdlt run` invocations of the SAME pipeline take, as opposed to
/// `run_load`'s single shared `dest`, whose repeated `.open()` calls
/// all share one owner.
async fn open_session(
    dir: &std::path::Path,
    pipeline: &str,
    load: &str,
) -> Result<Box<dyn rdlt_connector_sdk::spi::LoadSession>, rdlt_connector_sdk::spi::DestinationError>
{
    let config = local_dest(dir);
    let dest = destination::Shell::new(config).expect("valid");
    dest.open(OpenContext::new(
        PipelineId::new(pipeline),
        LoadId::new(load),
    ))
    .await
}

/// S6 (030): scope-wide reclaim meant two live sessions of ONE
/// pipeline silently destroyed each other's staging. Now the second
/// open refuses typed while the first holds the lease.
#[tokio::test]
async fn a_second_session_of_the_same_pipeline_is_refused_not_destroyed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = open_session(dir.path(), "one-pipeline", "load-a")
        .await
        .expect("first opens");
    let second = open_session(dir.path(), "one-pipeline", "load-b").await;
    let err = match second {
        Ok(_) => panic!("the lease refuses a concurrent same-pipeline session"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("holds the destination lease"),
        "{err}"
    );
    drop(first);
}

/// 037 US2 T7 fix round 1: the hole the coordinator's own instruction
/// created and this fix closes. A session that publishes TWO commits
/// must hold the lease BETWEEN them, not just up to the first — a
/// second connector's open is refused after commit 1 and before commit
/// 2. Red-proved against the code this fix replaces (release at the end
/// of `publish`): there, the assertion below fails because the second
/// open SUCCEEDS — `first`'s lease was already released after its
/// first commit, so `second` reacquires as a fresh session and would
/// have gone on to destroy `first`'s in-flight staging for commit 2 via
/// `prepare_staging`'s scope-wide wipe.
#[tokio::test]
async fn a_second_session_is_refused_between_two_commits_of_one_session() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("multi-commit-lease");

    let config = local_dest(dir.path());
    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-a");
    let mut first = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("first opens");
    first
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    first
        .write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    first
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("first commit");

    // BETWEEN commit 1 and commit 2: a second connector's open must
    // still be refused — the lease is not released until `close`.
    let second = open_session(dir.path(), "multi-commit-lease", "load-b").await;
    let err = match second {
        Ok(_) => panic!(
            "a second session must be refused BETWEEN this session's commits, not just \
             before its first"
        ),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("holds the destination lease"),
        "{err}"
    );

    first
        .write(&TableName::new("events"), batch_of(&[2]))
        .await
        .expect("write");
    first
        .commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("second commit");
    first.close().await.expect("close");

    // AFTER close: a new session is welcome.
    open_session(dir.path(), "multi-commit-lease", "load-c")
        .await
        .expect("a session opens freely once the prior one has closed");
}

/// 037 US2 T7 fix round 1: the UX half of the same fix. A clean run
/// (open, write, commit, close) must not lock a NEW process's
/// immediate next run out for any part of the lease's TTL — `close`
/// releases promptly, so a fresh connector instance (a new owner) opens
/// right away.
#[tokio::test]
async fn consecutive_runs_are_never_ttl_locked_out_after_a_clean_close() {
    let dir = tempfile::tempdir().expect("tempdir");
    run_load(
        &local_dest(dir.path()),
        &PipelineId::new("consecutive-runs"),
        "load-a",
        1,
        &WriteMode::Append,
        &[1],
    )
    .await;

    // A brand-new connector instance (a new owner — the shape a new
    // `rdlt run` process takes) opens IMMEDIATELY, no TTL wait.
    let opened_immediately = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        open_session(dir.path(), "consecutive-runs", "load-b"),
    )
    .await
    .expect("must not need to wait out any part of the 300s TTL");
    opened_immediately.expect("a clean close leaves nothing for the next run to wait on");
}

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
    // 037 US2 T7 fix round 1: production usage (the engine) always
    // closes a session that completed its last commit — a NEW `dest`
    // per call here mints a NEW owner, so a helper that skipped this
    // would deadlock every subsequent call against this same pipeline
    // for the lease's full TTL.
    s.close().await.expect("close");
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
