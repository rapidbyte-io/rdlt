//! Crash-recovery and shared-directory regressions for the parquet destination
//! (code-review findings on branch 002-file-arrow-ingestion).

use rdlt_connector::core::{LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector::{Destination, OpenCtx};
use rdlt_connector_file::ParquetDir;
use rdlt_testkit::{batch_of, meta_for, schema_for};

/// THE confirmed review finding: a Replace table must be truncated at most once per
/// LOAD, guarded durably. A crash between commit #1 and commit #2 recovers into a
/// fresh session — which must NOT re-truncate the files commit #1 already
/// published and logged.
#[tokio::test]
async fn replace_recovery_session_keeps_prior_commits_of_same_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = ParquetDir::open(dir.path()).expect("open dest");
    let pipeline = PipelineId::new("p1");
    let load = LoadId::new("load-a");
    let table = TableName::new("events");

    // Commit #1 lands durably.
    let mut s1 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open s1");
    s1.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s1.write(&table, batch_of(&[1, 2, 3])).await.expect("write");
    s1.commit(meta_for(&pipeline, &load, 1))
        .await
        .expect("commit 1");
    assert_eq!(dest.count_rows("events").expect("count"), 3);

    // Crash before commit #2's receipt: recovery opens a FRESH session with the
    // SAME load id and replays only the uncommitted tail.
    let mut s2 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open recovery session");
    s2.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure again");
    s2.write(&table, batch_of(&[4, 5]))
        .await
        .expect("write tail");
    s2.commit(meta_for(&pipeline, &load, 2))
        .await
        .expect("commit 2");

    assert_eq!(
        dest.count_rows("events").expect("count"),
        5,
        "commit #1's published rows must survive recovery (durable Replace guard)"
    );

    // A genuinely NEW load still replaces from scratch.
    let load_b = LoadId::new("load-b");
    let mut s3 = dest
        .open(OpenCtx::new(pipeline.clone(), load_b.clone()))
        .await
        .expect("open next load");
    s3.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s3.write(&table, batch_of(&[9])).await.expect("write");
    s3.commit(meta_for(&pipeline, &load_b, 1))
        .await
        .expect("commit");
    assert_eq!(
        dest.count_rows("events").expect("count"),
        1,
        "a new load's first commit still truncates Replace tables"
    );
}

/// Final part-file names must be a function of (load, seq, table, per-table index):
/// cross-table arrival order — nondeterministic under concurrent streams — must not
/// change any file's published name, or crash-replay double-publishes.
#[tokio::test]
async fn final_names_independent_of_cross_table_arrival_order() {
    let pipeline = PipelineId::new("p1");
    let load = LoadId::new("load-a");
    let a = TableName::new("alpha");
    let b = TableName::new("beta");

    let mut name_sets = Vec::new();
    for order in [["a", "b", "a"], ["b", "a", "a"]] {
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = ParquetDir::open(dir.path()).expect("open dest");
        let mut session = dest
            .open(OpenCtx::new(pipeline.clone(), load.clone()))
            .await
            .expect("open");
        session
            .ensure_table(&schema_for("alpha"), &WriteMode::Append)
            .await
            .expect("ensure a");
        session
            .ensure_table(&schema_for("beta"), &WriteMode::Append)
            .await
            .expect("ensure b");
        for which in order {
            let table = if which == "a" { &a } else { &b };
            session.write(table, batch_of(&[1])).await.expect("write");
        }
        session
            .commit(meta_for(&pipeline, &load, 1))
            .await
            .expect("commit");

        let mut names: Vec<String> = Vec::new();
        for table in ["alpha", "beta"] {
            for entry in std::fs::read_dir(dir.path().join(table)).expect("read dir") {
                let path = entry.expect("entry").path();
                if path.extension().is_some_and(|e| e == "parquet") {
                    names.push(format!(
                        "{table}/{}",
                        path.file_name().unwrap().to_string_lossy()
                    ));
                }
            }
        }
        names.sort();
        name_sets.push(names);
    }
    assert_eq!(
        name_sets[0], name_sets[1],
        "published names must not depend on cross-table interleaving"
    );
}

/// D4 teardown is scoped per pipeline: opening pipeline B against a shared output
/// directory must not destroy pipeline A's live staged data, state, or receipts.
#[tokio::test]
async fn open_does_not_destroy_another_pipelines_staging_or_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = ParquetDir::open(dir.path()).expect("open dest");
    let p1 = PipelineId::new("pipeline-one");
    let p2 = PipelineId::new("pipeline-two");
    let l1 = LoadId::new("load-1");
    let l2 = LoadId::new("load-2");
    let table = TableName::new("events");

    // Pipeline 1 is mid-flight: staged rows, nothing committed yet.
    let mut s1 = dest
        .open(OpenCtx::new(p1.clone(), l1.clone()))
        .await
        .expect("open p1");
    s1.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s1.write(&table, batch_of(&[1, 2])).await.expect("write");

    // Pipeline 2 opens the SAME output directory (its own D4 teardown runs).
    let mut s2 = dest
        .open(OpenCtx::new(p2.clone(), l2.clone()))
        .await
        .expect("open p2");
    s2.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s2.write(&table, batch_of(&[10])).await.expect("write");
    s2.commit(meta_for(&p2, &l2, 1)).await.expect("commit p2");

    // Pipeline 1's staged data survived and commits cleanly.
    s1.commit(meta_for(&p1, &l1, 1))
        .await
        .expect("p1 commit must still succeed — its staging was not torn down");
    assert_eq!(dest.count_rows("events").expect("count"), 3);

    // And each pipeline reads back its OWN state.
    let state1 = s1.read_state(&p1).await.expect("read p1");
    let state2 = s2.read_state(&p2).await.expect("read p2");
    assert_eq!(state1.expect("p1 state").pipeline, p1);
    assert_eq!(state2.expect("p2 state").pipeline, p2);
}

/// The commit contract has no recency clause: re-committing the same
/// `(load_id, commit_seq)` returns the prior receipt without re-publishing,
/// however many loads have run since.
///
/// Bounding the receipt log breaks exactly this. Once the redelivered load's
/// receipt is gone, the Replace guard concludes the load never committed and
/// truncates — destroying what LATER loads published — and Append re-publishes
/// its parts. Both are silent.
#[tokio::test]
async fn a_redelivered_commit_is_recognised_after_later_loads_have_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = ParquetDir::open(dir.path()).expect("open dest");
    let pipeline = PipelineId::new("p1");
    let table = TableName::new("events");

    let commit = |load: LoadId, rows: Vec<i64>| {
        let dest = dest.clone();
        let pipeline = pipeline.clone();
        let table = table.clone();
        async move {
            let mut s = dest
                .open(OpenCtx::new(pipeline.clone(), load.clone()))
                .await
                .expect("open");
            s.ensure_table(&schema_for("events"), &WriteMode::Replace)
                .await
                .expect("ensure");
            s.write(&table, batch_of(&rows)).await.expect("write");
            s.commit(meta_for(&pipeline, &load, 1))
                .await
                .expect("commit")
        }
    };

    commit(LoadId::new("load-a"), vec![1, 2, 3]).await;
    commit(LoadId::new("load-b"), vec![4]).await;
    commit(LoadId::new("load-c"), vec![5]).await;
    let settled = dest.count_rows("events").expect("count");
    assert_eq!(settled, 1, "each Replace load leaves only its own rows");

    // load-a is redelivered — a restored workdir replaying its WAL span, a second
    // engine sharing the output, or an embedder driving the session directly.
    commit(LoadId::new("load-a"), vec![1, 2, 3]).await;
    assert_eq!(
        dest.count_rows("events").expect("count"),
        settled,
        "a redelivered commit must publish nothing and truncate nothing — \
         load-c's data must survive"
    );
}
