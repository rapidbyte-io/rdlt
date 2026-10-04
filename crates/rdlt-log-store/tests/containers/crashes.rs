//! Loads killed as they commit, their logs on each server, then run again: every row lands once,
//! and no log is left.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use rdlt_connector::PipelineId;
use serde_json::json;

use crate::servers::{BUCKET, KEY_ID, SECRET_KEY, Server, opened};

/// The steps a commit takes, each a point a load is killed at, at its second commit.
const POINTS: [&str; 7] = [
    "engine.wal.sync.before",
    "engine.wal.sync.after",
    "engine.ack.early",
    "engine.commit.before",
    "engine.commit.after",
    "engine.receipt.after",
    "engine.wal.remove",
];

/// The harness, built beside this test.
fn harness() -> PathBuf {
    let here = std::env::current_exe().expect("the test knows where it runs");
    let deps = here.parent().expect("tests run in a directory");
    let examples = deps
        .parent()
        .expect("the directory has a parent")
        .join("examples");
    examples.join("log_crash")
}

/// Runs the harness in `dir` with its log at `endpoint` beneath `prefix`, killed where
/// `failpoints` says.
fn run(dir: &Path, endpoint: &str, prefix: &str, failpoints: Option<&str>) -> ExitStatus {
    let store = dir.join("store.json");
    let config = json!({ "s3": {
        "bucket": BUCKET, "prefix": prefix, "region": "us-east-1",
        "endpoint": endpoint, "path_style": true,
        "access_key_id": "${env:RDLT_S3_ACCESS_KEY_ID}",
        "secret_access_key": "${env:RDLT_S3_SECRET_ACCESS_KEY}",
    }});
    std::fs::write(&store, config.to_string()).expect("the configuration is written");
    let mut command = Command::new(harness());
    command
        .arg(dir)
        .arg(&store)
        .env_remove("FAILPOINTS")
        .env("RDLT_S3_ACCESS_KEY_ID", KEY_ID)
        .env("RDLT_S3_SECRET_ACCESS_KEY", SECRET_KEY)
        .stdout(std::process::Stdio::null());
    if let Some(failpoints) = failpoints {
        command.env("FAILPOINTS", failpoints);
    }
    command.status().expect("the harness runs")
}

/// Checks `dir`'s destination holds every message once, and its source was told so.
fn verify(dir: &Path, case: &str) {
    let batches =
        rdlt_connector_reference::sqlite::published(dir.join("out.db"), "events").expect("reads");
    let mut messages = Vec::new();
    for batch in &batches {
        let partitions = batch
            .column_by_name("partition")
            .expect("a partition column");
        let partitions = partitions.as_string::<i32>();
        let offsets = batch.column_by_name("offset").expect("an offset column");
        let offsets = offsets.as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            messages.push((partitions.value(row).to_owned(), offsets.value(row)));
        }
    }
    messages.sort();
    let every: Vec<(String, i64)> = ["p0", "p1"]
        .into_iter()
        .flat_map(|partition| (0..50).map(move |offset| (partition.to_owned(), offset)))
        .collect();
    assert_eq!(messages, every, "{case}");
    let kept = std::fs::read(dir.join("events.group")).expect("the group file reads");
    let mut kept: Vec<(String, String, i64)> = serde_json::from_slice(&kept).expect("positions");
    kept.sort();
    let told = ["p0", "p1"].map(|partition| ("events".to_owned(), partition.to_owned(), 50));
    assert_eq!(kept, told, "{case}");
}

async fn every_row_lands_once(server: Server) {
    let running = server.start().await;
    for (index, point) in POINTS.into_iter().enumerate() {
        let case = format!("{server:?}: killed at {point}");
        let dir = tempfile::tempdir().expect("a temporary directory");
        let prefix = format!("crash/{index}");
        let failpoints = format!("{point}=1*off->return");
        let killed = run(dir.path(), &running.endpoint, &prefix, Some(&failpoints));
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&killed),
            Some(6),
            "{case}"
        );
        let again = run(dir.path(), &running.endpoint, &prefix, None);
        assert!(again.success(), "{case}: {again:?}");
        verify(dir.path(), &case);
        let store = opened(&running.endpoint, &prefix).await.expect("opens");
        let pipeline = PipelineId::parse("crash").expect("a valid pipeline");
        assert_eq!(store.loads(&pipeline).await.expect("lists"), [], "{case}");
        assert_eq!(
            store.leftovers(&pipeline).await.expect("lists"),
            [],
            "{case}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_load_killed_as_it_commits_on_rustfs_lands_every_row_once() {
    every_row_lands_once(Server::RustFs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_load_killed_as_it_commits_on_minio_lands_every_row_once() {
    every_row_lands_once(Server::Minio).await;
}
