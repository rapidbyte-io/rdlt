//! Live conflict cell: two independent writers
//! hammering the SAME table concurrently — every commit lands, nobody's
//! snapshot is lost. (The deterministic retry/exhaustion pins live as
//! unit tests against a mock conflicting catalog in `dest/commit.rs` —
//! a live competitor cannot be timed into the load→commit window
//! reliably.) Skip-not-fail without a container runtime.

mod common;

use common::CatalogFixture;
use rdlt_connector::core::{LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector::{DestError, Destination, OpenCtx};
use rdlt_connector_iceberg::IcebergDest;
use rdlt_testkit::{batch_of, meta_for, schema_for};

const COMMITS_PER_WRITER: u64 = 4;

async fn run_writer(dest: IcebergDest, pipeline: &str, load: &str) -> Result<(), DestError> {
    let pipeline = PipelineId::new(pipeline);
    let load = LoadId::new(load);
    let table = TableName::new("contested");
    let mut session = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await?;
    session
        .ensure_table(&schema_for("contested"), &WriteMode::Append)
        .await?;
    for seq in 1..=COMMITS_PER_WRITER {
        session.write(&table, batch_of(&[seq as i64])).await?;
        session.commit(meta_for(&pipeline, &load, seq)).await?;
    }
    Ok(())
}

/// Two writers, one table, interleaved commits: the bounded retry
/// (refresh → rebuild → commit) must land every commit without ever
/// dropping the competitor's snapshots from history.
#[tokio::test(flavor = "multi_thread")]
async fn competing_writers_lose_no_snapshots() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let dest_a = IcebergDest::from_config(fixture.config("contest")).expect("dest a");
    let dest_b = IcebergDest::from_config(fixture.config("contest")).expect("dest b");

    let (a, b) = tokio::join!(
        run_writer(dest_a, "writer-a", "load-a"),
        run_writer(dest_b, "writer-b", "load-b"),
    );
    a.expect("writer a lands all commits");
    b.expect("writer b lands all commits");

    let snapshots = fixture.snapshot_summaries("contest", "contested").await;
    assert_eq!(
        snapshots.len() as u64,
        2 * COMMITS_PER_WRITER,
        "every commit from both writers is a snapshot — none lost"
    );
    for load in ["load-a", "load-b"] {
        let seqs: Vec<&str> = snapshots
            .iter()
            .filter(|s| s["rdlt.load-id"] == load)
            .map(|s| s["rdlt.commit-seq"].as_str())
            .collect();
        assert_eq!(
            seqs.len() as u64,
            COMMITS_PER_WRITER,
            "{load}: all identities present"
        );
    }
    let total: u64 = snapshots
        .iter()
        .map(|s| s["added-records"].parse::<u64>().expect("count"))
        .sum();
    assert_eq!(total, 2 * COMMITS_PER_WRITER, "one row per commit, exact");
}
