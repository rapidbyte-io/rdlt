//! The read-back probe for suites whose connector runs in ANOTHER
//! PROCESS. The in-process suites' read-only-open discipline
//! (`test_conformance`'s `FileCount`) does NOT transfer here, and the
//! difference is a lock mechanism, not a convention: duckdb's
//! cross-process file lock refuses a READ-ONLY open while a read-write
//! holder lives — measured (042 Task 6) as the SAME
//! `Could not set lock on file` refusal the second read-write open
//! gets. Same-process opens dodge that lock entirely (which is why the
//! crate carries its own in-process registry), so `FileCount` works
//! beside a live in-process shell and would fail beside a live spawned
//! one.
//!
//! What DOES work beside a live cross-process holder — also measured —
//! is reading the FILES: copy `{file, file.wal}` into a scratch
//! directory and open the COPY read-only; the read-only open replays
//! the copied WAL in memory, so the count sees every committed row.
//! The copy is consistent because every probe the certify kit and the
//! kill matrix make lands at a reply boundary, where the connector is
//! idle awaiting its next frame — nothing is mid-write, and duckdb
//! checkpoints only at commit-time thresholds or shutdown, never
//! spontaneously between frames.

use async_trait::async_trait;
use rdlt_connector_sdk::spi::TableName;
use rdlt_testkit::{ProbeError, TableProbe};

/// Counts a table's committed rows through a snapshot copy of the
/// database file — safe beside a LIVE connector process holding the
/// file read-write. A table the connector has not created yet counts
/// as 0 (D1 probes before any table exists); a store whose file cannot
/// be copied or whose copy cannot be opened is an oracle failure, not
/// an empty table.
pub(crate) struct SnapshotCount(pub(crate) std::path::PathBuf);

#[async_trait]
impl TableProbe for SnapshotCount {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        let oracle_failure = |message: String| ProbeError { message };
        let scratch = tempfile::tempdir()
            .map_err(|e| oracle_failure(format!("snapshot scratch dir failed: {e}")))?;
        let copy = scratch.path().join("snapshot.duckdb");
        std::fs::copy(&self.0, &copy).map_err(|e| {
            oracle_failure(format!(
                "copying the database file `{}` failed: {e}",
                self.0.display()
            ))
        })?;
        // The WAL carries every commit since the last checkpoint; a
        // missing WAL just means everything already checkpointed into
        // the main file. duckdb names it by APPENDING `.wal` to the
        // whole file name (never by swapping an extension).
        let wal = {
            let mut name = self.0.as_os_str().to_owned();
            name.push(".wal");
            std::path::PathBuf::from(name)
        };
        if wal.is_file() {
            std::fs::copy(&wal, scratch.path().join("snapshot.duckdb.wal"))
                .map_err(|e| oracle_failure(format!("copying the WAL failed: {e}")))?;
        }
        let read_only = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .map_err(|e| oracle_failure(format!("read-only config failed: {e}")))?;
        let conn = duckdb::Connection::open_with_flags(&copy, read_only)
            .map_err(|e| oracle_failure(format!("read-only open of the snapshot failed: {e}")))?;
        // The kit's table names pass through quoting to stay one rule.
        // Only ABSENCE reads as zero: the kit probes tables before the
        // connector creates them (D1), and a table never created holds
        // 0 published rows — that zero is a fact. duckdb's structured
        // channel is degenerate (031: classification is message-prefix
        // keyed), so absence is keyed on the measured `Catalog Error`
        // class; any other failure is the oracle failing, never an
        // empty table.
        let ident = format!("\"{}\"", table.as_str().replace('"', "\"\""));
        match conn.query_row(&format!("SELECT count(*) FROM {ident}"), [], |row| {
            row.get::<_, u64>(0)
        }) {
            Ok(count) => Ok(count),
            Err(e) if e.to_string().contains("Catalog Error") => Ok(0),
            Err(e) => Err(oracle_failure(format!(
                "counting `{}` in the snapshot failed: {e}",
                table.as_str()
            ))),
        }
    }
}

/// The fail-open fold closed (042 fix wave): only ABSENCE reads as
/// zero. The broken read is a view over a parquet file deleted after
/// planting — `CREATE VIEW` binds eagerly, so a never-valid view cannot
/// be planted, while a valid one whose file later vanishes fails at
/// QUERY time (`IO Error`, measured) — exactly the query-arm failure
/// the old `unwrap_or(0)` folded into an empty table.
#[tokio::test]
async fn absence_counts_zero_but_a_broken_read_is_a_probe_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("store.duckdb");
    let parquet = dir.path().join("gone.parquet");
    {
        let conn = duckdb::Connection::open(&file).expect("open");
        conn.execute_batch(&format!(
            "CREATE TABLE present(v BIGINT); INSERT INTO present VALUES (1), (2); \
             COPY (SELECT 1 AS v) TO '{path}' (FORMAT PARQUET); \
             CREATE VIEW broken AS SELECT * FROM read_parquet('{path}');",
            path = parquet.display()
        ))
        .expect("plant");
    }
    std::fs::remove_file(&parquet).expect("the parquet file vanishes");

    let probe = SnapshotCount(file);
    assert_eq!(
        probe
            .count(&TableName::new("present"))
            .await
            .expect("a present table counts"),
        2
    );
    assert_eq!(
        probe
            .count(&TableName::new("never_created"))
            .await
            .expect("absence is a fact, not a failure"),
        0
    );
    let err = probe
        .count(&TableName::new("broken"))
        .await
        .expect_err("a genuine read failure must never read as an empty table");
    assert!(
        err.message.contains("counting `broken`"),
        "the probe error names the failing count: {}",
        err.message
    );
}
