//! T022 (SC-002): a table ≥ 10× the enforced memory ceiling snapshots
//! successfully — memory is bounded by configuration, not table size. The
//! release CLI runs as a subprocess under `prlimit --data` (heap ceiling);
//! the test verifies the 10× ratio from pg_total_relation_size, so the claim
//! is measured, not asserted.
//!
//! Destination = the streaming parquet writer: the point under test is the
//! SOURCE/engine path (never materializes the table); DuckDB's buffer-pool
//! reservations scale with its own config, not table size, and belong to its
//! own memory story. Measured on the reference machine: 40 M rows / 6.86 GB
//! source table → 39 MB peak RSS under this ceiling.
//!
//! Self-skips (visibly) without `prlimit`, a container runtime, or a built
//! release CLI (`make release`) — UNLESS `RDLT_HEAVY=1` (the sweep/deep
//! targets), where a missing prerequisite is a hard FAIL with instructions:
//! the deep job must never green-wash this claim by silently skipping.

mod common;

use common::PgFixture;

/// RLIMIT_DATA ceiling for the CLI process (heap + data mmaps).
const CEILING_BYTES: u64 = 256 * 1024 * 1024;
/// Seeded rows: ~170 B/row on-disk ⇒ ~6.9 GB, ≥ 25× the ceiling.
const ROWS: u64 = 40_000_000;

fn release_cli() -> Option<std::path::PathBuf> {
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into());
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(target)
        .join("release/rdlt");
    path.exists().then_some(path)
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_ten_times_larger_than_memory_ceiling() {
    let heavy = std::env::var("RDLT_HEAVY").is_ok_and(|v| v == "1");
    if std::process::Command::new("prlimit")
        .arg("--version")
        .output()
        .is_err()
    {
        assert!(
            !heavy,
            "RDLT_HEAVY=1 but prlimit is missing — install util-linux; \
             this test must RUN in the deep job, not skip"
        );
        eprintln!("SKIP memory_bound: prlimit not available (util-linux)");
        return;
    }
    let Some(cli) = release_cli() else {
        assert!(
            !heavy,
            "RDLT_HEAVY=1 but the release CLI is not built — run `make release` \
             first; this test must RUN in the deep job, not skip"
        );
        eprintln!("SKIP memory_bound: release CLI not built (run `make release` first)");
        return;
    };

    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE big (id int8 PRIMARY KEY, bucket int4, payload text); \
             INSERT INTO big SELECT i, (i % 1000)::int4, repeat('x', 100) \
             FROM generate_series(1, 40000000) i;",
        )
        .await;

    // The 10× claim is MEASURED: on-disk table size vs the ceiling.
    let client = fixture.client().await;
    let table_bytes: i64 = client
        .query_one("SELECT pg_total_relation_size('big')", &[])
        .await
        .expect("size")
        .get(0);
    assert!(
        table_bytes as u64 >= 10 * CEILING_BYTES,
        "seeded table ({table_bytes} B) must be ≥ 10× the ceiling ({CEILING_BYTES} B)"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let yaml = dir.path().join("pg.yaml");
    std::fs::write(
        &yaml,
        format!(
            "conn: \"{}\"\ntables:\n  - name: big\n",
            fixture.conn.clone()
        ),
    )
    .expect("yaml");
    let spec = dir.path().join("pipeline.yaml");
    let report_path = dir.path().join("report.json");
    std::fs::write(
        &spec,
        format!(
            "pipeline: membound\nworkdir: {}\nsource:\n  postgres: {{config: {}}}\n\
             destination:\n  parquet: {{path: {}}}\n",
            dir.path().join(".rdlt").display(),
            yaml.display(),
            dir.path().join("pq-out").display()
        ),
    )
    .expect("spec");

    let output = std::process::Command::new("prlimit")
        .arg(format!("--data={CEILING_BYTES}"))
        .arg("--")
        .arg(&cli)
        .arg("run")
        .arg(&spec)
        .arg("--report")
        .arg(&report_path)
        .output()
        .expect("spawn CLI under prlimit");
    assert!(
        output.status.success(),
        "CLI under a {CEILING_BYTES}-byte data ceiling failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Row-count equality proves the stream completed, not just survived.
    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&report_path).expect("report"))
            .expect("report json");
    let rows = report["tables"]["big"]["rows"]
        .as_u64()
        .expect("rows in report");
    assert_eq!(rows, ROWS);
}
