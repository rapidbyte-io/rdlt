//! A table ≥ 10× the enforced memory ceiling snapshots successfully —
//! memory is bounded by configuration, not table size. The release CLI runs
//! as a subprocess under `prlimit --data` (heap ceiling); the test verifies
//! the 10× ratio from pg_total_relation_size, so the claim is measured, not
//! asserted.
//!
//! Destination = the streaming parquet writer: the point under test is the
//! SOURCE/engine path (never materializes the table); DuckDB's buffer-pool
//! reservations scale with its own config, not table size, and belong to its
//! own memory story. Measured on the reference machine (recorded on the
//! pre-swap in-process shape): 40 M rows / 6.86 GB source table → 39 MB
//! peak RSS under this ceiling.
//!
//! WHICH CONNECTORS THIS EXERCISES: since the 043 D1 swap the CLI spawns
//! the connectors a document names, so the `postgres:` source and `file:`
//! destination below resolve to THIS crate's release bin and the file
//! crate's, discovered off `<target>/release` prepended to the spawned
//! CLI's PATH. And prlimit's rlimits INHERIT across fork and exec, so the
//! `--data` ceiling binds the CLI AND each spawned connector process
//! individually — the per-process claim is now made of every process in
//! the pipeline, which strengthens what this test proves; that is
//! deliberate, not incidental.
//!
//! Self-skips (visibly) without `prlimit`, a container runtime, a built
//! release CLI (`make release`), or the two built release connector bins
//! (`make connector-bins`) — UNLESS `RDLT_HEAVY=1` (the sweep/deep
//! targets), where a missing prerequisite is a hard FAIL with instructions:
//! the deep job must never green-wash this claim by silently skipping.

use rdlt_connector_postgres::fixtures::PostgresContainer;

/// RLIMIT_DATA ceiling for the CLI process (heap + data mmaps).
const CEILING_BYTES: u64 = 256 * 1024 * 1024;
/// Seeded rows: ~170 B/row on-disk ⇒ ~6.9 GB, ≥ 25× the ceiling.
const ROWS: u64 = 40_000_000;

/// The release artifacts directory — `CARGO_TARGET_DIR` honored the way
/// the spawn suites honor it: `join` with an absolute value replaces the
/// prefix, so both spellings resolve exactly as cargo treats them.
fn release_dir() -> std::path::PathBuf {
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into());
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(target)
        .join("release")
}

fn release_bin(name: &str) -> Option<std::path::PathBuf> {
    let path = release_dir().join(name);
    path.exists().then_some(path)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_ten_times_the_memory_ceiling_still_snapshots_within_it() {
    let heavy = std::env::var("RDLT_HEAVY").is_ok_and(|value| value == "1");
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
    let Some(cli) = release_bin("rdlt") else {
        assert!(
            !heavy,
            "RDLT_HEAVY=1 but the release CLI is not built — run `make release` \
             first; this test must RUN in the deep job, not skip"
        );
        eprintln!("SKIP memory_bound: release CLI not built (run `make release` first)");
        return;
    };
    // Since the D1 swap the CLI spawns the connectors its document names,
    // so the two release bins are prerequisites exactly like the CLI —
    // same announced skip, same RDLT_HEAVY refusal, never a silent pass.
    for bin in ["rdlt-connector-postgres", "rdlt-connector-file"] {
        if release_bin(bin).is_none() {
            assert!(
                !heavy,
                "RDLT_HEAVY=1 but the release connector bin `{bin}` is not built — \
                 run `make connector-bins` first; this test must RUN in the deep \
                 job, not skip"
            );
            eprintln!(
                "SKIP memory_bound: release connector bin `{bin}` not built \
                 (run `make connector-bins` first)"
            );
            return;
        }
    }

    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    container
        .seed(
            "CREATE TABLE big (id int8 PRIMARY KEY, bucket int4, payload text); \
             INSERT INTO big SELECT i, (i % 1000)::int4, repeat('x', 100) \
             FROM generate_series(1, 40000000) i;",
        )
        .await;

    // The 10× claim is MEASURED: on-disk table size vs the ceiling.
    let client = container.client().await;
    let table_bytes: i64 = client
        .query_one("SELECT pg_total_relation_size('big')", &[])
        .await
        .expect("size")
        .get(0);
    assert!(
        table_bytes as u64 >= 10 * CEILING_BYTES,
        "seeded table ({table_bytes} B) must be ≥ 10× the ceiling ({CEILING_BYTES} B)"
    );

    let directory = tempfile::tempdir().expect("tempdir");
    let source_yaml = directory.path().join("pg.yaml");
    std::fs::write(
        &source_yaml,
        format!(
            "conn: \"{}\"\ntables:\n  - name: big\n",
            container.connection_string
        ),
    )
    .expect("yaml");
    let spec = directory.path().join("pipeline.yaml");
    let report_path = directory.path().join("report.json");
    std::fs::write(
        &spec,
        format!(
            // `parts` is pinned SMALL on purpose. The destination
            // accumulates an output part in memory before writing it,
            // so the shipping 128 MiB default would put half the
            // ceiling into the writer and make this measure the
            // DESTINATION's file sizing rather than the source path
            // it claims to measure. 8 MiB keeps the destination's
            // contribution a known constant. The consequence is worth
            // stating plainly: a file-destination pipeline under a
            // tight memory limit must size `parts` to fit it.
            // `batch_policy.every_bytes` is the same rule's WIRE half:
            // it is what the facade feeds the connector dial as the
            // flow-control budget, so it caps how many encoded bytes
            // each spawned connector may hold in flight. The 64 MiB
            // default plus decode/build buffers does NOT fit a 256 MiB
            // per-process ceiling (measured: the source connector died
            // on a failed 15.8 MB allocation); 8 MiB does. A pipeline
            // under a tight memory limit must size BOTH knobs to fit.
            // The source's config rides the path form (a bare string
            // is a path), keeping the credential out of the pipeline
            // document exactly as before.
            "pipeline: membound\nworkdir: {}\nbatch_policy:\n  every_bytes: 8388608\n\
             source:\n  postgres: {}\n\
             destination:\n  file:\n    path: {}\n    format: parquet\n    parts:\n      \
             target_bytes: 8388608\n      max_open_bytes: 8388608\n",
            directory.path().join(".rdlt").display(),
            source_yaml.display(),
            directory.path().join("pq-out").display()
        ),
    )
    .expect("spec");

    // The CLI discovers the connector bins over ITS OWN PATH, so the
    // release directory is prepended for the child alone. The rlimit set
    // by prlimit inherits to those spawned connectors too — see the
    // header: every process in the pipeline is individually bound.
    let mut paths = vec![release_dir()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let path_with_bins = std::env::join_paths(paths).expect("PATH entries join");
    let output = std::process::Command::new("prlimit")
        .env("PATH", path_with_bins)
        // RLIMIT_DATA counts VIRTUAL reservations, and each glibc malloc
        // arena reserves 64 MiB of address space up front — a handful of
        // concurrently-allocating runtime threads bust a 256 MiB ceiling
        // before any real data does (measured: the source connector died
        // on a 15.8 MB allocation with almost nothing resident). The CLI
        // bounds its own arenas via mallopt, deliberately CLI-only —
        // library embedders and spawned connectors own their allocator
        // policy — so the SPAWNED processes get the same bound the same
        // way an operator would give it: the process-tree env knob.
        .env("MALLOC_ARENA_MAX", "2")
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
