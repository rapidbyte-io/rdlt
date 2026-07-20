//! T020/T021: source crash sweep (feature 005 FR-009, 003 gate G2 style) —
//! every registered fail point, first- AND second-occurrence passes, against
//! real Postgres + DuckDB; plus engine-owned mid-COPY retry resuming from a
//! MID-TABLE checkpoint, and a real container-kill mid-read.
//!
//! Needs a container runtime; run with `--features failpoints` (wired into
//! `make test TARGET=sweep`).

#![cfg(feature = "failpoints")]

mod common;

use common::PgFixture;
use rdlt_connector::core::failpoint::fail;
use rdlt_dest_duckdb::DuckDb;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_postgres::source::PostgresSource;

const TOTAL_ROWS: u64 = 100;

/// Fail points are PROCESS-GLOBAL (`fail::cfg`); nextest runs this binary's
/// tests concurrently in one process — serialize every test that arms them.
/// (tokio Mutex: held across awaits by design, for the whole test.)
static FAIL_POINT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const SEED: &str = "CREATE TABLE ev (id int8 PRIMARY KEY, v text); \
    INSERT INTO ev SELECT i, 'row-'||i FROM generate_series(1,100) i;";

/// Incremental on id, small batches ⇒ mid-stream checkpoints exist for the
/// resume paths to bite on.
fn source(conn: &str) -> PostgresSource {
    PostgresSource::from_yaml(&format!(
        "conn: \"{conn}\"\nbatch_max_rows: 10\ntables:\n  - name: ev\n    cursor:\n      column: id\n"
    ))
    .expect("config")
}

struct Rig {
    dest: DuckDb,
    workdir: std::path::PathBuf,
}

impl Rig {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = DuckDb::open(dir.path().join("out.duckdb")).expect("open db");
        let workdir = dir.path().join("wal");
        std::mem::forget(dir);
        Self { dest, workdir }
    }

    async fn attempt(&self, conn: &str) -> Result<rdlt_engine::RunReport, String> {
        self.attempt_mode(conn, &rdlt_connector::core::WriteMode::Append)
            .await
    }

    async fn attempt_mode(
        &self,
        conn: &str,
        mode: &rdlt_connector::core::WriteMode,
    ) -> Result<rdlt_engine::RunReport, String> {
        let mut config = EngineConfig::new("pg-src-sweep");
        config.workdir = Some(self.workdir.clone());
        config.write_mode = mode.clone();
        let engine = Engine::new(config, source(conn), self.dest.clone());
        match tokio::spawn(engine.run()).await {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(e)) => Err(e.to_string()),
            Err(join) => Err(format!("panicked: {join}")),
        }
    }

    fn count(&self) -> u64 {
        self.dest.count_rows("ev").unwrap_or(0)
    }
}

/// Registry discipline (G2.2, source half): the crate's exported list is
/// pinned here, and the sweep below iterates exactly it.
#[test]
fn registry_is_pinned() {
    let mut registry: Vec<&str> = rdlt_postgres::source::FAIL_POINTS.to_vec();
    registry.sort_unstable();
    let mut expected = vec![
        "pg.src.after_reflect",
        "pg.src.mid_copy",
        "pg.src.after_batch_push",
        "pg.src.before_checkpoint",
    ];
    expected.sort_unstable();
    assert_eq!(registry, expected, "update BOTH the const and this list");
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_postgres_source() {
    let _guard = FAIL_POINT_LOCK.lock().await;
    let fixture = PgFixture::start().await;
    fixture.seed(SEED).await;
    let conn = fixture.conn_url();

    // Append + keyed structured Merge (feature 006): the merge axis drives the
    // keyed delete+insert commit path under every crash point.
    let modes = [
        rdlt_connector::core::WriteMode::Append,
        rdlt_connector::core::WriteMode::Merge {
            key: vec!["id".into()],
        },
    ];
    for &point in rdlt_postgres::source::FAIL_POINTS {
        // First-occurrence, panic, and SECOND-occurrence passes (003 lesson:
        // sweeps that only fire a point's first occurrence missed real bugs).
        for action in ["return", "panic", "1*off->return"] {
            for mode in &modes {
                let rig = Rig::new();
                fail::cfg(point, action).expect("configure fail point");
                let armed1 = rig.attempt_mode(&conn, mode).await;
                // Second attempt still armed: failure during recovery itself.
                let armed2 = rig.attempt_mode(&conn, mode).await;
                fail::remove(point);
                // The instrument must FIRE (005 review): a deleted or unreachable
                // crash_point! site would leave armed attempts green and this
                // sweep vacuous — the exact class the fail/failpoints fix killed.
                match action {
                    "1*off->return" => assert!(
                        armed1.is_err() || armed2.is_err(),
                        "[{point} / {action}] armed attempts never failed — point not firing"
                    ),
                    _ => assert!(
                        armed1.is_err(),
                        "[{point} / {action}] first armed attempt succeeded — point not firing"
                    ),
                }

                let recovered = rig.attempt_mode(&conn, mode).await;
                assert!(
                    recovered.is_ok(),
                    "[{point} / {action} / {mode:?}] recovery failed: {recovered:?}"
                );
                assert_eq!(
                    rig.count(),
                    TOTAL_ROWS,
                    "[{point} / {action} / {mode:?}] exactly-once violated"
                );
                // Convergence: one more clean run moves nothing.
                let stable = rig.attempt_mode(&conn, mode).await.expect("stable run");
                assert_eq!(
                    stable.total_rows(),
                    0,
                    "[{point} / {action} / {mode:?}] not convergent"
                );
                assert_eq!(rig.count(), TOTAL_ROWS);
            }
        }
    }
}

/// The headline robustness property (research R5/R6): a TRANSIENT mid-COPY
/// failure is retried by the ENGINE within one run, resuming from the last
/// committed MID-TABLE checkpoint — the dlt gap ("no mid-table resume"),
/// closed, observable in a single `run()` call.
#[tokio::test(flavor = "multi_thread")]
async fn transient_mid_copy_resumes_within_one_run() {
    let _guard = FAIL_POINT_LOCK.lock().await;
    let fixture = PgFixture::start().await;
    fixture.seed(SEED).await;
    let rig = Rig::new();

    // Fail the first two COPY chunk polls, then heal: attempt 1 commits some
    // checkpointed batches before dying, attempts 2–3 resume past them.
    fail::cfg("pg.src.mid_copy", "2*return->off").expect("configure");
    let report = rig
        .attempt(&fixture.conn_url())
        .await
        .expect("run recovers in-run");
    fail::remove("pg.src.mid_copy");

    assert_eq!(
        rig.count(),
        TOTAL_ROWS,
        "exactly-once across in-run retries"
    );
    assert!(
        report.retries > 0,
        "the engine's retry counter surfaces in the report"
    );
}

/// A REAL connection loss: the container dies mid-read. The error is typed
/// (copy/connect phase), committed work survives, and the engine's bounded
/// retries never double-apply.
#[tokio::test(flavor = "multi_thread")]
async fn container_kill_mid_read_is_typed_and_preserves_commits() {
    let fixture = PgFixture::start().await;
    // Big enough that the read is still streaming when the container dies.
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text); \
             INSERT INTO ev SELECT i, repeat('x', 200) FROM generate_series(1, 500000) i;",
        )
        .await;
    let rig = Rig::new();
    let conn = fixture.conn_url();

    let killer = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        drop(fixture); // container stops; sockets die mid-stream
    });
    let outcome = rig.attempt(&conn).await;
    killer.await.expect("killer task");

    let err = outcome.expect_err("mid-read container death must fail the run");
    assert!(
        err.contains("postgres source")
            && (err.contains("copy phase") || err.contains("connect phase")),
        "typed error names source + phase: {err}"
    );
    // Whatever committed before the kill is a consistent cursor-ordered
    // prefix: max(id) == count(*) under the ordered incremental read.
    let count = rig.count();
    if count > 0 {
        let max_id = rig
            .dest
            .query_string("SELECT CAST(max(id) AS VARCHAR) FROM ev")
            .expect("max id");
        assert_eq!(max_id, count.to_string(), "committed prefix is contiguous");
    }
}
