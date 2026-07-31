//! Feature 009 T009: CDC crash sweep (contract O6) — every registered CDC
//! fail point × three actions × both occurrence passes with armed-fire
//! pins, post-recovery source-equality checks, redelivery convergence, and
//! a real container-kill mid-catch-up.
//!
//! Needs a container runtime; run with `--features failpoints` (wired into
//! `make test TARGET=sweep`).

#![cfg(feature = "failpoints")]

mod common;

use common::CdcPgFixture;
use rdlt_connector::core::failpoint::fail;
use rdlt_connector_postgres::dest::{DestinationOptions, MergeStrategy, Postgres, TableOptions};
use rdlt_connector_postgres::source::PostgresSource;
use rdlt_engine::{Engine, EngineConfig};

/// Fail points are PROCESS-GLOBAL; serialize every arming test.
static FAIL_POINT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const SEED: &str = "CREATE TABLE public.ev (id int8 PRIMARY KEY, v text); \
    INSERT INTO public.ev SELECT i, 'row-'||i FROM generate_series(1, 100) i;";

/// The recommended composition, small batches so checkpoints and commit
/// units exist for the crash paths to bite on.
struct Rig {
    workdir: std::path::PathBuf,
}

impl Rig {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("workdir");
        let workdir = dir.path().to_path_buf();
        std::mem::forget(dir);
        Self { workdir }
    }

    fn source(conn: &str) -> PostgresSource {
        PostgresSource::from_yaml(&format!(
            "conn: \"{conn}\"\nbatch_max_rows: 10\n\
             cdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
             tables:\n  - name: ev\n"
        ))
        .expect("cdc source config")
    }

    fn dest(conn: &str) -> Postgres {
        Postgres::connect(conn)
            .dataset("mirror")
            .options(DestinationOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: [(
                    "ev".to_string(),
                    TableOptions {
                        hard_delete: Some("_rdlt_deleted".into()),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("valid dest options")
    }

    async fn attempt(&self, conn: &str) -> Result<rdlt_engine::RunReport, String> {
        let mut config = EngineConfig::new("cdc-sweep");
        config = config.with_workdir(self.workdir.clone());
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let engine = Engine::new(config, Self::source(conn), Self::dest(conn));
        match tokio::spawn(engine.run()).await {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(e)) => Err(e.to_string()),
            Err(join) => Err(format!("panicked: {join}")),
        }
    }
}

async fn assert_mirror_equals(fixture: &CdcPgFixture, context: &str) {
    let client = fixture.client().await;
    for (a, b) in [("public", "mirror"), ("mirror", "public")] {
        let diff: i64 = client
            .query_one(
                &format!(
                    "SELECT count(*) FROM (SELECT id, v FROM {a}.ev \
                     EXCEPT SELECT id, v FROM {b}.ev) d"
                ),
                &[],
            )
            .await
            .expect("equality query")
            .get(0);
        assert_eq!(diff, 0, "[{context}] {a} \\ {b} should be empty");
    }
}

/// Registry discipline (O6): the crate's exported CDC list is pinned here,
/// and the sweep iterates exactly it.
#[test]
fn cdc_registry_is_pinned() {
    let mut registry: Vec<&str> = rdlt_connector_postgres::source::CDC_FAIL_POINTS.to_vec();
    registry.sort_unstable();
    let mut expected = vec![
        "cdc.slot.create",
        "cdc.snapshot.copy",
        "cdc.stream.peek",
        "cdc.ack.advance",
    ];
    expected.sort_unstable();
    assert_eq!(registry, expected, "update BOTH the const and this list");
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_cdc_fail_points() {
    let _guard = FAIL_POINT_LOCK.lock().await;

    for &point in rdlt_connector_postgres::source::CDC_FAIL_POINTS {
        // `cdc.stream.peek` only fires on a CHANGE pass — those cells run a
        // clean snapshot first, then mutate, then arm. Every other point
        // fires on the first (snapshot) run.
        let needs_change_pass = point == "cdc.stream.peek";
        for action in ["return", "panic", "1*off->return"] {
            let context = format!("{point} / {action}");
            let Some(fixture) = CdcPgFixture::start().await else {
                return;
            };
            fixture.seed(SEED).await;
            let conn = fixture.conn_url();
            let rig = Rig::new();

            if needs_change_pass {
                rig.attempt(&conn).await.expect("clean snapshot run");
                fixture
                    .seed(
                        "UPDATE public.ev SET v = 'changed' WHERE id <= 50; \
                         DELETE FROM public.ev WHERE id > 90; \
                         INSERT INTO public.ev VALUES (101, 'new');",
                    )
                    .await;
            }

            fail::cfg(point, action).expect("configure fail point");
            let armed1 = rig.attempt(&conn).await;
            // Second attempt still armed: failure during recovery itself.
            let armed2 = rig.attempt(&conn).await;
            fail::remove(point);
            // The instrument must FIRE: a deleted or unreachable crash_point!
            // site would leave armed attempts green and the sweep vacuous.
            match action {
                "1*off->return" => assert!(
                    armed1.is_err() || armed2.is_err(),
                    "[{context}] armed attempts never failed — point not firing"
                ),
                _ => assert!(
                    armed1.is_err(),
                    "[{context}] first armed attempt succeeded — point not firing"
                ),
            }

            // Recovery: the next clean run converges to source-equal state.
            let recovered = rig.attempt(&conn).await;
            assert!(
                recovered.is_ok(),
                "[{context}] recovery failed: {recovered:?}"
            );
            assert_mirror_equals(&fixture, &context).await;

            // Convergence: one more clean run moves nothing and stays equal.
            let stable = rig.attempt(&conn).await.expect("stable run");
            assert_eq!(stable.total_rows(), 0, "[{context}] not convergent");
            assert_mirror_equals(&fixture, &context).await;
        }
    }
}

/// Redelivery convergence (US2-AS4): a run that dies AFTER applying changes
/// but BEFORE the ack redelivers the same transactions on the next run —
/// the redelivered update converges and the redelivered delete no-ops.
#[tokio::test(flavor = "multi_thread")]
async fn redelivered_changes_converge() {
    let _guard = FAIL_POINT_LOCK.lock().await;
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let conn = fixture.conn_url();
    let rig = Rig::new();

    rig.attempt(&conn).await.expect("snapshot run");
    fixture
        .seed("UPDATE public.ev SET v = 'v2' WHERE id = 1; DELETE FROM public.ev WHERE id = 2;")
        .await;

    // The armed run applies the update and the delete, then dies at the ack.
    fail::cfg("cdc.ack.advance", "return").expect("configure");
    let armed = rig.attempt(&conn).await;
    fail::remove("cdc.ack.advance");
    assert!(armed.is_err(), "ack point fired");

    // Recovery re-peeks the same feed range: the update re-applies to the
    // same final value, the delete deletes nothing — exactly-once OUTCOMES.
    rig.attempt(&conn).await.expect("recovery");
    assert_mirror_equals(&fixture, "redelivery").await;
    let client = fixture.client().await;
    let one: i64 = client
        .query_one("SELECT count(*) FROM mirror.ev WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    let gone: i64 = client
        .query_one("SELECT count(*) FROM mirror.ev WHERE id = 2", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!((one, gone), (1, 0), "update once, delete gone");
    let stable = rig.attempt(&conn).await.expect("stable");
    assert_eq!(stable.total_rows(), 0);
}

/// A REAL death mid-catch-up: the container dies while the change pass is
/// applying a large multi-transaction backlog. The error is typed, committed
/// work survives, and nothing double-applies (the destination here is
/// DuckDB — killing the source container must not take the destination down
/// with it).
#[tokio::test(flavor = "multi_thread")]
async fn container_kill_mid_catch_up_is_typed_and_preserves_commits() {
    // Arms nothing, but fail points are PROCESS-GLOBAL: without the lock,
    // the sweep's armed points fire inside THIS test's runs.
    let _guard = FAIL_POINT_LOCK.lock().await;
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture
        .seed("CREATE TABLE public.ev (id int8 PRIMARY KEY, v text);")
        .await;
    let conn = fixture.conn_url();

    let dir = tempfile::tempdir().expect("tempdir");
    let dest =
        rdlt_connector_duckdb::dest::DuckDb::open(dir.path().join("out.duckdb")).expect("open db");
    let workdir = dir.path().join("wal");
    std::mem::forget(dir);
    let run = |src_conn: String,
               dest: rdlt_connector_duckdb::dest::DuckDb,
               workdir: std::path::PathBuf| async move {
        let mut config = EngineConfig::new("cdc-kill");
        config = config.with_workdir(workdir);
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let engine = Engine::new(config, Rig::source(&src_conn), dest);
        engine.run().await.map_err(|e| e.to_string())
    };

    // Snapshot first (tiny), then a large backlog across MANY transactions
    // so the catch-up has many commit boundaries to die between. The
    // backlog must dwarf container-teardown time: a 010 make-check run
    // caught the 400k/120-byte version FINISHING (3.5 s) before the kill
    // landed under parallel load — expect_err saw a successful run.
    run(conn.clone(), dest.clone(), workdir.clone())
        .await
        .expect("snapshot run");
    let client = fixture.client().await;
    for chunk in 0..100i64 {
        client
            .execute(
                &format!(
                    "INSERT INTO public.ev \
                     SELECT g, repeat('x', 300) FROM \
                     generate_series({}, {}) g",
                    chunk * 10_000 + 1,
                    (chunk + 1) * 10_000
                ),
                &[],
            )
            .await
            .expect("backlog chunk");
    }

    // Deterministic kill: wait until at least one catch-up commit landed.
    let watched = dest.clone();
    let baseline = watched.count_rows("ev").unwrap_or(0);
    let killer = tokio::spawn(async move {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while watched.count_rows("ev").unwrap_or(0) <= baseline {
            assert!(
                std::time::Instant::now() < deadline,
                "no catch-up commit observed within 120s — cannot kill deterministically"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        drop(fixture); // container stops; sockets die mid-pass
    });
    let outcome = run(conn, dest.clone(), workdir).await;
    killer.await.expect("killer task");

    let err = outcome.expect_err("mid-catch-up container death must fail the run");
    assert!(
        err.contains("postgres source"),
        "typed error names the source: {err}"
    );
    // Committed work survives: a prefix of whole transactions, no dupes.
    let count = dest.count_rows("ev").unwrap_or(0);
    assert!(
        count > baseline,
        "kill protocol guarantees a committed prefix"
    );
    let distinct = dest
        .query_string("SELECT CAST(count(DISTINCT id) AS VARCHAR) FROM ev")
        .expect("distinct");
    assert_eq!(distinct, count.to_string(), "no double-applied rows");
}

/// Review F7: a TRANSIENT mid-snapshot failure is retried by the ENGINE
/// within one run — the retry must get FRESH connections (a cached dead
/// snapshot/control client would fail every attempt after the fault
/// cleared) and converge exactly-once.
#[tokio::test(flavor = "multi_thread")]
async fn transient_mid_snapshot_resumes_within_one_run() {
    let _guard = FAIL_POINT_LOCK.lock().await;
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let rig = Rig::new();

    // Fail the first two snapshot chunk polls, then heal: one run() call
    // recovers by itself.
    fail::cfg("cdc.snapshot.copy", "2*return->off").expect("configure");
    let report = rig
        .attempt(&fixture.conn_url())
        .await
        .expect("run recovers in-run");
    fail::remove("cdc.snapshot.copy");

    assert!(report.retries > 0, "the engine's retry counter surfaces");
    assert_mirror_equals(&fixture, "in-run retry").await;
    let stable = rig.attempt(&fixture.conn_url()).await.expect("stable");
    assert_eq!(stable.total_rows(), 0, "convergent after in-run retries");
}
