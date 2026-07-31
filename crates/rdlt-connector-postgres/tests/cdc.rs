//! Feature 009 — CDC conformance. This file starts with the T004 slot
//! lifecycle cells (distinguished errors, idempotent create-if-missing,
//! non-consuming peek + explicit advance); the US1 equality cycle and
//! boundary cells land on top of it.

mod common;

use common::CdcPgFixture;
use futures::TryStreamExt;
use rdlt_connector_postgres::source::config::{AckMode, CdcConfig, CdcMode, Wait};
use rdlt_connector_postgres::source::testhook::cdc_slot;
use rdlt_connector_postgres::source::testhook::cdc_slot::Change;

fn cdc(slot: &str, publication: &str, create_if_missing: bool) -> CdcConfig {
    CdcConfig {
        slot: slot.into(),
        publication: publication.into(),
        create_if_missing,
        mode: CdcMode::Catchup,
        idle_wait: Wait { seconds: 1 },
        flag_column: "_rdlt_deleted".into(),
        ack: AckMode::Auto,
    }
}

const SEED: &str = r#"
CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);
INSERT INTO public.orders VALUES (1, 10), (2, 20);
"#;

#[tokio::test(flavor = "multi_thread")]
async fn create_if_missing_is_idempotent_and_reports_the_consistent_point() {
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let client = fixture.client().await;
    let tables = vec!["orders".to_string()];

    let first = cdc_slot::ensure(&client, &cdc("s1", "p1", true), "public", &tables)
        .await
        .expect("first ensure creates");
    assert!(first.created_slot);
    assert!(first.consistent_point.is_some());

    let second = cdc_slot::ensure(&client, &cdc("s1", "p1", true), "public", &tables)
        .await
        .expect("second ensure is a no-op");
    assert!(!second.created_slot);
    assert!(second.consistent_point.is_none());

    // Both resources exist exactly once; rdlt never dropped or duplicated.
    let slots: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 's1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let pubs: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = 'p1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!((slots, pubs), (1, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_resources_without_create_are_typed_with_the_hint() {
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let client = fixture.client().await;
    let tables = vec!["orders".to_string()];

    // Nothing exists: the publication check fires first.
    let err = cdc_slot::ensure(&client, &cdc("s1", "p1", false), "public", &tables)
        .await
        .expect_err("missing publication");
    let msg = err.to_string();
    assert!(msg.contains("publication `p1`"), "{msg}");
    assert!(msg.contains("create_if_missing"), "{msg}");

    // Publication present, slot missing: the slot error, with the hint.
    client
        .batch_execute("CREATE PUBLICATION p1 FOR TABLE public.orders")
        .await
        .unwrap();
    let err = cdc_slot::ensure(&client, &cdc("s1", "p1", false), "public", &tables)
        .await
        .expect_err("missing slot");
    let msg = err.to_string();
    assert!(msg.contains("slot `s1`"), "{msg}");
    assert!(msg.contains("create_if_missing"), "{msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_gap_names_publication_and_missing_tables() {
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE public.a (id int8 PRIMARY KEY);\
             CREATE TABLE public.b (id int8 PRIMARY KEY);\
             CREATE PUBLICATION partial_pub FOR TABLE public.a;",
        )
        .await;
    let client = fixture.client().await;
    let tables = vec!["a".to_string(), "b".to_string()];

    // A gap is an error even under create_if_missing — the publication is
    // user-owned; rdlt creates, never alters.
    let err = cdc_slot::ensure(&client, &cdc("s1", "partial_pub", true), "public", &tables)
        .await
        .expect_err("publication gap");
    let msg = err.to_string();
    assert!(msg.contains("partial_pub"), "{msg}");
    assert!(msg.contains("`b`"), "{msg}");
    assert!(!msg.contains("`a`,"), "covered table not listed: {msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_plugin_slot_is_typed_naming_both_plugins() {
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let client = fixture.client().await;
    client
        .query_one(
            "SELECT 1 FROM pg_create_logical_replication_slot('their_slot', 'test_decoding')",
            &[],
        )
        .await
        .expect("foreign slot");

    let err = cdc_slot::ensure(
        &client,
        &cdc("their_slot", "p1", true),
        "public",
        &["orders".to_string()],
    )
    .await
    .expect_err("wrong plugin");
    let msg = err.to_string();
    assert!(msg.contains("test_decoding"), "{msg}");
    assert!(msg.contains("pgoutput"), "{msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn peek_is_nonconsuming_and_advance_acknowledges() {
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let client = fixture.client().await;
    let config = cdc("s1", "p1", true);
    cdc_slot::ensure(&client, &config, "public", &["orders".to_string()])
        .await
        .expect("ensure");

    client
        .batch_execute("INSERT INTO public.orders VALUES (3, 30), (4, 40); DELETE FROM public.orders WHERE id = 1;")
        .await
        .unwrap();
    let target = cdc_slot::current_wal_lsn(&client).await.expect("target");

    let first: Vec<Change> = cdc_slot::peek(&client, &config, target)
        .await
        .expect("peek")
        .try_collect()
        .await
        .expect("collect peek");
    assert!(!first.is_empty(), "changes visible");
    let second: Vec<Change> = cdc_slot::peek(&client, &config, target)
        .await
        .expect("re-peek")
        .try_collect()
        .await
        .expect("collect re-peek");
    assert_eq!(
        first.len(),
        second.len(),
        "peek consumed nothing — the per-table pass design depends on this"
    );
    assert!(
        first.iter().zip(&second).all(|(a, b)| a.lsn == b.lsn),
        "identical positions across passes"
    );

    let max_lsn = first.iter().map(|c| c.lsn).max().unwrap();
    cdc_slot::advance(&client, "s1", max_lsn)
        .await
        .expect("advance");
    let confirmed = cdc_slot::confirmed_flush_lsn(&client, "s1")
        .await
        .expect("confirmed");
    assert!(confirmed >= max_lsn, "ack recorded");
    let after: Vec<Change> = cdc_slot::peek(&client, &config, target)
        .await
        .expect("peek after ack")
        .try_collect()
        .await
        .expect("collect peek after ack");
    assert!(
        after.iter().all(|c| c.lsn > max_lsn),
        "acknowledged changes are gone from the feed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_consumer_is_typed_naming_the_pid() {
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let client = fixture.client().await;
    let config = cdc("s1", "p1", true);
    cdc_slot::ensure(&client, &config, "public", &["orders".to_string()])
        .await
        .expect("ensure");

    // A big backlog makes the competing peek hold the slot long enough to
    // observe `active_pid` deterministically (poll below).
    client
        .batch_execute(
            "INSERT INTO public.orders \
             SELECT g, g::int4 FROM generate_series(100, 300000) g;",
        )
        .await
        .unwrap();

    let competing = fixture.client().await;
    let hold = tokio::spawn(async move {
        let _ = competing
            .query(
                "SELECT count(*) FROM pg_logical_slot_peek_binary_changes(\
                 's1', NULL, NULL, 'proto_version', '1', 'publication_names', 'p1')",
                &[],
            )
            .await;
    });
    let mut held = false;
    for _ in 0..200 {
        let active: Option<i32> = client
            .query_one(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = 's1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if active.is_some() {
            held = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(held, "competing consumer never observed holding the slot");

    let err = cdc_slot::ensure(&client, &config, "public", &["orders".to_string()])
        .await
        .expect_err("slot held");
    let msg = err.to_string();
    assert!(msg.contains("slot `s1`"), "{msg}");
    assert!(msg.contains("pid"), "{msg}");
    assert!(msg.contains("one consumer"), "{msg}");
    hold.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn wal_retention_overrun_is_typed_with_fresh_snapshot_recovery() {
    let Some(fixture) = CdcPgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let client = fixture.client().await;
    let config = cdc("s1", "p1", true);
    cdc_slot::ensure(&client, &config, "public", &["orders".to_string()])
        .await
        .expect("ensure");

    // Cap slot WAL retention (sighup-reloadable), then churn well past the
    // cap so the server invalidates the slot.
    // ALTER SYSTEM and CHECKPOINT both refuse to run inside a transaction
    // block (which multi-statement batches imply) — one statement per call.
    client
        .batch_execute("ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
        .await
        .unwrap();
    client
        .batch_execute("SELECT pg_reload_conf()")
        .await
        .unwrap();
    let mut overrun = false;
    for _ in 0..30 {
        client
            .batch_execute(
                "INSERT INTO public.orders \
                 SELECT g, 0 FROM generate_series((SELECT max(id) FROM public.orders) + 1, \
                                                  (SELECT max(id) FROM public.orders) + 50000) g;\
                 SELECT pg_switch_wal();",
            )
            .await
            .unwrap();
        client.batch_execute("CHECKPOINT").await.unwrap();
        let status: Option<String> = client
            .query_one(
                "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 's1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if matches!(status.as_deref(), Some("lost") | Some("unreserved")) {
            overrun = true;
            break;
        }
    }
    assert!(overrun, "WAL churn never invalidated the slot");

    let err = cdc_slot::ensure(&client, &config, "public", &["orders".to_string()])
        .await
        .expect_err("invalidated slot");
    let msg = err.to_string();
    assert!(msg.contains("wal_status"), "{msg}");
    assert!(msg.contains("fresh snapshot"), "{msg}");
    assert!(msg.contains("never drops"), "{msg}");
}

// ───────────────────────── US1: bounded catch-up ─────────────────────────

use rdlt_connector_postgres::dest::{DestinationOptions, MergeStrategy, Postgres, TableOptions};
use rdlt_connector_postgres::source::PostgresSource;
use rdlt_engine::{Engine, EngineConfig};

/// The recommended composition (contract C3): CDC source → postgres dest,
/// `merge{key}` + `merge_strategy: upsert` + `hard_delete: <flag>` into a
/// `mirror` schema of the SAME database (equality checks become SQL).
struct CdcRig {
    fixture: CdcPgFixture,
    workdir: std::path::PathBuf,
    pipeline: String,
}

impl CdcRig {
    /// Skip-not-fail: `None` when no container runtime, so callers return early.
    async fn start(pipeline: &str) -> Option<Self> {
        let fixture = CdcPgFixture::start().await?;
        let dir = tempfile::tempdir().expect("workdir");
        let workdir = dir.path().to_path_buf();
        std::mem::forget(dir);
        Some(Self {
            fixture,
            workdir,
            pipeline: pipeline.to_string(),
        })
    }

    /// A fresh pipeline identity (new workdir + name): the documented
    /// recovery for wedged streams — cursors gone, next run snapshots.
    fn reset_state(&mut self, pipeline: &str) {
        let dir = tempfile::tempdir().expect("workdir");
        self.workdir = dir.path().to_path_buf();
        std::mem::forget(dir);
        self.pipeline = pipeline.to_string();
    }

    fn source(&self, tables: &[&str]) -> PostgresSource {
        let list = tables
            .iter()
            .map(|t| format!("  - name: {t}\n"))
            .collect::<String>();
        PostgresSource::from_yaml(&format!(
            "conn: \"{}\"\ncdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\ntables:\n{list}",
            self.fixture.conn_url()
        ))
        .expect("cdc source config")
    }

    fn dest(&self, tables: &[&str]) -> Postgres {
        Postgres::connect(self.fixture.conn_url())
            .dataset("mirror")
            .options(DestinationOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: tables
                    .iter()
                    .map(|t| {
                        (
                            t.to_string(),
                            TableOptions {
                                hard_delete: Some("_rdlt_deleted".into()),
                                ..TableOptions::default()
                            },
                        )
                    })
                    .collect(),
            })
            .expect("valid dest options")
    }

    async fn run(&self, tables: &[&str], key: &str) -> u64 {
        let mut config = EngineConfig::new(self.pipeline.as_str());
        config = config.with_workdir(self.workdir.clone());
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec![key.into()],
        });
        let report = Engine::new(config, self.source(tables), self.dest(tables))
            .run()
            .await
            .expect("cdc run");
        report.total_rows()
    }

    async fn run_expect_err(&self, tables: &[&str], key: &str) -> String {
        let mut config = EngineConfig::new(self.pipeline.as_str());
        config = config.with_workdir(self.workdir.clone());
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec![key.into()],
        });
        Engine::new(config, self.source(tables), self.dest(tables))
            .run()
            .await
            .expect_err("run should fail")
            .to_string()
    }

    /// Row-for-row equality on the projected columns, both directions.
    async fn assert_mirror_equals(&self, table: &str, cols: &str) {
        let client = self.fixture.client().await;
        for (a, b) in [("public", "mirror"), ("mirror", "public")] {
            let diff: i64 = client
                .query_one(
                    &format!(
                        "SELECT count(*) FROM (SELECT {cols} FROM {a}.\"{table}\" \
                         EXCEPT SELECT {cols} FROM {b}.\"{table}\") d"
                    ),
                    &[],
                )
                .await
                .expect("equality query")
                .get(0);
            assert_eq!(diff, 0, "{a} \\ {b} on {table} should be empty");
        }
    }

    async fn scalar(&self, sql: &str) -> i64 {
        self.fixture
            .client()
            .await
            .query_one(sql, &[])
            .await
            .expect("scalar")
            .get(0)
    }

    async fn scalar_text(&self, sql: &str) -> String {
        self.fixture
            .client()
            .await
            .query_one(sql, &[])
            .await
            .expect("scalar text")
            .get(0)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn us1_equality_cycle_snapshot_mutate_catch_up() {
    let Some(rig) = CdcRig::start("cdc-us1").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4, note text);\
             INSERT INTO public.orders VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');",
        )
        .await;

    // Run 1: snapshot.
    let rows = rig.run(&["orders"], "id").await;
    assert_eq!(rows, 3, "snapshot loads every row");
    rig.assert_mirror_equals("orders", "id, total, note").await;

    // Mutations: inserts, an update, a DELETE, a PK-changing update, a
    // multi-row transaction, and a net no-op transaction.
    let client = rig.fixture.client().await;
    client
        .batch_execute(
            "BEGIN;\
             INSERT INTO public.orders VALUES (4, 40, 'd'), (5, 50, 'e');\
             UPDATE public.orders SET total = 11 WHERE id = 1;\
             DELETE FROM public.orders WHERE id = 2;\
             COMMIT;\
             BEGIN; UPDATE public.orders SET id = 33 WHERE id = 3; COMMIT;\
             BEGIN; INSERT INTO public.orders VALUES (99, 0, 'x'); \
             DELETE FROM public.orders WHERE id = 99; COMMIT;\
             BEGIN; UPDATE public.orders SET total = 12 WHERE id = 1; COMMIT;",
        )
        .await
        .unwrap();

    // Run 2: catch-up. Destination equals source row-for-row; id 2 GONE,
    // id 3 relocated to 33, id 1 shows the LATER of the two updates
    // (commit order), 99 never materializes (net no-op transaction).
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals("orders", "id, total, note").await;
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders WHERE id IN (2, 3, 99)")
            .await,
        0,
        "deleted, relocated, and no-op keys are gone"
    );
    assert_eq!(
        rig.scalar("SELECT total::int8 FROM mirror.orders WHERE id = 1")
            .await,
        12,
        "sequential commits applied in order"
    );

    // Run 3: nothing changed — nothing moves, equality holds.
    let rows = rig.run(&["orders"], "id").await;
    assert_eq!(rows, 0, "a quiet feed moves nothing");
    rig.assert_mirror_equals("orders", "id, total, note").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_overlap_row_appears_exactly_once_with_final_state() {
    // The recorded refinement's NON-OPTIONAL proof (P2): a row mutated
    // between slot creation and snapshot end lands in BOTH the snapshot and
    // the feed — it must appear exactly once, with its final state.
    let Some(rig) = CdcRig::start("cdc-overlap").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10);\
             CREATE PUBLICATION p1 FOR TABLE public.orders;",
        )
        .await;
    let client = rig.fixture.client().await;
    // Slot FIRST (as run 1 would), then mutate INSIDE the window before the
    // snapshot begins — run 1 sees the slot pre-existing, snapshots the
    // final state, and initializes its cursor at the slot's confirmed
    // position, BEHIND the mutation.
    client
        .query_one(
            "SELECT 1 FROM pg_create_logical_replication_slot('s1', 'pgoutput')",
            &[],
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "UPDATE public.orders SET total = 77 WHERE id = 1;\
             INSERT INTO public.orders VALUES (2, 20);",
        )
        .await
        .unwrap();

    // Run 1 (snapshot): final states, once each.
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals("orders", "id, total").await;
    assert_eq!(rig.scalar("SELECT count(*) FROM mirror.orders").await, 2);

    // Run 2 replays the window through the feed (the overlap) — upsert
    // convergence: still exactly once, still the final state.
    rig.run(&["orders"], "id").await;
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders WHERE id = 1")
            .await,
        1,
        "the overlap row appears exactly once"
    );
    assert_eq!(
        rig.scalar("SELECT total::int8 FROM mirror.orders WHERE id = 1")
            .await,
        77
    );
    rig.assert_mirror_equals("orders", "id, total").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_never_exceeds_the_least_committed_cursor() {
    // The T007 weld (P6): the slot's confirmed position may only reflect
    // positions the DESTINATION durably committed — across full runs (the
    // ack trails one run behind) and across a partial run (no ack at all).
    let Some(rig) = CdcRig::start("cdc-ack").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.a (id int8 PRIMARY KEY, v int4);\
             CREATE TABLE public.b (id int8 PRIMARY KEY, v int4);\
             INSERT INTO public.a VALUES (1, 1); INSERT INTO public.b VALUES (1, 1);",
        )
        .await;
    let client = rig.fixture.client().await;
    let confirmed = || async {
        rig.fixture
            .client()
            .await
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                 WHERE slot_name = 's1'",
                &[],
            )
            .await
            .unwrap()
            .get::<_, String>(0)
    };

    // Run 1 (snapshot): both streams checkpoint their start point; the ack
    // lands at min(start points).
    rig.run(&["a", "b"], "id").await;
    let after_snapshot = confirmed().await;

    client
        .batch_execute("UPDATE public.a SET v = 2; UPDATE public.b SET v = 2;")
        .await
        .unwrap();

    // Run 2 applies the changes, but its own checkpoints are not yet
    // KNOWN-committed at ack time — the ack may only use run-1 floors: the
    // confirmed position must not pass the update transactions.
    rig.run(&["a", "b"], "id").await;
    assert_eq!(
        rig.scalar("SELECT v::int8 FROM mirror.a WHERE id = 1")
            .await,
        2,
        "changes applied"
    );
    let after_catch_up = confirmed().await;
    let changes_still_peekable: i64 = rig
        .scalar(
            "SELECT count(*) FROM pg_logical_slot_peek_binary_changes(\
             's1', NULL, NULL, 'proto_version', '1', 'publication_names', 'p1')",
        )
        .await;
    assert!(
        changes_still_peekable > 0,
        "run 2's changes stay in the feed until a later run proves their \
         commit (confirmed {after_snapshot} -> {after_catch_up})"
    );

    // Partial run: break table b's publication membership — the run fails
    // before completing every stream, so it must ack NOTHING.
    client
        .batch_execute("ALTER PUBLICATION p1 DROP TABLE public.b; UPDATE public.a SET v = 3;")
        .await
        .unwrap();
    let err = rig.run_expect_err(&["a", "b"], "id").await;
    assert!(err.contains("p1"), "{err}");
    assert_eq!(
        confirmed().await,
        after_catch_up,
        "a partial run acks nothing"
    );

    // Restore membership: the next full run converges and (one run later)
    // the ack finally passes the update transactions.
    client
        .batch_execute("ALTER PUBLICATION p1 ADD TABLE public.b")
        .await
        .unwrap();
    rig.run(&["a", "b"], "id").await;
    assert_eq!(
        rig.scalar("SELECT v::int8 FROM mirror.a WHERE id = 1")
            .await,
        3
    );
    rig.run(&["a", "b"], "id").await;
    let final_confirmed = confirmed().await;
    assert_ne!(
        final_confirmed, after_catch_up,
        "acks advance once commits are proven"
    );
}

// ───────────────────────── US3: continuous tail ─────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tail_applies_bursts_cancels_cleanly_and_resumes() {
    let Some(rig) = CdcRig::start("cdc-tail").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10);",
        )
        .await;

    // A tail-mode source (idle_wait 1s), run in the background.
    let source = PostgresSource::from_yaml(&format!(
        "conn: \"{}\"\ncdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
         \x20 mode: tail\n  idle_wait: \"1s\"\ntables:\n  - name: orders\n",
        rig.fixture.conn_url()
    ))
    .expect("tail config");
    let mut config = EngineConfig::new("cdc-tail");
    config = config.with_workdir(rig.workdir.clone());
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let engine = Engine::new(config, source, rig.dest(&["orders"]));
    let cancel = engine.cancellation_token();
    let tail = tokio::spawn(engine.run());

    let wait_for = |sql: String, expect: i64| {
        let rig = &rig;
        async move {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                let client = rig.fixture.client().await;
                if let Ok(row) = client.query_one(&sql, &[]).await
                    && row.get::<_, i64>(0) == expect
                {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for `{sql}` == {expect}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    };

    // Snapshot lands, then a first burst applies WITHOUT restart.
    wait_for("SELECT count(*) FROM mirror.orders".into(), 1).await;
    rig.fixture
        .seed("INSERT INTO public.orders VALUES (2, 20), (3, 30); UPDATE public.orders SET total = 11 WHERE id = 1;")
        .await;
    wait_for("SELECT count(*) FROM mirror.orders".into(), 3).await;
    wait_for(
        "SELECT total::int8 FROM mirror.orders WHERE id = 1".into(),
        11,
    )
    .await;

    // Quiet idle (≥ two idle_waits), then a SECOND burst still applies —
    // the loop wakes from idle rather than busy-spinning or wedging.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    rig.fixture
        .seed("DELETE FROM public.orders WHERE id = 2;")
        .await;
    wait_for("SELECT count(*) FROM mirror.orders".into(), 2).await;

    // Clean cancellation at a commit boundary.
    cancel.cancel();
    let outcome = tail.await.expect("tail task");
    let err = outcome.expect_err("cancelled run reports cancellation");
    assert!(err.to_string().to_lowercase().contains("cancel"), "{err}");
    rig.assert_mirror_equals("orders", "id, total").await;

    // A subsequent run (catch-up mode) resumes exactly: only the new delta
    // moves, nothing is lost or duplicated.
    rig.fixture
        .seed("INSERT INTO public.orders VALUES (4, 40);")
        .await;
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals("orders", "id, total").await;
    let stable = rig.run(&["orders"], "id").await;
    assert_eq!(stable, 0, "quiet catch-up after the tail moves nothing");
}

// ─────────────────── US4: TOAST policy + error matrix + lag ───────────────────

#[tokio::test(flavor = "multi_thread")]
async fn toast_full_identity_substitutes_from_the_old_image() {
    // O3, retain semantics: an unchanged out-of-line value rides through an
    // unrelated update because REPLICA IDENTITY FULL carries the old image.
    let Some(rig) = CdcRig::start("cdc-toast-full").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.docs (id int8 PRIMARY KEY, blob text, counter int4);\
             ALTER TABLE public.docs REPLICA IDENTITY FULL;\
             INSERT INTO public.docs \
             SELECT 1, (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 4000) g), 1;",
        )
        .await;

    rig.run(&["docs"], "id").await;
    rig.assert_mirror_equals("docs", "id, blob, counter").await;

    // Unrelated update: blob untouched (arrives as an unchanged-TOAST marker).
    rig.fixture
        .seed("UPDATE public.docs SET counter = 2 WHERE id = 1;")
        .await;
    rig.run(&["docs"], "id").await;
    assert_eq!(
        rig.scalar("SELECT counter::int8 FROM mirror.docs WHERE id = 1")
            .await,
        2
    );
    assert_eq!(
        rig.scalar(
            "SELECT count(*) FROM mirror.docs m JOIN public.docs p USING (id) \
             WHERE m.blob = p.blob AND length(m.blob) > 100000"
        )
        .await,
        1,
        "the TOAST value survived the unrelated update intact"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn toast_without_full_identity_fails_typed_never_nulls() {
    // O3, the other half: same shape under DEFAULT identity — no old image
    // to substitute from; typed error naming table + column + the ALTER.
    let Some(rig) = CdcRig::start("cdc-toast-default").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.docs (id int8 PRIMARY KEY, blob text, counter int4);\
             INSERT INTO public.docs \
             SELECT 1, (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 4000) g), 1;",
        )
        .await;
    rig.run(&["docs"], "id").await;
    rig.fixture
        .seed("UPDATE public.docs SET counter = 2 WHERE id = 1;")
        .await;
    let err = rig.run_expect_err(&["docs"], "id").await;
    assert!(err.contains("`blob`"), "{err}");
    assert!(err.contains("REPLICA IDENTITY FULL"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_preflight_matrix_is_typed_per_table() {
    let Some(rig) = CdcRig::start("cdc-identity").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.nopk (v int4);\
             CREATE TABLE public.nothing (id int8 PRIMARY KEY, v int4);\
             ALTER TABLE public.nothing REPLICA IDENTITY NOTHING;\
             CREATE TABLE public.collides (id int8 PRIMARY KEY, _rdlt_deleted bool);",
        )
        .await;

    // PK-less default identity: named table + the fix (O1).
    let err = rig.run_expect_err(&["nopk"], "v").await;
    assert!(err.contains("`nopk`"), "{err}");
    assert!(err.contains("REPLICA IDENTITY"), "{err}");

    // REPLICA IDENTITY NOTHING: unusable even with a PK (O1).
    let err = rig.run_expect_err(&["nothing"], "id").await;
    assert!(err.contains("`nothing`"), "{err}");
    assert!(err.contains("replica identity"), "{err}");

    // Flag-column collision: named table + column (C2).
    let err = rig.run_expect_err(&["collides"], "id").await;
    assert!(err.contains("`collides`"), "{err}");
    assert!(err.contains("_rdlt_deleted"), "{err}");

    // cdc + cursor exclusivity is a CONFIG-parse error (C1) — no server
    // round-trip, named table.
    let err = PostgresSource::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
         tables:\n  - name: t\n    cursor:\n      column: id\n",
    )
    .expect_err("exclusivity")
    .to_string();
    assert!(
        err.contains("`t`") && err.contains("mutually exclusive"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_dropped_mid_stream_never_misapplies() {
    // O4: the identity weakens AFTER the pipeline is established — the next
    // run refuses at preflight, before any change could be mis-applied.
    let Some(rig) = CdcRig::start("cdc-identity-drop").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.ev (id int8, v int4, CONSTRAINT ev_pk PRIMARY KEY (id));\
             INSERT INTO public.ev VALUES (1, 1);",
        )
        .await;
    rig.run(&["ev"], "id").await;

    rig.fixture
        .seed("ALTER TABLE public.ev DROP CONSTRAINT ev_pk; INSERT INTO public.ev VALUES (2, 2);")
        .await;
    let err = rig.run_expect_err(&["ev"], "id").await;
    assert!(err.contains("`ev`"), "{err}");
    assert!(err.contains("REPLICA IDENTITY"), "{err}");
    // Nothing moved: the mirror still holds exactly the pre-drop state.
    assert_eq!(rig.scalar("SELECT count(*) FROM mirror.ev").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn replication_lag_lands_on_the_dedicated_target() {
    use std::sync::{Mutex, OnceLock};

    /// Minimal collector for `rdlt::cdc` lag events.
    struct LagCollector;
    static EVENTS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

    impl tracing::Subscriber for LagCollector {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "rdlt::cdc"
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(Option<u64>);
            impl tracing::field::Visit for Fields {
                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    if field.name() == "lag_bytes" {
                        self.0 = Some(value);
                    }
                }
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            }
            let mut fields = Fields(None);
            event.record(&mut fields);
            if let Some(bytes) = fields.0 {
                EVENTS
                    .get_or_init(Mutex::default)
                    .lock()
                    .expect("collector lock")
                    .push(format!("lag_bytes={bytes}"));
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    let _ = tracing::subscriber::set_global_default(LagCollector);

    let Some(rig) = CdcRig::start("cdc-lag").await else {
        return;
    };
    rig.fixture
        .seed("CREATE TABLE public.ev (id int8 PRIMARY KEY, v int4); INSERT INTO public.ev VALUES (1, 1);")
        .await;
    rig.run(&["ev"], "id").await;
    rig.fixture.seed("UPDATE public.ev SET v = 2;").await;
    rig.run(&["ev"], "id").await;

    let events = EVENTS.get_or_init(Mutex::default).lock().expect("lock");
    assert!(
        events.len() >= 2,
        "one lag event per completed run (SC-006): {events:?}"
    );
}

// ──────────────── review round: regression cells for the fixes ────────────────

#[tokio::test(flavor = "multi_thread")]
async fn recreated_slot_with_resuming_cursor_is_typed_never_a_gap() {
    // Review F1: a slot recreated THIS run cannot cover a resuming stream's
    // history — that must be a typed error, never a silent WAL gap.
    let Some(rig) = CdcRig::start("cdc-slot-gap").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10);",
        )
        .await;
    rig.run(&["orders"], "id").await;
    rig.fixture
        .seed("INSERT INTO public.orders VALUES (2, 20);")
        .await;
    rig.run(&["orders"], "id").await; // cursor now exists

    // Simulate the retention-overrun recovery a user would perform.
    let client = rig.fixture.client().await;
    client
        .query_one("SELECT pg_drop_replication_slot('s1')", &[])
        .await
        .expect("drop slot");
    rig.fixture
        .seed("INSERT INTO public.orders VALUES (3, 30);")
        .await;

    let err = rig.run_expect_err(&["orders"], "id").await;
    assert!(err.contains("created THIS run"), "{err}");
    assert!(err.contains("reset the pipeline state"), "{err}");
    // Nothing skipped silently: row 3 must NOT be missing while the run
    // claims success — the run failed instead.
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders WHERE id = 3")
            .await,
        0
    );

    // The documented recovery converges: fresh state → fresh snapshot.
    let mut rig = rig;
    rig.reset_state("cdc-slot-gap-2");
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals("orders", "id, total").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_identity_index_is_typed_not_an_empty_key() {
    // Review F2: relreplident stays 'i' after the identity index is
    // dropped; the empty column set must be a typed error, never an empty
    // merge key.
    let Some(rig) = CdcRig::start("cdc-ident-index").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.ev (id int8 NOT NULL, v int4);\
             CREATE UNIQUE INDEX ev_ident ON public.ev (id);\
             ALTER TABLE public.ev ALTER COLUMN id SET NOT NULL;\
             ALTER TABLE public.ev REPLICA IDENTITY USING INDEX ev_ident;\
             DROP INDEX public.ev_ident;\
             INSERT INTO public.ev VALUES (1, 1);",
        )
        .await;
    let err = rig.run_expect_err(&["ev"], "id").await;
    assert!(err.contains("`ev`"), "{err}");
    assert!(err.contains("index"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn truncate_wedge_recovers_via_fresh_snapshot() {
    // Review F3: the TRUNCATE fatal must respect the already-applied filter
    // and the fresh-snapshot recovery must start PAST the truncation —
    // otherwise the error's own remedy can never clear it.
    let Some(mut rig) = CdcRig::start("cdc-truncate").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10), (2, 20);",
        )
        .await;
    rig.run(&["orders"], "id").await;

    rig.fixture
        .seed("TRUNCATE public.orders; INSERT INTO public.orders VALUES (5, 50);")
        .await;
    let err = rig.run_expect_err(&["orders"], "id").await;
    assert!(err.contains("TRUNCATE"), "{err}");
    assert!(err.contains("fresh snapshot"), "{err}");
    assert!(err.contains("re-initialize the destination"), "{err}");

    // The prescribed recovery: reset pipeline state AND the destination
    // table (a merge snapshot cannot remove truncated rows), then snapshot.
    // The replayed WAL still contains the TRUNCATE record; it must now be
    // subsumed, not fatal.
    rig.fixture.seed("DROP TABLE mirror.orders;").await;
    rig.reset_state("cdc-truncate-2");
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals("orders", "id, total").await;
    rig.fixture
        .seed("INSERT INTO public.orders VALUES (6, 60); DELETE FROM public.orders WHERE id = 5;")
        .await;
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals("orders", "id, total").await;
    assert_eq!(rig.scalar("SELECT count(*) FROM mirror.orders").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn toast_wedge_recovers_after_alter_and_reset() {
    // Review F4: the unchanged-TOAST fatal's advised fix (ALTER … FULL)
    // plus a state reset must actually recover the stream — the old WAL
    // record must not replay-fatal forever.
    let Some(mut rig) = CdcRig::start("cdc-toast-wedge").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.docs (id int8 PRIMARY KEY, blob text, counter int4);\
             INSERT INTO public.docs \
             SELECT 1, (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 4000) g), 1;",
        )
        .await;
    rig.run(&["docs"], "id").await;
    rig.fixture
        .seed("UPDATE public.docs SET counter = 2 WHERE id = 1;")
        .await;
    let err = rig.run_expect_err(&["docs"], "id").await;
    assert!(err.contains("REPLICA IDENTITY FULL"), "{err}");

    // Follow the error's advice, then reset state: snapshot restarts past
    // the unappliable record and later TOAST updates substitute.
    rig.fixture
        .seed("ALTER TABLE public.docs REPLICA IDENTITY FULL;")
        .await;
    rig.reset_state("cdc-toast-wedge-2");
    rig.run(&["docs"], "id").await;
    rig.assert_mirror_equals("docs", "id, blob, counter").await;
    rig.fixture
        .seed("UPDATE public.docs SET counter = 3 WHERE id = 1;")
        .await;
    rig.run(&["docs"], "id").await;
    assert_eq!(
        rig.scalar("SELECT counter::int8 FROM mirror.docs WHERE id = 1")
            .await,
        3
    );
    rig.assert_mirror_equals("docs", "id, blob, counter").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn session_gucs_do_not_break_change_decoding() {
    // Review F6: the peek session pins DateStyle/bytea_output — a database
    // with legacy settings must decode identically.
    let Some(rig) = CdcRig::start("cdc-gucs").await else {
        return;
    };
    let client = rig.fixture.client().await;
    client
        .batch_execute("ALTER DATABASE postgres SET datestyle = 'SQL, DMY'")
        .await
        .unwrap();
    client
        .batch_execute("ALTER DATABASE postgres SET bytea_output = 'escape'")
        .await
        .unwrap();
    rig.fixture
        .seed(
            "CREATE TABLE public.ev (id int8 PRIMARY KEY, at timestamptz, d date, raw bytea);\
             INSERT INTO public.ev VALUES \
             (1, '2026-07-21T10:11:12.123456Z', '2026-07-21', '\\x0aff10');",
        )
        .await;
    rig.run(&["ev"], "id").await;
    rig.fixture
        .seed(
            "UPDATE public.ev SET at = '2026-07-22T01:02:03Z', d = '2026-07-22', \
             raw = '\\xdeadbeef' WHERE id = 1;\
             INSERT INTO public.ev VALUES (2, '2025-01-31T23:59:59Z', '2025-01-31', '\\x00');",
        )
        .await;
    rig.run(&["ev"], "id").await;
    rig.assert_mirror_equals("ev", "id, at, d, raw").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_primary_key_override_keys_the_stream_under_full() {
    // Review F10: under REPLICA IDENTITY FULL a declared primary_key
    // override must win over the catalog PK (any key has values in the
    // full old image) — not be silently ignored.
    use rdlt_connector::Source;
    let Some(rig) = CdcRig::start("cdc-key-override").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, code text NOT NULL);\
             ALTER TABLE public.orders REPLICA IDENTITY FULL;\
             INSERT INTO public.orders VALUES (1, 'a');",
        )
        .await;
    let source = PostgresSource::from_yaml(&format!(
        "conn: \"{}\"\ncdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
         tables:\n  - name: orders\n    primary_key: [code]\n",
        rig.fixture.conn_url()
    ))
    .expect("config");
    let specs = source.streams().await.expect("streams");
    assert_eq!(
        specs[0].primary_key.as_deref(),
        Some(&["code".to_string()][..]),
        "the declared business key wins under FULL"
    );
}

// ---- Feature 011 (contract PM1/PM2): parameter-matrix gap cells ----

#[tokio::test(flavor = "multi_thread")]
async fn ack_off_never_advances_the_slot() {
    // `ack: off` — data flows, but the slot's confirmed position never
    // moves (debugging / fan-in staging; WAL retention documented).
    let Some(rig) = CdcRig::start("cdc-ack-off").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10);",
        )
        .await;
    let source = |conn: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{conn}\"\ncdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
             \x20 ack: off\ntables:\n  - name: orders\n"
        ))
        .expect("config")
    };
    let run = || async {
        let mut config = EngineConfig::new("cdc-ack-off");
        config = config.with_workdir(rig.workdir.clone());
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        Engine::new(
            config,
            source(&rig.fixture.conn_url()),
            rig.dest(&["orders"]),
        )
        .run()
        .await
        .expect("run")
    };
    run().await;
    let confirmed_after_snapshot = rig
        .scalar_text(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 's1'",
        )
        .await;
    rig.fixture
        .seed("INSERT INTO public.orders VALUES (2, 20);")
        .await;
    run().await;
    run().await;
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders").await,
        2,
        "data flows normally under ack: off"
    );
    assert_eq!(
        rig.scalar_text(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 's1'",
        )
        .await,
        confirmed_after_snapshot,
        "the slot's acknowledged position never advances under ack: off"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn custom_flag_column_flows_end_to_end() {
    // `flag_column` — a custom name rides the whole composition: the CDC
    // stream emits it, the destination hard-deletes by it.
    let Some(rig) = CdcRig::start("cdc-flag-name").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10), (2, 20);",
        )
        .await;
    let source = |conn: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{conn}\"\ncdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
             \x20 flag_column: gone\ntables:\n  - name: orders\n"
        ))
        .expect("config")
    };
    let dest = || {
        Postgres::connect(rig.fixture.conn_url())
            .dataset("mirror")
            .options(DestinationOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: [(
                    "orders".to_string(),
                    TableOptions {
                        hard_delete: Some("gone".into()),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("options")
    };
    let run = || async {
        let mut config = EngineConfig::new("cdc-flag-name");
        config = config.with_workdir(rig.workdir.clone());
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        Engine::new(config, source(&rig.fixture.conn_url()), dest())
            .run()
            .await
            .expect("run")
    };
    run().await;
    rig.fixture
        .seed("DELETE FROM public.orders WHERE id = 2;")
        .await;
    run().await;
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders").await,
        1,
        "the delete hard-applied via the CUSTOM flag column"
    );
    assert_eq!(
        rig.scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'mirror' AND table_name = 'orders' AND column_name = 'gone'"
        )
        .await,
        1,
        "the custom flag column exists at the destination"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_key_mismatch_under_default_identity_is_typed() {
    // `primary_key` override × CDC: under DEFAULT replica identity the
    // delete records only carry the identity columns — a mismatching
    // override is a typed error, never silent mis-keying.
    let Some(rig) = CdcRig::start("cdc-key-mismatch").await else {
        return;
    };
    rig.fixture
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, code text NOT NULL);\
             INSERT INTO public.orders VALUES (1, 'a');",
        )
        .await;
    let source = PostgresSource::from_yaml(&format!(
        "conn: \"{}\"\ncdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
         tables:\n  - name: orders\n    primary_key: [code]\n",
        rig.fixture.conn_url()
    ))
    .expect("config");
    let mut config = EngineConfig::new("cdc-key-mismatch");
    config = config.with_workdir(rig.workdir.clone());
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["code".into()],
    });
    let err = Engine::new(config, source, rig.dest(&["orders"]))
        .run()
        .await
        .expect_err("mismatching override must fail typed")
        .to_string();
    assert!(err.contains("differs from the replica identity"), "{err}");
    assert!(err.contains("`orders`"), "{err}");
}
