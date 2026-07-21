//! Feature 009 — CDC conformance. This file starts with the T004 slot
//! lifecycle cells (distinguished errors, idempotent create-if-missing,
//! non-consuming peek + explicit advance); the US1 equality cycle and
//! boundary cells land on top of it.

mod common;

use common::CdcPgFixture;
use rdlt_postgres::source::config::{AckMode, CdcConfig, CdcMode, Wait};
use rdlt_postgres::source::testhook::cdc_slot;

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
    let fixture = CdcPgFixture::start().await;
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
    let fixture = CdcPgFixture::start().await;
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
    let fixture = CdcPgFixture::start().await;
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
    let fixture = CdcPgFixture::start().await;
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
    let fixture = CdcPgFixture::start().await;
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

    let first = cdc_slot::peek(&client, &config, target)
        .await
        .expect("peek");
    assert!(!first.is_empty(), "changes visible");
    let second = cdc_slot::peek(&client, &config, target)
        .await
        .expect("re-peek");
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
    let after = cdc_slot::peek(&client, &config, target)
        .await
        .expect("peek after ack");
    assert!(
        after.iter().all(|c| c.lsn > max_lsn),
        "acknowledged changes are gone from the feed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_consumer_is_typed_naming_the_pid() {
    let fixture = CdcPgFixture::start().await;
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
    let fixture = CdcPgFixture::start().await;
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

use rdlt_engine::{Engine, EngineConfig};
use rdlt_postgres::dest::{MergeStrategy, PgDestOptions, PgTableOptions, Postgres};
use rdlt_postgres::source::PostgresSource;

/// The recommended composition (contract C3): CDC source → postgres dest,
/// `merge{key}` + `merge_strategy: upsert` + `hard_delete: <flag>` into a
/// `mirror` schema of the SAME database (equality checks become SQL).
struct CdcRig {
    fixture: CdcPgFixture,
    workdir: std::path::PathBuf,
    pipeline: String,
}

impl CdcRig {
    async fn start(pipeline: &str) -> Self {
        let fixture = CdcPgFixture::start().await;
        let dir = tempfile::tempdir().expect("workdir");
        let workdir = dir.path().to_path_buf();
        std::mem::forget(dir);
        Self {
            fixture,
            workdir,
            pipeline: pipeline.to_string(),
        }
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
            .options(PgDestOptions {
                merge_strategy: MergeStrategy::Upsert,
                tables: tables
                    .iter()
                    .map(|t| {
                        (
                            t.to_string(),
                            PgTableOptions {
                                hard_delete: Some("_rdlt_deleted".into()),
                                ..PgTableOptions::default()
                            },
                        )
                    })
                    .collect(),
            })
            .expect("valid dest options")
    }

    async fn run(&self, tables: &[&str], key: &str) -> u64 {
        let mut config = EngineConfig::new(self.pipeline.as_str());
        config.workdir = Some(self.workdir.clone());
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec![key.into()],
        };
        let report = Engine::new(config, self.source(tables), self.dest(tables))
            .run()
            .await
            .expect("cdc run");
        report.total_rows()
    }

    async fn run_expect_err(&self, tables: &[&str], key: &str) -> String {
        let mut config = EngineConfig::new(self.pipeline.as_str());
        config.workdir = Some(self.workdir.clone());
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec![key.into()],
        };
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
}

#[tokio::test(flavor = "multi_thread")]
async fn us1_equality_cycle_snapshot_mutate_catch_up() {
    let rig = CdcRig::start("cdc-us1").await;
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
    let rig = CdcRig::start("cdc-overlap").await;
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
    let rig = CdcRig::start("cdc-ack").await;
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
