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
