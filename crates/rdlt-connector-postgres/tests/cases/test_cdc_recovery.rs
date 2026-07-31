//! Damage and recovery: WAL retention overrun, recreated slots, and the
//! TRUNCATE/TOAST wedges that recover via a fresh snapshot.

use rdlt_connector_postgres::fixtures::CdcPgFixture;
use rdlt_connector_postgres::source::testhook::cdc_slot;

use crate::cases::cdc_rig::*;

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
