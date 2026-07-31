//! Replica identity and TOAST: substitution under FULL, typed refusal
//! without it, the per-table preflight matrix, and declared-key overrides.

use crate::cases::cdc_rig::*;
use rdlt_connector_postgres::source::PostgresSource;
use rdlt_engine::{Engine, EngineConfig};

// ─────────────── TOAST policy + error matrix + lag ───────────────

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
        rig.fixture.conn
    ))
    .expect("config");
    let specs = source.streams().await.expect("streams");
    assert_eq!(
        specs[0].primary_key.as_deref(),
        Some(&["code".to_string()][..]),
        "the declared business key wins under FULL"
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
        rig.fixture.conn
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
