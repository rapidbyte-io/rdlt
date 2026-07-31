//! Merge refinements against a live server: ordered survivor selection
//! (dedup_sort), scope-key replacement (merge_scope), the per-table
//! single-unit rule, and the open-time validation matrix.

use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector::{ConnectorSpec, Cursor, ReadRequest, Source, SourceError, StreamSpec};
use rdlt_connector_postgres::dest::{
    DedupSort, DestinationOptions, MergeStrategy, Postgres, SortOrder, TableOptions,
};
use rdlt_engine::{Engine, EngineConfig};

use crate::cases::common;
use rdlt_connector_postgres::fixtures::PgFixture;

/// (id, day, seq, name, deleted) — id is the identity key, day the
/// scope column, seq the dedup-sort column.
type Row = (i64, Option<i64>, Option<i64>, &'static str, Option<bool>);

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("day", DataType::Int64, true),
            Field::new("seq", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("deleted", DataType::Boolean, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.4).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

/// Pushes each batch with its own checkpoint — under the default
/// commit policy every batch is its OWN COMMIT UNIT (the multi-unit
/// cells depend on this).
struct UnitsSource {
    units: Vec<RecordBatch>,
}

#[async_trait]
impl Source for UnitsSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("refinements-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new("events")
                .with_structured()
                .with_primary_key(["id"]),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        for (i, unit) in self.units.iter().enumerate() {
            let _ = req.out.arrow(unit.clone()).await;
            let _ = req.out.checkpoint(Cursor::new(i as u64 + 1)).await;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
struct Opts {
    strategy: Option<MergeStrategy>,
    dedup: Option<(&'static str, SortOrder)>,
    merge_scope: Option<&'static [&'static str]>,
    hard_delete: bool,
    scd2_retire: bool,
}

fn dest(conn: &str, dataset: &str, opts: Opts) -> Postgres {
    Postgres::connect(conn)
        .dataset(dataset)
        .options(DestinationOptions {
            merge_strategy: opts.strategy,
            tables: [(
                "events".to_string(),
                TableOptions {
                    hard_delete: opts.hard_delete.then(|| "deleted".into()),
                    dedup_sort: opts.dedup.map(|(column, order)| DedupSort {
                        column: column.into(),
                        order,
                    }),
                    merge_scope: opts
                        .merge_scope
                        .map(|c| c.iter().map(|s| s.to_string()).collect()),
                    scd2: opts
                        .scd2_retire
                        .then(|| rdlt_connector_postgres::dest::Scd2Options {
                            absent: rdlt_connector_postgres::dest::AbsentPolicy::Retire,
                            ..rdlt_connector_postgres::dest::Scd2Options::default()
                        }),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
        })
        .expect("valid options")
}

async fn run(conn: &str, dataset: &str, opts: Opts, units: Vec<Vec<Row>>) {
    let mut config = EngineConfig::new(format!("mr-{dataset}"));
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let units = units.iter().map(|u| batch(u)).collect();
    Engine::new(config, UnitsSource { units }, dest(conn, dataset, opts))
        .run()
        .await
        .expect("merge run");
}

/// `(id, day, seq, name)` rows of `<dataset>.events`, id-ordered.
async fn rows(conn: &str, dataset: &str) -> Vec<(i64, Option<i64>, Option<i64>, String)> {
    let client = crate::cases::common::connect(conn).await;
    client
        .query(
            &format!("SELECT id, day, seq, name FROM \"{dataset}\".events ORDER BY id, day, seq"),
            &[],
        )
        .await
        .expect("rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get::<_, String>(3)))
        .collect()
}

async fn run_expect_err(conn: &str, dataset: &str, opts: Opts, units: Vec<Vec<Row>>) -> String {
    let mut config = EngineConfig::new(format!("mr-{dataset}"));
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let units = units.iter().map(|u| batch(u)).collect();
    Engine::new(config, UnitsSource { units }, dest(conn, dataset, opts))
        .run()
        .await
        .expect_err("run should fail")
        .to_string()
}

// ---- ordered survivor selection (dedup_sort) ----

#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_orders_survivors_not_arrival() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    // Wrong arrival order: the newest version arrives FIRST.
    let load: Vec<Vec<Row>> = vec![vec![
        (1, None, Some(5), "newest", None),
        (1, None, Some(3), "older", None),
    ]];

    // desc: greatest seq survives, despite arriving first.
    let desc = Opts {
        dedup: Some(("seq", SortOrder::Desc)),
        ..Opts::default()
    };
    run(&conn, "mr_desc", desc, load.clone()).await;
    assert_eq!(
        rows(&conn, "mr_desc").await,
        vec![(1, None, Some(5), "newest".into())]
    );

    // asc: least seq survives.
    let asc = Opts {
        dedup: Some(("seq", SortOrder::Asc)),
        ..Opts::default()
    };
    run(&conn, "mr_asc", asc, load.clone()).await;
    assert_eq!(
        rows(&conn, "mr_asc").await,
        vec![(1, None, Some(3), "older".into())]
    );

    // FR-002: absent the option, arrival-order last-wins is UNCHANGED.
    run(&conn, "mr_absent", Opts::default(), load.clone()).await;
    assert_eq!(
        rows(&conn, "mr_absent").await,
        vec![(1, None, Some(3), "older".into())]
    );

    // The same desc rule under the UPSERT arm — one shared shape.
    let upsert = Opts {
        strategy: Some(MergeStrategy::Upsert),
        dedup: Some(("seq", SortOrder::Desc)),
        ..Opts::default()
    };
    run(&conn, "mr_upsert", upsert, load).await;
    assert_eq!(
        rows(&conn, "mr_upsert").await,
        vec![(1, None, Some(5), "newest".into())]
    );
}

// ---- scope-key replacement (merge_scope) ----

#[tokio::test(flavor = "multi_thread")]
async fn merge_scope_replaces_delivered_scopes_only() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        merge_scope: Some(&["day"]),
        ..Opts::default()
    };
    // Seed two scopes.
    run(
        &conn,
        "mr_scope",
        opts,
        vec![vec![
            (1, Some(1), None, "d1-a", None),
            (2, Some(1), None, "d1-b", None),
            (3, Some(2), None, "d2-a", None),
        ]],
    )
    .await;
    // Re-deliver day 1 WITHOUT id 2, with id 1 updated; day 2 untouched.
    run(
        &conn,
        "mr_scope",
        opts,
        vec![vec![(1, Some(1), None, "d1-a2", None)]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_scope").await,
        vec![
            (1, Some(1), None, "d1-a2".into()),
            (3, Some(2), None, "d2-a".into()),
        ],
        "undelivered row in the delivered scope is GONE; day 2 intact"
    );

    // Review F8: the scope columns get a supporting index automatically
    // (the scope delete must never seq-scan the target).
    assert_eq!(
        common::scalar::<i64>(
            &conn,
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'mr_scope' \
             AND tablename = 'events' AND indexname LIKE 'rdlt_ix%' \
             AND indexdef LIKE '%(day)%'",
        )
        .await,
        1,
        "merge_scope scope index auto-ensured"
    );

    // An unseen scope simply lands; replay is idempotent.
    let unseen: Vec<Vec<Row>> = vec![vec![(9, Some(9), None, "d9", None)]];
    run(&conn, "mr_scope", opts, unseen.clone()).await;
    run(&conn, "mr_scope", opts, unseen).await;
    assert_eq!(
        rows(&conn, "mr_scope").await,
        vec![
            (1, Some(1), None, "d1-a2".into()),
            (3, Some(2), None, "d2-a".into()),
            (9, Some(9), None, "d9".into()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_scope_scope_moves_and_null_scopes() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        merge_scope: Some(&["day"]),
        ..Opts::default()
    };
    run(
        &conn,
        "mr_move",
        opts,
        vec![vec![
            (1, Some(1), None, "in-d1", None),
            (2, None, None, "no-scope", None),
        ]],
    )
    .await;
    // id 1 MOVES from day 1 to day 2 — held once, in its new scope
    // the NULL-scope row is untouched by scope deletion and
    // still merges by identity.
    run(
        &conn,
        "mr_move",
        opts,
        vec![vec![
            (1, Some(2), None, "in-d2", None),
            (2, None, None, "no-scope-v2", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_move").await,
        vec![
            (1, Some(2), None, "in-d2".into()),
            (2, None, None, "no-scope-v2".into()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_scope_requires_a_single_commit_unit() {
    // The NON-OPTIONAL cell (plan rule; the 008 S6/F2 lesson, sharpened
    // by this feature's own crash sweep): "the batch is the complete
    // truth for its scope" only holds when the scope's truth arrives in
    // ONE commit unit — a crash-resumed load is a NEW load delivering a
    // PARTIAL feed, indistinguishable destination-side from a fresh one.
    // Multi-unit scoped loads are therefore a TYPED error, never silent
    // partial replacement.
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        merge_scope: Some(&["day"]),
        ..Opts::default()
    };
    run(
        &conn,
        "mr_units",
        opts,
        vec![vec![(99, Some(1), None, "stale", None)]],
    )
    .await;
    let err = run_expect_err(
        &conn,
        "mr_units",
        opts,
        vec![
            vec![(1, Some(1), None, "u1-a", None)],
            vec![(2, Some(1), None, "u2-a", None)],
        ],
    )
    .await;
    assert!(
        err.contains("SINGLE commit unit") && err.contains("commit thresholds"),
        "{err}"
    );

    // Recovery: the same feed in one unit converges.
    run(
        &conn,
        "mr_units",
        opts,
        vec![vec![
            (1, Some(1), None, "u1-a", None),
            (2, Some(1), None, "u2-a", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_units").await,
        vec![
            (1, Some(1), None, "u1-a".into()),
            (2, Some(1), None, "u2-a".into()),
        ],
        "stale row gone exactly once; the full-feed retry converges"
    );

    // A later unit with NOTHING staged for the scoped table is fine —
    // multi-unit pipelines where the scoped table fits unit 1 work.
    run(
        &conn,
        "mr_units",
        opts,
        vec![
            vec![
                (1, Some(1), None, "v2", None),
                (2, Some(1), None, "u2-a", None),
            ],
            vec![],
        ],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_units").await,
        vec![
            (1, Some(1), None, "v2".into()),
            (2, Some(1), None, "u2-a".into()),
        ]
    );
}

// ---- per-table single-unit rule + composition pins ----

#[tokio::test(flavor = "multi_thread")]
async fn scoped_feed_in_a_later_unit_of_a_multi_unit_load_is_fine() {
    // Review F2: the single-unit rule is PER TABLE — other streams'
    // checkpoints split the LOAD without splitting this table's feed. A
    // leading empty unit (another stream committed first) must not
    // reject the scoped table.
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        merge_scope: Some(&["day"]),
        ..Opts::default()
    };
    run(
        &conn,
        "mr_lead_empty",
        opts,
        vec![vec![(99, Some(1), None, "stale", None)]],
    )
    .await;
    run(
        &conn,
        "mr_lead_empty",
        opts,
        vec![vec![], vec![(1, Some(1), None, "fresh", None)]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_lead_empty").await,
        vec![(1, Some(1), None, "fresh".into())],
        "scope replaced from the table's FIRST STAGED unit, wherever it lands"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn scd2_retire_shares_the_per_table_single_unit_rule() {
    // One rule, both consumers. Retire tolerates units where the table
    // stages nothing (an empty stage must not read as "every key absent"
    // = mass retirement), and rejects a split feed typed.
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        strategy: Some(MergeStrategy::Scd2),
        scd2_retire: true,
        ..Opts::default()
    };
    let full: Vec<Vec<Row>> = vec![vec![(1, None, None, "a", None), (2, None, None, "b", None)]];
    run(&conn, "mr_scd2_units", opts, full.clone()).await;
    // Trailing empty unit: fine — and it retires NOTHING.
    run(&conn, "mr_scd2_units", opts, vec![full[0].clone(), vec![]]).await;
    assert_eq!(
        common::scalar::<i64>(
            &conn,
            "SELECT count(*) FROM mr_scd2_units.events WHERE _rdlt_valid_to IS NULL",
        )
        .await,
        2,
        "empty unit retired nothing"
    );
    // Split feed: typed, names the single-unit rule.
    let err = run_expect_err(
        &conn,
        "mr_scd2_units",
        opts,
        vec![
            vec![(1, None, None, "a2", None)],
            vec![(2, None, None, "b2", None)],
        ],
    )
    .await;
    assert!(err.contains("SINGLE commit unit"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_survivor_drives_scd2_change_detection() {
    // The scd2 arm consumes the SAME deduped shape —
    // the ordered survivor decides the active version.
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        strategy: Some(MergeStrategy::Scd2),
        dedup: Some(("seq", SortOrder::Desc)),
        ..Opts::default()
    };
    // Wrong arrival order: the survivor (seq=5) becomes the active row.
    run(
        &conn,
        "mr_scd2_dedup",
        opts,
        vec![vec![
            (1, None, Some(5), "newest", None),
            (1, None, Some(3), "older", None),
        ]],
    )
    .await;
    assert_eq!(
        common::scalar::<String>(
            &conn,
            "SELECT name FROM mr_scd2_dedup.events WHERE _rdlt_valid_to IS NULL",
        )
        .await,
        "newest",
        "the ordered survivor is the active version"
    );
    // A later load creates history; the stale-arrival version never
    // polluted it.
    run(
        &conn,
        "mr_scd2_dedup",
        opts,
        vec![vec![(1, None, Some(9), "newer-still", None)]],
    )
    .await;
    assert_eq!(
        common::scalar::<i64>(&conn, "SELECT count(*) FROM mr_scd2_dedup.events").await,
        2,
        "exactly two versions ever existed"
    );
}

// ---- open-time validation matrix ----

#[tokio::test(flavor = "multi_thread")]
async fn refinement_options_validate_typed_at_open() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let one_row: Vec<Vec<Row>> = vec![vec![(1, Some(1), Some(1), "x", None)]];

    // Nonexistent columns: table AND column named, before any data moves.
    let err = run_expect_err(
        &conn,
        "mr_bad_dedup",
        Opts {
            dedup: Some(("nope", SortOrder::Desc)),
            ..Opts::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(err.contains("`nope`") && err.contains("`events`"), "{err}");

    let err = run_expect_err(
        &conn,
        "mr_bad_scope",
        Opts {
            merge_scope: Some(&["ghost"]),
            ..Opts::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(err.contains("`ghost`") && err.contains("`events`"), "{err}");

    // The hard_delete flag is neither an ordering column nor a scope.
    let err = run_expect_err(
        &conn,
        "mr_flag_dedup",
        Opts {
            dedup: Some(("deleted", SortOrder::Desc)),
            hard_delete: true,
            ..Opts::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(
        err.contains("hard_delete") && err.contains("`deleted`"),
        "{err}"
    );

    let err = run_expect_err(
        &conn,
        "mr_flag_scope",
        Opts {
            merge_scope: Some(&["deleted"]),
            hard_delete: true,
            ..Opts::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(err.contains("not a scope"), "{err}");

    // Review F4: a merge-key column is constant per identity group — the
    // ordering could never pick a survivor; silent no-op forbidden.
    let err = run_expect_err(
        &conn,
        "mr_key_dedup",
        Opts {
            dedup: Some(("id", SortOrder::Desc)),
            ..Opts::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(err.contains("part of the merge key"), "{err}");

    // Review F5: the options under a non-merge write mode are rejected,
    // never silently inert (the 008 F6 lesson).
    let mut config = EngineConfig::new("mr-inert");
    config = config.with_write_mode(rdlt_connector::WriteMode::Append);
    let units = one_row.iter().map(|u| batch(u)).collect();
    let err = Engine::new(
        config,
        UnitsSource { units },
        dest(
            &conn,
            "mr_inert",
            Opts {
                merge_scope: Some(&["day"]),
                ..Opts::default()
            },
        ),
    )
    .run()
    .await
    .expect_err("inert option must be rejected")
    .to_string();
    assert!(err.contains("requires the merge write mode"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn refinement_options_reject_shredded_streams() {
    use rdlt_testkit::MemorySource;
    use serde_json::json;

    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    for (dataset, table_opts, needle) in [
        (
            "mr_sh_dedup",
            TableOptions {
                dedup_sort: Some(DedupSort {
                    column: "seq".into(),
                    order: SortOrder::Desc,
                }),
                ..TableOptions::default()
            },
            "dedup_sort requires a KEYED structured",
        ),
        (
            "mr_sh_scope",
            TableOptions {
                merge_scope: Some(vec!["day".into()]),
                ..TableOptions::default()
            },
            "merge_scope requires a KEYED structured",
        ),
    ] {
        let dest = Postgres::connect(&conn)
            .dataset(dataset)
            .options(DestinationOptions {
                tables: [("users".to_string(), table_opts)].into_iter().collect(),
                ..DestinationOptions::default()
            })
            .expect("options");
        let mut config = EngineConfig::new(dataset);
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let source = MemorySource::single_stream(
            rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
            vec![json!({"id": 1, "seq": 2, "day": 3})],
        );
        let err = Engine::new(config, source, dest)
            .run()
            .await
            .expect_err("shredded stream must reject the option")
            .to_string();
        assert!(err.contains(needle), "{err}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_scope_composes_with_upsert_hard_delete_and_dedup_sort() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        strategy: Some(MergeStrategy::Upsert),
        dedup: Some(("seq", SortOrder::Desc)),
        merge_scope: Some(&["day"]),
        hard_delete: true,
        ..Opts::default()
    };
    run(
        &conn,
        "mr_compose",
        opts,
        vec![vec![
            (1, Some(1), Some(1), "keep-old", None),
            (2, Some(1), Some(1), "stale", None),
        ]],
    )
    .await;
    // Day 1 re-delivered: id 2 not re-delivered (scope-dies), id 1
    // arrives twice in wrong order (survivor by seq), id 3 arrives
    // flagged (hard-delete wins over insert).
    run(
        &conn,
        "mr_compose",
        opts,
        vec![vec![
            (1, Some(1), Some(9), "newest", None),
            (1, Some(1), Some(5), "older", None),
            (3, Some(1), Some(1), "kill", Some(true)),
        ]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_compose").await,
        vec![(1, Some(1), Some(9), "newest".into())],
        "scope delete + ordered survivor + hard delete compose"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_survivor_drives_hard_delete() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        dedup: Some(("seq", SortOrder::Desc)),
        hard_delete: true,
        ..Opts::default()
    };
    // Seed the key, then a load where the NEWEST version is flagged
    // deleted but an OLDER unflagged version arrives after it.
    run(
        &conn,
        "mr_flag",
        opts,
        vec![vec![(1, None, Some(1), "seed", None)]],
    )
    .await;
    run(
        &conn,
        "mr_flag",
        opts,
        vec![vec![
            (1, None, Some(5), "kill", Some(true)),
            (1, None, Some(3), "stale", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_flag").await,
        vec![],
        "the SURVIVOR's flag decides — the row is gone"
    );

    // Under asc the unflagged older version survives instead.
    let asc = Opts {
        dedup: Some(("seq", SortOrder::Asc)),
        hard_delete: true,
        ..Opts::default()
    };
    run(
        &conn,
        "mr_flag_asc",
        asc,
        vec![vec![(1, None, Some(1), "seed", None)]],
    )
    .await;
    run(
        &conn,
        "mr_flag_asc",
        asc,
        vec![vec![
            (1, None, Some(5), "kill", Some(true)),
            (1, None, Some(3), "stale", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_flag_asc").await,
        vec![(1, None, Some(3), "stale".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_null_and_tie_policy_is_deterministic() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let opts = Opts {
        dedup: Some(("seq", SortOrder::Desc)),
        ..Opts::default()
    };
    // NULL loses to a value in EITHER direction.
    run(
        &conn,
        "mr_null",
        opts,
        vec![vec![
            (1, None, None, "null-seq", None),
            (1, None, Some(3), "valued", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_null").await,
        vec![(1, None, Some(3), "valued".into())]
    );

    // All NULL: deterministic last-wins.
    run(
        &conn,
        "mr_all_null",
        opts,
        vec![vec![
            (1, None, None, "first", None),
            (1, None, None, "last", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&conn, "mr_all_null").await,
        vec![(1, None, None, "last".into())]
    );

    // Tie: arrival breaks it, replay converges to the same survivor
    // — a second identical run moves the state nowhere.
    let tie: Vec<Vec<Row>> = vec![vec![
        (1, None, Some(5), "first", None),
        (1, None, Some(5), "last", None),
    ]];
    run(&conn, "mr_tie", opts, tie.clone()).await;
    assert_eq!(
        rows(&conn, "mr_tie").await,
        vec![(1, None, Some(5), "last".into())]
    );
    run(&conn, "mr_tie", opts, tie).await;
    assert_eq!(
        rows(&conn, "mr_tie").await,
        vec![(1, None, Some(5), "last".into())]
    );
}
