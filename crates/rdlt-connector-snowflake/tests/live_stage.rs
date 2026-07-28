//! The bulk path, end to end: parquet parts in a real bucket, loaded by the
//! real service.
//!
//! Gated on BOTH an account and a bucket, and each independently: a
//! contributor with an account but no bucket skips these and keeps the insert
//! legs. Everything here is written to be true of the path a user runs — the
//! parts go through the engine, not through a helper that stages them by hand.

use rdlt_connector::StreamSpec;
use rdlt_connector_snowflake::dest::{S3Stage, Snowflake, SnowflakeConfig, Stage};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory::{MemoryBatch, MemorySource, MemoryStream};
use rdlt_testkit::snowflake::{credentials, scratch_schema, stage_credentials};
use serde_json::json;

/// A destination in `schema`, staging through the configured bucket.
///
/// `None` when either half of the convention is missing — which is a SKIP, and
/// the hazard worth naming: a leg that can only skip proves nothing.
fn config_in(schema: &str, with_stage: bool) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    let mut doc = json!({
        "account": creds.account,
        "user": creds.user,
        "database": creds.database,
        "schema": schema,
        "warehouse": creds.warehouse,
        "role": creds.role,
        "auth": {"key_pair": {
            "private_key": creds.private_key_path,
            "passphrase": creds.passphrase,
        }},
    });
    if with_stage {
        let stage = stage_credentials()?;
        let mut s3 = S3Stage::new(stage.bucket, stage.access_key, stage.secret_key);
        s3.region = stage.region;
        s3.endpoint = stage.endpoint;
        s3.prefix = stage.prefix;
        doc["stage"] = serde_json::to_value(Stage::s3(s3)).expect("the stage serializes");
    }
    Some(SnowflakeConfig::from_value(doc).expect("the convention yields a valid config"))
}

fn rows(from: i64, count: i64) -> Vec<serde_json::Value> {
    (from..from + count)
        .map(|id| json!({"id": id, "note": format!("row-{id}"), "ok": id % 2 == 0}))
        .collect()
}

/// Create a scratch schema, run `body` against a staging destination, then drop
/// the schema whatever happened.
async fn in_scratch_schema<F, Fut>(label: &str, body: F)
where
    F: FnOnce(SnowflakeConfig) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let (Some(admin), Some(_)) = (config_in("PUBLIC", false), stage_credentials()) else {
        return;
    };
    let schema = scratch_schema(label);
    let Some(config) = config_in(&schema, true) else {
        return;
    };
    let scratch = Snowflake::new(config.clone()).expect("valid config");
    rdlt_connector::Destination::open(
        &scratch,
        rdlt_connector::OpenCtx::new(
            rdlt_connector::core::PipelineId::from("setup"),
            rdlt_connector::core::LoadId::from("setup"),
        ),
    )
    .await
    .expect("the scratch schema is created by open");

    let outcome =
        futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body(config))).await;

    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
            admin.database.to_uppercase(),
            schema
        ),
    )
    .await;

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn count_of(config: &SnowflakeConfig, predicate: &str) -> String {
    rdlt_connector_snowflake::dest::testhook::connect_and_run(
        config,
        &format!("SELECT count(*) FROM \"EVENTS\" {predicate}"),
    )
    .await
    .expect("count")
}

#[tokio::test]
async fn a_staged_load_lands_exact_totals_and_replays_without_republishing() {
    in_scratch_schema("stagedappend", |config| async move {
        let dest = Snowflake::new(config.clone()).expect("valid config");
        // Several batches so the commit's single COPY names more than one
        // part — one part per COPY would hide a files-list bug entirely.
        let source = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![
                MemoryBatch::new(rows(1, 4)),
                MemoryBatch::new(rows(5, 4)),
                MemoryBatch::new(rows(9, 2)).with_checkpoint(1),
            ],
        )]);
        let report = Engine::new(EngineConfig::new("sf-staged"), source, dest.clone())
            .run()
            .await
            .expect("the load settles");
        assert_eq!(report.total_rows(), 10, "exact totals");
        assert_eq!(count_of(&config, "").await, "10", "every part loaded once");

        let again = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(rows(1, 10)).with_checkpoint(1)],
        )]);
        Engine::new(EngineConfig::new("sf-staged"), again, dest)
            .run()
            .await
            .expect("the replay settles");
        assert_eq!(
            count_of(&config, "").await,
            "10",
            "a resumed load publishes nothing"
        );
    })
    .await;
}

#[tokio::test]
async fn a_staged_replace_leaves_only_the_newest_rows() {
    in_scratch_schema("stagedreplace", |config| async move {
        let dest = Snowflake::new(config.clone()).expect("valid config");
        for (pipeline, first, count) in [("sf-st-a", 1, 6), ("sf-st-b", 100, 2)] {
            let source = MemorySource::new(vec![MemoryStream::new(
                StreamSpec::new("events"),
                vec![MemoryBatch::new(rows(first, count)).with_checkpoint(1)],
            )]);
            let mut engine_config = EngineConfig::new(pipeline);
            engine_config.write_mode = rdlt_connector::WriteMode::Replace;
            Engine::new(engine_config, source, dest.clone())
                .run()
                .await
                .unwrap_or_else(|e| panic!("{pipeline} must settle: {e:?}"));
        }
        // The clear and the COPY are in ONE transaction, so a reader never
        // sees the cleared-but-unfilled state; what it can check afterwards is
        // that the clear happened at all.
        assert_eq!(count_of(&config, "").await, "2");
        assert_eq!(count_of(&config, "WHERE \"ID\" < 100").await, "0");
    })
    .await;
}

#[tokio::test]
async fn staged_values_survive_the_round_trip_through_parquet() {
    in_scratch_schema("stagedvalues", |config| async move {
        // The insert path renders these into statement text; this one writes
        // them as parquet and lets the service read them back. Different
        // failure modes entirely — worth proving on both.
        let awkward = vec![
            json!({"id": 1, "note": "o'brien", "ok": true}),
            json!({"id": 2, "note": "back\\slash", "ok": false}),
            json!({"id": 3, "note": "'; DROP TABLE events; --", "ok": true}),
            json!({"id": 4, "note": null, "ok": null}),
            json!({"id": 5, "note": "ünïcodé — 日本語", "ok": true}),
        ];
        let source = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(awkward).with_checkpoint(1)],
        )]);
        Engine::new(EngineConfig::new("sf-staged-values"), source, {
            Snowflake::new(config.clone()).expect("valid config")
        })
        .run()
        .await
        .expect("the load settles");

        assert_eq!(count_of(&config, "").await, "5");
        assert_eq!(
            count_of(&config, "WHERE \"NOTE\" = 'o''brien'").await,
            "1",
            "the value landed as data"
        );
        assert_eq!(
            count_of(&config, "WHERE \"NOTE\" IS NULL").await,
            "1",
            "a null stayed null rather than becoming text"
        );
        assert_eq!(
            count_of(&config, "WHERE \"NOTE\" = 'ünïcodé — 日本語'").await,
            "1",
            "multi-byte text survives the parquet round trip"
        );
    })
    .await;
}

#[tokio::test]
async fn the_bulk_path_and_the_insert_path_agree_on_what_they_load() {
    // The two paths reach the same table by completely different routes —
    // statement text versus a file the service reads. A difference between
    // them is a data bug in whichever is wrong, and nothing else would catch
    // it: each path's own test only checks it against itself.
    let (Some(_), Some(_)) = (credentials(), stage_credentials()) else {
        return;
    };
    let schema = scratch_schema("stagedparity");
    let admin = config_in("PUBLIC", false).expect("credentials");
    let staged = config_in(&schema, true).expect("credentials");
    let inserted = config_in(&schema, false).expect("credentials");

    let load = |config: SnowflakeConfig, pipeline: &'static str, table: &'static str| async move {
        let dest = Snowflake::new(config).expect("valid config");
        rdlt_connector::Destination::open(
            &dest,
            rdlt_connector::OpenCtx::new(
                rdlt_connector::core::PipelineId::from(pipeline),
                rdlt_connector::core::LoadId::from("setup"),
            ),
        )
        .await
        .expect("open");
        let source = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new(table),
            vec![MemoryBatch::new(rows(1, 25)).with_checkpoint(1)],
        )]);
        Engine::new(EngineConfig::new(pipeline), source, dest)
            .run()
            .await
            .expect("the load settles");
    };
    load(staged.clone(), "sf-parity-staged", "staged_events").await;
    load(inserted, "sf-parity-inserted", "inserted_events").await;

    // Canonical-row equality: every row of one has an identical row in the
    // other, and the counts match.
    // Counted first, and asserted below: two EMPTY tables also differ by
    // nothing, so the difference alone would pass on a load that never ran.
    let staged_rows = rdlt_connector_snowflake::dest::testhook::connect_and_run(
        &staged,
        "SELECT count(*) FROM \"STAGED_EVENTS\"",
    )
    .await
    .expect("staged count");
    let inserted_rows = rdlt_connector_snowflake::dest::testhook::connect_and_run(
        &staged,
        "SELECT count(*) FROM \"INSERTED_EVENTS\"",
    )
    .await
    .expect("inserted count");

    let differing = rdlt_connector_snowflake::dest::testhook::connect_and_run(
        &staged,
        "SELECT count(*) FROM ( \
           SELECT \"ID\", \"NOTE\", \"OK\" FROM \"STAGED_EVENTS\" \
           MINUS SELECT \"ID\", \"NOTE\", \"OK\" FROM \"INSERTED_EVENTS\" \
           UNION ALL \
           SELECT \"ID\", \"NOTE\", \"OK\" FROM \"INSERTED_EVENTS\" \
           MINUS SELECT \"ID\", \"NOTE\", \"OK\" FROM \"STAGED_EVENTS\" )",
    )
    .await
    .expect("difference");

    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
            admin.database.to_uppercase(),
            schema
        ),
    )
    .await;

    assert_eq!(staged_rows, "25", "the bulk path loaded every row");
    assert_eq!(inserted_rows, "25", "the insert path loaded every row");
    assert_eq!(
        differing, "0",
        "the two ingestion paths must land identical rows"
    );
}
