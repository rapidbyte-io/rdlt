//! A whole load, through the engine, against the real account.
//!
//! Credential-gated. These are the US1 acceptance scenarios: exact totals, a
//! replay that publishes nothing, a Replace that leaves only the newest rows,
//! and a session that refuses to commit another load's identity.

use rdlt_connector::StreamSpec;
use rdlt_connector_snowflake::dest::{Snowflake, SnowflakeConfig};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory::{MemoryBatch, MemorySource, MemoryStream};
mod common;

use common::{credentials, scratch_schema};
use serde_json::json;

/// A destination pointed at a scratch schema this test owns.
fn config_in(schema: &str) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    let doc = json!({
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
    Some(SnowflakeConfig::from_value(doc).expect("the convention yields a valid config"))
}

fn rows(from: i64, count: i64) -> Vec<serde_json::Value> {
    (from..from + count)
        .map(|id| json!({"id": id, "note": format!("row-{id}"), "ok": id % 2 == 0}))
        .collect()
}

/// Create the scratch schema, run `body`, then drop it whatever happened.
///
/// Cleanup runs on the failure path too: a panicking test that left its schema
/// behind would slowly fill the account with debris nobody can attribute.
async fn in_scratch_schema<F, Fut>(label: &str, body: F)
where
    F: FnOnce(SnowflakeConfig) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema(label);
    let dest = Snowflake::new(admin.clone()).expect("valid config");
    let ctx = rdlt_connector::OpenContext::new(
        rdlt_connector::core::PipelineId::from("setup"),
        rdlt_connector::core::LoadId::from("setup"),
    );
    // `open` is the only public way to reach the account, and it happens to
    // create what a scratch schema needs anyway.
    let _ = rdlt_connector::Destination::open(&dest, ctx).await;
    let config = config_in(&schema).expect("credentials");
    let scratch = Snowflake::new(config.clone()).expect("valid config");
    rdlt_connector::Destination::open(
        &scratch,
        rdlt_connector::OpenContext::new(
            rdlt_connector::core::PipelineId::from("setup"),
            rdlt_connector::core::LoadId::from("setup"),
        ),
    )
    .await
    .expect("the scratch schema is created by open");

    // The panic is CAUGHT rather than allowed to unwind, because unwinding
    // past the cleanup below is exactly the case cleanup exists for: a failing
    // assertion is when a schema gets left behind. It is re-raised afterwards
    // so the test still fails.
    let outcome =
        futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body(config))).await;

    // Drop through a raw statement: there is no destination API for it, and
    // leaving the schema would be debris.
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

#[tokio::test]
async fn an_append_load_lands_exact_totals_and_replays_without_republishing() {
    in_scratch_schema("append", |config| async move {
        let dest = Snowflake::new(config.clone()).expect("valid config");
        let source = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(rows(1, 5)).with_checkpoint(1)],
        )]);
        let report = Engine::new(EngineConfig::new("sf-append"), source, dest.clone())
            .run()
            .await
            .expect("the load settles");
        assert_eq!(report.total_rows(), 5, "exact totals");

        // The same rows again under the same pipeline: the source resumes from
        // the committed cursor and delivers nothing, so the table is unchanged.
        let again = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(rows(1, 5)).with_checkpoint(1)],
        )]);
        let report = Engine::new(EngineConfig::new("sf-append"), again, dest)
            .run()
            .await
            .expect("the replay settles");
        assert_eq!(
            report.total_rows(),
            0,
            "a resumed load delivers nothing new"
        );

        let count = rdlt_connector_snowflake::dest::testhook::connect_and_run(
            &config,
            "SELECT count(*) FROM \"EVENTS\"",
        )
        .await
        .expect("count");
        assert_eq!(count, "5", "no row was published twice");

        // The receipt is what makes a redelivered unit a no-op rather than a
        // second publish, so the property is that no unit ever has two. Both
        // runs commit — the second moved no rows but still had to make the
        // resumed cursor durable — so counting receipts would only pin how
        // many loads ran, which is not the invariant.
        let doubled = rdlt_connector_snowflake::dest::testhook::connect_and_run(
            &config,
            "SELECT count(*) FROM ( \
               SELECT \"LOAD_ID\", \"COMMIT_SEQ\" FROM \"_RDLT_COMMITS\" \
               GROUP BY 1, 2 HAVING count(*) > 1 )",
        )
        .await
        .expect("receipts");
        assert_eq!(doubled, "0", "no commit unit was receipted twice");
    })
    .await;
}

#[tokio::test]
async fn a_replace_load_leaves_only_the_newest_rows() {
    in_scratch_schema("replace", |config| async move {
        let dest = Snowflake::new(config.clone()).expect("valid config");
        for (pipeline, first, count) in [("sf-replace-a", 1, 5), ("sf-replace-b", 100, 3)] {
            let source = MemorySource::new(vec![MemoryStream::new(
                StreamSpec::new("events"),
                vec![MemoryBatch::new(rows(first, count)).with_checkpoint(1)],
            )]);
            let mut engine_config = EngineConfig::new(pipeline);
            engine_config = engine_config.with_write_mode(rdlt_connector::WriteMode::Replace);
            Engine::new(engine_config, source, dest.clone())
                .run()
                .await
                .unwrap_or_else(|e| panic!("{pipeline} must settle: {e:?}"));
        }
        let count = rdlt_connector_snowflake::dest::testhook::connect_and_run(
            &config,
            "SELECT count(*) FROM \"EVENTS\"",
        )
        .await
        .expect("count");
        assert_eq!(count, "3", "the second load replaced the first");
    })
    .await;
}

#[tokio::test]
async fn values_survive_the_round_trip_including_the_awkward_ones() {
    in_scratch_schema("values", |config| async move {
        let dest = Snowflake::new(config.clone()).expect("valid config");
        // Quotes and backslashes are the ones that would change the STATEMENT
        // rather than the row if the escaping were wrong.
        let awkward = vec![
            json!({"id": 1, "note": "o'brien", "ok": true}),
            json!({"id": 2, "note": "back\\slash", "ok": false}),
            json!({"id": 3, "note": "'; DROP TABLE events; --", "ok": true}),
            json!({"id": 4, "note": null, "ok": null}),
            // Multi-byte text, across scripts and outside the BMP. A part is
            // written and read back by two different pieces of software, and a
            // width assumption in either would surface here rather than in a
            // user's data.
            json!({"id": 5, "note": "zażółć gęślą jaźń", "ok": true}),
            json!({"id": 6, "note": "日本語 مرحبا 🚀", "ok": true}),
        ];
        let source = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(awkward).with_checkpoint(1)],
        )]);
        Engine::new(EngineConfig::new("sf-values"), source, dest)
            .run()
            .await
            .expect("the load settles");

        let count = rdlt_connector_snowflake::dest::testhook::connect_and_run(
            &config,
            "SELECT count(*) FROM \"EVENTS\" WHERE \"NOTE\" = 'o''brien'",
        )
        .await
        .expect("count");
        assert_eq!(count, "1", "the quoted value landed as data, not as SQL");

        let nulls = rdlt_connector_snowflake::dest::testhook::connect_and_run(
            &config,
            "SELECT count(*) FROM \"EVENTS\" WHERE \"NOTE\" IS NULL",
        )
        .await
        .expect("count");
        assert_eq!(nulls, "1", "a null stayed null rather than becoming text");

        let multibyte = rdlt_connector_snowflake::dest::testhook::connect_and_run(
            &config,
            "SELECT count(*) FROM \"EVENTS\" \
             WHERE \"NOTE\" IN ('zażółć gęślą jaźń', \
                             '日本語 مرحبا 🚀')",
        )
        .await
        .expect("count");
        assert_eq!(multibyte, "2", "multi-byte text survived byte for byte");
    })
    .await;
}

/// A load with nothing to deliver still commits where it got to.
///
/// The connector returns early for an empty batch rather than writing a part
/// nobody would read. That shortcut sits in the middle of the write path, so
/// it is exactly the kind of thing that can skip the position with the data —
/// after which an empty run makes the NEXT run re-read from an older point.
#[tokio::test]
async fn a_load_delivering_no_rows_still_commits_its_position() {
    in_scratch_schema("emptyload", |config| async move {
        let pipeline = "sf-empty";
        // One real row first, so there is a position worth advancing past.
        let seeded = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(vec![json!({"id": 1, "note": "seed"})]).with_checkpoint(1)],
        )]);
        Engine::new(
            EngineConfig::new(pipeline),
            seeded,
            Snowflake::new(config.clone()).expect("valid config"),
        )
        .run()
        .await
        .expect("the seed load settles");

        // Then a load that delivers an empty batch at a LATER checkpoint. The
        // already-consumed batch stays in the list because resuming means
        // "after the batch that produced this cursor" — a list that omitted it
        // could not locate the cursor at all.
        let empty = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![
                MemoryBatch::new(vec![json!({"id": 1, "note": "seed"})]).with_checkpoint(1),
                MemoryBatch::new(vec![]).with_checkpoint(2),
            ],
        )]);
        Engine::new(
            EngineConfig::new(pipeline),
            empty,
            Snowflake::new(config.clone()).expect("valid config"),
        )
        .run()
        .await
        .expect("the empty load settles");

        let rows = rdlt_connector_snowflake::dest::testhook::connect_and_run(
            &config,
            "SELECT count(*) FROM \"EVENTS\"",
        )
        .await
        .expect("count");
        assert_eq!(rows, "1", "the empty load added nothing");

        // The position it committed is the empty load's, not the seed's.
        let state = rdlt_connector_snowflake::dest::testhook::script_rows(
            &config,
            &[],
            "SELECT \"DOC\" FROM \"_RDLT_STATE\"",
            &["DOC"],
        )
        .await
        .expect("the state document");
        assert!(
            state[0][0].contains('2'),
            "the empty load's position was committed: {}",
            state[0][0]
        );
    })
    .await;
}
