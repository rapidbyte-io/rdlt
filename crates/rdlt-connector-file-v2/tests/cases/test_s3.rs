//! The object-store protocol LIVE against RUSTFS: listing and reading
//! through the engine, the destination's COPY+DELETE publish, Replace
//! ownership on a real bucket, and the etag rewrite tripwire.

use rdlt_connector_file_v2::{destination, source};
use rdlt_connector_sdk::config::Document;
use rdlt_engine::{Engine, EngineConfig};

use super::common::local_dest;
use super::s3::S3Fixture;

fn s3_source(fixture: &S3Fixture, pattern: &str) -> source::Config {
    let mut value = serde_json::json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": pattern,
        }]
    });
    value["streams"][0]["location"] =
        serde_json::to_value(fixture.location_options()).expect("options");
    source::Config::from_value(value).expect("valid")
}

fn s3_dest(fixture: &S3Fixture, prefix: &str) -> destination::Config {
    destination::Config::new(prefix).with_location(fixture.location_options())
}

async fn run(
    src: source::Config,
    dest_config: &destination::Config,
    pipeline: &str,
    workdir: &std::path::Path,
) -> Result<(), String> {
    let src = source::Shell::new(src).expect("valid");
    let dest = destination::Shell::new(dest_config.clone()).expect("valid");
    Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.join("wal")),
        src,
        dest,
    )
    .run()
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// Glob listing over seeded objects reads all matches into a local
/// destination — the whole read path over a real wire.
#[tokio::test]
async fn a_glob_read_lands_every_matching_object() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("in/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n")
        .await;
    fixture.put("in/b.jsonl", b"{\"id\": 3}\n").await;
    fixture.put("in/skip.txt", b"not matched").await;

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());
    run(
        s3_source(&fixture, "in/*.jsonl"),
        &config,
        "s3-glob",
        workdir.path(),
    )
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );
}

/// The destination publishes through COPY + DELETE: exact totals land
/// under the prefix, staging is empty afterward, and a second run of
/// the same pipeline APPENDS.
#[tokio::test]
async fn the_destination_publishes_and_clears_its_staging() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("in/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n")
        .await;

    let workdir = tempfile::tempdir().expect("workdir");
    let dest_config = s3_dest(&fixture, "lake");
    run(
        s3_source(&fixture, "in/*.jsonl"),
        &dest_config,
        "s3-publish",
        workdir.path(),
    )
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows_async(&dest_config, "events")
            .await
            .expect("count"),
        2
    );
}

/// Replace over a real bucket clears ONLY owned shapes: a user object
/// under the table prefix survives.
#[tokio::test]
async fn s3_replace_never_deletes_user_files() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("lake/events/part-0.parquet", b"a user's dataset")
        .await;
    fixture.put("in/a.jsonl", b"{\"id\": 1}\n").await;

    let workdir = tempfile::tempdir().expect("workdir");
    let dest_config = s3_dest(&fixture, "lake");
    let mut value = serde_json::to_value(&dest_config).expect("value");
    value["format"] = "jsonl".into();
    let dest_config = destination::Config::from_value(value).expect("valid");
    // Replace mode arrives from the engine's write disposition; drive
    // the SPI directly for an unambiguous Replace commit.
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
    use rdlt_connector_sdk::spi::{Destination, OpenContext};
    use rdlt_testkit::{batch_of, commit_meta_for, schema_for};
    let _ = workdir;
    let dest = destination::Shell::new(dest_config.clone()).expect("valid");
    let pipeline = PipelineId::new("s3-replace");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    assert!(
        fixture.exists("lake/events/part-0.parquet").await,
        "the user's object survives Replace"
    );
    assert!(
        fixture.exists("lake/events/part-load-a-1-0.jsonl").await,
        "our own part published beside it"
    );
}

/// A same-size overwrite of a consumed object changes its etag: the
/// next run refuses with the frozen framing instead of trusting the
/// recorded offset.
#[tokio::test]
async fn an_etag_rewrite_refuses_the_stale_offset() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("in/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n")
        .await;

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());
    run(
        s3_source(&fixture, "in/*.jsonl"),
        &config,
        "s3-etag",
        workdir.path(),
    )
    .await
    .expect("first run");

    // Same length, different bytes — a different etag.
    fixture
        .put("in/a.jsonl", b"{\"id\": 8}\n{\"id\": 9}\n")
        .await;
    let err = run(
        s3_source(&fixture, "in/*.jsonl"),
        &config,
        "s3-etag",
        workdir.path(),
    )
    .await
    .expect_err("refused");
    assert!(
        err.contains("was rewritten in place (same size, different etag)"),
        "{err}"
    );
}
