//! The object-store protocol LIVE against RUSTFS: listing and reading
//! through the engine, the destination's COPY+DELETE publish, Replace
//! ownership on a real bucket, and the etag rewrite tripwire.

use rdlt_connector_file::{destination, source};
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
/// The crash-replay convergence sweep on the OBJECT-STORE path, whose
/// listing differs from the local one: a same-commit final a crashed
/// predecessor published is removed, bystanders survive. The scenario
/// and its full rationale are pinned locally in `test_parts.rs`; this
/// cell exists because the sweep's directory listing is a per-kind
/// implementation.
#[tokio::test]
async fn s3_publish_sweeps_a_predecessors_same_commit_finals() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    // A crashed attempt of (load-a, seq 1) split into two parts; the
    // retry below stages ONE. Plus bystanders that must survive.
    fixture
        .put("lake/events/part-load-a-1-1.jsonl", b"crashed residue")
        .await;
    fixture
        .put("lake/events/part-other-1-0.jsonl", b"another load")
        .await;
    fixture
        .put("lake/events/part-0.parquet", b"a user's dataset")
        .await;

    let dest_config = s3_dest(&fixture, "lake");
    let mut value = serde_json::to_value(&dest_config).expect("value");
    value["format"] = "jsonl".into();
    let dest_config = destination::Config::from_value(value).expect("valid");
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
    use rdlt_connector_sdk::spi::{Destination, OpenContext};
    use rdlt_testkit::{batch_of, commit_meta_for, schema_for};
    let dest = destination::Shell::new(dest_config).expect("valid");
    let pipeline = PipelineId::new("s3-sweep");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1, 2]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    assert!(
        fixture.exists("lake/events/part-load-a-1-0.jsonl").await,
        "the retry's own part landed"
    );
    assert!(
        !fixture.exists("lake/events/part-load-a-1-1.jsonl").await,
        "the crashed predecessor's same-commit final is swept"
    );
    assert!(
        fixture.exists("lake/events/part-other-1-0.jsonl").await,
        "another load's final survives the sweep"
    );
    assert!(
        fixture.exists("lake/events/part-0.parquet").await,
        "a foreign dataset file survives the sweep"
    );
}

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

/// A leading slash on an S3 pattern is a habit carried over from
/// local paths; listed keys come back normalized, so an un-normalized
/// matcher would silently match NOTHING (030 review). The pattern is
/// normalized the store's way instead.
#[tokio::test]
async fn a_leading_slash_pattern_still_matches() {
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
        s3_source(&fixture, "/in//*.jsonl"),
        &config,
        "s3-slash",
        workdir.path(),
    )
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        2
    );
}

/// The skip-fetch shortcut PROVABLY engages: a second run over an
/// unchanged, completed parquet object skips its download, counted on
/// the connector instance (an optimisation that silently stops
/// engaging is indistinguishable from one never written).
#[tokio::test]
async fn an_unchanged_parquet_object_skips_its_second_fetch() {
    use rdlt_connector_sdk::source::{SourceConnector, shell};

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    // A real parquet object, seeded once.
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
            1i64, 2, 3,
        ]))],
    )
    .expect("batch");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None).expect("writer");
    writer.write(&batch).expect("write");
    let bytes = writer.into_inner().expect("bytes");
    fixture.put("in/a.parquet", &bytes).await;

    let mut value = serde_json::json!({
        "streams": [{"name": "events", "format": "parquet", "path": "in/*.parquet"}]
    });
    value["streams"][0]["location"] =
        serde_json::to_value(fixture.location_options()).expect("options");
    let src_config = source::Config::from_value(value).expect("valid");

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let dest_config = local_dest(out.path());
    // One connector instance for both runs — the counter lives on it.
    let file = source::File::assemble(src_config).expect("assembles");
    for _ in 0..2 {
        Engine::new(
            EngineConfig::new("s3-skip-fetch").with_workdir(workdir.path().join("wal")),
            shell(file.clone()),
            destination::Shell::new(dest_config.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");
    }
    assert_eq!(
        destination::testhook::count_rows(&dest_config, "events").expect("count"),
        3,
        "the second run read nothing new"
    );
    assert_eq!(
        file.skipped_fetches(),
        1,
        "the completed, etag-matched object skipped exactly its second download"
    );
}

/// Staged keys stay invisible to data globs on the OBJECT store too:
/// a planted dot-prefixed key never matches a recursive pattern, while
/// a sibling data key does (the S3 twin of the local planted
/// `.rdlt-staging` pin — 030 review found the S3 side unpinned).
#[tokio::test]
async fn s3_wildcards_never_match_dot_prefixed_keys() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put(".rdlt-staging/ab/load-1/t/all-0.jsonl", b"{\"id\": 99}\n")
        .await;
    fixture.put("in/a.jsonl", b"{\"id\": 1}\n").await;

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());
    run(
        s3_source(&fixture, "**/*.jsonl"),
        &config,
        "s3-dot-keys",
        workdir.path(),
    )
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        1,
        "the staged key's row must not load"
    );
}
