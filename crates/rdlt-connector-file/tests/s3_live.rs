//! Feature 015 US2: live cells against the RUSTFS container (skip-not-fail
//! without a runtime socket — every cell early-returns visibly).

mod common;

use common::s3::S3Fixture;
use rdlt_connector_file::location::s3::S3Options;
use rdlt_connector_file::location::{Location, LocationOptions};

/// Discovery over a seeded bucket: deterministic order, glob filtering,
/// exact byte sizes, etags present.
#[tokio::test]
async fn seeded_bucket_lists_deterministically() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture.put("landed/b.jsonl", b"{\"id\":2}\n").await;
    fixture.put("landed/a.jsonl", b"{\"id\":1}\n").await;
    fixture.put("landed/skip.csv", b"id\n3\n").await;
    let files = fixture
        .location()
        .list("landed/*.jsonl")
        .await
        .expect("list");
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].path, "landed/a.jsonl");
    assert_eq!(files[1].path, "landed/b.jsonl");
    assert_eq!(files[0].size, 9);
    assert!(files[0].etag.is_some(), "etag is the object identity");
}

/// FF2: listings are COMPLETE across pagination — a prefix holding more
/// objects than one S3 listing page (1000) resolves every key.
#[tokio::test]
async fn listing_survives_pagination() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    let seeds: Vec<(String, Vec<u8>)> = (0..1100)
        .map(|i| {
            (
                format!("many/k{i:04}.jsonl"),
                format!("{{\"i\":{i}}}\n").into_bytes(),
            )
        })
        .collect();
    // Concurrent seeding (the fixture server is local).
    futures_seed(&fixture, seeds).await;
    let files = fixture.location().list("many/*.jsonl").await.expect("list");
    assert_eq!(files.len(), 1100, "every continuation page drained");
    assert_eq!(files[0].path, "many/k0000.jsonl");
    assert_eq!(files[1099].path, "many/k1099.jsonl");
}

async fn futures_seed(fixture: &S3Fixture, seeds: Vec<(String, Vec<u8>)>) {
    use futures::StreamExt;
    futures::stream::iter(seeds)
        .for_each_concurrent(32, |(key, body)| async move {
            fixture.put(&key, &body).await;
        })
        .await;
}

/// A named (glob-less) missing object is a typed error — parity with the
/// local missing-file rule; an empty PREFIX stays success.
#[tokio::test]
async fn missing_named_object_is_typed_and_empty_prefix_is_success() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    let location = fixture.location();
    let err = location
        .list("landed/ghost.jsonl")
        .await
        .expect_err("named missing object")
        .to_string();
    assert!(
        err.contains("ghost.jsonl") && err.contains("not found"),
        "{err}"
    );
    let empty = location
        .list("nothing-here/*.jsonl")
        .await
        .expect("empty glob");
    assert!(empty.is_empty());
}

/// Wrong credentials: typed, naming endpoint+bucket — never a silent
/// empty load (FF2/FF6).
#[tokio::test]
async fn wrong_credentials_are_typed() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    let location = Location::from_options(Some(&LocationOptions::s3(S3Options::new(
        fixture.endpoint.clone(),
        common::s3::BUCKET,
        "wrong-access-cred",
        "wrong-secret-cred",
    ))))
    .expect("connect builds");
    let err = location
        .list("landed/*.jsonl")
        .await
        .expect_err("bad credentials")
        .to_string();
    assert!(
        err.contains(&fixture.endpoint) && err.contains("raw"),
        "error names endpoint+bucket: {err}"
    );
    assert!(
        !err.contains("wrong-access-cred") && !err.contains("wrong-secret-cred"),
        "credential VALUE never renders: {err}"
    );
}

/// Unreachable endpoint: typed and named (transient — the engine budget).
#[tokio::test]
async fn unreachable_endpoint_is_typed() {
    let location = Location::from_options(Some(&LocationOptions::s3(S3Options::new(
        "http://127.0.0.1:9", // discard port: nothing listens
        "nope",
        "k",
        "s",
    ))))
    .expect("connect builds");
    let err = location
        .list("x/*.jsonl")
        .await
        .expect_err("unreachable")
        .to_string();
    assert!(err.contains("127.0.0.1:9"), "names the endpoint: {err}");
}

/// Range reads: open_from(start) returns exactly the tail.
#[tokio::test]
async fn range_read_returns_the_tail() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture.put("tail/data.jsonl", b"0123456789").await;
    let mut reader = fixture
        .location()
        .open_from("tail/data.jsonl", 6)
        .await
        .expect("open");
    let mut buf = [0u8; 16];
    let n = reader.read_full(&mut buf).await.expect("read");
    assert_eq!(&buf[..n], b"6789");
}

/// US2 through the ENGINE: seeded jsonl bucket → duckdb exact totals, then
/// a DELTA run (two new objects + one grown) transfers exactly the delta —
/// report.total_rows() is the read accounting (SC-003).
#[tokio::test(flavor = "multi_thread")]
async fn seeded_bucket_loads_and_delta_runs_through_the_engine() {
    use rdlt_connector_duckdb::dest::DuckDb;
    use rdlt_connector_file::FileSource;
    use rdlt_engine::{Engine, EngineConfig};

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("eng/a.jsonl", b"{\"id\":1}\n{\"id\":2}\n")
        .await;
    fixture.put("eng/b.jsonl", b"{\"id\":3}\n").await;

    let yaml = format!(
        "streams:\n  - name: events\n    format: jsonl\n    path: \"eng/*.jsonl\"\n    {}",
        fixture.location_yaml().replace('\n', "\n    ")
    );
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    let report = Engine::new(
        EngineConfig::new("s3-files"),
        FileSource::from_yaml(&yaml).expect("config"),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 1");
    assert_eq!(report.total_rows(), 3);
    assert_eq!(dest.count_rows("events").expect("count"), 3);

    // Delta: two new objects + one GROWN (re-upload old content + a tail).
    fixture
        .put("eng/a.jsonl", b"{\"id\":1}\n{\"id\":2}\n{\"id\":5}\n")
        .await;
    fixture.put("eng/c.jsonl", b"{\"id\":6}\n").await;
    fixture.put("eng/d.jsonl", b"{\"id\":7}\n").await;

    let report = Engine::new(
        EngineConfig::new("s3-files"),
        FileSource::from_yaml(&yaml).expect("config"),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 2");
    assert_eq!(
        report.total_rows(),
        3,
        "exactly the delta: the grown tail (id 5) + the two new objects"
    );
    assert_eq!(
        dest.count_rows("events").expect("count"),
        6,
        "no duplicates"
    );
}

/// FF3 on objects: same-size different-etag = rewritten in place — typed,
/// naming the key, never a stale-offset read.
#[tokio::test(flavor = "multi_thread")]
async fn same_size_rewrite_is_typed_by_etag() {
    use rdlt_connector_duckdb::dest::DuckDb;
    use rdlt_connector_file::FileSource;
    use rdlt_engine::{Engine, EngineConfig};

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture.put("trip/x.jsonl", b"{\"id\":1}\n").await;
    let yaml = format!(
        "streams:\n  - name: events\n    format: jsonl\n    path: \"trip/*.jsonl\"\n    {}",
        fixture.location_yaml().replace('\n', "\n    ")
    );
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    Engine::new(
        EngineConfig::new("s3-trip"),
        FileSource::from_yaml(&yaml).expect("config"),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 1");

    // Same byte length, different content → same size, new etag.
    fixture.put("trip/x.jsonl", b"{\"id\":9}\n").await;
    let err = Engine::new(
        EngineConfig::new("s3-trip"),
        FileSource::from_yaml(&yaml).expect("config"),
        dest.clone(),
    )
    .run()
    .await
    .expect_err("rewrite tripwire")
    .to_string();
    assert!(
        err.contains("trip/x.jsonl") && err.contains("etag"),
        "names the key: {err}"
    );
}

/// Parquet objects load through the temp-fetch path with row-group cursors.
#[tokio::test(flavor = "multi_thread")]
async fn parquet_objects_load_through_the_engine() {
    use rdlt_connector_duckdb::dest::DuckDb;
    use rdlt_connector_file::FileSource;
    use rdlt_engine::{Engine, EngineConfig};

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    // Build a small parquet file locally, then seed it as an object.
    let dir = tempfile::tempdir().expect("tempdir");
    let local = dir.path().join("m.parquet");
    write_parquet(&local, &[10, 20, 30]);
    fixture
        .put("pq/m.parquet", &std::fs::read(&local).expect("read"))
        .await;

    let yaml = format!(
        "streams:\n  - name: metrics\n    format: parquet\n    path: \"pq/*.parquet\"\n    {}",
        fixture.location_yaml().replace('\n', "\n    ")
    );
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    let report = Engine::new(
        EngineConfig::new("s3-pq"),
        FileSource::from_yaml(&yaml).expect("config"),
        dest.clone(),
    )
    .run()
    .await
    .expect("run");
    assert_eq!(report.total_rows(), 3);
    assert_eq!(dest.count_rows("metrics").expect("count"), 3);

    // A second run re-lists and skips the completed object entirely.
    let report = Engine::new(
        EngineConfig::new("s3-pq"),
        FileSource::from_yaml(&yaml).expect("config"),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 2");
    assert_eq!(report.total_rows(), 0, "completed object skipped");
}

fn write_parquet(path: &std::path::Path, ids: &[i64]) {
    use std::sync::Arc;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(ids.to_vec()))],
    )
    .expect("batch");
    let file = std::fs::File::create(path).expect("create");
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

/// T010 (SC-002/SC-003 closing cell): the quickstart shape LIVE —
/// compressed CSV in the bucket, hints + primary_key, engine → duckdb,
/// then a delta run over new objects only.
#[tokio::test(flavor = "multi_thread")]
async fn quickstart_shape_csv_gz_with_hints_and_delta() {
    use rdlt_connector_duckdb::dest::DuckDb;
    use rdlt_connector_file::FileSource;
    use rdlt_engine::{Engine, EngineConfig};
    use std::io::Write;

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    let gz = |body: &str| {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(body.as_bytes()).unwrap();
        enc.finish().unwrap()
    };
    fixture
        .put(
            "landed/2026/a.csv.gz",
            &gz("id,amount,created_at\n1,2.5,2026-01-01T00:00:00Z\n2,3,2026-01-02T00:00:00Z\n"),
        )
        .await;

    let yaml = format!(
        "streams:\n  - name: events\n    format: csv\n    path: \"landed/2026/*.csv.gz\"\n    primary_key: [id]\n    type_hints: {{amount: float64, created_at: timestamp_tz}}\n    {}",
        fixture.location_yaml().replace('\n', "\n    ")
    );
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    let report = Engine::new(
        EngineConfig::new("s3-quickstart"),
        FileSource::from_yaml(&yaml).expect("config"),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 1");
    assert_eq!(report.total_rows(), 2);
    assert_eq!(dest.count_rows("events").expect("count"), 2);
    // The declared hint typed the column (timestamp arithmetic works).
    assert_eq!(
        dest.query_string(
            "SELECT count(*)::VARCHAR FROM events WHERE created_at >= TIMESTAMPTZ '2026-01-02'"
        )
        .expect("typed query"),
        "1"
    );

    // Delta: one new object; the completed one is skipped (whole-file unit).
    fixture
        .put(
            "landed/2026/b.csv.gz",
            &gz("id,amount,created_at\n3,4,2026-01-03T00:00:00Z\n"),
        )
        .await;
    let report = Engine::new(
        EngineConfig::new("s3-quickstart"),
        FileSource::from_yaml(&yaml).expect("config"),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 2");
    assert_eq!(report.total_rows(), 1, "exactly the new object");
    assert_eq!(
        dest.count_rows("events").expect("count"),
        3,
        "no duplicates"
    );
}
