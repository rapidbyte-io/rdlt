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
