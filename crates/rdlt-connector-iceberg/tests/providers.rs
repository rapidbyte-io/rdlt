//! T010 provider/storage cells (contract ID4): credential delegation
//! (no user storage keys — the catalog's config-defaults carry them) is
//! the DEFAULT path; the family-S3 storage override is the explicit
//! alternative and genuinely takes precedence (a wrong override FAILS —
//! never silently ignored in favor of vended defaults). Live against
//! Polaris + RUSTFS, skip-not-fail.

mod common;

use common::CatalogFixture;
use rdlt_connector::StreamSpec;
use rdlt_connector_iceberg::{IcebergDest, S3Override};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

fn source() -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(vec![json!({"id": 1}), json!({"id": 2})]).with_checkpoint(2)],
    )])
}

/// Credential delegation: the dest config names NO storage keys at all —
/// the catalog's config-defaults response carries them (fixture.config
/// has no storage block). Full engine run, exact totals.
#[tokio::test(flavor = "multi_thread")]
async fn vended_credentials_no_storage_block() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let config = fixture.config("vended");
    assert!(
        config.storage.is_none(),
        "the delegation cell must carry no user storage keys"
    );
    let dest = IcebergDest::from_config(config).expect("dest");
    let report = Engine::new(EngineConfig::new("ice-vended"), source(), dest)
        .run()
        .await
        .expect("vended run");
    assert_eq!(report.total_rows(), 2);
    let snapshots = fixture.snapshot_summaries("vended", "events").await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["added-records"], "2");
}

/// The explicit family-S3 override: same run, keys supplied in the dest
/// config instead of delegated.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_storage_override_works() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let config = fixture.config("explicit").with_storage_s3(
        S3Override::new(common::S3_KEY, common::S3_SECRET)
            .with_endpoint(fixture.s3_endpoint.clone())
            .with_region("us-east-1"),
    );
    let dest = IcebergDest::from_config(config).expect("dest");
    let report = Engine::new(EngineConfig::new("ice-explicit"), source(), dest)
        .run()
        .await
        .expect("override run");
    assert_eq!(report.total_rows(), 2);
    let snapshots = fixture.snapshot_summaries("explicit", "events").await;
    assert_eq!(snapshots.len(), 1);
}

/// Precedence proof: a WRONG override must make the run fail — if the
/// vended defaults silently won, this would succeed and rows would land
/// with credentials the user explicitly replaced.
#[tokio::test(flavor = "multi_thread")]
async fn wrong_storage_override_fails_not_silently_ignored() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let config = fixture.config("wrongkeys").with_storage_s3(
        S3Override::new("wrong-key", "wrong-secret")
            .with_endpoint(fixture.s3_endpoint.clone())
            .with_region("us-east-1"),
    );
    let dest = IcebergDest::from_config(config).expect("dest");
    let err = Engine::new(EngineConfig::new("ice-wrongkeys"), source(), dest)
        .run()
        .await
        .expect_err("wrong override must fail — precedence over vended defaults");
    let text = format!("{err}");
    assert!(
        !text.contains("wrong-secret"),
        "secret never rendered: {text}"
    );
    let snapshots = fixture.snapshot_summaries("wrongkeys", "events").await;
    assert!(snapshots.is_empty(), "nothing may land under wrong keys");
}

/// T011: the bearer auth arm live — a static token attached as
/// `Authorization: Bearer` (the same header path Snowflake Open
/// Catalog / any bearer catalog sees; UC OSS write leg recorded
/// read-only at T001). The fixture's admin token doubles as the PAT.
#[tokio::test(flavor = "multi_thread")]
async fn bearer_auth_against_live_catalog() {
    use rdlt_connector_iceberg::{AuthOptions, IcebergConfig};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let config = IcebergConfig::new(
        fixture.catalog_uri.clone(),
        common::WAREHOUSE,
        AuthOptions::bearer(fixture.admin_token.clone()),
        "bearer_ns",
    )
    .with_create_namespace(true);
    let dest = IcebergDest::from_config(config).expect("dest");
    let report = Engine::new(EngineConfig::new("ice-bearer"), source(), dest)
        .run()
        .await
        .expect("bearer run");
    assert_eq!(report.total_rows(), 2);
    let snapshots = fixture.snapshot_summaries("bearer_ns", "events").await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["added-records"], "2");
}
