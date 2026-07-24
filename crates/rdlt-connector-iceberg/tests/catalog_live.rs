//! Live cells against Polaris + RUSTFS (skip-not-fail).

mod common;

use common::CatalogFixture;

/// Smoke: the fixture yields a working catalog — namespace create +
/// empty table create/load round-trip through the SAME library wiring the
/// destination will use.
#[tokio::test(flavor = "multi_thread")]
async fn fixture_smoke_namespace_and_table_round_trip() {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use iceberg_catalog_rest::RestCatalogBuilder;
    use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
    use std::collections::HashMap;
    use std::sync::Arc;

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load(
            "rdlt",
            HashMap::from([
                ("uri".to_string(), fixture.catalog_uri.clone()),
                ("warehouse".to_string(), common::WAREHOUSE.to_string()),
                (
                    "credential".to_string(),
                    format!("{}:{}", common::CLIENT_ID, common::CLIENT_SECRET),
                ),
                ("scope".to_string(), "PRINCIPAL_ROLE:ALL".to_string()),
            ]),
        )
        .await
        .expect("catalog loads");

    let ns = NamespaceIdent::new("smoke".into());
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .expect("namespace create");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("schema");
    catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("empty".into())
                .schema(schema)
                .build(),
        )
        .await
        .expect("table create");
    let table = catalog
        .load_table(&TableIdent::new(ns, "empty".into()))
        .await
        .expect("table load");
    assert_eq!(table.identifier().name(), "empty");
    assert!(table.metadata().current_snapshot().is_none(), "empty table");
}

/// Exact totals through the ENGINE into a Polaris-cataloged table —
/// one snapshot per non-empty commit, each carrying the commit identity,
/// asserted through the raw-catalog receipt oracle.
#[tokio::test(flavor = "multi_thread")]
async fn engine_append_exact_totals_one_snapshot_per_commit() {
    use rdlt_connector::StreamSpec;
    use rdlt_connector_iceberg::IcebergDest;
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
    use serde_json::json;

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![
                json!({"id": 1, "name": "a"}),
                json!({"id": 2, "name": "b"}),
                json!({"id": 3, "name": "c"}),
            ])
            .with_checkpoint(3),
            MemoryBatch::new(vec![json!({"id": 4, "name": "d"})]).with_checkpoint(4),
        ],
    )]);
    let dest = IcebergDest::from_config(fixture.config("engine_totals")).expect("dest");
    let report = Engine::new(EngineConfig::new("ice-totals"), source, dest)
        .run()
        .await
        .expect("run");
    assert_eq!(report.total_rows(), 4);
    assert_eq!(report.commits, 2);

    let snapshots = fixture.snapshot_summaries("engine_totals", "events").await;
    assert_eq!(snapshots.len(), 2, "one snapshot per non-empty commit");
    let added: Vec<&str> = snapshots
        .iter()
        .map(|s| s["added-records"].as_str())
        .collect();
    assert_eq!(added, ["3", "1"], "exact totals per commit window");
    for (snapshot, seq) in snapshots.iter().zip(["1", "2"]) {
        assert!(!snapshot["rdlt.pipeline"].is_empty(), "scope stamped");
        assert!(!snapshot["rdlt.load-id"].is_empty(), "load id stamped");
        assert_eq!(snapshot["rdlt.commit-seq"], seq, "commit seq stamped");
    }
}

/// An empty commit window publishes NOTHING — no snapshot, no
/// empty data file.
#[tokio::test(flavor = "multi_thread")]
async fn engine_empty_commit_publishes_no_snapshot() {
    use rdlt_connector::StreamSpec;
    use rdlt_connector_iceberg::IcebergDest;
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
    use serde_json::json;

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("sparse"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1),
            MemoryBatch::new(vec![]).with_checkpoint(2),
        ],
    )]);
    let dest = IcebergDest::from_config(fixture.config("engine_empty")).expect("dest");
    let report = Engine::new(EngineConfig::new("ice-empty"), source, dest)
        .run()
        .await
        .expect("run");
    assert_eq!(report.total_rows(), 1);

    let snapshots = fixture.snapshot_summaries("engine_empty", "sparse").await;
    assert_eq!(snapshots.len(), 1, "the empty window left no snapshot");
    assert_eq!(snapshots[0]["added-records"], "1");
}

/// Typed-error cells: unauthorized and missing-warehouse against a
/// LIVE catalog; each surfaces a typed DestError naming the subject —
/// never a panic or a silent success.
#[tokio::test(flavor = "multi_thread")]
async fn open_failures_are_typed_against_live_catalog() {
    use rdlt_connector::core::{LoadId, PipelineId};
    use rdlt_connector::{Destination, OpenCtx};
    use rdlt_connector_iceberg::{AuthOptions, IcebergConfig, IcebergDest};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let ctx = || OpenCtx::new(PipelineId::new("p"), LoadId::new("l"));

    // Wrong credentials: the OAuth leg fails, typed.
    let mut bad_auth = fixture.config("auth_fail");
    bad_auth.catalog.auth = AuthOptions::oauth2(
        common::CLIENT_ID,
        "wrong-secret",
        ["PRINCIPAL_ROLE:ALL".to_string()],
    );
    let err = IcebergDest::from_config(bad_auth)
        .expect("config valid")
        .open(ctx())
        .await
        .err()
        .expect("unauthorized must fail");
    let text = format!("{err}");
    assert!(
        text.contains(common::WAREHOUSE) || text.contains("401") || text.contains("OAuth"),
        "unauthorized error names its subject: {text}"
    );

    // Nonexistent warehouse: typed, names the warehouse.
    let missing = IcebergConfig::new(
        fixture.catalog_uri.clone(),
        "no-such-warehouse",
        AuthOptions::oauth2(
            common::CLIENT_ID,
            common::CLIENT_SECRET,
            ["PRINCIPAL_ROLE:ALL".to_string()],
        ),
        "ns",
    );
    let err = IcebergDest::from_config(missing)
        .expect("config valid")
        .open(ctx())
        .await
        .err()
        .expect("missing warehouse must fail");
    let text = format!("{err}");
    assert!(
        text.contains("no-such-warehouse"),
        "missing-warehouse error names the warehouse: {text}"
    );
}

/// Typed-error cell: an unreachable catalog endpoint is a typed
/// error naming the uri — needs no container.
#[tokio::test(flavor = "multi_thread")]
async fn open_unreachable_catalog_is_typed() {
    use rdlt_connector::core::{LoadId, PipelineId};
    use rdlt_connector::{Destination, OpenCtx};
    use rdlt_connector_iceberg::{AuthOptions, IcebergConfig, IcebergDest};

    // A port that was bound then released: nothing listens there.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr").port()
    };
    let uri = format!("http://127.0.0.1:{port}/api/catalog");
    let config = IcebergConfig::new(
        uri.clone(),
        "rdlt",
        AuthOptions::bearer("dummy-token"),
        "ns",
    );
    let err = IcebergDest::from_config(config)
        .expect("config valid")
        .open(OpenCtx::new(PipelineId::new("p"), LoadId::new("l")))
        .await
        .err()
        .expect("unreachable must fail");
    let text = format!("{err}");
    assert!(
        text.contains(&uri) || text.contains("127.0.0.1"),
        "unreachable error names the endpoint: {text}"
    );
}
