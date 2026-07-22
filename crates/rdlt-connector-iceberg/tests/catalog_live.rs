//! Feature 016 live cells against Polaris + RUSTFS (skip-not-fail).

mod common;

use common::CatalogFixture;

/// T004 smoke: the fixture yields a working catalog — namespace create +
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
