//! T006: catalog reflection against a real Postgres — quoted identifiers,
//! PKs, views, non-default schema, domains, enums, arrays, numerics.

use rdlt_connector_postgres::fixtures::PgFixture;
use rdlt_connector_postgres::source::PostgresConfig;
use rdlt_connector_postgres::source::testhook::reflect_for_tests;

const SEED: &str = r#"
CREATE SCHEMA sales;
CREATE TYPE sales.mood AS ENUM ('happy', 'grumpy');
CREATE DOMAIN sales.price AS numeric(12,4);
CREATE TABLE sales."Order Items" (
    "Id"        int8 NOT NULL,
    "qty"       int4,
    PRIMARY KEY ("Id")
);
CREATE TABLE sales.orders (
    id          int8 NOT NULL,
    created_at  timestamptz NOT NULL,
    total       numeric(10,2),
    unit_price  sales.price,
    tags        text[],
    mood        sales.mood,
    payload     jsonb,
    PRIMARY KEY (id, created_at)
);
CREATE VIEW sales.orders_view AS SELECT id, total FROM sales.orders;
CREATE TABLE public.not_in_scope (x int4);
"#;

#[tokio::test(flavor = "multi_thread")]
async fn reflects_schema_shape_pks_views_and_type_policies() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(SEED).await;

    // Tables only (no views).
    let config = PostgresConfig::from_yaml(&format!("conn: \"{}\"\nschema: sales\n", fixture.conn))
        .expect("config");
    let tables = reflect_for_tests(&config).await.expect("reflect");
    assert_eq!(
        tables.keys().collect::<Vec<_>>(),
        ["Order Items", "orders"],
        "views excluded by default; public schema out of scope"
    );

    let orders = &tables["orders"];
    assert_eq!(orders.primary_key(), vec!["id", "created_at"]);
    let col = |n: &str| orders.column(n).unwrap_or_else(|| panic!("column {n}"));
    // Declared-type facts, per the type-mapping contract.
    assert_eq!(col("total").arrow_type().to_string(), "Decimal128(10, 2)");
    // Domain resolves one level to numeric(12,4).
    assert_eq!(
        col("unit_price").arrow_type().to_string(),
        "Decimal128(12, 4)"
    );
    // Array + enum + jsonb → Utf8 policy rows.
    for policy_col in ["tags", "mood", "payload"] {
        assert_eq!(
            col(policy_col).arrow_type().to_string(),
            "Utf8",
            "{policy_col}"
        );
    }
    assert!(col("id").is_not_null(), "NOT NULL reflected");

    // Quoted mixed-case identifiers reflect verbatim.
    let items = &tables["Order Items"];
    assert_eq!(items.primary_key(), vec!["Id"]);

    // include_views picks up the view.
    let config_views = PostgresConfig::from_yaml(&format!(
        "conn: \"{}\"\nschema: sales\ninclude_views: true\n",
        fixture.conn
    ))
    .expect("config");
    let with_views = reflect_for_tests(&config_views)
        .await
        .expect("reflect views");
    assert!(with_views.contains_key("orders_view"));

    // Unknown listed table is a typed reflect error.
    let config_missing = PostgresConfig::from_yaml(&format!(
        "conn: \"{}\"\nschema: sales\ntables:\n  - name: ghost\n",
        fixture.conn
    ))
    .expect("config");
    let err = reflect_for_tests(&config_missing).await.unwrap_err();
    assert!(err.to_string().contains("fatal"), "{err}");
}
