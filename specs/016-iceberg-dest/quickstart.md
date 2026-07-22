# Quickstart: Iceberg Destination

## Land a lakehouse table through any REST catalog

```yaml
# pipeline.yaml
pipeline: orders-to-lakehouse
source:
  postgres: {config: source.yaml}
destination:
  iceberg:
    catalog:
      uri: "https://polaris.example.com/api/catalog"
      warehouse: analytics
      auth:
        oauth2_client_credentials:      # Polaris / Snowflake Open Catalog
          client_id: my-client
          client_secret: "${CATALOG_SECRET}"
          scopes: [PRINCIPAL_ROLE:ALL]
        # bearer: {token: "${UC_PAT}"}  # Databricks UC
    namespace: raw.orders
    create_namespace: true
    tables:
      orders:
        partition_by:
          - {column: created_at, transform: day}
# storage: omitted — the catalog VENDS credentials (session tokens);
# declare the family s3 block only for self-managed buckets.
```

`rdlt run pipeline.yaml` — every engine commit is one atomic Iceberg
snapshot; crash/replay never duplicates (the commit identity lives in
snapshot summary properties). The table is readable by Spark, Trino,
pyiceberg, DuckDB — anyone speaking Iceberg.

## Verify

```bash
cargo nextest run -p rdlt-connector-iceberg                 # unit + schema cells
cargo nextest run -p rdlt-connector-iceberg -E 'binary(catalog_live)'  # Polaris+RUSTFS (skips w/o podman)
cargo nextest run -p rdlt-connector-iceberg -E 'binary(interop)'       # pyiceberg read-back
cargo nextest run -p rdlt-connector-iceberg --features failpoints -E 'binary(sweep)'
cargo llvm-cov nextest -p rdlt-connector-iceberg            # ≥80% floor
make test TARGET=deep                                        # incl. Spark read-back
```

## The rules

`contracts/iceberg-dest.md` (ID1–ID8): one protocol behind one
boundary, snapshot-native exactly-once, bounded conflict retry, closed
type mapping, no silent capability degradation, secret redaction,
live crash sweep, interop as the oracle.
