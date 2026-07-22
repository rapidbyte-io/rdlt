# rdlt-connector-iceberg

A provider-agnostic Apache Iceberg destination: one Iceberg REST
catalog protocol, many providers (Apache Polaris, Snowflake Open
Catalog, Databricks Unity Catalog, Lakekeeper, …). Engine commits map
onto atomic Iceberg snapshots that carry the commit identity in their
summary properties — exactly-once is readable from table history alone,
by any Iceberg implementation. The Iceberg mechanics (manifests,
metadata, field IDs, the REST commit machinery) come from Apache
iceberg-rust, wrapped at one boundary: nothing from that library
crosses this crate's public surface.

Facade: `rdlt::connector::iceberg` (feature `iceberg`). CLI:
`destination: iceberg:` in the pipeline YAML.

```yaml
# pipeline.yaml
destination:
  iceberg:
    catalog:
      uri: https://catalog.example.com/api/catalog
      warehouse: analytics
      auth:
        oauth2_client_credentials:
          client_id: "${CATALOG_CLIENT_ID}"
          client_secret: "${CATALOG_CLIENT_SECRET}"
          scopes: ["PRINCIPAL_ROLE:ALL"]     # provider-specific
    namespace: raw.orders                     # dot-separated levels
    create_namespace: true
    tables:
      events:
        partition_by:
          - {column: region, transform: identity}
          - {column: created_at, transform: day}
```

Entry points: `IcebergDest::from_yaml`/`from_config(IcebergConfig)`,
`IcebergConfig::new(...)` + `with_*` builders, `config_schema()`
generated from the structs (schema and parser cannot drift). All
validation is eager and typed at construction.

## Catalog options

| option | default | meaning |
|---|---|---|
| `catalog.uri` | required | the Iceberg REST catalog root (http/https) |
| `catalog.warehouse` | required | provider warehouse identifier |
| `catalog.auth.oauth2_client_credentials` | one-of | `{token_url?, client_id, client_secret, scopes}` — client-credentials flow; `token_url` defaults to the catalog's own token endpoint |
| `catalog.auth.bearer` | one-of | `{token}` — a static token attached as `Authorization: Bearer` (PATs, Snowflake Open Catalog tokens) |
| `catalog.props` | `{}` | escape hatch: extra catalog properties passed through verbatim (they win over generated ones) |

Exactly one auth scheme must be set. All credentials are
`Secret`-wrapped: `Debug`/`Display` render `***`, and cells grep-prove
they never appear in any error or log line.

## Namespace, storage, tables

| option | default | meaning |
|---|---|---|
| `namespace` | required | destination namespace, dot-separated for multi-level catalogs |
| `create_namespace` | `false` | create the namespace iff missing; with `false` a missing namespace is a typed error naming the remedy |
| `storage.s3` | absent | absent = **credential delegation**: the catalog's `/v1/config` defaults carry the S3 endpoint/credentials (the recommended posture — no storage keys in the dest config). Set `{endpoint?, region?, access_key, secret_key, path_style}` to override explicitly; the override genuinely wins (a wrong override fails typed — it is never silently ignored) |
| `tables.<stream>.name` | stream name | rename a stream's table |
| `tables.<stream>.partition_by` | `[]` | list of `{column, transform}`; transforms: `identity`, `year`, `month`, `day`, `hour` |

Partition specs are applied at table **creation** and are fixed: a
config that disagrees with a live table's spec is a typed error (drop
the table or align the config — never a silent re-spec). Partitioned
tables write through a fanout writer: one data file per partition value
per commit. `_rdlt_state` is a reserved table name.

## Write semantics

- **Append** (the supported mode): each engine commit becomes one
  fast-append snapshot stamped with `rdlt.pipeline` (scope hash),
  `rdlt.load-id`, `rdlt.commit-seq` in its summary. Empty commit
  windows publish nothing.
- **Replace**: typed "not supported by this release" — iceberg-rust
  0.10 exposes no overwrite transaction, and emulating replace with a
  non-atomic delete+append is worse than refusing. Revisit when the
  library grows one.
- **Merge**: rejected by capability (`merge: false`).

**Exactly-once**: before committing, the session walks the fresh
table's snapshot history for its (load, seq) identity — a replayed
commit publishes nothing and returns the prior receipt. Data-file names
carry a per-session nonce, so a recovery session can never overwrite a
file a prior session committed. **Conflict retry**: optimistic-commit
conflicts (another writer landed first) retry refresh→rebuild→commit up
to 4 times with jittered backoff; exhaustion is a typed error naming
the table. A competitor's snapshots are never dropped.

**State**: the pipeline state document is stored as a property
(`rdlt.state.<scope>`) on a forever-empty marker table `_rdlt_state`
in the destination namespace, written after the data commits
(state-last). It is not in namespace properties because
`update_namespace` is unimplemented in iceberg-catalog-rest 0.10, and
not on stream tables because recovery cannot enumerate them from
config alone.

**Schema**: closed mapping from engine logical types (Json lands as
string — documented), structs and scalar lists recurse with unique
field IDs. Additive drift (new nullable columns) evolves the table in
one transaction; type conflicts are typed naming the column.

## Provider notes

- **Apache Polaris / Snowflake Open Catalog**: the conformance target —
  every live cell runs against a real Polaris + RUSTFS pair. Polaris
  parses partition specs strictly (explicit spec/field ids — handled).
  With `stsUnavailable` catalogs, clients must not send the
  `X-Iceberg-Access-Delegation` header (pyiceberg/Spark configs in
  `tools/interop/` show the blanking).
- **Databricks Unity Catalog**: the OSS build's Iceberg REST surface is
  read-only (verified) — writes need managed UC; the bearer scheme this
  crate would use is proven live against Polaris.
- **AWS Glue**: phase-2 — the REST client has no SigV4 signing seam
  yet (doors recorded in the feature research).

## Maintenance guidance

This destination appends snapshots and never rewrites data. Table
maintenance — snapshot expiry, compaction, orphan-file cleanup — is the
catalog/warehouse's job (Polaris/Snowflake housekeeping, `CALL
system.expire_snapshots(...)` from Spark, etc.). Note that crash
recovery may leave *orphaned* (never-committed, invisible) data files;
standard orphan-file cleanup removes them safely because no snapshot
references them.

## Verification

Traceability matrix: `specs/016-iceberg-dest/matrix.md` (zero uncited
rows). Parity record vs dlt: `specs/016-iceberg-dest/dlt-parity.md`.
Live cells (Polaris + RUSTFS containers, skip-not-fail without a
runtime), crash sweep (`make test TARGET=sweep`), pyiceberg read-back
(pinned venv: `python3 -m venv tools/interop/.venv &&
tools/interop/.venv/bin/pip install -r tools/interop/requirements.txt`),
Spark read-back in the deep tier (`make test TARGET=deep`), scoreboard
bench `iceberg-polaris-200k`.
