# rdlt-connector-duckdb

DuckDB destination: Arrow-native ingestion into an embedded DuckDB file —
struct-preserving lowering, temp-table staging, transactional commits, and
(feature 013) the FULL destination-options vocabulary through the shared
merge core.

```yaml
destination:
  duckdb:
    path: analytics.duckdb
    memory_limit: 512MB          # optional: cap DuckDB's buffer memory
    extensions: [httpfs]         # optional: LOAD extensions at open (G3)
    settings: {threads: "4"}     # optional: SET key='value' at open (G3)
    merge_strategy: upsert       # delete_insert (default) | upsert | scd2
    tables:
      events: {hard_delete: _deleted, dedup_sort: {column: seq, order: desc}}
      daily:  {merge_key: [day]}
```

## Options

`merge_strategy`, `hard_delete`, `dedup_sort`, `merge_key`, and the `scd2`
block are the shared vocabulary (`rdlt-connector-sqlcore`) — semantics,
validation, and typed errors are IDENTICAL to the postgres destination.
The full reference lives in `crates/rdlt-connector-postgres/README.md`
("Destination configuration"); everything there applies here by swapping
the destination block. Library builder: `DuckDb::open(path).options(...)`.

## Capabilities

| Capability | Value | Notes |
|---|---|---|
| merge | yes | all three strategies + refinements (shared core) |
| structs / scalar lists | yes | native STRUCT / LIST columns |
| json | yes | `Json` columns are native DuckDB JSON (queryable with `json_extract`); staged as VARCHAR, cast at publish |
| decimal | yes | `DECIMAL(p,s)` |

## DuckDB-specific notes (recorded deviations, none semantic)

- scd2 validity columns: DuckDB rejects `ADD COLUMN … NOT NULL`, so
  `valid_from` is added as `TIMESTAMPTZ DEFAULT now()` without the belt
  constraint postgres carries — every insert supplies the boundary
  explicitly, proven equivalent by the cross-destination differential
  suite (`tests/differential.rs`).
- Stage arrival order is `rowid` (append order) — the deterministic
  last-wins tie-breaker, expressed through the shared dialect seam.
- Verification: traceability matrix + probe outcomes in
  `specs/013-duckdb-completeness/matrix.md`; dlt parity record in
  `specs/013-duckdb-completeness/dlt-parity.md`.
