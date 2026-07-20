# rdlt-source-postgres

Bundled PostgreSQL source for rdlt: catalog reflection publishes declared
schemas, rows stream as typed Arrow batches decoded **directly from the
binary COPY wire format** (the engine's structured path — no JSON, no
shredding, no inference), and cursor-column incremental loading has
closed/open boundary semantics with mid-table checkpointed resume.

Contracts: `specs/005-postgres-source/contracts/source-config.md` (the YAML
document) and `contracts/type-mapping.md` (every Postgres type → engine
type rule, lossy rules explicit).

## Configuration

One document shape, three entry points sharing validation: `from_yaml`
(files/CLI), `from_json`, and `from_value(serde_json::Value)` — the
embedder path for platforms that store connector configs as JSON.

```yaml
conn: "postgresql://user:pass@host:5432/db"   # TLS: full sslmode matrix (verify_full recommended)
schema: public          # reflection scope (default: public)
include_views: false    # views/matviews join discovery when true
batch_target_bytes: 8388608
batch_max_rows: 65536
# omit `tables` to load every table in the schema
tables:
  - name: orders
    cursor:
      column: updated_at          # must exist; cursor-capable types only
      initial_value: "2026-01-01T00:00:00Z"   # optional first-run lower bound
      boundary: closed            # closed (>=, deduped) | open (>, monotonic cursors)
      direction: max              # max | min
      end_value: null             # optional upper bound (exclusive under max)
      nulls: exclude              # exclude | include (re-fetched every run)
    primary_key: [id]             # overrides the reflected PK
    excluded_columns: [internal]  # or included_columns (mutually exclusive)
  - name: customers               # snapshot stream (full re-read per run)
```

### TLS

```yaml
tls:
  mode: verify_full        # disable | prefer | require | verify_ca | verify_full
  root_cert: /etc/ca.pem   # path or inline PEM; omit for the platform store
```

libpq semantics: `require` encrypts WITHOUT validating (use `verify_full`
in production); conn-string `sslmode` covers disable/prefer/require;
verify-* needs the block; contradictions are typed config errors. The
DESTINATION takes the same policy (`Postgres::tls(...)` / CLI TOML
`tls = {...}`) through the same code path.

## Semantics worth knowing

- **Snapshot streams** re-read fully every run (use the pipeline's Replace
  write mode for mirrors). Each table reads under one statement-level
  snapshot — no torn reads within a table.
- **Incremental streams** are cursor-ordered and checkpoint mid-stream:
  crash, cancel, or transient network failure resumes from the last
  committed mid-table checkpoint (engine-owned retries; the source never
  retries — SPI clause S3).
- **Watermarks never regress**; closed boundaries re-fetch watermark-equal
  rows and dedup them by primary key (or whole-row hash when the table has
  no PK).
- **Types**: uuid/json/jsonb/arrays/enums land as text-typed columns
  (canonical text / JSON text) — the structured path derives logical types
  from Arrow, which carries no uuid/json. Unconstrained or >38-digit
  numerics arrive as lossless text, never truncated. `±infinity`
  timestamps saturate to the representable extremes, visibly.
- **Merge write mode is rejected** for structured streams by the engine
  (no per-row `_rdlt_id`); incremental is Append-mode. Merge-for-structured
  is a recorded backlog item.

## Verification

`cargo nextest run -p rdlt-source-postgres` — conformance (full
type-matrix round-trip against real Postgres), incremental boundary
semantics, differential property test (decoder ≡ an independent driver
reference), drift matrix. `--features failpoints` adds the crash sweep
(exactly-once under kill/panic at every registered fail point, both
occurrence passes) and the memory-ceiling test (a 6.9 GB table through a
256 MiB process ceiling). Fuzz target: `pg_copy_decode`.
