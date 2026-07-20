# rdlt-postgres

Bundled PostgreSQL connectors for rdlt — SOURCE and DESTINATION in one
crate (feature-gated `source`/`dest` modules, both default; shared `tls`
module). Source: catalog reflection publishes declared schemas, rows
stream as typed Arrow batches decoded **directly from the binary COPY
wire format** (the engine's structured path — no JSON, no shredding, no
inference), and cursor-column incremental loading has closed/open
boundary semantics with mid-table checkpointed resume.

Contracts: `specs/005-postgres-source/contracts/source-config.md` (the YAML
document), `contracts/type-mapping.md` (every Postgres type → engine type
rule, lossy rules explicit), and feature 006's
`specs/006-postgres-completeness/contracts/` (tls-policy, type-hints,
query-streams, merge-structured).

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
    type_hints:                   # per-column overrides (closed conversion table)
      total: decimal(12,4)        #   e.g. unconstrained numeric → real decimality
      created: timestamp_tz       #   text → typed via strict server-side cast
  - name: customers               # snapshot stream (full re-read per run)
# custom SQL streams: schema DESCRIBED by the database, wrapped read-only
queries:
  - name: order_totals
    sql: "SELECT o.id, max(o.updated_at) AS updated_at, sum(i.amount) AS total
          FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY o.id"
    cursor: { column: updated_at }
    primary_key: [id]
    type_hints: { total: decimal(14,2) }
```

The connector's `ConnectorSpec.config_schema` is a JSON Schema GENERATED
from these structs (`source::config_schema()`), so platform validation and
the parser cannot drift.

### TLS

```yaml
tls:
  mode: verify_full          # disable | prefer | require | verify_ca | verify_full
  root_cert: /etc/ca.pem     # path or inline PEM; omit for the platform store
  client_cert: /etc/c.pem    # mutual TLS (feature 007): both-or-neither with…
  client_key: /etc/c.key     # …an unencrypted PKCS#8/RSA/SEC1 key
```

libpq semantics: `require` encrypts WITHOUT validating (use `verify_full`
in production); contradictions are typed config errors. The DESTINATION
takes the same policy (`Postgres::tls(...)` / CLI TOML `tls = {...}`)
through the same code path. Server rejection of the client credential is
the distinguished `ClientCert` failure, separate from our verification of
the server.

Existing libpq URLs just work (feature 007): `sslmode=verify-ca|verify-full`,
`sslrootcert=` (`system` = platform store), `sslcert=`/`sslkey=` translate
into the policy; a conn parameter and a block field that disagree fail
typed naming both. Any unsupported parameter is rejected BY NAME — never a
bare parse error. libpq's implicit `~/.postgresql/*` file defaults are NOT
emulated. Every connection carries `application_name = rdlt` unless the
conn string sets its own.

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
- **Late-arriving rows**: `cursor.lag` (e.g. `"5m"`) re-scans a window
  behind the watermark every run, so commits that landed with an older
  cursor value are captured. Requires a closed boundary and a primary key;
  pair with Merge write mode for exact totals — under Append the window
  rows re-deliver each run (documented at-least-once). `nulls: error`
  makes a NULL cursor value a typed failure; `end_bound: inclusive` makes
  `end_value` a closed upper bound.
- **Discovery scope**: partition leaves AND classic `INHERITS` children
  are excluded (rows arrive once, via the parent); list a child explicitly
  under `tables:` to read it alone. Foreign tables (`relkind 'f'`) are
  never discovered.
- **Types**: uuid/json/jsonb/arrays/enums land as text-typed columns
  (canonical text / JSON text) — the structured path derives logical types
  from Arrow, which carries no uuid/json. Unconstrained or >38-digit
  numerics arrive as lossless text, never truncated. `±infinity`
  timestamps saturate to the representable extremes, visibly.
- **Merge write mode** works for structured streams WITH a declared
  `primary_key` (engine clause B4 as amended by feature 006): updates
  converge to one row per key, exactly-once under the crash model. Keyless
  structured streams still reject Merge at plan time.
- **Lossy mappings announce themselves**: every [documented-lossy] column
  emits one `tracing::warn!` on the `rdlt::lossy` target per read.

## Verification

`cargo nextest run -p rdlt-postgres` — conformance (full
type-matrix round-trip against real Postgres), incremental boundary
semantics, differential property test (decoder ≡ an independent driver
reference, single- AND multi-batch), drift matrix, TLS matrix (five
sslmode levels × cert scenarios, both directions), query streams, config
schema round-trips. `--features failpoints` adds the crash sweeps
(exactly-once under kill/panic at every registered fail point, both
occurrence passes, Append + Merge modes) and the memory-ceiling test (a
6.9 GB table through a 256 MiB process ceiling; `RDLT_HEAVY=1` makes
missing prerequisites a hard failure). Fuzz target: `pg_copy_decode`.
