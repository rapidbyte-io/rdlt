# rdlt-postgres

Bundled PostgreSQL connectors for rdlt — **source** and **destination**
in one crate (feature-gated `source`/`dest` modules, both on by default;
shared `tls` module — one connect path for both directions).

What makes them fast and safe, in one paragraph: the source streams rows
as typed Arrow batches decoded **directly from the binary COPY wire
format** (no JSON, no shredding, no inference) and the destination
writes back through the same binary COPY path with hand-rolled wire
encoders that are round-trip-proven against the source's decoders.
Incremental loads checkpoint **mid-table** and resume exactly; CDC
captures inserts, updates, and deletes with exactly-once outcomes; every
failure mode is a typed error that names what broke and how to fix it —
nothing fails silently. Every claim below is pinned by a conformance
test, a crash sweep, or a recorded measurement.

Authoritative contracts live in `specs/`: `005-postgres-source`
(source-config, type-mapping), `006-postgres-completeness` (tls-policy,
type-hints, query-streams, merge-structured), `007` (cursor-lag,
connstring-portability, tls-client-auth), `008-postgres-dest-completion`
(dest-types, merge-strategies, scd2), `009-postgres-cdc` (cdc-protocol,
cdc-config, cdc-operability), `010-merge-refinements`
(merge-refinements).

---

## Quick start

ONE YAML document describes the whole pipeline — pipeline-wide settings,
source, and destination (via the `rdlt` CLI — `rdlt run pipeline.yaml`):

```yaml
# pipeline.yaml — mirror two tables, incremental on one
pipeline: app-mirror
write_mode: {merge: {key: [id]}}

source:
  postgres:
    conn: "postgresql://etl@db.internal/app?sslmode=verify-full&sslrootcert=/etc/ca.pem"
    tables:
      - name: orders
        cursor: {column: updated_at}
      - name: customers

destination:
  postgres:
    conn: "host=warehouse user=loader password=… dbname=analytics"
    dataset: mirror
    merge_strategy: upsert
```

The source document can also live in its own reusable YAML/JSON file —
`source: postgres: {config: source.yaml}` — with the **same fields and
identical validation** (mixing `config` with inline fields is a loud
error). Every source example below drops into either place unchanged.

Library embedders build the same objects directly:
`PostgresSource::from_yaml / from_json / from_value` and
`Postgres::connect(conn).dataset("mirror").options(PgDestOptions {…})`.
All entry points share one validation path, and the connector's declared
`config_schema` is **generated from the config structs** (schemars), so
platform-side validation and the parser cannot drift.

---

## Pipeline spec (CLI)

The `rdlt` CLI runs one pipeline per YAML file. Top-level fields:

| Field | Type / values | Default | Description |
|---|---|---|---|
| `pipeline` | string | required | Pipeline id — names engine state; keep it stable across runs (cursors and resume state key on it). |
| `workdir` | path | `.rdlt` | Engine working directory (WAL, state). |
| `write_mode` | `append` \| `replace` \| `{merge: {key: [...]}}` | `append` | Write disposition for every stream. `append` adds rows; `replace` truncates once per load then loads; `merge` converges to one row per key — required for the upsert/scd2 strategies, cursor-lag exact totals, and the CDC composition. |
| `source.postgres` | inline document, or `{config: path}` | required | The source document — see the full reference below. |
| `destination.postgres` | inline fields | required | Connection + options — see the full reference below. |

(Other connectors — `source.rest`, `source.file`, `destination.duckdb`,
`destination.parquet` — take their own blocks; this README covers
postgres.)

---

## Source configuration — full reference

One YAML document, two carriers with identical fields and identical
validation: inline under `source: postgres:` in the pipeline file, or a
standalone YAML/JSON file via `source: postgres: {config: path}`.
Unknown fields are errors everywhere (schema AND parser). Validation
failures name the offending field/table/column.

### Top level

| Field | Type / values | Default | Description |
|---|---|---|---|
| `conn` | string | required | libpq-style connection string or URL (`host=… user=…` or `postgresql://…`). Parse failures are typed config errors up front, never retried. See **Connection strings** below for which parameters are honored. |
| `schema` | string | `public` | Reflection scope. All bare table names below resolve inside it; schema-qualified names in `tables` are rejected. |
| `include_views` | bool | `false` | Include views and materialized views in schema-wide discovery. A view listed by name under `tables:` is always included regardless. |
| `tables` | list of table entries | absent | Omit to discover **every** table in `schema`. Present-but-empty is an error. Discovery excludes partition leaves and `INHERITS` children (rows arrive once via the parent — list a child explicitly to read it alone) and never discovers foreign tables. |
| `queries` | list of query streams | `[]` | Custom SQL as streams; see **Query streams**. |
| `tls` | TLS block | absent (= `prefer`) | TLS posture; see **TLS**. `verify_ca`/`verify_full` are only expressible here or via conn-string `sslmode`. Contradicting an explicit conn `sslmode` is a typed error naming both. |
| `cdc` | CDC block | absent | Log-based capture for every configured table; see **CDC**. Mutually exclusive with any table's `cursor` (typed, names the table). |
| `batch_target_bytes` | int > 0 | `8388608` (8 MiB) | The decoder cuts an Arrow batch when this many bytes are buffered. |
| `batch_max_rows` | int > 0 | `65536` | Secondary cut: maximum rows per batch. |

### Table entry (`tables[]`)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Bare table name (no schema qualifier). Listed twice = error. |
| `cursor` | cursor block | absent | Incremental loading; absent = snapshot stream (full re-read every run — pair with the pipeline's `replace` write mode for mirrors). |
| `primary_key` | [string] | reflected PK | Overrides the reflected primary key — the dedup/merge identity. Present-but-empty is an error. Under CDC with `REPLICA IDENTITY FULL` the override also wins as the merge key; under default/index identity a mismatching override is a typed error (delete records only carry the identity columns). |
| `included_columns` | [string] | all | Load only these columns. Mutually exclusive with `excluded_columns`; empty list is an error. |
| `excluded_columns` | [string] | none | Load all but these columns. |
| `type_hints` | map column → hint | `{}` | Per-column overrides from a **closed** conversion table; see **Type hints**. Unknown columns or undefined (source → hint) pairs fail typed at open. |

### Cursor block (`tables[].cursor`)

Incremental loading with dlt-parity boundary semantics plus mid-table
checkpointed resume: a crash, cancel, or transient failure resumes from
the last **committed mid-table checkpoint**, not the top of the table.
The saved watermark never regresses.

| Field | Type / values | Default | Description |
|---|---|---|---|
| `column` | string | required | Must exist in the selection and map to a cursor-capable type (ints, decimals, text, uuid, timestamps, date, time). Validated at open, before any data moves. |
| `initial_value` | typed literal string | absent | First-run lower bound (e.g. `"2026-01-01T00:00:00Z"`, `"1000"`). Absent = full initial load. |
| `boundary` | `closed` \| `open` | `closed` | Resume semantics. `closed` re-fetches watermark-equal rows and dedups them source-side by primary key (or whole-row hash when the table has no PK) — safe for non-unique cursors like timestamps. `open` uses strict `>` with no dedup — only for strictly monotonic cursors (sequences). |
| `direction` | `max` \| `min` | `max` | Ascending (watermark = max seen) or descending. |
| `end_value` | typed literal string | absent | Optional upper bound — a read filter only, never resume state. |
| `end_bound` | `exclusive` \| `inclusive` | `exclusive` | Upper-bound semantics: `inclusive` makes `[start, end]` directly expressible. |
| `nulls` | `exclude` \| `include` \| `error` | `exclude` | NULL-cursor rows: filtered out, included on **every** run, or a typed data-contract failure naming stream + column (for pipelines where NULL `updated_at` is a bug). |
| `lag` | duration or magnitude string | absent | Attribution window: every **resumed** run widens the read window this far behind the watermark, capturing late-committed rows. Durations (`"90s"`, `"5m"`, `"2h"`, `"1d"`) for time cursors (whole days for `date`); plain magnitudes (`"1000"`, `"0.5"`) for numeric cursors. Requires a `closed` boundary and a primary key; pair with the merge write mode for exact totals (under append the window re-delivers each run — documented at-least-once). |

### Query streams (`queries[]`)

A stream per SQL statement. The schema is **described by the server**
(no inference), and the statement always executes as
`SELECT * FROM (sql) AS q` — subquery rules enforce read-only. Full
incremental support.

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Stream name; must be unique across tables AND queries. |
| `sql` | string | required | The SELECT/CTE text. |
| `cursor` | cursor block | absent | Same semantics as table cursors, applied to the wrapped query. |
| `primary_key` | [string] | none | Declared key (nothing to reflect): drives dedup and merge. |
| `type_hints` | map | `{}` | Same closed vocabulary as tables. |

### Type hints (`type_hints`)

String vocabulary (also enforced by the generated schema):
`bool`, `int64`, `float64`, `decimal(p,s)`, `utf8`, `binary`,
`timestamp_tz`, `timestamp_naive`, `date`, `time`, `uuid`, `json`.

The conversion table is **closed**: only documented (source type → hint)
pairs work — e.g. unconstrained `numeric` → `decimal(12,4)` for real
decimality, or a text column → `timestamp_tz` via a strict server-side
cast. Anything else is a typed error at open. Hints may change
cursor-capability (checked post-hint).

### CDC block (`cdc`)

Log-based capture via logical replication (built-in `pgoutput` — no
third-party server plugins). When present, **every configured table** is
captured through the slot; `cursor` on any of them is a typed error.
Query streams are unaffected.

| Field | Type / values | Default | Description |
|---|---|---|---|
| `slot` | string | required | Replication slot name. Single consumer: a slot actively held elsewhere is a typed error naming the pid. |
| `publication` | string | required | Must cover every CDC table (preflighted; gaps are typed errors naming the missing tables — rdlt creates but never alters publications). |
| `create_if_missing` | bool | `false` | Create slot and publication idempotently when absent. rdlt **never drops** either. |
| `mode` | `catchup` \| `tail` | `catchup` | `catchup`: consume the backlog to the run-start WAL position, then finish (cron-able). `tail`: chunked catch-up loop until cancelled, checkpointing per chunk. |
| `idle_wait` | duration string | `"1s"` | Tail-mode quiet wait between chunks (`"1s"`, `"5m"`, `"2h"`, `"1d"` — durations only). |
| `flag_column` | string | `_rdlt_deleted` | Deletion-flag column added to every CDC stream: NULL for insert/update rows, TRUE for deletes. Colliding with an existing column is a typed error. |
| `ack` | `auto` \| `off` | `auto` | `off` never advances the slot (debugging / fan-in staging) — the server retains WAL indefinitely. |

Semantics and requirements, briefly:

- **First run**: slot first, then ONE `REPEATABLE READ` snapshot of all
  CDC tables (cross-table consistent). The slot-to-snapshot window
  applies twice and converges — a row changed inside it appears exactly
  once with its final state.
- **Later runs**: per-table passes over a peeked WAL range; checkpoints
  land only at transaction-commit positions; PK-changing updates emit
  delete(old)+insert(new) in order.
- **Acks are conservative**: the slot's confirmed position only ever
  advances to destination-committed cursors — one run behind, hygiene
  never correctness. Long-lived tails therefore accumulate WAL retention
  (a warning fires past 256 MiB): cycle tail runs, or use catch-up on a
  cron for durable long-lived pipelines.
- **Requirements**: `wal_level = logical`; each table needs a usable
  replica identity — a primary key (default identity), `REPLICA IDENTITY
  FULL`, or `USING INDEX` — else a typed error names the table and the
  fix. Unchanged TOAST values substitute under `FULL` and fail typed
  (naming column + the `ALTER`) without it. `TRUNCATE` on a published
  table is a typed error with the recovery spelled out.
- **Recommended composition** (the CLI warns when absent):
  `write_mode: {merge: {key: […]}}` + destination `merge_strategy: upsert`
  + `hard_delete: <flag_column>` — deleted rows then actually disappear
  at the destination. Without hard-delete support the flag lands as data
  (documented soft delete).
- **Observability**: replication lag (`lag_bytes`, plus `lag_seconds`
  when the server has `track_commit_timestamp = on`) is emitted per
  completed run as a structured event on the `rdlt::cdc` tracing target.

### TLS (`tls`)

```yaml
tls:
  mode: verify_full          # disable | prefer | require | verify_ca | verify_full
  root_cert: /etc/ca.pem     # path or inline PEM; omit = platform trust store
  client_cert: /etc/c.pem    # mutual TLS: both-or-neither with…
  client_key: /etc/c.key     # …an unencrypted PKCS#8/RSA/SEC1 private key
```

| Mode | Encrypted | Chain verified | Hostname verified |
|---|---|---|---|
| `disable` | no | — | — |
| `prefer` (default) | when the server offers | no | no |
| `require` | always | **no** (libpq semantics) | no |
| `verify_ca` | always | yes | no |
| `verify_full` | always | yes | yes — **the production recommendation** |

Server rejection of the client credential is the distinguished
`ClientCert` failure, separate from our verification of the server. The
destination takes the same policy (`Postgres::tls(...)` / CLI
`tls = {...}`) through the same code path.

### Connection strings

Existing libpq URLs just work: `sslmode=verify-ca|verify-full`,
`sslrootcert=` (`system` selects the platform store), and
`sslcert=`/`sslkey=` translate into the TLS policy. A conn parameter and
a `tls:` field that disagree fail typed naming both. Any unsupported
parameter is rejected **by name** — never a bare parse error. libpq's
implicit `~/.postgresql/*` file defaults are NOT emulated. Every
connection carries `application_name = rdlt` unless the conn string sets
its own.

### Type mapping (source → engine)

Lossless scalar mappings: `bool`, `int2/4/8` → int64, `float4/8` →
float64, `numeric(p≤38,s)` → decimal(p,s), text family → utf8, `bytea` →
binary, `timestamp/timestamptz` (µs) with `±infinity` saturating
visibly, `date`, `time`, `uuid` → canonical text, `json`/`jsonb` → JSON
text. Unconstrained or >38-digit numerics arrive as **lossless text**,
never truncated. Everything else (arrays, enums, composites, ranges,
`interval`, `inet`, `money`, `xml`, …) arrives via a documented-lossy
textual/JSON conversion — and every documented-lossy column announces
itself once per read as a structured `tracing::warn!` on the
`rdlt::lossy` target, so representation changes are visible without
log-scraping.

---

## Destination configuration — full reference

Builder: `Postgres::connect(conn).dataset(schema).tls(policy).options(options)`.
CLI YAML: `destination: postgres:` with the connection fields below,
plus `merge_strategy` and per-table blocks under `tables:`. Options are
validated at construction (`options()` returns the error) — and again at
open against the live stream schema.

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `conn` | string | required | libpq-style connection string or URL — same parsing, portability rules, and typed rejections as the source (`application_name = rdlt` unless the string sets its own). |
| `dataset` | string | `public` | Target schema; created if missing. Engine bookkeeping tables (`_rdlt_state`, `_rdlt_commits`) and per-pipeline staging tables live here too. |
| `tls` | TLS block | absent (= `prefer`) | The SAME policy type and code path as the source — see **TLS** in the source reference. `tls: {mode: verify_full, root_cert: /ca.pem}`. |

Native types need **zero configuration**: decimals land as
`numeric(p,s)`, JSON as `jsonb`, UUIDs as `uuid`, required columns
`NOT NULL`. Values ride binary COPY end to end. Schema migrations are
additive (new columns via `ADD COLUMN`, widenings via `USING` casts).
Every destination error carries the server's message + SQLSTATE, and
COPY data errors name the failing column.

### Destination-wide options (`PgDestOptions`)

| Field | Type / values | Default | Description |
|---|---|---|---|
| `merge_strategy` | `delete_insert` \| `upsert` \| `scd2` | `delete_insert` | How the merge write mode executes, for every table unless overridden. EXPLICITLY configuring it (destination-wide or per-table) under an append/replace write mode is a typed error — the unconfigured default never rejects. |
| `tables` | map table → per-table options | `{}` | Per-table overrides below. |

The three strategies (they only apply under the pipeline's **merge**
write mode; append/replace are engine dispositions, not strategies):

- **`delete_insert`** — atomic delete-then-insert by the merge identity,
  inside one transaction. The default; the only strategy valid for
  shredded (JSON) streams, where it replaces whole subtrees by root id.
- **`upsert`** — `INSERT … ON CONFLICT DO UPDATE`: matched keys update
  in place with no delete-visibility window. Requires a keyed structured
  stream (typed error otherwise — a shredded stream's identity is a
  content hash and conflicts would never fire). The unique index it
  needs is auto-ensured; pre-existing duplicate keys fail typed naming
  the columns.
- **`scd2`** — full version history: validity columns on the target,
  change detection via `IS DISTINCT FROM` (excluding bookkeeping
  columns), one boundary timestamp per commit unit, redelivery-stable.

### Per-table options (`tables.<name>`)

| Field | Type | Default | Description |
|---|---|---|---|
| `merge_strategy` | strategy | destination-wide value | Per-table override. |
| `hard_delete` | column name | absent | CDC-style deletion flag: rows whose flag fires **delete their key** instead of merging (boolean columns compare `IS TRUE`, other types `IS NOT NULL`). The surviving in-load version's flag decides. Root tables only (typed error on children); the column must exist; not valid with scd2. |
| `dedup_sort` | `{ column, order: asc\|desc }` | absent (= last-wins) | **Ordered in-load survivor selection**: when one load carries several versions of the same key, the version this column ranks first survives — `desc` = greatest wins, `asc` = least wins — instead of arrival order. Values beat NULL; ties (and all-NULL groups) keep the deterministic arrival-order last-wins. The survivor drives every downstream decision (hard-delete flag, upsert content, SCD2 change detection). `order` is required. Typed errors: nonexistent column, the hard_delete flag, a merge-key column (constant per group — could never order), shredded streams, non-merge write modes. |
| `merge_key` | [column] | absent | **Scope replacement**: a non-unique column set, independent of the row identity. A merge load deletes every target row whose scope appears among the delivered rows, then applies the batch — undelivered rows in delivered scopes disappear; untouched scopes stay. NULL is not a scope (matches nothing, both sides). Scope columns are auto-indexed. The scoped **table's** feed must arrive in one commit unit — per-table, so other streams' checkpoints never trigger it; a split feed is a typed error advising the engine commit thresholds (recovery converges on re-run). One recorded caveat: scoped streams should checkpoint only at feed end (a mid-feed checkpoint plus a crash in the window resumes as a partial feed the destination cannot distinguish from a fresh load). Typed errors: nonexistent columns, the hard_delete flag, shredded streams, scd2, non-merge write modes. |
| `scd2` | scd2 block | defaults | See below; only valid with `merge_strategy: scd2` (typed both ways). |

Worked example:

```yaml
destination:
  postgres:
    conn: "host=warehouse user=loader password=… dbname=analytics"
    dataset: mirror
    merge_strategy: upsert
    tables:
      orders:
        hard_delete: _rdlt_deleted
        dedup_sort: {column: seq, order: desc}
        merge_key: [day]
      customers:
        merge_strategy: scd2
        scd2: {absent: retire}
```

### SCD2 block (`tables.<name>.scd2`)

| Field | Type / values | Default | Description |
|---|---|---|---|
| `valid_from` | column name | `_rdlt_valid_from` | Validity-start column added to the target (`TIMESTAMPTZ NOT NULL`). |
| `valid_to` | column name | `_rdlt_valid_to` | Validity-end column; `NULL` marks the active version. Must differ from `valid_from`; neither may collide with a stream column. |
| `absent` | `keep` \| `retire` | `keep` | Active keys **absent** from a load: `keep` leaves them active (incremental feeds are partial); `retire` closes them at the boundary (full-feed semantics). Retire requires the table's full feed in a single commit unit — same per-table rule as `merge_key`, same typed error, same thresholds remedy. |

### Indexes

Merge identities get supporting indexes automatically, with
deterministic names (`rdlt_ix_*` / unique `rdlt_ux_*`): the identity
index per strategy (unique for upsert), `(key…, valid_to)` for scd2
active-version lookups, and the scope columns for `merge_key`. Measured
where it matters: 20.4× on the incremental-regime merge DELETE
(`benches/RESULTS.md`).

---

## Operational semantics worth knowing

- **Crash discipline**: exactly-once outcomes under kill/panic at every
  registered fail point (source, destination, and CDC), both occurrence
  passes, verified by sweeps with armed-fire pins. Commits are
  idempotent by `(load_id, commit_seq)`; state travels in the same
  transaction as data.
- **Error posture**: sources classify transient (connection-shaped —
  the engine retries with backoff and resumes from committed state) vs
  fatal (config/auth/data-shaped — retrying cannot fix it); the
  destination distinguishes the same way, always carrying SQLSTATE.
  Misconfiguration fails at parse or open, before any data moves.
- **Memory is bounded** by the batch knobs regardless of table or
  transaction size (a 6.9 GB table streams through a 256 MiB process
  ceiling in the test suite).
- **Merge write mode** requires a declared `primary_key` for structured
  streams (keyless structured streams reject merge at plan time);
  shredded streams merge by content identity with subtree replacement.

## Verification

`cargo nextest run -p rdlt-postgres` — destination: native-type
fidelity, strategy conformance, SCD2 history, merge-refinement matrices;
source: conformance (full type-matrix round-trip against real Postgres),
incremental boundary semantics, differential property test (decoder ≡ an
independent driver reference), drift matrix, TLS matrix (five sslmode
levels × cert scenarios, both directions), query streams, config schema
round-trips, CDC (equality cycle, boundary overlap, ack pin, tail, TOAST
+ identity matrix, lag capture). `--features failpoints` adds the crash
sweeps and the memory-ceiling test (`RDLT_HEAVY=1` makes missing
prerequisites a hard failure). Fuzz targets: `pg_copy_decode`,
`pg_pgoutput_decode`. Scoreboards and gated bars: `benches/RESULTS.md`.
