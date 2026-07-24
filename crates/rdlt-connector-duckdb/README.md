# rdlt-connector-duckdb

DuckDB destination: Arrow-native ingestion into an embedded DuckDB file —
struct-preserving lowering (real `STRUCT`/`LIST` columns), native JSON,
temp-table staging, transactional commits that persist state atomically with
data, and the full destination-options vocabulary (merge strategies,
hard-delete, dedup ordering, scope replacement, SCD2) through the shared
merge core (`rdlt-connector-sqlcore`) — semantics, validation, and typed
errors identical to the postgres destination.

Builder: `DuckDb::open(path).memory_limit(…).setting(…).extension(…).options(…)`.
CLI YAML: `destination: duckdb:` with the fields below. Options are validated
at construction (`options()` returns the error) — and again at open against
the live stream schema.

```yaml
destination:
  duckdb:
    path: analytics.duckdb
    memory_limit: 512MB
    extensions: [httpfs]
    settings: {threads: "4"}
    merge_strategy: upsert
    tables:
      orders:
        hard_delete: _rdlt_deleted
        dedup_sort: {column: seq, order: desc}
        merge_scope: [day]
      customers:
        merge_strategy: scd2
        scd2: {absent: retire}
```

## Connection & runtime

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | file path | required | The DuckDB database file; created if missing. One shared database instance per destination — sessions clone connections from it (two independent opens of the same file are two instances that cannot see each other's un-checkpointed catalog). |
| `memory_limit` | size string (`"512MB"`) | DuckDB default (a fraction of system RAM) | Caps DuckDB's buffer/cache memory. DuckDB's default dominates pipeline RSS on large-memory machines; ingestion rarely needs it. |
| `extensions` | [name] | `[]` | Extensions to `LOAD` (bundled builds carry the core extensions statically — LOAD activates, no network install). Names must be bare identifiers (`[A-Za-z0-9_]`); anything else is a typed error, never interpolated. |
| `settings` | map key → value | `{}` | `SET key = 'value'` pairs (threads, temp_directory, TimeZone, …). Keys must be bare identifiers (typed error otherwise); values are escaped as literals. Applied eagerly at configuration (a bad key/value errors immediately) **and replayed on every session connection** — cloned connections are fresh DuckDB sessions that inherit nothing, so session-scoped settings work where the data is actually written. Unknown settings surface DuckDB's own error, typed. |

There is deliberately no `read_only` option — a destination writes.

Native types need **zero configuration**: decimals land as `DECIMAL(p,s)`,
JSON as native `JSON` (queryable with `json_extract` and friends), nested
objects as `STRUCT`, scalar arrays as `LIST`, timestamps as
`TIMESTAMP WITH TIME ZONE`. JSON values are staged as `VARCHAR` (the Arrow
appender's shape) and cast at publish — the cast validates the document.
Schema migrations are additive (`ADD COLUMN`; widenings via
`ALTER … SET DATA TYPE`, which migrates existing rows).

## Destination-wide options

| Field | Type / values | Default | Description |
|---|---|---|---|
| `merge_strategy` | `delete_insert` \| `upsert` \| `scd2` | `delete_insert` | How the merge write mode executes, for every table unless overridden. EXPLICITLY configuring it (destination-wide or per-table) under an append/replace write mode is a typed error — the unconfigured default never rejects. |
| `tables` | map table → per-table options | `{}` | Per-table overrides below. |

The three strategies (they only apply under the pipeline's **merge** write
mode; append/replace are engine dispositions, not strategies):

- **`delete_insert`** — atomic delete-then-insert by the merge identity,
  inside one transaction. The default; the only strategy valid for shredded
  (JSON) streams, where it replaces whole subtrees by root id.
- **`upsert`** — `INSERT … ON CONFLICT DO UPDATE`: matched keys update in
  place with no delete-visibility window. Requires a keyed structured stream
  (typed error otherwise — a shredded stream's identity is a content hash and
  conflicts would never fire). The unique index it needs (`rdlt_ux_*`, over a
  DuckDB ART index) is auto-ensured; pre-existing duplicate keys fail typed
  naming the columns (only genuine constraint violations get that diagnosis —
  locks/disk errors surface as themselves).
- **`scd2`** — full version history: validity columns on the target, change
  detection via `IS DISTINCT FROM` (excluding bookkeeping columns), one
  boundary timestamp per commit unit (`now()` is transaction-stable in
  DuckDB — probe-verified), redelivery-stable.

## Per-table options (`tables.<name>`)

| Field | Type | Default | Description |
|---|---|---|---|
| `merge_strategy` | strategy | destination-wide value | Per-table override. |
| `hard_delete` | column name | absent | CDC-style deletion flag: rows whose flag fires **delete their key** instead of merging (boolean columns compare `IS TRUE`, other types `IS NOT NULL`). The surviving in-load version's flag decides. Root tables only (typed error on children); the column must exist; not valid with scd2. |
| `dedup_sort` | `{ column, order: asc\|desc }` | absent (= last-wins) | **Ordered in-load survivor selection**: when one load carries several versions of the same key, the version this column ranks first survives — `desc` = greatest wins, `asc` = least wins — instead of arrival order. Values beat NULL; ties (and all-NULL groups) keep the deterministic arrival-order last-wins (stage `rowid` = append order). The survivor drives every downstream decision (hard-delete flag, upsert content, SCD2 change detection). `order` is required. Typed errors: nonexistent column, the hard_delete flag, a merge-key column (constant per group — could never order), shredded streams, non-merge write modes. |
| `merge_scope` | [column] | absent | **Scope replacement**: a non-unique column set, independent of the row identity. A merge load deletes every target row whose scope appears among the delivered rows, then applies the batch — undelivered rows in delivered scopes disappear; untouched scopes stay. NULL is not a scope (matches nothing, both sides). Scope columns are auto-indexed. The scoped **table's** feed must arrive in one commit unit — per-table, so other streams' checkpoints never trigger it; a split feed is a typed error advising the engine commit thresholds (recovery converges on re-run). With `merge_strategy: scd2` this instead **scopes retirement** (see the scd2 block — no scope delete runs; history is never destroyed). Typed errors: nonexistent columns, the hard_delete flag, shredded streams, scd2-without-retire, non-merge write modes. |
| `scd2` | scd2 block | defaults | See below; only valid with `merge_strategy: scd2` (typed both ways). |

## SCD2 block (`tables.<name>.scd2`)

| Field | Type / values | Default | Description |
|---|---|---|---|
| `valid_from` | column name | `_rdlt_valid_from` | Validity-start column added to the target (`TIMESTAMPTZ DEFAULT now()` — DuckDB rejects `ADD COLUMN … NOT NULL`, so the belt constraint postgres carries is omitted; every insert supplies the boundary explicitly, proven equivalent by the cross-destination differential suite). |
| `valid_to` | column name | `_rdlt_valid_to` | Validity-end column; `NULL` marks the active version (or the `active_record_timestamp` marker). Must differ from `valid_from`; neither may collide with a stream column. |
| `absent` | `keep` \| `retire` | `keep` | Active keys **absent** from a load: `keep` leaves them active (incremental feeds are partial); `retire` closes them at the boundary (full-feed semantics). Retire requires the table's full feed in a single commit unit — same per-table rule as `merge_scope`, same typed error, same thresholds remedy. With a `merge_scope` on the table, retirement is **scoped**: absent keys retire only within delivered scopes (requires `retire` — under `keep` the merge_scope would be inert, typed error). |
| `active_record_timestamp` | RFC3339 timestamp | absent (= NULL marker) | The OPEN-version marker written to `valid_to` instead of NULL (e.g. `9999-12-31T00:00:00Z` — some BI tools cannot range-query NULLs). Must be zone-explicit RFC3339 (zone-less literals resolve per session TimeZone — typed error) and must differ from `boundary_timestamp` (typed error). Active-version predicates treat NULL **and** the marker as open, so a table whose history predates the option keeps working; new versions open with the marker. |
| `boundary_timestamp` | RFC3339 timestamp | absent (= transaction timestamp) | Caller-supplied boundary used for close/open/retire instead of the transaction timestamp. Same zone-explicit validation; never interpolated unvalidated. |

## Capabilities

| Capability | Value | Notes |
|---|---|---|
| merge | yes | all three strategies + refinements (shared core) |
| structs / scalar lists | yes | native `STRUCT` / `LIST` columns, dot-queryable |
| json | yes | native DuckDB `JSON` (staged VARCHAR, cast at publish) |
| decimal | yes | `DECIMAL(p,s)` |

## Correctness protocol

Same D1–D5 clauses as every rdlt destination: staging in TEMP tables (they
die with the connection — dead sessions' staged rows are unreachable), one
transaction moving stage → target + state document + `(load_id, commit_seq)`
receipt, idempotent redelivery, durable once-per-load Replace truncation,
additive migrations. Strategy arms are crash-swept under the crate's fail
points with armed-fire pins (`tests/sweep.rs`); cross-destination behavior
is pinned by the differential oracle (`tests/differential.rs` — identical
feeds must produce identical canonical rows and history openness on
postgres and DuckDB).

## Verification records

- Traceability matrix + probe outcomes:
  `specs/013-duckdb-completeness/matrix.md`
- dlt parity record (source-grounded, per-option):
  `specs/013-duckdb-completeness/dlt-parity.md`
