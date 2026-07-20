# Research: Postgres SQL Source Connector

**Feature**: 005-postgres-source | **Date**: 2026-07-20

Inputs: spec.md; the committed dlt `sql_database` review (2026-07-20,
summarized in spec preamble); current SPI surface (`rdlt-connector`
v0.2.0); workspace dependency set.

## R1 — Extraction mechanism: binary COPY, decoded straight into Arrow

**Decision**: data path = `COPY (SELECT …) TO STDOUT (FORMAT BINARY)`
over `tokio_postgres::Client::copy_out`, parsed by an owned decoder
(`copy_decode.rs`) that appends directly into Arrow array builders and
emits `PushPayload::Arrow(RecordBatch)`. Control path (reflection,
version probe) = ordinary parameterized queries on the same client.

**Rationale**:
- The dlt review's central lesson: every dlt backend still pulls rows
  one-by-one through Python; its documented 20–30× "pyarrow backend"
  speedup is a normalizer skip, not fast extraction. Decoding the wire
  bytes column-wise into Arrow is the lever dlt only reaches via
  connectorx (a Rust reader) — and connectorx's recorded quirks (lost
  nullability, decimal precision loss, JSON double-wrapping, temporal
  fixups) come from NOT owning the type mapping. We own both ends.
- COPY BINARY is the fastest bulk-read surface Postgres offers a
  client; it streams (backpressure-friendly), and the binary tuple
  format is stable, documented, and self-describing per field (length-
  prefixed, NULL = -1) — a clean fit for a safe, allocation-conscious
  Rust decoder.
- A single COPY statement runs under one statement-level snapshot —
  per-table consistency (spec FR-005) falls out for free, without
  holding a REPEATABLE READ transaction across batches.
- Symmetry: `rdlt-dest-postgres` already writes binary COPY; the wire
  format knowledge lives in this workspace already.

**Alternatives considered**:
- `query_raw` row streaming (extended protocol + `FromSql`): row-object
  materialization per row — the dlt-shaped slow path. KEPT as the
  reference side of the differential test (R8), rejected as data path.
- `sqlx`: brings its own pool/TLS/runtime abstractions we don't need;
  no COPY-binary advantage; heavier dependency. Rejected.
- Embedding connectorx: it IS the thing we're building, minus control
  over type mapping; contradicts the no-correctness-debt goal. Rejected.
- Cursor/portal paging (`DECLARE CURSOR` + `FETCH n`): more round
  trips, text or per-row binary decode, no snapshot advantage over a
  single COPY. Rejected for data path.

**TLS** (corrected at implement time): the workspace's Postgres
DESTINATION connects `NoTls` — that is the current house posture for
the driver, and the source follows it rather than growing a one-sided
`rustls` dependency. `sslmode=require`/`verify-*` in the conn string →
typed Fatal config error stating TLS is not yet wired. TLS for BOTH
postgres connectors is a recorded backlog item (an ops requirement for
the platform, best added once, symmetrically).

## R2 — Type mapping: OID → LogicalType, lossy rules explicit

**Decision**: reflected `pg_type` OIDs map to the existing
`LogicalType` lattice (`rdlt-core/src/types.rs`) per the contract table
in `contracts/type-mapping.md`. Headline rules:

| Postgres | LogicalType | Note |
|---|---|---|
| bool | Bool | |
| int2/int4/int8 | Int64 | widened, lossless |
| float4/float8 | Float64 | widened, lossless |
| numeric(p≤38,s) | Decimal{p,s} | arrow decimal128 |
| numeric (unconstrained or p>38) | Utf8 | lossless text, documented lossy-of-type |
| text/varchar/char/citext/name | Utf8 | |
| bytea | Binary | |
| timestamptz | TimestampTz | µs; PG epoch rebased |
| timestamp | TimestampNaive | µs |
| date | Date | |
| time | Time | timetz → Utf8 (no tz-time type) |
| uuid | Utf8 | canonical text (structured-path constraint, see contract) |
| json/jsonb | Utf8 (canonical JSON text) | opaque, NOT shredded; structured path derives logical from arrow, so the Json label is unreachable (see contract note) |
| enum types | Utf8 | label text |
| arrays, composites, ranges | Utf8 (canonical JSON text) | server-side to_jsonb()::text, documented |
| domains | base type's mapping | |
| inet/cidr/macaddr/money/interval/other | Utf8 | canonical text; explicit "textual fallback" list |
| ±infinity timestamps/dates | saturate to the type's min/max representable instant, documented | never NULL-ed, never an error — value survives visibly |

**Rationale**: the lattice already has everything needed (Decimal,
Binary, TimestampNaive, Json as "top"); Json-as-escape-hatch matches
its documented intent ("undecomposable values are preserved verbatim").
No silent corruption: every non-obvious cell above is a documented,
tested rule; nothing falls through to inference.

**Alternatives considered**: shredding JSONB by default (rejected —
typed sources publish declared schemas; shredding is inference-world
behavior and would make the same column shape-dependent); mapping
unconstrained numeric to Float64 (rejected — silent precision loss);
erroring on infinity timestamps (rejected — real datasets contain
them; saturation is visible and documented).

## R3 — Reflection: pg_catalog, once per run

**Decision**: one catalog round-trip per run against `pg_class` /
`pg_attribute` / `pg_type` / `pg_namespace` / `pg_constraint`
(`contype='p'`), filtered by schema + relkind (`r`,`p` always; `v`,`m`
when `include_views`). Captures: column order, name, type OID,
`atttypmod` (numeric precision/scale, varchar length), NOT NULL,
primary key columns. Explicit table lists validate against the
reflected set (unknown table = typed config error).

**Rationale**: information_schema is slower and hides OIDs; pg_catalog
is stable across supported PG versions (13+) for these columns. One
round trip satisfies spec US1-AS2 (discovery cost bounded).

**Alternatives**: information_schema (rejected: no OIDs, slower);
per-table `SELECT … LIMIT 0` describe (rejected: N round trips, no PK/
nullability).

## R4 — Batching & memory: byte-targeted Arrow batches under the engine budget

**Decision**: the decoder accumulates rows into Arrow builders and cuts
a `RecordBatch` at `batch_target_bytes` (default 8 MiB, the engine slab
unit) or `batch_max_rows` (default 65 536), whichever first; each push
awaits `RecordsOut` (byte-budgeted semaphore) — that await IS the
backpressure that ultimately throttles the socket read. No prefetch
beyond one in-flight batch plus the OS socket buffer.

**Rationale**: matches the engine's existing flow-control design
(bounded in bytes, clause S5); memory bound = builders (≤ target) +
budget (engine-owned), independent of table size (spec SC-002).

## R5 — Incremental: dlt-parity semantics, watermark + boundary keys in one Cursor

**Decision**:
- Query shape: cursor predicate pushed into the COPY subselect. COPY
  does not accept bind parameters, so `sqlgen.rs` renders **typed
  literals with explicit casts** (e.g. `'2026-07-20T12:00:00Z'::timestamptz`,
  `42::int8`) from the strongly-typed watermark — never raw user
  strings; identifiers are strictly quoted; cursor column must exist in
  the reflected schema (validated) — injection-safe by construction.
- Semantics matrix (dlt parity, from the committed review): closed `>=`
  (default) / open `>` lower bound; optional `end_value` upper bound
  (`<` / `<=` mirrored); direction max (default) / min; NULL cursor
  include/exclude (`IS NULL` union / `IS NOT NULL` filter), recorded in
  the run report.
- State: one engine `Cursor` (JSON) per stream:
  `{ watermark: <typed value>, boundary_keys: [<pk-or-row-hash>…] }`
  where `boundary_keys` are the keys of rows whose cursor equals the
  watermark (bounded: only max-valued rows). On resume with the closed
  default, re-fetched boundary rows whose key is in the set are
  dropped source-side — exactly dlt's dedup guarantee without engine
  changes. Open boundary skips the set entirely.
- **Cursor-ordered reads + mid-table resume** (decided at implement
  time, upgrading the original R5): incremental queries emit
  `ORDER BY <cursor> ASC|DESC` (direction-aligned). Ordering makes
  clause S2 checkpoints legal MID-STREAM: whenever the cursor value
  changes, every row of the previous value is complete, so the source
  emits `Checkpoint({watermark: prev_value, boundary_keys: []})` at
  batch boundaries (empty keys ⇒ strict `>` resume — no dedup needed).
  The FINAL checkpoint carries `boundary_keys` for watermark-equal
  rows (closed-boundary next-run dedup). Result: a crash or transient
  retry resumes from the last committed mid-table checkpoint instead
  of the table start — the "no mid-table resume" dlt gap, closed. The
  unindexed-cursor ORDER BY cost is the documented caveat the spec
  already carries. Snapshot (cursor-less) streams stay unordered and
  never checkpoint — each run is a full read by definition.
- Watermark advances only through `PushPayload::Checkpoint` after the
  rows it covers are pushed (S2); the engine persists it only on
  destination commit (E6) — crash convergence (FR-007) rides the
  existing machinery. A candidate watermark lower than the stored
  one is never emitted (monotonicity guard; regressing clocks test).

**Alternatives**: engine-side dedup (rejected: SPI/engine change for a
source-local concern); storing full boundary rows like dlt's row hashes
for PK-less tables — ADOPTED in reduced form: PK if reflected, else
canonical row hash; hashing already exists in the engine's identity
vocabulary.

## R6 — Failure policy: classify, never retry (SPI clause S3) — CORRECTED at implement time

**Correction (2026-07-20, before any code)**: the original R6 proposed
source-side bounded connect retries. That contradicts SPI clause S3
("never retry internally — classify and return; retries are
engine-owned", `connector-spi.md`). The corrected decision:

- The source classifies every failure as a typed `SourceError` naming
  table + phase (connect / reflect / copy / decode / checkpoint):
  network and connection errors → `Transient` (engine retries with
  backoff + jitter, clause E5, counts surfaced in `RunReport`);
  auth/config/unknown-table/cursor-type errors → `Fatal`; decode/drift
  errors → `Fatal` (data problems must not be retried into).
- Mid-stream double-apply is impossible BY the engine contract, not by
  refusing retry: a retried `read` receives only the last
  destination-committed cursor (E6), sources must not re-emit covered
  rows (S1), and staging keeps uncommitted pushes invisible (D1–D4).
  Engine-owned retry is therefore SAFE mid-stream — strictly better
  than the plan's original "fail hard, never retry".
- Cancellation unchanged: `ChannelClosed` on push → return promptly
  (S4); dropping the connection aborts the server-side COPY.
- The config `retry` block is REMOVED (contract updated) — retry
  policy is not a source concern in this architecture.

**Rationale**: seams sacred — the engine already owns retry/backoff and
the crash model that makes it safe; duplicating it source-side was the
one place the plan drifted from the house architecture. Still beats
dlt (which has no retry at all and documents DIY subclassing with no
double-apply guard).

## R7 — Benchmark design: two cells, dlt fastest-config gated, measurement-first bars

**Decision**:
- Datasets (seeded deterministically by `benches/baseline/seed_pg.*`
  into postgres:16, identity = row count + content hash of the seed
  stream, recorded per 004 R7): **pg-wide** — 1 M rows × 12 typed
  columns (ints, numerics, timestamps, uuid, text, bool, one nullable);
  **pg-jsonb** — 200 k rows with the existing nested-generator document
  in a jsonb column (exercises the Json escape-hatch path end-to-end).
- Cells: postgres→DuckDB (pg-wide gated; pg-jsonb scoreboard context)
  and postgres→Postgres (pg-wide gated). Baseline measured FIRST, same
  session, in-process self-timing (unchanged 003 discipline): dlt
  `sql_database` **backend="pyarrow"** = the gated baseline (its
  fastest documented pure-Python-orchestrated config);
  backend="sqlalchemy" (dlt default) and backend="connectorx" =
  scoreboard rows (context: the latter is another Rust reader — beating
  it is context, not a gate).
- Bars: set AFTER both sides are measured, with explicit headroom, via
  version-policy entries linking evidence (the 004 protocol, now the
  house rule). No number in this plan is a bar.
- Gate: new iai bench `pg_copy_decode_10k` (canned COPY-binary bytes →
  Arrow, no network) added to the armed gate; its baseline is recorded
  at feature close in a commit naming this feature (P5-compliant NEW
  baseline, not a drift re-record).
- dlt pin: existing matrix pin (1.29.0) unless its own policy event
  bumps it first; recorded in the cells.

## R8 — Test strategy: conformance, differential, sweep, memory ceiling

**Decision**:
- **Conformance** (testcontainers postgres:16): full R2 type matrix
  round-trip (seed typed rows → pipeline → assert values/types in
  DuckDB), selection modes (list / schema / views), quoted identifiers,
  non-default schema, empty tables, drift cases (drop/rename between
  reflect and read → typed error or policy).
- **Differential property test**: proptest-generated typed row sets →
  the same SELECT read via (a) `copy_decode` and (b) driver `FromSql`
  rows built into Arrow the slow way; assert byte-identical batches.
  This is the decoder's correctness net (correctness before speed).
- **Crash sweep**: new fail points (`pg_after_reflect`,
  `pg_mid_copy_stream`, `pg_before_checkpoint`, `pg_after_batch_push`)
  registered in the 003 registry; sweep suite runs first- AND
  second-occurrence passes (the 003 lesson); plus a real
  connection-drop test (kill the container mid-table) asserting typed
  error + convergent re-run.
- **Memory ceiling** (SC-002): integration test seeds a table ≥ 10× the
  enforced ceiling, runs the CLI as a subprocess under
  `prlimit --as=<ceiling>` (util-linux, present on CI/reference
  machines; test self-skips with a visible note if absent), asserts
  success + row-count equality. No unsafe setrlimit in-process.
- **Mutation/fuzz**: `copy_decode` joins the mutation surface; a fuzz
  target feeds arbitrary bytes to the decoder (must never panic —
  typed decode errors only), joining the existing fuzz suite.

## R9 — Crate & surface layout

**Decision**: new crate `crates/rdlt-source-postgres` mirroring
`rdlt-source-rest`'s shape (YAML config → typed struct → `Source`
impl); facade export `rdlt::postgres_source::PostgresSource`
(destination stays `rdlt::postgres`); CLI gains
`SourceSpec::Postgres { config: PathBuf }`. No engine or SPI change;
`StreamSpec.cursor_field` + `ReadRequest.since` + `PushPayload::
{Arrow, Checkpoint}` already carry everything (verified against
current sources). Version stays 0.2.x (additive).

**Alternatives**: folding into `rdlt-dest-postgres` as one
`rdlt-postgres` crate (rejected: source and destination have disjoint
dependency needs and release cadences; the workspace's
one-connector-one-crate convention stands).
