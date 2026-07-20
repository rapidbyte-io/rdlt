# Feature Specification: Postgres Source Completeness — Parity + TLS

**Feature Branch**: `006-postgres-completeness`

**Created**: 2026-07-20

**Status**: Draft

**Input**: User description: "Postgres source completeness (006): close the remaining dlt sql_database parity gaps and ship TLS — full sslmode matrix (disable/prefer/require/verify-ca/verify-full) via rustls for the postgres source AND destination symmetrically; per-column type-hint overrides (parity with rest/file sources); custom SQL query streams with describe-based schema; merge/upsert for keyed structured streams (lifting engine clause B4 — the recorded 005 deviation); surface documented-lossy column mappings visibly; close the 005 review's test-quality advisories (differential multi-batch coverage, memory-test silent skip, container-kill determinism); emit real JSON config schemas from the config structs so ConnectorSpec's declared schema is generated, not aspirational"

## Parity baseline (the review this spec closes)

Feature 005 shipped the connector; this table is the measured gap
analysis against dlt's `sql_database` source (from the committed 005
code review of dlt), deciding what 006 must close, what stays
deliberately out, and where rdlt already leads:

| Capability | dlt `sql_database` | rdlt after 005 | 006 action |
|---|---|---|---|
| Table reflection, selection, views | ✓ | ✓ | — (parity) |
| Column include/exclude | ✓ | ✓ | — (parity) |
| Reflection depth | 3 levels, precision opt-in | always full precision | — (rdlt leads) |
| Incremental boundary semantics | ✓ | ✓ (+ mid-table resume dlt lacks) | — (rdlt leads) |
| Chunking / memory bounds | chunk_size | byte+row bounded, backpressure | — (rdlt leads) |
| Extraction speed | pyarrow/connectorx backends | 7.8× their fastest, 2.2× connectorx | — (rdlt leads) |
| Retry / resume / snapshot consistency | none | engine-owned, per-table snapshot | — (rdlt leads) |
| **TLS to the database** | ✓ (via drivers) | ✗ rejected with typed error | **US1** |
| **Per-column type-hint overrides** | `type_adapter_callback` | ✗ (rest/file have hints; pg does not) | **US2** |
| **Custom SQL per stream** | `query_adapter_callback` | ✗ (deferred in 005) | **US2** |
| **Merge/upsert write disposition** | ✓ | ✗ (engine clause B4 rejects for structured) | **US3** |
| Lossy-mapping visibility | n/a (implicit coercions) | flag computed, never surfaced | **US4** |
| Declared config schema | JSON-schema per source | claimed by SPI, hand-waved | **US4** |
| Custom aggregation cursors (`last_value_func`) | ✓ (disables pushdown) | ✗ | OUT — niche; full-scan semantics contradict rdlt's ordered-resume design; documented |
| Deferred reflection (Airflow DAG-build) | ✓ | ✗ | OUT — orchestrator-specific, no rdlt analog |
| FK → lineage hints | `resolve_foreign_keys` | ✗ | OUT — needs an SPI vocabulary for references; recorded backlog |
| Non-Postgres dialects | ✓ (sqlalchemy) | ✗ | OUT — dialect seam noted in 005; own feature |

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Encrypted connections, the full sslmode matrix (Priority: P1)

An operator points rdlt at a managed Postgres (RDS, Cloud SQL, Neon,
Supabase — all of which require or strongly default to TLS) and the
connection works with the same `sslmode` vocabulary every Postgres tool
uses: `disable`, `prefer`, `require` (encrypt, no identity check),
`verify-ca` (encrypt + certificate chain), `verify-full` (chain +
hostname). A custom root certificate can be supplied for private CAs.
The SAME behavior applies to the Postgres destination — one TLS story
for both connectors.

**Why this priority**: without TLS the connector cannot reach most
production databases at all — it is the single hardest functional
blocker to real use, and the 005 record explicitly promised it as the
top backlog item.

**Independent Test**: against a TLS-enabled Postgres with a self-signed
certificate: `disable` connects plaintext; `prefer` connects encrypted;
`require` connects encrypted without validating the certificate;
`verify-ca`/`verify-full` fail against the unknown CA and succeed once
the root certificate is supplied; `verify-full` additionally fails on a
hostname mismatch. Each mode's outcome is observable and typed.

**Acceptance Scenarios**:

1. **Given** a TLS-only server (`hostssl` required), **When** connecting
   with `sslmode=require`, **Then** the connection succeeds encrypted —
   and the 005-era "TLS not wired" rejection is gone.
2. **Given** `sslmode=verify-full` and a root certificate whose chain
   signs the server, **When** the hostname matches, **Then** the
   connection succeeds; **When** it does not match, **Then** a typed
   connect-phase error names the verification failure.
3. **Given** `sslmode=verify-ca`/`verify-full` with NO root configured,
   **When** connecting to a self-signed server, **Then** a typed error
   explains the missing trust anchor (system roots are used when
   present; the error says which was tried).
4. **Given** the Postgres DESTINATION with the same settings, **When**
   any scenario above runs, **Then** behavior is identical — verified by
   shared conformance cases.
5. **Given** `sslmode=prefer` (the default) against a server without
   TLS, **Then** the connection falls back to plaintext exactly as the
   standard tooling vocabulary promises.

---

### User Story 2 - Shape the extraction: type hints and query streams (Priority: P2)

A data engineer overrides how specific columns land — e.g. forcing a
`text` column that carries ISO timestamps to a timestamp type, or an
oversized numeric to text deliberately — using the same per-column
type-hint vocabulary the REST and file sources already accept. And for
cases no table selection covers (joins, projections, filtered subsets),
a stream can be an arbitrary SQL query whose result schema is
discovered automatically; query streams support the same cursor
configuration when the query's output contains the cursor column.

**Why this priority**: these are the two remaining capability gaps a
dlt user would actually miss (`type_adapter_callback`,
`query_adapter_callback`); together they make the connector cover the
"my schema isn't quite what the destination should see" long tail
without waiting for transformations.

**Independent Test**: a table with a text column of timestamps + a hint
lands as a timestamp column downstream; a query stream joining two
tables lands with the joined schema, correct types, and (when
configured) working incremental resume; an invalid query or a hint
naming a missing column fails with a typed config error before any data
moves.

**Acceptance Scenarios**:

1. **Given** a per-column type hint, **When** the pipeline runs, **Then**
   the column lands as the hinted type with a documented conversion rule,
   and an unconvertible value follows the pipeline's schema policy —
   never silent corruption.
2. **Given** a hint naming a column that does not exist, or a hint pair
   with no defined conversion, **Then** a typed config error at open.
3. **Given** a query stream, **When** streams are discovered, **Then**
   its schema (names, types, nullability where knowable) comes from the
   database's own description of the query, with the same type-mapping
   contract as tables.
4. **Given** a query stream with a cursor column present in its output,
   **When** runs repeat, **Then** incremental semantics (boundaries,
   dedup, mid-stream resume) match table streams; **Given** the cursor
   column is absent from the output, **Then** a typed config error.
5. **Given** a query stream, **Then** per-table consistency holds the
   same way it does for tables (one statement, one snapshot).

---

### User Story 3 - Upserts: merge write mode for keyed streams (Priority: P3)

A data engineer configures merge write mode on an incremental Postgres
stream with a primary key, and re-extracted rows (updates re-fetched by
the cursor) REPLACE their prior versions downstream instead of
appending duplicates — in both SQL destinations. This lifts the
recorded 005 deviation (engine clause B4: "Merge rejected for
structured streams") with a proper contract amendment: keyed structured
streams merge by their declared key; keyless structured streams still
reject merge with the existing typed plan-time error.

**Why this priority**: without upserts, cursor-incremental on tables
with updates produces duplicate rows in Append mode — the single
biggest semantic gap vs dlt for the core sync workflow. P3 only because
it is an engine + destinations change with contract ceremony, not a
source change.

**Independent Test**: seed, sync, update rows + advance their cursor,
sync again with merge configured: downstream row count equals the
source count, updated values win, unchanged rows intact — on both SQL
destinations; a keyless structured stream still gets the typed
rejection; a destination without merge capability still gets its typed
rejection.

**Acceptance Scenarios**:

1. **Given** a keyed structured stream in merge mode, **When** updated
   rows are re-fetched, **Then** downstream reflects exactly one row per
   key with the newest values (both SQL destinations).
2. **Given** a crash at any registered fail point during a merge load,
   **When** the pipeline re-runs, **Then** the destination converges to
   the same exactly-once merged state (crash sweep extended to merge
   mode).
3. **Given** a keyless structured stream configured for merge, **Then**
   the existing typed plan-time rejection stands, its message now
   pointing at the keyed alternative.
4. **Given** the contract documents, **Then** the B4 amendment is a
   recorded contract event (feature-002 contract updated by pointer,
   not silently rewritten), with capability declarations extended so
   destinations state merge-for-structured truthfully.

---

### User Story 4 - Trustworthy surfaces: visibility, schemas, test integrity (Priority: P4)

A platform (rapidbyte) and its operators can TRUST the connector's
surfaces: representation-changing type mappings are visible per run
(the [documented-lossy] promise made in the 005 contract, now honored);
every source's declared configuration schema is generated from the same
definition that parses the config (so UI forms, validation, and parsing
can never drift); and the 005 review's test-integrity advisories are
closed so the suites cannot silently weaken.

**Why this priority**: each item is small, but together they are the
difference between "works" and "operable" — and two of them
(schema-from-truth, lossy visibility) are prerequisites rapidbyte's
config UI and observability will build on.

**Independent Test**: a run over a table with policy-mapped columns
reports which columns changed representation and how, observably; each
source's declared schema validates its own documented example configs
and rejects its documented invalid ones; the three advisory tests fail
when their guarded regressions are injected.

**Acceptance Scenarios**:

1. **Given** a stream with [documented-lossy] columns, **When** a run
   executes, **Then** each such column and its rule is visible in the
   run's observable output exactly once (not per batch), and silent runs
   stay silent when nothing is lossy.
2. **Given** a source's declared config schema, **When** the documented
   example configs are validated against it, **Then** they pass — and
   configs with unknown fields or contract-violating shapes fail — for
   all three sources (postgres, rest, file).
3. **Given** the differential suite, **Then** multi-batch and
   chunk-boundary decode paths are differentially compared (not just
   single-batch); **Given** the memory-ceiling test's prerequisites are
   missing, **Then** the suite FAILS with instructions (in the
   environment that is supposed to run it) rather than skipping
   silently; **Given** the container-kill test, **Then** its integrity
   assertion cannot be skipped by a racy zero-commit run.

---

### Edge Cases

- TLS: server certificate expires mid-stream (long extraction) — the
  in-flight stream's failure is typed and the engine's existing
  transient/resume machinery applies; no special handling invented.
- TLS: `require` against a plaintext-only server fails typed (parity
  with standard tooling); `prefer` falls back.
- Root certificate file unreadable/malformed: typed config error at
  open, naming the path.
- Type hints on the cursor column: the hinted type must remain
  cursor-capable or the config is rejected.
- Type hint that widens vs narrows: conversions are defined per
  (source type → hinted type) pair; undefined pairs are config errors —
  no best-effort casting.
- Query streams that shadow a table name, or two query streams sharing
  a name: rejected (stream names stay unique).
- Query stream whose SQL mutates (`INSERT`/`UPDATE`/DDL): rejected
  before execution — extraction is read-only by contract.
- Query stream whose described schema changes between runs: existing
  schema-evolution policies apply, same as a table's drift.
- Merge with a multi-column key; merge where the key contains NULLs
  (rejected — keys must be non-null, typed error); merge on a
  destination without the capability (existing typed rejection).
- Config-schema emission and `deny_unknown_fields` must agree: a config
  the schema accepts must parse, and vice versa — round-trip tested.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Both Postgres connectors (source and destination) MUST
  support encrypted connections covering the standard `sslmode`
  vocabulary — `disable`, `prefer`, `require`, `verify-ca`,
  `verify-full` — with identical semantics to standard Postgres
  tooling, honoring the conn string's sslmode and an optional root
  certificate (file path or inline), with system trust roots used when
  no custom root is given.
- **FR-002**: TLS failures MUST be typed and phase-tagged (connect),
  distinguishing missing-trust-anchor, chain-verification, and
  hostname-verification failures; the 005 "TLS not wired" rejection is
  removed everywhere (code, contract, README).
- **FR-003**: The source MUST accept per-column type-hint overrides
  with the same configuration vocabulary as the REST/file sources;
  every supported (source type → hinted type) conversion is documented
  in the type-mapping contract; undefined pairs and unknown columns are
  typed config errors at open; hinted cursor columns must remain
  cursor-capable.
- **FR-004**: The source MUST support query streams: a stream defined
  by a SQL statement whose result schema is discovered from the
  database's own description, subject to the same type-mapping
  contract, per-statement snapshot consistency, and (when the cursor
  column is in the output) the same incremental semantics as table
  streams. Non-read statements are rejected before execution.
- **FR-005**: The engine MUST support merge write mode for structured
  streams that declare a key, by a recorded amendment to the feature-002
  contract (clause B4): keyed delete-and-insert semantics per key,
  exactly-once under the crash model, capability-declared by
  destinations; keyless structured streams and non-capable destinations
  keep their existing typed rejections.
- **FR-006**: Each run MUST make representation-changing column
  mappings ([documented-lossy]) observable exactly once per stream via
  the engine's existing observability surface, without altering data
  behavior.
- **FR-007**: Every source config MUST expose a machine-readable
  configuration schema generated from the same definition that parses
  the config; the connector's declared spec carries it; documented
  example configs validate against it and unknown-field configs fail —
  round-trip tested for all three sources.
- **FR-008**: The three 005 review test advisories MUST be closed:
  differential coverage includes multi-batch/chunk-boundary decoding;
  the memory-ceiling test fails loudly (with instructions) where its
  prerequisites are expected but absent, skipping only where genuinely
  inapplicable; the container-kill test's integrity assertions cannot
  be vacuously skipped.
- **FR-009**: All existing gates hold: no gated benchmark regresses
  beyond tolerance (TLS off-path; measured), crash sweeps stay green
  with armed-fire pins (extended to merge mode), full suite + doc-tests
  green, workspace safe-Rust policy unchanged.
- **FR-010**: Deliberate exclusions (custom aggregation cursors,
  deferred reflection, FK lineage hints, non-Postgres dialects) MUST be
  recorded with reasons in the feature's close-out notes — absence by
  decision, not omission.

### Key Entities

- **TLS policy**: the per-connection security posture derived from
  sslmode + optional root certificate; shared by source and
  destination.
- **Type hint**: a per-column override mapping a reflected source type
  to a target engine type via a documented conversion.
- **Query stream**: a stream defined by a SQL statement with a
  described (not reflected) schema; otherwise a first-class stream.
- **Keyed structured merge**: the amended write-mode semantics — one
  row per declared key downstream, newest wins, exactly-once.
- **Config schema**: the generated machine-readable description of a
  source's configuration document.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Connecting to a TLS-required Postgres works in all five
  sslmode levels with their standard semantics, for source and
  destination, proven by an automated matrix against a TLS-enabled
  server (self-signed + custom-CA cases), including the negative cases
  (wrong hostname, missing trust anchor).
- **SC-002**: A user can express every override the dlt capabilities
  table marks as a 006 gap: hinted columns land as hinted, query
  streams deliver joined/filtered shapes with automatic schemas and
  working incremental — each proven by conformance tests.
- **SC-003**: With merge configured, an update-heavy incremental
  workload converges to exactly one row per key with newest values on
  both SQL destinations, including under crash-sweep interruption at
  every registered fail point (armed-fire pins extended).
- **SC-004**: Zero silent lossy mappings: every representation-changing
  column in a run is observable, verified by tests that fail when the
  signal is suppressed.
- **SC-005**: For all three sources, the generated config schema
  accepts every documented example and rejects every documented
  invalid config (100% of both lists), and the connector spec exposes
  it.
- **SC-006**: The full verification suite (tests, sweeps with armed
  pins, gates, doc-tests) is green at close; no gated benchmark
  criterion regresses beyond the armed tolerance.
- **SC-007**: The parity table above ends the feature with every "006
  action" row closed and every OUT row carrying a recorded reason.

## Assumptions

- **TLS scope**: server-authenticated TLS only; client-certificate
  (mTLS) authentication and enterprise auth (Kerberos/GSSAPI, IAM
  tokens) remain out, recorded for the backlog. Certificate revocation
  checking follows the TLS library's defaults.
- **`require` semantics follow libpq**: encrypted, certificate NOT
  validated — documented loudly as such, with verify-* recommended for
  production (matching the ecosystem's long-standing vocabulary rather
  than inventing stricter local meaning).
- **Type-hint conversion set**: hints cover conversions expressible as
  safe server-side casts plus the existing decode set; the contract
  table is the closed list. Hints never change what the wire carries
  silently — each hinted column is part of the lossy-visibility
  surface when representation changes.
- **Query streams are read-only SELECT/CTE statements**; parameterized
  queries are out (the cursor predicate composes around the user's SQL
  as a subquery).
- **Merge amendment scope**: SQL destinations (DuckDB, Postgres) gain
  the capability; the parquet destination remains append/replace-only
  (its existing rejection stands).
- **Config-schema generation**: one schema per source config document,
  derived from the config definition itself; schema evolution follows
  config evolution automatically.
- **The 005 evidence/benchmark records are untouched**; if TLS-off
  hot paths regress beyond the gate, that is a defect to fix, not
  re-baseline.
