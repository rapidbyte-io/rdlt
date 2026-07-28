# Feature Specification: Snowflake Destination Connector

**Feature Branch**: `022-snowflake-dest`

**Created**: 2026-07-28

**Status**: Draft

**Input**: User description: "Snowflake DESTINATION connector — new thin crate
`rdlt-connector-snowflake`, the THIRD SQL destination after postgres and
duckdb, following the family layout and the 016 precedent of resolving the
driver survey at plan time. Destination only — no source, no CDC. Key-pair
JWT auth only. Full sqlcore merge-vocabulary parity on a dialect without
ON CONFLICT, DISTINCT ON, or enforced unique constraints. Stage-and-COPY
ingestion designed measurement-first. Live qual instance is the primary test
leg, credentials local-only and never committed. Recorded, unbarred
performance session. The two fired sqlcore extraction triggers are taken or
re-dispositioned here."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Land exactly-once loads into Snowflake with one YAML document (Priority: P1)

A data engineer points an existing rdlt pipeline — any source the engine
already speaks — at their Snowflake account by writing a `destination:
snowflake:` block naming account, user, key file, role, warehouse, database
and schema. The run lands exact totals into Snowflake tables, a re-run of a
committed load publishes nothing (replay is detected from the destination's
own records), and a crash at any point between staging and commit converges
to exactly-once on the next run. Authentication is by key pair only; the
private key never appears in logs, error messages, debug output, or any
committed file.

**Why this priority**: this is the MVP — an Append/Replace destination with
exactly-once semantics and typed errors is independently useful before any
merge strategy exists, exactly as the file and iceberg destinations began.
Everything else layers on this session/commit foundation.

**Independent Test**: run a postgres→snowflake pipeline against the qual
account with a small seeded table; verify exact rowcount, re-run and verify
zero new rows and an unchanged destination; kill the process at each staged
crash point and verify the next run converges with no duplicates.

**Acceptance Scenarios**:

1. **Given** a valid config and reachable account, **When** a fresh Append
   load runs, **Then** the destination table holds exactly the source
   rowcount and a receipt records the (load, seq).
2. **Given** a committed load, **When** the identical load replays (same
   load-id and seq), **Then** the destination publishes nothing and the
   run reports the prior receipt.
3. **Given** a Replace-mode stream, **When** two loads run in sequence,
   **Then** the table holds only the second load's rows and a reader never
   observes a cleared-but-unfilled table.
4. **Given** a wrong or rotated private key, **When** a run starts, **Then**
   it fails with a typed authentication error naming the account and user —
   never a raw library error and never key material.
5. **Given** a suspended warehouse with auto-resume, **When** a load runs,
   **Then** it succeeds (the resume latency is absorbed, not errored).

---

### User Story 2 - Full merge-strategy parity on a dialect with no enforced constraints (Priority: P1)

A user who runs keyed merges against postgres or duckdb switches the same
pipeline document to Snowflake and gets the same semantics: `upsert`,
`delete_insert`, and `scd2` strategies, with `hard_delete`, `dedup_sort`,
and `merge_scope` composing identically, validated by the same typed errors.
Snowflake enforces no unique constraints and has no conflict-target upsert,
so the destination must deliver last-wins dedup and key-identity outcomes by
construction of its own merge statements — and prove the outcomes equal the
other destinations' on identical inputs.

**Why this priority**: merge parity is why this connector is worth building
as the third SQL destination rather than a thin file drop. It is also the
first dialect that cannot ride the shared core's defaults, which makes it
the proving ground for the dialect seam. It ships second because it builds
on US1's session.

**Independent Test**: the cross-destination differential oracle — identical
seeded inputs through each strategy land canonical-row-equal results on
snowflake (live leg) and postgres, including SCD2 history openness.

**Acceptance Scenarios**:

1. **Given** a keyed stream with duplicate keys in one load, **When** an
   upsert merge runs, **Then** exactly one survivor per key lands, chosen
   by the same last-wins (or `dedup_sort`-ordered) rule the other
   destinations apply.
2. **Given** the full strategy × options matrix valid on postgres, **When**
   the same options are given to snowflake, **Then** every combination is
   accepted or rejected with the identical typed validation error.
3. **Given** an SCD2 stream with a changed row, an unchanged row, and an
   absent row under `absent: retire`, **When** the load commits, **Then**
   validity windows, retirement, and markers match the postgres outcome
   row-for-row on the differential oracle.
4. **Given** any merge strategy, **When** its SQL plan is generated,
   **Then** the emitted statements match committed golden pins
   byte-for-byte, and the existing postgres/duckdb golden pins are
   byte-identical to their pre-feature state.

---

### User Story 3 - A remote destination that is frugal with round trips (Priority: P2)

A user running against a real cloud endpoint (tens of milliseconds per
round trip, not loopback) gets a destination designed for that reality:
table-ensure reads existing structure once and issues only genuinely needed
schema statements; a steady-state load of an unchanged schema issues zero
schema-mutation statements; the number of statements per load is a measured,
recorded quantity, not an accident.

**Why this priority**: this project measured 72% of a merge load's
statements being no-op schema re-assertions and deferred the fix with the
trigger "first non-loopback deployment" — this feature IS that deployment.
It is P2 only because correctness (US1/US2) must exist before economy.

**Independent Test**: statement-count instrumentation on the live leg — run
the same load twice against an unchanged schema and count statements; the
second run's schema-phase count must be zero mutations and the total per
load must be constant in table count, never in column count.

**Acceptance Scenarios**:

1. **Given** a table that already matches the stream schema, **When** a
   load runs, **Then** the ensure phase issues no ALTER/ADD statements —
   only a bounded read of existing structure.
2. **Given** a stream that gained one nullable column, **When** the next
   load runs, **Then** exactly one additive schema statement is issued for
   that column and nothing else.
3. **Given** any steady-state load, **When** its statements are counted,
   **Then** the total is a recorded constant per table plus the data
   movement itself, and the count appears in the feature's close-out.

---

### User Story 4 - Verified like the other connectors, without a container (Priority: P2)

A contributor without Snowflake credentials runs the full local gate and
every snowflake-specific live test skips visibly (never fails, never
silently vanishes) — exactly the container posture, with credential
presence in place of runtime presence. A contributor (or the owner) with
the qual credentials in the documented local location runs the same gate
and the live legs execute: conformance, crash sweep, differential oracle,
and the strategy matrix, against the real service. No account identifier,
username, or key material exists anywhere in the committed tree.

**Why this priority**: the project's standard is verified connectors; a
SaaS dependency must not weaken either side of that — the gate must stay
runnable everywhere, and the verification must be real somewhere.

**Independent Test**: run the suite with credentials absent and confirm
every snowflake live test reports skipped-with-reason; run with credentials
present and confirm they execute; grep the committed tree for the account
pattern, the username, and key markers and find zero hits.

**Acceptance Scenarios**:

1. **Given** no credentials, **When** the workspace suite runs, **Then**
   snowflake live legs skip with a stated reason and the suite is green.
2. **Given** credentials in the documented location, **When** the suite
   runs, **Then** the live legs execute and pass, including the crash
   sweep's armed-fire pins.
3. **Given** the committed tree at any commit of this feature, **When** it
   is searched for the account identifier, user name, or private-key
   material, **Then** there are zero hits (mechanically verified).

---

### User Story 5 - A recorded, honest performance standing (Priority: P3)

The owner runs a recorded ingestion session against the qual account and
gets a written standing: rows/s and wall time for a known dataset shape,
the statement/batch/file-sizing choices that produced it, and every
optimization decision annotated with the measurement that justified it —
or the negative that declined it. The number is recorded as a scoreboard
entry / session record, never a gated bar.

**Why this priority**: the project's rule is that performance claims are
measurements. A SaaS cell cannot carry a bar (network variance is not the
connector's to control), but an unmeasured connector would be unverified
by this project's own definition.

**Independent Test**: the recorded session exists in the close-out with
dataset identity, timings, and the configuration that produced them; every
taken optimization cites its measurement; every declined one cites its
number.

**Acceptance Scenarios**:

1. **Given** the seeded 1M-row bench-shaped dataset, **When** the recorded
   session runs, **Then** wall time and rows/s are recorded with the
   ingestion configuration named.
2. **Given** any batching/file-sizing/ingestion-path choice in the shipped
   defaults, **When** the close-out is read, **Then** the choice cites a
   measurement, not a plausibility argument.

---

### Edge Cases

- **Identifier case folding**: Snowflake uppercases unquoted identifiers;
  the engine's normalized (lowercase) table/column names must round-trip
  losslessly — the quoting policy is decided once, pinned, and applied
  everywhere (DDL, merge statements, staging names, reads), or same-named
  tables silently diverge into `EVENTS` vs `"events"`.
- **DDL auto-commits**: any schema statement inside a transaction commits
  it. The commit protocol must never interleave DDL into the atomic
  publish+receipt+state unit; a design that accidentally does so would
  silently break exactly-once rather than fail loudly.
- **Session/token expiry mid-load**: a long COPY or merge outliving an
  auth token must resume or fail transient-retryable — never land a
  partial publish.
- **Warehouse suspended / auto-resume**: the first statement pays resume
  latency (seconds); it must be absorbed by timeouts, not classified
  fatal.
- **Credit/quota exhaustion and statement timeouts**: typed, actionable
  errors naming the resource — never a hang, never a raw library dump.
- **Staged-file residue**: a crash between staging and COPY must not leak
  storage without bound; residue is discarded or reclaimed by the replay
  path, and ownership rules ensure only this pipeline's staged artifacts
  are ever deleted.
- **Oversized values**: a JSON document exceeding the semi-structured size
  limit, a decimal exceeding precision 38, or a huge text value must be
  refused typed at write time — not silently truncated by the service.
- **NULLs in merge keys**: refused typed at write time, matching the
  established cross-destination rule.
- **Concurrent pipelines to one schema**: two pipelines sharing a schema
  must not truncate or dedup each other's staged data (pipeline-scoped
  staging identity, as in the other SQL destinations).
- **Rowcount disagreement**: if the service reports a loaded rowcount that
  disagrees with what was staged, the unit must fail loudly rather than
  commit a silently short table.
- **Clock skew**: key-pair auth tokens carry validity windows; skew beyond
  tolerance must produce the typed auth error with a hint, not a generic
  failure.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001 (crate & posture)**: a new `rdlt-connector-snowflake`
  destination crate — family layout, façade `rdlt::connector::snowflake`,
  workspace feature `snowflake`, CLI `destination: snowflake:` block —
  implementing the frozen SPI. Destination ONLY. The crate is THIN:
  config, session/commit mapping, dialect, error classification, tests.
  **Zero engine changes expected**; any engine change discovered necessary
  is a recorded deviation, not a quiet edit. The workspace `unsafe_code`
  denial stands.
- **FR-002 (driver survey — the gate)**: connectivity is adopted
  survey-first at plan time with registry facts and a live probe, the 016
  precedent. Presumption: a pure-Rust HTTPS path (an existing ecosystem
  crate, or a thin hand-rolled client over the service's SQL REST
  interface with key-pair JWT). Disqualifier grounds recorded per
  candidate: proprietary native-driver installs (a packaging burden on
  every consumer of a crate being prepared for publish) and heavyweight
  foreign-runtime FFI trees (the recorded Glue/aws-sdk precedent) are
  presumptively rejected. Library types are wrapped at ONE boundary
  (error classification + the session/commit seam); no library type
  crosses the public surface. If no candidate passes, the plan STOPS and
  escalates — the fallback is not improvised.
- **FR-003 (auth: key pair only)**: v1 authenticates exclusively by
  key-pair JWT. The private key is accepted as a file path or inline PEM,
  Secret-wrapped from the moment of parse, grep-proofed by test (debug
  output, error text, serialized config). Password and OAuth flows are
  typed-unsupported with a clear message, never silently ignored. Auth
  failures are their own error class, distinguishable from network and
  permission failures by shape.
- **FR-004 (config vocabulary)**: closed config — `account`, `user`,
  `private_key` (path or PEM, Secret), `role`, `warehouse`, `database`,
  `schema`, plus the destination options vocabulary of FR-007. Eager
  typed validation naming the field; generated schema; `from_yaml`/
  `from_json`/`from_value` entry points; CLI pipeline-spec round-trip
  including a spec-parse pin. Unknown fields are errors.
- **FR-005 (closed type mapping)**: every engine logical type maps to a
  documented service type or is typed-unsupported — nothing silently
  degrades. JSON documents land as native semi-structured values;
  decimals keep declared precision/scale; timestamps preserve
  zone-awareness (zone-carrying vs naive map to distinct service types);
  the UUID mapping is decided at plan time and documented. The mapping
  table ships in the crate README. Additive schema drift lands as
  additive column DDL; narrowing/incompatible drift follows the engine's
  policy verdicts with typed errors.
- **FR-006 (identifier policy)**: one recorded, pinned identifier policy
  reconciles the engine's normalized names with the service's
  case-folding and quoting rules, applied uniformly across DDL, merge
  SQL, staging names, and reads. Round-trip is proven by test with
  hostile identifiers (mixed case, reserved words, quoted specials). The
  persisted-identity prefix constants remain the single authority for
  `_rdlt_`-prefixed names.
- **FR-007 (write modes & options parity)**: Append, Replace, and keyed
  structured Merge with the FULL shared options vocabulary —
  `delete_insert`, `upsert`, `scd2` (with validity columns, markers,
  `absent` keep/retire and its recorded single-unit rule), `hard_delete`,
  `dedup_sort`, `merge_scope` — accepting and rejecting exactly the
  combinations the other SQL destinations accept and reject, with the
  identical typed validation errors from the shared core.
- **FR-008 (merge dialect without enforced constraints)**: the service
  enforces no unique constraints and offers no conflict-target upsert, so
  key identity and last-wins dedup are delivered by the destination's own
  merge construction (the merge-with-dedup shape is decided at plan;
  OUTCOMES, not statements, are the parity contract). Duplicate-key
  situations that the arbiter-index model would surface on postgres must
  surface equivalently (same outcome or same typed diagnosis). All
  emitted SQL is golden-pinned byte-for-byte; **the existing postgres and
  duckdb golden pins are byte-identical before and after this feature** —
  proof the dialect seam carried the divergence, not the shared planner.
- **FR-009 (fired extraction triggers dispositioned)**: two recorded
  deferrals name "the third SQL destination" as their trigger — the
  shared ensure-table choreography extraction, and the session-protocol
  extraction into the shared core. This feature TAKES each one or
  re-records it with a reasoned rejection and a new trigger; neither may
  end the feature silently open. Any extraction taken must leave the two
  existing destinations' behavior and golden pins byte-identical.
- **FR-010 (ingestion path, measurement-first)**: bulk ingestion is
  designed from measurement, not assumption. Candidates — staged
  columnar-file loading (reusing the workspace's existing columnar writer
  and object-store machinery) vs direct batched DML; service-managed
  staging vs user-provided external staging — are decided by a plan-time
  probe on the live account, and the shipped default's batch/file sizing
  cites a measured knee, not a guess. The losing candidates' numbers are
  recorded. The declined-optimization precedent binds: any later
  "obvious" improvement is measured before it is taken.
- **FR-011 (exactly-once & commit protocol)**: the established
  receipts/state pattern — pipeline-scoped `_rdlt_`-prefixed state,
  receipt, and staging identities — adapted to a service where every
  schema statement auto-commits the open transaction. The atomic unit
  (publish + receipt + state) is pure DML inside one transaction; all DDL
  runs outside it; replay of a committed (load, seq) publishes nothing
  and returns the prior receipt; recovery after a crash at any protocol
  point converges. Crash points cover stage-write, publish, and
  receipt-visible boundaries, each swept with armed-fire pins proving the
  point actually fired.
- **FR-012 (round-trip economy)**: table-ensure reads existing structure
  once per session and issues only genuinely needed statements. A
  steady-state load over an unchanged schema issues ZERO schema-mutation
  statements. Statements-per-load is instrumented in test, counted on the
  live leg, and recorded; the recorded count is constant in table count
  and independent of column count at steady state.
- **FR-013 (typed error taxonomy)**: service errors are classified by
  structured error codes into the SPI taxonomy — authentication,
  permission, transient (network, resume, timeout, throttle), fatal
  (schema conflict, oversized value, SQL error) — with the transient
  class actually retried by the existing driver policy.
  Substring-matching of rendered error text is FORBIDDEN in
  classification and in tests. Rowcount-verification failures and quota
  exhaustion are typed and named.
- **FR-014 (live-leg testing posture)**: snowflake live tests gate
  skip-not-fail on credential presence, mirroring the container posture:
  a documented local convention (environment variables and/or the user
  config directory holding the key pair) supplies account, user, and
  key; absence means a visible skip with reason; presence means the legs
  run in the standard gate. **No account identifier, user name, or key
  material may appear in any committed file at any commit of this
  feature** — mechanically verified. A hermetic local emulator is
  surveyed at plan time with a fidelity probe and adopted or rejected
  with the outcome recorded either way; the live account remains the leg
  of record regardless.
- **FR-015 (conformance, differential, parity)**: the destination passes
  the existing destination-conformance harness; the cross-destination
  differential oracle proves canonical-row equality (including SCD2
  history openness) against the postgres destination on identical seeded
  inputs for every strategy; a dlt-parity matrix records feature parity
  with named, reasoned deviations.
- **FR-016 (recorded performance standing)**: one recorded ingestion
  session against the live account on a bench-shaped dataset, with
  dataset identity, wall, rows/s, and configuration recorded in the
  close-out. UNBARRED — no gated bar may reference a SaaS cell; the
  session is a scoreboard/record entry. Existing gated bars are untouched
  and re-verified green at feature close.
- **FR-017 (docs)**: crate README with the closed type mapping, the
  identifier policy, the auth setup walk (key generation → account
  configuration → rdlt config), the credential-location convention, and
  known service-semantics caveats; a config-only quickstart walked
  verbatim; public items documented to the workspace `missing_docs`
  standard; the docs gate stays clean.
- **FR-018 (delivery discipline)**: independently mergeable increments in
  value-per-risk order; the full local gate green at every merge; a
  close-out matrix with zero uncited dispositions; every deviation,
  declined measurement, and unperformed verification recorded. Semver:
  the feature is purely ADDITIVE (new crate, new façade feature) — the
  semver gate reports no required bump on the semver-sacred crates, and
  nothing in this feature forces the standing publish-time window.

### Key Entities

- **Destination config**: account/user/key-pair identity, role,
  warehouse, database, schema, plus the shared write-mode options
  vocabulary; closed, validated, schema-generated.
- **Load session**: one open destination connection's lifecycle — ensure,
  stage, publish, commit — owning pipeline-scoped staging identity and
  the statement-economy contract.
- **Commit unit / receipt / state doc**: the exactly-once triad,
  persisted in `_rdlt_`-prefixed tables; the receipt keyed by
  (load, seq); state carries cursors; all three move atomically in one
  DML transaction.
- **Merge plan & dialect**: the shared core's strategy plan realized as
  service-specific SQL text — the dedup-carrying merge construction,
  scope replacement, SCD2 windows — golden-pinned.
- **Staged artifact set**: the per-load staged data files (or batches)
  awaiting publish; pipeline-owned, crash-reclaimable, never another
  pipeline's to touch.
- **Type mapping table**: the closed logical→service type mapping,
  documented and enforced.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: engine runs against the qual account land exact totals for
  Append, Replace, and every merge strategy; a full crash sweep over the
  protocol's crash points converges with zero duplicate publishes and
  zero lost loads, each point proven armed.
- **SC-002**: the differential oracle shows canonical-row equality with
  the postgres destination on identical inputs for the full strategy ×
  options matrix, including SCD2 history openness; every combination's
  accept/reject verdict and error text match the shared core's.
- **SC-003**: the existing postgres and duckdb golden-SQL pins are
  byte-identical to their pre-feature state; the new dialect's pins are
  committed and green.
- **SC-004**: a steady-state load over an unchanged schema issues zero
  schema-mutation statements, and the measured statements-per-load count
  is recorded in the close-out as constant per table.
- **SC-005**: with credentials absent the full workspace gate is green
  with snowflake live legs visibly skipped; with credentials present the
  same gate runs them green; a tree-wide mechanical search at feature
  close finds zero occurrences of the account identifier, user name, or
  key material in committed files.
- **SC-006**: a config-only user goes from an empty schema to a loaded,
  merge-maintained table with one YAML document, following the
  quickstart verbatim (key setup included).
- **SC-007**: one recorded ingestion session exists in the close-out with
  dataset identity, wall time, rows/s, and the configuration that
  produced it; every shipped ingestion default cites a measurement; every
  declined optimization cites its number. No gated bar references the
  SaaS cell; all existing gated bars remain green.
- **SC-008**: coverage stays at or above the 80% floor workspace-wide;
  the dlt-parity matrix has zero unexplained gaps; the close-out matrix
  has zero uncited dispositions, and both fired extraction triggers carry
  a terminal disposition.
- **SC-009**: the semver gate reports no required version bump on the
  semver-sacred crates at every increment merge.

## Assumptions

- A qual Snowflake account is available to the owner with a provisioned
  service user, key-pair authentication configured, and rights to create
  and drop schemas/tables in a dedicated test database; test datasets are
  small enough that credit consumption is not a constraint.
- Credentials (account identifier, user, key pair) live ONLY in the local
  environment — the documented convention is environment variables and/or
  the user's local config directory; the committed tree records the
  convention, never the values. The spec deliberately does not name the
  qual account.
- The live legs tolerate SaaS latency: suite timeouts accommodate
  warehouse auto-resume and network round trips without weakening the
  container legs' timings.
- The engine's existing write-mode and schema-policy semantics are the
  contract; this feature adds a destination, not new engine behavior.
- The workspace's existing columnar-file and object-store machinery is
  reusable for staged ingestion if the measurement selects that path; no
  new storage subsystem is in scope.
- dlt's Snowflake destination is the parity reference, consistent with
  the established competitor-parity precedent.

## Out of Scope

- A Snowflake SOURCE (reads, CDC, streams/tasks) — destination only.
- Streaming ingestion (the service's streaming-ingest channel APIs) —
  recorded as a phase-2 door with its trigger, not built.
- Password, OAuth, SSO, or MFA authentication flows — key pair only;
  others are typed-unsupported.
- A gated benchmark bar for any SaaS cell — recorded sessions and
  scoreboard entries only, under the standing bench governance.
- Iceberg-format or external tables on Snowflake, dynamic tables,
  clustering keys, and warehouse/cost-management features.
- CI repair (the recorded external blocker stands); CI-only verifications
  are recorded UNPERFORMED, never claimed.
- Publishing to the registry (that is the reserved neighboring feature);
  this feature only preserves publish-readiness (additive semver, clean
  docs and packaging posture).
