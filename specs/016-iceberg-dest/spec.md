# Feature Specification: Iceberg Destination (Provider-Agnostic REST Catalog)

**Feature Branch**: `016-iceberg-dest`

**Created**: 2026-07-22

**Status**: Draft

**Input**: User description: "Plan the iceberg destination crate — provider-agnostic, working against Glue, Snowflake Open Catalog (Polaris), and Databricks UC; REST-catalog-first; built on iceberg-rust (survey-first, presumption to take — if we don't have to roll everything by hand we shouldn't); integration via Polaris + rustfs; interop proven by pyiceberg and Spark read-back."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Land exactly-once Iceberg tables through any REST catalog (Priority: P1)

A data engineer points an rdlt pipeline at an Iceberg REST catalog —
a self-hosted Polaris, Snowflake Open Catalog, Databricks Unity
Catalog, or AWS Glue's REST endpoint — with a warehouse and
credentials, and rdlt lands streams as real Iceberg tables: schema
mapped from the pipeline's types, data files written to the table's
storage, every engine commit becoming one atomic Iceberg snapshot.
Crash and replay converge to exactly-once outcomes, and the tables are
readable by any Iceberg-compliant engine.

**Why this priority**: this is the feature — one catalog protocol,
many providers, with rdlt's exactly-once discipline mapped onto
Iceberg's native transaction model. Everything else composes on it.

**Independent Test**: against a containerized REST catalog backed by a
containerized object store, pipelines land exact totals; a replayed
commit publishes nothing new (snapshot history shows no duplicate);
crash injection at every transaction boundary converges on rerun.

**Acceptance Scenarios**:

1. **Given** a catalog endpoint + warehouse + credentials and a stream
   with declared types, **When** the pipeline runs, **Then** a table
   exists in the declared namespace with the mapped schema and exact
   row totals, and its snapshot summary records the pipeline's commit
   identity.
2. **Given** a completed commit, **When** the same (load, commit) is
   replayed after a crash, **Then** no new snapshot appears and totals
   are unchanged (exactly-once via commit-identity receipts in
   snapshot properties).
3. **Given** a concurrent writer committed to the table between rdlt's
   read of table metadata and its commit, **When** rdlt commits,
   **Then** the commit retries against refreshed metadata per the
   catalog's optimistic-concurrency contract and lands without losing
   either writer's snapshot.
4. **Given** append write mode, **Then** each commit appends a
   snapshot; **given** replace mode, **Then** the first commit of a
   load replaces the table's contents (overwrite semantics) exactly
   once per load — the durable once-per-load guard, Iceberg-native.
5. **Given** wrong credentials, a missing warehouse, or an
   unreachable catalog, **Then** the run fails with a typed error
   naming catalog/warehouse/table — never a silent no-op.

---

### User Story 2 - Provider matrix: the same document, three providers (Priority: P2)

The same pipeline document (modulo the auth block and endpoint) works
against Polaris/Snowflake Open Catalog (OAuth2 client credentials),
Databricks UC (bearer token), and AWS Glue (SigV4) — including
catalogs that VEND storage credentials per table, where the user
brings no bucket keys at all.

**Why this priority**: provider-agnosticism is the stated goal; auth
and credential-vending are where providers actually differ.

**Independent Test**: the auth vocabulary round-trips config schema;
OAuth2 and vended-credential flows are proven against the local
catalog (Polaris vends credentials); bearer is proven against a local
UC-compatible catalog if a container leg exists; SigV4/Glue is proven
by an opt-in gated-live cell (environment-gate verified — local Glue
emulation is presumed inadequate until proven otherwise).

**Acceptance Scenarios**:

1. **Given** `auth: {oauth2_client_credentials: …}`, **Then** tokens
   are fetched/refreshed and the credential values never render in any
   output (the Secret discipline).
2. **Given** a catalog that vends storage credentials, **Then** data
   files are written with the vended (temporary, session-token)
   credentials without any user-supplied bucket keys, and expiry
   mid-run classifies transient (engine budget), never partial-success.
3. **Given** the bearer/PAT scheme, **Then** the header attaches on
   every catalog request.
4. **Given** the SigV4 scheme (Glue), **Then** requests are signed;
   proven live under the opt-in gate.

---

### User Story 3 - Interop proof: other engines read what rdlt writes (Priority: P2)

Tables rdlt writes are read back by independent Iceberg
implementations — pyiceberg in the standard test gate, Spark in the
heavy tier — with matching row counts, schemas, and partition
semantics. This is the point of choosing Iceberg: rdlt's output is
nobody's proprietary format.

**Why this priority**: field-ID assignment, stats, and manifest
correctness only count if OTHER readers agree; self-read-back would
prove nothing.

**Independent Test**: after an rdlt run against the local catalog +
object store, a pyiceberg script loads the table via the same REST
catalog and asserts counts/schema/partitions; a Spark job does the
same in the deep tier.

**Acceptance Scenarios**:

1. **Given** an rdlt-written table (append, two commits), **When**
   pyiceberg reads it through the catalog, **Then** counts, column
   names/types, and snapshot count match.
2. **Given** a partitioned table (identity + temporal transform),
   **When** read back, **Then** partition pruning sees the declared
   spec and per-partition counts match.
3. **Given** the deep tier, **When** Spark reads the same tables,
   **Then** the same assertions hold.

---

### Edge Cases

- Commit conflict storms (another writer commits repeatedly): bounded
  retries, then a typed error naming the table — never an unbounded
  loop (the S3-posture analog for optimistic concurrency).
- Table exists with an INCOMPATIBLE schema (type conflict with the
  stream): typed error naming table + column; additive drift (new
  nullable columns) follows the existing drift rules mapped to
  Iceberg schema evolution.
- Namespace does not exist: created if configured to
  (`create-if-missing` posture, explicit), else typed error.
- Vended credentials expire mid-write: transient classification; rerun
  converges.
- A crash between data-file upload and catalog commit: orphaned data
  files are invisible (no snapshot references them) — replay re-lands
  the commit; orphan cleanup is recorded as maintenance-out-of-scope.
- Replayed (load, commit) after the ORIGINAL commit landed but the
  receipt read races a concurrent snapshot: identity check reads the
  full snapshot history's properties, not just the latest.
- Engine batch types with no Iceberg mapping (if any) fail typed at
  ensure-table time, naming the column — the closed-table posture.
- Empty commits (zero rows staged): no snapshot published; replay
  still returns the receipt.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001 (crate & posture)**: a new `rdlt-connector-iceberg`
  destination crate (family layout, façade `rdlt::connector::iceberg`)
  implementing the frozen SPI — Append and Replace write modes;
  merge stays out (documented, like the file destination). The crate
  is a THIN wrapper: config, commit mapping, error classification,
  tests — the Iceberg mechanics come from the surveyed library.
- **FR-002 (library survey — the gate)**: Apache `iceberg-rust` is
  adopted survey-first with presumption to TAKE (the metadata/manifest
  layer is far past the hand-roll threshold). Two disqualifier checks
  run at the environment gate before any code: (a) an available
  release's arrow major must be compatible with the workspace pin
  (RecordBatch crosses the SPI boundary; two arrow majors is a
  design-changing conflict), (b) the write path must support append
  and overwrite transactions against a REST catalog with commit
  retry. The verdict (versions pinned, capabilities probed live) is
  RECORDED; if either check fails, the plan stops and the fallback
  decision is escalated, not improvised.
- **FR-003 (catalog config)**: one declarative document — catalog
  `uri`, `warehouse`, `namespace`, auth block, optional
  storage-override block sharing the FAMILY-WIDE S3 vocabulary
  spelling (endpoint/bucket/credentials as in the file family; the
  vocabulary is shared, the plumbing is not), per-stream table
  mapping and partition spec. Generated schema, eager typed
  validation, additive evolution, `from_yaml`/`from_json`/`from_value`
  entry points, CLI `destination: iceberg:` block.
- **FR-004 (auth vocabulary)**: `oauth2_client_credentials`
  (token URL, client id/secret, scopes — Polaris/Snowflake), `bearer`
  (UC PATs), `sigv4` (Glue; region + AWS credentials), all
  Secret-wrapped with the grep-proof cell. Credential VENDING is
  requested from the catalog when available and used for data-file IO
  (temporary credentials incl. session tokens, refreshed per the
  catalog's contract); user-supplied storage credentials remain
  possible as the override.
- **FR-005 (schema mapping)**: the engine's logical types map to
  Iceberg types as a CLOSED documented table (typed error for
  unmappable columns at ensure-table); field IDs are assigned by the
  library/catalog, never invented; additive drift maps to Iceberg
  schema evolution (add nullable column), contradictory drift stays
  typed.
- **FR-006 (commit mapping — exactly-once)**: one engine commit = one
  Iceberg transaction (data files written, then a single atomic
  catalog commit); the commit identity (pipeline, load, commit_seq)
  lands in the snapshot summary properties; replayed identities are
  detected from snapshot history and return the prior receipt without
  publishing (the D3 discipline, Iceberg-native). State docs ride the
  same mechanism the SPI expects. Commit conflicts retry bounded
  against refreshed metadata; exhaustion is typed.
- **FR-007 (partitioning)**: per-table partition spec with identity
  and temporal transforms (year/month/day/hour) on declared columns;
  unknown columns/transform spellings typed at parse; the spec is
  visible to readers (US3 asserts it).
- **FR-008 (write modes)**: Append appends snapshots. Replace is a
  typed "not supported by this release" error at `ensure_table` —
  the RECORDED T001/T008 narrowing: iceberg-rust 0.10 exposes no
  overwrite transaction, and ID5 forbids emulating one (delete-all +
  append is not atomic replace). Revisit when the library grows an
  overwrite action. Merge stays rejected by capability
  (`merge: false`).
- **FR-009 (typed errors)**: every failure names its subject
  (catalog/namespace/table/column); classification per the standing
  posture — network/5xx/throttle/credential-expiry transient,
  auth/config/schema conflicts fatal, commit-conflict exhaustion
  fatal with the conflict context.
- **FR-010 (crash discipline)**: crash points at the transaction
  boundaries (data-file write, catalog commit, receipt visibility)
  swept with armed-fire pins against the LIVE local catalog
  (container-gated, skip-not-fail), crash/rerun converging to
  exactly-once totals AND a duplicate-free snapshot history.
- **FR-011 (test matrix)**: canonical local leg = Polaris container +
  RUSTFS container (full live-cell discipline, skip-not-fail);
  a UC-compatible local leg (Unity Catalog OSS container) if the
  environment gate verifies it speaks Iceberg REST usably — else
  recorded as deferred; Glue = SigV4 unit/wiremock coverage + an
  opt-in gated-live cell (`RDLT_NET`-class) against real AWS —
  local Glue emulation (moto) is verified at the gate and presumed
  inadequate. pyiceberg read-back runs in the standard gate
  (python venv, competitor-harness pattern); Spark read-back in the
  heavy/deep tier only.
- **FR-012 (verification record)**: traceability matrix zero uncited
  rows; parity record vs dlt's Iceberg destination support with
  deviations named; ≥80% coverage baseline-first; scoreboard bench
  cell (never gated — the floor would measure the catalog/store
  containers); comprehensive README; quickstart walked.

### Key Entities

- **Catalog connection**: uri, warehouse, namespace, auth scheme,
  optional storage override — the provider-agnostic surface.
- **Table mapping**: stream → (namespace, table name), partition
  spec, per-table options.
- **Commit receipt (snapshot-native)**: (pipeline, load, commit_seq)
  in snapshot summary properties — the exactly-once identity readable
  from table history alone.
- **Type mapping table**: the closed engine-type → Iceberg-type
  contract.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: engine runs against the local catalog land exact totals
  and one snapshot per non-empty commit; a full crash sweep over the
  transaction boundaries converges with zero duplicate snapshots.
- **SC-002**: the SAME pipeline document (auth block aside) validates
  and runs against the canonical local leg and — under the opt-in
  gates — at least one managed provider, recorded.
- **SC-003**: pyiceberg reads every table shape the cells write
  (plain, partitioned, post-drift) with matching counts/schemas in
  the standard gate; Spark does the same in the deep tier.
- **SC-004**: vended-credential runs complete with zero user-supplied
  storage keys against the local catalog.
- **SC-005**: coverage ≥80% for the new crate, matrix zero uncited
  rows, all existing gated bars untouched.
- **SC-006**: a config-only user goes from an empty namespace to a
  partitioned, Spark-readable table with one YAML document
  (quickstart walked verbatim).

## Assumptions

- The Iceberg REST catalog protocol is the ONLY catalog surface this
  feature speaks (Glue via its REST endpoint) — no Hive metastore, no
  JDBC catalogs; that is what makes provider-agnosticism one
  implementation.
- `iceberg-rust` is presumed adopted; the environment gate's recorded
  survey (FR-002) is the go/no-go. Its own IO layer handles data-file
  writes — the file family's location PLUMBING is not reused (no
  extraction; the trigger we recorded does not fire), only the config
  VOCABULARY spelling is shared.
- The rdlt-connector-rest crate is NOT a dependency — the library
  brings its own catalog client; the rest crate's client seam remains
  for actual REST-API connectors.
- Iceberg format version 2 tables, single `main` branch.
- Interop containers (Polaris, RUSTFS, UC OSS candidate) follow the
  established podman skip-not-fail pattern; their images/env are
  verified at the environment gate like RUSTFS was in 015.
- The workspace's recorded 0.2→0.3 semver major stands; a new crate
  is additive.

## Out of Scope

- Row-level deletes / merge-on-read / equality deletes (the merge
  write mode stays with the SQL destinations for now).
- Branching, tagging, WAP (write-audit-publish) flows.
- Bucket/truncate partition transforms (identity + temporal only in
  v1).
- Table maintenance: compaction, snapshot expiration, orphan-file
  cleanup (recorded as operator guidance in the README).
- Reading Iceberg tables (a future source feature).
- Non-REST catalogs (Hive metastore, JDBC, Nessie-native).
- Provider-managed table creation UIs/permissions (the catalog's
  ACLs govern; rdlt surfaces typed errors).
