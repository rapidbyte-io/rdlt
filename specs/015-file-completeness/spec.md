# Feature Specification: Filesystem/Object-Store Completeness

**Feature Branch**: `015-file-completeness`

**Created**: 2026-07-22

**Status**: Draft

**Input**: User description: "Filesystem/object-store completeness (015): unify the file family and take it to full capability — merge the parquet crate into the file family (parquet is a FORMAT, not a system), then full source/dest capability over local paths AND S3-compatible object storage, with the RUSTFS container as the live-test object store."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - One file family, nothing breaks (Priority: P1)

An existing rdlt user has pipelines reading JSONL/parquet files and writing
parquet output. The two file-family crates become one, organized like every
other connector family — and every existing pipeline document, behavior,
and guarantee is preserved exactly. Nothing about the merge is visible to a
running pipeline.

**Why this priority**: the unification is the foundation every other story
builds on, and it touches the flagship benchmark's destination. If existing
behavior shifts, the feature has failed before adding anything. This is the
same weld discipline as features 013 (duckdb restructure) and 014 (rest
restructure): moves only, behind a behavior-preservation net.

**Independent Test**: the ENTIRE pre-015 test surface for both crates runs
green unchanged (beyond mechanical import paths); the gated
parquet-passthrough benchmark stays in-band; every pre-015 pipeline-YAML
spelling parses identically.

**Acceptance Scenarios**:

1. **Given** a pre-015 pipeline YAML using the file source (jsonl/parquet,
   globs, per-file cursors) or the parquet destination, **When** it runs
   after the merge, **Then** results, cursors, staging layout, receipts,
   and error behavior are identical to pre-015.
2. **Given** the gated `parquet-passthrough` bench cell, **When** re-run
   after the merge, **Then** the bar is met with the median in-band.
3. **Given** the pre-015 crash fail points (`pq.*` and the file-source
   points), **When** the sweep runs, **Then** every pin fires and
   exactly-once outcomes hold, unchanged.
4. **Given** code that consumed the old public items, **When** built
   against the unified crate's façade re-exports, **Then** it compiles
   (the recorded one-time 0.2→0.3 break this cycle covers the crate
   removal; spellings inside documents never break).

---

### User Story 2 - Read files from object storage (Priority: P2)

A data engineer has JSONL, CSV, and parquet files landing in an
S3-compatible bucket (AWS S3, Cloudflare R2, any S3-compatible server).
They point a stream at `s3://bucket/prefix/*.jsonl` with endpoint +
static credentials, and rdlt discovers the files, reads them through the
engine with exact totals, and on later runs loads ONLY new or grown
files — the same incremental discipline local files already have.

**Why this priority**: files-on-object-storage is the most common
ingestion shape in practice; it is the reason this feature exists. Local
CSV support rides in this story because the format layer and the location
layer land together.

**Independent Test**: against a containerized S3-compatible server
(RUSTFS — Apache-2.0; the former de-facto test server changed license),
a bucket seeded with known files loads to exact totals through the engine
into a real destination; a second run with two files added and one grown
loads exactly the delta. Skips (not fails) when no container runtime is
available — the postgres-cell pattern.

**Acceptance Scenarios**:

1. **Given** a bucket with N seeded JSONL/CSV/parquet files and a stream
   with a prefix/glob, **When** the pipeline runs, **Then** row totals in
   the destination equal the seeded totals exactly, with deterministic
   file ordering.
2. **Given** a completed run, **When** two new files appear and one prior
   file grows (append), **Then** the next run reads only the two new
   files plus the grown tail, exactly once.
3. **Given** a CSV stream with declared delimiter/header options and
   per-column type hints, **When** it loads, **Then** typed columns land
   as declared and a malformed row fails typed, naming file and row.
4. **Given** credentials that are wrong or a bucket that does not exist,
   **When** the pipeline starts, **Then** it fails with a typed error
   naming the endpoint/bucket — never a silent empty load (the
   no-silent-failures posture: an empty PREFIX is success; an unreachable
   or unauthorized location is an error).
5. **Given** a gzip- or zstd-compressed JSONL/CSV file with the matching
   extension, **When** it loads, **Then** contents decode transparently
   and totals are exact (compressed files are whole-file units:
   re-reading after growth is not resumable mid-file and is documented).

---

### User Story 3 - Write files to object storage, partitioned (Priority: P3)

A pipeline lands parquet (or JSONL) output either on local disk (as
today) or directly into an S3-compatible bucket, with a predictable
layout: one directory/prefix per table, optional partitioning by a
declared column, atomic visibility per commit, and crash-safe exactly-once
outcomes — the same commit discipline the parquet destination already has
locally, extended to object storage.

**Why this priority**: completes the lake story both directions, but
depends on the location layer (US2) and the merge (US1).

**Independent Test**: engine runs (including crash/rerun sweeps) writing
to the containerized object store produce exactly-once file sets whose
row totals match the source; partitioned layouts split rows by the
declared column with deterministic file naming.

**Acceptance Scenarios**:

1. **Given** a destination configured to a bucket/prefix, **When** a
   pipeline commits, **Then** the finalized objects appear only at commit
   (readers can never observe partial files) and totals are exact.
2. **Given** a declared partition column, **When** rows land, **Then**
   files are organized under one prefix per partition value, and rows
   appear in exactly one partition file set.
3. **Given** a crash injected at any write/finalize boundary, **When**
   the pipeline reruns, **Then** the destination converges to exactly-once
   totals with no stray visible partial objects (staging leftovers are
   permitted only under staging-designated names).
4. **Given** JSONL output format is selected instead of parquet, **Then**
   the same layout, atomicity, and totals guarantees hold.

---

### Edge Cases

- A file matched by the listing disappears between listing and reading —
  typed error naming the file (the snapshot lied; never silently skip).
- A file shrinks or is rewritten in place between runs (size same, mtime
  moved) — loud typed failure, never a stale-offset read (the existing
  local rule, proven for object storage too using size + the store's
  content-identity equivalent).
- Listing pagination: prefixes with more objects than one listing page
  return the COMPLETE set (proven with a seeded bucket larger than one
  page).
- Two streams pointing at overlapping prefixes — allowed (streams are
  independent); the same file loading into two streams is the operator's
  declared intent.
- CSV with a header row but zero data rows; JSONL empty file; parquet
  file with zero row groups — all legitimately empty, success with zero
  rows.
- CSV type inference sees mixed values (e.g. "1", "x") — inference
  resolves per the documented widening rules; a declared type hint that a
  value cannot satisfy fails typed naming file, row, and column.
- Compressed file whose extension does not match its actual codec —
  typed error naming the file; never garbage rows.
- An object-store write interrupted mid-upload — rerun converges;
  incomplete uploads are aborted or superseded, never visible as data.
- Credentials expire mid-run — classified transient (the engine's retry
  budget governs), never a partial-success commit.
- Local path spellings behave exactly as pre-015; object-store URLs are a
  NEW spelling — no existing document changes meaning.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001 (unification)**: the file family MUST become one connector
  crate holding source and destination sides with a shared format layer,
  presented like the other families (one façade module). The standalone
  parquet-destination crate ceases to exist; its full behavior,
  staging/receipt formats, fail points, and options survive unchanged
  inside the family.
- **FR-002 (preservation net)**: every pre-015 behavior of both crates
  MUST be preserved and proven: existing test cells run green unchanged
  (mechanical import paths only), the gated parquet-passthrough bench
  stays in-band, pre-015 pipeline-YAML spellings (`file:` source,
  `parquet:` destination) parse with identical meaning, and pre-015
  cursor/staging/receipt documents remain readable (persisted-format
  compatibility).
- **FR-003 (locations)**: source streams and the destination MUST accept
  a location that is either a local path/glob (exactly as today) or an
  S3-compatible object-store URL with endpoint, bucket, region, and
  static credentials configurable per source/destination; credentials
  are secret-redacted in every rendering (the 014 Secret discipline).
- **FR-004 (discovery)**: file discovery MUST support explicit paths,
  globs (local), and prefix + glob-suffix matching (object store), with
  deterministic lexicographic ordering, complete listings across
  pagination, and the existing empty-vs-missing semantics: an empty
  match-set is success, an explicitly named missing file or unreachable
  location is a typed error.
- **FR-005 (incremental)**: per-file incremental cursors MUST extend to
  object storage with the same loud-failure rules (grown = read the tail
  where the format permits, shrunk/rewritten = typed error naming the
  file, completed = skipped), riding the existing engine checkpoint
  machinery with exactly-once outcomes across re-listing.
- **FR-006 (CSV)**: the source MUST read CSV with declared options
  (delimiter, header presence, quoting) and documented type-inference
  rules, overridable per column by the existing type-hints vocabulary;
  malformed rows fail typed naming file + row number.
- **FR-007 (formats preserved)**: JSONL reading keeps its existing fast
  path and resume semantics; parquet reading keeps structured
  (Arrow-native) delivery and row-group units.
- **FR-008 (compression)**: gzip and zstd compressed JSONL/CSV MUST read
  transparently by extension, with codec mismatch a typed error;
  compressed files are whole-file incremental units (documented).
- **FR-009 (destination formats & layout)**: the destination MUST write
  parquet (as today) and JSONL, to local or object-store locations, with
  one prefix per table and an optional declared partition column
  producing one prefix per partition value; file naming stays
  deterministic per (load, commit, table, sequence) so recovery
  converges.
- **FR-010 (commit discipline)**: destination visibility MUST be
  commit-atomic per the existing contract (staged names → finalize at
  commit; on object storage, finalize semantics MUST guarantee readers
  never observe partial objects); crash fail points cover open/write/
  finalize boundaries on BOTH location kinds and join the sweep with
  armed-fire pins.
- **FR-011 (typed errors)**: every failure names its subject — file,
  row/byte or row-group position, bucket/endpoint — and classifies per
  the S3 posture (network/service = transient for the engine budget;
  malformed data/config = fatal).
- **FR-012 (config evolution)**: config growth is ADDITIVE: every
  pre-015 spelling parses unchanged; new location/format/partition
  options are new fields with schema round-trips; validation is eager
  and typed at parse.
- **FR-013 (live object-store proof)**: object-store behavior MUST be
  proven against a real S3-compatible server (RUSTFS, Apache-2.0) in a
  container under the established podman pattern — skip-not-fail without
  a container runtime; cells cover discovery, incremental delta, CSV
  options, compression, destination atomicity, and listing pagination.
- **FR-014 (verification record)**: traceability matrix with zero
  uncited rows; dlt-parity record vs dlt's filesystem source and
  destination naming deliberate deviations; coverage ≥80% for the
  unified crate measured baseline-first; comprehensive README (013/014
  standard); quickstart walked; bench: existing gated bars untouched,
  plus at least a file→duckdb scoreboard cell over the new path.
- **FR-015 (dependency discipline)**: new external dependencies require
  the 009 crate-survey rule applied per dependency (the object-store
  client and CSV parsing are the expected candidates; each gets a
  recorded survey verdict); the S3 test server is a container image,
  never a build dependency.

### Key Entities

- **Location**: where files live — local filesystem or an S3-compatible
  store (endpoint, bucket, region, credentials). One vocabulary shared
  by source streams and the destination.
- **File snapshot**: the deterministic, complete, ordered list of files
  a stream's discovery resolved this run, with per-file identity
  (path/key, size, mtime or content-identity equivalent).
- **Per-file cursor**: the existing progress record (`done`, `size`,
  boundary flag, rewrite tripwire) extended with object-store identity
  fields; completed files skipped, grown files resumed, changed files
  loudly rejected.
- **Format**: jsonl | csv | parquet, with per-format options and shared
  ownership by source and destination sides (one place defines what each
  format is to rdlt).
- **Partitioned layout**: destination file organization — table prefix,
  optional partition column → one prefix per value, deterministic file
  names, staged-vs-final naming.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of pre-015 file/parquet test cells pass unchanged
  after the merge, and the gated parquet-passthrough bar is met in the
  same session as the move.
- **SC-002**: seeded-bucket loads (each format × plain/gzip/zstd where
  applicable) land exact totals through the engine into a real
  destination, including a listing larger than one pagination page.
- **SC-003**: a delta run over a mutated bucket (files added + one
  grown) transfers exactly the delta — proven by read accounting, not
  just final totals.
- **SC-004**: crash sweep over every file fail point (both location
  kinds) shows exactly-once convergence with all armed pins fired.
- **SC-005**: unified-crate line coverage ≥80% with classified
  exclusions recorded; matrix has zero uncited rows.
- **SC-006**: all existing gated bench bars remain met in the close-out
  session; the new file→duckdb scoreboard cell is recorded
  baseline-first.
- **SC-007**: a config-only user can declare an S3 source stream (CSV
  with options + type hints) and a partitioned parquet destination and
  run end-to-end with zero code — proven by a quickstart walked
  verbatim.

## Assumptions

- The S3-compatible surface (endpoint + static access/secret key,
  path-style or virtual-host addressing) is the scope; provider-native
  auth flows (IAM roles, OAuth service accounts, Azure/GCS native APIs)
  are OUT this feature and recorded in the parity table as deferred.
- RUSTFS is the containerized S3-compatible server for live cells
  (Apache-2.0; chosen because the previous de-facto standard test
  server changed to a restrictive license); cells follow the container
  skip-not-fail pattern established by the postgres suites.
- The recorded one-time semver major this cycle (from 014) covers
  removing the standalone parquet crate; pipeline-document spellings
  still never break (FR-002) — the compatibility promise that matters is
  to DOCUMENTS, not crate names.
- Object-store finalize semantics differ from POSIX rename; the commit
  contract is stated in reader-observable terms (never a partial object
  visible as data) rather than mandating a mechanism — the plan chooses
  the mechanism per store capability and records it.
- Local-file behavior (including the flagship JSONL fast path) is the
  performance-critical path and must not regress; object-store paths are
  correctness-first this feature, with throughput recorded as scoreboard
  data, not gated bars.
- Schema drift across files of one stream follows the existing additive
  drift rules; contradictory drift stays a typed error.

## Out of Scope

- Delta Lake / Iceberg / table formats (future feature; parquet-files
  layout only).
- File watching / streaming tail / event-driven ingestion (runs are
  batch: list, load, finish).
- Provider-native auth (IAM roles, workload identity, SAS tokens);
  GCS/Azure-specific APIs beyond their S3-compatible endpoints.
- Merge strategies on the file destination (append/replace only, as
  today — merge stays the SQL destinations' capability).
- Client-side encryption; encryption configuration beyond what the store
  applies transparently.
- Schema evolution beyond the existing additive drift rules.
