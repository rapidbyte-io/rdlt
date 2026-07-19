# Feature Specification: File & Arrow-Native Ingestion

**Feature Branch**: `002-file-arrow-ingestion`

**Created**: 2026-07-19

**Status**: Draft

**Input**: User description: "File & Arrow-native ingestion (fast-follow to feature 001): a bundled file source connector reading local files — JSONL first, Parquet second — with glob patterns, and incremental resume via completed-file + byte-offset checkpoints so re-runs skip already-loaded data. Parquet files are pushed as already-structured Arrow batches, which requires engine-level Arrow passthrough: structured batches bypass the shredder (schema mapping + policy checks + run-lineage stamping only). Design decision to encode: Arrow-pushed streams carry only _rdlt_load_id lineage (no per-row identity), therefore Merge write mode is rejected for them at build time in v1 — this is a new connector-contract clause."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Load local files into a destination (Priority: P1)

A pipeline developer points the bundled file source at a directory of newline-delimited
JSON files (a path or a glob pattern) and runs the pipeline. Every record across every
matching file lands in the destination with the same automatic typing, nesting, and
lineage guarantees as any other source. On the next run, files that were fully loaded
are skipped entirely, and a file that grew since the last run is read only from where
the previous run stopped.

**Why this priority**: Files-to-warehouse is one of the most common ingestion jobs in
practice and the engine's flagship performance claim is defined on it; today it is only
reachable through a demo binary, not a supported connector.

**Independent Test**: Point the connector at a temp directory of generated JSONL files;
run twice (second run after appending to one file and adding a new one); verify full
load, then verify the second run reads only the appended tail and the new file.

**Acceptance Scenarios**:

1. **Given** a glob matching three JSONL files, **When** the pipeline runs, **Then**
   every record from all three files is loaded exactly once, with lineage and inferred
   types identical to the equivalent records arriving from any other source.
2. **Given** a completed first run, **When** a second run starts after one file gained
   appended lines and a new file appeared, **Then** only the appended lines and the new
   file's records are read and loaded — completed ranges are never re-read.
3. **Given** a file containing a malformed line, **When** the pipeline runs, **Then**
   the failure is classified and reported naming the file (and the run fails without
   publishing the partial batch), never silently skipping data.

---

### User Story 2 - Move already-structured data without re-processing (Priority: P2)

A pipeline developer loads Parquet files (or any source that emits already-structured
record batches). Because the data is already typed and shaped, the engine moves it
along a fast path: no re-inference, no re-shredding — only schema compatibility
checks, schema-change policy enforcement, and run-provenance stamping. Structured and
record-oriented streams can coexist in one pipeline.

**Why this priority**: Already-structured data is the second-largest file workload,
and re-processing it wastes exactly the work the engine's streaming design exists to
avoid; this also unblocks the published pass-through performance claim.

**Independent Test**: Generate Parquet files with known schema and contents; run a
pipeline into an analytical destination; verify contents, types, and per-row run
provenance; verify a schema-frozen table rejects a Parquet file whose schema drifted.

**Acceptance Scenarios**:

1. **Given** Parquet files whose schema matches the destination table, **When** the
   pipeline runs, **Then** all rows land with their original types preserved and each
   row carries the run identifier that loaded it.
2. **Given** a later Parquet file that adds a column, **When** the pipeline runs under
   the default evolve policy, **Then** the destination table gains the column (earlier
   rows read as absent), and under a frozen policy the run fails with a typed error
   naming the table and column before any violating row is published.
3. **Given** a structured stream, **When** it is loaded twice due to a crash-recovery
   redelivery window, **Then** duplicates are possible and documented (append-mode
   at-least-once for structured streams without per-row identity) — never silent
   corruption.

---

### User Story 3 - Fail fast when a mode needs per-row identity (Priority: P3)

Structured pass-through streams carry run-level provenance but no per-row identity.
A developer who configures identity-dependent behavior (merge/upsert by key) on such a
stream is told at configuration time — with an actionable error naming the stream —
not after data has moved.

**Why this priority**: Guardrail; prevents a silent correctness surprise (an upsert
that cannot deduplicate), but only matters when someone combines the new stream kind
with merge mode.

**Independent Test**: Configure merge mode on a Parquet-backed stream; building the
pipeline must fail with an error naming the stream and the reason; the same
configuration on a record-oriented (JSONL) stream must succeed.

**Acceptance Scenarios**:

1. **Given** a structured (pass-through) stream configured with merge-by-key, **When**
   the pipeline is built, **Then** building fails with an error identifying the stream
   and stating that merge requires per-row identity, before any connection is opened.
2. **Given** the same pipeline with append or replace mode, **When** it is built and
   run, **Then** it succeeds.

---

### Edge Cases

- A previously-completed file **shrinks or its recorded offset exceeds its current
  size** (rewritten/rotated file): the connector must not read garbage — treat as a
  changed file, fail with an actionable error naming it (never silently reload or skip).
- A glob matches **no files**: the run succeeds with zero rows and says so (empty
  stream), not an error — but an explicitly named single file that is missing IS an
  error.
- Files **appear mid-run**: they are picked up on the next run, not torn mid-run
  (stable file list per run).
- **Empty files** and files ending without a trailing newline load correctly.
- Two matched files map to the **same stream**: all files of one glob feed one stream
  (one table family); per-file attribution is visible via the checkpoint/cursor.
- A structured file whose column **type conflicts irreconcilably** with the existing
  table (not a widening): schema-change policy decides (evolve where the type system
  allows widening, otherwise the run fails typed) — never silent coercion.
- Non-UTF-8 bytes in a JSONL file: classified as a malformed-input failure naming the
  file.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The bundled file source MUST read newline-delimited JSON files selected
  by explicit path or glob pattern, feeding each configured stream as one table family
  with the same typing/nesting/lineage semantics as record streams from any source.
- **FR-002**: The file source MUST support incremental resume: its checkpoints record
  per-file progress (completed files and the byte offset within the in-progress file),
  and a resumed run MUST NOT re-read completed ranges. Appended data and newly matched
  files are picked up on the next run.
- **FR-003**: The file source MUST detect files that changed incompatibly with its
  recorded progress (shrunk/rewritten) and fail with an error naming the file — never
  read from a stale offset.
- **FR-004**: The file source MUST pass the public connector conformance suite —
  "certified = passes conformance" applies to it like any bundled connector.
- **FR-005**: The file source MUST read Parquet files and deliver their contents as
  already-structured batches (no re-inference of types the file already declares).
- **FR-006**: The engine MUST accept already-structured batches from any source
  (pass-through): mapping the batch's schema onto the engine's logical schema,
  enforcing schema-change policies (evolve/freeze/discard) exactly as for record
  streams, and applying capability-driven lowering for the destination.
- **FR-007**: Pass-through batches MUST be stamped with run provenance
  (`_rdlt_load_id`) and MUST NOT be re-shredded; their row data moves to the
  destination without value-level transformation beyond capability lowering.
- **FR-008** *(contract clause)*: Structured pass-through streams carry no per-row
  identity in v1; configuring merge mode on such a stream MUST fail at build time with
  an error naming the stream. Append and replace modes are supported; the redelivery
  window under crash recovery is at-least-once for structured streams and MUST be
  documented in the connector contract.
- **FR-009**: Structured batches participate in the recovery model like all data:
  covered by checkpoints, buffered/replayed by the local log, exactly-once visible
  under append commits (duplicates only within the documented redelivery window of
  FR-008).
- **FR-010**: The benchmark harness MUST gain product-level cells measured against
  the pinned incumbent baseline: files→warehouse via the bundled JSONL source, the
  parquet→parquet pass-through path, and a parquet→analytical-destination bonus row;
  results recorded with the existing baseline-first methodology.
- **FR-011**: A minimal file destination MUST write each table's batches as Parquet
  files. Write-only and append/replace only (no merge); it still honors the full
  destination correctness contract — staged invisibility until commit, atomic
  publication with state, idempotent commits, staging teardown on open — and passes
  the public destination conformance suite.

### Key Entities

- **File stream**: A named stream backed by an explicit file path or glob; owns one
  table family; its cursor is per-file progress (completed set + in-progress offset).
- **File cursor**: The checkpoint payload for a file stream: which files are complete,
  and the byte offset (JSONL) or row-group position (Parquet) of the in-progress file.
- **Structured (pass-through) stream**: A stream whose source emits typed batches;
  schema-checked and policy-governed but not re-shredded; run-level provenance only.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The file source passes 100% of the public conformance suite in CI.
- **SC-002**: A re-run over an unchanged file set reads zero bytes of file content and
  loads zero rows; after appending N records to one file, the re-run loads exactly N.
- **SC-003**: The files→warehouse benchmark (bundled JSONL source → embedded
  analytical destination) meets the existing ≥10× target against the pinned baseline,
  measured by the published harness — as a supported connector, not a demo binary.
- **SC-004**: The structured pass-through benchmark is published as TWO rows, both
  against the pinned baseline: the design doc's "parquet → parquet" cell (engine-bound,
  via a new minimal parquet-file destination) with the ≥2× target, plus a
  parquet → embedded-analytical-destination bonus row (pass-through + ingestion
  combined) for context. *(Clarified 2026-07-19: option C — do both.)*
- **SC-005**: Misconfigured merge on a structured stream is rejected at build time in
  100% of cases, with the stream named in the error.
- **SC-006**: Zero silent-failure paths: malformed files, shrunk files, and empty
  globs each produce the documented, distinct, observable outcome.

## Assumptions

- Local filesystem only in v1 (no object stores); paths/globs resolve on the machine
  running the pipeline. Object-store listing is a future feature.
- One glob = one stream = one table family; per-file fan-out into separate tables is
  out of scope.
- File ordering within a stream follows a stable, documented order (lexicographic by
  path) so runs are deterministic.
- CSV and other formats are out of scope for this feature (JSONL and Parquet only).
- The pass-through lineage decision (run-level provenance only, no per-row identity,
  merge rejected) is a v1 contract clause, revisitable later by adding optional
  source-supplied identity.
- Compressed files (gzip etc.) are out of scope for v1; a follow-up can add
  decompression transparently.
- The parquet-file destination is deliberately minimal (benchmark + file-export use
  cases); rich layout options (partitioning, compaction) are out of scope.
