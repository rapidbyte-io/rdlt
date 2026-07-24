# Tasks: Workspace Refactoring Program

**Input**: Design documents from `/specs/017-workspace-refactoring/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/workspace-refactoring.md, quickstart.md

**Tests**: REQUIRED for US1 — every B-item fix carries a red-first regression test (WR6, research D-14). Elsewhere the existing gate (conformance, golden pins, crash sweeps) is the behavior pin; new tests appear only where a contract clause demands one (parity pin, probe pins, posture verification).

**Organization**: grouped by user story (US1–US7 from spec.md). The recommended *execution* order is the plan's increment sequence — see the Increment Mapping table at the end; story phases remain independently completable.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

- [X] T001 Record pre-feature coverage baseline (`make coverage`) and create the close-out matrix skeleton at specs/017-workspace-refactoring/close-out.md — one row per catalogue item (B1–B12; R1–R13 with Part 3 sub-items; D1–D15 + Part 5 low notes), columns per data-model.md §1, baseline number in the header
- [X] T002 [P] Probe duckdb structured error codes (research D-02): write a probe test in crates/rdlt-connector-duckdb/tests/error_codes.rs pinning the `code`/`extended_code` a live upsert-precondition constraint violation produces; record outcome (or fallback trigger) in close-out.md row B5
- [X] T003 [P] Probe iceberg auth error context (research D-03): write a probe (container test, skip-not-fail) in crates/rdlt-connector-iceberg/tests/auth_probe.rs asserting a live REST-catalog 401 carries the `status` context value; record outcome (or fallback trigger) in close-out.md row B6

## Phase 2: Foundational

No foundational tasks — the existing workspace, test gate, and CI are the foundation. T002/T003 gate only the two classification fixes (T008/T009); everything else can start once T001 records the baseline.

**Checkpoint**: baseline recorded; probes resolved; user stories may begin.

---

## Phase 3: User Story 1 — Latent defects no longer corrupt or abort pipelines (Priority: P1) 🎯 MVP

**Goal**: all 12 catalogued defects fixed on unmoved code, each pinned by a regression test demonstrated red against the pre-fix tree (quickstart red-first procedure; evidence cited in close-out.md).

**Independent Test**: each regression test fails pre-fix / passes post-fix; full gate + failpoint sweeps green.

- [X] T004 [P] [US1] B1: red-first regression test + fix in crates/rdlt-connector-rest/src/source/read/mod.rs — `read_children` preserves the incoming SourceError variant (Transient/RateLimited stay intact) while adding parent context; add a `with_context`-style helper instead of `SourceError::fatal(...)`
- [X] T005 [P] [US1] B2: red-first regression test (table `a` vs `ab`) + fix in crates/rdlt-connector-file/src/dest/mod.rs — `count_rows_async` lists with the `"{table}/"` tail and strips the exact prefix (interim guard; the shared `keys_of_table` helper lands in T034)
- [X] T006 [P] [US1] B3 stopgap: sync the diverged `DestSpec` (add `File`/`Iceberg` variants) in crates/rdlt-bench/src/library_mode.rs and add a parity test pinning the CLI and bench spec models against each other until T036/T037 retire both copies (research D-01)
- [X] T007 [P] [US1] B4: red-first regression test + fix in crates/rdlt-connector-postgres/src/source/cursor.rs — out-of-order stream rows surface as a typed Fatal SourceError in all build profiles (replace the `debug_assert!`); `row_key` propagates key-format failures instead of `unwrap_or_default()`
- [X] T008 [US1] B5: replace substring classification in crates/rdlt-connector-duckdb/src/dest/commit.rs with structured `code`/`extended_code` matching per T002's probe (fallback per research D-02 if the probe triggered it); regression test asserts a non-constraint error containing "violate" is NOT classified as precondition failure
- [X] T009 [US1] B6: replace `"401 Unauthorized"` string sniffing in crates/rdlt-connector-iceberg/src/dest/errors.rs with `status` context-value matching per T003's probe (fallback per research D-03); regression test asserts classification survives a reworded Display
- [X] T010 [P] [US1] B7: introduce `const SCOPE_HASH_LEN` shared by crates/rdlt-connector-iceberg/src/dest/dest.rs:84/331 and one `fn state_key(scope)` shared by crates/rdlt-connector-iceberg/src/dest/commit.rs:345/385; test pins write/read round-trip through the shared definitions
- [X] T011 [P] [US1] B8: give duckdb a transient channel — classify open/connect and lock-shaped errors as `DestError::transient` in crates/rdlt-connector-duckdb/src/dest/mod.rs and commit.rs (parse/config stay fatal); regression test with a locked database file
- [X] T012 [P] [US1] B9 (interim): route dest-side object-store errors in crates/rdlt-connector-file/src/dest/mod.rs through the source-side classification in crates/rdlt-connector-file/src/location/s3.rs, and carry typed errors through `S3Reader::read_full` instead of `io::Error::other`; regression test for a mid-stream transient reset (full unification lands in T034)
- [X] T013 [US1] B10: two-pass WAL replay in crates/rdlt-engine/src/wal/resume.rs — pass 1 validates segments (and logs the previously swallowed damage reason), pass 2 streams segments one at a time under the byte budget; regression test bounds recovery RSS for a large uncommitted span
- [X] T014 [P] [US1] B11: repoint the `parse_slab` fuzz target in crates/rdlt-engine/src/fuzzing.rs at `Arena::parse_rows`; move `table::parse_rows` (crates/rdlt-engine/src/shred/table.rs:148-159) under `#[cfg(test)]` or delete it
- [X] T015 [P] [US1] B12: resolve the provenance-hashing doc contradiction in crates/rdlt-core/src/schema.rs — document the current behavior (provenance IS hashed) on `Provenance` and `content_hash`; add a test pinning that a provenance-only change flips `SchemaHash` (the persisted format is unchanged)
- [X] T016 [US1] Checkpoint: full gate (`cargo nextest run`, `cargo test --doc`, `--features failpoints` sweeps) green; close-out.md rows B1–B12 filled with red-run evidence citations (WR6)

**Checkpoint**: US1 deliverable — all defects fixed and pinned; mergeable as plan increment 1.

---

## Phase 4: User Story 2 — Every message and comment stands alone (Priority: P2)

**Goal**: zero citation IDs in user-facing strings; all comments self-contained; rotted citations corrected (R1; constitution V/VI).

**Independent Test**: quickstart WR2 sweep returns zero hits; gate unchanged.

- [X] T017 [US2] Strip citation IDs from all user-facing strings: crates/rdlt-connector-postgres/src/source/cdc/mod.rs ("(contract O1)"/"(contract C2)" sites), crates/rdlt-connector-iceberg/src/dest/schema.rs ("contract ID4" + version-pinned claim in the Replace rejection), crates/rdlt-cli/src/main.rs ("(contract C3)" warnings) — then sweep the whole workspace for remaining hits and fix them
- [X] T018 [P] [US2] Correct the catalogued rotted citations: rdlt-core charter (crates/rdlt-core/src/lib.rs dependency list vs Cargo.toml), rdlt facade feature list (crates/rdlt/src/lib.rs), iceberg "parquet-dest ordering" reference (crates/rdlt-connector-iceberg/src/dest/dest.rs), engine channel.rs "wired by US1 (T024)" note, sqlcore "MOVED VERBATIM" headers
- [X] T019 [P] [US2] Self-containment pass, engine: rewrite/delete spec-citation comments across crates/rdlt-engine/src (62 hits/20 files incl. workdir "US3" doc, table.rs module-doc history lesson); each surviving comment states its rule inline
- [X] T020 [P] [US2] Self-containment pass, postgres: crates/rdlt-connector-postgres/src (rustdoc spec paths, dest/*.rs relocation breadcrumbs, stale "rdlt-source-postgres" crate name in source/mod.rs)
- [X] T021 [P] [US2] Self-containment pass, sqlcore + duckdb + iceberg: "013 review finding N" citations, `legacy_unique_index_name` branch-history comment, iceberg spec-citation cluster
- [X] T022 [P] [US2] Self-containment pass, core + connector + facade + file + rest: rdlt-core citation cluster (~20 files), stream.rs changelog-in-docs, file crate spec paths (incl. the `…`-character path) and stale lib.rs/source docs, REST "The S3 classification" mislabel + overclaiming OAuth2 comment + stale "~80 lines" metric
- [X] T023 [P] [US2] Self-containment pass, testkit + bench + cli: conformance/mod.rs spec path, stale "crash-matrix row 2" double-citation (verify against the current matrix), bench "review finding N" sites
- [X] T024 [US2] Verify WR2: run the quickstart citation sweep (user-facing strings and comments) — zero hits; record the command output in close-out.md row R1

**Checkpoint**: US2 deliverable — mergeable as plan increment 2.

---

## Phase 5: User Story 3 — One source of truth for correctness-critical logic (Priority: P3)

**Goal**: the five named live duplications collapse to single sources (R3, R6, R2, R7, B3-structural); behavior pinned by golden SQL, conformance, and crash sweeps throughout.

**Independent Test**: WR3 greps find former definitions gone; golden pins byte-identical; crash sweeps duplicate-free.

- [X] T025 [US3] R3: create `rdlt_connector::secret::Secret` in crates/rdlt-connector/src/secret.rs (newtype, masking Debug/Display, transparent serde, From impls; schemars behind new SPI feature `schema` per research D-05) with a grep-proof `reveal()` audit surface
- [X] T026 [US3] R3: migrate rest/file/iceberg to the shared Secret (delete crates/rdlt-connector-rest/src/source/client/secret.rs, crates/rdlt-connector-file/src/location/secret.rs, the copy in crates/rdlt-connector-iceberg/src/dest/config.rs; keep old paths as re-exports); document the headers/params redaction posture in crates/rdlt-connector-rest/src/source/config.rs and record the validate-time-warning decision in close-out.md
- [X] T027 [US3] R6: extract `apply_delta`/`apply_batch` into crates/rdlt-engine/src/load/apply.rs per data-model.md §2.4 (the lower_schema→ensure_table→record-hash triple and lower_batch→write pair)
- [X] T028 [US3] R6: consume the helpers from `Loader::process` (crates/rdlt-engine/src/load/mod.rs) and `replay` (crates/rdlt-engine/src/wal/resume.rs), removing the redundant double table-ensure; crash-sweep suite re-run as the behavior pin
- [X] T029 [US3] R2 prerequisite: split `PgSession::commit` in crates/rdlt-connector-postgres/src/dest/commit.rs into `handle_replay`/`publish_table` with a `MergeCtx` struct (R4 row), behavior-identical, golden pins unchanged
- [X] T030 [P] [US3] R2 prerequisite: split `DuckDbSession::commit` in crates/rdlt-connector-duckdb/src/dest/commit.rs into `replay_committed_unit`/`publish_table`/`check_single_unit` (R4 row), behavior-identical
- [X] T031 [US3] R2: lift the mechanically duplicated helpers into crates/rdlt-connector-sqlcore — one `quote` (delete duckdb's local dialect-bypassing copy), `column_list`, `root_of` with a named constant for the magic 64, the index-name hash formula, `hard_delete` resolution, the `MergePlan` construction literal, `scoped`/`retire`, Append/Replace INSERT-SELECT, DELETE-FROM-stage, `setting()`/`extension()`
- [X] T032 [US3] R2: implement the protocol planner `commit_script(tables, options, replayed) -> Vec<Step>` in crates/rdlt-connector-sqlcore (Step enum per data-model.md §2.3; pure, no driver types); add golden pins covering the emitted step script for both dialects
- [X] T033 [US3] R2: make postgres and duckdb sessions execute the planner's steps (delete the per-destination protocol logic in both commit.rs files); existing golden-SQL pins and both crates' conformance suites pass unchanged
- [ ] T034 [US3] R7: unify `Location` in crates/rdlt-connector-file/src/location/ with read+write halves absorbing dest `Store`, one shared error-classification fn (canonicalizing the source rulebook — completes B9), and one `keys_of_table` ownership helper used by counting and truncation (completes B2's root-cause fix; retires T005's interim guard)
- [ ] T035 [US3] R7: migrate crates/rdlt-connector-file/src/dest/mod.rs onto the unified location; move `FileMeta`/`FileTask`/`FileProgress` from source/cursor.rs into location/ types (kills the upward import); RUSTFS container cells + conformance pass unchanged
- [ ] T036 [US3] B3 structural: create `rdlt::pipeline_spec` in crates/rdlt/src/pipeline_spec.rs — the YAML spec model, Spec→Pipeline construction, and `is_json` helper, supporting all destination kinds
- [ ] T037 [US3] B3 structural: CLI (crates/rdlt-cli/src/main.rs) and bench (crates/rdlt-bench/src/library_mode.rs) consume `rdlt::pipeline_spec`; delete both local copies and retire T006's parity test (superseded by the single model)

**Checkpoint**: US3 deliverable — WR3 counts reach 1 for all five duplications.

---

## Phase 6: User Story 4 — Structural decomposition (Priority: P4)

**Goal**: every R4 table row split along its seams (those not already done in US3), R5 validation monoliths decomposed, R11 context structs in place.

**Independent Test**: R4/R5/R11 close-out rows applied-or-deviation-recorded; gate green; public API unchanged.

- [X] T038 [US4] Engine: decompose `run_once` in crates/rdlt-engine/src/runtime/graph.rs into `validate_streams`/`recover_wal`/named `stream_task`/`drain_loader`; introduce the ping-pong owner type (run_blocking takes self, returns Self — removes the shredder/registry expects); rename module to crates/rdlt-engine/src/runtime/run.rs
- [X] T039 [P] [US4] Engine complexity items: split `build_scalar` (crates/rdlt-engine/src/shred/build.rs) per logical type; pair the three index-aligned slices in `drain_tables` into a `TableDrain` struct; extract `shred_root`/`enqueue_children` from `push_and_drain`; unify `write_compact_json` with `canonical_json_bytes` (one parameterized serializer) and add `append_hex_id`
- [ ] T040 [P] [US4] Postgres: split crates/rdlt-connector-postgres/src/tls.rs into tls/{policy,connstring,rustls_config,connect}.rs; isolate + pin the string-sniffing TLS refusal check
- [ ] T041 [US4] Postgres: split crates/rdlt-connector-postgres/src/source/cdc/mod.rs into cdc/{runtime,read,tail,apply}.rs; extract the duplicated COPY pump (`pump_copy`) and the twice-written Emit delivery loop
- [ ] T042 [US4] Postgres: move the 120-line cursor arm from `PostgresSource::read` (crates/rdlt-connector-postgres/src/source/mod.rs) into cursor.rs as `IncrementalPlan::prepare`; extract `prepare_stream` shared by `streams()`/`read()` and `ReflectedTable::effective_pk` (3 copies)
- [ ] T043 [US4] File: split crates/rdlt-connector-file/src/dest/mod.rs into dest/{session,layout,truncate,inspect}.rs (store absorbed by T034); extract the four comment-delimited phases of `FileSession::commit`; extract the jsonl `SlabReader` (read_task vs read_task_whole ~45-line duplicate) in crates/rdlt-connector-file/src/formats/jsonl.rs
- [ ] T044 [P] [US4] REST: split crates/rdlt-connector-rest/src/source/read/mod.rs into read/{driver,fanout}.rs; move `substitute_body` into resolve.rs; extract `build_page_request` from `fetch_page` with one `match (method, body)`
- [ ] T045 [US4] Iceberg: split crates/rdlt-connector-iceberg/src/dest/commit.rs into {catalog,writer,commit,state,ensure}.rs; extract `check_mode`/`reinstall_state` from `ensure_table` (cheap reserved-name check first); extract pure `catalog_props` from `connect` (concentrates the reveal() audit surface); one `From` impl for the duplicated PartitionTransform match
- [X] T046 [P] [US4] sqlcore: split crates/rdlt-connector-sqlcore/src/plan.rs into plan/{mod,arms,validate,index}.rs; decompose `ensure_table` (DDL/migration/scd2/indexes) and `scd2_merge_sql` (shared scope-clause + literal-escaping helpers); name the `index_plan` tuple as `IndexSpec`
- [ ] T047 [P] [US4] Testkit: split `MemorySession::commit` in crates/rdlt-testkit/src/memory/dest.rs into `apply_append`/`apply_replace`/`apply_merge_keyed`/`apply_merge_by_id`; add the `try_step!` helper collapsing the ~8 push-failure repetitions in `verify_destination`
- [ ] T048 [US4] CLI: split crates/rdlt-cli/src/main.rs into cdc.rs (warnings) + main.rs (args/drive) with per-destination `build_*` fns replacing the 109-line embedded macro (spec model already moved by T037)
- [ ] T049 [P] [US4] Bench: split `cmd_run` into `prepare`/`run_one_cell`/`print_run_summary` sharing bar evaluation with `cmd_gate`; split `fixtures::start` (extract `start_container`) and `run_once_subprocess`; restructure `run_cell` to yield-then-build (removes `return_side`'s 8-arg signature, the allow, and the dead `_paths` param); move `Paths`/`substitute` out of runner.rs into paths.rs/template.rs; generic `load_toml<T>`; `last_json_field` helper
- [ ] T050 [P] [US4] R5 postgres: decompose validate (crates/rdlt-connector-postgres/src/source/config.rs) into `validate_conn`/`validate_cursors`/`validate_cdc`/`validate_tables`
- [ ] T051 [P] [US4] R5 rest + iceberg: decompose crates/rdlt-connector-rest/src/source/config.rs validate into `validate_stream_aliases`/`validate_selectors`/`validate_response_actions`/`validate_parent`; crates/rdlt-connector-iceberg/src/dest/config.rs into `validate_catalog`/`validate_namespace`/`validate_tables`
- [ ] T052 [P] [US4] R5 sqlcore + file: one `check_*` fn per rule group in crates/rdlt-connector-sqlcore/src/options.rs and plan/validate.rs; unify the file crate's four `validate()` conventions and error-text prefix
- [ ] T053 [P] [US4] R11 context structs: `ShredCtx { registry, load_id, mode, policy }` with one field order for both construction sites (crates/rdlt-engine/src/shred/{tape.rs,passthrough.rs}); `TableCtx` for the 5 suppressed 6-arg-prefix functions in crates/rdlt-connector-postgres/src/source/cdc/; reduce `Loader::new`'s 9 args via the same structs

**Checkpoint**: US4 deliverable — all splits applied or deviations recorded.

---

## Phase 7: User Story 5 — Honest taxonomy, panic-free library paths (Priority: P5)

**Goal**: R8 alignment (incl. `DestError::RateLimited`), R9 panic elimination, taxonomy verified by fault injection (WR4, WR5).

**Independent Test**: fault-injection over catalogued sites — zero panics, zero recoverable-mistyped-as-fatal.

- [X] T054 [US5] R8: add `RateLimited` to `DestError` in crates/rdlt-connector/src/error.rs (additive on the `#[non_exhaustive]` enum, mirrors SourceError with retry-after); handle it in the engine retry loop (crates/rdlt-engine/src/runtime/run.rs) like the source path; map iceberg REST-catalog 429/vending-expiry to it in crates/rdlt-connector-iceberg/src/dest/errors.rs
- [X] T055 [P] [US5] R8 sqlcore: replace `Result<_, String>` validation with small typed enums with frozen Display text in crates/rdlt-connector-sqlcore/src/{options.rs,plan/validate.rs} (messages pinned by existing tests)
- [ ] T056 [P] [US5] R8/R9 rest: give the public `Paginator` trait a typed error in crates/rdlt-connector-rest/src/source/read/paginate.rs; run `validate()` in `RestSource::new`/make `from_config` return Result (kills the 6 "validated at config parse" expects); add `Pagination::selector_paths()` removing the double-destructure; drop the redundant defensive checks
- [ ] T057 [P] [US5] R8 postgres: classify DDL errors via the shared `is_transient_sqlstate` (no more all-Transient DDL) in crates/rdlt-connector-postgres/src/dest/commit.rs; extract the triplicated `pg_error_detail` + twice-written SQLSTATE list into one module; unify the three decode error conventions (values.rs/pgoutput.rs/copy_decode.rs) on thiserror
- [X] T058 [P] [US5] R8 engine: correct error-variant misuse — task panics no longer `RdltError::config` (crates/rdlt-engine/src/runtime/run.rs), workdir-lock failures no longer `RdltError::wal` (runtime/lock.rs), `RecordsOut::rows` serialization failure no longer `ChannelClosed` (crates/rdlt-connector/src/lib.rs); document the channel byte-budget u32 clamp and clock fallback
- [ ] T059 [US5] R9 iceberg: one parameterized `commit_with_retry` helper in crates/rdlt-connector-iceberg/src/dest/commit.rs replacing the triplicated divergent retry scaffolding (unifies loop shape, backoff base, `already_committed` re-check; kills both `unreachable!` tails); add real jitter or fix the "Jittered" naming/comment
- [ ] T060 [P] [US5] R9 postgres: give CDC `RunState` `ensure_control`/`ensure_snapshot` methods so the ~10 hand-tracked `.expect("control client")` calls live in one audited place (crates/rdlt-connector-postgres/src/source/cdc/runtime.rs); fix `streams()` panicking on missing reflection entry where `read()` handles it
- [ ] T061 [P] [US5] R9 cross-module invariants → typed internal errors: postgres copy_decode.rs decimal-shape expect, sqlcore plan.rs `expect("hard_delete present")` (resolved structurally by T031's helper), duckdb commit.rs `expect("scd2 options resolved")`; file `Store::s3_list` `unreachable!` is retired by T034 — record in close-out
- [ ] T062 [US5] WR5 verification: fault-injection/conformance pass over all catalogued panic and misclassification sites — zero panics in library code, classification matches the taxonomy per connector; evidence into close-out.md rows R8/R9

**Checkpoint**: US5 deliverable — taxonomy honest workspace-wide.

---

## Phase 8: User Story 6 — One voice: naming, dead code, constants (Priority: P6)

**Goal**: R13 constants centralized, R12 dead code resolved, R10 renames applied/shimmed/deferred.

**Independent Test**: close-out rows show every R10/R12/R13 item terminal; paired literals change in one place.

- [X] T063 [P] [US6] R13 constants: single `SLAB_BYTES` in the file crate (crates/rdlt-connector-file — csv.rs/jsonl.rs/source mod.rs); named channel-capacity constants (rdlt-connector lib.rs 64, engine 4096/256, testkit 16<<20 ×2); iceberg REST-catalog property-key constants extending the existing snapshot-key discipline (crates/rdlt-connector-iceberg/src/dest/catalog.rs); rename `default_wal_version` → `initial_wal_version` with the deliberate-pin doc (crates/rdlt-engine/src/wal/mod.rs)
- [X] T064 [P] [US6] R12 core + engine: delete `flattened_column_name` (crates/rdlt-core/src/naming.rs, zero callers, false test-comment claim); make `needs_lowering`/`arrow_scalar_type`/`ArrayShape` private; remove `channel.rs` `#![allow(dead_code)]`; move the byte-budget channel subsystem from crates/rdlt-connector/src/lib.rs root into channel.rs; unify `CommitCounters`/`TableReport` (crates/rdlt-core/src/{commit.rs,report.rs}); `to_hex` via `write_hex`
- [X] T065 [P] [US6] R12 postgres + duckdb: unify on one canonical streaming peek (fix the inconsistent parameter binding at cdc read.rs while removing dead `slot::peek`/`slot::Change` or making them the canonical impl); drop parsed-but-never-read pgoutput fields; narrow `tls::resolve_policy`/`classify_connect_error` to pub(crate); gate duckdb `count_rows`/`query_string` behind a test feature or `#[doc(hidden)]`
- [X] T066 [P] [US6] R12 rest + iceberg + file: drop REST `SequenceDriver::started`/`last_count` write-only fields; tighten `RestClient` pub fields and the `read::{extract,resolve}` public tree; parse headers once in `RestClient::new`; remove iceberg `from_json`/`with_catalog_prop`/`_uses` shim and add missing `#[must_use]` on IcebergConfig builders; decide file `format_version` (enforce or drop) and `ParquetDir` deprecation intent; single canonical `Format` re-export; narrow pub-but-internal file constants
- [X] T067 [P] [US6] R12 + polish bench: remove `BenchError::msg`, `Variant.role` (or implement its documented gating), `Bar.policy`, `VerifyOutcome.ok`; pub→crate-internal for `rdlt_side`/`begin_marker`/`end_marker`; derive ValueEnum on `Class` + Display for `Mode`, delete `ClassArg`; fingerprint pins as `BTreeMap<variant, pin>`; rename `hash`→`hash_files`, `data`→`data_dir`; fix the malformed message in main.rs, name offenders at bare `?` sites, stop `load_cells` silently dropping unreadable entries, cross-validate `reset_sql`-without-container at load time
- [ ] T068 [P] [US6] R10 non-breaking renames: `format_lsn`, `rollback_snapshot` (pre_batch/snapshot/pre_batch_snapshot), `ArenaNode`/`StoredNode`, `name_map` direction names, `TapeShredder` decision (rename or define "tape" once), postgres timestamp spellings + one `Bound` enum, `RootCert`→`PemSource`, file `key`/`root` vocabulary + `ended_at_record_boundary` (serde rename) + `Unit` newtype for size/done + failpoint prefixes into `Store::Local` arms, testkit `truncated_tables`/`replaced_root_ids`/`fatal_after`/`CrashDestination`, iceberg `session.rs`/`window_seq`/writer-builder de-stutter/`read_state_doc`, CLI `CliError::Config`, REST `kind` for `action.action`
- [ ] T069 [US6] R10 aliasable renames: `DestinationError`/`DestinationCapabilities` type aliases with `#[deprecated]` on the old names (crates/rdlt-connector/src/error.rs); `OAuth2ClientCredentials` + `Pagination::BodyCursor` via serde alias/rename keeping wire compat (crates/rdlt-connector-rest/src/source/config.rs); builder-idiom normalization where non-breaking
- [ ] T070 [US6] R10 deferrals: record the named deferrals to the 0.2→0.3 window in close-out.md (`merge_key`→`merge_scope` config vocabulary, `ColumnDef.ty`→`column_type`, duckdb vs postgres re-export prefix convention, prelude/root re-export unification, `MergePlan` field `*_sql` renames) — each row cites the window per WR8

**Checkpoint**: US6 deliverable — one voice, breaking changes safely staged.

---

## Phase 9: User Story 7 — Build, CI, and test-support surfaces share the discipline (Priority: P7)

**Goal**: D1–D15 single-source state (FR-025); uniform skip-not-fail; delivery pins agree by construction.

**Independent Test**: gate runs with and without a container runtime produce uniform visible skips; WR3 delivery-surface greps pass.

- [X] T071 [US7] D1/D2: create `rdlt_testkit::containers` in crates/rdlt-testkit/src/containers.rs — `runtime_available()` superset probe (env override → docker/podman sockets → `podman ps`) and `PgFixture::start() -> Option<PgFixture>` + `CdcPgFixture` (skip-not-fail posture; `16-alpine` tag, port, conn-string template defined once per data-model.md §2.6)
- [X] T072 [P] [US7] D4: create `rdlt_testkit::fixtures` in crates/rdlt-testkit/src/fixtures.rs owning `batch_of`/`schema_for`/`meta_for` (canonical single-`id` schema, Arrow batch, CommitMeta/StateDoc builders); build the Arrow schema from the TableSchema (kills the 15-lines-apart sync hazard)
- [X] T073 [US7] D2/D3/D4: migrate postgres + duckdb tests to the shared containers/fixtures — delete `start_pg()` copies in crates/rdlt-connector-postgres/tests/{dest_conformance.rs,scd2.rs,dest_recovery.rs,dest_crash_sweep.rs,common/mod.rs} and crates/rdlt-connector-duckdb/tests/{differential.rs,recovery.rs}; the `.expect()` hard-fail posture becomes skip-not-fail everywhere
- [X] T074 [P] [US7] D1/D4: migrate file + iceberg tests to the shared probe/fixtures (crates/rdlt-connector-file/tests/common/s3.rs, crates/rdlt-connector-iceberg/tests/common/mod.rs + the six fixture-trio sites)
- [X] T075 [P] [US7] D5: add `stream_yaml(uri, path, extra)` builders and migrate the ~44 near-duplicate YAML scaffolds in crates/rdlt-connector-rest/tests/{actions.rs,pagination.rs} and crates/rdlt-connector-file/tests/jsonl.rs
- [X] T076 [P] [US7] D6/D8/D9/D10: create .github/actions/free-disk/action.yml and reference it from all 5 jobs in .github/workflows/{ci.yml,deep-checks.yml} plus the semver job; drop the redundant `@stable` toolchain installs (keep `@nightly` for fuzz only); one canonical disk-rationale comment; document the deep-checks RUSTFLAGS divergence
- [X] T077 [P] [US7] D7/D13/D14: move `iai-callgrind` and `libc` to `[workspace.dependencies]` (Cargo.toml + crates/rdlt-engine/Cargo.toml, crates/rdlt-connector-postgres/Cargo.toml, crates/rdlt-cli/Cargo.toml); add `rust-version = "1.96"` to `[workspace.package]` inherited per-crate; add the CI version-agreement check for the iai runner (research D-13); drop implied features (`postgres-source`, `file`) from CLI/bench lists; trim rdlt-connector's redundant tokio features
- [X] T078 [P] [US7] D11/D12: hoist shared defaults in benches/competitors/dlt/variants.toml (pin/image once) and benches/fixtures/fixtures.toml (postgres image + conn template once); dedupe `reset_sql`; mark or normalize the exec-only fixtures' missing `conn`; make merge_prepare.sh's strategy arg optional (drop `strat_duck_unused`); pick one cell-id convention for the strategy family in benches/cells/merge.toml
- [X] T079 [P] [US7] D15 + prose: `git rm -r --cached mutants.out.old/` (keep the ignore entry); correct CLAUDE.md drift (rustc floor → 1.96, arrow → 58.3) in the 016 reference block; record the root-README decision (minimal README before publish) in close-out.md
- [X] T080 [US7] Posture verification: run the gate once with the container runtime stopped and once with it running — uniform visible skips, zero panics, identical pass set otherwise; evidence into close-out.md rows D1/D2

**Checkpoint**: US7 deliverable — delivery surfaces single-source.

---

## Phase 10: Polish & Close-Out

- [ ] T081 [P] Re-run the WR2 citation sweep and WR3 single-source greps (quickstart) — zero hits/one-definition confirmed after all increments; outputs cited in close-out.md
- [ ] T082 [P] Final coverage run (`make coverage`) — at or above the T001 baseline (SC-004); number recorded in close-out.md header
- [ ] T083 Complete specs/017-workspace-refactoring/close-out.md — every catalogue row terminal with non-empty evidence (FR-023/WR7); empty-cell grep returns nothing
- [ ] T084 Final full gate: `cargo nextest run`, `cargo test --doc`, failpoint sweeps, container cells (both runtime postures), `make bench TARGET=gate`; bench RESULTS.md regeneration diff-clean for untouched cells (WR8)
- [ ] T085 Record REFACTORING.md's disposition (append a header note pointing at close-out.md as the execution record); update the CLAUDE.md SPECKIT block if any plan-level decision changed during implementation

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: T001 first; T002/T003 parallel after it.
- **US1 (Phase 3)**: T004–T007, T010–T015 independent [P]; T008 needs T002; T009 needs T003; T016 last.
- **US2 (Phase 4)**: independent of US1 (textual only); T024 after T017–T023.
- **US3 (Phase 5)**: T026 after T025; T028 after T027; T031–T033 after T029+T030 (splits make the copies visibly identical before lifting); T034 retires T005/T012 interim guards; T035 after T034; T037 after T036 and retires T006.
- **US4 (Phase 6)**: T038 benefits from T027/T028 (apply helpers shrink run_once); T043 after T034/T035; T048 after T037; the rest independent.
- **US5 (Phase 7)**: T054 engine wiring lands in the module T038 renames; T059 within T045's split layout; T061 partially resolved by T031/T034 (record, don't redo); T062 last.
- **US6 (Phase 8)**: T063–T068 [P]; T069 after T068; T070 anytime before close-out.
- **US7 (Phase 9)**: T073/T074 after T071+T072; T080 last in phase.
- **Close-out (Phase 10)**: after all selected stories; T083/T084 final.

### Increment Mapping (recommended execution path = plan.md sequence)

| Plan increment | Tasks |
|---|---|
| 1 defect fixes | T001–T016 |
| 2 citation sweep | T017–T024 |
| 3 mechanical sweep | T063–T067 (R13/R12), T076–T079 (D6–D15) |
| 4 shared infra | T025–T026 (Secret), T071–T075, T080 (testkit) |
| 5 engine | T027–T028, T038–T039, T053(engine), T058 |
| 6 sqlcore | T029–T033, T046, T052(sqlcore), T055, T061 |
| 7 postgres | T040–T042, T050, T057, T060, T053(postgres), T065 |
| 8 file | T034–T035, T043, T052(file), T066(file) |
| 9 rest | T044, T051(rest), T056, T066(rest) |
| 10 iceberg | T045, T051(iceberg), T059, T066(iceberg) |
| 11 naming | T068–T070 |
| 12 cli/bench | T036–T037, T048–T049, T067 |
| close-out | T081–T085 |

Story phases stay independently completable; the mapping above is the drift-minimizing merge order (WR8) — each row is one mergeable increment with the full gate green.

## Parallel Example: User Story 1

```bash
# After T001–T003, launch the independent defect fixes together:
Task: "T004 B1 rest fan-out classification preservation"
Task: "T005 B2 file count prefix guard"
Task: "T007 B4 postgres cursor typed ordering error"
Task: "T010 B7 iceberg shared literals"
Task: "T011 B8 duckdb transient channel"
Task: "T014 B11 fuzz repoint"
Task: "T015 B12 provenance doc"
```

## Implementation Strategy

- **MVP = US1** (T001–T016): every latent defect fixed and pinned — standalone value even if nothing else ships.
- **Incremental delivery**: follow the Increment Mapping table; merge each row with the gate green; update close-out.md rows as evidence lands (not retroactively at T083).
- **Behavior discipline**: WR1 applies to every task — golden pins, conformance clauses, and crash sweeps are the refactoring safety net; a pin change is a stop-and-investigate signal, never a test update.
