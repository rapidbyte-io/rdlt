# Close-Out Matrix: Workspace Refactoring Program

Contract WR7: every catalogue item reaches a terminal disposition
(`applied` / `shimmed` / `deferred` / `overtaken`) with non-empty evidence.
R-theme rows expand to Part 3 sub-items at the granularity the tasks touch
them; a row is filled when its task completes, not retroactively.

**Coverage baseline (pre-feature, `cargo llvm-cov nextest --features
failpoints` on merge base + rustfs pin, 595 tests green)**: **83.68% lines**
(82.48% regions, 78.04% functions) — recorded 2026-07-24. SC-004 compares
final line coverage against 83.68%.

**Red-run evidence method**: per research D-14 (test-before-fix ordering or
stash-red capture; excerpts inline or cited by test name + run).

**Scope amendment (user directive, 2026-07-24, during increment 4)**:
GREENFIELD — no legacy paths, no compatibility shims, no deprecated
aliases. Nothing is published, so renames and moves land directly and
consumers update in the same change. Supersedes research D-10's
"deprecated aliases for one window" bucket (breaking renames now apply
directly in increment 11) and the spec assumption about alias-shims;
named deferrals to the 0.3 window remain only for items NOT worth doing
at all now (none currently). Persisted **data** formats (WAL, StateDoc,
receipts) and golden pins remain frozen — the directive covers API
paths/names and in-repo vocabulary, not on-disk data written by runs.

## Part 1 — Defects

| Item | Increment | Disposition | Evidence |
|---|---|---|---|
| B1 | 1 | applied | `with_parent_context` preserves Transient/RateLimited (incl. retry_after) through child fan-out; only fatal stays fatal. Red captured: `child_retryable_failure_keeps_classification` FAIL pre-fix — "HTTP 500 child failure must stay retryable, got Fatal(…transient source error: HTTP 500…)" (the upstream classification visibly wrapped inside Fatal). Green: rest suite 68/68. Note for the R8 pass: `SourceError::RateLimited`'s Display omits its inner message — parent context on rate-limited children is only visible via the source() chain (pre-existing; candidate alongside T054) |
| B2 (interim guard) | 1 | overtaken | Catalogue false positive: object_store 0.12.5 appends `/` to the server-side list prefix (`client/list.rs:72`) — segment semantics, `out/a` cannot match `out/ab/`. Pinned by `rdlt-connector-file::prefix_semantics::list_prefix_is_segment_based_not_byte_based` (PASS) |
| B2 (root cause, keys_of_table) | 8 | | Still worthwhile as shared-helper consolidation under R7 (dedup, not defect) |
| B3 (stopgap sync + parity pin) | 1 | applied | Bench `DestSpec` gained `File`/`Iceberg` + construction arms (library_mode.rs); shared fixture `benches/parity_specs.yaml` (5 docs, every dest kind) pinned by `shared_parity_specs_all_parse` in BOTH rdlt-cli and rdlt-bench (PASS ×2). Red: pre-sync the bench parser had no such variants (catalogue-verified drift) |
| B3 (structural, rdlt::pipeline_spec) | 12 | | |
| B4 | 1 | applied | Ordering violation now typed Fatal in all profiles; `row_key` renders through fallible `render_cell`. Red: `out_of_order_arrival_fails_in_all_profiles` panicked on the old `debug_assert` (captured). The "adjacent" key-format defect proved LIVE, not latent: arrow cannot display `Timestamp(_, Some("UTC"))` without a tz database, so EVERY timestamptz boundary key was silently an empty colliding component — surfaced by `pkless_table_dedups_via_row_hash` failing once rendering became fallible; fixed by re-labeling zoned→naive (instant unchanged) before display. Pins: `row_key_threads_and_composes`, `zoned_timestamp_keys_are_distinct_and_nonempty` (fails against pre-fix empty-component behavior), full pg suite PASS. Note: boundary keys for timestamptz cursors change value (empty→rendered) — a defect-confined behavior change per WR1; old keys could only over-deliver boundary rows, and dedup is per-run |
| B5 | 1 | applied | Probe `error_codes.rs` (2 PASS): structured channel degenerate → designed fallback (research D-02). Fix: `is_constraint_violation` prefix classifier applied on the library error pre-wrap; broad `"violate"` needle deleted. Red: pre-fix classifier absent (E0425 stash-run) — old logic was inline/untestable; regression `violation_wording_in_other_errors_is_not_misdiagnosed` (PASS) |
| B6 | 1 | applied | `status_from_context` parses the `status:` CONTEXT ENTRY from the pinned Display form (anchored on the `, context: { ` block — a status quoted in a response BODY renders after the ` => ` marker and cannot match); 401/403→fatal, else transient; user-facing wording byte-identical. Probe facts: `iceberg-catalog-rest-0.10.0/src/client.rs:343` attaches the context; no public getter (research D-03 fallback). Red captured: `body_text_status_without_context_stays_transient` FAIL pre-fix (5xx whose body quotes "401 Unauthorized" was misclassified fatal). Green: iceberg 48/48 with live container legs incl. `auth_probe::live_auth_rejection_classifies_fatal` (real 401 end-to-end) |
| B7 | 1 | applied | One `const SCOPE_HASH_LEN` (dest.rs, both sites) + one `fn state_key(scope)` (commit.rs, write+read); pin `state_key_write_and_read_agree`. Iceberg suite 117/117 PASS incl. exactly-once container legs |
| B8 | 1 | applied | `classify` (IO-prefix → Transient) at open/appender/tx sites. Red: `unopenable_file_is_transient_not_fatal` FAIL with `fatal` at open, PASS with `classify` (captured) |
| B9 (interim dest classification) | 1 | applied | One recoverability rule `location::s3::is_recoverable` shared by source classify + new dest `store_err` (8 object_store sites rewired: get/put/list/delete/copy incl. staged put + count reads); `S3Reader::read_full` carries recoverability via ConnectionReset kind; consumers classify through `classify_read_error` (3 sites). Pin `store_error_recoverability_is_shared_and_honest` PASS. Red: helpers absent pre-fix (compile-fail shape, same caveat as B5) |
| B9 (root cause, unified Location) | 8 | | |
| B10 | 1* | applied | Two-pass replay: pass 1 decodes every segment batch-at-a-time and drops (validation without retention — `Vec<LoadItem>` buffer deleted), pass 2 streams through the session; damage reasons now logged (`tracing::warn!`) instead of swallowed. RSS bound is structural (no span-sized collection exists; grep `Vec<LoadItem>` in resume.rs = 0). Engine suite 75/75 PASS incl. recovery tests. *Landed early (increment 1) — the split (T038) no longer gates it |
| B11 | 1 | applied | `parse_slab` fuzz target now drives `Arena::parse_rows` (production parser); `table::parse_rows` gated `#[cfg(test)]` as the arena's differential oracle (its only remaining caller). Oracle test `arena_and_value_agree_on_canonical_bytes` PASS |
| B12 | 1 | applied | `Provenance` doc corrected to actual persisted semantic (provenance IS hashed); pin `provenance_participates_in_the_hash` PASS; hash bytes unchanged |
| B13 (new — YAML transform spelling) | 1 | applied | Red: parity fixture rejected documented `transform: {bucket: 16}` ("expected a YAML tag starting with '!'", captured). Fix: `singleton_map` on `PartitionField.transform` + `#[schemars(with)]`; pins `yaml_transform_spellings_parse` + both parity tests PASS; JSON path re-verified by existing `bucket_and_truncate_spellings_and_validation` PASS |

## Part 2 — Cross-cutting themes

| Item | Increment | Disposition | Evidence |
|---|---|---|---|
| R1 user-facing citation strip | 2 | applied | All catalogued sites + more found in-flight: pg cdc "(contract O1)/(C2)", iceberg "contract ID4" + version-pinned Replace claim (actually in dest.rs, both fixed), CLI "(contract C3)" ×5, PLUS 6 engine error strings (clause E7/B4/S7 etc.), 7 sqlcore validation strings (MR6/M2/M7/S1/S6/S8), file "(clause E1)/(S7)". Verification grep over src string literals: 0 hits. Two duckdb tests that substring-matched citation IDs in rendered errors (the forbidden pattern) repointed at stable text |
| R1 rotted-citation corrections | 2 | applied | All 9 catalogued: core charter now matches Cargo.toml; facade doc gained iceberg/postgres-dest; SPI persisted-formats citation restated inline; file `…`-path doc replaced; file lib/source docs now name CSV/dest/location; rest "S3 classification" = mis-transcribed clause → real 429/5xx rule; OAuth2 comment now truthful (fresh client, shared classification only); naming.rs false-caller claim fixed; stream.rs changelog narrative dropped. Also: crash.rs "row 2 ×2" resolved — points ARE different stages, now described by behavior; iceberg "parquet-dest ordering" → actual state-last rule |
| R1 self-containment: engine | 2 | applied | 19 files; history lessons deleted (table.rs), stale "US1 (T024)" replaced with current fact, unenumerated invariant-N/FR-0xx tags inlined. Kept: in-file "view contract" (enumerated same file). 75/75 |
| R1 self-containment: postgres | 2 | applied | 22 files (379+/419−), residual grep 0; stale "rdlt-source-postgres" name fixed; relocation breadcrumbs gone; cross-crate assertion on "scd2.md S6" repointed |
| R1 self-containment: sqlcore/duckdb/iceberg | 2 | applied | "MOVED VERBATIM" headers → live golden-pin invariant statement; legacy_unique_index_name branch history → live constraint; duckdb src + iceberg src&tests swept. Flagged remainder: duckdb/tests comment tags (T0xx/SMx) — deferred to increment 6 tasks touching those files |
| R1 self-containment: core/connector/facade/file/rest | 2 | applied | 42 files across 5 crates; residual grep 0; 185/185 incl. S3 live leg |
| R1 self-containment: testkit/bench/cli | 2 | applied | Conformance clause IDs D1–D8/S1–S6 AND printed E-clauses KEPT (crate-own vocabulary, defined+printed in-crate — the good pattern); bench BH/FR rules inlined; library_mode duplication-constraint doc now cites the parity fixture. 3 documented keeps remain workspace-wide (testkit E1/E6 comments, bench cells.rs test-data policy path). 52/52 |
| R2 commit splits (pg + duckdb) | 6 | | |
| R2 mechanical helpers (quote/column_list/root_of/index-name/hard_delete/MergePlan/scoped/retire/insert-select/delete-stage/setting) | 6 | | |
| R2 protocol planner commit_script | 6 | | |
| R2 destinations execute planner | 6 | | |
| R3 shared Secret in SPI | 4 | applied | `rdlt_connector::secret::Secret` (newtype, `***` mask, transparent serde, `reveal()` audit surface) with schemars behind new SPI feature `schema` (optional dep; SPI builds with/without). Copies differed only in schemars description text — majority form adopted (nothing pins it) |
| R3 three copies migrated + re-exports | 4 | applied (greenfield) | Per user directive mid-increment: NO legacy paths — both secret.rs shims deleted, all module-path chains killed; one canonical spelling per crate (rest/iceberg root re-export; file uses the SPI path — it never exported one). Grep-zero over every old path; `reveal()` production sites byte-identical (13); 196/196 + 140/140 after cleanup |
| R3 headers/params redaction posture | 4 | applied | Doc comments on both maps (credentials belong in `auth:`) + validate() REJECTS `authorization`/`x-api-key` header names (case-insensitive, source+per-stream) with a typed error pointing at `auth:`. Tests `credential_header_names_are_rejected_toward_auth`, `ordinary_headers_still_accepted` |
| R4 engine run_once split + ping-pong owner | 5 | applied | graph.rs → run.rs (no shim, greenfield); run_once 388→105 lines via `validate_streams`(56)/`recover_wal`(62)/named `stream_task`(120)/`drain_loader`(65); `ShredOwner` consumes-self/returns-Self — both expects deleted, panic-free by construction. 80/80 failpoints green after every step |
| R4 postgres tls.rs split | 7 | | |
| R4 postgres cdc/mod.rs split | 7 | | |
| R4 postgres PgSession::commit split | 6 | | |
| R4 postgres source read cursor-arm move | 7 | | |
| R4 duckdb commit split | 6 | | |
| R4 file dest/mod.rs split | 8 | | |
| R4 rest read/mod.rs split | 9 | | |
| R4 iceberg commit.rs split | 10 | | |
| R4 sqlcore plan.rs split | 6 | | |
| R4 testkit memory commit split | 4 | | |
| R4 cli main.rs split | 12 | | |
| R4 bench cmd_run/fixtures/runner splits | 12 | | |
| R5 postgres validate decomposition | 7 | | |
| R5 rest validate decomposition | 9 | | |
| R5 sqlcore options/plan validate decomposition | 6 | | |
| R5 iceberg validate decomposition | 10 | | |
| R5 file validate convention unification | 8 | | |
| R6 shared apply_delta/apply_batch | 5 | applied | load/apply.rs owns the lower_schema→ensure_table→record-hash triple and lower_batch→write pair |
| R6 replay consumes helpers | 5 | applied | Loader::process + both replay arms consume the helpers (ensure SEMANTICS unchanged — only the code deduplicated); borrowed-box fixed (`&mut dyn LoadSession`); crash sweeps + recovery pins green |
| R7 Location unification (read+write) | 8 | | |
| R7 FileMeta/FileTask/FileProgress relocation | 8 | | |
| R8 DestError::RateLimited | 7* | | |
| R8 sqlcore typed validation errors | 6 | | |
| R8 rest Paginator typed error | 9 | | |
| R8 postgres DDL classification + decode conventions | 7 | | |
| R8 engine error-variant misuse | 5 | applied | Task panics → new additive `RdltError::Internal` (enum was non_exhaustive; CLI catch-all absorbs); workdir-lock failures → `config` (operator-actionable, consistent with sibling); `RecordsOut::rows` ChannelClosed lie → `.expect()` on genuinely-infallible serialization (writer infallible, non-finite rejected at construction; SPI signature change considered and declined — expect is truthful and simpler; greenfield permits revisiting if the SPI ever gains fallible pushes) |
| R9 engine ping-pong expects | 5 | applied | Via ShredOwner (see R4 engine row) |
| R9 postgres RunState expects | 7 | | |
| R9 rest validated-at-parse expects | 9 | | |
| R9 iceberg retry unreachable tails | 10 | | |
| R9 file s3_list partial method | 8 | | |
| R9 cross-module invariant panics (pg decimal / sqlcore hard_delete / duckdb scd2) | 6-7 | | |
| R10 non-breaking renames | 11 | | |
| R10 aliasable renames (DestinationError etc.) | 11 | | |
| R10 named deferrals to 0.3 window | 11 | | |
| R11 ShredCtx | 5 | applied | One `ShredCtx {registry, load_id, mode, policy}` field order; both former two-order sites + 3 fuzz/bench entry points updated; Loader::new 8→7 via cohesive `Sink {session, caps}` (matches the apply seam), too_many_arguments allow removed; no mega-struct forced |
| R11 postgres TableCtx | 7 | | |
| R11 bench return_side restructure | 12 | | |
| R12 core/engine dead code + visibility | 3 | applied | `flattened_column_name` DELETED; `needs_lowering`/`arrow_scalar_type`/`ArrayShape`(+constructor) private; channel.rs `#![allow(dead_code)]` removed (no dead code surfaced — allow was unnecessary); channel subsystem moved to rdlt-connector/src/channel.rs w/ unchanged public paths; CommitCounters/TableReport: parallel accumulators (no conversion site exists) → `From` impl + binding docs; `to_hex` via `write_hex`; `SchemaRegistry::apply` returns the schema (2 expect sites gone); `append_hex_id` ×3; `source_retryable` saturating (lived in rdlt-core). 115 tests PASS; workspace + fuzz check clean |
| R12 postgres/duckdb dead code (peek unification, pgoutput fields, query_string gating) | 3 | applied | ONE canonical `slot::peek`: streams (production semantics won — no whole-changeset Vec) + fully-parameterized binding (slot form won; interpolated LSN literal gone); pgoutput dead fields dropped w/ wire bytes still consumed in order; `tls::resolve_policy`/`classify_connect_error` → pub(crate); duckdb `count_rows`/`query_string` `#[doc(hidden)]` (cross-crate consumers forbid gating). CDC suite 25 + crash sweep 5 PASS through the new peek path; 227 total |
| R12 rest/iceberg/file dead code + visibility | 3 | applied | rest: `SequenceDriver::started/last_count` deleted, `extract`/`resolve` → pub(crate), `RestClient` fields private + dead `http()` deleted, headers parsed ONCE in new (parseability now a validate()-time typed guarantee); iceberg: `with_catalog_prop` + `_uses` shim deleted, `from_json` kept (API symmetry), `#[must_use]` completed; file: `format_version` ENFORCED (`check_readable`, test `future_commit_log_version_is_a_typed_error_current_is_fine`), version consts → pub(crate), one canonical `Format` re-export, ParquetDir documented as frozen stable spelling (not deprecated — sweep tooling + bench consume it). 191 tests PASS incl. container legs |
| R12 bench dead surface | 3 | applied | `BenchError::msg` deleted; `Variant.role`+`Role` enum DELETED (provably never read — gating is bars.toml-driven; variants.toml + header corrected); `Bar.policy` kept as documented informational provenance; `VerifyOutcome.ok` removed from struct+JSON (was always true); `rdlt_side`/markers → pub(crate); fingerprint → `competitor_pins: BTreeMap` w/ legacy-scalar migration — RESULTS.md regenerates BYTE-IDENTICAL (WR8 proof). 43/43 PASS |
| R13 SLAB_BYTES / channel caps / iceberg prop keys / root_of 64 / initial_wal_version | 3 | applied | One `SLAB_BYTES` (formats/mod.rs, 3 consumers); `CHANNEL_MSG_CAPACITY`=64, `EVENT_CHANNEL_CAPACITY`=4096, `STAGE_MSG_CAPACITY`=256 named+documented; 11 `CAT_*` catalog property-key constants beside the PROP_* discipline; `initial_wal_version` renamed w/ deliberate-pin doc (wire unchanged). Remaining: testkit 16<<20 caps + `root_of` 64 → land with increments 4/6 (testkit + sqlcore tasks own those files) |

*R8 RateLimited is scheduled with increment 7 in tasks.md (T054) though
plan.md's increment table folds it into the taxonomy work — either landing
spot satisfies WR8 as long as the gate is green.

## Part 3 — Per-crate findings not covered above

| Item | Increment | Disposition | Evidence |
|---|---|---|---|
| 3.1 CommitCounters/TableReport unification | 3 | | |
| 3.1 to_hex/write_hex | 3 | | |
| 3.1 merge-key validation dedup + error constructors macro | 3 | | |
| 3.1 channel subsystem → channel.rs | 3 | | |
| 3.1 API items (merge-key precedence doc, StateDoc::new version, re-export completeness, Pipeline::run typestate, merge_streams return, prelude) | 11 | | |
| 3.2 write_compact_json/canonical_json_bytes | 5 | applied (bound, not unified) | The two differ in KEY ORDERING and that difference is load-bearing: compact preserves insertion order for stored Json; canonical sorts for order-independent `_rdlt_id`. Unifying behind a flag = persisted identity one boolean from silent change. Cross-binding comments added; identity/canon oracle tests pin both |
| 3.2 hex-id append helper ×3 | 3* | applied | `append_hex_id` landed with the increment-3 core/engine sweep |
| 3.2 lowering rule duplication | 5 | deferred-in-place | lower_column/flatten_array parity still hand-maintained; revisit if a third site appears (no natural shared shape found during the split) |
| 3.2 root-name normalization ×2, fuzz scaffolding ×2, registry.get expect ×4, TapeRow/Queued | 3+5 | applied | registry.apply returns schema (inc.3); Queued lifted to module scope beside shred_root/enqueue_children (inc.5); remaining lows absorbed by the splits |
| 3.2 build_scalar/drain_tables/push_and_drain complexity | 5 | applied | build_scalar → dispatch + per-type scalar_* helpers; `TableDrain` zips the three parallel slices (misalignment unrepresentable); push_and_drain → shred_root + enqueue_children |
| 3.2 replay damage-reason logging, byte-budget clamp doc, clock fallback doc | 1+5 | applied | Damage logging landed with B10; clamp + clock-fallback documented with self-contained comments (inc.5) |
| 3.2 borrowed-box replay signature | 5 | applied | `&mut dyn LoadSession` |
| 3.3 pg_error_detail ×3 + SQLSTATE list ×2 | 7 | | |
| 3.3 ConnectResult match ×2, quoting ×2, effective_pk ×3, prepare_stream, serde vocab ×3, decimal/date parsing ×2, pump_copy ×2, Emit loop ×2, strategy wrappers, PEM loading ×2, select_sql WHERE dup | 7 | | |
| 3.3 slot peek binding inconsistency | 3 | applied | Folded into the R12 peek unification (one implementation, one binding form) |
| 3.4 duckdb quote-bypass deletion | 6 | | |
| 3.4 DestOptions/TableOptions re-export convention | 11 | | |
| 3.4 MergePlan field naming | 11 (deferral candidate) | | |
| 3.5 jsonl SlabReader, FileTask ×4, staged-part path ×3, owned-tail ×2, fill loops, compression-ext ×2 | 8 | | |
| 3.5 FileSource::read split, csv convert_cell catch-all | 8 | | |
| 3.5 read_doc reserialize round-trip | 8 | | |
| 3.5 ParquetDir deprecation intent, format_version, pub constants | 3 | applied | See R12 rest/iceberg/file row: intent recorded (frozen stable spelling), format_version enforced, constants narrowed |
| 3.5 parquet footer re-parse perf note | 8 | | |
| 3.6 Pagination selector_paths, stop-block dup, json_kind/render_scalar, base-url join, derive stacks | 9 | | |
| 3.6 fetch_page split, read_children/current_token | 9 | | |
| 3.7 conflict-retry triplication → commit_with_retry | 10 | | |
| 3.7 PartitionTransform From impl, fatal() ×3, arrow-target ×2 | 10 | | |
| 3.7 doubled exhausted phrasing, Debug tuples in message, AuthOptions error type | 10 | | |
| 3.7 root re-export completeness | 11 | | |
| 3.8 CLI run() macro → build_* fns | 12 | | |
| 3.8 testkit try_step!, fixture schema-from-TableSchema, util visibility, verify_* re-exports, Row alias | 4 | | |
| 3.8 bench four TOML loaders, last_json_field, container boilerplate | 3* | applied | One `load_toml<T>`; `protocol::last_json_field` ×3; `start_container` helper. *Landed early with increment 3 (same files) |
| 3.8 bench error hygiene (offender naming, malformed message, load_cells drops, reset_sql validation) | 3* | applied | Paths carried via `at()` helper; malformed message fixed; load_cells loud; reset_sql load-time validated. *Landed early with increment 3 |
| 3.8 bench ClassArg/Display/fingerprint/hash-data renames | 3* | applied | ValueEnum on Class (ClassArg deleted); Display for Mode; `hash_files` (serde keeps TOML key)/`data_dir`; competitor_pins map w/ migration. *Landed early with increment 3 |

## Part 5 — Delivery surfaces

| Item | Increment | Disposition | Evidence |
|---|---|---|---|
| D1 runtime probe 3→1 | 4 | applied | ONE `rdlt_testkit::containers::runtime_available()` — 5 documented arms incl. `RDLT_TESTKIT_FORCE_NO_CONTAINERS` override (posture verifiable without stopping the runtime, which would kill this session's own container); file S3Fixture + iceberg CatalogFixture + all pg/duckdb sites route through it |
| D2 skip-not-fail posture | 4 | applied | `PgFixture::start() -> Option` (eprintln SKIP + None); the `.expect()` hard-fail posture eliminated; duckdb differential uses visible top-of-test guards where the Option didn't fit a shared helper (net posture identical). Posture runs recorded below |
| D3 PgFixture into testkit | 4 | applied | `PgFixture`/`CdcPgFixture` behind testkit feature `containers` (optional testcontainers-modules + tokio-postgres); `POSTGRES_TAG` pin const (16-alpine, one definition); all ~6 `start_pg` copies deleted across postgres+duckdb tests (grep: 0 remain) |
| D4 fixture trio into testkit | 4 | applied | `rdlt_testkit::{schema_for,batch_of,meta_for}` with the Arrow field DERIVED from the TableSchema (can't drift); 6 byte-identical sites migrated (file recovery/preservation, duckdb recovery, iceberg exactly_once/conflict, pg dest_recovery); file dest_options kept its two-column local variant (not the canonical trio — correctly excluded) |
| D5 stream_yaml builders | 4 | applied / partially overtaken | rest: `stream_yaml` helper in tests/common; 26 tests migrated, 3 left inline with recorded reasons (top-level config fields ×2; frozen pre-014 spellings pin ×1); counts identical 68→68. file jsonl.rs OVERTAKEN — already fully abstracted by existing `source_for` helper (catalogue premise stale), 12→12 |
| D6 free-disk composite action | 3 | applied | `.github/actions/free-disk/action.yml` with THE canonical disk rationale; 6 jobs reference it (checkout-first verified per job) |
| D7 iai-callgrind pin unification | 3 | applied | `[workspace.dependencies] iai-callgrind = "=0.16.1"` cross-linked to ci.yml; both consumers inherit; perf-gate guard step fails loudly on mismatch (sed tested against real manifest) |
| D8 CI env/comment dedup | 3 | applied | One canonical rationale in the action; per-workflow env var kept (cannot be shared) with one-line pointers |
| D9 semver job disk step | 3 | applied | free-disk added to semver (builds two full trees — heaviest case) |
| D10 toolchain install cleanup | 3 | applied | `@stable` installs dropped (rust-toolchain.toml governs); `@nightly` kept for fuzz only; per-job needs verified |
| D11 variants.toml defaults | 3 | applied | `[defaults]` pin/image, per-variant override, loud missing-field error; `role=` lines removed with the dead field |
| D12 fixtures.toml defaults | 3 | applied | `[defaults]` postgres image + conn template ({{port}}); `[snippets]` drop_raw_schemas via `@name`; exec-only fixtures gained conn naturally; `strat_duck_unused` dropped (merge_prepare.sh arg now optional); reset_sql-without-container = load-time error |
| D13 workspace rust-version | 3 | applied | `rust-version = "1.96"` in [workspace.package], inherited by all 13 crates; matches rust-toolchain.toml |
| D14 inheritance stragglers + implied features | 3 | applied | iai-callgrind + libc → workspace deps; `postgres-source`/`file` dropped from CLI+bench (implication proven from rdlt [features] + resolved-tree check); rdlt-connector tokio simplified (strict-superset union). `cargo check --workspace` clean |
| D15 mutants.out.old untracking | 3 | applied | `git rm -r --cached` — 694 files untracked, ignore entry kept, directory on disk |
| D16 container image tags pinned (rustfs ×3 sites; polaris pending) | 1 | partially applied | Discovered at T001: gate red on merge base — file-crate S3 dest tests 500ing. Root cause = HOST DISK 100% FULL (168GB podman test residue: 188 stopped pg containers, 1117 anonymous volumes, dangling images — pruned, 158G freed); tests green after cleanup. Floating `rustfs:latest` pinned to 1.0.0-beta.11 at all 3 sites (file s3.rs, iceberg common, fixtures.toml ×2 refs) as drift-proofing; `apache/polaris:latest` pin deferred to a later increment with a live-verified tag. Leak pattern OBSERVED LIVE during increment 4: 16 orphaned postgres containers (fail-fast skips fixture Drop) + target/ ballooned to 851GB under parallel agent rebuilds — cleaned (866GB reclaimed); reaper/labeling convention remains the recorded follow-up |
| P5-low Makefile check/coverage notes | 3 | | |
| P5-low deep-checks RUSTFLAGS doc | 3 | applied | Deliberate-divergence comment added (deep tier measures, PR tier lints) |
| P5-low CLAUDE.md drift (rustc/arrow numbers) | 3 | applied | 016 block corrected: arrow 58.3, toolchain 1.96.0 |
| P5-low root README decision | close-out | | |
| P5-low bench reset_sql dup / conn-less fixtures / strat_duck_unused / cell-id convention | 3 | applied | First three via D12; cell-id renames SKIPPED deliberately (RESULTS.md history would orphan) — naming note for NEW cells added to benches/README.md |
