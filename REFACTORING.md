# REFACTORING.md — Code-wide refactoring opportunities

A full review of the workspace (~54k lines of Rust across 13 crates) for DRY
violations, single-responsibility/god-file problems, naming consistency, comment
quality, complexity, error handling, dead code, and API design.

Every finding cites the file and line as of this writing. Severity scale:

- **high** — correctness risk, live duplication already drifting, or a structural
  problem that will compound with the next feature.
- **medium** — real debt worth scheduling; slows readers or invites future bugs.
- **low** — polish; cheap wins and consistency.

---

## Part 1 — Bugs found while reviewing (fix before any refactoring)

These are not style issues; they are latent or live defects uncovered by reading
the code closely. Fix them first, independently of the refactor program.

### B1. REST child fan-out downgrades every child error to fatal
`crates/rdlt-connector-rest/src/source/read/mod.rs:247-253`
`read_children` wraps every error from `read_child_pages` in
`SourceError::fatal(...)` to attach parent context. A transient 5xx or a 429
from a child request — correctly classified by `classify_status` upstream — is
reclassified as fatal and aborts the run instead of consuming retry budget. This
contradicts the crate's own retry model. **Fix:** annotate the message while
preserving the variant (match on the incoming `SourceError`, or add a
`with_context` helper that keeps Transient/RateLimited intact).

### B2. `count_rows_async` over-counts when one table name prefixes another
`crates/rdlt-connector-file/src/dest/mod.rs:262`
`s3_list(table)` lists by raw key prefix, so counting table `a` also counts
`out/ab/part-….parquet`. The Replace-truncation path guards against exactly this
with an `rfind("{table}/")` tail check (lines 647-664); the counting path has no
guard. **Fix:** list with a `"{table}/"` tail and strip that exact prefix —
ideally via one shared "keys belonging to table T" helper used by both paths
(see F-D3).

### B3. CLI ↔ bench pipeline-spec duplication has already drifted
`crates/rdlt-cli/src/main.rs:23-124, 208-378` vs
`crates/rdlt-bench/src/library_mode.rs:26-94, 107-244`
The YAML spec structs, the `run_with!` construction macro, and the `is_json`
helper are character-for-character copies (library_mode.rs admits it in a
comment). The copies have diverged: the CLI's `DestSpec` has 5 variants
including `File` and `Iceberg`; the bench's copy has only
`Duckdb`/`Postgres`/`Parquet`. A `file:` or `iceberg:` pipeline runs under
subprocess mode but fails to parse under library mode. **Fix:** move the spec
model + `Spec → Pipeline` construction into the `rdlt` library (e.g.
`rdlt::pipeline_spec`) so CLI and bench share one parser. This also makes the
"CLI adds zero engine capability" claim structurally true.

### B4. Cursor ordering guarantee is a `debug_assert!` — silent watermark corruption in release
`crates/rdlt-connector-postgres/src/source/cursor.rs:349-352`
If a stream ever arrives out of order (collation drift, a wrapped query that
reorders), release builds silently accept the row and compute wrong watermarks.
Adjacent: `row_key` maps format failures to `unwrap_or_default()`
(cursor.rs:261, 274-276), turning a key-format failure into a colliding empty
key component. **Fix:** make the ordering violation a typed Fatal error (or at
minimum a loud `tracing::error!` plus skip); propagate key-format failures.

### B5. DuckDB constraint-violation detection is substring matching on Display text
`crates/rdlt-connector-duckdb/src/dest/commit.rs:227-228`
`msg.contains("Constraint Error") || msg.contains("violate")` classifies the
upsert-precondition failure. `"violate"` is a broad English substring; any
rewording silently loses the diagnosis, and any unrelated message containing it
misdiagnoses. Postgres uses structured SQLSTATE 23505 for the same check.
**Fix:** match duckdb-rs's error code if exposed; otherwise narrow to the full
`"Constraint Error"` prefix and add a probe test pinning duckdb's message so a
rewording fails loudly in CI.

### B6. Iceberg auth-rejection detection greps the library's rendered error string
`crates/rdlt-connector-iceberg/src/dest/errors.rs:21-31`
`error.to_string().contains("401 Unauthorized")` decides fatal-vs-transient. If
iceberg-rust rewords its Display, a rejected credential silently becomes
transient and burns the entire engine retry budget. **Fix:** match on the
`status` context value the library carries (the crate's own tests already
construct errors with `.with_context("status", ...)`), or pin the assumption
with a narrower needle and an upstream-issue link.

### B7. Two write/read-paired literals in the iceberg crate can drift silently
`crates/rdlt-connector-iceberg/src/dest/dest.rs:84` vs `:331` (scope hash length
`12`) and `crates/rdlt-connector-iceberg/src/dest/commit.rs:345` vs `:385`
(state property key `format!("{PROP_STATE_PREFIX}{scope}")`).
If either literal diverges, state reads silently miss written state — an
exactly-once regression with no error. **Fix:** one `const SCOPE_HASH_LEN` and
one `fn state_key(scope)` shared by both sides.

### B8. DuckDB has no transient error channel at all
`crates/rdlt-connector-duckdb/src/dest/mod.rs:64,165-167`; every
`.map_err(fatal)` in `src/dest/commit.rs`
The crate's only error constructor is `fatal()`. A locked database file (the
classic recoverable DuckDB failure), commit-time I/O errors, and appender flush
failures all map to `DestError::fatal`, so the engine can never retry a
recoverable condition — an asymmetry with postgres, which carefully splits
transient/fatal. **Fix:** classify open/connect and lock-shaped errors as
transient; keep parse/config errors fatal.

### B9. File destination maps *all* object-store errors to fatal; streaming reads lose classification
`crates/rdlt-connector-file/src/dest/mod.rs:166,182,199,557,666,723`;
`crates/rdlt-connector-file/src/location/s3.rs:262-267`
The source side classifies store errors into transient/fatal
(`location/s3.rs:127-146`); the dest side funnels every `object_store::Error`
through `fatal()`. Additionally `S3Reader::read_full` flattens mid-stream errors
into `io::Error::other`, losing the variant, and consumers wrap that as fatal —
a transient network reset mid-object fails the whole run. **Fix:** share one
classification function between source and dest; carry typed errors through
`read_full`.

### B10. WAL replay buffers the entire uncommitted span in memory
`crates/rdlt-engine/src/wal/resume.rs:155-200`
`let mut items: Vec<LoadItem> = Vec::new()` collects every decoded batch before
any write. The comment justifies a readability *check*, not *retention*. With a
time-based commit policy a span is unbounded, so recovery RSS can far exceed the
configured byte budget — exactly when the system is already degraded. **Fix:**
two-pass replay: pass 1 opens/builds readers to validate, pass 2 streams
segments through the session one at a time.

### B11. Fuzz target exercises a retired parser
`crates/rdlt-engine/src/fuzzing.rs:11` → `crates/rdlt-engine/src/shred/table.rs:148-159`
`table::parse_rows` is production-dead (the production path parses exclusively
through `Arena::parse_rows`), but the `parse_slab` fuzz target still exercises
it — fuzzer cycles spent on unreachable code while the real parser is only
fuzzed indirectly. **Fix:** repoint `parse_slab` at `Arena::parse_rows`; move
`table::parse_rows` under `#[cfg(test)]` or delete it.

### B13. Iceberg partition transforms unparseable in documented YAML spelling
*(discovered during 017 implementation, via the B3 parity fixture)*
`crates/rdlt-connector-iceberg/src/dest/config.rs` (`PartitionField.transform`)
The `PartitionTransform` doc promises `transform: {bucket: 16}` /
`{truncate: 8}` single-key maps, but `IcebergConfig::from_yaml` parses with
plain serde_yaml, which demands `!bucket 16` tag syntax for externally-tagged
enums — the documented spelling was rejected with "expected a YAML tag
starting with '!'". The crate's own tests exercised the map form only
through JSON, which masked it. **Fix:** `serde_yaml::with::singleton_map` on
the field (Deserializer-generic — JSON unchanged) + a YAML spelling pin test.

### B12. Provenance-hashing doc contradiction on a persisted format
`crates/rdlt-core/src/schema.rs:27-28` vs `:90-94`
`Provenance`'s doc says it is excluded from the content hash's semantic meaning,
but `content_hash` serializes the whole struct including provenance — a
provenance-only change flips `SchemaHash`, which is a persisted format.
**Fix:** decide and document one semantic (current behavior = provenance is
hashed); correct the `Provenance` doc.

---

## Part 2 — The big rocks (cross-cutting themes)

### R1. Spec/review citations pervade the code; comments must be self-contained
Every crate carries comments whose meaning lives entirely in `specs/` documents:
contract clause IDs ("contract O1", "SM1", "ID4", "RS3", "FF1", "clause D3"),
section numbers ("data-model.md §6", "design doc §4.3"), review-finding IDs
("013 review finding 5", "review F10", "finding #4"), task IDs ("US1 (T024)",
"T009 sweep"), and literal paths
(`crates/rdlt-connector/src/lib.rs:3-5` cites
`specs/001-rdlt-ingestion-engine/contracts/persisted-formats.md`;
`crates/rdlt-connector-file/src/source/config.rs:1` cites a path containing a
literal `…` character, pointing at a superseded contract).
Worst offenders by crate: rdlt-core (~20 files), rdlt-engine (62 hits/20 files),
rdlt-bench (~10 unresolvable "review finding N" sites), postgres, iceberg.
**Three escalation levels make this worse than mere staleness:**
1. These doc comments ship in published crates — on docs.rs the paths resolve to
   nothing.
2. Several citations have **already rotted**: rdlt-core's charter under-reports
   its dependencies (`lib.rs:8-10` vs Cargo.toml); rdlt's facade doc omits the
   `iceberg`/`postgres-dest` features (`rdlt/src/lib.rs:21-24`); iceberg's
   `dest.rs:322` references "the parquet-dest ordering" of a crate that no
   longer exists; `channel.rs:8-9` says "wired by US1 (T024)" about wiring that
   landed long ago; sqlcore's "MOVED VERBATIM" headers describe byte-identity
   that no longer holds.
3. Contract IDs leak into **user-facing strings**: postgres error messages cite
   "(contract O1)"/"(contract C2)" (`cdc/mod.rs:190,220,1081`), iceberg's
   Replace rejection cites "contract ID4" (`schema.rs:43`), and the CLI prints
   "(contract C3)" in warnings (`rdlt-cli/src/main.rs:396-453`). End users
   cannot resolve these.
**Policy to adopt:** every comment must stand alone — state the rule, then
optionally append a tag. Strip all citations from user-facing error/warning
strings. Delete relocation/task breadcrumbs ("Feature 008 T001: relocated
verbatim…") and historical narratives ("a prior incarnation…", "this branch's
earlier commits…"). Keep short clause IDs only where failures actually print
them (testkit D1-D8/S1-S6 is the good pattern).

### R2. The SQL commit-unit protocol is duplicated per destination, not shared
`crates/rdlt-connector-duckdb/src/dest/commit.rs:260-503` vs
`crates/rdlt-connector-postgres/src/dest/commit.rs:284-549`
The replay check, `load_committed_before` guard, single-unit discipline,
scope-replacement ordering, `MergePlan` construction, arm dispatch, stage
truncation, state upsert, and receipt insert are ~240 lines re-implemented per
destination — duckdb's own comment admits "the bookkeeping mirrors the postgres
session clause for clause". Drift has begun: pg converts the guard to bool at
the query, duckdb compares `u64 == 0`; pg inserts into `single_unit_done`
directly, duckdb collects a `marks` vec. `rdlt-connector-sqlcore` owns merge
*shapes* but not the commit-unit *protocol* — which is the correctness-critical
half. **Fix:** extract a protocol planner in sqlcore (e.g.
`commit_script(tables, options, replayed) -> Vec<Step>`) so destinations execute
steps instead of re-deriving decisions. Also lift the mechanically duplicated
helpers: `quote` (3 copies), `column_list` (2), `root_of` with its magic `64`
(2), the index-name hash formula (3 sites, twice in duckdb alone),
`hard_delete` resolution closure (2), the `MergePlan` 10-field construction
literal (2).

### R3. `Secret` is implemented three times
`crates/rdlt-connector-rest/src/source/client/secret.rs:13`,
`crates/rdlt-connector-file/src/location/secret.rs:11`,
`crates/rdlt-connector-iceberg/src/dest/config.rs:23`
~50 lines each (newtype + masking Debug/Display + transparent serde + schemars +
From impls). The iceberg copy's own module note records that "the extraction
trigger has FIRED". **Fix:** one shared `Secret` in `rdlt-connector` (the SPI
crate), re-exported by the three connectors. Related leak: REST's free-form
`headers`/`params` maps are plain `String` and print via derived `Debug`
(`source/config.rs:28-33,168-173`) — a credential placed there bypasses
redaction. Document that sensitive values belong in `auth:`, and consider a
validate-time warning for header names matching `authorization`/`x-api-key`.

### R4. God files / god functions
The workspace's top structural debts, each with mechanical split plans:

| Location | Size | Distinct responsibilities | Proposed split |
|---|---|---|---|
| `rdlt-engine/src/runtime/graph.rs` `run_once` | 391 lines | validation, workdir lock, session open + state recovery, WAL replay orchestration, task wiring, 144-line inline stream closure (6-level nesting), loader drain, commit | `validate_streams`, `recover_wal`, named `stream_task`, `drain_loader`; move load-id/backoff/classify out of "graph" (which contains no graph — rename module `runtime/run.rs`) |
| `rdlt-connector-postgres/src/tls.rs` | 1,191 lines | policy types, 4 error types, conn-string parsing, policy resolution, rustls config, connect + classification | `tls/{policy,connstring,rustls_config,connect}.rs` |
| `rdlt-connector-postgres/src/source/cdc/mod.rs` | 1,151 lines | run state, preflight, snapshot COPY, read dispatch, ack/lag, tail loop, `Apply` state machine | `cdc/{runtime,read,tail,apply}.rs` |
| `rdlt-connector-postgres/src/dest/commit.rs` `PgSession::commit` | ~265 lines, 5-level nesting | replay branch, per-table publish, single-unit guard, stage truncation, state upsert, receipt | `handle_replay`, `publish_table`, `MergeCtx` struct |
| `rdlt-connector-postgres/src/source/mod.rs` `read` | ~290 lines | lossy warnings, plan build, CDC dispatch, 120-line cursor arm, COPY pump, checkpoint | move cursor arm to `cursor.rs` as `IncrementalPlan::prepare` |
| `rdlt-connector-duckdb/src/dest/commit.rs` `commit` | 243 lines, 6-level nesting | same protocol as pg (see R2) | `replay_committed_unit`, `publish_table`, `check_single_unit`; prerequisite for R2 |
| `rdlt-connector-file/src/dest/mod.rs` | 834 lines | naming vocabulary, durable-JSON IO, `Store` abstraction, Destination impl, session staging, 210-line commit (6-level nesting), truncation, row counting, failpoints | `dest/{store,session,layout,truncate,inspect}.rs` |
| `rdlt-connector-rest/src/source/read/mod.rs` | 668 lines | fan-out orchestration, mpsc concurrency machinery, `SequenceDriver` state machine, body substitution, action matching, cursor helpers | `read/{driver,fanout}.rs` + move `substitute_body` into `resolve.rs` |
| `rdlt-connector-iceberg/src/dest/commit.rs` | 863 lines (+ tests = 32% of file) | catalog construction, writer, commit loop, retry policy, state marker table, provisioning/reconcile | `catalog.rs`, `writer.rs`, `commit.rs`, `state.rs`, `ensure.rs` |
| `rdlt-connector-sqlcore/src/plan.rs` | 580 lines | `HardDelete`, `MergePlan` + fragments, 5 strategy-arm generators, open-time validation, index planning | `plan/{mod,arms,validate,index}.rs` |
| `rdlt-testkit/src/memory/dest.rs` `commit` | 124 lines, 4-level nesting | replay detection, replace bookkeeping, 3 merge algorithms | `apply_append`/`apply_replace`/`apply_merge_keyed`/`apply_merge_by_id` — this is the *reference implementation* others are certified against; its clarity matters most |
| `rdlt-cli/src/main.rs` | 893 lines | spec model, allocator FFI, arg parsing, pipeline construction, CDC linting, event rendering, 380 lines of tests | `spec.rs`, `cdc.rs` (warnings), `main.rs` (args/drive) |
| `rdlt-bench/src/main.rs` `cmd_run` | 145 lines | load, fixture mgmt, quiet guard, competitors, cell run, summary + ad-hoc duplicate of gate logic | `prepare`/`run_one_cell`/`print_run_summary`; share bar evaluation with `cmd_gate` |

### R5. Validation monoliths
Five `validate` functions are flat rule piles where each rule deserves a named
home next to its error message:
- `rdlt-connector-postgres/src/source/config.rs:460-588` (~128 lines, 8 rule families) → `validate_conn`/`validate_cursors`/`validate_cdc`/`validate_tables`
- `rdlt-connector-rest/src/source/config.rs:406-579` (174 lines, 5 kinds) → `validate_stream_aliases`/`validate_selectors`/`validate_response_actions`/`validate_parent`
- `rdlt-connector-sqlcore/src/options.rs:169-273` + `plan.rs:392-525` → one `check_*` fn per rule group
- `rdlt-connector-iceberg/src/dest/config.rs:362-424` (63 lines) → `validate_catalog`/`validate_namespace`/`validate_tables`
- `rdlt-connector-file` has the opposite problem: 4 `validate()` fns returning
  `Result<(), String>` mapped differently by source vs dest — unify the
  convention and the error-text prefix.

### R6. Exactly-once-critical apply logic duplicated between live path and WAL replay
`crates/rdlt-engine/src/load/mod.rs:130-186` vs
`crates/rdlt-engine/src/wal/resume.rs:205-240`
The triple `lower_schema → session.ensure_table → state.schema_hashes.insert`
appears three times across the two files; `lower_batch → session.write` twice
more. The recovery path re-implements the live path by hand — the highest-stakes
duplication in the engine. Replay also redundantly ensures tables twice
(pre-loop + per-delta). **Fix:** shared `apply_delta`/`apply_batch` helpers used
by both `Loader::process` and `replay`.

### R7. Two parallel "local or S3" abstractions in the file crate
`crates/rdlt-connector-file/src/location/mod.rs:51-89` (`Location`, read-only)
vs `crates/rdlt-connector-file/src/dest/mod.rs:138-203` (`Store`, read+write)
Both are enums over `Local`/`S3` connecting via the same `build_store`, but
listing, key-joining, and error classification are re-implemented with different
behavior (this is where B2 and B9 come from). Also, `FileMeta` is defined in
`source::cursor`, so the "shared" location/formats layers import upward from the
source layer. **Fix:** unify into one location abstraction with read/write
halves; move `FileMeta`/`FileTask`/`FileProgress` into `location/` or a `types`
module.

### R8. Error-taxonomy inconsistencies across the workspace
- **`DestError` lacks `RateLimited`** (`rdlt-connector/src/error.rs:39-48`):
  sources have Transient/RateLimited/Fatal; destinations only Transient/Fatal —
  yet REST-catalog and warehouse destinations are rate-limited in practice.
  Add the variant or document why destinations may not signal it.
- **Stringly-typed "typed" errors**: sqlcore's `validate_*` returns
  `Result<_, String>` while its docs promise "identical TYPED errors"
  (`plan.rs:356-525`, `options.rs:162-273`); REST's public `Paginator` trait
  uses `String` as its error type (`read/paginate.rs:43`), forcing fatal at the
  single call site; postgres decode modules use three conventions in three
  adjacent files (`values.rs` `String`, `pgoutput.rs` hand-rolled struct without
  `Error` impl, `copy_decode.rs` thiserror). Introduce small enums with frozen
  Display text; keep messages pinned, make the type channel honest.
- **Error-variant misuse**: engine task panics become `RdltError::config`
  (`runtime/graph.rs:328,372,460`); workdir lock failures become `RdltError::wal`
  (`runtime/lock.rs:20-30`); `RecordsOut::rows` maps a JSON serialization
  failure to `ChannelClosed` (`rdlt-connector/src/lib.rs:183`).
- **Postgres DDL errors are all Transient** (`dest/commit.rs:116-217`) while the
  write path carefully maps SQLSTATE 22/23/42 to Fatal — unwinnable retries by
  definition. Reuse one classification helper.

### R9. Panic paths in library code
Dozens of `expect`/`assert!`/`unreachable!` in library code. Most are honest
invariants; the clusters worth fixing structurally:
- **Engine ping-pong expects** (`runtime/graph.rs:301,352`): `expect("shredder
  present")`/`expect("registry present")` depend on every exit path of a 40-line
  block restoring an `Option` — an invariant maintained by convention. A small
  owner type whose `run_blocking` takes `self` and returns `Self` makes it
  panic-free by construction.
- **Postgres CDC run-state**: ~10 hand-tracked `.expect("control client")` calls
  across three mutex round-trips (`cdc/mod.rs:366-598`) — give `RunState`
  methods (`ensure_control`, `ensure_snapshot`) so expects live in one audited
  place.
- **REST `expect("validated at config parse")`** ×6 (`read/paginate.rs:65-99`,
  `read/mod.rs:61`): `RestSource::new` never calls `validate()`, so hand-built
  configs panic at read time. Run `validate()` in `new` or make `from_config`
  return `Result`.
- **Iceberg retry `unreachable!` tails** ×2 (`commit.rs:371,584`): disappear
  with the shared retry helper (I2).
- **File crate partial method**: `Store::s3_list` has
  `unreachable!("s3_list on a local store")` (`dest/mod.rs:192`) — return an
  error or expose `as_s3()`.
- **Cross-module invariants enforced by panic**: postgres `copy_decode.rs:68`
  ("reflection produced a valid decimal shape"), sqlcore `plan.rs:240`
  ("hard_delete present"), duckdb `commit.rs:441` ("scd2 options resolved") —
  convert to typed internal errors.

### R10. Naming-convention unification (workspace-wide)
Consolidated proposal — the individual findings live in Part 3. These are the
renames that would make the codebase read as one voice:

| Inconsistency | Where | Proposal |
|---|---|---|
| `Dest*` abbreviation vs full `Destination`/`Source*` symmetry | `DestCapabilities`, `DestError` (rdlt-connector) | `DestinationCapabilities`/`DestinationError` (alias for one semver window) |
| Builder idioms: `with_*` vs bare verbs vs mixed | `StreamSpec::with_primary_key` but `structured()`; `SchemaPolicy::table()`; `PipelineBuilder::write_mode()`; testkit mixes both | One convention (bare verbs read best): normalize |
| `merge_key` (user-facing) vs `merge_scope` (internal) — and it collides with the identity merge key | sqlcore `options.rs:143` vs `plan.rs:63` | Rename config vocabulary to `merge_scope` at next semver break; `merge_scope` already won internally |
| `Pg*` vs `Postgres*` prefix flipping; `Postgres` (dest) vs `PostgresSource` (source) asymmetry | postgres crate | One rule per layer; document or rename |
| `RootCert` newtype reused for client certs *and private keys* | postgres `tls.rs:45-65` | Rename to `PemInput`/`PemSource` — it's a path-or-inline-PEM source |
| Timestamp spellings: `Timestamp{tz}` vs `TimestampNaive` vs bare `Timestamp` for naive | postgres types.rs/cursor.rs/encode.rs | Align on one spelling across encode/decode |
| `Boundary::{Closed,Open}` vs `EndBound::{Exclusive,Inclusive}` — two vocabularies for one idea | postgres config.rs:258-277 | One `Bound::{Inclusive,Exclusive}` enum |
| `path`/`name`/`key`/`tail`/`out`/`prefix`/`dir` for the same concept | file crate (7 synonyms catalogued) | `key` for location-relative identity, `root` for dest base |
| `FileMeta.size`/`FileProgress.done` silently unit-polymorphic (bytes vs row-groups) | file cursor.rs:24-69 | `Unit` newtype or rename to `*_units` |
| `eol: bool` actually means "ended at record boundary" (resume-safety flag) | file cursor.rs:28 | `ended_at_record_boundary` (serde rename keeps wire format) |
| "Tape" undefined jargon for the central production type | engine `TapeShredder` | Rename (`ArenaShredder`) or define the term in one sentence |
| `name_map` keyed by source name in one file, by normalized name in the sibling | engine build.rs:69 vs passthrough.rs:167 | Rename both to say direction |
| `ANode` vs `Node` vs `Kind` — three near-synonyms | engine arena.rs | `ArenaNode`/`StoredNode` |
| `pre_batch`/`snapshot`/`pre_batch_snapshot` — three names for one rollback concept | engine tape.rs/mod.rs/table.rs | `rollback_snapshot` everywhere |
| `action.action` stutter; `Oauth2ClientCredentials` casing; `Pagination::Cursor` impl is `BodyCursor` | REST config/read | `kind`; `OAuth2ClientCredentials` (serde rename); `BodyCursor` variant with `#[serde(alias)]` |
| `replaced` shadows two concepts in one function | testkit memory/dest.rs:33 vs 286 | `truncated_tables` field; `replaced_root_ids` local |
| `fail_after` injects *fatal* while siblings say `transient_*` | testkit memory/source.rs | `fatal_after` |
| `FlakyDestination` is deterministic single-fault injection, not flaky | testkit crash.rs | `CrashDestination`/`FaultInjectingDestination` |
| `hash` field holds file patterns; `data` field holds a TempDir | bench fixtures.rs:43,116 | `hash_files`; `data_dir` |
| `CliError::Usage` covers config errors, not usage errors | cli main.rs:189 | `Config`/`Setup` |
| `dest::dest` module inception suppressed with allow; `windows` plural for a counter; `RdltDataFileWriterBuilder` stutter; `read_state as read_state_prop` alias | iceberg dest | `session.rs`; `window_seq`; drop `Rdlt` prefix; rename fn `read_state_doc` |
| `fmt_lsn` is the crate's only abbreviated fn name | postgres slot.rs:31 | `format_lsn` |
| `ColumnDef.ty` abbreviation beside `type_hints` spelled out | rdlt-core schema.rs:67 | `column_type` (serde rename keeps wire) |

### R11. `#[allow(clippy::too_many_arguments)]` is telling you where structs want to exist
- Postgres CDC: 5 suppressions (`cdc/mod.rs:303,327,621,715,845`) sharing the
  same 6-arg prefix — the `TableCtx` struct already exists implicitly as
  `Apply`'s fields.
- Engine `Loader::new` (9 args, `load/mod.rs:81`) and the shred context
  `(registry, load_id, mode, policy)` threaded in **two different orders**
  (`tape.rs:70-77` vs `passthrough.rs:34-42`) — a transposition hazard that
  compiles. One `ShredCtx` struct fixes arity and ordering.
- Bench `return_side` (8 args + dead `_paths` param, `runner.rs:536`).

### R12. Dead code & over-wide visibility (representative, per-crate details in Part 3)
- `rdlt-core::naming::flattened_column_name` — zero workspace callers, and its
  test comment cites a caller that doesn't exist. Dead public API in a
  semver-gated crate.
- Engine `table::parse_rows` (B11); `needs_lowering`, `arrow_scalar_type`,
  `ArrayShape` are `pub(crate)` with single-module use — make private.
- REST `SequenceDriver::started`/`last_count` — write-only fields, zero reads.
- REST `RestClient` pub fields + whole `read::{extract,resolve}` tree public
  with no external consumers — tighten before the semver major locks them in.
- Postgres `slot::peek`/`slot::Change` unused by src (only tests via testhook),
  while production re-implements the same query inline with *inconsistent
  parameter binding* (`cdc/mod.rs:736-746` interpolates an LSN literal via
  `format!` while binding `$1`/`$2`). Unify on one canonical streaming peek.
- Postgres pgoutput parsed-but-never-read fields (`Begin.{final_lsn,xid}`,
  `Commit.{commit_lsn,end_lsn}`, `RelationColumn.{flags,type_oid,typmod}`,
  `Relation.replident`).
- DuckDB `count_rows`/`query_string` — test-only helpers in public API;
  `query_string` is a raw-SQL execution hole shipping in every release. Gate
  behind a feature or `#[doc(hidden)]`.
- Bench `BenchError::msg` (zero callers), `Variant.role` (doc promises gating
  behavior that doesn't exist), `Bar.policy` (never consumed),
  `VerifyOutcome.ok` (always `true` — schema noise in every artifact),
  `rdlt_side`/`begin_marker`/`end_marker` pub but crate-internal.
- Iceberg `from_json`/`with_catalog_prop` (zero call sites), `_uses` dead-code
  shim (`commit.rs:753`).
- File `format_version` fields written and decoded but never validated —
  decorative; enforce or drop.
- `channel.rs:8-9` `#![allow(dead_code)]` — provably unnecessary today, and it
  permanently suppresses detection of real dead code.
- `#[must_use]` inconsistency: iceberg `IcebergConfig` builders lack it while
  sibling builders have it (`config.rs:323-341`).

### R13. Magic constants to centralize
- `records_channel(64)` bound (rdlt-connector lib.rs:252), engine
  `broadcast::channel(4096)` + `mpsc::channel(256)`, testkit
  `records_channel(16 << 20)` ×2.
- `SLAB_BYTES = 8 << 20` defined 3 times in the file crate (csv.rs:20,
  jsonl.rs:28, source/mod.rs:299 as a bare literal).
- Iceberg REST-catalog property keys (`"uri"`, `"credential"`, `"s3.endpoint"`,
  …) scattered as 11 string literals (`commit.rs:49-84`) while the snapshot
  property keys right below are properly centralized constants — extend the
  discipline.
- `root_of`'s unexplained magic `64` (duckdb + pg).
- `default_wal_version()` shadows `WAL_FORMAT_VERSION` with a bare `1` —
  semantically loaded duplication; rename to `initial_wal_version` with a doc
  stating the pin is deliberate (engine wal/mod.rs:31-35).

---

## Part 3 — Per-crate findings (complete catalogue)

### 3.1 `rdlt-core` + `rdlt` (facade) + `rdlt-connector` (SPI)

**DRY**
- `CommitCounters` (commit.rs:9-15) and `TableReport` (report.rs:17-24) are
  field-identical structs (`rows, bytes, discarded_rows, discarded_values`)
  maintained in parallel by the engine with no `From` conversion — unify before
  a fifth counter is added to only one. *(medium)*
- `to_hex`/`write_hex` contain byte-identical encoding loops (ids.rs:80-98) —
  implement one via the other. *(low)*
- Duplicated merge-key-empty validation in `builder.rs:123-136` with
  inconsistent messages — extract `check_merge_key(scope, mode)`. *(low)*
- Copy-pasted `transient`/`fatal` constructor pairs on `SourceError`/`DestError`
  (error.rs:29-58) — consider a shared macro or generic. *(low)*

**SRP**
- `rdlt-connector/src/lib.rs:131-289` carries the whole byte-budget channel
  subsystem (`SourcePush`, `RecordsOut`, `records_channel`, …) in the crate
  root — move to `channel.rs`, leave root as traits + re-exports. *(medium)*

**Naming** — see R10 rows for `Dest*`, builder idioms, `ColumnDef.ty`.

**Comments** — the largest single cluster of spec citations (R1); plus stale
charter (B12-adjacent), stale feature list in facade docs, false caller claim in
naming.rs:183-186, semver/changelog commentary embedded in field docs
(`stream.rs:19-23` "shipped in 0.2.0…" belongs in CHANGELOG). *(medium)*

**Errors**
- `source_retryable` silently truncates `u128`→`u64` (error.rs:98) — use
  saturating conversion consistent with the crate's own idiom. *(low)*
- `RecordsOut::rows` misclassifies serialization failure as `ChannelClosed`
  (lib.rs:183). *(low)*
- `DestError::RateLimited` asymmetry — see R8. *(medium)*

**Dead code** — `flattened_column_name` (R12). *(medium)*

**API**
- Two competing sources of truth for merge keys: `WriteMode::Merge { key }` vs
  `StreamSpec.primary_key` — no documented precedence, no agreement validation
  in `PipelineBuilder::build`. Document + validate. *(medium)*
- `StateDoc::new` stamps rdlt-core's own version as "engine version"
  (state.rs:30) — take it as a parameter or relabel the field. *(low)*
- Root re-export surface incomplete: `ColumnRef`, `UnsupportedStateVersion`,
  `InvalidHexId`, `is_widening_of` etc. not re-exported alongside their owners.
  *(low)*
- `Pipeline::run` is a runtime-consume (`Option<Engine>` + error string) in a
  crate that already has compile-time typestate — make `run(self)`. *(low)*
- `merge_streams` uses `Option<StreamName>` as a sentinel and drops per-stream
  info in the error — return `(bool, Vec<StreamName>)`. *(low)*
- `prelude` and root re-exports disagree (`Cursor`, `ResumedFrom`,
  `TableReport` missing from prelude) — unify or document the rule. *(low)*

### 3.2 `rdlt-engine`

**DRY**
- `write_compact_json` (build.rs:369-406) is a near-verbatim copy of
  `canonical_json_bytes` (canon.rs:37-79) — ~35 lines where drift silently
  changes `_rdlt_id` hashes. Extract one parameterized serializer. *(medium)*
- Live/replay apply duplication — see R6. *(high)*
- Hex-encoding of ID/PARENT_ID/ROOT_ID columns copy-pasted 3× in
  `build_batch` (build.rs:83-119) — one `append_hex_id` helper. *(low)*
- Lowering name/nullability rule duplicated in `lower_column` vs
  `flatten_array` (lowering.rs:62-67, 135-151) — the parity requirement is
  currently maintained by hand. *(medium)*
- Root table name normalization computed twice per stream (graph.rs:129,262).
  *(low)*
- Fuzz/bench shred scaffolding duplicated (fuzzing.rs:16-28 vs 52-65). *(low)*
- `registry.get(...).expect("apply() just stored").clone()` ×4 — have
  `SchemaRegistry::apply` return the schema. *(low)*
- `TapeRow` vs local `Queued` struct are near-identical (tape.rs:34-40,88-95).
  *(low)*

**SRP** — `run_once` god function (R4); `table.rs` mixes buffer state, identity
hashing, a dead `Value` parser, and schema resolution; `Loader::process` folds 5
concerns into one 118-line match with a 13-field struct — extract per-variant
methods (enables R6). *(high/low)*

**Naming** — R10 rows: Tape, `name_map` polarity, `column_name` mutating
memoizer named like a getter (table.rs:94), `ANode`, `pre_batch`,
`runtime/graph.rs` contains no graph. *(medium)*

**Comments** — 62 spec-citation hits (R1); stale `channel.rs:8-9` comment +
`#![allow(dead_code)]` (delete both — provably unnecessary); stale `workdir`
doc (lib.rs:36 "US3"); module-doc history lesson in table.rs:5-10. *(medium)*

**Complexity**
- Inline stream-task closure: 144 lines, 6 nesting levels; two `spawn_blocking`
  arms repeat take/expect/spawn/restore scaffolding — extract `stream_task` + a
  ping-pong owner (also kills the expects, R9). *(high)*
- `build_scalar`: 165-line, 11-arm match — split per logical type or a generic
  primitive builder. *(medium)*
- `drain_tables`: 125 lines interleaving cascade filtering with policy, plus
  three index-aligned parallel slices (`pre_batch.get(idx)` silently tolerates
  misalignment) — pair into one `TableDrain` struct. *(medium)*
- WAL replay memory blow-up — B10. *(medium)*
- `push_and_drain`: 118 lines of parse+BFS+child-management+drain — extract
  `shred_root`/`enqueue_children`. *(low)*

**Errors** — panic classification (R8); replay swallows segment-damage reasons
(resume.rs:175-195 — `Err(_) => return Ok(None)` discards a carefully built
message; log it); byte-budget permit clamp silently caps at 4 GiB for
`byte_budget > u32::MAX` (channel.rs:58-62); clock fallback `unwrap_or(0)`
undocumented (graph.rs:44). *(medium/low)*

**Dead code** — B11 + single-use `pub(crate)` items (R12). *(medium)*

**API** — context params in two orders (R11); `replay` takes
`&mut Box<dyn LoadSession>` (borrowed-box anti-pattern → `&mut dyn LoadSession`);
crate doc contradicts itself about `pub` surface (fuzzing is `#[doc(hidden)]
pub`); magic channel capacities (R13). *(medium/low)*

### 3.3 `rdlt-connector-postgres`

**God files** — tls.rs, cdc/mod.rs, `PgSession::commit`, `read_stream_inner`
(~275 lines, 3 mutex round-trips), `PostgresSource::read` (~290 lines) — R4.
*(high)*

**DRY**
- The "tokio-postgres Display is opaque" workaround exists **three times**
  (`dest/mod.rs:56-75`, `source/errors.rs:52-62`, `tls.rs:692-695`) plus the
  transient SQLSTATE class list verbatim twice (`tls.rs:740`,
  `errors.rs:91-95`) — one `pg_error_detail` + one `is_transient_sqlstate`.
  *(high)*
- `ConnectResult` → SPI-error match duplicated source vs dest
  (`source/mod.rs:352-364` vs `dest/config.rs:39-51`). *(medium)*
- Two identifier-quoting functions in one crate (`dest/mod.rs:98` `quote` vs
  `sqlgen.rs:12` `quote_ident`) — the injection-safety invariant should have
  exactly one implementation. *(medium)*
- Primary-key resolution block copied 3× (`source/mod.rs:435-442, 680-687`,
  `cdc/mod.rs:202-209`) — `ReflectedTable::effective_pk`. *(medium)*
- `streams()`/`read()` repeat the per-stream setup pipeline — extract
  `prepare_stream`. *(medium)*
- String-vocabulary serde machinery hand-rolled 3× (`Lag`, `Wait`, `HintType`)
  — ~90 lines of byte-identical-shape impls; one macro. *(medium)*
- Decimal/date/time text parsing duplicated cursor.rs vs cdc/values.rs — a
  format fix applied once and missed in the other is waiting to happen. *(medium)*
- COPY stream → decode → push scaffolding duplicated for CDC snapshots
  (`source/mod.rs:731-772` vs `cdc/mod.rs:440-496`) — one `pump_copy`. *(medium)*
- `Emit` delivery loop written twice in one function (cdc/mod.rs:766-804). *(low)*
- Peek SQL duplicated with inconsistent parameter binding + dead shared version
  — see R12. *(high)*
- Four identical strategy-executor wrappers + repeated TRUNCATE-stage and
  stage-nonempty SQL (commit.rs:577-616, 344-361, 425-429, 508-515). *(medium)*
- Path-or-inline-PEM loading duplicated inside tls.rs (518-529 vs 582-593).
  *(low)*
- `select_sql`/`select_sql_from` WHERE/ORDER duplication; `apply_hint` guard ×5;
  testhook `FieldPlan` fixture lists ×2. *(low)*

**Naming** — R10 rows: `RootCert`, `Pg*`/`Postgres*`, timestamp spellings,
`Boundary`/`EndBound`, `fmt_lsn`. *(medium/low)*

**Comments** — spec paths in rustdoc + **contract IDs in user-facing error
strings** (R1); stale relocation breadcrumbs in dest/*.rs headers; stale crate
name in `source/mod.rs:1` ("rdlt-source-postgres"). *(medium)*

**Errors** — B4 (debug_assert watermark), DDL-all-transient (R8), three decode
error styles (R8), string-sniffing TLS refusal (tls.rs:715 — isolate + pin),
`streams()` panics on missing reflection entry while `read()` handles it
gracefully (source/mod.rs:390). *(high/medium)*

**Dead code** — pgoutput unread fields; `slot::peek`; `tls::resolve_policy`/
`classify_connect_error` are `pub` but crate-internal — `pub(crate)`. *(low)*

### 3.4 `rdlt-connector-sqlcore` + `rdlt-connector-duckdb`

**The big one** — R2 (protocol duplication, high) and its mechanical helpers:
`quote` ×3 (and duckdb's local `quote` bypasses the `DuckDialect` seam it claims
to honor — delete it, quote through the dialect), `column_list` ×2,
`root_of` ×2, index-name formula ×3, `MergePlan` construction literal ×2,
`hard_delete` closure ×2, `scoped`/`retire` computation ×2, Append/Replace
`INSERT INTO … SELECT` ×2, `DELETE FROM stage` ×2, `setting()`/`extension()`
identical 18-line bodies. *(high/medium)*

**Naming** — `merge_key` vs `merge_scope` (R10, high); duckdb re-exports bare
`DestOptions`/`TableOptions` while postgres aliases `PgDestOptions`/`PgTableOptions`
— pick one convention; `MergePlan` mixes pre-rendered SQL text with raw
identifiers and field names don't say which (`target` vs `key`) — rename to
`*_sql` or take raw identifiers and quote via dialect. *(medium/low)*

**SRP/Complexity** — plan.rs 5-way split (R4); `DuckDbSession::commit` 243
lines/6 levels (R4); `validate_merge` 133 lines + `DestOptions::validate` 104
lines monoliths (R5); `ensure_table` 136 lines doing DDL+migration+scd2+indexes;
`scd2_merge_sql` 92 lines with the scope-clause pattern in its third copy and
literal-escaping inline ×2. *(high/medium)*

**Errors** — stringly "typed" errors (R8); B5 (substring classification);
B8 (no transient channel); `expect("hard_delete present")` /
`expect("scd2 options resolved")` cross-module panic invariants (R9). *(high/medium)*

**Comments** — dozens of "013 review finding N" / clause-ID citations (R1);
misplaced doc paragraph + branch-history comment on `legacy_unique_index_name`;
stale "MOVED VERBATIM" headers. *(medium/low)*

**Dead code/API** — vestigial `let _ = key;` (plan.rs:523); test-only
`count_rows`/`query_string` public (R12); `index_plan` tuple soup → named
`IndexSpec`; document `DuckDb: Clone` sharing semantics (boundary otherwise
verified clean — no duckdb-rs types leak). *(medium/low)*

### 3.5 `rdlt-connector-file`

**God file** — dest/mod.rs 9-responsibility split (R4); `FileSession::commit`
210 lines/6 levels — extract the four comment-delimited phases. *(high)*

**DRY**
- jsonl slab-read loop duplicated ~45 lines near-verbatim (`read_task` vs
  `read_task_whole`) — extract a `SlabReader` parameterized over sync/async
  fill. *(high)*
- `FileTask` construction ×4 + rewrite-guard blocks ×2 in cursor planning
  (cursor.rs:118-245) — `new_task` + `check_rewrite` helpers. *(medium)*
- Local staged-part path hand-built ×3 (dest/mod.rs:538,587,694) — mirror the
  S3-side `staging_tail` helper. *(medium)*
- "Owned part" truncation implemented twice (local paths vs S3 segments) — one
  `owned_tail(segments, ext, partitioned)` rule (also fixes B2's root cause).
  *(medium)*
- `SLAB_BYTES` ×3; fill-loops ×4; per-(table,partition) part counting ×2 with an
  unstated cross-function invariant (record the index on `StagedPart`);
  compression-extension knowledge in 2 places. *(low)*

**SRP** — two Location/Store abstractions + upward imports (R7); `FileSource::read`
130 lines/4 concerns → `resolve_inputs`/`plan_tasks`/`stage_s3_fetches`;
`jsonl::read_task` 145 lines → `verify_tail`/`record_progress`;
`csv::read_task` two passes in one body + `convert_cell` 63-line/13-arm match
whose catch-all `Declared(_)` silently swallows future hint variants. *(medium)*

**Errors** — B2, B9; `expect("reserialize")` + pointless parse→serialize
round-trip in `Store::read_doc` (Local) — read raw bytes like the S3 branch;
`unreachable!`/`expect` sprinkles (R9); 4 error vocabularies (R5). *(high/medium)*

**Comments** — literal spec paths incl. one with a `…` character pointing at a
superseded contract; stale lib.rs/source-mod docs omitting CSV/dest/location
(first thing a new reader sees); "015 review finding 2" documents a
security-critical constant. *(medium)*

**Naming** — R10 rows: 7 synonyms for path, unit-polymorphic `size`/`done`,
`eol`, `pq.*` vs `file.*` fail-point prefixes (and two `pq.*` points fire on S3
runs — move inside `Store::Local` arms). *(low)*

**Dead code/API** — `ParquetDir` "frozen" alias carries no `#[deprecated]` and
isn't actually frozen (exposes all new API) — decide intent; `format_version`
fields decorative (R12); pub constants only crate-internal in use → `pub(crate)`;
`Format` re-exported through 3 public paths while `DestFormat` is a different
enum — one canonical re-export. *(medium/low)*

**Perf note** — parquet reader re-opens and re-parses the footer per row group
(formats/parquet.rs:38-49) — reuse the first builder's metadata or document why
per-group readers are required. *(low)*

### 3.6 `rdlt-connector-rest`

**Bugs** — B1 (fatal downgrade, high).

**SRP/Complexity** — read/mod.rs 4-way split (R4); `validate` monolith (R5);
`fetch_page` ~137 lines with a 50-line nested closure that matches
`HttpMethod` twice — extract `build_page_request` with one `match (method, body)`;
`read_children`/`current_token` each >60 lines of interleaved concerns. *(medium)*

**DRY**
- `Pagination` variant knowledge destructured twice in two files
  (config.rs:447-490 vs paginate.rs:51-103) — add
  `Pagination::selector_paths()`; also eliminates the double parse behind the
  R9 expects. *(medium)*
- Identical "declared total reached" stop block in `PageNumber`/`OffsetLimit`
  (paginate.rs:144-149, 186-191). *(low)*
- Three hand-rolled "kind of JSON value" matches + three scalar-render matches
  with subtly different policies — one `json_kind` + one `render_scalar`. *(low)*
- Base-URL join ×2, "not valid JSON" mapping ×3, absent-cursor guards ×2 —
  small helpers. *(low)*
- Repetitive derive/serde attribute stacks across 11 config types —
  organization only; group `default_*` fns, move `auth_compat` beside `Auth`. *(low)*

**Errors** — B1; `Paginator` trait's `String` error type (R8); R9 expects;
redundant defensive checks duplicating validation invariants
(`max_concurrency.max(1)`, unreachable `or_else` cursor fallback) — drop or make
the invariant real. *(high/medium)*

**Comments** — R1 cluster incl. "The S3 classification" in a REST-crate comment
(client/mod.rs:171 — typo for RS3 or copy-paste; actively misleading where S3
means object storage); overclaiming OAuth2 comment (token fetch bypasses the
shared client); stale "~80 lines" metric. *(medium/low)*

**Naming/API** — R10 rows (`action.action`, `Oauth2ClientCredentials`,
`Pagination::Cursor`); `Paginator::first`/`next` understate what they return →
`initial_params`/`decide`; header parsing deferred to per-request work — parse
once in `RestClient::new` (validates earlier, removes per-page parsing); Secret
leak via headers/params (R3). *(low/medium)*

**Dead code/visibility** — R12 (`started`/`last_count`, over-wide pub surface).
*(medium)*

### 3.7 `rdlt-connector-iceberg`

**God file** — commit.rs 7-responsibility split (R4); also
`IcebergSession::ensure_table` does mode gating + provisioning + writer-retirement
surgery in 77 lines (extract `check_mode`/`reinstall_state`; move the cheap
reserved-name check above the fallible schema mapping); `connect` mixes secret
reveal with 3 prop-translation blocks (extract pure `catalog_props` —
concentrates the `reveal()` audit surface). *(high/medium)*

**DRY**
- Bounded conflict-retry scaffolding triplicated **with divergence**
  (append_commit 1-based `loop` vs write_state/reconcile 0-based `for`,
  different backoff bases 100ms vs 50ms, only one site re-checks
  `already_committed`, two verbatim `unreachable!` tails) — one parameterized
  `commit_with_retry`. *(high)*
- `PartitionTransform → Transform` 7-arm match duplicated verbatim (commit.rs:487
  vs schema.rs:121) — one `From` impl. *(medium)*
- `fatal()` helper copy-pasted in 3 files — move into errors.rs, the stated
  boundary. *(low)*
- B7 paired literals; arrow-target conversion ×2; REST property keys (R13);
  third `Secret` copy (R3). *(medium/low)*

**Errors** — B6; doubled "exhausted" phrasing on state-write exhaustion
(commit.rs:355 + errors.rs:35); Debug-formatted tuples in an operator-facing
partition-mismatch message (commit.rs:499-505); inconsistent validation error
types (`AuthOptions` String vs `IcebergConfig` ConfigError). *(medium/low)*

**Comments** — R1 cluster incl. "contract ID4" in a user-facing error, stale
"parquet-dest ordering" reference, and **version-pinned claims** ("iceberg-rust
0.10 has no overwrite transaction") baked into the Replace rejection users see —
drop versions from user strings, re-check probes at each dep bump. *(medium)*

**Naming** — R10 rows (module inception, `windows`, `Rdlt*` stutter, import
alias); "Jittered" backoff with zero jitter — `50 * (1 << n) + (n*13) % 37` is
deterministic; two colliding writers back off identically, defeating the stated
purpose — add real randomness or fix the comment; inconsistent import
discipline (arrow/parquet fully-qualified inline while iceberg gets a use block).
*(low)*

**API** — boundary claim **verified clean** (no iceberg/arrow/opendal types in
any public signature — keep it that way); root re-exports omit
`CatalogOptions`/`StorageOptions`/`ConfigError` etc. that appear in re-exported
signatures; `#[must_use]` inconsistency (R12). *(low)*

### 3.8 `rdlt-cli` + `rdlt-testkit` + `rdlt-bench`

**CLI** — B3 (spec duplication, high); main.rs split (R4); `run()` 178 lines
with a 109-line embedded macro → per-destination `build_*` fns; contract IDs in
user-facing warnings (R1); `CliError::Usage` misnomer (R10). *(high/medium)*

**Testkit**
- `MemorySession::commit` 124-line 3-algorithm function (R4) + `replaced`
  shadowing (R10). *(high/medium)*
- "push failure and bail" pattern repeated ~8× in `verify_destination` — one
  `try_step!` helper makes each clause check one line (the clause IDs are the
  product; let them shine). *(medium)*
- Builder convention inconsistency + `fail_after` naming + `FlakyDestination`
  (R10); `batches` param shadows field. *(low)*
- Magic channel cap ×2; fixture schema/batch column lists must be kept in sync
  by hand 15 lines apart — build the arrow `Schema` from the `TableSchema`. *(low)*
- `util` module public but used once internally; `verify_source`/
  `verify_destination` not re-exported while `assert_conformant` is — fix both
  directions. Private `Row` alias in public signatures. *(low)*
- Comments: full `specs/001-…` path in conformance/mod.rs:3; "crash-matrix row 2"
  cited for *both* `BeforeWrite` and `BeforeCommit` — at least one is stale;
  verify against the current matrix. *(low)*

**Bench**
- God functions (R4: `cmd_run`, `fixtures::start` 175 lines, `run_once_subprocess`
  137 lines); container fixture arms repeat ~20 lines of boilerplate →
  `start_container`; four TOML loaders are one generic `load_toml<T>` away from
  collapsing; "last JSON line with field" convention implemented 3× →
  `last_json_field`. *(medium)*
- `runner.rs` hosts the crate's generic utilities (`Paths`, `substitute`) used
  by four other modules with inconsistent import styles — move to `paths.rs`/
  `template.rs`. *(low)*
- `run_cell`'s three arms each call `return_side` with the same 8 args (+
  `too_many_arguments` allow + dead `_paths` param + redundant `streams = vec![]`
  at runner.rs:451) — restructure to yield-then-build. *(medium)*
- Dead/mislabeled surface (R12: `BenchError::msg`, `Variant.role`, `Bar.policy`,
  `VerifyOutcome.ok`, pub-but-internal fns). *(medium)*
- Stringly `BenchError` is fine for a dev tool, but runner.rs's bare `?` sites
  violate the crate's own "offender always named" rule (`io: No such file or
  directory` with no path); `main.rs:217` contains a malformed message with a
  ~26-space gap where a line continuation was lost; `load_cells` silently drops
  unreadable dir entries; `Started::reset` silently no-ops when `reset_sql` is
  declared without a container — cross-validate at load time. *(medium/low)*
- `ClassArg` duplicates `Class` solely to derive `ValueEnum`; `Mode` printed via
  `{:?}` while `Class` has `Display` — derive on `Class`, add `Display` for
  `Mode`, delete `ClassArg`. Fingerprint records only the first competitor's pin
  — `BTreeMap<variant, pin>`. `hash`/`data` field names (R10). *(low)*

---

## Part 4 — Suggested execution order

Sequenced by value-per-risk: bugs first, then the structural changes that unlock
each other, then polish. Each item is independently mergeable.

1. **Fix the Part 1 bugs** (B1-B12). Small, isolated, high-value. B1, B2, B4,
   B6, B7 are one-to-few-line fixes; B3 and B10 are larger.
2. **Delete stale comments + strip citations from user-facing strings** (R1
   escalations 2-3). Pure deletion, zero behavior change, immediately improves
   every future read.
3. **R13 constants + R12 dead code/visibility sweep.** Mechanical, reviewable
   crate-by-crate. Do this before the big splits so less code moves.
4. **R3 Secret unification** into `rdlt-connector`. Small, unblocks two crates.
5. **Engine: R6 shared apply helpers → R4 `run_once`/`stream_task` split →
   B10 two-pass replay.** The apply-helper extraction makes the god-function
   split safe; the split makes the replay fix obvious.
6. **sqlcore: R2 protocol extraction**, preceded by splitting
   `DuckDbSession::commit` and `PgSession::commit` (R4) so the two copies become
   visibly identical before being lifted. Includes the `quote`/`column_list`/
   `root_of`/index-name helper moves.
7. **Postgres: tls.rs + cdc/mod.rs splits (R4)** and the triplicated
   error-detail/SQLSTATE helpers; then B8/R8 error-classification alignment
   across postgres/duckdb/file.
8. **File crate: R7 Location/Store unification** (subsumes B2/B9 root causes),
   then the dest/mod.rs split (R4).
9. **REST: read/mod.rs split (R4)** + `Paginator` error type + `selector_paths`.
10. **Iceberg: commit.rs split (R4)** + `commit_with_retry` helper (kills the
    triplicated retry divergence and both `unreachable!` tails).
11. **Naming pass (R10)** — schedule with the next semver window for the
    breaking ones (`merge_scope`, `DestinationError`, `OAuth2ClientCredentials`);
    alias-shim the rest. Non-breaking renames (field/local/param names,
    `rollback_snapshot`, `format_lsn`, …) can go anytime.
12. **CLI/bench: `rdlt::pipeline_spec`** (B3) and the bench god-function splits.

---

## Part 5 — Discovered opportunities (surfaces outside the Rust-file review)

The Part 1–3 review covered the 208 production Rust files. A follow-up sweep
of the surfaces that review skipped — test support code, CI workflows,
Makefile, bench TOML config, workspace manifests, and repo-root hygiene —
found the items below. Same severity scale. Sequencing: D1–D5 pair naturally
with step 4 of Part 4 (they grow the same testkit that R3 touches); D6–D15
are standalone mechanical fixes that can land with step 3.

### 5.1 Test-support code

- **D1. Container-runtime probe implemented three incompatible ways** *(high)*
  `crates/rdlt-connector-file/tests/common/s3.rs:27` (filesystem probe of
  `DOCKER_HOST`/podman socket/docker socket) vs
  `crates/rdlt-connector-iceberg/tests/common/mod.rs:66` (`podman ps`
  subprocess) vs `crates/rdlt-connector-postgres/tests/dest_crash_sweep.rs:85,250,403`
  (infers from `start().await` failing). The probes can disagree on the same
  machine. **Fix:** one `rdlt_testkit::containers::runtime_available()`
  superset probe used by every crate.
- **D2. Skip-vs-hard-fail posture inconsistent, even within one crate** *(high)*
  `crates/rdlt-connector-postgres/tests/common/mod.rs:20` panics via
  `.expect()` when no runtime is present, while `dest_crash_sweep.rs:85`
  skips with `eprintln!` and the file/iceberg crates skip via `Option`. The
  same missing-runtime condition fails CI or silently skips depending on
  which test binary runs. **Fix:** standardize on skip-not-fail (the
  documented intent) through an `Option`-returning testkit fixture start.
- **D3. Postgres container fixture re-implemented ~6 times, cross-crate** *(high)*
  `PgFixture::start()` exists in `crates/rdlt-connector-postgres/tests/common/mod.rs:20`,
  yet `dest_conformance.rs:16`, `scd2.rs:21`, `dest_recovery.rs:59` redefine
  a local `start_pg()`, `dest_crash_sweep.rs` inlines it 3×, and
  `crates/rdlt-connector-duckdb/tests/differential.rs:91` copies it into
  another crate — all repeating the `16-alpine` tag, port mapping, and
  conn-string literal. **Fix:** promote `PgFixture`/`CdcPgFixture` into
  rdlt-testkit; delete the copies.
- **D4. Destination-conformance fixture trio duplicated byte-for-byte across
  4 crates** *(high)* `batch_of`, `schema_for`, and `meta_for` are identical
  in file (`tests/{recovery,preservation,dest_options}.rs`), duckdb
  (`tests/recovery.rs`), iceberg (`tests/{exactly_once,conflict}.rs`), and
  postgres (`tests/dest_recovery.rs`). A `StateDoc`/`CommitMeta` field change
  is a 6-file edit. **Fix:** move the trio into `rdlt_testkit` (conformance
  or a fixtures module) and re-export.
- **D5. Config-YAML preamble repeated ~44× across REST and file tests** *(medium)*
  `crates/rdlt-connector-rest/tests/actions.rs` (~20 near-identical
  `base_url`/`streams` scaffolds), `pagination.rs` (~9),
  `crates/rdlt-connector-file/tests/jsonl.rs` (~15). Each test varies one
  field but re-inlines the whole scaffold. **Fix:** a small
  `stream_yaml(uri, path, extra)` builder so tests state only their delta.
- Clean (verified): failpoint gating (`#![cfg(feature = "failpoints")]`) is
  consistent across all 7 crash-sweep files; `RDLT_NET=1` gating has a single
  user; no monolithic multi-scenario `#[test]` fns; no re-rolled comparators.

### 5.2 CI workflows

- **D6. "Free runner disk space" step copy-pasted into 5 jobs** *(high)*
  Identical block in `ci.yml` (`check`, `test`, `perf-gate`) and
  `deep-checks.yml` (`nightly`, `weekly-mutants`). **Fix:** local composite
  action `.github/actions/free-disk/action.yml`.
- **D7. `iai-callgrind` version pinned in three unlinked places** *(high)*
  `ci.yml:68` hardcodes `iai-callgrind-runner --version 0.16.1` while
  `rdlt-engine/Cargo.toml:36` and `rdlt-connector-postgres/Cargo.toml:41`
  pin `iai-callgrind = "0.16"` per-crate (the only dev-deps bypassing
  `[workspace.dependencies]`). A lib bump without a runner bump breaks the
  perf gate. **Fix:** workspace-inherit the dep and cross-link/derive the
  runner version in CI.
- **D8. Shared env block + disk-constraint rationale duplicated across
  workflows** *(medium)* `ci.yml:8-14` vs `deep-checks.yml:14-18` repeat
  `CARGO_PROFILE_DEV_DEBUG: line-tables-only` and its multi-line comment.
  **Fix:** one canonical copy with a pointer.
- **D9. `semver` job omits the disk-free step every other build job has**
  *(medium)* `ci.yml:72-95` builds two full trees (release baseline +
  current) without the mitigation the env comment says is required.
  **Fix:** add the step or document the exemption.
- **D10. Redundant/misleading toolchain installs** *(medium)*
  `dtolnay/rust-toolchain@stable` is installed but `rust-toolchain.toml`
  (1.96.0) governs every real build; `deep-checks.yml:40-41` installs both
  stable and nightly where only nightly (fuzz) is used. **Fix:** install the
  pinned toolchain explicitly or drop `@stable`; keep nightly only for fuzz.
- `deep-checks.yml` lacks the `RUSTFLAGS: -D warnings` that `ci.yml` sets —
  likely intentional (deep tier doesn't lint) but undocumented. *(low)*

### 5.3 Makefile

- Clean on the major axes: single source of truth (CI calls its verbs),
  documented targets, consistent `TARGET=` dispatch, no dead targets.
- `make check` is contributor-only (CI decomposes it into parallel jobs) and
  can drift from the CI job graph — note it in the header or assert
  equivalence in CI. *(low)*
- The 80% coverage floor exists only in the `coverage` target's prose. *(low)*

### 5.4 Bench configuration (TOML)

- **D11. Competitor pin/image duplicated across all four variants** *(medium)*
  `benches/competitors/dlt/variants.toml` repeats `pin = "dlt 1.29.0"` and
  `image = "rdlt-baseline"` 4× (and the harness records only the first pin —
  see 3.8). **Fix:** hoist shared defaults; per-variant overrides only.
- **D12. Postgres fixture image/conn-template duplicated 7×** *(medium)*
  `benches/fixtures/fixtures.toml` repeats `image = "postgres:16"` and the
  conn-string template in every postgres fixture; only the port varies.
  **Fix:** shared postgres-fixture defaults. Related lows: `reset_sql`
  duplicated verbatim in `rest-pg`/`pg-src`; `merge-index-pg`/`refine-pg`
  omit `conn` (exec-only) without being marked a distinct kind; the
  `strat_duck_unused` placeholder arg in `benches/cells/merge.toml` papers
  over a script signature mismatch; cell-id naming mixes three conventions
  for the strategy family.

### 5.5 Workspace manifests

- **D13. No `rust-version` in `[workspace.package]`; MSRV facts disagree**
  *(medium)* The floor lives only in `rust-toolchain.toml` (1.96.0) while
  prose (CLAUDE.md) still says 1.94 — cargo enforces nothing. **Fix:** add
  `rust-version` to `[workspace.package]`, inherit per-crate, correct prose.
- **D14. Inheritance/feature stragglers** *(low-medium)* `libc = "0.2"`
  (rdlt-cli) and the two `iai-callgrind` pins (D7) are the only deps
  bypassing `[workspace.dependencies]`; CLI and bench enable features
  (`postgres-source`, `file`) already implied by (`postgres`, `parquet`);
  rdlt-connector re-specifies tokio features the workspace set supersedes.
  **Fix:** inherit, and drop implied features.
- Clean (verified): `[lints] workspace = true` in all 13 crates; license/
  edition/version/repository inherited everywhere.

### 5.6 Container image pinning (discovered during 017 implementation)

- **D16. Test-container images ride floating tags** *(high — promoted from
  implementation)* `rustfs:latest` in 3 places
  (`crates/rdlt-connector-file/tests/common/s3.rs`,
  `crates/rdlt-connector-iceberg/tests/common/mod.rs`,
  `benches/fixtures/fixtures.toml` ×2 refs) and `apache/polaris:latest` +
  `postgres:16-alpine`-adjacent tags elsewhere. A floating tag re-resolves
  whenever upstream pushes, so an upstream regression fails our gate with no
  change on our side — and misdirects diagnosis (the 017 baseline run's
  RUSTFS 500s initially looked like image drift; the actual cause was host
  disk exhaustion from accumulated container-test residue). **Fix:** pin
  every test/bench image to a specific tag; bump deliberately with the live
  cells green. Secondary: test fixtures leak stopped containers + anonymous
  volumes on abnormal exits (188 stopped postgres containers, 1117 volumes
  observed) — the testkit containers module (D1-D3) should also own a
  best-effort reaper/labeling convention so residue is identifiable.

### 5.7 Repo-root hygiene

- **D15. `mutants.out.old/` is gitignored yet 694 files under it are
  tracked** *(medium)* The ignore rule is dead for already-tracked files;
  the directory is committed mutation-test output. **Fix:**
  `git rm -r --cached mutants.out.old/` (or delete outright), keep the
  ignore entry.
- CLAUDE.md drift: "rustc floor 1.94" vs toolchain 1.96.0; "arrow 58.4" vs
  workspace pin 58.3. Informational, but both numbers are wrong. *(low)*
- No repo-root README while manifests set `repository` for publishing — add
  a minimal one before publish. *(low)*
- Clean (verified): `rustfmt.toml` consistent with the 2024 edition; no
  orphaned `deny.toml`/`clippy.toml`; `tools/interop/.venv` correctly
  untracked.

---

*Method note: every Part 1–3 finding was read out of the code by a
line-level review of all 208 Rust files; Part 1 items B1-B7 and the
R2/R3/R10 claims were additionally verified by direct re-inspection. Part 5
came from a follow-up sweep of the non-Rust and test-support surfaces that
review skipped. Line numbers drift as code changes — treat them as locators,
not contracts.*
