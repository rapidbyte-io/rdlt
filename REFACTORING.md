# REFACTORING.md — adopt the snowflake-connector-rs house style, crate by crate

**Reference codebase:** `../snowflake-connector-rs` at rev
`210ffec9a7bd2826792302f41305d86fe5f1b89e` (branch `feat/put-file-upload`).
Every rule below cites a real file there; when an instruction and the reference
disagree, read the reference and follow it.

**What this is.** A driver document in the 017 mold: the previous REFACTORING.md
was executed end-to-end as feature 017 and then deleted. This one restructures
every crate to the reference's conventions — file layout, naming, module
organization, visibility, comments, tests — while changing **zero behavior**.
It is written so each crate section can be pasted into an AI prompt together
with Parts 1–3; each section is self-contained given those parts.

**Why this codebase.** Its names carry information at every level: the file
path reads as a sentence (`statement/put/stage/s3.rs`), the type name states
the role (`PlannedFile`, `QueryHandle`, `WireQueryResponse`, `CellRef`), the
function name states the outcome (`plan_files`, `local_path_from_sql`,
`find_scheme`), and the constant name states the domain fact
(`SESSION_EXPIRED: &str = "390112"`). Modules are small, layered, and curated.
rdlt's comment discipline is already strong (its doc density is *higher* than
the reference's); what it lacks is the reference's **structural** discipline —
`mod.rs` files holding a thousand lines of logic, 900-line multi-concern
files, grab-bag names (`objects.rs`, `util.rs`), no wire/domain separation,
and unconditionally-public test hooks.

---

## Part 1 — The rulebook

Numbered so crate sections can cite them. Each rule names its exemplar in the
reference.

### S1. One file, one noun, small

A file holds ONE concept and is named by that concept's noun:
`cursor.rs`, `plan.rs`, `handle.rs`, `manifest.rs`, `cell.rs`, `row.rs`,
`schema.rs`, `table.rs` (see `src/result_table/`, `src/statement/`). The
directory path composes into a sentence: `statement/put/stage/s3.rs` is "the
S3 backend of the stage half of PUT, which is a statement concern."

Targets: average ~330 lines/file; nothing above ~600 without a written
justification in the module doc; a file that needs a plural name
(`errors.rs`, `types.rs`, `utils.rs`) is holding more than one concept —
split it until the singular is honest.

### S2. `mod.rs` is a table of contents, never a home for logic

Every `mod.rs` in the reference declares submodules, curates re-exports, and
at most hosts a small `#[cfg(test)] mod tests` or a gated `test_data` module
(see `src/result_table/mod.rs`, `src/statement/mod.rs`, `src/rowset/mod.rs` —
5 to ~90 lines each). Logic lives in a named submodule. A `mod.rs` above ~100
lines of non-test code is a defect under this rule.

### S3. Layered visibility, curated surface

- The public API is assembled at `lib.rs` with explicit `pub use` lists —
  the reference's `lib.rs` is the crate's front page and its table of exports.
- Internals default to `pub(crate)`; things one parent module shares with its
  children are `pub(super)` (see `src/statement/put/plan.rs` — everything
  `pub(super)`).
- Modules are private (`mod x;`) unless the module itself is the product
  (`pub mod bind;`).
- Test-only internals are **gated**, not public:
  `#[cfg(any(test, feature = "bench-internals"))] #[doc(hidden)] pub mod
  test_data` (`src/result_table/mod.rs`). Never an unconditionally public
  hook module.

### S4. Wire types are quarantined and named `Wire*`

> **SCOPE, measured — read before applying.** This rule has exactly ONE
> legitimate site in rdlt's 54k lines:
> `crates/rdlt-connector-postgres/src/source/cdc/pgoutput.rs`, which decodes
> the pgoutput logical-replication format (`TupleData`, `TupleValue`,
> `RelationColumn`, `Relation`, `Message`). It does **not** apply to
> `rdlt-connector-snowflake/src/dest/client.rs`: that file contains **zero**
> `Deserialize` derives (verified) because the forked driver owns the wire and
> the file is already the crate's single library boundary under Principle III —
> wrapping does S4's job there. Applying S4 anywhere else in this workspace
> means renaming config or state types, which violates C1/C3.

Everything shaped by an external protocol lives in a `wire/` module, named
with a `Wire` prefix, `pub(crate)`, and never crosses into domain code:
`WireQueryResponse`, `WireQueryData`, `WireRowType`, `WireChunk`
(`src/statement/wire/response.rs`). Fields the runtime ignores are omitted
from the DTO **and the omission is documented** ("Response metadata ignored by
the runtime is omitted from the deserialization DTOs: …"). Domain types never
carry serde compromises; a translation function owns the crossing.

### S5. Names state roles

- Types: role-suffixed nouns. `*Config` (construction-time, one type per
  concern — the reference has `ClientConfig`, `SessionConfig`, `QueryConfig`,
  `TransportConfig`, `EndpointConfig`, `ProxyConfig`, not one giant config),
  `*Options` (per-call overrides — `QueryOptions`, `CollectOptions` — with the
  layering documented: "Unset options inherit the defaults from
  [`QueryConfig`]"), `*Builder`, `*Handle`, `*Executor`, `*Cursor`,
  `Planned*` (output of a planning step: `PlannedFile`), `*Ref` (borrowed
  view: `CellRef`, `RowRef`) vs `*Value` (owned: `CellValue`,
  `BinaryValue`).
- Functions: verb phrases naming the outcome — `plan_files`,
  `local_path_from_sql`, `find_scheme`, `paths_match`, `expand`
  (`src/statement/put/plan.rs`). Constructors that transform name their
  provenance: `ColumnType::from_driver_metadata`.
- Builder setters: consuming `with_*` returning `Self` (`src/session.rs`).
- Constants: the domain fact, documented — `SESSION_EXPIRED: &str = "390112"`,
  `QUERY_IN_PROGRESS_CODE`, each with a doc comment saying what the value
  means (`src/statement/wire/response.rs`).

### S6. Comments: one line of WHAT, then WHY with evidence

The reference pattern (`src/statement/put/plan.rs`):

```rust
/// Resolve the local files a `PUT` refers to.
///
/// The path is taken from the statement the caller wrote, not from the
/// server's echo of it. Snowflake's own drivers carry the same check
/// (SNOW-15153): without it a compromised endpoint could name any readable
/// path and have the connector upload it.
```

- First line: what, imperative or noun phrase.
- Body: why, with the evidence (an upstream issue, a protocol fact, a
  measurement) — never a restatement of the code.
- Inline comments only where the code would mislead without them (the
  Windows-path case in `local_path_from_sql`).
- Intra-doc links everywhere: `[`Error::kind`]`, `[`QueryConfig`]` — a name in
  a doc comment that exists in code is a link, not plain text.
- rdlt already lives by this (Principle VI); keep its comment content,
  reshape only where structure moves.

### S7. `lib.rs` is the front page

Crate-level docs open with a **compiling, runnable example** of the primary
workflow (`src/lib.rs`: create client → session → query → collect, then the
feature-gated PUT example), then the module declarations, then the curated
re-exports. A user should be able to learn the crate's shape from `lib.rs`
without opening another file.

### S8. Error module: one public type, structured accessors, private repr

The reference's `error.rs` + `error/` split (`repr.rs`, `decode.rs`,
`schema.rs`, `parse.rs`, `query_scoped.rs`, `display.rs`):

- One public `Error` with `kind() -> ErrorKind` for the stable category and
  named accessors (`snowflake_code()`, `query_id()`, `as_schema_error()`)
  for details — documented as "the semver-stable surface; the concrete source
  type reached by downcasting is not."
- The internal representation is a private boxed enum (`Repr`), one variant
  per failure family, each family its own struct in its own file.
- A "Handling errors" doc section with a runnable match example.

**Scope limit for rdlt:** the SPI taxonomy (`SourceError` /
`DestinationError`: Transient / RateLimited / Fatal) is the engine's contract
and is NOT restructured (Part 2, C2). This rule applies to each connector's
**internal** error modules — the files currently named `pgerror.rs`,
`errors.rs`, `tls_verify.rs`.

### S9. Tests: one integration root, topic files, shared `common.rs`

The reference (`tests/`):

- ONE compile root per suite: `integration.rs` containing `mod cases;` —
  `tests/cases/test_put.rs`, `test_decode.rs`, … compile as one binary
  instead of N.
- `tests/cases/common.rs`: a process-shared session behind
  `tokio::sync::OnceCell` (`default_session()`), `fresh_*` variants for
  isolation, and `unique_temp_table_name()` so shared-session tests never
  collide on names.
- Fixtures under `tests/fixtures/`.
- Unit tests inline in `#[cfg(test)] mod tests` next to the code; type-level
  pins like `assert_send_sync::<ResultTable>()` (`src/result_table/mod.rs`).
- In-crate mock infrastructure under `src/test_support/`, declared
  `#[cfg(test)]` from `lib.rs`.

**Scope limit for rdlt:** binary names are load-bearing in two places — the
Makefile sweep target selects `-E 'binary(crash_sweep)'` (and siblings), and
`.config/nextest.toml` scopes the iceberg live group by
`binary(config_schema)`. Crash-sweep and conformance binaries KEEP their own
roots. Consolidation applies to the ordinary topic tests, and every
consolidation updates the Makefile/nextest filters in the same commit and
proves `make test TARGET=sweep` still selects a non-empty set.

### S10. Config family, not config monolith

Construction-time configuration is a small family of single-concern types
composed at the top (`ClientConfig` holding `EndpointConfig`, `ProxyConfig`,
`TransportConfig`, `SessionConfig`, `QueryConfig`), each with `with_*`
setters and cross-referenced docs. Per-call knobs are a separate `*Options`
type whose docs state the override order explicitly.

**Scope limit for rdlt:** the user-facing YAML vocabulary is FROZEN in this
refactor (Part 2, C3). This rule governs the internal Rust shape — how a
900-line `config.rs` is split and named — not the serde field names.

### S11. Support crates mirror the discipline

The `derive/` crate is four phases, four files: `input.rs` (parse),
`attrs.rs` (attributes), `naming.rs` (the name-conversion rules),
`codegen.rs` (emission). A support crate is not exempt from S1–S6.

### S12. Examples are part of the surface

`examples/` holds one runnable file per capability (`put_file.rs`,
`error_handling.rs` — the latter against a mock server, producing real errors
per category). Doc examples compile (`no_run` where they need a network).

### S13. Cargo.toml is groomed

Sorted dependency tables, one feature per line in sorted arrays,
`default-features = false` wherever a default is not needed, optional deps in
their own block beneath the mandatory ones (reference `Cargo.toml`). Keywords,
description, readme present for every publishable crate.

### S14. Naming has a home

The reference dedicates `derive/src/naming.rs` to name-conversion rules —
naming logic is code with a file of its own, not string calls sprinkled
around. rdlt's equivalent (`rdlt-core/src/naming.rs`) already exists; every
crate that manipulates identifiers routes through it or through a similarly
dedicated module, never inline `.to_uppercase()` scattered at call sites.

---

## Part 2 — Binding constraints (verbatim into every prompt)

These override the rulebook wherever they conflict.

- **C1. Zero behavior change.** This is a structural refactor. Persisted DATA
  formats (WAL v2, bench artifact v3, `_rdlt_*` table shapes, StateDoc),
  golden SQL pins (sqlcore + postgres + duckdb + snowflake), and every
  recorded test expectation stay byte-identical. If a move would change
  emitted SQL or a serialized byte, the move is wrong.
- **C2. The SPI taxonomy is the contract.** `SourceError`/`DestinationError`
  variants and semantics in `rdlt-connector` do not change shape. `rdlt-core`
  and `rdlt-connector` are SEMVER-SACRED: any public rename there fails the
  `cargo semver-checks` gate against `main` and requires an explicit owner
  decision (the standing 0.2→0.3 bump recorded since 014 is the vehicle if
  taken). Default: internal reorganization only; public renames in these two
  crates are proposed in the increment's notes, not applied.
- **C3. User vocabulary is frozen.** YAML/JSON config field names, CLI
  arguments, feature names, crate names, error *rendering* users may have
  seen: unchanged. Renames apply to Rust identifiers, files, and modules.
- **C4. Constitution.** Principle V (typed errors, classification by
  structured code, substring-matching rendered errors FORBIDDEN), Principle
  VI (self-contained comments, no task/feature IDs in code), Principle III
  (library types wrapped at one boundary), Principle IX (contracts/persisted
  formats frozen). Workspace denies `unsafe_code`.
- **C5. Gate discipline.** Every increment ends with
  `env -u RUSTUP_TOOLCHAIN make check` green (that env var silently overrides
  the 1.96.0 pin; only the perf gate notices, by refusing a comparison — never
  re-record baselines to clear it). Tests run under `cargo nextest run`.
  Back-to-back gate runs on this host need `make reclaim` plus a TIME_WAIT
  drain first (see `specs/023-snowflake-put/close-out.md`).
- **C6. Commit taxonomy.** Three commit kinds, never mixed:
  1. **move-only** — `git mv` + path fixes, no content edits; reviewable by
     `git diff --find-renames` showing ~100% similarity.
  2. **rename** — identifier renames, mechanical, one concern per commit.
  3. **reshape** — splitting a file, extracting a module, rewriting docs.
  A reviewer must be able to verify each kind by its own cheap method.
- **C7. Test-binary names are load-bearing**, and in TWO opposite directions.
  The Makefile's `sweep` target selects `binary(crash_sweep)` / `binary(sweep)`
  positively — consolidating those empties the selection. But
  `.config/nextest.toml`'s iceberg override is a **negative** filter
  (`package(rdlt-connector-iceberg) and not binary(config_schema)`), so
  consolidation there does the reverse: it drags the one cheap non-container
  binary INTO the 3-thread JVM bound, and naming a live binary `config_schema`
  RELEASES it from the bound and starts 6+ Polaris JVMs whose health checks
  time out. A non-empty check passes in both failure modes. Update filters in
  the same commit as any consolidation, prove positive sets non-empty AND
  group membership unchanged, and note that nine `--no-tests=pass` flags
  currently let an empty selection read as a pass (Wave 0 P2 removes them).
- **C8. Live legs and credentials.** Credential-gated tests keep their
  skip-not-fail behavior and their announcements. Nothing from
  `~/.config/rdlt/` ever enters the tree (SC-005 mechanical sweep at each
  crate's close).
- **C9. The measurement instruments** (`ingestion_session.rs`,
  `scratch_reclaim.rs`, and the `#[ignore]` convention) keep their exact
  names and ignore-reasons — close-out documents cite them.
- **C10. Freeze lists — cross-crate symbols that may not move for this
  program's duration.** Established because the alternative (rewriting every
  dependent in the same commit) is only affordable where the surface is small.
  Frozen: `rdlt_engine::{Engine, EngineConfig, EventStream, RdltError,
  RunReport}` (4 root paths covering all 58 consumer references);
  `rdlt_connector_duckdb::dest::{DuckDb, DuckDb::open, FAIL_POINTS}` and the
  `failpoints` feature name; `rdlt_connector_file::dest::FAIL_POINTS` and
  `ParquetDir::open`; `rdlt_connector_postgres::dest::Postgres` plus the nine
  `dest::sqlgen` items `postgres/tests/golden_sql.rs` imports — narrowing
  `dest::sqlgen` forces those pins inline, destroying the only evidence that
  the extraction was behavior-free.
  **Deliberately NOT frozen, and why:** `rdlt-connector-sqlcore` (30
  module-qualified consumer paths, the same item reachable at two spellings)
  and `rdlt-testkit` (19, likewise double-spelled). Freezing either would gut
  its wave, which is exactly why both are scheduled early — see §3.1.

---

## Part 3 — Execution protocol

### 3.0 Does the order matter, and how much?

Yes, and it matters on three separate axes. Two are quantifiable.

**Axis 1 — post-reshape re-touching, weighted by net cost.** The count that matters is not "how many waves edit crate C" (a hub wave edits every consumer regardless of order) but "how many waves edit C *after* C's own careful reshape", because each of those forces C's safety net to be paid again. The drafted order incurs **11** such re-touches; the order below incurs **3**, all on the pilot, all ≤ ~15 reference lines, all provable inside `make check` in minutes.

The expensive part of that difference is one crate. The drafted order reshapes snowflake at wave 4 and then re-touches it at waves 5 (postgres — `differential_oracle.rs` constructs `rdlt_connector_postgres::dest::Postgres`), 6 (sqlcore — 32 reference lines across 8 snowflake files, five of which are credential-gated) and 7 (engine). Snowflake's honest gate is not `make check`: the Makefile has no snowflake line in the `sweep` target (verified: lines 99–105 cover engine, postgres, duckdb, rest, file, iceberg only), and `crates/rdlt-connector-snowflake/tests/crash_sweep.rs` is `#![cfg(feature = "failpoints")]`, a feature no gate command enables — so nothing in any pipeline even *type-checks* that file. Its run of record is 101.5 min (`specs/023-snowflake-put/close-out.md:176`, 4,308 s → 6,092 s, +41.4%) against live credentials. The drafted order owes that sweep three times; the order below owes it once, bracketed entry/exit. **Measured saving: ~3.4 h of live-credential sweep time, two booked quiet windows, and the account cost — plus ~190 duplicate cross-crate reference-line edits** (85 sqlcore refs in 19 files re-edited after their crates closed, plus most of the 100 testkit refs, plus duckdb's 46 unfinished-upstream refs).

**Axis 2 — gate tolls.** `make check` is workspace-wide and byte-identical no matter which crate changed: `lint` (clippy `--workspace --all-targets`) + `docs` + `nextest run --workspace` + doc-tests + the six-crate `sweep` arm + valgrind `iai` + hyperfine `cold` on a quiet machine — plus, on this host, containers and the `make reclaim` / TIME_WAIT ritual between back-to-back runs. So "which crate is cheap to gate" is a category error: the toll is per **increment**. The drafted "one crate = one increment" prices 13 tolls; batching crates that share no surface prices **10**. That is a rule change, not just an order change, and it is adopted below.

**Axis 3 — whether C1 is provable at all (not quantifiable, and the reason three placements are blocking).** In three places the drafted order reshapes a crate at the one moment its evidence for zero behavior change does not exist:

- **sqlcore at wave 6.** `crates/rdlt-connector-sqlcore/` has **no `tests/` directory** (verified). Its commit-protocol pins are the 15 `pin_*` functions inline at `src/protocol/mod.rs:455–1087` — inside the very 1,087-line file §4.8 splits. Its only external pins are `postgres/tests/golden_sql.rs`, `postgres/tests/golden_unit_sql.rs`, `postgres/tests/golden_ensure_sql.rs` and `duckdb/tests/golden_ensure_sql.rs` — **four files, in the two crates the draft reshapes at waves 1 and 5**, and S9 explicitly re-lays-out `tests/` directories. The draft reshapes the prover before the proven, then splits the remaining net in the same commit as the code it protects.
- **snowflake at wave 4.** Zero of its 15 test binaries pin generated SQL text offline; the two defects the live account caught (close-out D-32: `MATCH_BY_COLUMN_NAME` nulling an absent column, making merge survivors arbitrary; D-33: `$1:"COL"` into a staged file is case-sensitive, so the symmetrical-looking upper-case projection matched nothing and every column arrived NULL) are exactly the class a structural move can silently restore, and the symmetrical form is the wrong one.
- **engine's `wal/`.** WAL v2 is a persisted format under C1 whose only version guard is inline in `src/wal/mod.rs` and `src/wal/resume.rs` — the two files being split. `tests/wal_lifecycle.rs` pins directory lifecycle, not bytes; there is no `shred_identity_pin.rs` equivalent for the segment/manifest encoding.

None of those three is fixed by reordering alone. They are fixed by Wave 0.

### 3.1 Where the lenses disagreed, and what won

**sqlcore: early or last?** Two lenses put it first-of-the-consumers (churn: 85 refs × 3 nets, one of them the hand sweep); two put it dead last (provability: its net lives in its consumers, so only after they stop moving is a pin failure unambiguously sqlcore's fault). This is a genuine conflict and it dissolves on one measurement. **sqlcore's consumer-facing surface is not freezable; engine's is.** Consumers reach sqlcore through **30 distinct module-qualified paths** — `options::DestOptions`, `names::{STATE_TABLE, ARRIVAL_COL, stage_table, index_name, …}`, `plan::{scope_replace_sql, ValidateError}`, `protocol::{unit, FullLoadPublish}`, `ensure::*`, `MergeDialect`, `quote_ident`, `column_list`, `root_of` — with the same item reachable at two spellings (`DestOptions` at the root in duckdb and postgres, `options::DestOptions` in snowflake). S3 plus a split of `protocol/mod.rs` moves those paths by construction; freezing them would gut the wave on the crate this document itself calls the worst offender. So sqlcore's re-touch cost is unavoidable and must be paid **once, before** its three consumers. **I follow the churn lens on position and the provability lens on precondition:** sqlcore goes at wave 3, and Wave 0 lifts its 16 `pin_*` functions verbatim into a real `tests/` file so it owns an external net no consumer wave can move. Once that file exists, the "attribution" argument loses its force and the churn argument keeps all of its own.

**engine: early or late?** The churn lens pulls it to wave 4 on 58 reference lines. Measured, those 58 lines resolve to **4 distinct paths**, all crate-root re-exports: `rdlt_engine::{Engine, EngineConfig, EventStream, RdltError, RunReport}` (`EngineConfig` via `crash_sweep.rs:25` in snowflake, etc.). §4.9's prescribed work — `runtime/run.rs` split by phase, `load/mod.rs` to a TOC, `wal/resume.rs` replay-vs-validation — is entirely internal to those four symbols. Freeze them (C10) and engine-late costs approximately **zero** re-touch, while buying the crate with the highest correctness risk after postgres (Principle IV exactly-once, frozen WAL v2, `crash_sweep` + `shred_identity_pin`) the maximum calibration behind it. **Engine stays late.** The draft's stated reason for that placement ("internal-only, no public-surface risk") is wrong on the facts — it is a dev-dep of all six connectors — but the placement it produced is right for a different reason, and the reason matters because it licenses carelessness otherwise.

**The pilot.** Four candidates were argued: duckdb (draft), rest, core+connector, bench+cli. **rest wins.** duckdb is close to the worst available choice: it is *not* small in coupling (dev-dep of engine, postgres, rest, file — and its `dest::FAIL_POINTS` at `src/dest/mod.rs:262` plus `dest::DuckDb::open` are consumed by `engine/tests/crash_sweep.rs` at lines 175/185/189/238/240/329/338 and by postgres's `crash_sweep.rs`, i.e. the pilot's most likely mistake class — over-narrowing visibility during §4.1's S3 pass — lands on the workspace's most important net during the wave the rulebook is least trusted); it is *not* cheap (its dev-deps pull in `rdlt-connector-postgres` at 11,346 lines plus testcontainers, because it is the 013 differential oracle, so its inner loop boots a postgres container); and it teaches least (four src files, no `config.rs`, no error module, no naming module — it cannot exercise S4, S8, S10, S11 or S14, the five rules most likely to be applied wrongly). rest exercises the most rules against the most precise cheap net: `source/config.rs` at 809 lines with 13 serde types all deriving `schemars::JsonSchema`, pinned by `tests/config_schema.rs`, so the S10 split under C3's frozen vocabulary is proven by a **generated artifact** rather than by inspection; plus the 014 tagged-YAML compat deserializer (the subtlest frozen-serde hazard in the workspace), three `mod.rs` files holding logic, a public `PaginatorError` (S8), a 12-item `lib.rs` list (S7), an in-gate `binary(sweep)`, and nine test files already in S9's target shape — against wiremock, with no container, no JVM and no credential. core+connector cannot be the pilot (their renames cannot be *applied* without an owner decision, so they cannot calibrate S5/S14). bench+cli calibrate process but not the rulebook.

**testkit: wave 1 alone, wave 4, or wave 8?** Its consumer surface is 19 distinct paths, double-spelled the same way sqlcore's is (`PgFixture` at the root *and* `containers::PgFixture`; `MemorySource` at the root *and* `memory::`), so like sqlcore it is not freezable and must move before its 8 dev-dependents. But the provability lens's blocking claim about it is real and is about *detection*, not position: `containers::runtime_available`, the Option-returning `PgFixture` and `snowflake::credentials` decide whether 8 crates' legs **run or skip**, and a reshape that silently makes them report unavailable disarms every container and live net while every suite reports green. That is answered by Wave 0's committed test-**and-skip**-count baseline plus an assertive mode, not by ordering. **testkit goes at wave 4, batched with duckdb** (both ripples are the same cheap kind — compiler-verified import fixes — so they belong in one gate cycle rather than two).

**bench: early or late?** One lens claims bench is re-touched by the duckdb/file/postgres waves through the facade. **That is false, measured:** `crates/rdlt-bench/src` contains **zero** `rdlt::` or `rdlt_<crate>::` references (every hit is `rdlt_bench::` or a field name like `rdlt_rss`), and `Cargo.toml:21` declares `rdlt` with five features that appear to be library-unused. Its position is therefore **free** on churn grounds — which means it should be chosen on provability grounds, and there it is weak: 1,033 inline test lines against a 99-line `tests/selftest.rs`, it owns persisted artifact v3 and the `bars.toml` gate, and the `iai` arm is the only thing that catches `RUSTUP_TOOLCHAIN` silently overriding the 1.96.0 pin — which it catches by *refusing* a comparison whose benches show zero regressions. Reshaping it early removes the drift detector while the detector is under the knife. **bench goes last**, with fixture-driven pins as its entry condition.

**iceberg's nextest filter.** One lens has this backwards. Verified `.config/nextest.toml`: the override is `filter = "package(rdlt-connector-iceberg) and not binary(config_schema)"` — a **negative** filter. Consolidating iceberg's tests does not silently empty a group; it drags the one cheap non-container binary **into** the 3-thread JVM bound (a gate-time regression C7's non-empty check would pass), and folding live cells into a binary named `config_schema` **releases** them from the bound and starts 6+ Polaris JVMs whose health checks time out. Both directions are constrained; the fix is Wave 0's positive re-spelling. The genuine silent-empty risk is elsewhere: **nine** `--no-tests=pass` flags, including `binary(/e2e/)` whose only two matches in the workspace are `crates/rdlt-connector-file/tests/{e2e_copy.rs,e2e_duckdb.rs}` — and `TARGET=e2e` is not in `check` at all.

### 3.2 Wave 0 — PRE-WORK (lands on main; no crate is reshaped in it)

Every item is test-only, tooling-only or document-only. Nothing here has a `src/` diff outside item **P7**.

- **P1 — sqlcore gets a `tests/` directory.** Lift `src/protocol/mod.rs:455–1087` (the 15 `pin_*` functions) verbatim into a new `crates/rdlt-connector-sqlcore/tests/protocol_pins.rs`, switching `use super::*` to public `rdlt_connector_sqlcore::` paths and nothing else. Verified liftable as-is: the module calls only public API (`commit_script`, `prepare_target`, `staged_probe_targets`, `CommitError`; zero references to the private `check_single_unit` or `select_arm`, and `AbsentPolicy`/`MergeStrategy`/`Scd2Options`/`TableOptions` are all `pub`). Do the same for the inline modules in `ensure.rs`, `options.rs`, `names.rs`, `protocol/unit.rs`. **Proof: identical test names, identical count, zero `src/` behavior diff.** This single file is what makes wave 3 legal.
- **P2 — test selection stops lying.** Drop `--no-tests=pass` from every line whose target binary is known to exist (all nine: engine `crash_sweep`; postgres `crash_sweep`/`dest_crash_sweep`/`cdc_crash_sweep`/`memory_bound`; duckdb, rest, file, iceberg `sweep`; iceberg `spark_deep`; `binary(/e2e/)`), or replace it with a minimum-count assertion. Re-spell the iceberg override positively (`binary(catalog_live) or binary(conflict) or …`). Add `test TARGET=e2e` to `check`. Commit a per-binary **test-count and skip-count** baseline for `make check`, `make test TARGET=sweep` and the hand-run snowflake sweep; every wave diffs against it.
- **P3 — assertive gating mode in testkit.** Env flags that make `containers::runtime_available()`, `PgFixture::start()` and `snowflake::credentials()` **fail** instead of returning unavailable/None, plus `crates/rdlt-testkit/tests/gating_pin.rs` asserting each probe's decision against a forced environment. Every wave from 1 on runs its verification with assertive mode set, so a skip is never mistaken for a pass.
- **P4 — crash-point registries stop being self-referential.** `crates/rdlt-engine/tests/crash_sweep.rs:227–231` asserts that the engine's `crash_point!` *sites* equal `ENGINE_POINTS`. The five connectors only assert `fired == FAIL_POINTS` (duckdb `sweep.rs:169`, file `sweep.rs:94,168`, iceberg `sweep.rs:112`, rest `sweep.rs:120`, postgres `dest_crash_sweep.rs:171,323`) — compared against a constant that lives inside the `mod.rs` being split, so a split that drops a crash point passes with a smaller matrix and no signal. Port engine's site-vs-list assertion to all five.
- **P5 — `make semver` becomes a real verb.** `cargo semver-checks` appears nowhere in the Makefile; the only invocation is in CI, against `origin/main`, which is **73 commits stale** (local main 15f17c65 vs origin/main 1076398b) — a green run there would diff against a pre-001..023 surface and report a wall of pre-existing breakage indistinguishable from the program's own. Add `make semver` pinned to a **recorded baseline sha on the local tree**, establish that sha here, and add it to `check`. Without this, wave 2's only safety net does not exist.
- **P6 — C10, the freeze lists.** Add a binding constraint enumerating, per crate, the cross-crate-visible symbols that may not move for the program's duration, with the alternative stated (rewrite every dependent in the same commit):
  - `rdlt_engine::{Engine, EngineConfig, EventStream, RdltError, RunReport}` — 4 root paths covering all 58 consumer reference lines.
  - `rdlt_connector_duckdb::dest::{DuckDb, DuckDb::open, FAIL_POINTS}` and the `failpoints` feature name (consumed by engine, postgres, rest, file; `rest/Cargo.toml` chains `rdlt-connector-duckdb/failpoints`).
  - `rdlt_connector_file::dest::FAIL_POINTS`, `rdlt_connector_file::ParquetDir::open` (engine `crash_sweep.rs:155,161,165`).
  - `rdlt_connector_postgres::dest::Postgres` (duckdb `differential.rs`, snowflake `differential_oracle.rs`) **and** the nine items `postgres/tests/golden_sql.rs` imports across `dest::sqlgen` — `{HardDelete, MergePlan, PgDialect, identity_delete_insert_sql, keyed_delete_insert_sql, keyed_upsert_sql, scd2_merge_sql, scope_replace_sql}` plus `dest::{DedupSort, Scd2Options, SortOrder}`. Narrowing `dest::sqlgen` forces the pins inline, which destroys the only evidence the extraction was behavior-free; that file's own header already states the rule. Any narrowing proposal is a separate, later, owner-visible change.
  - **Explicitly NOT frozen, and why:** sqlcore (30 module-qualified consumer paths, double-spelled) and testkit (19, double-spelled). Those two are unfreezable without gutting their waves, which is precisely why they go early.
- **P7 — snowflake stops being invisible.** Add a snowflake line to the `sweep` target (or at minimum `cargo clippy -p rdlt-connector-snowflake --all-targets --features failpoints -- -D warnings` to `lint`) so `crash_sweep.rs` is at least type-checked per increment, and codify in the `coverage` target the `-E 'not (package(rdlt-connector-snowflake) and binary(crash_sweep))'` exclusion that `close-out.md:311–313` records as having been used but which exists in no Makefile, script or config. This is the only `src`-adjacent item in Wave 0 and it is Makefile-only.
- **P8 — rulebook corrections.** (a) **Narrow S4 to one named site.** `crates/rdlt-connector-snowflake/src/dest/client.rs` contains **zero** `Deserialize` (verified) — the forked `snowflake-connector-rs` owns the wire, and that file's own module doc already declares it the crate's one boundary with the library, i.e. Principle III does S4's job by wrapping instead of by naming. The only serde in snowflake's `src` is `dest/config.rs`, frozen by C3. S4's single legitimate site in 54k lines is `crates/rdlt-connector-postgres/src/source/cdc/pgoutput.rs` (`TupleData`, `TupleValue`, `RelationColumn`, `Relation`, `Message` — literally the pgoutput logical-replication format). **Delete §4.6's `wire/` + `Wire*` instruction**; left standing, the cheapest thing an executing AI can find to rename `Wire*` is the config or state types, a C1/C3 violation discovered at wave 7 on the most expensive net in the repo. (b) **Write §4.14 rdlt-connector-iceberg** — Part 4 has sections 4.1–4.13 covering 13 of 14 crates and iceberg is the omission, so Part 3's prompt-assembly instruction ("paste the one crate section from Part 4") cannot be executed for it. Include the S9 consolidation **exemption** with both failure directions from §3.1. (c) **Write the S9 `tests/cases/common.rs` template as a concrete file here.** §4.2 asks testkit to establish it; testkit cannot — its `tests/` is two files (`conformance_memory.rs` 38 lines, `conformance_negative.rs` 133) with no expensive resource to share behind a `OnceCell`. The cheap half is validated at wave 1 (rest, wiremock); the expensive half at wave 4 (duckdb's container). (d) Amend this Part: **one WAVE = one gate cycle**, and add the `make reclaim` + TIME_WAIT drain ritual to the prompt so back-to-back gates do not die on podman `rootlessport bind`.

**Wave 0 exit:** `env -u RUSTUP_TOOLCHAIN make check` green with the new legs; the skip-count baseline committed and reproducing on a second run; `make semver` reporting no update required against the recorded sha; owner decisions §3.6 recorded in writing.

### 3.3 The order

| wave | crates | position rationale (graph + net, not size) |
|---|---|---|
| 0 | *(none — pre-work)* | Makes C1 provable for sqlcore, snowflake and engine; makes an emptied filter fail; makes the sacred surface machine-gated |
| 1 | rdlt-connector-rest | **Pilot.** Most rules per gate-minute, zero dev-dependents, generated-schema pin proves the frozen-serde split, no container/JVM/credential |
| 2 | rdlt-core, rdlt-connector | Vocabulary final **before** 10 dependents are touched; frozen public surface ⇒ zero ripple by construction; net is `make semver`, seconds |
| 3 | rdlt-connector-sqlcore | 30 unfreezable consumer paths + 3 `impl MergeDialect`; must precede duckdb/postgres/snowflake or 85 refs and 3 nets are paid twice. Legal only because of P1 |
| 4 | rdlt-testkit, rdlt-connector-duckdb | The two dev-dep hubs, one gate cycle; both ripples are compiler-verified import fixes. duckdb also certifies the differential oracle before four crates measure against it |
| 5 | rdlt-connector-file, rdlt-connector-iceberg | Container wave, two parallel branches, one gate cycle. file's `location/mod.rs` 511 is the biggest connector seam decision — taken with the rulebook calibrated, not before |
| 6 | rdlt-connector-postgres | Largest crate, best net, all upstream final; its three `golden_*.rs` are only re-laid-out **after** sqlcore has finished using them |
| 7 | rdlt-connector-snowflake | Last connector. Its 101.5-min hand sweep is paid exactly once, bracketed |
| 8 | rdlt-engine | Highest correctness risk after postgres; C10 makes its 4-symbol surface free to move late; its `crash_sweep` consumes duckdb/file symbols that are final by now |
| 9 | rdlt (facade), then rdlt-cli + rdlt-bench | Pure leaves. Facade first (it is `cli/tests/build_parity.rs`'s subject); cli and bench after, in the same cycle |

### 3.4 Entry and exit conditions

**Wave 1 — rest.** *Entry:* Wave 0 on main; assertive mode on; `cargo nextest list -p rdlt-connector-rest` snapshotted. *Exit:* `tests/config_schema.rs` green with the generated schema **byte-identical** and the documented example still validating and parsing; `binary(sweep)` non-empty and its count matching baseline; the S9 idiom and the S3 visibility recipe written back into Part 1 as this wave's non-code deliverable; increment-size answer recorded; `make check` green.

**Wave 2 — core + connector.** *Entry:* owner's §3.6 answer recorded; `make semver` (P5) working. *Exit:* `make semver` reports **no update required** — or the owner has taken the 0.2→0.3 window and the renames plus their call sites (7 `objects::` sites, all in `crates/rdlt-connector-file/src/location/{mod.rs,s3.rs}`; 4 `rdlt_core::identity::` sites) land in this wave; `tests/object_safe.rs` green; secret sweep clean.

**Wave 3 — sqlcore.** *Entry:* `git diff main -- crates/rdlt-connector-postgres/tests/golden_sql.rs crates/rdlt-connector-postgres/tests/golden_unit_sql.rs crates/rdlt-connector-postgres/tests/golden_ensure_sql.rs crates/rdlt-connector-duckdb/tests/golden_ensure_sql.rs` **empty** (those four files are sqlcore's entire external net; note there are four, in two crates); `tests/protocol_pins.rs` from P1 green and its name set snapshotted; the root-vs-module rule for `DestOptions`/`quote_ident`/`column_list` decided up front (today duckdb and postgres import from the root, snowflake from `options::` — one spelling wins). *Exit:* all 85 reference lines plus the 3 `impl MergeDialect` sites (`duckdb/src/dest/dialect.rs:14`, `postgres/src/dest/dialect.rs:11`, `snowflake/src/dest/dialect.rs:47`) rewritten in the same commit; the four golden files **byte-identical**; `protocol_pins.rs` name set identical, not merely green; all three consumers' suites pass unchanged.

**Wave 4 — testkit + duckdb.** *Entry:* wave 3 merged; container runtime up; C10's duckdb freeze restated in the prompt; duckdb's commits ordered before any file-crate work is scheduled. *Exit:* the skip-count baseline from P2 **identical** (this is testkit's real proof, not exit 0); all 8 dev-dependents compile and pass; `golden_ensure_sql.rs` and `differential.rs` observed green **with a runtime present** under assertive mode; `binary(sweep)` count matching; the `OnceCell`-shared-container + `unique_*` idiom validated and written back into Part 1. If testkit's `snowflake.rs` → `credentials/snowflake.rs` rename is taken, it is its own commit with the 10 external call sites listed in the body — and it defers to wave 7 if it would touch a credentialed file.

**Wave 5 — file + iceberg.** *Entry:* wave 2 vocabulary final (so file's `objects` sites are touched once) and wave 4 merged; RUSTFS and Polaris up under assertive mode; §4.14 written; the iceberg S9 exemption in the prompt verbatim. *Exit:* `cargo nextest list -p rdlt-connector-iceberg` **group membership** diffed identical, not just test counts; `iceberg/dest/test_support.rs` still private; `binary(/e2e/)` non-empty (its only two matches are in file, and P2 has removed the flag that hid an empty selection); both `binary(sweep)` lines matching baseline; flake profile classifications recorded before and after.

**Wave 6 — postgres.** *Entry:* wave 3 merged and its four pins re-verified; C10's `dest::sqlgen` exemption declared for the increment. Split the wave in two stages: **(a)** reshape `src/` with `golden_sql.rs`, `golden_unit_sql.rs`, `golden_ensure_sql.rs` and `pg_copy_wire_pin.rs` **untouched**; **(b)** only then re-lay-out `tests/` under S9. *Exit:* all four pins byte-identical (a required diff **ends** the reshape — the file's own header states this); `crash_sweep`, `dest_crash_sweep`, `cdc_crash_sweep`, `memory_bound` each non-empty and matching count; duckdb's `differential.rs` still green from outside; `iai_pg` baselines compared by name, and any rename reviewed as a name change with an unchanged count rather than cleared with `--record`.

**Wave 7 — snowflake.** *Entry:* 023 merged to main (or explicitly frozen) — a structural refactor landing before that merge either rebases across a 3,979-line rewrite or conflicts file-for-file, and no `--find-renames` review survives that; live credentials present; a quiet window booked; **and, as this wave's first commit (test-only): `crates/rdlt-connector-snowflake/tests/golden_snowflake_sql.rs`**, modelled on postgres's `golden_sql.rs`, pure builders, no account, routed through the existing `dest::testhook` seam rather than widening the public API, pinning as literal text the COPY INTO with its **explicit** column projection in both cases, `FILES=()` built from PUT's reported `target` (basename + compression suffix, relative to the FROM prefix — never LIST's `name`), the named-stage DDL, the MERGE INTO + QUALIFY publish, and CASE_INSENSITIVE column matching. *Exit:* the 101.5-min sweep run to completion **before and after** the reshape with identical fired-point matrices; the credentialed live legs run with their skip announcements itemized (password + OAuth remain UNPERFORMED by owner decision, the same call 022 and 023 made); `ingestion_session.rs`, `scratch_reclaim.rs` and `crash_sweep` keep their exact names and ignore-reasons (C9); D-32/D-33 comments moved verbatim; the golden text pins byte-identical. **Standing rule: nothing re-touches snowflake after this wave. If a later wave would, it belongs before this one.**

**Wave 8 — engine.** *Entry:* all six connectors final; C10's 4-symbol freeze restated; **and, as this wave's first commit (test-only): `crates/rdlt-engine/tests/wal_format_pin.rs`** — a committed v2 manifest + arrow-IPC segment fixture that must replay to identical rows and identical `_rdlt_id`s, with exact-match refusal cells in **both** directions (v1 refused, v3 refused), captured from the shipping build and carrying the same regenerate-deliberately warning `shred_identity_pin.rs` carries. Then lift `load/lowering.rs`'s 397 and `wal/resume.rs`'s 395 inline test lines to `tests/`. *Exit:* `shred_identity_pin` and `wal_format_pin` byte-identical; `binary(crash_sweep)` and `test(shred_property)` both non-empty with counts matching; `iai_hotpath`'s four baselines compared by name; `run.rs` history shows moves with no logic edits in the same commit.

**Wave 9 — facade, then cli + bench.** *Entry:* everything merged; for bench, test-only pre-work first — lift the artifact v1-rejection (`src/artifact.rs:279`) and v3 round-trip into `tests/artifact_format_pin.rs` with a committed v3 fixture, plus fixture-driven pins for `sample.rs` median selection (including warmup-uncounted) and `gate.rs` bar evaluation (including the refuse-on-zero-regressions case); for cli, lift `main.rs:222–592` and `cdc.rs:92–188` out and add an invocation-level pin over the frozen flag/subcommand surface. Facade lands first and `cli/tests/build_parity.rs` is green against it before cli itself moves. *Exit:* every facade path any README mentions exists; 13 READMEs re-checked; `pub use rdlt_connector_file as parquet` alias intact; CLI vocabulary unchanged; cold-start re-measured ≤ 40 ms on a quiet machine; `make bench TARGET=gate` green against committed artifacts; `make semver` clean; coverage ≥ 80; `env -u RUSTUP_TOOLCHAIN make check` **twice clean**; the closing rename-proposal list (anything discovered in waves 3–8) delivered as one document; this file deleted with its conclusions in the close-out.

### 3.5 What is parallelizable

- **Wave 5 only, as two branches:** file and iceberg share no code and neither is a dev-dep of the other. Precondition: all shared-file edits (Makefile filter block, `.config/nextest.toml`) were done in Wave 0, so each branch touches only its own crate directory — otherwise three concurrent branches conflict on the same two files and every rebase re-runs a container gate.
- **Wave 4 is batched but sequenced internally:** testkit and duckdb in one gate cycle, duckdb's commits landing before any file-crate work is scheduled (file dev-depends on duckdb).
- **Wave 9 is batched:** cli and bench share no surface, but the facade's commits land first.
- **Not parallelizable, contrary to the draft's wave 3:** rest, file and iceberg are independent in the graph but rest's and file's suites both import `rdlt_connector_duckdb::dest::DuckDb` (file at seven sites across `e2e_duckdb.rs`, `s3_live.rs`, `sweep.rs`), so none of them may run concurrently with duckdb's wave. Waves 3, 6, 7 and 8 are strictly serial.

### 3.6 Decisions that are the owner's, not the executor's

1. **Does the standing 0.2→0.3 window open for this program?** It gates two named renames: `objects` → `object_store` (`rdlt-connector/src/lib.rs:24`, ripple = 3 production + 4 inline-test call sites, all in `crates/rdlt-connector-file/src/location/`) and `identity.rs` → `row_identity.rs` (4 external `rdlt_core::identity::` sites). **Recommendation: NO — keep the window closed, but answer the question at Wave 0 rather than at wave 9.** Cost of NO: two documented S1 violations persist, carried forward on the proposal list to whatever feature next needs a real break; zero code. Cost of YES: ~11 call sites plus a MAJOR bump on the two sacred crates — cheap in code, but it spends the single recorded vehicle (untaken since feature 014, across nine features) on cosmetics, and no persisted format or SPI shape is changing here. Note the asymmetry that argues the other way if the owner wants it: `rdlt`, `rdlt-cli` and `rdlt-connector-snowflake` are already unpublishable while 023's git-without-version fork dep stands, so a bump is unusually cheap **right now**. Either answer is fine; **deciding late is not** — wave 9 placement structurally forbids the renames from reaching the eleven crates refactored earlier.
2. **Is one 101.5-min hand sweep bracketed (≈3.4 h total, two runs) budgeted for wave 7, with the credentialed live legs?** **Recommendation: yes.** Cost of no: snowflake must be dropped from the program entirely (a structural refactor of 3,979 lines with no admissible proof is not a refactor), or accepted unproven with the risk of silently restoring D-32/D-33 — the two defects only the live account caught.
3. **Is 023 merged to main before wave 1?** **Recommendation: merge it** (its two deliberately un-amended misses are recorded and independent of this program). Cost of not merging: wave 7 rebases across a wholesale rewrite; `git diff --find-renames` review, which is C6's only cheap verification method for move-only commits, stops working.
4. **May `make check` grow?** Wave 0 adds `TARGET=e2e`, `make semver`, a snowflake failpoints clippy leg, and removes nine `--no-tests=pass`. **Recommendation: yes.** Cost: a few minutes per gate cycle × 10 cycles. Cost of no: an emptied filter reads as a passing gate, and wave 2's only net does not exist.
5. **One wave = one gate cycle (10 tolls) or one crate = one increment (13)?** **Recommendation: batch.** C6's commit taxonomy already keeps reviewability intact; the saving is three full workspace cycles including containers, valgrind, hyperfine and the reclaim ritual.
6. **Confirm the C10 freeze lists (P6) as binding**, including the `dest::sqlgen` exemption — i.e. accept that this program will leave a handful of test-facing paths un-idealised and route them through a later, owner-visible change. **Recommendation: yes.** Cost of no: waves 6 and 8 become multi-crate commits and the postgres golden pins have to be relocated inside the commit that reshapes what they pin, which forfeits the extraction's only evidence.

### 3.7 Corrections to facts stated elsewhere in this document

- sqlcore's external pins are **four** files in **two** crates: `postgres/tests/{golden_sql.rs, golden_unit_sql.rs, golden_ensure_sql.rs}` and `duckdb/tests/golden_ensure_sql.rs`. §4.8's "golden pins are the safety net" is true and is an argument for scheduling sqlcore **before** the crates that hold them.
- Part 4 has **no iceberg section** (4.1–4.13 cover the other 13 crates), so Part 3's prompt-assembly instruction cannot be executed for it as written. P8(b) fixes this.
- The crate size column mixes conventions for two crates: sqlcore's 3,544 includes 1,202 inline test lines (~2,342 production) and cli's 780 includes 468 (312 production). Both are smaller in code and larger in net-to-be-relocated than the column implies; scope their waves off the corrected numbers.
- iceberg is not "containers only": it ships `tests/sweep.rs` behind Makefile line 105 and a `config_schema` binary that the 3-thread group is keyed on by **exclusion**.
- `crates/rdlt-bench/src` has **zero** library coupling to `rdlt` — the declared `rdlt` dependency with five features (`Cargo.toml:21`) appears library-unused and should be investigated, not blindly removed, during S13 grooming.

---

## Part 4 — Crate-by-crate instructions

Line counts measured at this document's writing; treat them as pointers, not
gospel — re-measure before cutting.

### 4.1 rdlt-connector-duckdb (wave 4, batched with testkit — NOT the pilot; see §3.1)

Current: `lib.rs`, `dest/mod.rs`, `dest/commit.rs` (509), `dest/dialect.rs`.

- **C10 freeze, binding:** `dest::DuckDb`, `dest::DuckDb::open` and
  `dest::FAIL_POINTS` do NOT move. `engine/tests/crash_sweep.rs` reaches them
  at seven sites and postgres's sweep at more; over-narrowing them during the
  S3 pass breaks the workspace's most important net. Either keep the paths or
  rewrite all 23 dependent references in the same commit.
- `dest/mod.rs` (373 production lines) holds the `Destination`/session
  implementation — logic in a `mod.rs` (S2). Extract into `dest/session.rs`
  (the session type and its trait impls) and leave `mod.rs` as declarations +
  re-exports, with `FAIL_POINTS` still re-exported at its current path.
- `dest/commit.rs`: verify it is one concept; if it holds both step execution
  and DDL/ensure logic, split along the reference's noun rule (S1) —
  `commit.rs` keeps step execution, extract `ensure.rs`/`ddl.rs` as content
  dictates.
- Apply S3 visibility: internals `pub(crate)`/`pub(super)`; audit `lib.rs`
  re-exports into a curated list (S7) with a front-page doc example (the
  crate has `examples/jsonl_to_duckdb.rs` — the lib.rs example can mirror it).
- Tests: inline unit tests stay; if `tests/` has several small topic files,
  consolidate under one root with `cases/` (S9) — checking C7 first
  (`binary(sweep)` is in the Makefile for this crate).
- Golden pins byte-identical (C1). Gate green.

### 4.2 rdlt-testkit

Current: `conformance/`, `containers.rs`, `crash.rs`, `fixtures.rs`,
`memory/`, `snowflake.rs`, `util.rs`.

- `util.rs` is a name the reference would never ship (S1): open it, name what
  it actually holds, and move each piece to that name — dissolve the file.
- `snowflake.rs` holds the credential gate + scratch naming; rename toward
  role clarity (e.g. `credentials/snowflake.rs` if more gates are plausible,
  else keep but restructure internally by S5).
- This crate DEFINES the S9 idioms other crates copy: make `memory/` and
  `conformance/` exemplary — module docs stating the contract each conformance
  suite enforces, `Planned*`-style naming where suites build plans.
- Establish here the documented pattern for per-crate `tests/cases/common.rs`
  (OnceCell-shared expensive resources, `unique_*` name helpers) that waves
  3–5 will follow.

### 4.3 rdlt-bench

Current: 13 flat files; `competitors.rs` (869), `fixtures.rs` (634),
`runner.rs` (626), `report.rs` (560).

- `competitors.rs` → `competitors/` directory: `mod.rs` (TOC),
  `dlt.rs`, `airbyte.rs`, plus whatever shared probe/setup logic earns its
  own noun (S1/S2).
- `fixtures.rs` → `fixtures/` split by fixture kind (containers, datasets,
  seeds) if the content supports it.
- `runner.rs`/`report.rs`/`artifact.rs`/`gate.rs` names are already good
  nouns; check each for second concepts hiding inside (S1).
- Bench artifact format v3 is persisted — C1 applies to its serde shape.
- publish=false, no semver concerns: this crate can take the full treatment.

### 4.4 rdlt-connector-file

Current: `location/mod.rs` (713!), `location/s3.rs`, `location/types.rs`,
`source/` (mod 574, config, cursor), `dest/` (7 files), `formats/`.

- `location/mod.rs` is the biggest S2 violation in this crate: extract the
  Location enum + dispatch into `location/location.rs`… no — better nouns:
  keep `types.rs` for the vocabulary, extract listing/IO logic into
  `local.rs` (the reference has `put/stage/local.rs`) and let `s3.rs` hold
  everything S3, leaving `mod.rs` as TOC.
- `source/mod.rs` (574): extract the Source impl into a named file
  (`reader.rs` or `source.rs`-adjacent noun), TOC the mod.
- `dest/` file names are already good (`layout.rs`, `truncate.rs`,
  `inspect.rs`, `writer_props.rs`) — audit contents against their names,
  apply S3/S5/S6.
- `formats/` matches the reference's shape already; polish docs per S6.

### 4.5 rdlt-connector-rest

Current: `source/client/` (auth, mod), `source/read/` (driver, extract,
fanout, paginate, resolve), `source/config.rs` (809), `source/mod.rs`.

- `read/` submodule names are strong; keep. `client/mod.rs` — move logic to
  named files (`http.rs` or `transport.rs` for the request path), TOC the mod
  (S2).
- `config.rs` (809): split by concern per S10 — auth config, pagination
  config, incremental config, stream config as separate files under
  `source/config/`, with the top-level type composing them. Serde shape
  FROZEN (C3): the split is `mod`-internal, the YAML view unchanged —
  round-trip tests prove it.
- The 014 tagged-YAML compat deserializer is subtle code: move it whole
  (move-only commit), never reshape it in the same commit as anything else.
- Response-action matching is Principle V territory — do not touch its typed
  matching while moving it.

### 4.6 rdlt-connector-snowflake

Current: `dest/{client (689), session (780), stage (566), ddl (591), encode,
config, dialect, mod}`; `testhook` in `dest/mod.rs`.

This crate shares a domain with the reference — it should end up looking like
a sibling of it.

- `client.rs` (563 production lines) holds transport + executor + error
  translation. Split it into `executor.rs` (the execute/rows surface) and
  `error.rs` (the structured-code→taxonomy translation; S8, scoped to
  translation INTO the SPI taxonomy, which is itself frozen per C2).
  **Do NOT create a `wire/` module or any `Wire*` type here** — see the scope
  box on S4: this file has zero serde derives, the forked driver owns the wire,
  and the only `Deserialize` in the crate is `dest/config.rs`, frozen by C3.
- `session.rs` (780): extract the publish-step execution and the
  staged-parts bookkeeping into named files (`publish.rs`, `pending.rs` or
  nouns the content dictates); the session struct and trait impl stay in
  `session.rs` (S1).
- `stage.rs` (566) is close to right; consider the reference's
  `put/plan.rs` precedent: the naming/prefix derivation could be `naming.rs`
  (S14), reclaim into `reclaim.rs` — only if the seams are clean.
- `testhook` → `test_support`, `#[doc(hidden)]`, gated where feasible
  (S3): live tests in `tests/` need it compiled without `cfg(test)`, so use
  the self-dev-dependency feature trick or keep it pub+hidden — decide once,
  document at the module.
- Comments here already carry the account-measured facts (D-30/D-31 case
  sensitivity, MATCH_BY_COLUMN_NAME) — preserve them verbatim through moves
  (C1 spirit: those comments are load-bearing).

### 4.7 rdlt-connector-postgres

Current: 31 files; `source/config.rs` (984), `source/cursor.rs` (926),
`source/copy_decode.rs` (813), `source/mod.rs` (792), `dest/commit.rs`
(821), `dest/encode.rs` (698), plus floating `pgerror.rs`, `tls_verify.rs`,
`source/errors.rs`.

- Error organization per S8: `pgerror.rs` + `source/errors.rs` +
  `tls_verify.rs` → an `error/` module family: `error/mod.rs` (TOC),
  `error/db.rs` (SQLSTATE + server-message extraction), `error/tls.rs`,
  `error/cdc.rs` if the content splits that way. Classification stays typed
  (C4/Principle V).
- `source/mod.rs` (792): extract the Source type into a named file; TOC the
  mod (S2).
- `source/config.rs` (984): S10 split under `source/config/` — connection,
  tls-policy view, discovery, streams, incremental — serde shape frozen (C3).
- `source/cursor.rs` (926) and `copy_decode.rs` (813): split only along real
  seams (cursor arithmetic vs watermark policy; decode dispatch vs per-type
  decoders). If no honest seam exists, a justified size is acceptable (S1's
  escape hatch) — write the justification into the module doc.
- `cdc/` names are already good nouns (`slot.rs`, `tail.rs`, `pgoutput.rs`,
  `apply.rs`); `pgoutput.rs` is wire territory — consider `wire/` naming
  (S4) but weigh against its fuzz-target coupling (`pg_pgoutput_decode`
  target name must keep working).
- `dest/` mirrors the snowflake treatment (4.6).

### 4.8 rdlt-connector-sqlcore

Current: `protocol/mod.rs` (1,087 — the workspace's largest file, in a
`mod.rs`), `protocol/unit.rs`, `plan/` (arms, index, validate, mod),
`dialect.rs`, `options.rs` (528), `ensure.rs`, `names.rs`.

- `protocol/mod.rs` → TOC (S2). Extract: `step.rs` (the `Step` enum and its
  invariants), `script.rs` (the `commit_script` planner), `context.rs`
  (`CommitCtx` and friends) — or the nouns the content dictates. The module
  doc's planner/executor contract (pure function, no driver types,
  executors may not reorder) moves to wherever `commit_script` lands.
- The golden pins are the safety net and the acceptance test: **byte-identical
  emitted SQL** after every commit in this crate (C1). Run the pin suites of
  all three consuming destinations, not just this crate's.
- `options.rs` (528): S10 audit — likely fine as the shared vocabulary file,
  but check for a validation module hiding inside.
- `names.rs` is S14 done right already; leave.

### 4.9 rdlt-engine

Current: `runtime/run.rs` (944), `wal/resume.rs` (751), `shred/` (8 files,
good nouns), `load/` (lowering 593, mod 532, apply), `schema/`,
`fuzzing.rs`.

- `runtime/run.rs` (944): the run loop has phases — split by phase noun
  (`retry.rs` for the backoff/classification loop, `drive.rs` or
  `progress.rs` for the pump, whatever the seams support). The exactly-once
  machinery is Principle IV territory: moves only, no logic edits, crash
  sweep green after every commit.
- `load/mod.rs` (532): TOC per S2; extract the load orchestration into a
  named file.
- `wal/resume.rs` (751): split replay vs validation if the seam is real;
  WAL v2 format frozen (C1).
- `shred/` is already reference-grade naming (`arena`, `tape`, `canon`,
  `infer`, `view`) — S6 doc polish only.
- `fuzzing.rs`: fuzz-target coupling — the `fuzz/` workspace references these
  paths; verify targets still build.

### 4.10 rdlt-cli

Current: `main.rs` (592), `cdc.rs`.

- Principle II says thin CLI; the reference's analog is examples-grade
  clarity. Split `main.rs`: argument surface (`args.rs`), command dispatch
  (`commands/run.rs`, `commands/cdc.rs` absorbing `cdc.rs`), rendering
  (`render.rs`) — `main.rs` becomes parse → dispatch, under ~80 lines.
- The quickstart/config parse pins live in CLI tests — they are close-out
  citations; names stay (C9-adjacent).

### 4.11 rdlt (facade)

Current: `lib.rs`, `builder.rs`, `pipeline_spec.rs`.

- Already thin. Work: S7 front-page — the lib.rs doc example should be the
  reference-grade "whole workflow in one glance" (build a pipeline from a
  YAML spec, run it, read the report; `no_run`).
- Add `examples/`: one per major path (S12) — `pipeline_from_yaml.rs`,
  `embedded_source_to_dest.rs`. Examples compile in `make check` via the
  normal cargo example build; keep them dependency-light.
- Audit the re-export surface: every facade path a README mentions exists and
  is documented.

### 4.12 rdlt-core (SEMVER-SACRED — internal only)

Current: 14 well-named files (`commit.rs`, `cursor.rs`, `schema.rs`, …) plus
the pair `ids.rs` / `identity.rs`.

- The `ids.rs` vs `identity.rs` pair fails S1's "the name states the one
  thing" test: `ids.rs` holds the identifier NEWTYPES (`PipelineId`,
  `LoadId`, …) while `identity.rs` holds deterministic ROW-identity hashing
  (`_rdlt_id` derivation) — related words, unrelated concepts, and nothing in
  the names says which is which. PROPOSE (do not apply — C2)
  `identity.rs` → `row_identity.rs`; meanwhile both module docs already state
  their scope in line one — keep that.
- S6/S7 polish freely (docs are not semver). Internal `pub(crate)` tightening
  is allowed where semver-checks proves it invisible.
- Ship the accumulated public-rename proposals from all waves as a single
  decision list for the owner (the 0.2→0.3 vehicle).

### 4.13 rdlt-connector (SEMVER-SACRED — internal only)

Current: 10 files; the taxonomy in `error.rs` (frozen, C2); `objects.rs`.

- `objects.rs` is the S1 failure the reference would not ship: it holds the
  object-store recoverability rule behind the `object-store` feature, and its
  name says neither. The module is declared `pub mod objects` (lib.rs:24), so
  renaming it IS a public-surface change: it goes on the proposal list
  (`objects` → `object_store`, matching the feature name), not into this
  increment. The file connector's seven call sites are the ripple.
- S7: the lib.rs is already close to reference-grade; add the missing
  runnable example (implementing a toy Source against the SPI, `no_run`).
- Everything else: doc polish, visibility tightening proven invisible by
  semver-checks.

### 4.14 rdlt-connector-iceberg (wave 5, parallel branch with file)

The section Part 4 originally omitted. Current: 13 src files, 2,934 lines —
`dest/schema.rs` (571), `dest/session.rs` (457), `dest/config.rs` (425),
`dest/ensure.rs` (260), `dest/commit.rs` (247), `dest/errors.rs` (232),
`dest/test_support.rs` (177), plus catalog/state/writer/writer_props/mod.
13 test files with a `common/` and a `fixtures/`.

- `dest/errors.rs` is the S8 site: it is the ONE boundary where
  `iceberg-rust` types are classified into the SPI taxonomy (Principle III).
  Restructure it into an `error/` family only if it holds more than one
  concept; otherwise rename nothing and polish docs. The taxonomy it
  translates INTO is frozen (C2).
- `dest/schema.rs` (571) is the largest file: the closed type mapping plus
  additive drift plus field-ID handling. Split along those seams if they are
  real — `type_map.rs` for the mapping, `drift.rs` for `UpdateSchema` — and
  write the justification into the module doc if they are not.
- `dest/test_support.rs` is already `mod test_support;` — **private, and it
  must stay private** (S3 done right; this is the crate to copy, not fix).
- `dest/config.rs` (425): S10 audit. Serde shape frozen (C3);
  `tests/config_schema.rs` is the generated-artifact proof, exactly as in rest.

**S9 EXEMPTION, binding — do not consolidate this crate's test binaries.**
`.config/nextest.toml` bounds the iceberg live group with a NEGATIVE filter
(`package(rdlt-connector-iceberg) and not binary(config_schema)`), so both
directions of consolidation break it: folding `config_schema` into another
binary drags the one cheap non-container test into the 3-thread JVM bound (a
gate-time regression that a non-empty check passes), and naming a live binary
`config_schema` releases 6+ Polaris JVMs from the bound and their health checks
time out. Wave 0 P2 re-spells the filter positively; until that has landed and
been verified, leave all 13 binaries exactly as named. `binary(sweep)` is
selected positively by the Makefile and is subject to the ordinary C7 rule.

**Gate note:** this crate's fixtures are a Polaris JVM plus RUSTFS on
host-network podman with PID-derived ports below the ephemeral range. Bring
them up before starting and verify `cargo nextest list -p
rdlt-connector-iceberg` reports identical GROUP MEMBERSHIP — not merely
identical counts — at exit.

---

## Part 5 — Definition of done

Per crate:

- [ ] No `mod.rs` holds logic (S2); no plural-named grab-bag files remain (S1)
- [ ] Every public item documented; intra-doc links used; `lib.rs` is the
      front page with a compiling example (S6/S7)
- [ ] Wire DTOs quarantined and `Wire*`-named — applies to
      `postgres/src/source/cdc/pgoutput.rs` ONLY (S4 scope box)
- [ ] Test hooks gated or `#[doc(hidden)]`, never bare-public (S3)
- [ ] Positive `binary(...)` selections non-empty AND their counts match the
      Wave 0 baseline; nextest GROUP MEMBERSHIP unchanged (C7, both directions)
- [ ] Skip-count identical to the Wave 0 baseline under assertive gating — a
      test that silently began skipping is the failure a green exit hides
- [ ] Commit history separates move / rename / reshape (C6)
- [ ] `env -u RUSTUP_TOOLCHAIN make check` green; golden pins byte-identical;
      crash sweeps green where the crate has them (C1/C5)
- [ ] Secret sweep clean (C8)

Wave 0, before any crate is touched:

- [ ] sqlcore owns `tests/protocol_pins.rs` — the 15 `pin_*` functions lifted
      verbatim out of the file §4.8 splits, verified liftable (the inline
      module references no private item)
- [ ] `--no-tests=pass` removed from all nine Makefile lines; the iceberg
      filter re-spelled positively; `TARGET=e2e` reachable from `check`
- [ ] Assertive gating mode in testkit + committed test/skip-count baseline
- [ ] Crash-point site-vs-registry assertion ported from engine to all five
      connectors that only assert `fired == FAIL_POINTS`
- [ ] `make semver` exists, pinned to a recorded LOCAL baseline sha (CI's
      `origin/main` is 73 commits stale and would report pre-existing breakage)
- [ ] C10 freeze lists confirmed by the owner; §3.6 decisions recorded

Workspace, at the end:

- [ ] All 14 crate READMEs re-checked against the moved reality
- [ ] `cargo semver-checks` on rdlt-core + rdlt-connector: "no update
      required" — or the owner has explicitly taken the rename list with the
      0.2→0.3 bump
- [ ] The public-rename proposal list delivered as one document
- [ ] This file deleted, its conclusions recorded in the executing feature's
      close-out (the 017/REFACTORING.md precedent)
