# rdlt-engine House-Style Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reshape `crates/rdlt-engine` so its layout, naming, imports, comments, docs, and testing patterns read as if written by the author of `../snowflake-connector-rs` — with zero behavior change and the full gate green.

**Architecture:** Pure style refactor. The engine's runtime behavior, persisted formats (WAL v2 bytes, shred identity pins), crash-point registry, and observable API semantics are invariant. Public *paths* are preserved via `pub use`; the one deliberate API-shape change is `EngineConfig` moving from public fields to the reference's `with_*` builder idiom (ripples into 7 consumer crates, all updated in the same task). Work proceeds in ordered tasks, each independently green and committed; move-only commits are separated from edit commits so `git diff --find-renames` stays reviewable.

**Tech Stack:** Rust 1.96.0 (pinned; run gates with `env -u RUSTUP_TOOLCHAIN`), cargo-nextest, workspace lints, `make check` gate.

## Global Constraints

Copied from measured facts; every task implicitly includes these.

- **G1 — Zero behavior change.** No logic edits. Golden pins (`shred_identity_pin`, WAL exact-match refusal tests) must pass byte-identical; their assertion bodies are never edited in the same commit as src changes (imports/renames in pin files are allowed, assertions are not).
- **G2 — Frozen names (gate-coupled, verified 2026-07-30):**
  - Test binary names `crash_sweep` and `shred_property` (Makefile:112 `-E 'binary(crash_sweep)'`, Makefile:120-123 `-E 'binary(shred_property)'`; no `--no-tests=pass` anywhere — an emptied selector FAILS, which is our safety net, not our enemy).
  - Bench target names `shred`, `iai_hotpath` (Makefile:232,234) and iai bench fn names `shred_nested_10k`, `passthrough_10k`, `identity_keyed_10k`, `identity_keyless_10k` (keys in `benches/perf-baselines.json`, matched by `benches/compare-iai.sh`). Never re-record baselines to clear a refusal.
  - `src/fuzzing.rs` path (`.cargo/mutants.toml:19` excludes it by path) and its five `pub fn` names `parse_slab`, `shred_slab`, `map_arrow_type`, `bench_shred_bytes`, `bench_passthrough` (consumed by the out-of-workspace `fuzz/` targets, `benches/iai_hotpath.rs`, `examples/shred_only.rs`, and pinned by `tests/entry_points.rs`).
  - Crate directory name `crates/rdlt-engine` (rdlt-testkit `tests/scanner_selfcheck.rs:23` reaches it by directory-name string; `fuzz/Cargo.toml:16` by relative path).
  - The 7 crash-point string literals and the `const ENGINE_POINTS: &[&str] = &[` … `];` declaration **shape** (rdlt-testkit `crash.rs` recognises declarations by the literal token shape `": &[&str] = &["`; only `crash_point!(` / `crash_at(` arming spellings are recognised — introduce no third spelling). Sites may move between files **under `src/`** (scan root is `CARGO_MANIFEST_DIR/src`); count is pinned at 7 by `scanner_selfcheck.rs`.
  - `EventStream` type name (leaks into the facade's public API at `crates/rdlt/src/builder.rs:207`).
- **G3 — Public path stability:** `rdlt_engine::{Engine, EngineConfig, EventStream, RdltError, RunReport}` keep resolving at the crate root (via `pub use` after moves). `rdlt_engine::fuzzing::*` keeps resolving.
- **G4 — Consumers updated in the same commit as any API-shape change.** The full external surface is 6 paths across: `crates/rdlt/src/builder.rs` (production), 46 test files across rdlt-connector-{postgres,file,iceberg,duckdb,snowflake,rest}, `fuzz/fuzz_targets/*`. `cargo check --workspace --all-targets` must pass at every commit.
- **G5 — Gate discipline (learned three times in 024):** make all edits, then run one untouched gate; never edit during a `make check` run; wait on a completion marker in the log, not on a `pgrep` PID. Final acceptance: `env -u RUSTUP_TOOLCHAIN make check` TWICE clean, test+skip counts equal to the recorded baseline.
- **G6 — Comment content is preserved, voice is translated.** Every WHY (e.g. "retrying a single stream in place would double-publish") survives; only the prose style changes. Comments become self-contained: spec/feature citation IDs (`feature 003 US1`, `gate G2.2`, `embedder-api.md O1`) are replaced by the constraint stated in full, except regeneration warnings on golden pins, which stay verbatim.

---

## The Style Rulebook (extracted from snowflake-connector-rs, 2026-07-30)

Each rule cites the reference file it was extracted from. This section is the spec each task applies.

**S1 — Module layout.** A module whose root carries code is `name.rs` + `name/` dir (`error.rs` + `error/`, `statement/put.rs` + `put/`, `result_cursor/remote.rs` + `remote/`). A module whose root is only declarations + re-exports is `dir/mod.rs` (`statement/mod.rs`, `result_cursor/mod.rs`). Manifest files contain, in order: `mod` list (alphabetical, `pub(crate) mod` where needed), blank line, `pub use` list, `pub(crate) use` list, feature-gated items last. Nothing else.

**S2 — lib.rs shape** (from reference `lib.rs`): crate doc (`//!`) with a short pitch and a *runnable* `rust,no_run` example, then optional feature-area sections; then private `mod` list; `#[cfg(test)] mod test_support;` if present; then grouped `pub use` blocks (one per module, multi-line braces sorted); then feature-gated `pub use`; then `pub(crate) use` re-exports at the bottom. lib.rs holds **no** type definitions.

**S3 — Imports.** Three blocks separated by blank lines: `std` (one nested `use std::{...}`), external crates (one `use` per crate, nested braces, alphabetical), then `crate`/`super` (nested braces). Example from `session.rs`:

```rust
use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use uuid::Uuid;

use crate::{
    ClientShared, IntoStatement, Result,
    result_cursor::{ResultCursor, TypedResultCursor},
    statement::{QueryControl, QueryHandle, StatementExecutor, builder::into_statement_parts},
};
```

**S4 — Naming.** No abbreviations in type/fn names: `Context` not `Ctx`, `Object` not `Obj`, `Array` not `Arr`, `Column` not `Col`. Domain acronyms stay (`Wal` like the reference's `Api`). Constructors for tests are `for_test` under `#[cfg(test)]`. Local module `type XResult<T> = Result<T, XError>;` aliases where one error type dominates a file (reference `rowset/parser.rs`). Short closure locals (`e`, `err`) are fine; API-level names are spelled out.

**S5 — Config types** (from `config.rs`, `session.rs` `QueryOptions`): fields private or `pub(crate)`, never `pub`. Construction = `new(...)` with required args + consuming `with_*(mut self, ...) -> Self` for options. Defaults via `Default` or named `DEFAULT_*` consts (`pub(crate) const DEFAULT_QUERY_RESPONSE_TIMEOUT`). Read access for other crates via accessor methods. Secrets/tokens get manual `Debug` with `"<redacted>"` and a comment saying why.

**S6 — Doc comments.** Complete sentences, wrapped long (~110-120 cols). Public items link aggressively: ``[`Session::query`]``, ``[`QueryConfig`](crate::QueryConfig)``. Public fallible fns carry `# Errors` sections naming the error categories. Doc paragraphs explain *when to use this vs. the alternative* (see `Session::query_handle`). Module docs (`//!`) are 1 short title sentence + 1-2 paragraphs of mechanism.

**S7 — Inline comments.** Calm, complete sentences on their own line above the code, explaining constraints the code can't show: "The session token authenticates every request; never print it." Rare, load-bearing, no headers/banners, no `— foo — bar` telegraphic chains, no ALL-CAPS emphasis (reference uses none; the engine uses it heavily — translate to plain emphasis by sentence structure).

**S8 — Error style.** Engine keeps `RdltError` from `rdlt-core` (workspace taxonomy — NOT movable). What transfers: local `pub(crate)` error enums get constructor helper fns (`ProtocolError::json_parse(...)`), `Box<str>` for stored messages, `debug_assert!` guards on classification invariants, and a `const _: fn() = || { assert_send_sync::<T>(); }` block where auto-trait guarantees matter.

**S9 — Unit tests** live in `#[cfg(test)] mod tests` at the bottom of the src file (reference `error.rs` carries a 600-line one), names are un-prefixed sentences: `fn error_accessors_expose_structured_details()`. Assertion messages state expectation and observed value: `"expected SEQ {expected} but got {seq} (duplicate or gap)"`.

**S10 — Integration tests** (from `tests/`): one `integration.rs` declaring `mod cases;`, `tests/cases/mod.rs` listing `common` + `test_*` modules, `tests/cases/common.rs` holding shared builders (`default_session`-style `OnceCell` sharing, `unique_temp_table_name`-style uniqueness helpers). Case files are `test_<topic>.rs`; test fns are `#[tokio::test] async fn test_<behavior>() -> Result<()>` where a `Result` return helps, plain `()` otherwise. Special suites that need their own binary (reference: `derive_trybuild.rs`, `auto_trait_trybuild.rs`) stay separate root files.

**S11 — Cargo.toml** (taplo-style): every list multi-line, one item per line, trailing comma, alphabetized; keys within tables alphabetized where cargo semantics allow; features alphabetized with `dep:` spelled out; comments only where a feature needs explanation ("# Uploading local files to a stage with `PUT`.").

**S12 — Constants.** Named `SCREAMING_CASE` at module top with a doc comment explaining the choice of value, near their first user (`VALUE_PREVIEW_MAX_CHARS` in `error.rs`, `DEFAULT_COLLECT_PREFETCH_CONCURRENCY` in `config.rs`).

---

## File Structure (current → target)

```
src/lib.rs (151, holds Engine/EngineConfig/EventStream)  → doc + manifest only (S2)
src/config.rs (new)                                       ← EngineConfig + defaults
src/engine.rs (new)                                       ← Engine, EventStream, EVENT_CHANNEL_CAPACITY
src/fuzzing.rs                                            = path/fn names frozen; internals restyled
src/load/mod.rs (532, code-bearing)                       → src/load.rs + load/{apply.rs, lowering.rs}
src/runtime/mod.rs (manifest — already S1-conformant)     = stays; run.rs, lock.rs stay
src/schema/mod.rs (manifest — already S1-conformant)      = stays; contracts.rs, registry.rs stay
src/shred/mod.rs (425, code-bearing)                      → src/shred.rs + shred/{arena,build,canon,infer,passthrough,table,tape,view}.rs
src/wal/mod.rs (441, code-bearing)                        → src/wal.rs + wal/resume.rs

tests/us1_full_sync.rs        → tests/cases/test_full_sync.rs
tests/us2_incremental.rs      → tests/cases/test_incremental.rs
tests/us3_crash_matrix.rs     → tests/cases/test_crash_matrix.rs   (uses testkit faults, no feature gate — verified)
tests/us4_policies.rs         → tests/cases/test_policies.rs
tests/us5_observability.rs    → tests/cases/test_observability.rs
tests/value_fidelity.rs       → tests/cases/test_value_fidelity.rs
tests/passthrough.rs          → tests/cases/test_passthrough.rs
tests/shred_roundtrip.rs      → tests/cases/test_shred_roundtrip.rs
tests/wal_lifecycle.rs        → tests/cases/test_wal_lifecycle.rs
tests/entry_points.rs         → tests/cases/test_entry_points.rs
tests/misbehaving_source.rs   → tests/cases/test_misbehaving_source.rs
tests/mutation_closures.rs    → tests/cases/test_mutation_closures.rs
tests/shred_identity_pin.rs   → tests/cases/test_shred_identity_pin.rs (assertions untouched, G1)
tests/integration.rs (new)    ← `mod cases;`
tests/cases/{mod.rs,common.rs} (new)
tests/crash_sweep.rs          = binary name frozen (G2); internals restyled
tests/shred_property.rs       = binary name frozen (G2); internals restyled; proptest-regressions/ path unchanged
benches/, examples/           = names frozen; internals restyled
```

---

### Task 1: Baseline and Cargo.toml

**Files:** Modify `crates/rdlt-engine/Cargo.toml`. Create `docs/superpowers/plans/2026-07-30-baseline-counts.md` (scratch record, not committed).

- [ ] **Step 1: Record the baseline.** Run and save the pass/skip counts (these are the acceptance numbers for every later task):

```bash
cd /var/home/netf/Repos/rapidbyte/rdlt
env -u RUSTUP_TOOLCHAIN cargo nextest run -p rdlt-engine 2>&1 | tail -3
env -u RUSTUP_TOOLCHAIN cargo nextest run -p rdlt-engine --features failpoints -E 'binary(crash_sweep)' 2>&1 | tail -3
env -u RUSTUP_TOOLCHAIN sh -c "PROPTEST_CASES=16 cargo nextest run -p rdlt-engine -E 'binary(shred_property)'" 2>&1 | tail -3
env -u RUSTUP_TOOLCHAIN cargo test --doc -p rdlt-engine 2>&1 | tail -3
```

- [ ] **Step 2: Restyle Cargo.toml per S11** — alphabetize `[dependencies]` (rdlt-core, rdlt-connector first is NOT the reference style; strict alphabetical: arrow, bytes, chrono, fs4, rdlt-connector, rdlt-core, serde, serde_json, thiserror, tokio, tokio-util, tracing), alphabetize `[dev-dependencies]`, multi-line the `keywords`/`categories` arrays one item per line, keep workspace inheritance untouched. Do not reorder `[[bench]]` names (G2).
- [ ] **Step 3: Verify:** `cargo check -p rdlt-engine --all-targets` passes.
- [ ] **Step 4: Commit** `style(engine): Cargo.toml manifest formatting`.

### Task 2: lib.rs becomes a manifest; config.rs and engine.rs are born

**Files:** Create `src/config.rs`, `src/engine.rs`. Modify `src/lib.rs`.

**Interfaces produced:** `crate::config::EngineConfig` (re-exported at root), `crate::engine::{Engine, EventStream}` (re-exported at root). Later tasks import `crate::EngineConfig` unchanged.

- [ ] **Step 1: Move** `EngineConfig` (+ `mode_for`) verbatim into `src/config.rs`; move `Engine`, `EventStream`, `EVENT_CHANNEL_CAPACITY`, both `Debug` impls verbatim into `src/engine.rs`. This is a move-only commit: no wording or naming edits yet.
- [ ] **Step 2: Rewrite `src/lib.rs`** to the S2 shape:

```rust
//! # rdlt-engine
//!
//! The rdlt ingestion engine: shredding, schema registry, write-ahead log, and load
//! orchestration over byte-bounded channels.
//!
//! ```rust,no_run
//! # use rdlt_engine::{Engine, EngineConfig};
//! # use rdlt_testkit::{MemoryDestination, MemorySource};
//! # async fn run(source: MemorySource, destination: MemoryDestination) -> Result<(), rdlt_engine::RdltError> {
//! let config = EngineConfig::new("example-pipeline");
//! let report = Engine::new(config, source, destination).run().await?;
//! assert_eq!(report.total_rows(), report.total_rows());
//! # Ok(())
//! # }
//! ```

mod config;
mod engine;
mod load;
mod runtime;
mod schema;
mod shred;
mod wal;

#[doc(hidden)]
pub mod fuzzing;

pub use config::EngineConfig;
pub use engine::{Engine, EventStream};
pub use rdlt_core::{PipelineEvent, RdltError, RunReport};
```

(Adjust the doc example to whatever actually compiles against testkit's constructors — verify with `cargo test --doc -p rdlt-engine`.)
- [ ] **Step 3: Verify:** `cargo nextest run -p rdlt-engine` count equals baseline; `cargo check --workspace --all-targets` (external `rdlt_engine::Engine` paths unchanged).
- [ ] **Step 4: Commit** `style(engine): lib.rs is a manifest; Engine and EngineConfig move to their own files`.

### Task 3: EngineConfig adopts the with_* builder idiom (ripple task)

**Files:** Modify `src/config.rs`; `crates/rdlt/src/builder.rs` (11 sites); every external test site assigning `config.<field> =` across rdlt-connector-{postgres,file,iceberg,duckdb,snowflake,rest} and `crates/rdlt-engine/tests/*` (find them all with `grep -rn 'config\.\(write_mode\|write_modes\|schema_policy\|commit_policy\|workdir\|byte_budget\)\s*=' crates/`).

**Interfaces produced (exact signatures later tasks and consumers rely on):**

```rust
impl EngineConfig {
    pub fn new(pipeline: impl Into<PipelineId>) -> Self;
    pub fn with_write_mode(mut self, mode: WriteMode) -> Self;
    pub fn with_write_mode_for(mut self, stream: impl Into<StreamName>, mode: WriteMode) -> Self;
    pub fn with_schema_policy(mut self, policy: SchemaPolicy) -> Self;
    pub fn with_commit_policy(mut self, policy: CommitPolicy) -> Self;
    pub fn with_workdir(mut self, workdir: impl Into<PathBuf>) -> Self;
    pub fn with_byte_budget(mut self, bytes: usize) -> Self;
    // Read accessors for the facade (crates/rdlt/src/builder.rs:107,134,143 reads):
    pub fn write_mode(&self) -> &WriteMode;
    pub fn write_modes(&self) -> &BTreeMap<StreamName, WriteMode>;
}
```

- [ ] **Step 1:** Fields go `pub(crate)` (engine internals in `runtime/run.rs` etc. keep direct reads — the reference's `QueryOptions` pattern). Add the builders + accessors above, with S6 doc comments; hoist `64 << 20` into `pub(crate) const DEFAULT_BYTE_BUDGET: usize` with a doc comment (S12).
- [ ] **Step 2:** Rewrite consumers mechanically. Facade (methods take `mut self`, so field-move-and-reassign compiles): `self.config.write_mode = mode;` → `self.config = self.config.with_write_mode(mode);` and reads → accessors. Tests: `let mut config = EngineConfig::new("incr"); config.commit_policy = X;` → `let config = EngineConfig::new("incr").with_commit_policy(X);`.
- [ ] **Step 3: Verify:** `cargo check --workspace --all-targets` clean; `cargo nextest run -p rdlt-engine` = baseline; `cargo nextest run -p rdlt` (facade unit tests).
- [ ] **Step 4: Commit** `style(engine): EngineConfig builds with with_* methods; fields are crate-private`.

### Task 4: Module roots follow S1 (shred.rs / load.rs / wal.rs)

**Files:** `git mv src/shred/mod.rs src/shred.rs`, `git mv src/load/mod.rs src/load.rs`, `git mv src/wal/mod.rs src/wal.rs`; fix `mod` path attributes are NOT needed (Rust resolves `name.rs` + `name/` natively).

- [ ] **Step 1:** Move the three files; compile; fix nothing else (imports inside are unchanged — submodule resolution is identical for both spellings).
- [ ] **Step 2: Verify the scanner still sees 7 armed sites:** `cargo nextest run -p rdlt-engine --features failpoints -E 'binary(crash_sweep)'` (runs `assert_registry_is_armed` over `src/`) AND `cargo nextest run -p rdlt-testkit -E 'binary(scanner_selfcheck)'`.
- [ ] **Step 3: Verify counts** = baseline; **Commit** `style(engine): code-bearing module roots move from mod.rs to name.rs (move-only)`.

### Task 5: Naming pass (S4)

**Files:** All of `src/`; `src/fuzzing.rs` call sites (its five `pub fn` names DO NOT change, G2).

Renames (all `pub(crate)` or private — zero external ripple, verified against the consumer map):

| Current | Target | Home |
|---|---|---|
| `ShredCtx` | `ShredContext` | shred.rs |
| `ColState` | `ColumnState` | shred/infer.rs |
| `ObjIter` / `ArrIter` / `ValueObjIter` | `ObjectIter` / `ArrayIter` / `ValueObjectIter` | shred/view.rs, arena.rs |
| `DrainRow::get_top` | `DrainRow::top_level` | shred.rs |
| `EngineConfig::mode_for` | `EngineConfig::write_mode_for` *(pub(crate))* | config.rs |

- [ ] **Step 1:** Apply via LSP rename or sed + compile loop. Sweep for further `Ctx|Obj|Arr|Col|Cfg`-style abbreviations introduced by the same author habit (`grep -n 'Ctx\|ObjI\|ArrI' src/ -r`) and rename by the same rule. Do NOT touch: `ENGINE_POINTS` literals, crash-point strings, fuzzing pub fns, bench fn names, `rx`/`tx` channel locals (reference uses them too).
- [ ] **Step 2: Verify** counts = baseline (`cargo nextest run -p rdlt-engine`, plus the sweep leg since crash_sweep exercises internals); `cargo check --workspace --all-targets`.
- [ ] **Step 3: Commit** `style(engine): spell out abbreviated type and method names`.

### Task 6: Import regrouping (S3)

**Files:** every `src/*.rs`, `src/**/*.rs`.

- [ ] **Step 1:** Rewrite each file's use-block into the three-block S3 shape (std nested / one-per-external-crate nested / crate+super nested). Fold stragglers (e.g. `runtime/run.rs` has a `use rdlt_connector::channel::...` stranded below the crate imports) into their proper block. `cargo fmt` after.
- [ ] **Step 2: Verify:** `cargo check -p rdlt-engine --all-targets && cargo nextest run -p rdlt-engine` = baseline. **Commit** `style(engine): imports grouped std / external / crate per house style`.

### Task 7: Comment and doc voice (S6, S7, S9, G6) — three commits

**Files:** (a) `shred.rs` + `shred/*`, `schema/*`; (b) `wal.rs` + `wal/resume.rs`, `load.rs` + `load/*`; (c) `runtime/*`, `engine.rs`, `config.rs`, `lib.rs`, `fuzzing.rs`.

Worked example of the translation (from `runtime/run.rs:48-51`), preserving every fact:

```rust
// BEFORE
// A wall clock before the Unix epoch yields no usable millis; fall back to 0.
// The load id only needs to be UNIQUE within a pipeline, not monotonic, and
// the process-id + atomic sequence below already guarantee that — the millis
// are a human-readable prefix, not the uniqueness source.

// AFTER
// Uniqueness comes from the process id and the atomic sequence; the millisecond
// timestamp is only a human-readable prefix. A wall clock before the Unix epoch
// therefore falls back to 0 rather than failing.
```

- [ ] **Step 1 (each commit):** For each file: rewrite `//!` docs to S6 module-doc shape; rewrite `///` docs into linked prose with `# Errors` on public fallible items; translate inline comments per S7 (no ALL-CAPS, no telegraphic em-dash chains, no spec citation IDs — state the constraint itself; golden-pin regeneration warnings stay verbatim); rename unit-test fns to S9 sentence style where they aren't already.
- [ ] **Step 2 (each commit): Verify** counts = baseline; `cargo test --doc -p rdlt-engine` (new doc links compile). **Commit** `style(engine): house comment voice — <area>`.

### Task 8: Test layout follows S10

**Files:** Create `tests/integration.rs`, `tests/cases/mod.rs`, `tests/cases/common.rs`; `git mv` the 13 renameable suites per the File Structure table; modify `tests/crash_sweep.rs` + `tests/shred_property.rs` internals only.

- [ ] **Step 1:** `tests/integration.rs` = `mod cases;`. `cases/mod.rs` lists `common` + the 13 `test_*` modules. Extract into `common.rs` only helpers duplicated across ≥2 files (the `stream_with_batches` / seeded-batch builders; follow the reference's `common.rs` shape — plain `pub fn` builders, doc comments, a uniqueness helper if any test needs unique table names). Case files get `use super::common;`.
- [ ] **Step 2:** Rename test fns in `cases/` to `test_`-prefixed reference style (e.g. `second_run_resumes_from_committed_cursor` → `test_second_run_resumes_from_committed_cursor`); restyle comments per S7. `shred_identity_pin` assertions and fixture bytes untouched (G1). `crash_sweep.rs`/`shred_property.rs`: keep binary names and any fn name referenced in Makefile comments; restyle prose only.
- [ ] **Step 3: Verify:** total test count across `cargo nextest run -p rdlt-engine` + sweep leg + prop leg equals baseline totals exactly (nextest runs each test in its own process; consolidation changes binary count, not test count — 024's reachability evidence docs in `specs/024-*/` describe that feature's moment and are deliberately left as history). Check `proptest-regressions/` still matches (`shred_property` untouched by moves).
- [ ] **Step 4: Commit** `style(engine): integration tests consolidate under tests/cases with shared common builders`.

### Task 9: Benches, examples, fuzzing internals, README

**Files:** `benches/shred.rs`, `benches/iai_hotpath.rs`, `examples/shred_only.rs`, `src/fuzzing.rs`, `README.md`.

- [ ] **Step 1:** Restyle comments/imports per S3/S7 with names frozen per G2 (bench fns, example name, fuzzing pub fns). README: align voice with the reference README (short pitch, runnable example, feature table) without inventing content.
- [ ] **Step 2: Verify:** `cargo check -p rdlt-engine --all-targets`; `make bench TARGET=iai` refuses nothing (bench names unchanged ⇒ baselines all found). **Commit** `style(engine): benches, examples, fuzz shims, README`.

### Task 10: The gate, twice

- [ ] **Step 1:** `make reclaim` + TIME_WAIT drain (container-port flake ritual), then `env -u RUSTUP_TOOLCHAIN make check 2>&1 | tee /tmp/gate1.log` — run to a completion marker in the log; NO edits while it runs (G5).
- [ ] **Step 2:** Compare test+skip counts to baseline; on any failure, fix, commit, restart the gate from scratch (never trust a mixed run).
- [ ] **Step 3:** Second clean run (`gate2.log`). **Commit** any final fixes; record both counts in the final report.

---

## Self-Review (performed at write time)

- **Spec coverage:** layout (T2,T4,T8), file names (T4,T8), naming conventions (T5), vars/objects/methods (T5), comments (T7), testing patterns (T8), imports (T6), manifests (T1), benches/examples/README (T9), "same person" acceptance (rulebook S1-S12 + gate T10). No gaps found.
- **Placeholder scan:** no TBDs; every step names exact files, commands, and expected outcomes. Rulebook exemplars are verbatim from the reference.
- **Type consistency:** `with_*` signatures in T3 match the facade rewrite shown; renames in T5 are referenced consistently in T7/T8 (post-rename names).
- **Known deliberate exclusions:** `RdltError` stays in rdlt-core (S8); `EventStream` name frozen (facade leak); evidence docs under `specs/024-*` left stale as history.
