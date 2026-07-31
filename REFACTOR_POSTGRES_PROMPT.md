# PROMPT — refactor `crates/rdlt-connector-postgres` to the house style

You are executing a **structural refactor with zero behavior change** of
`crates/rdlt-connector-postgres` in the rdlt workspace. This is the largest
and oldest connector (~13.3k src lines, 22 test roots) and it carries THREE
kinds of frozen behavior at once: emitted SQL, wire-format encoders, and a
hand-rolled CDC protocol parser. Its nets are already external — that is an
advantage the engine and sqlcore refactors did not have.

Work on a fresh branch off `main`. Three completed refactors are the in-tree
precedent and define the house style as applied HERE (the external reference
is `../snowflake-connector-rs`, as amended by the owner — in-tree precedent
wins where they differ):

- `crates/rdlt-engine` — module splits with item census, test consolidation
  (`775a6f1e..add593f6`).
- `crates/rdlt-testkit` — single-spelling paths, naming, front-page doctest
  (`c1ef56ab..6ede0fc5`).
- `crates/rdlt-connector-sqlcore` — the closest precedent: a connector-family
  crate, tests-first safety net, visibility narrowing, owner naming pass
  (`cbb0fae3..3bb82536`, 11 commits; its deleted driver prompt's conclusions
  are recorded in the final commits and the memory log).

## 1. The house rules, as amended by the owner

1. **A deep module lives wholly in its directory behind a pure-TOC `mod.rs`**
   (mod decls + curated re-exports + `#[cfg(test)]` re-exports only). No
   `name.rs` beside a `name/` directory; no logic in any `mod.rs`.
2. **One file, one noun, small** — content-honest names; ~600-line ceiling
   without a written justification in the module doc.
3. **Names answer the caller's question**; spell out abbreviations
   (precedents: `DestinationOptions`, `CommitContext`, `quote_identifier`,
   `plan_commit`, `uses_stage`).
4. **One spelling per item** — the canonical vocabulary documented in
   `lib.rs`; every consumer rewritten in the same commit.
5. **Tests**: one `tests/integration.rs` + `cases/test_<noun>.rs`, shared
   helpers in `cases/common.rs` — EXCEPT binaries a gate selects by name
   (§5), which keep their own roots with the reason written in
   `integration.rs`'s doc.
6. **`lib.rs` is the front page** with a doc example of the primary workflow
   (running if it needs no resource — config parse/validate runs without a
   database; a connect example is `no_run`).
7. **Comments self-contained**; load-bearing invariant paragraphs move
   VERBATIM (this crate is dense with them: CDC slot lifecycle, snapshot
   visibility horizon, COPY error classes, cursor lag truth).
8. **Cargo.toml groomed** — but see §4: every existing feature here is
   consumer-wired and STAYS.

## 2. What this crate is, measured (re-measure before cutting)

| File | Lines | Verdict |
|---|---|---|
| `src/source/mod.rs` | 792 | **Code-bearing `mod.rs` — main event #1.** Also hosts `pub mod testhook` (§5 frozen) |
| `src/dest/mod.rs` | 502 | **Code-bearing `mod.rs` — main event #2.** Hosts the doc-hidden `sqlgen` re-export block (§5) |
| `src/source/config.rs` | 984 | Audit: the YAML vocabulary + its validation — split only if a real second noun hides; serialized names frozen (§5) |
| `src/source/cursor.rs` | 926 | Audit for a second noun (lag windows / watermark discipline) |
| `src/dest/commit.rs` | 821 | Likely one noun (the unit executor); justify or split |
| `src/source/copy_decode.rs` | 813 | Wire decoding — one noun, likely justified |
| `src/dest/encode.rs` | 698 | Hand-rolled COPY encoders — one noun, likely justified |
| `src/source/types.rs` | 582 | Grab-bag name candidate — audit content honesty |
| `src/source/reflect.rs` | 488 | Fine |
| `src/tls/connstring.rs` | 470 | Fine; inline tests to audit |
| `src/tls_verify.rs` | 137 | **Sits OUTSIDE `src/tls/`** — belongs in the tls module (locative fix) |
| `src/fixtures.rs` | 176 | Feature-gated test support; the two RECORDED candidates live here (§3) |
| `src/pgerror.rs` | 65 | Abbreviated name; audit location too |
| `src/source/cdc/` | ~1.9k | Already fine-grained behind a 27-line TOC `mod.rs` — likely untouched structurally |

16 src files carry inline `#[cfg(test)]` modules. Tests: 22 root files
including the three golden-SQL pin files (the SQLCORE net — §5) and four
gate-named binaries (§5).

## 3. Owner decisions and recorded candidates

- ONE merged crate with `source`/`dest` features (006 owner decision) — do
  not revisit.
- Recorded candidates to EXECUTE here: `PgFixture` conn-field vs
  `conn_url()` split; `PgFixture`/`CdcPgFixture` `start()` duplication.
- sqlcore owns `_rdlt_*` naming and the merge vocabulary; nothing moves
  between the crates in this refactor.

## 4. Features — all four stay

`source = []`, `dest = []` gate real compile surface and are wired into the
`rdlt` facade's `postgres-source`/`postgres-dest` features. `failpoints`
forwards to the SPI. `fixtures` is the recorded off-default test-support
decision, consumed by duckdb and snowflake dev-deps. The testkit "delete the
feature" precedent does NOT apply — that feature guarded zero deps; these
guard code and dependencies.

## 5. Binding constraints (override everything above)

- **C1 — zero behavior change**, three nets, all pre-existing:
  (a) `tests/{golden_sql,golden_unit_sql,golden_ensure_sql}.rs` byte-identical
  — these are ALSO sqlcore's external net; never regenerate, never re-derive;
  (b) `tests/pg_copy_wire_pin.rs` + `tests/differential.rs` (proptest vs the
  driver's own encoding as oracle) pin the wire encoders;
  (c) the conformance/incremental/cdc/scd2/tls container suites pin runtime
  behavior — run with a container runtime at every milestone.
- **Frozen spellings**: every serde-serialized name in `source/config.rs` and
  `dest/config.rs` (pipeline-YAML vocabulary incl. sslmode spellings, cursor
  fields, `type_hints` strings like `"decimal(p,s)"` — Rust identifiers may
  move, serialized names may not); SQLSTATE/classification texts asserted by
  tests; the `application_name=rdlt` default.
- **`source::testhook` is FROZEN at its path** — the out-of-workspace fuzz
  crate imports `rdlt_connector_postgres::source::testhook::{fuzz_copy_decode,
  fuzz_pgoutput_decode}` and no gate compiles it. If the source split moves
  the hooks' internals, the `testhook` module path and fn names stay; verify
  with `cd fuzz && cargo +nightly check` (or at minimum `grep`) before
  claiming done. Same precedent as the engine's frozen `fuzzing.rs`.
- **Gate-named binaries keep their names**: `crash_sweep`, `dest_crash_sweep`,
  `cdc_crash_sweep` (Makefile `TARGET=sweep` selects them `binary(...)` BY
  NAME), `memory_bound` (`TARGET=heavy`). Renaming any requires the Makefile
  edited in the same commit — and re-read the 024 lesson in CLAUDE.md before
  touching those lines: an empty selection must FAIL, no `--no-tests=pass`.
- **Crash points**: the three postgres points armed INDIRECTLY (macro takes a
  variable, literal at the constructor) are exactly what the testkit registry
  scanner was designed around — moving code that contains `crash_point!` /
  `crash_at` sites is fine, but keep the arming spellings intact and run the
  scanner's suite after any such move. Do not rename fail-point string names.
- **Test consolidation is judged, not mechanical.** The container-heavy
  suites (`dest_conformance` 2,390 lines, `cdc` 1,351, `conformance`,
  `incremental`, `tls_matrix`, `scd2`, …) each run today as separate binaries
  — nextest serializes ACROSS binaries differently than within one, and the
  recorded rootlessport flake has an intra-run concurrency mechanism that
  reclaiming cannot fix. Consolidate the container-FREE suites
  (`config_schema`, `golden_*`, `differential`, `pg_copy_wire_pin`,
  `query_streams`, `dest_recovery`, …) into `integration.rs` + `cases/`;
  for container suites either keep them as named roots with the reason
  written in `integration.rs`'s doc, or consolidate WITH a nextest
  `test-threads` bound and prove stability with three consecutive
  containerized runs before keeping it. `differential.proptest-regressions`
  must keep pairing with its test's binary/file name.
- **Commit taxonomy + census**: move-only / rename / reshape, never mixed;
  splits verified by normalized-line item census; moved blocks verbatim.
- **The traps already paid for** (each cost a gate run once):
  feature-gated test files hide from default checks — `cargo check
  --all-targets --features failpoints` for ALL SIX connector crates after any
  cross-crate edit; `cargo fmt --all` before every commit; after any
  visibility narrowing run the docs leg (`RUSTDOCFLAGS="-D warnings" cargo
  doc --workspace --no-deps --all-features`) — rustdoc refuses public docs
  linking now-private modules; `env -u RUSTUP_TOOLCHAIN` on every gate;
  never edit during a gate run, wait on a `GATE_EXIT=` marker not a PID;
  `make reclaim` + TIME_WAIT drain between gates; if disk pressure appears,
  `target/debug` under parallel agents is the culprit (hit 1.5 TB once —
  prune `debug`/`llvm-cov-target`/`mutants`, keep `release` and `iai`).
- **Semver**: `rdlt-core`/`rdlt-connector` untouched; `make semver` stays
  "no update required". This crate is greenfield — renames land directly
  with consumers (`rdlt` facade, duckdb + snowflake tests, testkit doc
  pointer in `gate.rs`) rewritten in-commit.

## 6. The work, in order (one concern per commit)

1. **Commit 0 — safety net (test-only).** Lift the PUBLIC-API portions of the
   16 inline `#[cfg(test)]` modules into the (already external) test surface
   under `cases/`; private-access tests stay inline. Snapshot
   `cargo nextest list -p rdlt-connector-postgres` (names per binary) and run
   the three golden suites + wire pins green. Commit.
2. **`src/source/mod.rs` → pure TOC** — candidate nouns from its own content
   (re-read before cutting); `testhook` keeps its path (§5). Census.
3. **`src/dest/mod.rs` → pure TOC** — the doc-hidden `sqlgen` block moves
   intact (its consumers are external pins). Census.
4. **`tls_verify.rs` → `src/tls/`**, `pgerror.rs` location/name audit,
   `source/types.rs` honesty audit — move-only commits.
5. **Test consolidation** per the §5 judgment, gate-named roots untouched.
6. **Single-spelling pass** across the facade + duckdb/snowflake consumers;
   document the canonical vocabulary in `lib.rs`.
7. **`lib.rs` front page** — running doctest for what runs without a
   database (config `from_value` parse + validation); `no_run` for connect.
8. **Fixtures pass** — the two recorded candidates (§3), zero new dep edges.
9. **Cleanup + naming pass** — the four-angle review procedure (reuse /
   simplification / efficiency / altitude + naming lens); efficiency findings
   measure-first (two allocation removals in this workspace measured WORSE).

## 7. Definition of done

1. Commit 0's baseline test-name set stable modulo the consolidation map
   recorded in the closing note; golden pins + wire pins byte-identical
   throughout; conformance/cdc suites green WITH containers.
2. No code-bearing `mod.rs`; no file above ~600 lines without written
   justification; every split census-verified; `testhook` path proven intact
   against the fuzz crate.
3. One spelling per public item documented in `lib.rs`; facade + all
   consumers compile; all six connectors check under `--features failpoints`.
4. `lib.rs` opens with the primary-workflow doc example.
5. Cleanup/naming applied with the skip-list recorded.
6. `env -u RUSTUP_TOOLCHAIN make check` green (964/964-scale, semver clean,
   benches unregressed, cold start under the bar) — with a container runtime
   present so the postgres legs actually run; fmt/clippy/docs-leg clean.
7. A closing note: renames proposed-not-applied, splits declined with
   reasons, the test-consolidation map (old binary → new home), and this
   file deleted with its conclusions recorded.
