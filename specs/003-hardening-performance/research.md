# Research: Hardening & Performance (feature 003)

Numbering continues from feature 002 (R13–R19).

## R20 — Crash-point injection via the `fail` crate, in-process sweep

- **Decision**: Instrument every filesystem/protocol boundary WE own with
  `fail::fail_point!()` behind a `failpoints` cargo feature (no-op when off):
  WAL segment write, WAL fsync, manifest append, manifest fsync; parquet dest
  truncate, staged-file sync, rename, table-dir fsync, state write, receipt
  write; the engine's session call sites (post-ensure, post-write, post-commit).
  The sweep test enumerates registered points, and for each point N runs the
  pipeline with "return error at N" (and a second pass "panic at N"), drops the
  engine, restarts with the same workdir/destination, and asserts exactly-once
  totals. A third pass composes two points to cover crash-during-recovery.
- **Rationale**: `fail` is the battle-tested TiKV approach; in-process abort +
  fresh engine run reproduces process death for everything except torn writes
  (which fsync ordering + rename atomicity already guard); enumerating points
  from a registry means a NEW boundary added in future code automatically joins
  the sweep (the registry assertion fails if an instrumented crate registers a
  point the sweep doesn't know).
- **Alternatives**: subprocess + SIGKILL (true death, but slow, flaky in CI, and
  cannot target exact boundaries deterministically); filesystem shim trait
  (invasive refactor of std::fs usage for test-only value); LD_PRELOAD/strace
  tricks (non-portable). Rejected.
- **Scope guard**: DuckDB/Postgres internal transaction boundaries are the DB's
  own atomicity domain — each transaction is one step; sweep covers our side of
  the protocol (D1–D6 obligations), the conformance suite covers theirs.

## R21 — Mutation testing with cargo-mutants

- **Decision**: `cargo-mutants` over `rdlt-engine`, `rdlt-core`,
  `rdlt-connector`; `mutants.toml` excludes benches, examples, testkit render
  helpers, and display impls; timeout multiplier 3×; threshold **85% of viable
  mutants killed**; every survivor gets a disposition in
  `specs/003-hardening-performance/mutation-report.md` (new test | dead code
  removed | waiver-with-reason). Scheduled CI (weekly + manual dispatch), not
  per-PR.
- **Rationale**: the feature-002 review proved green tests ≠ constrained
  behavior; mutation testing measures the constraint directly. 85% is the
  practical sweet spot reported by rustc/tikv-adjacent projects; 100% forces
  waiver-noise.
- **Alternatives**: `mutagen` (unmaintained), coverage-only (`cargo-llvm-cov`
  measures execution, not assertion strength — we still add it as a report, not
  a gate).

## R22 — Fuzzing with cargo-fuzz

- **Decision**: `fuzz/` cargo-fuzz workspace (nightly-only, excluded from the
  main workspace) with five targets: (1) `jsonl_slab` — slab reader + validation
  over arbitrary bytes; (2) `cursor_decode` — `FileCursor::decode` +
  re-encode roundtrip; (3) `file_config` — YAML config parse; (4)
  `arrow_schema_map` — `column_type_from_arrow` over `arbitrary`-derived
  DataTypes + passthrough schema mapping; (5) `shred_push` — full
  `push_bytes`/drain over arbitrary bytes asserting no panic and invariant
  spot-checks. ASAN on; hangs count (libFuzzer `-timeout`); corpus + minimized
  crashers committed under `fuzz/corpus/`; every finding becomes a permanent
  unit test in the owning crate. Scheduled CI nightly 1h/target until the
  24-CPU-hour SC-003 budget is met, then weekly.
- **Rationale**: these are exactly the surfaces that consume bytes rdlt did not
  produce; libFuzzer coverage-guided beats hand-written adversarial cases.
- **Alternatives**: AFL++ (heavier orchestration), proptest-only (no coverage
  guidance). `arbitrary` used where structured input helps (DataTypes).

## R23 — Shredder property test (end-to-end invariants)

- **Decision**: `proptest` strategy generating bounded arbitrary JSON documents
  (depth ≤ 5, ≤ 64 keys, all scalar types incl. 2^53-boundary ints, floats,
  unicode keys that collide after normalization, lists of objects/scalars/mixed,
  nulls); drive the full shred → build path; assert: **row conservation** (input
  row count = root rows; every child list item lands exactly once), **lineage
  integrity** (every `_rdlt_parent_id` resolves to a real parent row, every
  `_rdlt_root_id` to the originating root), **schema monotonicity** (re-shredding
  the same batch after any prefix of batches only ever widens), and **naming
  safety** (no two distinct source keys share a destination column). 256 cases
  per run in nextest; 4096-case extended run in the scheduled job.
- **Rationale**: the review-found `UniqueNamer` aliasing bug was exactly a
  property violation no example test expressed; the strategy above would have
  generated it (a source key literally named `_rdlt_id`).

## R24 — Streaming no-`Value` shred path

- **Decision**: New `shred/stream.rs` implementing the seam left in
  `shred/build.rs`: parse each slab ONCE into a compact borrowed **tape**
  (token stream with byte-range slices into the slab — no per-row
  `serde_json::Value` tree), run shape observation (`ColState`) over the tape,
  resolve schemas exactly as today, then build Arrow arrays directly from tape
  slices. Canonicalization for `_rdlt_id` renders from the tape through the SAME
  `canonical_json_bytes` rules (shared function, one implementation). Rollout is
  gated: `shred_equivalence.rs` propends old-vs-new on arbitrary documents and
  asserts byte-identical batches, identities, and schema sequences; the old path
  stays compiled (test-only reference) until one full feature cycle later.
  Duplicate-key tie-break (last wins, matching serde_json), lone surrogates, and
  number-boundary behavior are pinned by explicit cases copied from the old
  path's observable behavior.
- **Rationale**: the per-row `Value` tree is the dominant allocator in the shred
  profile and the ≥20× target's main risk; a borrowed tape keeps the two-pass
  need (types before build) without materializing trees.
- **Alternatives**: `simd-json` (owned DOM — same materialization problem; its
  tape API is unsafe-heavy and pins arrow off our versions); serde
  `DeserializeSeed` straight into builders (single-pass — but type inference
  needs the batch's shapes before builders can be typed; would force
  re-parsing); rejected for v1 of this path.

## R25 — Row-id hash decision (clarified: measure AND switch past threshold)

- **Decision**: Bench `blake3` (incumbent) vs `xxh3-128` (`xxhash-rust`) inside
  the shred microbench (keyed and keyless identity paths) AND on the flagship
  200k e2e row. Switch iff xxh3-128 wins the **flagship e2e by >30%** (clarified 2026-07-20;
  raised from the 10% draft) — microbench-only wins do not qualify. Either way the decision, numbers, and
  rationale land in the design doc §5.4 before any release tag. If switched:
  `RowIdBuilder` swaps internally (API unchanged), WAL/cursor formats unaffected
  (ids are opaque 128-bit values already), and a one-line migration note states
  that pre-switch dev-machine state must be reset.
- **Rationale**: identity hashing is per-row hot-path work; xxh3-128 is
  ~5–10× faster per small input than blake3. 128-bit collision risk at any
  realistic row count (≪ 2^40 rows) is ~2^-48 — acceptable for dedup identity;
  blake3's cryptographic strength buys nothing here (ids are not a security
  boundary). But below the 30% e2e bar, incumbent stability wins.

## R26 — Perf-regression gate with iai-callgrind

- **Decision**: `benches/iai_hotpath.rs` (iai-callgrind) measuring instruction
  counts for: shred 10k nested rows, passthrough 10k-row batch, keyed + keyless
  identity for 10k rows. Baselines recorded in `benches/perf-baselines.json`
  (updated deliberately, in-diff, like a lockfile). CI job (valgrind on
  ubuntu-latest) fails if any count regresses **>3%** vs the recorded baseline;
  improving runs update the file in the same PR. Blocking, same semantics as the
  semver gate.
- **Rationale**: instruction counts are deterministic on shared runners (SC-007
  demands a gate that doesn't flap); wall-time gates flap. 3% covers allocator
  noise while catching any real regression (the review-class mistakes cost
  10–100%).
- **Alternatives**: criterion + threshold (flaky in CI), bencher.dev (external
  service), codspeed (service). Self-contained wins for a library repo.

## R27 — Flagship RSS closure (≤1/5th target)

- **Decision**: Set DuckDB `memory_limit` from the destination config (default
  `256MB` for the bench profile; configurable), and cap appender chunk size.
  Measure RSS on the established 200k harness; if still >397 MB (1/5 of the
  1,985 MB baseline), profile allocations (`dhat`) before touching engine
  buffers — the 64 MB channel budget is NOT the suspect (642 MB total, DuckDB
  dominant).
- **Rationale**: feature-001 caveat already predicted `memory_limit` closes the
  gap; cheapest lever first, measured.

## R28 — Cold-start cell (≤1/20th)

- **Decision**: One-row pipeline both sides; rdlt timed by the run report's
  `elapsed_ms` on a release binary via `hyperfine` (10 runs, median); dlt timed
  in-process (same convention as every existing baseline row: interpreter
  startup excluded, `dlt.pipeline(...)` + `run()` included). Record both medians
  and the multiple.
- **Rationale**: the honest comparison is engine overhead vs engine overhead;
  excluding CPython startup is GENEROUS to the baseline — state that caveat in
  RESULTS.md like the others.

## R29 — REST→Postgres cell (≥5×)

- **Decision**: Reuse the feature-001 wiremock harness as a standalone mock API
  binary serving 100k records over 100 pages with cursor pagination; baseline =
  pinned dlt `rest_api` source → postgres destination (container postgres,
  same instance for both sides, sequential runs); rdlt = bundled REST source →
  postgres via the CLI. Baseline measured first; both self-timed in-process.
- **Rationale**: matches the design-doc cell definition and the existing
  baseline-first methodology; a shared postgres instance removes container
  variance from the comparison.

## R30 — Release-profile tuning (thin-LTO)

- **Decision**: Try `lto = "thin"` + `codegen-units = 4` on the release profile;
  keep iff the flagship e2e improves ≥2% with build time increase <2×. Measured
  before/after in the PR description (FR-012 applies to this like any
  optimization).
- **Rationale**: commonly free single-digit wins; not worth degraded build
  ergonomics if the measurement disagrees.
