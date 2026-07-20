# Implementation Plan: Postgres SQL Source Connector

**Branch**: `005-postgres-source` | **Date**: 2026-07-20 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/005-postgres-source/spec.md`

## Summary

Add `rdlt-source-postgres`: a declarative Postgres source that reflects
table structure from the catalog, streams rows as typed Arrow batches on
the engine's structured path (shredder bypassed — the schema is declared,
not inferred), and supports cursor-column incremental with dlt-parity
boundary semantics. The performance thesis, grounded in the committed
review of dlt's `sql_database` source: dlt's fast path only skips its
normalizer while extraction stays row-by-row in Python — rdlt instead
decodes Postgres's **binary COPY stream directly into Arrow columns**
(`COPY (SELECT …) TO STDOUT (FORMAT BINARY)`), the lever dlt can only
reach by delegating to a Rust library (connectorx) with documented
correctness debt. Robustness beats dlt's recorded gaps: engine-owned retry via S3 error
classification, cursor-ordered mid-table checkpointed resume, per-table
statement-level snapshot consistency, crash-sweep coverage to the 003
standard. Two benchmark
cells (postgres→DuckDB, postgres→Postgres) land baseline-first with
bars set measurement-first per the 004 version-policy protocol.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: `tokio-postgres` 0.7 (already a workspace
dependency via `rdlt-dest-postgres`; adds `runtime` + TLS feature
decision in research R1), `arrow` 58.3 (pinned workspace-wide),
`serde_yaml` (config, as in rest/file sources), existing `fail`-points
registry (crash sweep), `testcontainers-modules` 0.11 postgres (dev).
NO new heavyweight dependencies: no sqlx, no connectorx, no SQLAlchemy
analog — reflection and extraction are hand-rolled on tokio-postgres.

**Storage**: n/a (source only; pipeline state/WAL machinery reused as-is)

**Testing**: `cargo nextest run` (+ `cargo test --doc`); testcontainers
Postgres conformance suite; differential property test (binary-COPY
decoder vs driver row path must produce identical Arrow); crash sweep
with new registered fail points (both passes); memory-ceiling
subprocess test

**Target Platform**: Linux (reference machine for new benchmark cells =
the 003/004 matrix machine)

**Project Type**: Rust library workspace + dev CLI (unchanged shape; one
new crate)

**Performance Goals**: postgres→DuckDB and postgres→Postgres cells
measured baseline-first vs pinned dlt `sql_database` (pyarrow backend =
gated baseline; sqlalchemy default + connectorx = scoreboard); gated
bars derived measurement-first (004 protocol) — no aspirational bar is
written before both sides are measured. New gated iai bench for the
COPY-binary→Arrow decoder hot path.

**Constraints**: bounded memory independent of table size (byte-budgeted
`RecordsOut` backpressure is the engine mechanism; decoder must respect
it); safe Rust only (`unsafe_code = "deny"`, no new exceptions); no
`rdlt-core`/`rdlt-connector` public-API breakage (StreamSpec/ReadRequest/
Cursor as-is — verified sufficient in Phase 1); existing gated benches
stay within the armed ±3% gate; dlt-side benchmark scripts follow the
frozen-methodology discipline (in-process self-timing, baseline first)

**Scale/Scope**: 1 new crate (`rdlt-source-postgres`), facade export +
CLI `SourceSpec::Postgres` arm, 2 baseline scripts + seed script, 1-2
iai bench entries, conformance/property/sweep suites, benchmark records.
Type matrix: the full set of common Postgres scalar types (research R2
table) + documented policy rules for the rest.

## Constitution Check

`.specify/memory/constitution.md` remains the unfilled template (004
precedent). Governing principles are the approved design doc
(`2026-07-18-rdlt-engine-design.md`) as applied by features 001–004:
correctness before speed, seams sacred, no silent failures, benchmark
honesty. Gate evaluation:

- **Seams sacred**: PASS — the source implements the existing `Source`
  trait exactly (`streams()` + `read(ReadRequest)`); resume via
  committed `Cursor` (clause S1), backpressure via push-await (S5),
  cancellation via `ChannelClosed` (S4). No SPI change is planned; if
  Phase 1 had found one necessary it would be a recorded semver event
  (spec FR-012). Post-design check: none needed — verified against
  `rdlt-connector/src/lib.rs` current surface.
- **No silent failures**: PASS — typed `SourceError` for every failure
  mode (FR-008); unmappable values follow documented rules or schema
  policies, never silent coercion (FR-003); watermark advances only on
  commit (FR-007).
- **Correctness before speed**: PASS — the binary decoder ships with a
  differential test against the driver's own row decoding plus a
  typed round-trip conformance matrix; crash sweep extended before the
  benchmark cells are recorded (tasks order US1/US4 nets before US3
  numbers are quoted).
- **Benchmark honesty**: PASS — baseline-first, pinned version, fastest
  documented dlt configuration as the gated baseline, measurement-first
  bars (004 policy), dataset identity recorded, gated/scoreboard status
  explicit on every new row.

## Project Structure

### Documentation (this feature)

```text
specs/005-postgres-source/
├── plan.md              # This file
├── research.md          # Phase 0 (R1–R9: extraction, type map, incremental, benches, tests)
├── data-model.md        # Phase 1 (config, reflected schema, cursor state, records)
├── quickstart.md        # Phase 1 (local run + bench recipe)
├── contracts/
│   ├── source-config.md # declarative YAML contract (validation rules)
│   └── type-mapping.md  # Postgres type → LogicalType contract (lossy rules explicit)
└── tasks.md             # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
crates/rdlt-source-postgres/
├── Cargo.toml           # tokio-postgres, arrow, serde/serde_yaml, rdlt-connector
└── src/
    ├── lib.rs           # PostgresSource: Source impl (streams/read), retry wrapper
    ├── config.rs        # declarative YAML config (contract: source-config.md)
    ├── reflect.rs       # pg_catalog reflection → ReflectedTable (+ PK, types)
    ├── types.rs         # OID → LogicalType mapping (contract: type-mapping.md)
    ├── copy_decode.rs   # binary COPY stream → Arrow builders (THE hot path)
    ├── sqlgen.rs        # identifier quoting + typed cursor literals (injection-safe)
    └── cursor.rs        # watermark + boundary-key state (Cursor encode/decode)
crates/rdlt-source-postgres/tests/
├── conformance.rs       # testcontainers: type matrix, selection, views, drift
├── incremental.rs       # boundary semantics, dedup, NULL policy, regression guard
├── differential.rs      # proptest: copy_decode ≡ driver-row path
└── crash_sweep.rs       # fail-point sweep (both passes) + connection-drop
crates/rdlt-engine/benches/iai_hotpath.rs   # + pg copy-decode bench (gated)  [or a
                                            #   sibling bench in the new crate — R7]
crates/rdlt-cli/src/main.rs                 # SourceSpec::Postgres { config } arm
crates/rdlt/src/lib.rs                      # facade: rdlt::postgres_source
benches/
├── baseline/pipeline_pg_duckdb.py          # dlt sql_database → duckdb (backend param)
├── baseline/pipeline_pg_pg.py              # dlt sql_database → postgres (backend param)
├── baseline/seed_pg.sql|py                 # deterministic dataset seed (identity recorded)
├── run-e2e.sh                              # + pg cells recipe (or sibling runner)
└── RESULTS.md                              # + two rows, policy entries, history
```

**Structure Decision**: mirror `rdlt-source-rest` (the existing source
crate) exactly in shape: config-from-YAML constructor, `Source` impl,
facade re-export, CLI arm. The binary COPY decoder lives in the source
crate, not the engine — the engine's structured path (`PushPayload::
Arrow`) is already the seam and stays untouched.

## Phase ordering (mirrors spec priorities)

1. **US1 — snapshot MVP**: reflection → type mapping → COPY-binary
   decoder → structured push → conformance matrix green → CLI/facade
   wiring. Independently shippable.
2. **US2 — incremental**: cursor config → SQL generation with typed
   literals → watermark/boundary-key state in `Cursor` → checkpoint
   integration → boundary/dedup/NULL/regression tests.
3. **US4 — robustness** (ordered before benchmarks by design: numbers
   are only quoted on hardened code): fail points registered + sweep
   suites (both passes), connection-drop tests, retry policy,
   memory-ceiling test.
4. **US3 — benchmark cells**: seed + baseline scripts (dlt measured
   FIRST, pyarrow backend gated / sqlalchemy + connectorx scoreboard),
   rdlt cells, measurement-first bars via version-policy entries,
   full-matrix regression check, iai gate re-record including the new
   decoder bench (a NEW baseline, not a drift re-record — FR-007/004-P5
   compliant: the commit names this feature).

## Complexity Tracking

No constitution violations. Two deliberate scope guards:

- The differential test (decoder vs driver rows) is a correctness
  net, not a performance comparison — no benchmark claims from it.
- Cross-table snapshot, custom SQL streams, CDC, and non-Postgres
  dialects are recorded deferrals (spec Assumptions); the dialect seam
  is "don't hard-code where free", with zero second-dialect code.
