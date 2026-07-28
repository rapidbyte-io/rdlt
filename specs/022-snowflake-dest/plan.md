# Implementation Plan: Snowflake Destination Connector

**Branch**: `022-snowflake-dest` | **Date**: 2026-07-28 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/022-snowflake-dest/spec.md`

## Summary

A new thin destination crate `rdlt-connector-snowflake` — the third SQL
destination — built on the adopted `snowflake-connector-rs 1.1.0` session
layer, wrapped at one boundary exactly as duckdb-rs is (owner decision;
research D1 records the survey, the source-verified PUT gap, and the
designed hand-rolled fallback). Key-pair JWT auth only, proven live against
the qual account. Full sqlcore merge-vocabulary parity through a new dialect
whose merge is `MERGE INTO` + `QUALIFY` dedup (proven live, including the
structured duplicate-diagnosis code 100090), with the existing
postgres/duckdb golden pins byte-identical. Ingestion ships two paths —
batched INSERT as the universal default, external-stage `COPY INTO` (user
bucket, plain SQL through the crate) as the bulk option — with the
internal-stage PUT path DEFERRED on a named upstream trigger and the
crossover measured, not guessed. The commit protocol is pure-DML
transactions with all DDL strictly outside the unit — DDL auto-commit was
proven live, so this is a guarded invariant, not a convention. Both fired
sqlcore extraction triggers are TAKEN (ensure choreography and session
protocol), with pin byte-identity as the non-negotiable constraint. Live
legs gate skip-not-fail on the credential convention; fakesnow server mode
is a T001 fidelity probe, adopted or rejected on its transcript.

## Technical Context

**Language/Version**: Rust, workspace toolchain 1.96.0 (pinned;
`rust-toolchain.toml` + workspace `rust-version`)

**Primary Dependencies**: `snowflake-connector-rs 1.1.0` (default
features: key-pair auth; wrapped at one boundary; brings `reqwest 0.13` — a
second reqwest major behind the `snowflake` feature, the recorded
0.12/0.13-double-tree shape), `object_store` (existing) for external-stage
parquet placement, the workspace parquet writer via the file family's
format machinery, `rdlt-connector-sqlcore` for options/planning/dialect. NO
arrow version change (the single arrow 58 tree stands). The hand-rolled
session client is the recorded fallback, not a dependency.

**Storage**: Snowflake (SaaS; qual account AWS_EU_CENTRAL_1) — destination
tables, `_rdlt_`-prefixed state/receipt tables; optional user-owned cloud
bucket as the external stage for bulk COPY. No persisted-format changes
anywhere in rdlt.

**Testing**: `cargo nextest run` (doc-tests via `cargo test --doc`);
credential-gated live legs (skip-not-fail, testkit-style
`snowflake_available()` probe); mock transport for protocol/statement-count
tests; golden-SQL pins for the new dialect; existing pg/duckdb pins
byte-identical; destination-conformance harness; crash sweep with armed-fire
pins; optional fakesnow hermetic leg pending its T001 fidelity probe;
differential oracle vs the postgres destination on the live leg.

**Target Platform**: Linux dev/CI parity with existing workspace; the
connector itself is platform-neutral Rust.

**Project Type**: workspace library crate (connector family) + facade
feature `snowflake` + CLI `destination: snowflake:` block.

**Performance Goals**: recorded (UNBARRED) ingestion session on the
bench-shaped 1M×12 dataset over whichever shipped paths the session's
credentials allow (INSERT always; external-stage COPY iff a bucket is
provided), saying which ran; statement economy — zero schema-mutation
statements at steady state, statements-per-load constant per table; INSERT
batch-size knee and the INSERT-vs-COPY crossover measured on the qual
account.

**Constraints**: workspace denies `unsafe_code`; one-boundary wrapping
(Principle III) around the adopted crate; typed taxonomy by
structured Snowflake error codes (100090 duplicate-row, 391911-class API
refusals, auth vs permission vs transient); DDL-never-inside-unit invariant
(guarded); existing golden pins byte-identical; purely additive semver.

**Scale/Scope**: one new crate (config/boundary/dest modules + tests), two
sqlcore extractions (ensure choreography, session protocol) with
byte-identity proofs, facade/CLI/pipeline-spec wiring, docs. No engine
changes expected.

## Constitution Check

*GATE: evaluated against constitution v1.1.0 before Phase 0; re-checked
after Phase 1 design — PASS, no Complexity Tracking entries needed.*

- **I. Small Core, Verified Breadth**: PASS with the required argument made
  explicitly — Snowflake is in the "few most-used" set (the dominant cloud
  DW destination in the cohort rdlt benchmarks against); the capability is a
  CONNECTOR, not engine surface; zero engine changes expected. Verification
  depth is the feature's center (live legs, conformance, differential,
  crash sweep).
- **II. Library-First, Thin CLI**: PASS — all capability in the crate behind
  the facade; the CLI block only parses into the same public config.
- **III. One-Boundary Wrapping**: PASS — `snowflake-connector-rs` is
  wrapped at exactly one module boundary (the duckdb-rs precedent): its
  types never cross the crate's public surface, and error translation
  (`snowflake_code()`/`ErrorKind` → SPI taxonomy) happens there and nowhere
  else. The rejected sidecar-PUT stack would have violated this principle;
  recorded in research D1.
- **IV. Exactly-Once Is Sacred**: PASS — receipts/state/replay per the
  established pattern; DML-only unit transaction with the DDL-outside
  invariant guarded in code; crash points at stage-write, publish, and
  receipt-visible, swept live with armed-fire pins; COPY rowcount verified
  against staged counts; unsupported capability degrades typed (e.g. PUT
  refusal shapes, oversized VARIANT).
- **V. Typed Error Taxonomy**: PASS — classification by structured codes
  proven available live (100090, 391911, auth-class codes); substring
  matching forbidden in code and tests; no citation IDs in user-facing
  strings.
- **VI. Self-Contained Code & Comments**: PASS — comments state invariants
  (e.g. why DDL cannot enter the unit, why identifiers are quoted-upper);
  no task/finding IDs; no unsafe.
- **VII. Test-and-Verification Gate**: PASS — nextest; credential-gated
  legs skip-not-fail exactly like container legs, with the convention
  verified at feature start (T001); conformance certification; close-out
  matrix zero uncited; coverage ≥80% baseline-first; parity records vs dlt
  with deferrals named.
- **VIII. Benchmark Governance**: PASS — no bar is added; the recorded
  session is scoreboard/record-only; existing bars re-verified untouched.
- **IX. Frozen Contracts & Persisted Formats**: PASS — contract
  `contracts/snowflake-dest.md` SD1–SD8; no persisted-format changes; golden
  pins guard the shared core through both extractions; config enums
  `#[non_exhaustive]`; additive semver only.

## Project Structure

### Documentation (this feature)

```text
specs/022-snowflake-dest/
├── plan.md              # This file
├── research.md          # Phase 0 — survey + live-probe decisions D1–D10
├── data-model.md        # Phase 1 — entities and state transitions
├── quickstart.md        # Phase 1 — config-only walk (key setup → merge load)
├── contracts/
│   └── snowflake-dest.md  # SD1–SD8
└── tasks.md             # Phase 2 (/speckit-tasks — not created here)
```

### Source Code (repository root)

```text
crates/rdlt-connector-snowflake/
├── Cargo.toml               # feature-gated; snowflake-connector-rs at one boundary
├── README.md                # type mapping, identifier policy, auth walk, caveats
├── src/
│   ├── lib.rs               # thin façade: config + Snowflake destination entry
│   ├── config.rs            # account/user/key/role/warehouse/database/schema
│   │                        # + DestOptions vocabulary; schemars; Secret key
│   ├── boundary.rs          # ONE BOUNDARY — client construction, session
│   │                        # lifecycle, statement execution seam, typed
│   │                        # SnowflakeError from snowflake_code()/ErrorKind;
│   │                        # the DDL-inside-unit refusal lives here
│   └── dest/
│       ├── mod.rs           # Snowflake (Destination impl), open/session wiring
│       ├── ddl.rs           # describe-once ensure; quoted-upper identifiers
│       ├── dialect.rs       # sqlcore MergeDialect: MERGE INTO + QUALIFY dedup
│       ├── ingest.rs        # batched INSERT path; external-stage COPY path
│       │                    # (parquet parts to the user bucket via object_store)
│       └── commit.rs        # DML-only unit tx; receipts/state; replay; COPY verify
└── tests/
    ├── golden_sql.rs        # snowflake dialect pins (byte-for-byte)
    ├── config_schema.rs     # schema + round-trip + secret grep-proof
    ├── boundary_mock.rs     # mock statement seam: statement counts, retry classes
    ├── live_dest.rs         # credential-gated: conformance, strategies, differential
    └── crash_sweep.rs       # armed-fire pins at sf.* crash points

crates/rdlt-connector-sqlcore/src/…   # ensure choreography + session protocol
                                      # extractions (pins byte-identical)
crates/rdlt/                          # facade feature `snowflake`, module alias
crates/rdlt-cli/ + rdlt/pipeline_spec # `destination: snowflake:` block
crates/rdlt-testkit/                  # snowflake_available() credential probe
```

**Structure Decision**: family layout mirroring the postgres crate
(config/client/dest split), with the protocol client as the single wrapped
boundary per Principle III. sqlcore work lands as separate increments ahead
of the snowflake consumer so pin byte-identity is proven before the third
consumer exists.

## Complexity Tracking

No constitution violations to justify. The one deliberate scope note: the
adopted crate cannot reach internal-stage PUT (verified in source), so the
bucket-free bulk path is a NAMED deferral on an upstream trigger rather
than a shipped capability — recorded in research D1/D6, surfaced in the
dlt-parity matrix, never silent. The second reqwest major it brings is
feature-gated and recorded (the established double-tree shape).
