# Implementation Plan: Iceberg Destination (Provider-Agnostic REST Catalog)

**Branch**: `016-iceberg-dest` | **Date**: 2026-07-22 | **Spec**: specs/016-iceberg-dest/spec.md

**Input**: Feature specification from `/specs/016-iceberg-dest/spec.md`

## Summary

A new THIN destination crate, `rdlt-connector-iceberg`, that maps engine
commits onto atomic Iceberg snapshots through the REST catalog protocol —
one implementation, many providers (Polaris/Snowflake Open Catalog,
Databricks UC; Glue as a phase-2 leg). The Iceberg mechanics (Avro
manifests, metadata, field IDs, stats, REST commit machinery) come from
Apache `iceberg-rust` — the FR-002 survey ran DURING planning with
registry facts and a live resolution probe: `iceberg` 0.10.0 requires
arrow `^58`/parquet `^58` (our exact workspace major; single arrow tree
confirmed at 58.4), catalog client = `iceberg-catalog-rest` 0.10, data
IO = `iceberg-storage-opendal` 0.10 (`opendal-s3` feature). What stays
ours: the config vocabulary (family S3 spelling + `Secret`, generated
schema), exactly-once receipts in snapshot SUMMARY properties (D3 made
Iceberg-native, readable from table history alone), bounded
commit-conflict retry, typed-error classification wrapping the library
(the duckdb-rs precedent), crash points at transaction boundaries, and
the container/interop matrix (Polaris + RUSTFS canonical; pyiceberg
read-back in the gate, Spark in deep).

## Technical Context

**Language/Version**: Rust 2024 workspace, `unsafe_code = "deny"`;
iceberg 0.10 requires rustc ≥ 1.94 (workspace toolchain 1.96 ✓)

**Primary Dependencies**: NEW (surveyed R1, verdict TAKE): `iceberg`
0.10.0 + `iceberg-catalog-rest` 0.10.0 + `iceberg-storage-opendal`
0.10.0 (feature `opendal-s3` only) — Apache-governed, arrow ^58 /
parquet ^58 / reqwest ^0.12 / tokio ^1 / zstd ^0.13 all matching
existing workspace pins; brings apache-avro/opendal/moka/roaring/
fastnum transitively (recorded). NOT taken: `iceberg-catalog-glue`
(native aws-sdk smithy tree — the phase-2 Glue decision, R4).
rdlt-connector-rest is NOT a dependency; the file crate's location
plumbing is NOT extracted (vocabulary shared, plumbing not — R2).

**Storage**: whatever the catalog governs — vended credentials
preferred (session tokens), user-supplied S3-compatible override
possible; local leg = Apache Polaris container + RUSTFS container.

**Testing**: cargo nextest; container cells via testcontainers +
podman (skip-not-fail); pyiceberg read-back venv in the standard gate;
Spark read-back in the deep tier; crash sweep `--features failpoints`
against the LIVE local catalog; coverage baseline-first.

**Target Platform**: Linux (distrobox; podman via host shim)

**Project Type**: library crate + façade re-export
(`rdlt::connector::iceberg`) + CLI `destination: iceberg:` block

**Performance Goals**: correctness-first; one scoreboard cell
(`iceberg-polaris-200k`, never gated — the floor would measure the
catalog/store containers); existing gated bars untouched.

**Constraints**: SPI frozen (Append/Replace; merge rejected by
capability like the file dest); config additive with generated
schemas; a NEW crate is semver-additive (the standing 0.2→0.3 major is
unaffected); the ONE unresolved capability (overwrite transaction
support in iceberg-rust 0.10) carries a T001 probe + a DESIGNED
fallback (v1 narrows to Append with Replace typed-unsupported) — never
improvised mid-implementation (FR-002).

**Scale/Scope**: one new crate (~config + dest impl + error mapping,
est. 1.5–2k lines), 3 container fixtures (polaris, rustfs reused, UC
OSS candidate), an estimated 40–60 cells, one venv interop harness.

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from 001–015. **Seams sacred**: PASS — SPI untouched; the
library is wrapped at one boundary (typed errors, receipts, config),
never leaked through the SPI. **No silent failures**: PASS — closed
type-mapping table, typed catalog/namespace/table/column errors,
bounded conflict retries, replay detection from snapshot history,
unsupported capabilities typed (never no-op). **Correctness before
speed**: PASS — interop read-back by independent engines is the
oracle; bench is scoreboard-only. **Measured, not asserted**: PASS —
the survey ran on registry facts + a live resolution; remaining
capability questions are T001 probes with recorded verdicts and
designed fallbacks. **Safe Rust**: PASS — no unsafe; new deps
surveyed (R1) under Apache governance.

Post-design re-check: PASS.

## Project Structure

### Documentation (this feature)

```text
specs/016-iceberg-dest/
├── plan.md              # This file
├── research.md          # R1–R10 (survey verdict RECORDED with facts)
├── data-model.md
├── quickstart.md
├── contracts/
│   └── iceberg-dest.md  # ID1–ID8
└── tasks.md             # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
crates/rdlt-connector-iceberg/
├── Cargo.toml               # iceberg + catalog-rest + storage-opendal
├── src/
│   ├── lib.rs               # thin façade + re-exports
│   ├── config.rs            # catalog/auth/tables/partition vocabulary
│   │                        #   (Secret fields, generated schema,
│   │                        #   from_yaml/from_json/from_value)
│   ├── schema.rs            # CLOSED engine-type → Iceberg-type table
│   ├── commit.rs            # engine commit → transaction; receipts in
│   │                        #   snapshot properties; conflict retry;
│   │                        #   replay detection from history
│   ├── errors.rs            # iceberg::Error → typed Dest classification
│   └── dest.rs              # Destination + LoadSession impls
├── tests/
│   ├── common/mod.rs        # polaris + rustfs fixture (testcontainers,
│   │                        #   skip-not-fail; catalog health = /v1/config)
│   ├── config_schema.rs
│   ├── catalog_live.rs      # US1/US2 cells against Polaris+RUSTFS
│   ├── interop.rs           # pyiceberg read-back (venv harness)
│   └── sweep.rs             # failpoints, live-catalog arm
crates/rdlt/…                # facade: feature `iceberg`, module re-export
crates/rdlt-cli/src/main.rs  # DestSpec::Iceberg
benches/…                    # iceberg-polaris-200k scoreboard cell +
                             #   polaris fixture (Container kind; rustfs reused)
tools/interop/               # pyiceberg_readback.py + spark_readback
                             #   (deep tier) — venv pattern from
                             #   benches/competitors
```

**Structure Decision**: single-purpose destination crate in the family
pattern (dest-only; no `source/` until a reading feature exists). The
library boundary is `errors.rs` + `commit.rs` — nothing above them
sees iceberg-rust types.

## Design Notes (delta-level)

- **Survey resolution (R1, recorded)**: the arrow-major disqualifier
  CLEARED at plan time with registry facts + a live `cargo tree`
  resolution (single arrow 58.4 tree). The remaining probe is the
  WRITE-PATH surface: append transactions are known-supported;
  overwrite is probed at T001 with the fallback DESIGNED — v1 ships
  Append, and Replace is a typed "not supported by this release" until
  the probe is green (FR-008 narrows, recorded, never silently).
- **Exactly-once (R3)**: commit() writes data files via the library,
  sets snapshot summary properties `rdlt.pipeline`, `rdlt.load-id`,
  `rdlt.commit-seq`, then commits atomically. Replay detection walks
  snapshot-history properties for the (load, seq) identity BEFORE
  building the transaction — a replayed commit discards staged work
  and returns the prior receipt. StateDoc rides TABLE PROPERTIES
  (`rdlt.state` JSON, small, updated in the same commit) — readable by
  read_state from the catalog alone (alternatives recorded in R3).
- **Conflict retry (R3)**: bounded (default 4 attempts, jittered)
  around refresh-metadata → rebuild → commit; exhaustion is typed
  fatal naming the table and the competing snapshot.
- **Auth (R4)**: OAuth2 client-credentials + bearer flow through
  `iceberg-catalog-rest` config props; credentials live in OUR config
  as `Secret` and are revealed only into the catalog builder.
  SigV4/Glue is PHASE-2: probe whether catalog-rest permits a signing
  middleware, else `iceberg-catalog-glue` becomes its own surveyed
  decision — not bundled into v1 (parity records the deferral).
- **Vending (R5)**: request vended credentials
  (`X-Iceberg-Access-Delegation`) via the library's catalog config;
  the storage-override block (family S3 vocabulary spelling) is the
  explicit alternative; both proven against Polaris.
- **Schema (R6)**: closed mapping table in schema.rs; field IDs come
  from the catalog/library exclusively; additive drift = UpdateSchema
  add-nullable-column; contradictory drift typed.
- **Crash points (R9)**: `ice.files.write`, `ice.commit`,
  `ice.receipt.visible` — swept against the live Polaris arm
  (container-gated), with a DUPLICATE-FREE snapshot history asserted
  after convergence.

## Verification Map (story → proof)

- US1 → catalog_live.rs: exact totals, one snapshot per non-empty
  commit, replayed (load, seq) publishes nothing, conflict-retry cell
  (a competing committer between refresh and commit), typed
  unreachable/unauthorized/missing-warehouse cells; sweep.rs
  crash/rerun with snapshot-history pins.
- US2 → auth vocabulary schema round-trips + grep-proof; OAuth2 +
  vended-credential cells against Polaris; bearer cell against the UC
  OSS leg IF the T001 gate verifies it (else recorded deferred);
  SigV4 deferred to phase-2 (recorded in parity/matrix).
- US3 → interop.rs: pyiceberg reads plain/partitioned/post-drift
  tables via the same catalog (counts/schema/partitions); the Spark
  read-back script runs in the deep tier only.
- Close-out → matrix zero uncited, parity record vs dlt's Iceberg
  support, coverage ≥80% baseline-first, scoreboard cell recorded,
  README (option-complete), quickstart walked, make check + semver
  (additive crate).

## Phase 2 note for /speckit-tasks

Order: T001 environment gate (Polaris/UC images + env VERIFIED live
like RUSTFS in 015; the overwrite + vending capability probes — the
recorded go/narrow decisions) → crate skeleton + config/schema
round-trips → append path against Polaris → exactly-once receipts +
conflict retry → Replace (or the recorded narrowing) → partitioning →
vending + auth matrix → crash sweep → pyiceberg interop → CLI/facade/
bench → matrix/parity/README/close-out. The pyiceberg venv follows the
competitors-harness pattern; Spark lands in the deep target only.
