# Implementation Plan: Postgres Source Completeness — Parity + TLS

**Branch**: `006-postgres-completeness` | **Date**: 2026-07-20 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/006-postgres-completeness/spec.md`

## Summary

Close the four measured dlt-parity gaps and the trust surfaces. TLS
lands inside a MERGED `rdlt-postgres` crate (owner decision — the two
postgres connectors become one crate with `source`/`dest` feature-gated
modules and a shared `tls` module; facade paths unchanged via
re-exports; amends 005 R9, recorded in research R1) — full sslmode
matrix with libpq semantics, custom root CAs, typed verification
errors, structurally drift-proof between the two directions. The source gains per-column type hints (server-side cast
projections, closed conversion table) and query streams (user SQL
wrapped as a subquery — which also enforces read-only — with
describe-based schemas). The engine's clause B4 is amended by a
recorded contract event: keyed structured streams merge through the
EXISTING `DestCapabilities.merge` capability (key = declared primary
key instead of `_rdlt_root_id`). Trust surfaces: lossy mappings warn
once per column via tracing; config schemas are generated (schemars)
from the same structs that parse configs and wired into the EXISTING
`ConnectorSpec.config_schema` field (verified present — zero SPI
change); the three 005 review test advisories close.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies** (new): `tokio-postgres-rustls` + `rustls`
(default provider) + `rustls-pemfile` + `rustls-native-certs` (TLS,
runtime); `schemars` (config schemas, runtime for the three source
crates); `rcgen` (dev — self-signed certs for the TLS test matrix).
No sqlx, no openssl.

**Storage**: n/a (state machinery reused; merge uses existing staging)

**Testing**: existing suites + TLS matrix against a cert-configured
postgres container (rcgen-generated CA/server certs, hostname
match/mismatch pairs), hint/query conformance, merge conformance +
crash sweep extension (armed-fire pins extended), schema round-trip
tests, the three advisory closures

**Target Platform**: Linux; reference machine unchanged

**Project Type**: Rust library workspace + dev CLI; NET −1 crate (the
two postgres crates merge into `rdlt-postgres`)

**Performance Goals**: no new cells; existing gated benches stay within
the armed ±3% gate (TLS is off-path for the plaintext benchmarks —
verified by the gate at close)

**Constraints**: safe Rust (`unsafe_code = "deny"`, no new exceptions —
the rustls "danger" verifiers for require/verify-ca are safe-Rust trait
impls, quarantined in one module with loud docs); no
`rdlt-core`/`rdlt-connector` breaking changes (verified: merge uses the
existing capability flag, schemas use the existing `config_schema`
field, `PipelineEvent` is `#[non_exhaustive]` if ever needed); the B4
lift is a RECORDED amendment to the feature-002 contract, not a silent
rewrite; 005 benchmark records untouched

**Scale/Scope**: one crate-merge migration (mechanical moves, facade/
Makefile/fuzz/CI references updated); the source module grows hints +
query streams; engine plan-time merge validation change + both SQL
destinations' merge-by-key; 3 config crates gain schemars derives;
contracts: 3 new + 2 amended; test surface grows the TLS matrix and
merge sweep modes

## Constitution Check

Constitution remains the unfilled template; governing principles per
features 001–005. **Seams sacred**: PASS — zero SPI signature changes
(verified against current `capabilities.rs`, `spec.rs`); the B4
amendment is the sanctioned contract-evolution path with a recorded
event. **No silent failures**: PASS — every new failure mode
(trust-anchor, chain, hostname, hint conversion, non-read query, NULL
merge keys) is a typed, phase-tagged error; lossy visibility removes an
existing silence. **Correctness before speed**: PASS — merge ships
inside the crash-sweep + armed-fire-pin regime before anything is
claimed. **Benchmark honesty**: PASS — no new claims; the gate guards
the old ones.

## Project Structure

### Documentation (this feature)

```text
specs/006-postgres-completeness/
├── plan.md
├── research.md          # R1–R8 decisions
├── data-model.md        # config/entity extensions, merge semantics
├── quickstart.md
├── contracts/
│   ├── tls-policy.md    # sslmode matrix, root resolution, error taxonomy
│   ├── type-hints.md    # closed conversion table (amends 005 type-mapping)
│   ├── query-streams.md # wrapping, describe, read-only, incremental
│   └── merge-structured.md  # the recorded B4 amendment
└── tasks.md
```

### Source Code (repository root)

```text
crates/rdlt-postgres/        # MERGED crate (features: source, dest — both default)
├── src/tls.rs               # shared: TlsPolicy, roots, connector construction
├── src/tls_verify.rs        # require/verify-ca verifiers (quarantined, documented)
├── src/source/              # moved from rdlt-source-postgres (module tree intact)
│   ├── config.rs            # + tls block, + type_hints, + queries[], schemars derive
│   ├── mod.rs               # TLS-aware connect, query streams, lossy tracing
│   ├── types.rs             # hint conversion table (HintType → cast + decode)
│   └── reflect.rs           # describe-based schema for query streams
├── src/dest/                # moved from rdlt-dest-postgres; TLS + merge-by-key
└── tests/                   # moved suites (dest_* prefixes), + tls_matrix.rs
crates/rdlt-dest-duckdb/     # merge-by-key for structured streams
crates/rdlt-engine/          # plan-time B4 lift (keyed structured + merge capability)
crates/rdlt-source-rest/     # schemars derive + config_schema in spec()
crates/rdlt-source-file/     # schemars derive + config_schema in spec()
crates/rdlt-cli/             # dest tls options passthrough
```

**Structure Decision**: one `rdlt-postgres` crate, two feature-gated
direction modules, one shared `tls` module — drift between the
directions is structurally impossible, and the crate count drops.
Facade (`rdlt::postgres`, `rdlt::postgres_source`), Makefile, fuzz,
and CI references are updated in the migration task; everything else
extends in place.

## Phase ordering

1. **US1 TLS**: policy crate → source + destination wiring → cert-
   matrix conformance (rcgen + container entrypoint shim) → remove the
   005 rejections everywhere (code + contract + README).
2. **US2 hints + query streams**: conversion table + config → describe
   reflection → conformance (each independently testable).
3. **US3 merge**: contract amendment text FIRST (recorded event), then
   engine plan-time lift, dest merge-by-key (both), conformance +
   crash-sweep modes + armed-fire pins.
4. **US4 trust surfaces**: lossy tracing + capture test; schemars +
   `config_schema` wiring + round-trip tests; the three advisory
   closures.
5. Close-out: full sweep, gate, semver-checks, parity-table completion
   (SC-007), implementation notes.

## Complexity Tracking

No violations. Guards: the rustls custom verifiers implement libpq's
long-standing `require`/`verify-ca` semantics — deliberately weaker
validation levels users explicitly opt into, never defaults (`prefer`
never validates anyway; `verify-full` is the recommended production
mode and the README says so). Client certs / GSSAPI / revocation
tuning: recorded out.
