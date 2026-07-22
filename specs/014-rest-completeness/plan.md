# Implementation Plan: REST Source Completeness

**Branch**: `014-rest-completeness` | **Date**: 2026-07-22 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/014-rest-completeness/spec.md`

## Summary

Restructure `rdlt-connector-rest` into three public layers — client
(auth incl. OAuth2 client-credentials with single-flight refresh +
redacted secrets, transient/fatal classification, bounded Retry-After,
pacing), read (the 7-family paginator vocabulary behind a public
`Paginator` trait with loop guards, JSONPath-subset extraction,
incremental start/end binding, parent-child resolution), and the
additively-evolved config document (R1–R7). Response actions are
declared allow-lists over the unchanged typed-error posture. Crash
points (`rest.request`/`rest.decode`/`rest.checkpoint`) join the engine
sweep with armed-fire pins (R8). Zero new external dependencies (R9 —
OAuth2 is one POST; Link-header and JSONPath subsets are hand-rolled
per the 009 survey discipline). Verified to the house standard:
traceability matrix, ≥80% coverage baseline-first, wiremock conformance
per pagination×auth×action, dlt-parity record (surface audited from
`../dlt`), the env-gated PokeAPI live cell (FR-013), and the gated
REST→PG ≥5× bar re-measured at close-out. The composition claim (US3)
is proven by an in-crate example connector built only from public
pieces.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: NONE new — reqwest (existing) carries HTTP +
TLS; OAuth2/Link-header/JSONPath-subset hand-rolled (R9); wiremock
(existing dev-dep) drives conformance

**Storage**: none (source connector; cursors ride engine state)

**Testing**: wiremock conformance matrix (pagination × auth × actions ×
incremental × parent-child), config-schema round-trips, secret
grep-proof cell, engine crash sweep (new rest points), env-gated
PokeAPI live cell (`RDLT_NET=1`), composed-example build as CI proof

**Target Platform**: Linux (dev/CI); network only in the gated live cell

**Project Type**: existing crate restructure — `src/{config.rs,
client/, read/}` + `lib.rs` façade; no new crates (R1)

**Performance Goals**: gated REST→PG bar ≥5× stays green (the
no-selector passthrough is preserved byte-for-byte in behavior, RS5);
no new gates — scoreboard only if measurement-first justified

**Constraints**: SPI frozen; existing config spellings frozen (aliases
where superseded); configs are data (no callbacks — RS1); S3 retry
posture unchanged (engine owns retries, RS3); secrets never render
(RS4); default `make check` stays network-free

**Scale/Scope**: 7 pagination families, 6 auth schemes, actions +
incremental + parent blocks; ~3 new fail points; matrix ≈ the full
config surface; 1 composed example + 1 live cell

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from 001–013. **Seams sacred**: PASS — SPI untouched; the new
public surface is THIS crate's library layer, contract-bound (RS6).
**No silent failures**: PASS — loop guards typed, no paginator
auto-detection (deliberate anti-guessing decision, R2), actions are
declared allow-lists, no-match selectors typed. **Correctness before
speed**: PASS — conformance matrix + crash sweep before parity claims.
**Measured, not asserted**: PASS — coverage baseline-first, gated bar
re-measured, parity recorded per-option. **Safe Rust**: PASS.

Post-design re-check: PASS.

## Project Structure

### Documentation (this feature)

```text
specs/014-rest-completeness/
├── plan.md              # This file
├── research.md          # R1–R9
├── data-model.md        # config §1–§9
├── quickstart.md        # use + verify + compose
├── contracts/
│   └── rest-source.md   # RS1–RS8
├── matrix.md            # built in implementation (011 pattern)
└── tasks.md             # /speckit-tasks output (NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
crates/rdlt-connector-rest/
├── src/
│   ├── lib.rs               # RestSource façade (Source impl, wiring)
│   ├── config.rs            # document (additive; aliases for old spellings)
│   ├── client/
│   │   ├── mod.rs           # RestClient: send + classify + pacing + Retry-After
│   │   ├── auth.rs          # schemes incl. OAuth2 lifecycle; Secret newtype
│   │   └── secret.rs        # Secret(String), redacted Debug/Display
│   └── read/
│       ├── mod.rs           # per-stream read loop (fail points live here)
│       ├── paginate.rs      # Paginator trait + 7 config-backed impls + guards
│       ├── extract.rs       # JSONPath-subset Selector + records extraction
│       └── resolve.rs       # parent-child placeholder resolution
├── examples/
│   └── composed_api.rs      # US3 proof: public-pieces-only mini connector
└── tests/
    ├── conformance.rs       # existing, extended
    ├── pagination.rs        # 7 families × termination × guards (wiremock)
    ├── auth.rs              # schemes + OAuth2 refresh/401 + secret grep-proof
    ├── actions.rs           # response actions + POST bodies + selectors
    ├── children.rs          # parent-child resolution + failure naming
    └── pokeapi_live.rs      # RDLT_NET=1-gated (FR-013)
crates/rdlt-engine/tests/crash_sweep.rs   # + rest source sweep arm
```

## Design Notes (delta-level)

- **Read-loop rewrite is the risky core**: today's single `read` fn
  becomes client+paginator+extractor composition. The existing
  conformance cells (page/offset/none, cursor resume) are the
  behavior-preservation net — they must pass UNCHANGED before any new
  family lands (the 013 extraction discipline applied at test level:
  old cells green first, then additions).
- **Paginator state machine**: `next()` consumes a bounded response
  summary (status, headers of interest, parsed cursor/link values —
  never the whole body twice); the read loop owns the request build.
  The same-request guard hashes the (url, query, body) triple.
- **OAuth2 as data**: the token endpoint call is issued through the
  SAME client classify path (a 5xx token fetch is transient); token
  cache is per-RestSource (one credential set per source document).
- **Parent-child buffering**: only resolved placeholder values + the
  declared include fields buffer (bounded); the child reads parents
  from the ENGINE-visible stream feed, not a re-request.
- **PokeAPI cell**: 100-record page cap, 100ms pacing, structural
  asserts only; skipped (not failed) without `RDLT_NET=1`.
- **dlt parity record**: paginator/auth mapping from R2/R3; deliberate
  deviations = no auto-detection, no OAuth JWT (yet), callables→seam.

## Verification Map (story → proof)

| Story | Proof surface |
|---|---|
| US1 declarative surface | pagination.rs (7 families + guards), auth.rs, actions.rs; config round-trips; typed-error cells naming fields |
| US2 incremental + politeness | conformance resume cells (kept green), Retry-After live-mock cell, pacing observability cell, engine crash sweep w/ rest pins |
| US3 composition | examples/composed_api.rs builds + runs against wiremock via public API only; children.rs; PokeAPI live cell (list + detail) |
| Governance | make check network-free; gated REST→PG bar in tolerance; coverage ≥80% recorded; matrix zero uncited rows; parity record |

## Phase 2 note for /speckit-tasks

Order: T001 coverage baseline + existing-cell green-pin (the weld) →
client layer (secret newtype + classify extraction, no behavior
change) → paginator trait + the frozen three families rewired (old
conformance cells must stay green) → new families + guards → auth
additions (OAuth2 + api_key) → selectors + actions + POST → incremental
block → parent-child → fail points + sweep arm → composed example →
PokeAPI cell → matrix + coverage + parity + bench re-measure + README.
Config spellings frozen throughout; every stage keeps the whole suite
green (no big-bang rewrite commit).
