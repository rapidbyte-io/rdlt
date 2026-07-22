# Implementation Plan: Filesystem/Object-Store Completeness

**Branch**: `015-file-completeness` | **Date**: 2026-07-22 | **Spec**: specs/015-file-completeness/spec.md

**Input**: Feature specification from `/specs/015-file-completeness/spec.md`

## Summary

Unify the file family — `rdlt-connector-parquet` merges INTO
`rdlt-connector-file` (parquet is a format, not a system) with the
`src/source/` + `src/dest/` family layout and a shared `formats` module —
behind the behavior-preservation net (every pre-015 cell green unchanged,
the gated `parquet-passthrough` bar in-band same-session, pipeline-YAML
spellings and persisted cursor/staging/receipt formats FROZEN). Then
completeness on the unified crate: a `Location` abstraction (local
filesystem exactly as today | S3-compatible object store via the
`object_store` crate, static credentials in `Secret` fields), discovery
with deterministic ordering across listing pagination, per-file
incremental cursors extended with object identity (same loud-failure
rules), CSV as a first-class record format (declared options + documented
inference + type hints), gzip/zstd transparent decode (codecs already in
the tree via parquet), and the destination writing parquet AND jsonl to
either location kind with an optional partition column and commit-atomic
visibility (staged keys → finalize at commit; readers can never observe a
partial object). Live cells run against RUSTFS (Apache-2.0 S3-compatible
server) in a container under the podman skip-not-fail pattern. House
verification closes it: matrix (zero uncited), dlt-parity vs dlt's
filesystem source/destination, ≥80% coverage baseline-first, crash sweep
over both location kinds, file→duckdb scoreboard cell, README,
quickstart.

## Technical Context

**Language/Version**: Rust 2024 workspace (rustc pinned by rust-toolchain),
`unsafe_code = "deny"` (sole exception: CLI mallopt FFI — untouched here)

**Primary Dependencies**: existing — arrow/parquet 58.3 (workspace-pinned
major), tokio, serde/serde_yaml/serde_json, glob, schemars. NEW (surveyed,
R1): `object_store` (aws feature only) — the one genuinely new external
dep. NOT new (already in the lock, promoted to direct workspace deps):
`csv` 1.4 (via arrow-csv today), `flate2`, `zstd` (via parquet codecs
today).

**Storage**: local filesystem + any S3-compatible object store (endpoint +
static access/secret key; path-style addressing for test servers).
Live-test store: RUSTFS container (Apache-2.0) via the podman shim.

**Testing**: cargo nextest (doc-tests via `cargo test --doc`); wiremock is
NOT used here — object-store cells hit the real RUSTFS container,
skip-not-fail without a container runtime (postgres-cell pattern); crash
sweeps `--features failpoints` with armed-fire pins; `cargo llvm-cov`
coverage baseline-first.

**Target Platform**: Linux (distrobox dev env; podman via host shim)

**Project Type**: library crate inside the rdlt workspace (family crate +
façade re-export `rdlt::connector::file`)

**Performance Goals**: local JSONL fast path (flagship bench input) must
not regress — `jsonl-duckdb-200k` and `parquet-passthrough` gated bars
stay in-band same-session as the merge; object-store throughput recorded
as scoreboard data only (new `file-s3-duckdb` cell class scoreboard).

**Constraints**: SPI (`rdlt-connector`) untouched; persisted formats
(cursor doc `CURSOR_FORMAT_VERSION 1`, staging layout
`LAYOUT_FORMAT_VERSION 1`, receipts) readable unchanged; pipeline-YAML
spellings frozen (`file:` source, `parquet:` destination); config growth
additive; semver: the crate REMOVAL rides the already-recorded 014
one-time major (0.2→0.3 at next publish) — no second break.

**Scale/Scope**: one unified crate (~1.1k existing lines + the new
location/format/dest surface), CLI spec additions, container fixture, an
estimated 60–80 new test cells across unit/live/sweep.

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from 001–014. **Seams sacred**: PASS — SPI untouched; the family
merge changes crate topology, not contracts; persisted formats versioned
and preserved. **No silent failures**: PASS — unreachable/unauthorized
locations are typed errors (empty PREFIX ≠ unreachable bucket), listing
must be complete-or-fail across pagination, shrunk/rewritten files stay
loud on both location kinds, codec mismatch typed. **Correctness before
speed**: PASS — object-store paths are correctness-first (scoreboard, not
gated); the weld is proven before any capability lands. **Measured, not
asserted**: PASS — coverage baseline-first, both touched gated bars
re-measured same-session, delta runs proven by read accounting.
**Safe Rust**: PASS — no new unsafe; dependency additions surveyed per
the 009 rule (R1).

Post-design re-check: PASS.

## Project Structure

### Documentation (this feature)

```text
specs/015-file-completeness/
├── plan.md              # This file
├── research.md          # Phase 0 (R1–R10)
├── data-model.md        # Phase 1
├── quickstart.md        # Phase 1
├── contracts/
│   └── file-family.md   # FF1–FF8
└── tasks.md             # Phase 2 (/speckit-tasks — not this command)
```

### Source Code (repository root)

```text
crates/rdlt-connector-file/
├── src/
│   ├── lib.rs               # thin façade: pub mod source/dest + re-exports
│   ├── location/            # SHARED: local | s3-compatible (object_store)
│   │   ├── mod.rs           # Location enum, list/read/write/finalize ops
│   │   └── s3.rs            # object_store wiring, creds (Secret), errors
│   ├── formats/             # SHARED: what each format IS to rdlt
│   │   ├── mod.rs           # Format enum (jsonl|csv|parquet) + codecs
│   │   ├── jsonl.rs         # moved from src/jsonl.rs (fast path intact)
│   │   ├── csv.rs           # NEW: reader → NDJSON records, inference
│   │   └── parquet.rs       # moved reader; writer pieces used by dest
│   ├── source/
│   │   ├── mod.rs           # FileSource (moved lib.rs Source impl)
│   │   ├── config.rs        # moved src/config.rs + location/format opts
│   │   └── cursor.rs        # moved src/cursor.rs + object identity
│   └── dest/
│       ├── mod.rs           # FileDest (the parquet crate's Destination)
│       └── config.rs        # dest options: format, partition_by, location
├── tests/                   # existing cells (mechanical paths) + new
│   ├── s3_live.rs           # RUSTFS container cells (skip-not-fail)
│   ├── csv.rs               # format cells (local)
│   └── sweep.rs             # both-location crash sweep
crates/rdlt-connector-parquet/   # DELETED (absorbed)
crates/rdlt/src/…                # façade: rdlt::connector::file gains dest;
                                 # rdlt::connector::parquet re-export REMOVED
crates/rdlt-cli/src/main.rs      # DestSpec::Parquet kept (frozen spelling)
                                 # + DestSpec::File (new location/format opts)
benches/…                        # parquet-passthrough cell UNTOUCHED;
                                 # + file-s3-duckdb scoreboard cell + fixture
```

**Structure Decision**: the 013/014 family layout applied to the file
family; `location/` and `formats/` sit at crate root because BOTH sides
consume them (the sqlcore lesson applied in-crate: one place defines what
a format/location is — never per-direction copies).

## Design Notes (delta-level)

- **The weld (US1)** is two mechanical moves + one absorption: file
  source → `src/source/` (T001-style, moves only), parquet destination
  crate → `src/dest/` (public items re-exported from the façade;
  `pq.*` fail-point names, staging constants, receipt/state formats
  byte-identical). The CLI keeps `DestSpec::Parquet { path }` parsing
  exactly as today (frozen spelling), now constructing the unified
  crate's dest.
- **Location** is an internal trait-shaped enum (list / read-range /
  put-staged / finalize / identity), NOT a public trait this feature —
  the public surface is config vocabulary; the seam can open later
  without breaking documents.
- **Finalize semantics** (FR-010): local = the existing staged-name →
  atomic rename protocol, unchanged. Object store = staged keys under the
  staging prefix, finalize = server-side copy-to-final + delete-staged at
  commit (single-object copy is atomic-visibility per key; readers never
  see a partial object because visibility IS the copy). Recovery
  converges because final names stay deterministic per
  (load, commit, table, n) — re-copy is idempotent; leftover staged keys
  are superseded/aborted. Mechanism recorded per store capability (R6).
- **CSV is a RECORD format** (R4): rows convert to NDJSON records
  (inference: bool/int64/float64/utf8 with documented widening;
  `type_hints` override per column), riding the same shred path,
  primary_key/dedup/merge semantics as jsonl — NOT a structured stream.
  parquet stays structured (S7), unchanged.
- **Compression** (R5): extension-driven gzip/zstd decode wraps the
  jsonl/csv byte readers; compressed files are whole-file incremental
  units (cursor `done` = decompressed bytes consumed, resume only at
  done==size i.e. skip-completed; growth of a compressed file = rewrite
  → the existing loud-failure rule).
- **Object identity for cursors** (R3): local keeps (size, mtime); object
  store uses (size, etag) with the same tripwire semantics — etag change
  at same size = rewritten in place = typed error.
- **Crash points**: existing `pq.*` set preserved; new points at
  `file.list`, `file.read`, `file.stage.put`, `file.finalize.copy`,
  `file.finalize.delete` — swept on local AND container cells.

## Verification Map (story → proof)

- US1 → pre-015 suites green unchanged + `parquet-passthrough` and
  `jsonl-duckdb-200k` gated bars in-band same-session + persisted-format
  fixture cells (pre-015 cursor/receipt documents parse).
- US2 → RUSTFS container cells: seeded-bucket exact totals per format ×
  codec, pagination-complete listing (seed > one page), delta run by
  read accounting, CSV options + hints, typed unreachable/unauthorized.
- US3 → container + local dest cells: commit-atomic visibility probe
  (list DURING run sees no final-name partials), partition split,
  crash sweep both location kinds, jsonl output parity.
- Close-out → matrix (zero uncited), dlt-parity record, coverage ≥80%
  baseline-first, file-s3-duckdb scoreboard cell recorded, README,
  quickstart walked, `make check` + doc-tests + semver (no NEW break
  beyond the recorded 014 major).

## Phase 2 note for /speckit-tasks

Task order mirrors the dependency spine: baseline+weld (source move, dest
absorption, net proven) → location layer + config evolution → formats
(CSV, codecs) → incremental-over-objects → dest locations/partitioning →
crash points → RUSTFS fixture + live cells → matrix/parity/close-out.
The RUSTFS image/tag/env verification is an explicit early task (the
podman shim may also need recreating — check first, standing note).
