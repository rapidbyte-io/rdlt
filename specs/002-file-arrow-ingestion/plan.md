# Implementation Plan: File & Arrow-Native Ingestion

**Branch**: `002-file-arrow-ingestion` | **Date**: 2026-07-19 | **Spec**: [spec.md](spec.md)

**Input**: Feature spec from `/specs/002-file-arrow-ingestion/spec.md`. This is an
incremental slice on the architecture established by feature 001
([plan](../001-rdlt-ingestion-engine/plan.md), [research](../001-rdlt-ingestion-engine/research.md),
[contracts](../001-rdlt-ingestion-engine/contracts/)) — those documents remain
authoritative; this plan records only what this feature adds or amends.

## Summary

Two new bundled connectors and one engine capability: `rdlt-source-file` (JSONL +
Parquet, glob selection, per-file byte/row-group cursors with shrunk-file detection),
`rdlt-dest-parquet` (minimal write-only parquet destination honoring the full D-clause
correctness contract), and engine-level **Arrow passthrough** — structured batches
bypass the shredder (schema mapping + policy enforcement + `_rdlt_load_id` stamping
only). New contract clause: structured streams declare themselves in `StreamSpec`,
carry no per-row identity, and reject `Merge` at build time (v1). Unblocks the
jsonl→DuckDB benchmark as a product claim and both parquet passthrough cells
(parquet→parquet published, parquet→DuckDB bonus).

## Technical Context

**Language/Version**: unchanged (Rust, workspace toolchain 1.96 / edition 2024)

**Primary Dependencies**: no new heavyweight deps — `parquet` (already
workspace-pinned 58.3) gains use in two more crates; `glob` crate added for pattern
matching (tiny, std-adjacent). Everything else inherited from feature 001.

**Storage**: local filesystem (source reads; parquet destination writes with
temp-dir staging + atomic rename publication + JSON state/receipt files).

**Testing**: unchanged (nextest, conformance suites; no containers needed); both new
connectors must pass the public conformance suites (spec FR-004/FR-011).

**Target Platform / Project Type**: unchanged.

**Performance Goals**: jsonl→DuckDB ≥10× via the bundled source (SC-003);
parquet→parquet ≥2× engine-bound (SC-004); parquet→DuckDB bonus row.

**Constraints**: unchanged (byte-bounded memory, exactly-once visibility, no silent
failures). New: passthrough must not copy batch data (schema check + one appended
constant column only).

**Scale/Scope**: +2 connector crates, ~4 engine files touched, 1 contract amendment,
3 benchmark rows.

## Constitution Check

Unchanged from feature 001: `.specify/memory/constitution.md` is still an unratified
template — **gate PASS (vacuous)**, checked against the working-principles table in
[001's plan](../001-rdlt-ingestion-engine/plan.md). This feature touches
`rdlt-connector` (semver-sacred): the `StreamSpec.structured` field addition is
**additive** (serde-default; pre-1.0) — the PR's now-blocking semver-checks job
verifies this claim against `origin/main`.

*Post-Phase-1 re-check: design adds no platform-scope creep; the two new crates are
connectors (SPI-only), consistent with 001's structure decision. PASS.*

## Project Structure

### Documentation (this feature)

```text
specs/002-file-arrow-ingestion/
├── spec.md, plan.md, research.md, data-model.md, quickstart.md
├── contracts/
│   ├── spi-amendments.md      # StreamSpec.structured; clauses S7/E7/B4; Merge rejection
│   └── file-connectors.md     # file-source config + cursor format; parquet-dest layout
└── checklists/requirements.md
```

### Source Code (delta on feature 001's tree)

```text
crates/
├── rdlt-source-file/        # NEW: JSONL + Parquet source (glob, per-file cursors)
│   └── src/{lib.rs, config.rs, cursor.rs, jsonl.rs, parquet.rs}
├── rdlt-dest-parquet/       # NEW: minimal write-only parquet destination
│   └── src/lib.rs
├── rdlt-connector/          # StreamSpec.structured field (additive)
├── rdlt-engine/
│   └── src/
│       ├── runtime/graph.rs       # Arrow arm → passthrough path
│       └── shred/passthrough.rs   # NEW: arrow schema → TableSchema, load-id stamping
├── rdlt/                    # features += ["file", "parquet"]; build-time Merge rejection
└── rdlt-cli/                # source.file + destination.parquet TOML arms
benches/                     # jsonl product cell; parquet→parquet; parquet→DuckDB bonus
```

**Structure Decision**: connectors stay SPI-only (feature 001 rule: "if you need
engine internals, the SPI is wrong"). Passthrough lives inside the engine deep module
(`shred/passthrough.rs`) — it is the shredder's sibling fast path and shares the
registry/policy seam, so schema policies behave identically for both stream kinds.

## Complexity Tracking

No gate violations. One deliberate scope guard: the parquet destination is
benchmark/export-grade (append/replace, no merge, no partitioning) — richer layouts
are explicitly out of scope (spec Assumptions).
