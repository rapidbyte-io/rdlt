# Contract Amendments: Connector SPI (feature 002)

**Amends**: [feature 001 connector-spi.md](../../001-rdlt-ingestion-engine/contracts/connector-spi.md)
— all existing clauses stand; this file adds the structured-stream clauses. Fold into
the base contract when feature 002 merges.

## StreamSpec (additive field)

```rust
pub struct StreamSpec {
    // …existing fields…
    /// This stream pushes already-structured Arrow batches (passthrough).
    /// serde-default false; additive change, verified non-breaking by semver-checks.
    pub structured: bool,
}
```

## New source obligation

| # | Clause |
|---|---|
| S7 | A source that pushes `arrow(batch)` on a stream MUST declare that stream `structured: true` in its `StreamSpec`. Pushing Arrow on an undeclared stream is a contract violation (the engine rejects it at runtime with a typed error). Mixed pushes (raw_json + arrow) on one stream are not supported in v1. |

## New engine guarantees / semantics

| # | Clause |
|---|---|
| E7 | Structured streams: batches bypass the shredder. The engine maps the batch's arrow schema to the logical schema (typed error naming the column for unmappable types — never silent coercion), enforces schema-change policies identically to record streams, appends run provenance (`_rdlt_load_id`) as the ONLY system column, and applies capability lowering. Row data is never copied or value-transformed. Delivery semantics: within the crash-recovery redelivery window, Append-mode structured streams are **at-least-once** (no per-row identity to dedup with) — documented, never silent. |

## New build-time validation (embedder API)

| # | Clause |
|---|---|
| B4 | `Merge` write mode on a stream declared `structured: true` fails at run-start planning (stream specs come from the async `streams()` call), strictly before the destination is opened with an error naming the stream and stating that merge requires per-row identity (`_rdlt_id`), which structured streams do not carry in v1. Append and Replace are permitted. |
