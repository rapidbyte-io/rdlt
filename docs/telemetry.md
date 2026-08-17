# Telemetry

rdlt's telemetry is a LIBRARY property. The CLI's live display, its
summary table, and its `--events` NDJSON are all consumers of the
same three seams any embedder gets — nothing the CLI shows is
computed anywhere but the library.

## The three seams

### 1. Events — the domain feed

`Pipeline::events()` returns a stream of `PipelineEvent`: facts about
data movement, serde-serializable (`{"event": "batch_loaded", ...}`),
`#[non_exhaustive]` so new variants are additive. Events are
ADVISORY: dropping them changes nothing about the load, and a
consumer that lags loses the OLDEST events rather than being allowed
to slow the pipeline — a still-connected consumer must not infer a
complete feed. The run report (`report::Run`) remains the complete record either
way.

| event | carries | meaning |
|---|---|---|
| `run_started` | load_id, resumed_from | The first event of an attempt that reaches identification: which load this is, and whether it resumed (fresh / cursor / WAL replay). Replayed recovery work itself emits nothing — it belongs to the crashed load; `resumed_from: wal` is its record. |
| `stream_started` / `stream_finished` | stream (+ `table` on start: the destination ROOT table its rows land in, the engine's normalization applied) | Read-side lifecycle. Nested payloads may shred into further child tables beyond the announced one. |
| `batch_read` | stream, rows, bytes | What the SOURCE delivered, before batching/merges/discards. Bytes are the raw payload for JSON sources, the Arrow footprint for structured ones. |
| `batch_loaded` | table, rows, bytes | A batch reached the destination (not yet committed). Bytes are the Arrow IN-MEMORY footprint. |
| `schema_evolved` | delta | Always precedes the first batch at the new version. |
| `commit_started` | commit_seq | Paired with `committed`, makes commit latency observable. |
| `committed` | commit_seq, cursors | Durable; follows everything it covers. |
| `part_closed` | table, encoded_bytes, reason | A destination closed one output file. `encoded_bytes` is the EXACT on-the-wire size — the only honest basis for output throughput (in-memory bytes differ from encoded by multiples). Reasons: `target`, `time`, `budget`, `commit`, `schema`. Emitted by the file-materialising destinations (file, iceberg, snowflake's staged uploads); postgres and duckdb never emit it. |
| `retried` | stream?, attempt | An engine retry of a transient failure. Announces the UPCOMING attempt, so it precedes that attempt's `run_started`. |
| `discarded` | table, rows, values, reason | Data dropped under a Discard* policy — counted, never silent. |
| `heartbeat` | elapsed_ms | A liveness tick (1 s) once the streams are wired (discovery, session open and WAL recovery precede the ticker). The first beat fires synchronously at wiring itself, before the 1 s ticker is even spawned, so every identified run carries at least one: events may legitimately go quiet, heartbeats may not. |

Sensitivity: `committed` carries each commit's CURSORS, and the
`report::Run` carries the final ones. Cursor values are source-defined —
an offset is harmless, a resume token or signed continuation URL is
not — so anything that persists the feed or the report (the CLI's
`--events` NDJSON file, `--report`/stdout JSON, a CI artifact store)
holds state-store material and deserves the same handling.

Causal-order guarantees: `run_started` first (within an attempt — a
`retried` announcing the next attempt precedes ITS `run_started`, and
an attempt that fails before identification emits only that
`retried`); a stream's `batch_read`
precedes the `batch_loaded` carrying those rows; `schema_evolved`
precedes the first batch at its version; `commit_started` precedes
its `committed`; `committed` follows everything it covers. One
honest asymmetry: a `commit_started` whose attempt then failed has no
matching `committed` — the next attempt runs under a new load id.

### 2. `Metrics` — the canonical fold

`rdlt::Metrics` folds the event stream into live numbers ONCE, so
every consumer shows the same rows/s:

```rust,ignore
use rdlt::metrics::Snapshot;
use rdlt::prelude::*;

let mut events = pipeline.events();
let mut metrics = Metrics::new();
tokio::spawn(async move {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => metrics.apply(&event),
                None => break,
            },
            _ = tick.tick() => {
                let snap: Snapshot = metrics.snapshot();
                // serve it, log it, ship it — it's plain serializable data
                println!("{} rows/s", snap.rows_per_sec.unwrap_or(0.0));
            }
        }
    }
});
```

`metrics::Snapshot` carries per-stream read totals, the stream→table
mapping the engine announced, per-table write and output totals,
sliding-window and cumulative rates, commit recency, and whether a
commit is in flight. The fold is advisory like the feed
that drives it: FINAL totals belong to the `report::Run` — the
exactly-once record — and a consumer showing final numbers must take
them from there.

### 3. `tracing` — the diagnostic spans

The engine instruments with [`tracing`]; attach any subscriber
(fmt, OTel bridge, journald) and the spans arrive. The contract —
names and fields are frozen once published, additions are additive:

| span | fields | covers |
|---|---|---|
| `rdlt.run` | `rdlt.pipeline`, `rdlt.load_id` (recorded once minted), `rdlt.attempt` | One run attempt, root of everything below. |
| `rdlt.extract` | `stream` | One stream's read/shred task. |
| `rdlt.shred` / `rdlt.passthrough` | — | CPU work inside extract. |
| `rdlt.load` | — | The loader task: WAL, destination writes, commits. |

Spans bind to FUTURES, not thread guards, so concurrent streams never
attribute each other's work.

## The report's telemetry fields

`report::Run` (format v1, additions additive with `#[serde(default)]`):
per-stream `streams.{rows_read, bytes_read}`, per-table
`tables.{output_bytes}` (zero where no files were written), and
`rows_per_sec_avg`. These are counted at the source and in the part
callbacks — never folded from the droppable event bus — so they are
exact.

## What rdlt deliberately does not ship

No metrics push, no Prometheus endpoint, no OpenTelemetry dependency,
no ETA estimation (no source can promise totals, and a fake ETA is
worse than none). The library exposes the fold and the spans;
transports and dashboards belong to the products built on top.
