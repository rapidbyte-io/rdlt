# rdlt-engine

The deep module: shred → schema → WAL → load, orchestrated over
byte-bounded channels.

Everything below `lib.rs` is `pub(crate)` — unit-tested privately and free to
churn without semver cost. The public surface is `Engine` / `EngineConfig`
plus the vocabulary re-exported from
[`rdlt-core`](https://docs.rs/rdlt-core). Most users want the
[`rdlt`](https://docs.rs/rdlt) facade instead of depending on this directly.

## The stages

| Stage | Job |
|---|---|
| **shred** | JSON → Arrow, inferring and evolving the schema; nested objects become struct columns and nested collections become child tables |
| **schema** | the registry and contract enforcement: what changed, and whether policy permits it |
| **wal** | write-ahead log — durable intent before the destination sees a batch, so recovery replays instead of re-extracting |
| **load** | drives one `LoadSession` through ensure → write → commit, and owns the run's accounting |

## Backpressure is by bytes, not batches

Stage channels are bounded by BYTES. A batch-count bound would let peak
memory scale with schema width; a byte budget keeps it capped whatever the
rows look like. A slow consumer exhausts the budget and the producer parks on
it — that parking *is* the backpressure.

## Commits happen only at checkpoint boundaries

Committing mid-span would publish rows the committed cursor does not cover,
and a crash would then re-extract them as duplicates. So commits land at
source checkpoints, plus one final commit for trailing work. State travels
with the data in the same atomic commit — the property everything else rests
on.

## Nothing is silent

Rows a policy discards are counted and reported. A value that will not fit
its column is refused, not coerced. A retry is bounded and recorded. If the
run did less than you asked, the `RunReport` says so.
