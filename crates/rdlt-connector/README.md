# rdlt-connector

The in-process connector SPI: the `Source`, `Destination` and `LoadSession`
traits and their adjuncts. Vocabulary types come from
[`rdlt-core`](https://docs.rs/rdlt-core) (re-exported as `core`); this crate
adds only the traits.

## The contract these traits obey

- A **source** resumes from a committed cursor without re-emitting rows it
  already covered.
- A **destination** stages writes invisibly and publishes them atomically
  with pipeline state.
- Delivery is **at-least-once and idempotent per commit** — re-committing the
  same `(load_id, commit_seq)` returns the prior receipt without
  re-publishing.

None of that is enforced by the type system, so it is enforced by tests: the
public conformance suites in [`rdlt-testkit`](https://docs.rs/rdlt-testkit)
are the definition of "certified". A connector that passes them behaves; one
that does not, does not — regardless of what it claims.

## Semver-sacred

`cargo semver-checks` gates this crate. Traits are object-safe and every
exchange type is serde-serializable, so a future out-of-process or WASM host
can adapt this SPI over a wire without engine changes.

## Writing a connector

Implement `Source` (streams + a read that emits records and checkpoints) or
`Destination` (open a session; ensure tables, write batches, commit). Rows
reach the engine through `RecordsOut`, which offers three shapes:

| Method | Use when |
|---|---|
| `raw_json` | the payload is already JSON bytes — passed through byte-identical, no parse/reserialize |
| `rows` | you have `serde_json::Value` rows |
| `arrow` | you already have a `RecordBatch` |

`DestinationCapabilities` is how a destination declines what it cannot do
(native structs, decimals, merge) — the engine lowers the data to fit rather
than the destination failing at write time.
