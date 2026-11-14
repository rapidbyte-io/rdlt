# Transforms, in Rust

A shop's orders reshaped before they land in SQLite. Transforms run on each batch, in the order a
stream lists them, before the engine fits the batch to its table, so the table holds what they
make of it. [`src/main.rs`](src/main.rs) is the whole of it.

The orders come inline, to the reference `MemorySource`, each with a quantity as text and one of
them a test order; their emails are made up, on the reserved domain `example.com`. The `orders`
stream lists four steps:

| Step | In code | What it does here |
|---|---|---|
| Rename | `TransformStep::rename` | Gives `qty` its table's name, `quantity` |
| SQL | `TransformStep::Sql`, the query `PRICED` | Casts each `quantity` to an integer, computes `total_cents`, and keeps the orders whose `status` is not `test` |
| Mask | `TransformStep::Mask`, with `MaskWith::HmacSha256` keyed by `${env:ORDERS_KEY}` | Writes each email as its HMAC-SHA256, in hex: the same email is the same text in every load, and no email is kept |
| Constant | `TransformStep::constant` | Adds a column, `currency`, holding `EUR` in every row |

- **Rename, mask and constant are ops:** data, bounded and checked when the pipeline is built, so
  they are safe in a tenant's configuration.
- **There is no cast or filter op.** SQL carries both: one query of the table `input`, projected
  and optionally filtered, built with the facade's `sql` feature, which `Cargo.toml` turns on.
- **The mask's key is a secret reference,** `${env:ORDERS_KEY}`, never the key itself: the host
  resolves it, from the one variable `Host::allow_env` allows, when the pipeline is built.
- **A filter leaves no stale row:** this stream replaces its table on each load. A filter on a
  stream merged by key is refused when the plan is built, unless the stream reads changes and
  applies their deletes, which take away a row the filter stops keeping.

## Running it

From `examples/rust`, with a key for the mask, naming the database, in a directory only you may
write, as the SQLite destination requires:

```console
$ cd examples/rust
$ export ORDERS_KEY=example-key
$ mkdir -p /tmp/transforms-rust
$ cargo run -q -p transforms -- /tmp/transforms-rust/orders.db
Succeeded: committed 3 rows, 1 filtered out
id  quantity  total_cents  status    currency  email
1   2         2500         paid      EUR       925ab9f1e88ac03cf1328a8a0cc12c35736ea4dfa0f36f50f1bdb0a5a38de010
3   4         4000         paid      EUR       36163d280fedb0c05655abbddd3357865808c1541151a51ba22092576c04ba66
4   1         310          refunded  EUR       f6902a6fa51a54ee4367b72119f2f2977e4bf3ec9a1de4317952212b1b09f5a9
Failed: transform_failed, stream orders: transform sql failed on a batch: column quantity: evaluating the expression failed
id  quantity  total_cents  status    currency  email
1   2         2500         paid      EUR       925ab9f1e88ac03cf1328a8a0cc12c35736ea4dfa0f36f50f1bdb0a5a38de010
3   4         4000         paid      EUR       36163d280fedb0c05655abbddd3357865808c1541151a51ba22092576c04ba66
4   1         310          refunded  EUR       f6902a6fa51a54ee4367b72119f2f2977e4bf3ec9a1de4317952212b1b09f5a9
```

- **The first load** commits three orders, and its report's `filtered.dropped` counts the test
  order its filter dropped. The table holds what the steps made: each quantity an integer, each
  total computed, each email masked, and `currency` added.
- **The second load** carries one more order, whose quantity is `"two"`. The cast refuses it, and
  the run fails at once: a transform's failure is `ErrorKind::Transform`, coded `transform_failed`,
  and not retried, since a batch fails the same way every time. The error names the stream and the
  step, and its cause the column; its texts are bounded and show no value of the row. Nothing of
  the load lands, and the table keeps what the first load replaced it with.

The program exits 0 once both loads are told, and 1, printing the error, where the first fails or
its pipeline cannot be built: without `ORDERS_KEY`, the mask's key does not resolve, coded
`secret_unresolved`. `sqlite3 /tmp/transforms-rust/orders.db .schema` shows the table's columns,
what the steps made, and after them the engine's own, `_rdlt_load_epoch` and `_rdlt_loaded_at`.

## Tests

[`tests/it/main.rs`](tests/it/main.rs) runs the program with the key the console block exports
and checks that it prints what the block shows, and that the table's columns are the steps' and
then the engine's two; and that without the key it exits 1, coded `secret_unresolved`. From
`examples/rust`, `cargo nextest run -p transforms` runs them.

Next: [observing a run](../observing), its events, report and metrics, and a stop; the same
orders reshaped by Python's own code are [`python/transforms`](../../python/transforms). The
repository README's [Transforms](../../../README.md#transforms) says how keys, change streams and
partial updates pass them.
