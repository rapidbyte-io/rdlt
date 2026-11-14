# Transforms, in Python

The [Rust transforms example](../../rust/transforms)'s orders, reshaped by Python's own code: a
filter and a transform written as Python functions over pyarrow batches, between the same ops.
The table they land in is the Rust example's, row for row. [`main.py`](main.py) is the whole of
it.

The orders come inline to the reference memory connector, their emails made up, on the reserved
domain `example.com`, and the `orders` stream lists five steps, each reshaping what the step
before it leaves:

| Step | In `main.py` | What it does here |
|---|---|---|
| Rename | `{"rename": {"qty": "quantity"}}` | Gives `qty` its table's name |
| Filter | `@rdlt.filter()` on `real` | Keeps the orders whose `status` is not `test`, answering one boolean a row |
| Transform | `@rdlt.transform()` on `priced` | Casts each `quantity` to an integer and appends `total_cents` |
| Mask | `{"mask": {"columns": ["email"], "with": "hmac_sha256", "key": "${env:ORDERS_KEY}"}}` | Writes each email as its HMAC-SHA256, in hex |
| Constant | `{"constant": {"currency": "EUR"}}` | Adds a column holding `EUR` in every row |

- **The ops are a document's words,** as mappings, and run as the Rust ones do. The package has
  no SQL step: its native module is built without the facade's `sql` feature, so a Python
  function carries the cast and the filter.
- **A transform** answers the batch reshaped, the same rows in the same order. Its output's
  schema is what it answers for an empty batch. **A filter** answers which rows to keep, and the
  engine drops the others. Both run on the engine's compute pool, and take the GIL only inside
  their call.
- **The mask's key is a secret reference,** never the key itself: the host resolves it, from the
  one variable `allow_env` allows, when the pipeline is built.
- **A filter leaves no stale row:** this stream replaces its table on each load, and a filter on a
  stream merged by key is refused unless the stream reads changes and applies their deletes.

## Running it

It needs the package, built into `.venv` by `just python-develop`, and the reference memory and
SQLite connectors, installed as the README's
[From the command line](../../../README.md#from-the-command-line) shows. Run it from the
repository's root, with a key for the mask, the connectors' directory and a directory for the
database:

```console
$ export ORDERS_KEY=example-key
$ mkdir -p /tmp/transforms-python
$ .venv/bin/python examples/python/transforms/main.py "${XDG_DATA_HOME:-$HOME/.local/share}/rdlt/connectors" /tmp/transforms-python
succeeded: committed 3 rows, 1 filtered out
id  quantity  total_cents  status    currency  email
1   2         2500         paid      EUR       925ab9f1e88ac03cf1328a8a0cc12c35736ea4dfa0f36f50f1bdb0a5a38de010
3   4         4000         paid      EUR       36163d280fedb0c05655abbddd3357865808c1541151a51ba22092576c04ba66
4   1         310          refunded  EUR       f6902a6fa51a54ee4367b72119f2f2977e4bf3ec9a1de4317952212b1b09f5a9
failed: transform_failed, stream orders: transform priced failed on a batch: pyarrow.lib.ArrowInvalid: Failed to parse string: 'two' as a scalar of type int64
id  quantity  total_cents  status    currency  email
1   2         2500         paid      EUR       925ab9f1e88ac03cf1328a8a0cc12c35736ea4dfa0f36f50f1bdb0a5a38de010
3   4         4000         paid      EUR       36163d280fedb0c05655abbddd3357865808c1541151a51ba22092576c04ba66
4   1         310          refunded  EUR       f6902a6fa51a54ee4367b72119f2f2977e4bf3ec9a1de4317952212b1b09f5a9
```

- **The first load** commits three orders. The report's `filtered` counts the test order its
  filter dropped. Python's `StreamReport` types its rows, bytes and commits alone, where Rust's
  types `filtered` too, so the program reads it from `raw`, the stream's report as the facade
  serializes it.
- **The second load** carries one more order, whose quantity is `"two"`. `priced`'s cast raises,
  and the run fails at once: `pipeline.run()` raises `rdlt.TransformError`, coded
  `transform_failed` and not retried, since a batch fails the same way every time. It names the
  stream and the transform, and its first cause is the exception, as Python raised it; the next
  is its traceback. Nothing of the load lands, and the table keeps what the first load replaced it
  with.

A build or a run that fails otherwise raises its `rdlt.Error`, so the program exits 1, printing
the error's traceback: without `ORDERS_KEY`, the mask's key does not resolve, and building the
pipeline raises `rdlt.ConfigError` coded `secret_unresolved`. On Linux both
connectors run in bubblewrap, the SQLite one able to write in the database's directory alone.

## Tests

[`test_transforms.py`](test_transforms.py) runs the program with the key the console block
exports and checks that it prints what the block shows; that without the key the build is
refused, coded `secret_unresolved`, and the program exits 1. It
certifies `priced` and `real` with `rdlt.testing.certify_transform`, which runs the transform
clauses against each. Each passes T-REPLAY, T-ROWWISE, T-SCHEMA and T-ROWS: applied twice it
agrees with itself, a batch cut in two gives what the whole does, and it answers the schema it
declares, keeping its input's rows in their order. T-KEYS does not apply, as the orders name no
key, nor do T-BOUND, T-LINEAGE and T-READS, as neither declares a bound, a lineage or the columns
it reads. `just python-test examples/python/transforms` runs them.

Next: [a destination](../destination), the warehouse that keeps the whole contract; then
[observing a run](../observing), its events, its report and a stop.
