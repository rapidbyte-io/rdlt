# A destination, in Python

A destination connector that keeps the whole contract: every write mode, merged by key in Rust,
fenced by epoch, its table writers called at once, and certified with every destination clause.
It keeps what it loads in one sqlite3 file. It is not small, as no destination that keeps the
contract is, but each part is one an author of any destination needs.

| File | What it is |
|---|---|
| [`warehouse.py`](warehouse.py) | The connector: its configuration, capabilities, sessions, table writers and commit; run as a program, it serves itself as a connector binary does |
| [`warehouse_store.py`](warehouse_store.py) | Its store: the sqlite3 tables it keeps, and what one transaction reads and writes of them |
| [`main.py`](main.py) | A program that merges generated rows into it, in process |
| [`test_warehouse.py`](test_warehouse.py) | Its certification and tests |

## The connector

`Warehouse`, `io.example.warehouse`, takes `WarehouseConfig`, the file's `path`.

- **What it can store** is its `capabilities()`: append, replace, merge and history; hard and soft
  deletes; partial updates and change merges; dropping tables; and four writers at once.
- **A session** is what `open(context)` answers for a pipeline: its epoch, one above every earlier
  open's, kept in a fence row, with the receipt of the last commit that landed and the pipeline's
  state. A session's writer buffers a table's batches and stages them on `flush()`, unless a newer
  session has opened since, which raises `Fenced`.
- **A commit** publishes what the session staged, in one transaction with its state and its
  receipt. `Fence(epoch, landed).decide(meta)` decides first, under the transaction's write lock:
  a commit that landed already is answered its receipt and publishes nothing, so one sent again
  never lands twice, and one a newer session fenced raises `Fenced`.
- **A merge table** is merged as a Rust destination merges one, by `rdlt.connector.merge`:
  `merge_sparse` runs Rust's merge over the Arrow it is handed, with the GIL released, keeping one
  row per key, and `read_back` gives a reader every column.
- **Writers at once:** every call works in a transaction of its own connection, which takes the
  file's write lock first, so its four writers' flushes need no lock of their own.
- **What it refuses:** a widened column, since a widen joins two types on the engine's lattice,
  which a Python destination does not hold; the engine then routes what the column cannot hold
  elsewhere. A table another pipeline owns is refused, coded `table_owned`.

## Running it

[`main.py`](main.py) places the warehouse in process with `rdlt.local` and merges the reference
generator's 1,000 rows into `warehouse.db` by their `id`, committing every 250 rows. It needs the
package, built into `.venv` by `just python-develop`, and the generator, installed with the
reference connectors as the README's
[From the command line](../../../README.md#from-the-command-line) shows. Run it twice from the
repository's root, with the connectors' directory and a directory for the warehouse's file:

```console
$ mkdir -p /tmp/destination-python
$ .venv/bin/python examples/python/destination/main.py "${XDG_DATA_HOME:-$HOME/.local/share}/rdlt/connectors" /tmp/destination-python
succeeded: committed 1000 rows, holding 1000
$ .venv/bin/python examples/python/destination/main.py "${XDG_DATA_HOME:-$HOME/.local/share}/rdlt/connectors" /tmp/destination-python
succeeded: committed 1000 rows, holding 1000
```

Each run reads every row again, and the second commits all 1,000 once more, merged over the first:
the table holds each `id` once. A build or a run that fails raises its `rdlt.Error`, so the program
exits 1, printing the error's traceback. On Linux the generator runs in bubblewrap, with no access;
the warehouse runs in this process, with yours.

Run as a program, `warehouse.py` serves its class, `serve(Warehouse)`, as a connector binary does:
a host spawns it through a launcher, as the certification's kill clause does below, and
`rdlt connector new --lang python` makes a project that installs such a connector, as the README's
[Spawned connectors](../../../README.md#python) says.

## Certification and tests

`just python-test examples/python/destination` runs them:

- **Certification**, `rdlt.testing.certify_destination`, serves the class over a socket in the
  test's process and spawns its launcher for the kill clause. It passes the protocol's clauses but
  P-CREDIT, which does not apply, since a destination grants credit and spends none, and every
  destination clause: D-CHECK through D-FENCE, and K-DESTINATION, which kills the process as it
  writes, before a commit and after one, and finds every row published exactly once.
- **Placed both ways:** a merge load keeps one row per key however often it runs, in process and
  spawned, and a replaced table reads back as its last load alone.
- **Fenced:** a session a newer one fenced claims no table its pipeline owns.
- **The program** prints what the console block above shows, which the test reads from this
  README, and the table holds each `id` once. A run that fails exits 1.

The package's own tests certify it too, listening over mutual TLS.

Next: [observing a run](../observing), its events, its report and a stop. For a destination in
Rust, the reference destinations are the models,
[`sqlite.rs`](../../../crates/rdlt-connector-reference/src/sqlite.rs) on the SQL planner `sqlgen`
and [`files/destination.rs`](../../../crates/rdlt-connector-reference/src/files/destination.rs) on
the Arrow merge.
