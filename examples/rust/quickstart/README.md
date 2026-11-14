# Quick start, in Rust

The smallest pipeline: `LogSource`, seeded offset logs like a message queue's, loaded into SQLite
through `SqliteDestination`, both compiled into the program. It reads its one stream
incrementally, so each run starts where the last committed one ended.
[`src/main.rs`](src/main.rs) is the whole of it, and the repository's README shows it as its Rust
quick start, which a test holds to this code.

Run it twice from `examples/rust`, naming the database, in a directory only you may write, as
the SQLite destination requires. The first run builds rdlt and its dependencies, which `-q`
keeps quiet:

```console
$ cd examples/rust
$ mkdir -p /tmp/quickstart
$ cargo run -q -p quickstart -- /tmp/quickstart/quickstart.db
Succeeded: committed 1000 rows
$ cargo run -q -p quickstart -- /tmp/quickstart/quickstart.db
Succeeded: committed 0 rows
```

The second run commits nothing: each partition resumes from the cursor the first run committed
into the database in the same transaction as its rows.
`sqlite3 /tmp/quickstart/quickstart.db .tables` shows the `events` table beside the destination's
bookkeeping, the `_rdlt_*` tables. The program exits 0 once its run succeeds, and 1, printing the
error, where the pipeline cannot be built or its run fails.

[`tests/it/main.rs`](tests/it/main.rs) runs the program twice and checks that it prints
what the console block above shows, and the repository's README's too, which shows the program
whole.

Next: [the PokeAPI source](../pokeapi), a connector of a real HTTP API.
