# A PokeAPI source, in Rust

A source connector of [PokeAPI](https://pokeapi.co), a free API of Pokémon data, written as a
connector of any HTTP API is: a configuration, streams with a key, a cursor checkpointed after
each page, and failures told apart so the engine retries the ones that pass. It is certified in
its tests, against a local server.

| File | What it is |
|---|---|
| [`src/lib.rs`](src/lib.rs) | The connector: its configuration, its two streams and their cursor |
| [`src/client.rs`](src/client.rs) | Its HTTP client, and what each failure is to the engine |
| [`src/main.rs`](src/main.rs) | A program that loads both streams into SQLite in process |
| [`src/bin/rdlt-connector-pokeapi.rs`](src/bin/rdlt-connector-pokeapi.rs) | The connector as its own binary, which a host spawns |
| [`rdlt.yaml`](rdlt.yaml) | A pipeline document that places that binary and grants it the network |
| [`tests/it`](tests/it) | Its certification and tests, against [a local server](tests/it/server.rs) |

## The connector

Its id is `io.example.pokeapi`, and its configuration's JSON Schema says what each field means:

| Field | Default | Means |
|---|---|---|
| `base_url` | `https://pokeapi.co/api/v2` | The API's root |
| `page_size` | 50 | How many entries each list request asks for, from 1 to 100 |
| `limit` | 151 | The most rows each stream reads in all its runs together, or `null` for every one |

- **`pokemon`** reads the list `/pokemon?offset=&limit=`, then each entry's `/pokemon/{id}`. Its
  key is `id`. Each row keeps the pokemon's `id`, `name`, `height`, `weight` and
  `base_experience`, which the API leaves null for some, and its `types`, `abilities` and
  `stats` as nested JSON, which a pipeline can keep or normalize into child tables.
- **`types`** reads the list `/type?offset=&limit=`, each row an `id` read from the entry's URL
  and a `name`. Its key is `id`.
- **The cursor** of each is `{"offset": n}`, the list offset its next page starts at. A stream
  checkpoints after each page, so an incremental read resumes after the last page a run
  committed. It plans the default partition, one that never ends: with `limit: null`, a later run
  reads on from there to what the API has added since, and with a limit, a stream that has read
  that many rows reads nothing more.
- **Failures** are the kinds the engine decides a retry by:

  | The API answers | The error | The engine |
  |---|---|---|
  | 429 | `RateLimited`, with the wait its `Retry-After` gives, in seconds or as an HTTP date, or 1 s | Waits, then retries |
  | 5xx, a timeout, a refused connection, a response cut short | `Transient` | Retries |
  | 401, 403 | `Auth` | Ends the run |
  | Any other 4xx | `Config`, naming the status and the path | Ends the run |
  | A body of another shape | `Data` | Ends the run |

- **It asks gently:** it names itself `rdlt-example-pokeapi` in its `User-Agent`, holds one
  request in flight across both streams, gives each request 30 s, its body read, and stops at the
  list's end or at `limit`, whichever comes first. PokeAPI is free and asks to be used gently, so
  the defaults read 151 pokemon, about 160 requests a run, one at a time.
- **It stops at once:** each request selects on the emitter's `stopped()`, so a read the engine
  asks to stop drops the request in flight and ends, rather than reading the rest of its page.

## Running it

### In process

[`src/main.rs`](src/main.rs) loads both streams into SQLite, the connector and the destination
compiled in. Both are read incrementally and merged by their key. Run it twice from
`examples/rust`, naming the database, in a directory only you may write, as the SQLite
destination requires, and, optionally, another root than PokeAPI's:

```console
$ cd examples/rust
$ mkdir -p /tmp/pokeapi-rust
$ cargo run -q -p pokeapi -- /tmp/pokeapi-rust/pokeapi.db
Succeeded: committed 172 rows
$ cargo run -q -p pokeapi -- /tmp/pokeapi-rust/pokeapi.db
Succeeded: committed 0 rows
```

The first run commits the first 151 pokemon and the 21 types the API lists. The second commits
nothing: each stream resumes after the last page the first committed.
`sqlite3 /tmp/pokeapi-rust/pokeapi.db .tables` shows `pokemon` and `types` beside the
destination's bookkeeping, the `_rdlt_*` tables. A pokemon's `types`, `abilities` and `stats` land
as JSON in its row; `.nested(Nested::Normalize { max_depth: 1 })` on the `pokemon` stream lands
them in child tables instead, as the document below does. The program exits 0 once its run
succeeds, and 1, printing the error, where the pipeline cannot be built or its run fails.

### As its own binary, from the command line

A connector's binary serves it: its `main` calls `rdlt_connector::serve` and nothing else. A host
finds it by its name, `rdlt-connector-pokeapi`, in its connectors' directory, and spawns it.
[`rdlt.yaml`](rdlt.yaml) is the program's pipeline as a document, loading into
`/tmp/pokeapi/pokeapi.db`, with each pokemon's nested lists normalized into the child tables
`pokemon__types`, `pokemon__abilities` and `pokemon__stats`. It asks for the network for the
source, with `access: { network: true }`, which the host allows with `--allow-network`. With
`rdlt` and the reference connectors installed as the repository's README
[shows](../../../README.md#from-the-command-line), from `examples/rust`:

```sh
cargo build -p pokeapi --bin rdlt-connector-pokeapi
cp ../../target/debug/rdlt-connector-pokeapi "${XDG_DATA_HOME:-$HOME/.local/share}/rdlt/connectors/"
mkdir -p /tmp/pokeapi
rdlt run pokeapi/rdlt.yaml --writable /tmp/pokeapi --allow-network
```

On Linux both connectors run in bubblewrap, the source with the host's network. macOS has no
sandbox: pass `--trust-binaries` there instead of `--writable`, which runs the connectors with
your own access.

## Certification and tests

The tests run against [a local server](tests/it/server.rs) that generates pages in the API's
shape, as many as each test asks, and answers a path once with a failure or a body where a test
says. No test reaches the network. From `examples/rust`, `cargo nextest run -p pokeapi` runs them:

- **Certification** in process, `rdlt_connector::testing::certify_source`, passes S-CHECK,
  S-DISCOVER, S-PLAN, S-RESUME and S-STOP. S-PARTITION does not apply, as each stream plans a
  partition that never ends; nor S-BARRIER, as a stream checkpoints after its pages and not on
  demand; nor S-ACK, as the source keeps no position outside the engine. The Python source is
  certified through the protocol and its kill clause too.
- **Configuration:** its published JSON Schema gives each field's default and `page_size`'s
  range.
- **Failures:** each status above is the error of its kind, a 429 that names an HTTP date asks for a
  wait until then, a refused connection and a response cut short are `Transient`, and a body that is
  not JSON, or JSON of another shape, is `Data`, a count that is text, negative, a float or past
  `u64`'s largest, or an id that is no run of digits, among them, which ends a run with no retry; a
  page that names no `next` is no error. A timeout is `Transient` as a refused connection is, and no
  test waits for one: the timeout is a fixed 30 s, and a field only a test would set is no part of
  an example.
- **Runs:** the program prints what the console block above shows, which the test reads from this
  README: 151 pokemon and 21 types, then nothing, each pokemon's nested lists as JSON in its row,
  with never more than one request at the server at once. With a 500, a response cut short and a
  429 on its way, which the server checks it gave, it loses no row. A run that fails exits 1.
- **Stopping:** a run asked to stop while the server holds a detail request for a minute ends
  within the test's 20 s.
- **The document** spawns the binary in the sandbox, granted the network, against the local
  server's address, and lands each pokemon's nested lists in child tables. Where no sandbox can
  be made it is skipped, but in CI on Linux, where it fails.

Next: the repository README's [Certifying a connector](../../../README.md#certifying-a-connector)
and [Placing connectors](../../../README.md#placing-connectors); the same source in Python is
[`python/pokeapi`](../../python/pokeapi).
