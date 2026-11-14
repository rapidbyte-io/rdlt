# Examples

Programs to run and copy, from a first pipeline to a connector of a real HTTP API, a stream
reshaped, a destination and a run watched as it goes. Each teaches one thing, runs with the
command its README gives, and is tested on every change.

| Example | Rust | Python | Shows |
|---|---|---|---|
| Quick start | [`rust/quickstart`](rust/quickstart) | [`python/quickstart`](python/quickstart) | The smallest pipeline, into SQLite. Run twice, the second run resumes where the first ended and commits nothing. |
| PokeAPI | [`rust/pokeapi`](rust/pokeapi) | [`python/pokeapi`](python/pokeapi) | A source connector of [PokeAPI](https://pokeapi.co): its configuration, two streams with a key, a cursor checkpointed after each page, failures the engine retries, and its certification. It runs in process, and the Rust one also as its own binary, which a host spawns in the sandbox. |
| Transforms | [`rust/transforms`](rust/transforms) | [`python/transforms`](python/transforms) | Orders reshaped before they land: a column renamed, quantities cast, totals computed, test orders filtered out, emails masked and a column added, by ops and SQL in Rust and by Python functions in Python, into the same table; then a load a transform refuses, and how that is reported. |
| Destination | — | [`python/destination`](python/destination) | A destination that keeps the whole contract: every write mode, merged in Rust, fenced by epoch, its writers called at once, and certified with every destination clause. |
| Observing | [`rust/observing`](rust/observing) | [`python/observing`](python/observing) | A bulk load watched: its events as they happen, its report, and in Rust its metrics, once it ends; then a stop after a commit, asked at Ctrl-C or from code, and a run that reads on from it. |

Read them in that order, then the README's [Writing a connector](../README.md#writing-a-connector)
and [Certifying a connector](../README.md#certifying-a-connector). Where an example is in both
languages, the two read side by side, since the Python SDK uses the Rust SDK's names: the PokeAPI
sources have the same streams, configuration and cursor, and the transforms land the same table.

A destination keeps a larger contract than a source and is no small example. The Python one is
the warehouse, which keeps its rows in one sqlite3 file. In Rust the reference destinations are
the models, [`sqlite.rs`](../crates/rdlt-connector-reference/src/sqlite.rs) on the SQL planner
`sqlgen` and [`files/destination.rs`](../crates/rdlt-connector-reference/src/files/destination.rs)
on the Arrow merge.

## Running them

- **Rust** needs Rust 1.98 or newer and a C compiler, as the [quick start](../README.md#quick-start)
  says. The examples are a Cargo workspace of their own, run from `examples/rust` with
  `cargo run -p <example>`, and they build into the repository's `target` directory: run from
  elsewhere, with `--manifest-path`, Cargo does not read `examples/rust/.cargo/config.toml` and
  builds them all again in `examples/rust/target`.
- **Python** needs the package built into the repository's environment, `.venv`, by
  `just python-develop`, and runs from the repository's root with `.venv/bin/python`. Its
  examples use the reference connectors, installed as
  [From the command line](../README.md#from-the-command-line) shows: the SQLite one, and the
  memory and generator ones, which that install's `test-connectors` feature builds. On Linux they
  run in bubblewrap.

Each example's README gives its command, any argument it takes and what it prints, which its
test checks it prints:

| Example | Command |
|---|---|
| Rust quick start | `cd examples/rust && mkdir -p /tmp/quickstart && cargo run -q -p quickstart -- /tmp/quickstart/quickstart.db` |
| Rust PokeAPI | `cd examples/rust && mkdir -p /tmp/pokeapi-rust && cargo run -q -p pokeapi -- /tmp/pokeapi-rust/pokeapi.db` |
| Rust transforms | `cd examples/rust && export ORDERS_KEY=example-key && mkdir -p /tmp/transforms-rust && cargo run -q -p transforms -- /tmp/transforms-rust/orders.db` |
| Rust observing | `cd examples/rust && mkdir -p /tmp/observing-rust && cargo run -q -p observing -- /tmp/observing-rust/observing.db 3` |
| Python quick start | `.venv/bin/python examples/python/quickstart/main.py ~/.local/share/rdlt/connectors .` |
| Python PokeAPI | `mkdir -p /tmp/pokeapi-python && .venv/bin/python examples/python/pokeapi/main.py ~/.local/share/rdlt/connectors /tmp/pokeapi-python` |
| Python transforms | `export ORDERS_KEY=example-key && mkdir -p /tmp/transforms-python && .venv/bin/python examples/python/transforms/main.py ~/.local/share/rdlt/connectors /tmp/transforms-python` |
| Python destination | `mkdir -p /tmp/destination-python && .venv/bin/python examples/python/destination/main.py ~/.local/share/rdlt/connectors /tmp/destination-python` |
| Python observing | `mkdir -p /tmp/observing-python && .venv/bin/python examples/python/observing/main.py ~/.local/share/rdlt/connectors /tmp/observing-python 3` |

Run twice, the quick starts and the PokeAPI programs load once and then commit nothing, and the
destination merges the same rows over themselves; the observing programs read on from where the
last run stopped. The SQLite destination keeps a database only in a directory no other user may
write, so each command names a directory of its own rather than `/tmp` itself. The PokeAPI
programs read the live API; a URL after the database or its directory reads another root
instead.

## How they are tested

- **Rust:** `just examples` checks the workspace's formatting, runs clippy with every warning an
  error, and runs its tests, which CI's `examples` job does on Linux with the sandbox.
  `cargo xtask lint` holds the examples to the code standard, and `cargo xtask deny` checks their
  lockfile with every other.
- **Python:** pytest collects `examples/python` with the package's own suite, in `just python-test`
  and in CI's test and wheel jobs; ruff and pyright check them in `just lint`.
- **No test reaches the network.** The PokeAPI tests run against a local server that generates
  pages in the API's shape, at the size each test asks.
- **A README that shows code shows the example's own code,** and a test fails where they differ.
- **Each console block is checked:** the program run as the block runs it prints what the block
  shows. An observing run's counts and times depend on how fast it reads, so its test holds each
  commit line, printed and shown, to its form and its place: as many before the stop note as the
  count names, and any others between the note and the run's end. Every other line is the
  block's, digits and all, but the report's and the counters of what landed, whose numbers must
  add up to the commits', in the block as in the run's output, and to the table's.
