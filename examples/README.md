# Examples

Programs to run and copy, from a first pipeline to a connector of a real HTTP API. Each teaches
one thing, runs with the command its README gives, and is tested on every change.

| Example | Rust | Python | Shows |
|---|---|---|---|
| Quick start | [`rust/quickstart`](rust/quickstart) | [`python/quickstart`](python/quickstart) | The smallest pipeline, into SQLite. Run twice, the second run resumes where the first ended and commits nothing. |
| PokeAPI | [`rust/pokeapi`](rust/pokeapi) | [`python/pokeapi`](python/pokeapi) | A source connector of [PokeAPI](https://pokeapi.co): its configuration, two streams with a key, a cursor checkpointed after each page, failures the engine retries, and its certification. It runs in process, and the Rust one also as its own binary, which a host spawns in the sandbox. |

Read them in that order, then the README's [Writing a connector](../README.md#writing-a-connector)
and [Certifying a connector](../README.md#certifying-a-connector). The two PokeAPI sources have
the same streams, configuration and cursor, and read side by side, since the Python SDK uses the
Rust SDK's names.

A destination keeps a larger contract than a source and is no small example: the reference
destinations are the models, [`sqlite.rs`](../crates/rdlt-connector-reference/src/sqlite.rs) on
the SQL planner `sqlgen` and
[`files/destination.rs`](../crates/rdlt-connector-reference/src/files/destination.rs) on the
Arrow merge.

## Running them

- **Rust** needs Rust 1.98 or newer and a C compiler, as the [quick start](../README.md#quick-start)
  says. The examples are a Cargo workspace of their own, run from `examples/rust` with
  `cargo run -p <example>`, and they build into the repository's `target` directory: run from
  elsewhere, with `--manifest-path`, Cargo does not read `examples/rust/.cargo/config.toml` and
  builds them all again in `examples/rust/target`.
- **Python** needs the package built into the repository's environment, `.venv`, by
  `just python-develop`, and runs from the repository's root with `.venv/bin/python`. Its
  examples load into SQLite through the reference SQLite connector, installed as
  [From the command line](../README.md#from-the-command-line) shows; on Linux it runs in
  bubblewrap.

Each example's README gives its command, any argument it takes and what it prints, which its
test checks it prints. Run twice, each loads once and then commits nothing:

| Example | Command |
|---|---|
| Rust quick start | `cd examples/rust && mkdir -p /tmp/quickstart && cargo run -q -p quickstart -- /tmp/quickstart/quickstart.db` |
| Rust PokeAPI | `cd examples/rust && mkdir -p /tmp/pokeapi-rust && cargo run -q -p pokeapi -- /tmp/pokeapi-rust/pokeapi.db` |
| Python quick start | `.venv/bin/python examples/python/quickstart/main.py ~/.local/share/rdlt/connectors .` |
| Python PokeAPI | `mkdir -p /tmp/pokeapi-python && .venv/bin/python examples/python/pokeapi/main.py ~/.local/share/rdlt/connectors /tmp/pokeapi-python` |

The SQLite destination keeps a database only in a directory no other user may write, so each
command names a directory of its own rather than `/tmp` itself. The PokeAPI programs read the
live API; a URL after the database or its directory reads another root instead.

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
