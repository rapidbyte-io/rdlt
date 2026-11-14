# A PokeAPI source, in Python

The [Rust PokeAPI source](../../rust/pokeapi), written as a Python connector class on the Rust
SDK: the same id, `io.example.pokeapi`, configuration, streams, cursor and failures, in the Rust
SDK's names. It reads with the standard library's `urllib.request`, so it needs nothing the
package does not. It is certified in its tests, against a local server.

| File | What it is |
|---|---|
| [`pokeapi.py`](pokeapi.py) | The connector, which serves itself as a connector binary does when run as a program |
| [`main.py`](main.py) | A program that loads both streams into SQLite, the connector in process |
| [`test_pokeapi.py`](test_pokeapi.py) | Its certification and tests, against [a local server](local_api.py) |

## The connector

- **Configuration:** `PokeApiConfig`, a pydantic model whose JSON Schema is published:
  `base_url` (by default `https://pokeapi.co/api/v2`), `page_size` (1 to 100, by default 50) and
  `limit`, the most rows each stream reads in all its runs together (by default 151, or `None`
  for every row; with a limit, a stream that has read that many reads nothing more).
- **Streams:** `pokemon` reads the list `/pokemon?offset=&limit=`, then each entry's
  `/pokemon/{id}`, keeping its `id`, `name`, `height`, `weight` and `base_experience`, and its
  `types`, `abilities` and `stats` as nested JSON. `types` reads the list `/type?offset=&limit=`,
  each row an `id` read from the entry's URL and a `name`. Each is keyed by `id`.
- **The cursor** of each is `Position`, `{"offset": n}`: the list offset its next page starts at,
  checkpointed after each page.
- **What it reads** is parsed strictly into pydantic models, `Page`, `Entry` and `Detail`, as the
  Rust source parses it into structs: no text or float is taken for a count, none is negative or
  past `u64`'s largest, an id is a run of digits, and a page that names no `next` is the last.
- **Failures:** a 429 raises `RateLimited` with the wait its `Retry-After` gives, in seconds or as
  an HTTP date, or 1 s; a 5xx, a timeout, a
  refused connection or a broken response `Transient`, which the engine retries; a 401 or 403
  `Unauthorized`; any other 4xx `InvalidConfig`, naming the status and the path; a body that is
  not JSON, or JSON of another shape, `InvalidData`.
- **It asks gently:** `User-Agent: rdlt-example-pokeapi`, one request in flight across both
  streams, whose reads run on threads of their own, 30 s for each wait on a request's socket
  (`urlopen`'s timeout, which bounds each wait, not the whole request), and a page loop that stops
  at the list's end or at `limit`.
- **It stops between requests:** each read asks `ctx.stopped()` before each request and returns
  where the engine asked it to stop. A request in flight blocks its thread, since `urllib`'s call
  cannot be interrupted, and its timeout bounds each wait. Naming `ctx` declares that the read
  checkpoints on demand, so it answers a barrier, `ctx.checkpoint_due()`, with a checkpoint before
  its next page.

## Running it

[`main.py`](main.py) places the connector in process with `rdlt.local` and loads both streams
into `pokeapi.db` through the reference SQLite connector, installed as the README's
[From the command line](../../../README.md#from-the-command-line) shows. Both are read
incrementally and merged by their key. Run it twice from the repository's root, with the
connectors' directory, a directory for the database and, optionally, another root than PokeAPI's:

```console
$ mkdir -p /tmp/pokeapi-python
$ .venv/bin/python examples/python/pokeapi/main.py "${XDG_DATA_HOME:-$HOME/.local/share}/rdlt/connectors" /tmp/pokeapi-python
succeeded: committed 172 rows
$ .venv/bin/python examples/python/pokeapi/main.py "${XDG_DATA_HOME:-$HOME/.local/share}/rdlt/connectors" /tmp/pokeapi-python
succeeded: committed 0 rows
```

The first run commits the first 151 pokemon and the 21 types the API lists, and the second nothing,
as each stream resumes after the last page the first committed. A build or a run that fails raises
its `rdlt.Error`, so the program exits 1, printing the error's traceback. The source runs in this
process, with its access; on Linux the SQLite connector runs in bubblewrap, which may write in the
database's directory alone. A pokemon's `types`, `abilities` and `stats` land as JSON in its row;
`nested={"normalize": {"max_depth": 1}}` on the `pokemon` stream lands them in child tables instead.

Run as a program, `pokeapi.py` serves its class, `serve(PokeApi)`, as a connector binary does: a
host spawns it through a launcher, as the certification's kill clause does below.
`rdlt connector new --lang python` makes a project that installs such a connector, as the
README's [Spawned connectors](../../../README.md#python) says.

## Certification and tests

The tests run against [a local server](local_api.py) that generates pages in the API's shape, as
many as each test asks, and answers a path once with a failure or a body where a test says. No
test reaches the network. `just python-test examples/python/pokeapi` runs them:

- **Certification**, `rdlt.testing.certify_source`, serves the class over a socket in the test's
  process and spawns its launcher for the kill clause. It passes the protocol's seven clauses,
  S-CHECK, S-DISCOVER, S-PLAN, S-RESUME, S-STOP, S-BARRIER and K-SOURCE. S-PARTITION and S-ACK do
  not apply, as for the Rust source. S-BARRIER applies here and not to the Rust source: a Python
  read that names `ctx`, as these do to see a stop, checkpoints on demand, and answers a barrier
  before its next page. The kill clause observes a kill only in a load that outlasts several
  commits, so the server lists 40,000 types for it.
- **Failures:** each status is the error of its kind, a 429 that names an HTTP date asks for a wait
  until then, a refused connection and a response cut short are `Transient`, a body that is not
  JSON, or JSON of another shape, is `InvalidData`, a count that is text, negative, a float or past
  `u64`'s largest, or an id that is no run of digits, among them, and a page that names no `next` is
  the last. A timeout is `Transient` as a refused connection is, and no test waits for one: the
  timeout is a fixed 30 s, and a field only a test would set is no part of an example.
- **Runs:** the program prints what the console block above shows, which the test reads from this
  README: 151 pokemon and 21 types, then nothing, each pokemon's nested lists as JSON in its row,
  with never more than one request at the server at once. With a 500, a response cut short and a
  429 on its way, which the server checks it gave, it retries and loses no row. A run that fails
  exits 1.

Next: the repository README's [Writing a connector](../../../README.md#writing-a-connector); then
[transforms](../transforms), a stream reshaped before it lands.
