# Examples

Runnable pipelines. Both were executed exactly as written before being
committed — the row counts below are what they actually produced, not
what they ought to produce.

| example | needs | runs in |
|---|---|---|
| [`pokemon-to-jsonl`](pokemon-to-jsonl/) | a network connection | ~5 s |
| [`oracle-to-jsonl`](oracle-to-jsonl/) | an Oracle + client libraries | ~0.1 s for 250 rows |

Run one with:

```sh
rdlt run examples/<name>/pipeline.yaml
```

Each example is a SINGLE file. Every connector accepts its config
either inline — as these do — or as `config: <path>` pointing at a
separate YAML/JSON document with the identical shape:

```yaml
source:
  oracle:
    config: secrets/oracle.yaml     # the same document, kept apart
```

Inline keeps a small pipeline in one place; a path is better once a
document is shared between pipelines, or holds a credential you want
gitignored on its own. Mixing the two — `config:` alongside inline
keys — is refused, so half a document can never be silently ignored.

Paths in the pipeline files are relative to the repository root, so
run them from there. If you have not built the CLI yet:

```sh
cargo build --release -p rdlt-cli    # binary at target/release/rdlt
```

---

## `pokemon-to-jsonl` — REST → newline-delimited JSON

Reads every Pokémon from [PokéAPI](https://pokeapi.co) and writes it
to `examples/pokemon-to-jsonl/out/`. It needs no credentials and no
setup, which is why it is the one to try first.

```sh
rdlt run examples/pokemon-to-jsonl/pipeline.yaml
```

**Verified:** 1,351 rows, matching the `count` PokéAPI reports for the
same endpoint — so pagination followed every page rather than stopping
at the first. Running it a second time leaves 1,351, not 2,702:
`write_mode: replace` truncates rather than appends.

What the pipeline says:

- The `rest:` block is the source document. PokéAPI returns
  `{count, next, previous, results: [...]}`, so `records_path: results`
  says where the rows are, and the `next_url` paginator follows the
  fully-formed `next` link the API supplies. When an API gives you the
  next URL, following it beats doing arithmetic on `offset`/`limit`.
- Around it, `workdir` is where rdlt keeps its write-ahead log, and
  `write_mode` decides whether a re-run replaces, appends, or merges.

Output rows carry two engine columns beside your data:

```json
{"_rdlt_load_id":"19fc713958c-dbf6b-0","_rdlt_id":"0599eb7c…","name":"bulbasaur","url":"https://pokeapi.co/api/v2/pokemon/1/"}
```

`_rdlt_id` is the row identity used for deduplication and merges;
`_rdlt_load_id` says which load wrote the row.

---

## `oracle-to-jsonl` — Oracle → newline-delimited JSON

Reads an Oracle table and writes it to
`examples/oracle-to-jsonl/out/`. Unlike the Pokémon example this one
needs two things before it will run.

**1. Edit the `oracle:` block in `pipeline.yaml`.** Every value is a
placeholder — host, service, user, password, and the table name. The
password sits in the pipeline file, which is exactly the case for
moving it to `config: <path>` and gitignoring that document instead.

**2. Install Oracle Client libraries.** rdlt's Oracle source is built
on ODPI-C, which loads them at RUNTIME. Nothing is needed to *build*
rdlt; the requirement appears when a connection is opened, and its
absence is reported as `DPI-1047`.

Instant Client Basic Lite is a free download from Oracle under their
OTN licence (we cannot redistribute it):

```sh
# Linux x86-64; see Oracle's downloads page for other platforms.
curl -LO https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip
unzip instantclient-basiclite-linuxx64.zip
export LD_LIBRARY_PATH=$PWD/instantclient_23_8:$LD_LIBRARY_PATH
```

On Fedora/RHEL it also needs `libaio` (`sudo dnf install libaio`); on
Debian/Ubuntu, `libaio1`.

Then:

```sh
rdlt run examples/oracle-to-jsonl/pipeline.yaml
```

**Verified** against Oracle Free 23ai with a 250-row `EMPLOYEES`
table. A row comes out like this:

```json
{"_rdlt_load_id":"19fc71538ff-de58e-0","employee_id":151,"name":"employee-151","salary":51585.50,"hired":"2020-05-31T00:00:00","updated_at":"2026-08-03T10:04:34.844871Z"}
```

Three things in that row are worth noticing, because they are choices
rather than accidents:

- `salary` is `51585.50` — an exact decimal at its declared scale.
  `NUMBER(12,2)` crosses as a real decimal, not as a rounded float and
  not as a quoted string.
- `hired` has a **time component**. Oracle's `DATE` carries one, so it
  maps to a timestamp rather than a date; treating it as a date would
  silently drop the time.
- `updated_at` is UTC. `TIMESTAMP WITH TIME ZONE` is normalised to the
  instant, so a value stored as `+02:00` arrives as the UTC moment it
  denotes rather than its wall-clock face.

### Reading only what changed

Uncomment `cursor:` in the `oracle:` block and switch `write_mode` to
`merge`. Each run then reads only rows whose cursor column is above
the last checkpoint, and upserts them on `primary_key`.

The cursor column must be `NOT NULL`. That is enforced before any row
is read, and the reason is worth knowing: Oracle sorts NULLs last, so
a nullable cursor would deliver those rows once and then silently skip
them on every later run — the failure would look like success.

---

## Controlling how rows are grouped

Two DIFFERENT things decide the shape of the output, and it is worth
keeping them apart.

**How many rows land in each part FILE** is `batch_policy` — how much
the engine accumulates before each destination write. It is
destination-agnostic: the engine does the accumulating, so the same
setting means the same thing to a file, a table or a warehouse.

```yaml
batch_policy:
  every_rows: 50000
  every_bytes: 134217728      # … or 128 MB in memory, whichever first
```

`every_bytes` counts the ARROW IN-MEMORY footprint, not the bytes
written. It is a memory bound, not an output-size one. Arrow reports
allocated capacity — buffers grow geometrically, and per-value offsets
and validity bitmaps count too — and the output format is the
destination's business, JSONL text here but compressed parquet
elsewhere. Measured on the pokemon stream: `every_bytes: 100000` gave
400-row parts of ~73 KB. Use `every_rows` when you want a predictable
file size.

Both thresholds are FLOORS, not targets: a source batch is never
split, so accumulation stops at the first batch to cross the line.
With 100-row pages you get multiples of 100.

Measured on the pokemon example, whose source still pages 100 rows at
a time: without it, 14 parts of 100; with `every_rows: 600`, **3 parts
of 600/600/151**. Read granularity and write granularity are separate
decisions. Omitting it writes each source batch straight through,
which is what happened before this existed.

**How often work is COMMITTED** is `commit_policy` — a durability
decision, not a file-size one. A commit is the unit a crash can cost
you and the point a resume restarts from.

```yaml
commit_policy:
  every_bytes: 104857600      # 100 MB …
  every_seconds: 900          # … or every 15 minutes, whichever first
```

Both take any combination of thresholds and fire on whichever is
reached first. A `commit_policy` naming NO threshold is refused — it
would hold everything uncommitted until the run ended. An empty
`batch_policy` is fine and means "write straight through".

### The one interaction worth knowing

**A batch never spans a commit, so the commit cadence is an upper
bound on batching.** Set `batch_policy: {every_rows: 50000}` while
committing at every checkpoint, and if checkpoints arrive every 100
rows you will still get 100-row writes — the commit flushes what has
accumulated before it closes. To get large writes you must let commits
be at least as coarse:

```yaml
batch_policy:  {every_rows: 50000}
commit_policy: {every_checkpoints: 500}
```

A source also only checkpoints where it has a resumable position. The
pokemon stream declares no cursor, so it checkpoints once at the end —
which is exactly why `every_rows: 600` works there with the default
commit policy.

## Where to go next

- `rdlt run <pipeline>` prints a JSON report: rows, commits, retries,
  and where it resumed from.
- Delete a `workdir` to force a fresh run; keep it to let a crashed
  run resume where it stopped.
- The `out/` directories also hold `_rdlt_state.*.json` and
  `_rdlt_commits.*.json`. Those are rdlt's bookkeeping — the receipt
  log that makes a re-run idempotent instead of duplicating. Leave
  them beside the data.
