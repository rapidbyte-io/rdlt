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

What the two files show:

- `rest.yaml` — the source. PokéAPI returns
  `{count, next, previous, results: [...]}`, so `records_path: results`
  says where the rows are, and the `next_url` paginator follows the
  fully-formed `next` link the API supplies. When an API gives you the
  next URL, following it beats doing arithmetic on `offset`/`limit`.
- `pipeline.yaml` — what connects to what, plus the `workdir` where
  rdlt keeps its write-ahead log.

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

**1. Edit `oracle.yaml`.** Every value in it is a placeholder — host,
service, user, password, and the table name.

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

Uncomment `cursor:` in `oracle.yaml` and switch `write_mode` to
`merge`. Each run then reads only rows whose cursor column is above
the last checkpoint, and upserts them on `primary_key`.

The cursor column must be `NOT NULL`. That is enforced before any row
is read, and the reason is worth knowing: Oracle sorts NULLs last, so
a nullable cursor would deliver those rows once and then silently skip
them on every later run — the failure would look like success.

---

## Where to go next

- `rdlt run <pipeline>` prints a JSON report: rows, commits, retries,
  and where it resumed from.
- Delete a `workdir` to force a fresh run; keep it to let a crashed
  run resume where it stopped.
- The `out/` directories also hold `_rdlt_state.*.json` and
  `_rdlt_commits.*.json`. Those are rdlt's bookkeeping — the receipt
  log that makes a re-run idempotent instead of duplicating. Leave
  them beside the data.
