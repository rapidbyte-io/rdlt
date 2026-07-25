# Quickstart: measuring feature 019

Every increment in this feature is judged by a measurement (contract PI1).
This is how to take one that counts. The protocol below is the one that
produced `PERF_ANALYSIS.md`; following it is what makes a new number
comparable with the baseline of record.

## 0. Prerequisites

```sh
cargo build --release -p rdlt-cli      # or: make release
```

A container runtime (podman), `python3`, and `hyperfine` for the cold-start
guard. Measurement runs inside the project's usual build container; the fixture
containers are addressed over the host ports below.

## 1. Which instrument answers which question

| question | instrument | immune to machine load? |
|---|---|---|
| Did wall time move? | `rdlt-bench` cells, or `/usr/bin/time` around the release CLI | no — needs the quiet guard |
| Did processor time move? | `/usr/bin/time` (`%U`+`%S`) | mostly |
| Did a specific function get cheaper? | `perf record --call-graph fp` on a frame-pointer build | mostly |
| Did instruction count move? | `make bench TARGET=iai` | **yes** — use this when the machine is busy |
| Did the server or the client change? | fixture statement log (§5) | yes |
| Did allocation churn drop? | `/usr/bin/time` `%w` (voluntary context switches) + `perf` libc share | mostly |

**Rule of thumb**: a claim about wall time needs the quiet guard. A claim about
processor time or instruction count survives a moderately busy machine. Never
mix the two in one comparison.

## 2. The canonical A/B

The single most important discipline: **interleave the arms**. Machine drift
over minutes is larger than most of the effects being measured, and an A-then-B
layout attributes drift to the change.

```sh
for i in 1 2 3 4 5; do
  reset_destination
  printf "  before: "; /usr/bin/time -f '%e wall %U user %S sys %P cpu %M KB %w vcsw' \
      ./target/release/rdlt run "$SPEC" 2>&1 >/dev/null | tail -1
  reset_destination
  printf "  after : "; /usr/bin/time -f '%e wall %U user %S sys %P cpu %M KB %w vcsw' \
      ./target/before-change/rdlt run "$SPEC" 2>&1 >/dev/null | tail -1
done
```

Report **medians of at least 5 pairs**, never a single run and never a mean —
one scheduler hiccup moves a mean and not a median. Record the loadavg before
and after.

For an A/B that needs a code change on one arm only, build the variant into a
separate target directory so both binaries exist at once:

```sh
cargo build --release -p rdlt-cli --target-dir target/variant
```

Prefer an environment-gated switch inside one binary over two builds when the
change is small — it removes codegen differences from the comparison.

## 3. Fixtures

The bench harness owns fixture lifecycle; use it (`make bench TARGET=<cell>`)
for anything that will be recorded as evidence. Drive the CLI directly only
when you need `perf` wrapped around the process:

```sh
podman run -d --name rdlt-perf-pg -p 5439:5432 -e POSTGRES_PASSWORD=postgres postgres:16
until podman exec rdlt-perf-pg pg_isready -U postgres; do sleep 1; done
podman exec -i rdlt-perf-pg psql -q -U postgres -f - < benches/fixtures/seed_pg.sql

podman run -d --name rdlt-perf-rustfs -p 19110:9000 \
  -e RUSTFS_ACCESS_KEY=rdlt-bench -e RUSTFS_SECRET_KEY=rdlt-bench-secret \
  docker.io/rustfs/rustfs:1.0.0-beta.11
python3 benches/fixtures/gen_jsonl.py 200000 "$DATA/rows.jsonl"
python3 benches/fixtures/s3_bucket.py http://127.0.0.1:19110 rdlt-bench rdlt-bench-secret lake
python3 benches/fixtures/seed_s3.py http://127.0.0.1:19110 rdlt-bench rdlt-bench-secret \
        raw landed/rows.jsonl "$DATA/rows.jsonl"
```

**Verify fixture identity before trusting any number.** The seed prints row
counts and content hashes; they must match the recorded session
(`events` `e840f51738a6b4b15f9f085ea85e3df8`, `events_v2`
`7e208273f4d5333658fff2fa1c9839d9`). A drifted fixture invalidates the
comparison silently.

**Reset the destination before every single run** — mirroring
`fixtures.toml`'s `@reset_dest_schemas` — or the second run measures a
different starting state:

```sh
podman exec -i rdlt-perf-pg psql -q -U postgres <<'SQL'
\c dest_rdlt
DO $$ DECLARE s text; BEGIN FOR s IN SELECT nspname FROM pg_namespace
  WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema'
  LOOP EXECUTE format('DROP SCHEMA %I CASCADE', s); END LOOP; END $$;
CREATE SCHEMA public;
SQL
rm -rf "$WORK/.rdlt"
```

Tear the fixtures down when finished: `podman rm -f rdlt-perf-pg rdlt-perf-rustfs`.

## 4. Profiling

DWARF unwinding does not produce usable stacks through this codebase's async
frames. Build with frame pointers or the call graph will be empty:

```sh
RUSTFLAGS="-C force-frame-pointers=yes -C debuginfo=1" \
  cargo build --release -p rdlt-cli --target-dir target/fp

perf record -F 1999 --call-graph fp -o prof.data -- ./target/fp/release/rdlt run "$SPEC"
perf report -i prof.data --children --stdio -g none        # inclusive cost by subsystem
perf report -i prof.data --no-children --stdio -g none     # self time, the hotspot list
perf report -i prof.data --no-children --stdio \
    -g graph,0.5,caller -S '<symbol>'                      # who calls this
```

**Known limitation on this host**: `kernel.perf_event_paranoid = 2` and the
sysctl is not writable from the build container, so profiles are user-space
only. Kernel time — syscalls, socket I/O, page faults — is unattributed. Any
claim that depends on blocked or kernel time must be cross-checked with a
wall-clock A/B, which sees what the sampler cannot.

## 5. Splitting client cost from server cost

For the relational cells, most of the elapsed time is server-side. Attribute it
directly rather than inferring:

```sh
podman exec rdlt-perf-pg psql -q -U postgres \
  -c "ALTER SYSTEM SET log_min_duration_statement=0;" -c "SELECT pg_reload_conf();"
MARK=$(podman logs rdlt-perf-pg 2>&1 | wc -l)
./target/release/rdlt run "$SPEC"
podman logs rdlt-perf-pg 2>&1 | tail -n +$((MARK+1)) \
  | grep -oE 'duration: [0-9.]+ ms[^:]*: .{0,70}'
podman exec rdlt-perf-pg psql -q -U postgres \
  -c "ALTER SYSTEM SET log_min_duration_statement=-1;" -c "SELECT pg_reload_conf();"
```

Note the log records `COPY` under `execute`, not `statement` — grep for both or
you will conclude the data transfer is free. **Always restore the setting**:
logging every statement perturbs the very measurement you are taking.

## 6. Isolating a component

When a change is confined to one function, a standalone microbenchmark against
the workspace's pinned crate versions is faster to iterate on and immune to
pipeline noise. Build it outside the repo, pin the same versions
(`arrow`/`parquet` 58.3, `postgres-types` 0.2), and operate on the **real batch
shape** — 57,813 rows of the twelve-column bench schema, not a toy.

A microbenchmark bounds the win; it does not establish it. Every component
result must be confirmed end-to-end on a cell before it is recorded as evidence.

## 7. The instrument-track gates

These run in CI and must stay green at every increment:

```sh
make bench TARGET=iai        # instruction counts vs benches/perf-baselines.json (3% tolerance)
                             # + the cold-start check
./benches/check-cold-start.sh   # <= 40 ms median, quiet machine required
make check                   # lint + test + sweep + perf gate
```

Instruction counts shift for build-profile changes and rewritten hot paths.
When they shift for a reason you understand, re-record them deliberately in the
same change (`benches/compare-iai.sh --record`) with the reason in the commit
message — do not widen the tolerance (contract PI1).

## 8. Recording a result

A number becomes evidence when it is taken through the harness on a machine
that passed the quiet guard, against a fixture whose identity matched, with the
competitor arm measured in the same session. `make bench TARGET=<cell>` does
all of this; `TARGET=report` regenerates the results tables from the committed
artifacts. Numbers taken by the ad-hoc methods above are for *deciding* — the
recorded matrix is what is *claimed*.
