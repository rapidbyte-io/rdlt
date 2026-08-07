# Benchmark results

Three-way end-to-end matrix: **rdlt vs dlt vs Airbyte**, same seeded source,
same destination instance, same quiet machine — each cell measured baseline-first
and reported. Every number in the Matrix and Trends sections is generated from
committed artifacts (`TARGET=report make bench`); nothing is quoted without its
competitor column.

**Pin policy**: each competitor variant carries a version pin
(`benches/competitors/*/variants.toml`); a pin bump re-measures every cell
before any multiple is quoted (bump ⇒ re-measure). Coverage, semver, and
classified-exclusion records live in [`GOVERNANCE.md`](GOVERNANCE.md).

**Policy log** (one entry per governance event; newest first):

- **2026-08-07 — the wire measured against the bars; five remote twins added
  as SCOREBOARD, no bars minted (feature 041)**: each of the five e2e cells
  gained a `<cell>-remote` twin that runs the identical workload with the
  connectors spawned as separate release binaries over the connector protocol
  (`connector:` refs; `io.rapidbyte.postgres` / `io.rapidbyte.file`), so the
  cost of taking the connectors out of process is measured rather than argued.
  Ten cells were recorded in one session on a quiet machine, every arm
  rowcount-verified. **All four bars hold in remote mode**, compared against
  the bar VALUES rather than the in-process session: `pg-to-pg-1m-remote`
  **9.5×** (bar ≥ 4×), `s3jsonl-to-pg-200k-remote` **60.0×** (≥ 40×),
  `s3jsonl-to-s3parquet-200k-remote` **52.3×** (≥ 45×),
  `pg-to-pg-dedup-1m-remote` **2.4×** (≥ 2×). The unbarred
  `pg-to-s3parquet-1m-remote` measured 1.7× against its twin's 1.9×.
  The wire costs **+114 ms to +463 ms** per cell (×1.10 to ×1.54 of the
  in-process wall) and roughly doubles peak RSS and CPU; process spawn is not
  where it goes — spawn → handshake-complete for the postgres bin is **1.81 ms
  median** (min 1.63, p90 2.06, 20 sequential cold spawns), so two spawns are
  ≈3.6 ms of a 114 ms floor.
  **No bar is minted for any remote cell.** Governance is the same rule that
  left `pg-to-s3parquet-1m` unbarred: a bar sits below a recorded floor and one
  session on a new cell is not a basis for one (018 BR8, constitution
  Principle VIII). The five twins are reported as measured; a second session
  may propose bars for them. The existing four bars continue to bind the
  in-process cells only — this session re-confirmed them at 11.5× / 93.5× /
  59.1× / 2.7×.

- **2026-07-25 — the dedup cell was measuring three times its own claim
  (feature 019 US1)**: `pg-to-pg-dedup-1m` declared one query stream and its
  postgres source ALSO discovered every table in the schema, so rdlt moved
  `events` + `events_v2` + the declared `events_merged` — 3,000,000 rows —
  while dlt's script moved 1,000,000. The arms were never comparable. The
  committed artifact recorded both numbers side by side (`rdlt.rows` 3000000,
  `verify.actual_rows` 1000000) and nothing compared them.
  **Superseded values, withdrawn**: rdlt 14.81 s median (±8%) = **0.8× vs
  dlt** in the three-way session, and 14.75 s = 0.9× in the two-way session the
  same day. **Corrected, same machine and fixtures**: 5.00 s (±1%) =
  **2.5× vs dlt**, peak RSS 284 → 143 MB, CPU 5.55 → 1.51 s. The cell was
  never behind; the earlier entry below calling the merge path "an
  optimization target" was chasing an artifact of the cell spec.
  Three things changed so it cannot recur: an empty `tables:` list now means
  "discover no tables" (previously inexpressible — the only spellings were
  "these tables" and "all tables"); every cell declares its full expected table
  set and the harness **fails any run whose delivered set differs**; and the
  artifact `format_version` goes 2 → 3, so pre-check artifacts are refused
  rather than quoted. Re-recording under v3 exposed the same blind spot in two
  more cells — `s3jsonl-to-pg-200k` and `s3jsonl-to-s3parquet-200k` verified
  only `events` while also landing `events__tags` at 400,000 rows — now both
  declared. A `ratio_vs` bar at 2.0× is set for the cell from this session's
  2.5× floor; unlike the three bars below it rests on ONE recorded session, so
  a second session may tighten it. Session: 2026-07-25, five cells, 15/15 arms
  rowcount-verified, quiet guard passed.

- **2026-07-25 — bars return, measurement-first (feature 018 P4)**: three
  bars set from the first recorded three-way session (15/15 arms,
  rowcount-verified), each below its recorded floor: `pg-to-pg-1m` ≥ 4×
  vs dlt (floors 5.3× three-way / 4.6× two-way), `s3jsonl-to-pg-200k`
  ≥ 40× vs dlt (floors 55.3× / 54.9×), `s3jsonl-to-s3parquet-200k` ≥ 45×
  vs dlt (floors 60.1× / 61.1×). Deliberately NOT barred:
  `pg-to-s3parquet-1m` (recorded parity, 1.0×) and `pg-to-pg-dedup-1m`
  (recorded 0.9× — rdlt behind; the matrix reports it until the merge
  path improves and a new session justifies a bar); no RSS bar (one bar
  per cell, and the wall ratio is the flagship claim); no bar ever binds
  an Airbyte ratio (its job wall is floor-dominated context, not an
  engine comparison). A three-way Iceberg cell was considered and NOT
  taken — the owner did not elevate lakehouse scope this feature
  (plan P4); the 016 `iceberg-polaris-200k` evidence remains in the
  archive at `40841ab`.

- **2026-07-25 — dlt baseline = connectorx (first recorded session)**: the
  dlt arm's headline backend is `connectorx` — dlt's fastest supported
  postgres extractor — with `dlt-pyarrow` kept as labeled context, per the
  honest-fastest rule. Consequence, accepted knowingly: multiples compress
  or invert versus the retired pyarrow-baselined bars (pg-to-pg-1m 4.6×
  where the retired bar era showed 7.6×; pg-to-s3parquet-1m is at parity
  1.0×; pg-to-pg-dedup-1m rdlt LOSES at 0.9× — recorded as-is, the merge
  path is an optimization target, not a reporting problem). The s3-jsonl
  cells (54.9×, 61.1×) reflect dlt's filesystem/jsonl reader; the cell
  notes state the regime. Session: 2026-07-25, dlt 1.29.0, five cells,
  every arm rowcount-verified.

- **2026-07-24 — matrix rebuild + 8-bar retirement (feature 018, archive
  commit `40841ab`)**: the benchmark collapsed to five end-to-end cells. The
  gated/scoreboard taxonomy, cell suites, the library/hyperfine run modes, 25
  legacy cells, 10 fixtures, every v1 artifact, and all 8 bars were retired in
  one migration commit; the cold-start check moved to the instruments track
  (`benches/check-cold-start.sh`, ≤ 40 ms). Enforcement returns
  measurement-first (constitution v1.1.0): `bars.toml` is empty until the first
  recorded three-way session sets at most one bar per cell, each below its cited
  session floor with a policy-log entry here. Every retired cell's final value
  is recorded under Milestones below; the full pre-migration matrix and its
  artifacts are checkout-able at `40841ab`.

## Matrix

The five e2e cells, three-way, plus their five `-remote` wire twins (same work,
connectors spawned as separate processes over the connector protocol) and the
Oracle cell. Every arm is rowcount-verified against the cell's DECLARED table
set — a run that lands a table the cell did not declare fails before it is
recorded, because its timing would cover work the competitor arm never did.

The `-remote` rows carry no Target/Status: they are SCOREBOARD, and the bars in
`bars.toml` bind the in-process cells only (see the 2026-08-07 policy entry).

<!-- rdlt-bench:BEGIN matrix -->
| Cell | rdlt median | vs baseline | Target | Status | rows/s | MB/s | peak RSS |
|---|---|---|---|---|---|---|---|
| pg-to-pg-1m | 896.9 ms (±3%) | **11.5×** (dlt: 10.32 s); 19.6× (dlt-pyarrow: 17.58 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | ≥ 4× | PASS | 1114983 | 212.4 | 113 MB |
| pg-to-s3parquet-1m | 899.8 ms (±9%) | **1.9×** (dlt: 1.69 s); 12.3× (dlt-pyarrow: 11.08 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | — | — | 1111403 | 211.7 | 138 MB |
| s3jsonl-to-pg-200k | 697.9 ms (±3%) | **93.5×** (dlt: 65.23 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | ≥ 40× | PASS | 859685 | 204.9 | 173 MB |
| s3jsonl-to-s3parquet-200k | 1.00 s (±5%) | **59.1×** (dlt: 59.33 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | ≥ 45× | PASS | 597184 | 142.3 | 206 MB |
| pg-to-pg-dedup-1m | 4.89 s (±3%) | **2.7×** (dlt: 13.01 s); 4.3× (dlt-pyarrow: 20.90 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | ≥ 2× | PASS | 204434 | 38.8 | 110 MB |
| pg-to-pg-1m-remote | 1.09 s (±8%) | **9.5×** (dlt: 10.32 s); 16.2× (dlt-pyarrow: 17.61 s) | — | — | 920041 | 2118.0 | 282 MB |
| pg-to-s3parquet-1m-remote | 1.02 s (±6%) | **1.7×** (dlt: 1.70 s); 10.8× (dlt-pyarrow: 11.05 s) | — | — | 976216 | 2247.3 | 308 MB |
| s3jsonl-to-pg-200k-remote | 1.07 s (±63%) | **60.0×** (dlt: 64.48 s) | — | — | 558330 | 133.1 | 285 MB |
| s3jsonl-to-s3parquet-200k-remote | 1.12 s (±7%) | **52.3×** (dlt: 58.53 s) | — | — | 536123 | 127.8 | 307 MB |
| pg-to-pg-dedup-1m-remote | 5.35 s (±11%) | **2.4×** (dlt: 12.94 s); 3.9× (dlt-pyarrow: 20.94 s) | — | — | 186765 | 426.0 | 268 MB |
| oracle-to-pg-200k | 832.6 ms (±4%) | **4.1×** (dlt: 3.42 s); 54.6× (airbyte: 45.45 s) | — | — | 240205 | 46.9 | 60 MB |

_Generated by `rdlt-bench report` from committed artifacts (recorded 2026-08-03, 2026-08-07; airbyte 2.1.1, dlt 1.29.0)._
<!-- rdlt-bench:END matrix -->

## The wire overhead — recorded session 2026-08-07 (feature 041)

Hand-written from the ten artifacts above (the generated matrix reports each
cell alone; this pairs the twins). Same machine, same session, same fixtures,
quiet guard passed on every cell, all ten rowcount-verified, none `forced`.

| Twin pair | in-process | remote | overhead | ×  | ratio vs dlt (in-proc → remote) | bar |
|---|---|---|---|---|---|---|
| pg-to-pg-1m | 896.9 ms | 1086.9 ms | +190.0 ms | ×1.212 | 11.5× → **9.5×** | ≥ 4× **HOLDS** |
| pg-to-s3parquet-1m | 899.8 ms | 1024.4 ms | +124.6 ms | ×1.138 | 1.9× → **1.7×** | none |
| s3jsonl-to-pg-200k | 697.9 ms | 1074.6 ms | +376.7 ms | ×1.540 | 93.5× → **60.0×** | ≥ 40× **HOLDS** |
| s3jsonl-to-s3parquet-200k | 1004.7 ms | 1119.1 ms | +114.4 ms | ×1.114 | 59.1× → **52.3×** | ≥ 45× **HOLDS** |
| pg-to-pg-dedup-1m | 4891.6 ms | 5354.3 ms | +462.8 ms | ×1.095 | 2.7× → **2.4×** | ≥ 2× **HOLDS** |

**The verdict: GREEN.** All four bars hold with the connectors out of process,
compared against the bar VALUES (4.0 / 40 / 45 / 2.0), not against the
in-process session. Narrowest margin: `s3jsonl-to-s3parquet-200k-remote` at
52.3× against a 45× bar (×1.16 headroom). Widest absolute cost:
`pg-to-pg-dedup-1m` at +462.8 ms — which is also the *smallest* proportional
cost (+9.5%), because that cell is dominated by server-side merge work the
wire does not touch.

**This session ran slower than the 2026-08-01 one, and BOTH arms did — but not
proportionally. Recorded as an open observation, not attributed.** Measured
against the artifacts this session replaced:

| Cell | rdlt | dlt |
|---|---|---|
| `pg-to-pg-1m` | 744.2 → 896.9 ms (**+20.5%**) | 10.17 → 10.32 s (+1.5%) |
| `pg-to-s3parquet-1m` | 913.8 → 899.8 ms (**−1.5%**) | 1.67 → 1.69 s (+1.2%) |
| `s3jsonl-to-pg-200k` | 639.8 → 697.9 ms (**+9.1%**) | 62.76 → 65.23 s (+3.9%) |
| `s3jsonl-to-s3parquet-200k` | 848.9 → 1004.7 ms (**+18.4%**) | 58.52 → 59.33 s (+1.4%) |
| `pg-to-pg-dedup-1m` | 4.37 → 4.89 s (**+11.9%**) | 12.45 → 13.01 s (+4.5%) |

rdlt moved +20.5% where dlt moved +1.5% on the same cell — up to **13× more**.
Uniform machine slowness predicts PROPORTIONAL movement, so the differential is
NOT explained by "the machine was busier", and this data does not settle what
it is. A plausible but unsettled mechanism: dlt's walls are 10–65 s and
Python/IO-dominated, so they are largely insensitive to sustained-clock and
turbo-residency effects, while rdlt's ~900 ms walls are CPU- and
bandwidth-bound and are not. Corroborating but not decisive: the branch's
non-bench diff carries no hot-path change — `rdlt-engine` is untouched, and the
non-test changes are config-shape resolution in `pipeline_spec.rs`, a `NAME`
const, and CDC spec plumbing.

**Which way it cuts is the part that matters here: the ratios are DEFLATED.**
Every remote ratio in this session was divided by an rdlt wall that was high
relative to its own baseline, so the four bars cleared on a pessimistic
session. Start-of-cell loadavg ran 1.53–4.84 against a 32-core quiet threshold
of 8.0, so every cell passed the guard and none is `forced`.

The Airbyte arm recorded `Missing{abctl cluster unreachable}` on all five
in-process cells — the kind cluster was not up on this machine — so this
session is 2-way and those matrix rows carry the reason rather than a number.
None of this touches the verdict: both arms of every twin pair were measured in
the same session minutes apart, and the four bars are compared against their
VALUES, all of which the remote arms clear.

**Where the time goes — not spawn.** Spawn → handshake-complete for
`io.rapidbyte.postgres` (source role, release bin, 20 sequential cold spawns,
each child dropped before the next; `cargo run --release -p rdlt-runtime
--features spawn-bins --example spawn_latency`):

| min | median | p90 |
|---|---|---|
| 1.63 ms | 1.81 ms | 2.06 ms |

Two spawns per pipeline is ≈3.6 ms — 3% of the smallest per-cell overhead
(114 ms) and 0.8% of the largest (463 ms). The cost is the wire itself: CPU
`user_sys` roughly doubles across every pair (e.g. pg-to-pg-1m 530 → 1000 ms)
and peak RSS goes 113 → 282 MB, which is the signature of an extra
encode/decode pass and a second process's buffers, not of process startup.

**The overhead band is LIKELY an upper bound — likely, not proven.** The
byte-accounting caveat below records that a decoded-over-the-wire Arrow batch
reports ≈17× its real footprint (≈12× its in-process twin's already-inflated
figure), and that the same expression meters source backpressure — so the
remote arms ran with a far smaller in-flight window than configured. Widening
it back should recover some of the +114…+463 ms, but that is the expected
direction, not a measured one, and a wider window also raises resident buffers
in a constellation whose RSS is already 2–3× the in-process arm's. The house
rule applies to this as to any counting argument: guilty until measured
(019 D-13/D-21). Nothing above is restated on that basis.

## Caveats

Stated so the numbers stay honest as the matrix fills:

- **Per-product timing boundaries** (what each column measures): rdlt and dlt
  are single-process pipelines timed by the harness wall clock around the
  release CLI / the baseline's own self-timed `seconds` line — the number is
  the pipeline, nothing else. On the `-remote` cells rdlt is a process TREE
  rather than one process (see the twins caveat below); the boundary is
  unchanged — the harness still wraps the release CLI, which now also pays for
  its children. Airbyte's headline `seconds` is the **job wall**
  (orchestration, connector-pod scheduling, and platform overhead included, and
  labeled as such); its attempt time rides `extra.sync_s` as recorded context.
  The three columns are comparable as "how long to move this data with this
  tool as operated", not as isolated engine microbenchmarks.
- **Airbyte's fixed floor**: the first recorded 3-way session measured a
  ~35–45 s per-job orchestration floor (pod scheduling, check + replication
  container spin-up) that dominates its wall at these volumes — four
  full-refresh cells all median ≈45 s regardless of whether 200k or 1M rows
  moved. The Airbyte columns say "the platform's unit of work costs this
  much end to end", not "its connectors stream this slowly"; at much larger
  volumes the floor amortizes. Its `peak_rss_kb` is the whole-cluster cgroup
  high-water (labeled context, never barred). Airbyte arms run 3 times (the
  floor makes 5 runs pure cost); rdlt/dlt run 5.
- **Quiet machine**: every recorded session passes the classless quiet guard
  first (loadavg below 0.25×cores). A forced run on a loaded machine is stamped
  `forced: true` in its artifact — the number is context, not evidence.
- **Dedup cell regime**: the `pg-to-pg-dedup-1m` cell measures LOAD 2 only
  (full re-delivery + dedup by `id`); all three products run the full-redelivery
  regime, so Airbyte's cheaper incremental mode is deliberately not benched
  (no dlt counterpart). The cell's note renders as the matrix caption. Its
  source declares `tables: []` so only the query stream is delivered — without
  that, schema discovery adds every table in `public` on top, which is what the
  superseded 0.8× figure was measuring (see the policy log).
- **Oracle cell — what the driver switch bought** (`oracle-to-pg-200k`, 032):
  this row supersedes an earlier reading of the same cell. rdlt's *first*
  Oracle read paged by ROWID keyset and sized each page so one query reply fit
  ONE 8 KB packet — 14 rows per round trip on this table — because the
  pure-Rust driver could not continue a cursor. That shape was measured at
  **3-7 minutes extrapolated** for 200k rows, and could not complete past ~297
  pages at all (the driver never closed a server cursor). rdlt now reads
  through `oracle`/ODPI-C, streaming ONE cursor per stream into Arrow batches:
  **837.3 ms**. Both ceilings are gone, so the cell is now a fair read-path
  comparison rather than a record of a self-imposed cap. It still carries **no
  bar** — one recorded session is not a basis for one (018 BR8).
- **Oracle cell — read the Airbyte ratio with 018's caveat, not as a
  headline.** 54.6× (45.45 s) is JOB WALL CLOCK including orchestration: a
  Kubernetes pod per check, per discover and per replication attempt, on a
  single-node kind cluster. 018 recorded Airbyte's floor at ~45-60 s across
  every cell regardless of dataset size, and 45.45 s here sits squarely in
  that band — which means this row measures Airbyte's fixed startup cost far
  more than its Oracle read throughput. Its sync is CORRECT (200,000 rows
  verified in the destination) and its own reported figures were
  `driver_wall≈91 s / api_duration≈79 s` on the first attempt. The dlt ratio
  is the informative comparison; the Airbyte one bounds the difference
  between an embedded engine and an orchestrated platform.
- **Oracle cell — dlt's fastest backend is deliberately NOT run**: ConnectorX
  does support Oracle, but through ODPI-C, which dlopen's `libclntsh` from
  Oracle Instant Client at run time. Instant Client is not pip-installable and
  carries Oracle's OTN license, so the pg cells' headline `backend=connectorx`
  does not transfer. dlt's arm here runs python-oracledb **thin** mode
  (verified: no `libclntsh` anywhere in the baseline image). This is a recorded
  handicap, not a hidden one; closing it means adding Instant Client to the
  competitor image and a third arm — an owner decision, not a default.
- **Oracle cell — the Airbyte arm may be a documented absence**:
  `airbyte/source-oracle` is alpha/community (ELv2, in the default OSS catalog,
  so no custom registration) and its docs claim testing only through 21c, while
  the fixture is 23ai. Nothing documents a 23ai failure and nothing verifies
  one. If discover or sync fails, the arm records `Missing{reason}` and the
  matrix runs two-way. Substituting a 21c container for that arm alone is
  refused: it would give one arm a different source server and break the
  same-conditions rule the whole matrix rests on.
- **The `-remote` twins — what the number covers** (041): the twin runs the
  same pipeline with `connector:` refs, so the harness wall clock now also
  covers spawning each connector binary, its handshake, config validation in
  the child, and every batch crossing a unix socket. The bins come from
  `<target>/release` unconditionally — a measured cell spawns the shipped
  shape, never a debug build. `peak RSS` and `CPU` are process-TREE samples,
  so the children are inside them: the remote rows' RSS is the whole
  constellation (≈2–3× the in-process row), not a regression in the engine.
  The competitor arms are byte-identical to the twin's — dlt is not spawned
  differently — so the ratio column compares like with like. Airbyte is
  deliberately NOT an arm on the remote cells: its job wall is
  floor-dominated (~45–60 s regardless of dataset) and contributes nothing to
  a wire-overhead verdict.
- **`s3jsonl-to-pg-200k-remote` spread is warm-up, and it is recorded rather
  than re-rolled** (041 session): its five runs were 1435.5 / 1532.4 / 1074.6 /
  860.0 / 858.2 ms — ±63%, by far the widest in the session, with the cost
  falling monotonically after run 2. Its in-process twin was ±3% on the same
  fixture in the same session, so the shape belongs to the remote path (first
  spawns of the file+postgres bins paying page-cache and allocator warm-up),
  not to the cell. The **median as measured** (1074.6 ms → 60.0×) is what is
  recorded and what the verdict uses; the steady-state tail would read ≈75×,
  and quoting that instead would be picking the number. No re-roll was taken.
- **The remote rows' MB/s column is not comparable to their twins', and the
  cause is not cosmetic** (041 session, recorded as a defect, not a result):
  on the two pg-source remote cells the byte total the run reports is ≈12× its
  in-process twin's for the identical 1,000,000 × 12 workload
  (`pg-to-pg-1m` 199,720,864 B vs `pg-to-pg-1m-remote` 2,413,845,024 B;
  `pg-to-pg-dedup-1m` 198,849,808 B vs 2,391,469,392 B). The file-source
  remote cells report byte-IDENTICAL totals to their twins (149,965,184 B).
  **Diagnosed**: both modes count with the same expression,
  `RecordBatch::get_array_memory_size()`, which sums `Buffer::capacity()` once
  per buffer. Arrow's IPC reader allocates the whole message body as ONE
  allocation and hands every column a zero-copy SLICE of it, and a slice keeps
  the parent's capacity — so a decoded batch reports ≈ `n_buffers × body_len`.
  This table has 17 buffers (8 fixed-width columns + 3 Utf8 × 2 + 1 nullable
  Utf8 × 3); the in-process arms over-report too (builder buffers grow by
  doubling, ≈1.4×), and 17 ÷ 1.4 ≈ 12.1 — the observed ratio. The 12 columns
  are a coincidence, and the wire is NOT moving 2.4 GB. The file cells escape
  it because the jsonl source ships raw slabs and the engine shreds them into
  locally-built batches in BOTH modes.
  Wall time, rows/s and every ratio in this session are unaffected — `bytes`
  feeds only MB/s, which is context and has never carried a bar — so
  **read the MB/s cell on `*-remote` pg-source rows as unreliable**.
  **But the same expression meters source backpressure**, so a remote Arrow
  source over-charges its byte budget ≈17× against the batch's TRUE footprint
  — and ≈12× against what the in-process arm charges for the same data, since
  that arm over-reports ≈1.4× itself — and therefore runs with a far smaller
  in-flight window than configured. That makes the wire overhead recorded above
  **likely an upper bound**: some part of the +114…+463 ms is plausibly a
  throttled window rather than the socket. Likely, not proven — widening the
  window also raises resident buffers, and the remote RSS is already 2–3× the
  in-process arm's, so the net is a measurement nobody has taken. No number
  here is restated on the strength of an unmeasured fix.
- **Cold start** lives on the instruments track, not the matrix: a one-row
  file → duckdb pipeline, ≤ 40 ms absolute (`benches/check-cold-start.sh`,
  run by `TARGET=iai make bench` and therefore `make check`).

## Trends

Generated from `benches/history.jsonl` (one line per cell×variant per recorded
invocation) — the latest two medians per pair and their delta.

<!-- rdlt-bench:BEGIN trends -->
| Cell | Variant | Latest | Previous | Δ |
|---|---|---|---|---|
| oracle-to-pg-200k | airbyte | 45.45 s | — | — |
| oracle-to-pg-200k | dlt | 3.36 s | 3.61 s | -7.0% |
| oracle-to-pg-200k | rdlt | 845.9 ms | 923.5 ms | -8.4% |
| pg-to-pg-1m | airbyte | 60.44 s | 45.37 s | +33.2% |
| pg-to-pg-1m | dlt | 10.32 s | 10.17 s | +1.5% |
| pg-to-pg-1m | dlt-pyarrow | 17.58 s | 17.19 s | +2.3% |
| pg-to-pg-1m | rdlt | 896.9 ms | 744.2 ms | +20.5% |
| pg-to-pg-1m-remote | dlt | 10.32 s | — | — |
| pg-to-pg-1m-remote | dlt-pyarrow | 17.61 s | — | — |
| pg-to-pg-1m-remote | rdlt | 1.09 s | — | — |
| pg-to-pg-dedup-1m | airbyte | 45.38 s | 45.39 s | -0.0% |
| pg-to-pg-dedup-1m | dlt | 13.01 s | 12.45 s | +4.5% |
| pg-to-pg-dedup-1m | dlt-pyarrow | 20.90 s | 20.74 s | +0.8% |
| pg-to-pg-dedup-1m | rdlt | 4.89 s | 4.37 s | +11.9% |
| pg-to-pg-dedup-1m-remote | dlt | 12.94 s | — | — |
| pg-to-pg-dedup-1m-remote | dlt-pyarrow | 20.94 s | — | — |
| pg-to-pg-dedup-1m-remote | rdlt | 5.35 s | — | — |
| pg-to-s3parquet-1m | airbyte | 45.37 s | 45.39 s | -0.0% |
| pg-to-s3parquet-1m | dlt | 1.69 s | 1.67 s | +1.2% |
| pg-to-s3parquet-1m | dlt-pyarrow | 11.08 s | 10.80 s | +2.6% |
| pg-to-s3parquet-1m | rdlt | 899.8 ms | 913.8 ms | -1.5% |
| pg-to-s3parquet-1m-remote | dlt | 1.70 s | — | — |
| pg-to-s3parquet-1m-remote | dlt-pyarrow | 11.05 s | — | — |
| pg-to-s3parquet-1m-remote | rdlt | 1.02 s | — | — |
| s3jsonl-to-pg-200k | airbyte | 45.38 s | 45.37 s | +0.0% |
| s3jsonl-to-pg-200k | dlt | 65.23 s | 62.76 s | +3.9% |
| s3jsonl-to-pg-200k | rdlt | 697.9 ms | 639.8 ms | +9.1% |
| s3jsonl-to-pg-200k-remote | dlt | 64.48 s | — | — |
| s3jsonl-to-pg-200k-remote | rdlt | 1.07 s | — | — |
| s3jsonl-to-s3parquet-200k | airbyte | 45.38 s | 45.37 s | +0.0% |
| s3jsonl-to-s3parquet-200k | dlt | 59.33 s | 58.52 s | +1.4% |
| s3jsonl-to-s3parquet-200k | rdlt | 1.00 s | 848.9 ms | +18.4% |
| s3jsonl-to-s3parquet-200k-remote | dlt | 58.53 s | — | — |
| s3jsonl-to-s3parquet-200k-remote | rdlt | 1.12 s | — | — |
| selftest-protocol | rdlt | 22.0 ms | 21.3 ms | +3.4% |
<!-- rdlt-bench:END trends -->

## Milestones

Claims from the pre-018 matrix, retired by the rebuild and preserved here with
their final recorded values. Evidence for every entry: **archive commit
`40841ab`** (the last session before the migration, checkout-able with its
cells and artifacts).

- **Flagship jsonl → DuckDB (jsonl-duckdb-200k)**: 13.5× vs dlt
  (14869 ms / 1105 ms, 5-run medians), peak RSS 1/5.4 (353 MB / 1910 MB).
  Evidence: commit `40841ab`.
- **Shred-only (shred-only-200k)**: 12.0× vs dlt (5898 ms / 490 ms).
  Evidence: commit `40841ab`.
- **REST → Postgres (rest-pg-100k)**: 6.7× vs dlt (5523 ms / 820 ms).
  Evidence: commit `40841ab`.
- **Parquet passthrough (parquet-passthrough)**: 3.5× vs dlt
  (331 ms / 93 ms). Evidence: commit `40841ab`.
- **Postgres → DuckDB, 1M wide (pg-wide-duckdb-1m)**: 7.8× vs dlt-pyarrow
  (10243 ms / 1306 ms). Evidence: commit `40841ab`.
- **Postgres → Postgres, 1M wide (pg-wide-pg-1m)**: 7.6× vs dlt-pyarrow
  (17652 ms / 2318 ms). Evidence: commit `40841ab`.
- **Cold start (cold-start)**: 24.2 ms median, ≤ 40 ms absolute — relocated
  live to the instruments track. Evidence: commit `40841ab`.
- **Postgres CDC catch-up (cdc-change-apply-500k)**: ≈72k changes/s
  (6.96 s for a 500k-change catch-up on 1M rows). Evidence: commit `40841ab`.
