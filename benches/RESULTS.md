# Benchmark results

End-to-end matrix: **rdlt vs dlt**, same seeded source, same destination
instance, same quiet machine — each cell measured baseline-first and
reported, with rdlt's connectors spawned as separate release binaries over
the connector protocol (the only architecture that exists — ADR 0001 D1).
Airbyte remains as recorded context on the Oracle cell; its arms on the five
e2e cells retired 2026-08-11 (policy log below). Every number in the Matrix
and Trends sections is generated from committed artifacts
(`TARGET=report make bench`); nothing is quoted without its competitor
column.

**Pin policy**: each competitor variant carries a version pin
(`benches/competitors/*/variants.toml`); a pin bump re-measures every cell
before any multiple is quoted (bump ⇒ re-measure). Coverage, semver, and
classified-exclusion records live in [`GOVERNANCE.md`](GOVERNANCE.md).

**Policy log** (one entry per governance event; newest first):

- **2026-08-14 — THE RE-POINT: the live ledger is the recorded ledger again
  (feature 046, D-046-4)**: the 2026-08-13/14 recorded session re-minted
  baselines on the post-split + byte-bound shape — the connectors living in
  their own repo since 044, every pipeline spawning them over the protocol,
  with 046's byte-bound serve channel in the spawned bins — and
  `rdlt-bench gate` / `rdlt-bench report` bind the live ledger
  (`benches/results/` + `benches/history.jsonl`, the paths `run` writes)
  again. The pre-split recordings stay byte-identical as dated history in
  `benches/records/archive-2026-08-13/`, read by no command; a checkout
  without a recorded ledger now REFUSES with instructions (gate and report
  both) instead of failing every bar separately or splicing an empty matrix
  over recorded tables. **All four bars held on the fresh recordings,
  mechanically** — `pg-to-pg-1m` 9.9× (≥ 4×), `s3jsonl-to-pg-200k` 79.4×
  (≥ 40×), `s3jsonl-to-s3parquet-200k` 56.6× (≥ 45×), `pg-to-pg-dedup-1m`
  2.6× (≥ 2×); `gate: all bars met`, every `bars.toml` value untouched. The
  Oracle cell's Airbyte arm recorded `Missing{abctl cluster unreachable}` —
  the kind cluster is down on this machine; skip-if-down ruling, one
  liveness probe, no resurrection — so the matrix's only Airbyte column is
  a loud absence. `oracle-to-pg-200k` is that cell's FIRST
  spawned-over-the-protocol recording: the archived 832.6 ms row was the
  032-era in-process connector, and the fresh 878.4 ms (+5.5%) also pays
  two spawns + handshakes. **Cross-regime deltas are the identity of the
  new shape, not regressions**: against the archive the five e2e cells sit
  within ±4.1% (`pg-to-pg-1m` +4.0%, `pg-to-s3parquet-1m` +1.4%,
  `s3jsonl-to-pg-200k` +0.6%, `s3jsonl-to-s3parquet-200k` −1.2%,
  `pg-to-pg-dedup-1m` +0.2%) — the post-split + byte-bound architecture
  costs nothing measurable on the flagship cells — and NO archived figure
  is restated as current: the archive's values describe the retired
  pre-split regime and live only there. The history feed restarted with
  this session, so Trends shows first-session points until the next
  recorded one. The MB/s column is the first RECORDED one rendered from
  042's footprint meter (`06b2bc2a` re-pointed the engine's report-total
  sites right after the 2026-08-10 session recorded): the pg-source rows'
  ≈12×-inflated byte totals are gone — `pg-to-pg-1m` reports 178,240,270 B
  for the 1M×12 workload, not 2.4 GB — but the metered totals are
  plausible rather than audited; two residuals remain unreconciled (see
  the amended byte-accounting caveat).

- **2026-08-13 — THE HISTORY RESET: the pre-split recorded regime ENDS here
  (feature 045, D-045-5)**: every recorded session above this line was
  measured under the pre-split regime, and its records — `history.jsonl`,
  `flakes.log`, and the six committed `results/*.json` artifacts — retire
  content-byte-identical to `benches/records/archive-2026-08-13/` (archive
  commit `b3d962d8`). No figure is restated and no bar moves: `bars.toml`
  KEEPS BINDING — `rdlt-bench gate` enforces its unchanged floors against
  the archived recordings' values, because the floors are facts about
  recorded runs. The live ledger (`benches/results/`, the `history.jsonl`
  trends feed) is EMPTY from here until 046's post-byte-bound session mints
  fresh baselines on the post-split shape and re-mints or re-rules the bars
  under 004 governance; until then `rdlt-bench report` keeps rendering the
  ARCHIVED recordings — it reads the same archived artifacts and history
  feed the gate's bars bind against, never the empty live ledger — so the
  Matrix and Trends sections below keep showing the recorded truth the
  bars cite, and the recorded-session narratives below describe the
  archived regime. 046 re-points report and gate at the live ledger
  together.

- **2026-08-11 — the D1 swap: the spawned-connector recordings ARE the
  benchmark identity (feature 043, ADR 0001 D1)**: the in-process build tier
  is deleted — every rdlt pipeline now spawns its connectors as separate
  release binaries over the connector protocol — so the matrix stops naming
  two modes. The five `<cell>-remote` cells take the BASE ids (`pg-to-pg-1m`,
  `pg-to-s3parquet-1m`, `s3jsonl-to-pg-200k`, `s3jsonl-to-s3parquet-200k`,
  `pg-to-pg-dedup-1m`): cell blocks, committed artifacts and bars are
  re-keyed with every value, floor and policy citation UNCHANGED — no figure
  is restated, the 2026-08-10 recordings simply own the ids of the only
  architecture that exists. The five in-process cell blocks and their four
  bars retired with the deleted tier; each retired bar carried the SAME value
  as its re-keyed successor (the 2026-08-10 minting deliberately mirrored
  them), so the enforced floors are unmoved. The **Airbyte arms retired**
  with the in-process cells — they were context (floor-dominated ~45–60 s
  job wall regardless of dataset), never bars; the Oracle cell keeps its
  Airbyte arm. `history.jsonl` keeps every line under its recorded name,
  with a dated note entry marking the seam: base-id lines at or before
  2026-08-10 belong to the retired in-process cells, and the `-remote` lines
  are this identity's recordings. The hand-written session records and
  caveats below keep naming `-remote` ids — they are the record of the
  sessions that measured under those names. **Cold start re-derived
  spawn-inclusive**: the one-row file → duckdb instruments check now spawns
  both connector bins (two spawn+handshakes inside the measured wall),
  median **27.1 ms** (mean 27.2 ± 0.7 ms, range 25.8–28.8 ms, 20 runs,
  loadavg 0.87) on the swapped tree against the UNCHANGED 40 ms bar
  (`benches/harness/check-cold-start.sh`). One survivor spelling was repaired to get
  there: the cold and oracle pipelines carried the retired
  `source: <name>: {config: <path>}` sub-key form, which the spawned
  connectors' own document gates refuse (`unknown field \`config\``) — both
  now use the path form (`source: file: <path>`). Text change, same
  documents; nothing about the protocol moved.

- **2026-08-10 — the second wire session: the byte-fix verdict is STANDS, and
  four remote bars are minted (feature 042, D-042-4)**: the ten e2e cells and
  their dlt arms re-recorded in one quiet-machine session (loadavg 0.15 at
  start, every cell under the guard, none `forced`, all ten rowcount-verified)
  with 042's byte-meter fix in the spawned binaries — the connector channel's
  budget now counts an IPC batch's true slice footprint instead of its parent
  allocations' capacity (≈17× over-charge), so the remote arms ran the
  in-flight window the operator configured. **The verdict on the fix: it
  STANDS.** Against the 041 session artifacts, every remote cell's wall fell
  (−3.9% to −24.4%), CPU stayed within −8%/+5%, and peak RSS moved net DOWN
  31 MB across the five (−59/+4/+55/−7/−24 MB) — the 019 D-13/D-21 risk (an
  RSS regression outweighing the wall gain) did not materialize; the one RSS
  rise (`s3jsonl-to-pg-200k-remote`, +55 MB) sits on the cell with the largest
  wall win (−262 ms). The isolating figure is the twin-pair overhead:
  `pg-to-pg-dedup-1m` +462.8 → +216.5 ms (halved). NOTE the fix repaired the
  BUDGET meter only: the engine's reporting sites still capacity-sum, so the
  artifact `bytes`/MB/s statistic on pg-source remote rows is unchanged and
  still unreliable (caveat amended below). **Four remote bars are minted** —
  two sessions now exist, which is the governance threshold one session did
  not meet (018 BR8, constitution Principle VIII). Each MIRRORS its in-process
  twin's value rather than hugging the higher remote floors, enforcing the
  claim that matters: the wire does not cost the flagship bars.
  `pg-to-pg-1m-remote` ≥ 4× (two-session floors 9.50×/10.13×),
  `s3jsonl-to-pg-200k-remote` ≥ 40× (60.0×/78.4×),
  `s3jsonl-to-s3parquet-200k-remote` ≥ 45× (52.3×/57.1×),
  `pg-to-pg-dedup-1m-remote` ≥ 2× (2.42×/2.62×). `pg-to-s3parquet-1m-remote`
  stays UNBARRED for its twin's reason — near-parity (1.75× both sessions) is
  reported, not gated. **041's open session differential is re-read and
  CONFIRMED as session-state sensitivity, not a code regression**: this
  session the asymmetry ran the opposite direction — rdlt's in-process arms
  moved −4.5% to −10.0% against their 041 values while dlt's moved −3.0% to
  +5.3% — the same magnitude imbalance that 041 recorded as +9.1…+20.5% rdlt
  vs +1.2…+4.5% dlt, now reverting. Sub-second CPU-bound walls swing with
  machine state in a way 10–65 s Python/IO-dominated walls do not; the
  specific mechanism (sustained-clock/turbo residency) remains plausible and
  unproven, and no figure is corrected on its account.

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
  in-process wall); CPU `user_sys` rises **×1.12 to ×1.89** and peak RSS
  **×1.49 to ×2.48** — both largest on `pg-to-pg-1m`, neither reaching 3× on
  any pair. So "roughly doubles" was wrong on both counts: no pair doubles its
  CPU at all, and RSS clears 2× on three of the five pairs while the other two
  sit at ×1.65 and ×1.49. Process spawn is
  not where it goes — spawn → handshake-complete for the postgres bin is
  **1.81 ms median** (min 1.63, p90 2.06, 20 sequential cold spawns), so two
  spawns are ≈3.6 ms of a 114 ms floor.
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
  (`benches/harness/check-cold-start.sh`, ≤ 40 ms). Enforcement returns
  measurement-first (constitution v1.1.0): `bars.toml` is empty until the first
  recorded three-way session sets at most one bar per cell, each below its cited
  session floor with a policy-log entry here. Every retired cell's final value
  is recorded under Milestones below; the full pre-migration matrix and its
  artifacts are checkout-able at `40841ab`.

## Matrix

The five e2e cells — rdlt's connectors spawned as separate release binaries
over the connector protocol — plus the Oracle cell. Every arm is
rowcount-verified against the cell's DECLARED table set — a run that lands a
table the cell did not declare fails before it is recorded, because its
timing would cover work the competitor arm never did.

Four cells carry bars (minted 2026-08-10 from two recorded sessions,
re-keyed 2026-08-11 to the base ids with values and floors unchanged);
`pg-to-s3parquet-1m` stays scoreboard — near-parity is reported, not gated
(see the 2026-08-10 and 2026-08-11 policy entries).

<!-- rdlt-bench:BEGIN matrix -->
| Cell | rdlt median | vs baseline | Target | Status | rows/s | MB/s | peak RSS |
|---|---|---|---|---|---|---|---|
| pg-to-pg-1m | 894.2 ms (±2%) | **12.0×** (dlt: 10.72 s); 20.2× (dlt-pyarrow: 18.10 s) | ≥ 4× | PASS | 1118344 | 230.3 | 238 MB |
| pg-to-s3parquet-1m | 831.6 ms (±1%) | **2.3×** (dlt: 1.88 s); 13.7× (dlt-pyarrow: 11.42 s) | — | — | 1202550 | 247.7 | 279 MB |
| s3jsonl-to-pg-200k | 737.3 ms (±7%) | **90.5×** (dlt: 66.69 s) | ≥ 40× | PASS | 813763 | 220.4 | 290 MB |
| s3jsonl-to-s3parquet-200k | 1.00 s (±2%) | **61.3×** (dlt: 61.28 s) | ≥ 45× | PASS | 599954 | 162.5 | 363 MB |
| pg-to-pg-dedup-1m | 4.77 s (±3%) | **2.7×** (dlt: 12.91 s); 4.4× (dlt-pyarrow: 20.99 s) | ≥ 2× | PASS | 209529 | 42.8 | 234 MB |
| oracle-to-pg-200k | 878.4 ms (±3%) | **3.8×** (dlt: 3.37 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | — | — | 227678 | 44.0 | 99 MB |

_Generated by `rdlt-bench report` from committed artifacts (recorded 2026-08-14, 2026-08-21; airbyte 2.1.1, dlt 1.29.0)._
<!-- rdlt-bench:END matrix -->

## The second wire session + the byte-fix verdict — recorded session 2026-08-10 (feature 042)

Hand-written from the ten artifacts above (the generated matrix reports each
cell alone; this pairs the twins and reads them against the 041 session).
One continuous session, five twin-pair invocations, session start loadavg
0.15, per-cell start loadavg 1.94–3.73 against a quiet threshold of 8.0,
every arm rowcount-verified, none `forced`, no re-rolls. The Airbyte arm
again recorded `Missing{abctl cluster unreachable}` on the five in-process
cells — verbatim, the kind cluster stays down on this machine — so the
session is 2-way like 041's.

| Twin pair | in-process | remote | overhead | ratio vs dlt (in-proc → remote) | bar |
|---|---|---|---|---|---|
| pg-to-pg-1m | 807.4 ms | 1002.1 ms | +194.7 ms | 12.7× → **10.1×** | ≥ 4× **PASS** |
| pg-to-s3parquet-1m | 835.7 ms | 952.9 ms | +117.2 ms | 2.0× → **1.7×** | none |
| s3jsonl-to-pg-200k | 666.5 ms | 812.2 ms | +145.7 ms | 95.1× → **78.4×** | ≥ 40× **PASS** |
| s3jsonl-to-s3parquet-200k | 954.4 ms | 1075.7 ms | +121.3 ms | 65.5× → **57.1×** | ≥ 45× **PASS** |
| pg-to-pg-dedup-1m | 4597.0 ms | 4813.5 ms | +216.5 ms | 2.7× → **2.6×** | ≥ 2× **PASS** |

**The byte-fix verdict (D-042-4): the fix STANDS.** This session is the
measurement the 041 caveat demanded before restating anything: the spawned
binaries carry the channel byte-meter fix (an IPC-decoded batch now charges
its true slice footprint, not ≈17× of it), so the remote arms ran the
configured in-flight window. Per remote cell against the 041 artifacts:

| Remote cell | wall (041 → now) | CPU user_sys | peak RSS |
|---|---|---|---|
| pg-to-pg-1m-remote | 1086.9 → 1002.1 ms (−7.8%) | 1000 → 920 ms (−8.0%) | 282 → 223 MB (**−59 MB**) |
| pg-to-s3parquet-1m-remote | 1024.4 → 952.9 ms (−7.0%) | 1050 → 1050 ms (0%) | 308 → 312 MB (+4 MB) |
| s3jsonl-to-pg-200k-remote | 1074.6 → 812.2 ms (−24.4%) | 900 → 920 ms (+2.2%) | 286 → 341 MB (**+55 MB**) |
| s3jsonl-to-s3parquet-200k-remote | 1119.1 → 1075.7 ms (−3.9%) | 960 → 1010 ms (+5.2%) | 308 → 301 MB (−7 MB) |
| pg-to-pg-dedup-1m-remote | 5354.3 → 4813.5 ms (−10.1%) | 960 → 1010 ms (+5.2%) | 268 → 244 MB (−24 MB) |

Every wall fell; CPU held within −8%/+5%; RSS moved net −31 MB across the
five. The 019 D-13/D-21 failure shape — widening the window buys wall and
pays more in resident memory — did not appear: the only RSS rise (+55 MB,
`s3jsonl-to-pg-200k-remote`, whose postgres DESTINATION child decodes
Arrow IPC and so was also over-charged before the fix) sits on the largest
wall win in the table (−262 ms). Either direction of the criterion would
have been a complete outcome; the measured one is a net win.

Two honest confounds, stated rather than netted. First, the machine ran
faster this session for BOTH modes — the in-process arms fell −4.5% to
−10.0% against 041 with no code change on their hot path — so the raw remote
wall deltas overstate the fix. The isolating figure is the twin-pair
overhead, where session speed largely cancels: `pg-to-pg-dedup-1m` +462.8 →
+216.5 ms (halved) and `pg-to-s3parquet-1m` +124.6 → +117.2 ms, while
`pg-to-pg-1m` sat flat (+190.0 → +194.7 ms) but dropped 59 MB of RSS and 8%
CPU — the fix's win is real but lands differently per shape. Second,
`s3jsonl-to-pg-200k-remote`'s −262 ms is mostly the 041 warm-up artifact
clearing, not the fix: 041's median carried first-spawn warm-up (±63%
spread, recorded not re-rolled); this session the same cell ran ±1.6%
(797.5–823.0 ms), landing on 041's own steady-state tail (≈858 ms) — the
warm-up shape did not recur.

**What the fix did NOT change**: the artifact `bytes` statistic. The fix
lives in the connector channel's budget meter; the engine's reporting sites
(`load/loader.rs`, `load/item.rs`, `runtime/extract.rs`) still sum buffer
capacity, so the pg-source remote rows still report the ≈12× figure
(2,413,845,024 B) and their MB/s column remains unreliable — the 041 caveat
below is amended, not retired. Closing the reporting site is bookkeeping,
not behavior, and is left for its own change.

## The wire overhead — recorded session 2026-08-07 (feature 041)

Hand-written from the ten 041 artifacts (superseded in `benches/results/` by
the 2026-08-10 session above; the figures here are the 041 session's own and
remain the record it produced). Same machine, same session, same fixtures,
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
RE-READ 2026-08-10 (the second session): CONFIRMED as session-state
sensitivity, not a regression — the same-magnitude asymmetry ran the
OPPOSITE direction (rdlt in-process arms −4.5%…−10.0% against these values,
dlt −3.0%…+5.3%), which a code regression cannot do. The mechanism stays
unproven; see the 2026-08-10 policy entry.

**Which way it cuts is the part that matters here: the ratios are DEFLATED.**
Every remote ratio in this session was divided by an rdlt wall that was high
relative to its own baseline, so the four bars cleared on a pessimistic
session. Start-of-cell loadavg ran 1.53–4.84 against a 32-core quiet threshold
of 8.0, so every cell passed the guard and none is `forced`. It is NOT the only
asymmetry in the session, and the other one cuts the other way — the remote arm
started on the quieter machine in four of the five pairs, which understates the
wire cost (see the load-symmetry caveat below). Neither is quantified, so
neither is netted against the other.

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
`user_sys` rises on every pair, by **×1.12 to ×1.89** — largest on
`pg-to-pg-1m` (530 → 1000 ms), smallest on `s3jsonl-to-s3parquet-200k`
(860 → 960 ms), so "roughly doubles" describes the top of the range and not
the middle of it — and on that largest pair peak RSS goes 113 → 282 MB, which
is the signature of an extra
encode/decode pass and a second process's buffers, not of process startup.

**The overhead band is LIKELY an upper bound — likely, not proven.** The
byte-accounting caveat below records that a decoded-over-the-wire Arrow batch
reports ≈17× its real footprint (≈12× its in-process twin's already-inflated
figure), and that the same expression meters source backpressure — so the
remote arms ran with a far smaller in-flight window than configured. Widening
it back should recover some of the +114…+463 ms, but that is the expected
direction, not a measured one, and a wider window also raises resident buffers
in a constellation whose peak RSS is already **×1.49 to ×2.48** the in-process
arm's (measured across the five pairs; largest on `pg-to-pg-1m`, and no pair
reaches 3×). The house
rule applies to this as to any counting argument: guilty until measured
(019 D-13/D-21). Nothing above is restated on that basis.

## Caveats

Stated so the numbers stay honest as the matrix fills:

- **Per-product timing boundaries** (what each column measures): rdlt is a
  process TREE — the release CLI plus the connector binaries it spawns —
  timed by the harness wall clock around the CLI, which pays for its
  children; dlt is a single-process pipeline timed by its own self-timed
  `seconds` line. The number is the pipeline, nothing else (see the
  connector-spawn caveat below). Airbyte's headline `seconds` is the **job wall**
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
  (full re-delivery + dedup by `id`); both products run the full-redelivery
  regime (Airbyte's arm retired 2026-08-11 with the in-process cells; its
  cheaper incremental mode was deliberately never benched — no dlt
  counterpart). The cell's note renders as the matrix caption. Its
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
- **The connector spawn — what the number covers** (041, recorded on the
  `-remote` ids; since 2026-08-11 these are the matrix's e2e rows): the
  pipeline runs with `connector:` refs, so the harness wall clock also
  covers spawning each connector binary, its handshake, config validation in
  the child, and every batch crossing a unix socket. The bins come from
  `<target>/release` unconditionally — a measured cell spawns the shipped
  shape, never a debug build. `peak RSS` and `CPU` are process-TREE samples,
  so the children are inside them: the rows' RSS is the whole
  constellation (**×1.49 to ×2.48** what the retired in-process recordings
  showed across the five pairs —
  ×2.48 / ×2.23 / ×1.65 / ×1.49 / ×2.44 in matrix order), not a regression in
  the engine.
  The competitor arms are byte-identical across the re-keying — dlt is not
  spawned differently — so the ratio column compares like with like.
- **`s3jsonl-to-pg-200k-remote` spread is warm-up, and it is recorded rather
  than re-rolled** (041 session): its five runs were 1435.5 / 1532.4 / 1074.6 /
  860.0 / 858.2 ms — ±63%, by far the widest in the session, with the cost
  falling monotonically after run 2. Its in-process twin was ±3% on the same
  fixture in the same session, so the shape belongs to the remote path (first
  spawns of the file+postgres bins paying page-cache and allocator warm-up),
  not to the cell. The **median as measured** (1074.6 ms → 60.0×) is what is
  recorded and what the verdict uses; the steady-state tail would read ≈75×,
  and quoting that instead would be picking the number. No re-roll was taken.
  AMENDED 2026-08-10: the shape did not recur — the second session ran the
  same cell at ±1.6% (797.5–823.0 ms, median 812.2), on 041's own
  steady-state tail. The warm-up read was correct and is now bounded to that
  one session.
- **The twin pairs are NOT load-symmetric, and the asymmetry cuts AGAINST the
  wire — disclosed because the other two caveats both cut for it** (041
  session). Baseline-first ordering runs each in-process arm before its remote
  twin, and the machine kept settling in between: `loadavg_at_start` fell from
  the in-process arm to the remote arm on four of the five pairs — `pg-to-pg-1m`
  1.99 → 1.90, `pg-to-s3parquet-1m` 2.13 → 1.95, `s3jsonl-to-pg-200k`
  2.26 → 1.53, `pg-to-pg-dedup-1m` 4.84 → 2.11 (the fifth,
  `s3jsonl-to-s3parquet-200k`, was flat at 1.61 → 1.60). So the REMOTE arm
  generally ran on the quieter machine, and the measured overhead
  (+114…+463 ms) is if anything an UNDER-statement of what the wire costs on
  equal footing. This is the opposite direction from the two caveats above —
  the deflated-ratio observation and the throttled-window one both argue the
  remote numbers are pessimistic — and all three are unquantified, so none of
  them is netted against another or used to restate a figure. **The verdict is
  unmoved either way**: all four bars clear when each remote cell's SLOWEST of
  five runs is used instead of its median (9.0× / 42.1× / 50.3× / 2.3× against
  4 / 40 / 45 / 2), which is a harder test than any load correction implies.
  Every cell passed the quiet guard (loadavg below 0.25×cores = 8.0) and none
  is stamped `forced`.
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
  window also raises resident buffers, and the remote peak RSS is already
  **×1.49 to ×2.48** the in-process arm's, so the net is a measurement nobody
  has taken. No number
  here is restated on the strength of an unmeasured fix.
  AMENDED 2026-08-10: the measurement was taken, and the budget half of this
  caveat is RETIRED — 042's channel fix (commit `ae181184`) meters an
  IPC-decoded batch by its true slice footprint, and the second recorded
  session judged it STANDS (every remote wall down, net RSS down; see the
  2026-08-10 section above). The "upper bound" hedge resolved as partly
  right: the throttled window was real on the dedup pair (overhead halved)
  and negligible on `pg-to-pg-1m`'s wall (flat overhead, but −59 MB RSS and
  −8% CPU). The REPORTING half of this caveat still stands as written: the
  engine's `bytes` statistic capacity-sums at its own sites, the pg-source
  remote rows still print ≈12× figures, and their MB/s stays unreliable
  until that bookkeeping site is closed.
  AMENDED 2026-08-11 (the D1 swap): those recordings now render under the
  base ids, so read the standing warning against the matrix as printed —
  the MB/s column on the pg-source rows (`pg-to-pg-1m`,
  `pg-to-s3parquet-1m`, `pg-to-pg-dedup-1m`) is unreliable until the
  engine's reporting sites are closed; the file-source rows are unaffected.
  AMENDED 2026-08-14 (the 046 re-point): the matrix as printed no longer
  carries the ≈12× figure. 042's `06b2bc2a` re-pointed the engine's
  report-total sites at the channel's footprint meter (its own claim:
  stage window, commit policy, report totals) right after the 2026-08-10
  session recorded, and the 2026-08-13/14 session is the first RECORDED
  one under it: `pg-to-pg-1m` reports 178,240,270 B for the identical
  1M×12 workload where the archived row reported 2,413,845,024 B.
  PLAUSIBLE, not audited — two residuals are unreconciled: that 178 MB
  sits ≈25% above what this caveat's own arithmetic predicts for the
  honest footprint (≈143 MB — the in-process 199.7 MB divided by the
  ≈1.4× builder overshoot), and the file-source rows report −11% against
  their archived byte-identical datasets (133,489,034 B vs 149,965,184 B
  for `s3jsonl-to-pg-200k`) though neither dataset changed. Read the
  fresh bytes/MB/s as channel-metered and roughly right, not as an
  audited count; full confidence needs a one-cell A/B against a known
  byte volume — an owner note, not taken here. Rows in the dated archive
  keep their inflated figures and this caveat is how to read them.
- **Cold start** lives on the instruments track, not the matrix: a one-row
  file → duckdb pipeline that spawns both connector bins (two
  spawn+handshakes inside the measured wall since the D1 swap, 2026-08-11),
  ≤ 40 ms absolute (`benches/harness/check-cold-start.sh`,
  run by `TARGET=cold make bench` and therefore `make check`).

## Trends

Generated from `benches/history.jsonl` (one line per cell×variant per recorded
invocation) — the latest two medians per pair and their delta.

<!-- rdlt-bench:BEGIN trends -->
| Cell | Variant | Latest | Previous | Δ |
|---|---|---|---|---|
| oracle-to-pg-200k | dlt | 3.37 s | — | — |
| oracle-to-pg-200k | rdlt | 878.4 ms | — | — |
| pg-to-pg-1m | dlt | 10.72 s | 10.47 s | +2.4% |
| pg-to-pg-1m | dlt-pyarrow | 18.10 s | 17.63 s | +2.7% |
| pg-to-pg-1m | rdlt | 894.2 ms | 1.02 s | -12.3% |
| pg-to-pg-dedup-1m | dlt | 12.91 s | 13.00 s | -0.7% |
| pg-to-pg-dedup-1m | dlt-pyarrow | 20.99 s | 20.75 s | +1.2% |
| pg-to-pg-dedup-1m | rdlt | 4.77 s | 5.18 s | -7.8% |
| pg-to-s3parquet-1m | dlt | 1.88 s | 1.86 s | +0.8% |
| pg-to-s3parquet-1m | dlt-pyarrow | 11.42 s | 11.11 s | +2.8% |
| pg-to-s3parquet-1m | rdlt | 831.6 ms | 960.6 ms | -13.4% |
| s3jsonl-to-pg-200k | dlt | 66.69 s | 64.13 s | +4.0% |
| s3jsonl-to-pg-200k | rdlt | 737.3 ms | 801.1 ms | -8.0% |
| s3jsonl-to-s3parquet-200k | dlt | 61.28 s | 58.67 s | +4.4% |
| s3jsonl-to-s3parquet-200k | rdlt | 1.00 s | 1.07 s | -6.6% |
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
