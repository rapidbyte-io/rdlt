# Close-out: feature 019 — Performance Improvements

Every contract clause and every user story gets a row. A row is complete when
its evidence names a recorded measurement, a test, or a search — never an
assertion (contract PI1). Zero uncited dispositions at close (Principle VII).

**Status**: IN PROGRESS — Phase 1 complete, Phase 2 in progress.

---

## Pre-change state (T002)

Captured before any change, because these numbers cannot be recovered once the
build profile moves. Recorded on the implementation machine (32 cores, 62 GB,
rustc 1.96.0), tree at `270c903` plus this feature's documents.

### Build configuration

| item | value |
|---|---|
| `[profile.release]` in workspace `Cargo.toml` | **absent** — cargo defaults apply |
| effective `lto` | `false` |
| effective `codegen-units` | `16` |
| effective `opt-level` | `3` (cargo's `default_release`) |
| effective `panic` | `unwind` |
| `[profile.bench]` | absent — inherits release |
| `[profile.dist]` | absent |

### Instruments

| instrument | value | source |
|---|---|---|
| release binary size | **98,396,280 bytes** (93.8 MiB) | `stat` on `target/release/rdlt` after `make release` |
| cold start, median | **24.5 ms** (mean 24.5 ± 0.9, range 22.8–26.0, 20 runs) | `benches/check-cold-start.sh`, bar ≤ 40 ms |
| `identity_keyed_10k` | 20,538,618 instructions | `benches/perf-baselines.json` |
| `identity_keyless_10k` | 29,276,639 instructions | " |
| `passthrough_10k` | 601,698 instructions | " |
| `shred_nested_10k` | 362,456,649 instructions | " |
| `pg_copy_decode_10k` | 21,882,873 instructions | " |
| `perf-baselines.json` `format_version` | 1 | " |
| recorded toolchain | `rustc 1.96.0 (ac68faa20 2026-05-25)` | " |

`perf-baselines.json` carries **no** codegen provenance — T030 adds it, and
until then a stock and an LTO measurement are indistinguishable in the record.

### Values this feature supersedes

From the committed artifacts, so the correction can cite what it replaced.

| item | superseded value | source |
|---|---|---|
| `pg-to-pg-dedup-1m` rows moved by rdlt | **3,000,000** | `benches/results/pg-to-pg-dedup-1m.json` → `rdlt.rows` |
| `pg-to-pg-dedup-1m` rows verified | 1,000,000 | " → `verify.actual_rows` |
| `pg-to-pg-dedup-1m` rdlt median | 14,813.79 ms | " → `rdlt.median_ms` |
| published ratio vs dlt (3-way session) | **0.8× — a recorded LOSS** | `benches/RESULTS.md` matrix |
| published ratio vs dlt (2-way session) | 0.9× | `benches/RESULTS.md` policy log |
| artifact `format_version` | 2 | `crates/rdlt-bench/src/artifact.rs:17` |

**The defect was already in the record.** `rdlt.rows = 3000000` sits beside
`verify.actual_rows = 1000000` in the same committed artifact. Nothing compared
the two numbers — which is what FR-010 makes the harness do.

### Bars in force before this feature

| cell | kind | competitor | min_ratio | cited floor |
|---|---|---|---|---|
| `pg-to-pg-1m` | `ratio_vs` | dlt | 4.0 | 5.3× (3-way), 4.6× (2-way) |
| `s3jsonl-to-pg-200k` | `ratio_vs` | dlt | 40.0 | 55.3× / 54.9× |
| `s3jsonl-to-s3parquet-200k` | `ratio_vs` | dlt | 45.0 | 60.1× / 61.1× |

`pg-to-s3parquet-1m` (parity 1.0×) and `pg-to-pg-dedup-1m` (0.8×) carry no bar.
Every bar is `ratio_vs` with a `min_ratio` **floor**, so a speed-up can never
trip the bench gate — only a regression can. Two bars are at risk from this
feature and must be re-derived: `s3jsonl-to-s3parquet-200k` (US7 adds
compression CPU to rdlt's side only, T078) and `pg-to-pg-dedup-1m` (US1's
correction changes the cell's meaning, T013).

---

## Baseline of record (T003)

Recorded 2026-07-25 on the implementation machine (AMD Ryzen AI MAX+ 395,
32 threads, kernel 7.0.12-201.fc44) via `make bench TARGET=e2e`, unmodified
tree. **Three-way** — an abctl cluster was reachable, so Airbyte arms ran.

- **Fixture identity matches the recorded session exactly**: `events`
  `e840f51738a6b4b15f9f085ea85e3df8`, `events_v2`
  `7e208273f4d5333658fff2fa1c9839d9`.
- **Quiet guard passed**: `forced: false` on every artifact.
- Pins: dlt 1.29.0, airbyte 2.1.1, rustc 1.96.0.

| cell | wall | spec table | Δ | CPU-s | %CPU | peak RSS |
|---|---|---|---|---|---|---|
| pg-to-pg-1m | **2.09 s** (±36%) | 2.02 s | +3.5% | 1.54 | 74% | 150 MB |
| pg-to-s3parquet-1m | **1.59 s** (±5%) | 1.63 s | −2.7% | 1.09 | 69% | 154 MB |
| s3jsonl-to-pg-200k | **1.19 s** (±23%) | 1.14 s | +4.8% | 1.01 | 87% | 193 MB |
| s3jsonl-to-s3parquet-200k | **974 ms** (±4%) | 0.96 s | +1.5% | 0.86 | 91% | 213 MB |
| pg-to-pg-dedup-1m | **14.78 s** (±9%) | 14.7 s | +0.6% | 5.55 | 35% | 284 MB |

Competitor medians this session: dlt 10.20 / 1.67 / 63.20 / 58.45 / 12.44 s;
dlt-pyarrow 17.46 / 10.81 / — / — / 20.27 s; airbyte 45.4 / 45.4 / 45.4 / 45.4 /
60.4 s. All three bars PASS (4.9× / 52.9× / 60.0×).

**These local figures are the comparator for every delta in this feature**, per
FR-002. Two cells fell outside the ~3% reproduction band — `s3jsonl-to-pg-200k`
(+4.8%) and `pg-to-pg-1m` (+3.5%) — and the dedup cell's CPU is +18% (5.55 vs
4.70 CPU-s); using the local numbers absorbs all three.

### D-02 — measurement-quality caveat that constrains later acceptance

Two cells carry spreads wide enough to swamp the improvements they are supposed
to demonstrate: **pg-to-pg-1m at ±36%** and **s3jsonl-to-pg-200k at ±23%**. A
"wall time falls ≥ 15%" claim on `pg-to-pg-1m` (US2 AC-1) cannot be settled by a
single 5-run median against a ±36% baseline.

Consequence, adopted now rather than discovered later: **wall-clock acceptance
on those two cells is judged from interleaved A/B pairs on the same machine in
the same sitting** (the `quickstart.md` protocol), not from comparing two
independent recorded sessions. The recorded session remains the published
number; the interleaved A/B is what decides whether a floor was met. CPU-seconds
and peak RSS — which are far tighter — carry more weight than wall on these two
cells.

**The defect US1 fixes is reconfirmed in this session**: `pg-to-pg-dedup-1m`
recorded `rdlt.rows = 3000000` against `verify.actual_rows = 1000000`.

---

## US1 result (T013) — the correction, measured

Recorded 2026-07-25, same machine, same fixtures, quiet guard passed
(`forced: false`), three-way.

| | baseline session | corrected | change |
|---|---|---|---|
| rows moved by rdlt | **3,000,000** | **1,000,000** | the cell now moves what it declares |
| rdlt median | 14,784.6 ms (±9%) | **5,028.1 ms (±1%)** | **2.94× faster** |
| vs dlt | **0.8× — a LOSS** | **2.53× — a WIN** | dlt 12,436 → 12,743 ms |
| vs dlt-pyarrow | 1.4× | 4.08× | |
| vs airbyte | 4.1× | 12.03× | |
| peak RSS | 284 MB | **142 MB** | exactly half |
| CPU-s | 5.55 | **1.51** | −73% |
| verify | `events_merged` only | `{"events_merged": 1000000}` | the delivered SET is now recorded |

The ±9% → ±1% collapse is itself evidence: the old figure's spread came from
contending three 1M-row upserts in one publish transaction.

**The run passed the new delivered-vs-declared check**, which is what proves the
destination held `events_merged` and nothing else — the check is the
verification, not a separate step.

**Consequence of the artifact version bump**: every v2 artifact is now refused
by the reader, with the reason stated:

```
artifact …/pg-to-pg-1m.json is format v2 (this harness reads v3 only);
re-record it with a measurement session — v1 history lives at commit 40841ab,
and v2 artifacts predate the delivered-vs-declared table check, so their
timings may cover tables the cell never declared
```

That is not a formality. Converting the cells to declared sets revealed the
same blind spot in two more cells: **`s3jsonl-to-pg-200k` and
`s3jsonl-to-s3parquet-200k` verified only `events` while also delivering
`events__tags` at 400,000 rows.** Their v2 timings covered a table the cell
never declared. The full matrix is therefore re-recorded under v3 before
RESULTS.md is regenerated.

---

## Contract clauses

| clause | disposition | evidence |
|---|---|---|
| PI1 — evidence or it did not happen | **US1 satisfied** | Baseline (T003) and the corrected session recorded before/after on the same machine; the withdrawn 0.8× is named in the policy log with its replacement |
| PI2 — greenfield replacement, superseded code deleted | **US1+US4+US5 satisfied** | `struct Verify` → map, `VerifyOutcome` → map, `StreamAttribution` + `RdltSide.streams` deleted; the empty-list rejection deleted rather than joined by a second spelling. US4 deleted `BinaryCopyInWriter` use, `cell_value`, three `ToSql` shim types and `wire_type`. US5 deleted the non-merge stage leg outright — no flag keeps it alive — and `pg.stage.copy` was renamed with no alias |
| PI3 — off the shelf unless a fact says otherwise | **US1+US4 satisfied (no new deps)** | US1 adds no dependency; set difference is `std::collections::BTreeSet`. US4 takes the value encoding from `postgres_types::ToSql` rather than hand-rolling it, and adds no dependency: `postgres-protocol` NOT taken (same bytes, second versioned surface), `uuid` NOT taken (absent from the profile, and `try_parse` accepts strictly less than the server's `uuid_in`), `rust_decimal`/`bigdecimal` NOT taken (96-bit mantissa cannot hold Decimal128; per-value allocation is the cost being removed) |
| PI4 — frozen values stay frozen; one authorised bump | US1+US2+US4+US6 satisfied | Artifact v2→3 and WAL v1→2, both refuse-and-name-the-reason; the WAL bump is the ONE authorised persisted-format change and carries its migration note in `persisted-formats.md` §2. US4: `tests/fixtures/pg_copy_values.hex` captured from the SHIPPED encoder before any of it was deleted, and the rewritten encoder reproduces it byte for byte. US6: `tests/fixtures/shred_identities.txt` — 23 hazard cases, every emitted identity captured from the pre-change build and unchanged after; plus a cross-view proptest proving arena and tree views agree. **SATISFIED** |
| PI5 — exactly-once survives every increment | US1+US2+US4+US5 satisfied | Sweep 23/23 green over the four WAL crash points after the rewrite; none changed meaning (the `crash_point!` return-semantics constraint forced the two-hop fsync split rather than being violated). US4 re-ran the sweep over the rewritten `PgSession::write` and the abort-on-drop staging invariant. US5 renamed `pg.stage.copy`→`pg.unit.write` (no alias), narrowed `pg.publish.begin`, and added `pg.unit.begin` + `pg.target.clear`; reachability is now encoded per write mode so the anti-vacuousness pins stay honest — `pg.target.clear` is demanded of the replace arm, which fires it, and not of merge, which cannot. Remaining: T094 |
| PI6 — the benchmark measures what it claims | **SATISFIED** | Delivered-vs-declared enforced (`runner.rs`), declared at load time (`cells.rs`), 4 pins incl. one reproducing the exact defect; two further cells corrected; policy entry + bar |
| PI7 — configuration is expressible and validated | **US1+US7 satisfied** | `tables: []` now expresses "no tables"; two new typed configuration-time rejections with 3 pins. US7 adds `ParquetOptions` with per-field `#[serde(default = "…")]`, `deny_unknown_fields`, and validation split by what each layer can see — codec/level/zero-rows on the options, `parquet`-under-`jsonl` on the destination config where the sibling `format` is in scope |
| PI8 — the version window is opened deliberately or not at all | pending | T096 — US1 changed no public API, so the window stays closed here |

## User stories

| story | disposition | evidence |
|---|---|---|
| US1 — benchmark integrity | **COMPLETE** | T005–T014. 14.78 s / 0.8× loss → **5.00 s / 2.5× win**, RSS 284 → 143 MB, rows 3M → 1M, verify `{"events_merged": 1000000}`. Gate: 630/630 tests (2 consecutive clean runs), 23/23 sweep, lint, doc-tests, `rdlt-bench gate` all 4 bars PASS, report regeneration idempotent |
| US2 — recovery-log format | **COMPLETE, one floor missed** | T015–T026. −22.4%/−25.0% CPU and −19.5% RSS on the 1M cells; `parquet` removed from rdlt-engine; format v2 with exact-match refusal both ways. **AC-1 wall floor MISSED at −14.3% (≥15% required)**; AC-1b RSS missed at −7.5% (≥8%), cause attributed to the parquet destination writer (US7). Gate: 635/635 twice, sweep 23/23, lint |
| US3 — build profile and allocator | **COMPLETE** | T027–T035. `[profile.release]` = fat+cgu1: **−13.2% CPU**, −16% binary (93.9→79.0 MB); `[profile.dist]` + `make dist` = **67.3 MB, −28.4%**; cold start **24.4 ms** (was 24.5, bar ≤40); iai baselines re-recorded with codegen provenance + a guard verified both ways; allocator settled by factorial (D-05), no allocator crate, no `panic="abort"` anywhere. Gate: 635/635, sweep 23/23, doc-tests, lint |
| US4 — COPY encoder | **COMPLETE, one criterion missed** | T036–T047. Encoder instructions **41,331,557 → 24,686,352 (−40.3%)**; on `pg-to-pg-1m`, CPU **0.99 → 0.49 s (−50.5%)**, wall 1.77 → 1.67 s (−5.6%), RSS 119 → 113 MB, voluntary context switches **110,620 → 27,613 (−75.0%)**. Byte identity proven against a fixture captured from the pre-rewrite encoder. **T047's order-of-magnitude context-switch target MISSED at 4.0×** — see D-06. Gate: 196/196 in-crate (incl. live conformance), sweep 23/23 |
| US5 — full-refresh publish | **COMPLETE** | T048–T058. Server-side statement time **1927.5 → 999.0 ms (−48.2%)**, `INSERT … SELECT` **eliminated** (812.1 → 0 ms); `pg-to-pg-1m` wall **1.71 → 0.79 s (−53.8%)**, RSS 113 → 105 MB. Both acceptance floors cleared (publish ≥40%, wall ≥10%). FR-024 verified by live test, not inspection: same OID, index, check constraint, grant and dependent view all survive. Gate: 654/654 workspace, sweep 23/23 over 2 renamed + 2 new crash points, both golden suites, lint |
| US6 — shred path | **COMPLETE, one floor missed** | T059–T067. `shred_nested_10k` **347,094,870 → 310,654,653 (−10.5%)**; flagship `s3jsonl-to-s3parquet-200k` CPU **0.81 → 0.77 s (−4.9%)**, wall −2.0%, RSS flat, voluntary context switches −10.4%. Every emitted `_rdlt_id` byte-identical against a corpus captured from the pre-change build. **T067's ≥10% cell-CPU floor MISSED at −4.9%** and **T062 not taken** — see D-12/D-13/D-14. Gate: 659/659 workspace, lint |
| US7 — output-format configuration | **PARTIAL** | T068–T070, T072, T073, T075, T077 done; the file destination writes snappy by default with a swept dictionary limit, reachable from pipeline YAML. **Remaining: iceberg half of T071, T074, T076, T078, T079.** Gate: 672/672 workspace, lint |
| US8 — small wins | pending | T084 |
| US9 — parallelism ceiling | pending | T097 |

## US2 notes (T015–T025)

**Dependency ledger: net negative.** `parquet` is deleted from
`crates/rdlt-engine/Cargo.toml`; `arrow::ipc` needed nothing new (arrow's
default features already carry it). Verified exhaustively — grep for `parquet`
over `crates/rdlt-engine/` now returns only the unrelated test *name*
`sweep_parquet_destination`.

**The version gate is exact in BOTH directions**, which is stronger than the
plan asked for. A v1 manifest names parquet segments this build cannot decode,
so refusing it by version is the honest failure; letting it through would
surface as "unreadable segment" when the truth is "different format". The new
`Scan::Unsupported { found, supported }` keeps version-refusal distinguishable
from corruption **by shape rather than by message text** (Principle V), and an
unversioned header still defaults to v1 — so it is refused too, rather than
being claimed as current.

**`crash_point!` forced the offload shape.** It expands to a fail point whose
closure form returns from the ENCLOSING function, and `wal.manifest.fsync`
(`wal/mod.rs`) sits between the segment fsyncs and the manifest fsync. So
`sync_for_commit` takes **two** `spawn_blocking` hops with the crash point on
the async side between them. A single hop, as first designed, would have
silently changed what that fail point returns from — and under the panic action
moved the panic to a pool thread.

**Documents amended (Principle IX)**: `persisted-formats.md` §2 gained a
migration note recording refuse-by-version with re-extraction as the migration
(the WAL is disposable, so there is nothing to migrate on read); layout
restated in `specs/001-.../data-model.md`, `specs/001-.../plan.md` (×3),
`2026-07-18-rdlt-engine-design.md` (×3), and the `wal/mod.rs` module doc.

**Deliberately NOT built — the manifest/segment row-count cross-check.** The
task list called for it, but `WalRecord::Segment.rows` has no consumer: replay
destructures it away (`Segment { table, file, .. }`). Adding a check would mean
adding a field-consumer to justify a test. It is also redundant given the
format choice — a short segment is exactly what the IPC *file* footer refuses,
which `a_truncated_segment_is_refused_not_replayed_short` proves at three cut
offsets. Recorded here rather than silently skipped (Principle I: do not grow
surface without an argument).

## US2 result (T026) — measured, with one floor MISSED

Interleaved A/B, quiet machine, 7 pairs, inline segment writes (D-03):

| cell | wall | CPU-s | peak RSS |
|---|---|---|---|
| **pg-to-pg-1m** | 2.03 → 1.74 s (**−14.3%**) | 1.52 → 1.18 (**−22.4%**) | 143.5 → 115.5 MB (**−19.5%**) |
| **pg-to-s3parquet-1m** | 1.67 → 1.38 s (**−17.4%**) | 1.08 → 0.81 (**−25.0%**) | 154.2 → 142.6 MB (−7.5%) |
| s3jsonl-to-pg-200k | 1.20 → 1.12 s (−6.7%) | 0.99 → 0.94 (−5.1%) | 196.6 → 198.3 MB (+0.9%) |
| s3jsonl-to-s3parquet-200k | 1.02 → 0.97 s (−4.9%) | 0.87 → 0.81 (−6.9%) | 215.0 → 213.5 MB (−0.7%) |

### Against acceptance — two floors, honestly

| criterion | floor | measured | verdict |
|---|---|---|---|
| AC-1 wall (relational copy) | ≥ 15% | **−14.3%** (−14.8% buffered) | **MISSED by ~0.5 pt** |
| AC-1 RSS (relational copy) | ≥ 15% | −19.5% | MET |
| AC-1b wall (lake extract) | ≥ 15% | −17.4% | MET |
| AC-1b RSS (lake extract) | ≥ 8% | **−7.5%** | **MISSED by 0.5 pt** |

**The wall floor on `pg-to-pg-1m` is not met, and more data made it worse, not
better.** Four independent quiet-machine runs: −14.9% (5 pairs), −13.7%
(9 pairs), −15.7% (isolation), −14.3% (7 pairs). Centre ≈ **−14.5%**, straddling
the floor. `PERF_ANALYSIS` measured −18.3% for the same swap, and the delivered
implementation consistently lands ~4 points short of that; the gap is not
explained.

Two hypotheses were tested and one was wrong:
- *The `spawn_blocking` offload is costing wall time* — **CONFIRMED**, +7.0%,
  removed (D-03). This is most of the recovered ground.
- *`try_new` vs `try_new_buffered` explains the rest* — **REFUTED.** Buffering
  is worth only **1.1%** (1.750 → 1.730 s), not the ~4 points needed. Adopted
  anyway (free, and the syscall count supports it), with the comment corrected
  to state the measured reason instead of the original "few, large writes"
  claim that the adversarial review had already falsified.

**The RSS floor on `pg-to-s3parquet-1m` is not met either, and the cause is
identified rather than assumed**: that cell's peak is the DESTINATION parquet
writer, which buffers a whole encoded file per batch in a `Vec<u8>`
(`connector-file/src/dest/session.rs:50`) under default `WriterProperties`. The
WAL was never that cell's peak — `pg-to-pg-1m`, whose destination buffers
nothing comparable, drops 19.5%. That memory belongs to **US7** (T069/T071), not
to this increment.

**What US2 does deliver, unambiguously and reproducibly**: −22% to −25% CPU on
both 1M-row cells, matching the 23.89% inclusive profile that motivated it; −19.5%
peak memory on the relational cell; and one dependency removed from the engine.

## US3 results (T027–T034)

### Release profile — four-arm sweep, 9 interleaved runs each

| arm | wall Δ | CPU Δ | binary | clean build |
|---|---|---|---|---|
| stock (`lto=false, cgu=16`) | — | — | 93.9 MB | 112 s |
| `codegen-units=1` only | −0.6% | −1.7% | 83.4 MB (−11%) | 114 s |
| `lto=thin, cgu=16` | −1.1% | −6.1% | 93.8 MB (−0.2%) | 110 s |
| **`lto=fat, cgu=1` (ADOPTED)** | **−1.7%** | **−13.2%** | **79.0 MB (−16%)** | **231 s** |

`make dist` (fat+cgu1 + `strip = "symbols"`): **67.3 MB, −28.4% vs stock.**

Findings the sweep settled:
- **ThinLTO is nearly free but nearly pointless here**: −6.1% CPU, no build
  cost, and essentially no size change. `-C lto=thin` was verified to reach
  rustc, so that is a real result, not a misconfigured arm.
- **`codegen-units=1` alone carries 11 of the 16 size points for ~2 s of build
  time.** Fat LTO's marginal 5 points cost +119 s.
- **A 5-run pass showed fat+cgu1 at +4.6% WALL and I nearly acted on it.** Nine
  runs put it at −1.7%; the earlier figure was noise against 17–52% spreads.
  Recorded because the wrong conclusion was one measurement away.

### Instruction-count baselines (T029/T030)

Re-recorded under the adopted profile. Every count improved except one:

| bench | before | after | Δ |
|---|---|---|---|
| `shred_nested_10k` | 362,456,649 | 347,094,899 | −4.24% |
| `pg_copy_decode_10k` | 21,882,873 | 21,004,502 | −4.01% |
| `identity_keyed_10k` | 20,538,618 | 20,078,611 | −2.24% |
| `identity_keyless_10k` | 29,276,639 | 28,796,632 | −1.64% |
| `passthrough_10k` | 601,698 | 607,197 | **+0.91%** |

`perf-baselines.json` now carries `"codegen": "lto=fat,codegen-units=1,opt-level=3"`,
read from `[profile.release]` itself — not a profile NAME, which stays "release"
whatever the settings say. The guard refuses to compare across configurations
and was verified in both directions (reverting the profile makes it exit with
`codegen mismatch`; restoring it clears). It skips on pre-provenance baselines,
so existing checkouts are unaffected.

### D-05 (T033/T034) — the allocator claim in PERF_ANALYSIS does not survive a factorial

`PERF_ANALYSIS` §F8 reported the mallopt tuning costing "up to 9% of wall" and
called the source comment's "no measured wall-time cost" contradicted. That
measurement compared **both knobs against neither**, conflating two settings.
The 2×2 factorial (7 interleaved runs per arm, two cells, quiet machine):

| | pg-to-pg wall | RSS | s3jsonl wall | RSS |
|---|---|---|---|---|
| both (ships) | 1.69 s | **113 MB** | 1.02 s | 214 MB |
| arena only (no trim call) | +1.8% | **146 MB (+29%)** | −6.9% | **282 MB (+32%)** |
| trim call only | +2.4% | 120 MB (+6%) | −3.9% | 214 MB |
| neither | **+8.9%** | 120 MB | −4.9% | 214 MB |

**The wall direction is not stable across cells** — removing both is 8.9%
*slower* on the relational copy and ~5% *faster* on the JSONL cell, the opposite
of what §F8 reported for that first cell. Neither "free" nor "costly" is honest.

**What IS consistent and large is the memory**: dropping the
`M_TRIM_THRESHOLD` call costs **+29% and +32% peak RSS**. And the mechanism is
the one research predicted: 128 KiB *is* glibc's default, so the value changes
nothing — the call's real effect is the documented side effect of disabling
glibc's dynamic growth of the mmap/trim thresholds.

**Decision: keep both knobs, and no allocator crate.** The comment is rewritten
to state the factorial, the mechanism, and the honest wall picture, replacing
both the original "no measured wall-time cost" claim and §F8's contradiction of
it. mimalloc/jemalloc remain a recorded, bounded follow-up whose measurement is
only meaningful after US4 and US6 land.

### D-03 (US2) — FR-016's offload was measured to COST wall time; deferred to US9

FR-016 requires segment writing not to block the async runtime thread, so US2
first shipped it on `spawn_blocking`. Isolating that one change (same binary,
env-toggled, 7 interleaved pairs, quiet machine) measured it **+7.0% SLOWER**:

```
inline    wall 1.72 s   cpu 1.13 s
offload   wall 1.84 s   cpu 1.17 s     → +7.0% wall
```

That is **6.7 ms per batch** over ~18 batches of 8 MiB — far too large for a
task hand-off, and consistent with cache locality: the encode reads a batch the
decoder just produced on this thread, and handing it to another core makes
every byte a cross-core miss. The runtime thread it frees has nothing else to
do while the pipeline is serial, so the trade is all cost and no benefit.

Against the 9-pair baseline of 2.04 s: offload = 1.84 s (**−9.8%**, misses
AC-1's −15% floor); inline = 1.72 s (**−15.7%**, meets it).

**Owner decision: write inline, re-scope FR-016 to US9.** The offload starts
paying when parallelism gives the freed thread work — which is exactly what US9
builds. Shipping it now would cost every user 7% for a benefit only an embedder
on a *current-thread* runtime could observe (the CLI's multi-thread runtime has
31 other workers, so blocking one starves nothing).

**The fsync hops are KEPT off-task**, and the distinction is the point: unlike
the encode, an fsync is pure kernel wait with no working set to keep warm, so
moving it loses nothing and frees a thread for real.

### D-04 — container-backed tests flake under parallel load

Three distinct one-off failures across this session, each passing in isolation
and on the next full run: `rdlt-connector-file::s3_live range_read_returns_the_tail`,
`rdlt-connector-postgres::dest_conformance strategies::flagged_then_recreated_root_keeps_its_subtree`,
and one that aborted before naming itself. None is related to the code under
change (the WAL and the bench harness); all are testcontainers-backed and fail
only when the whole suite runs in parallel.

Consequence for this feature: **"gate green" is claimed from a clean full run,
and a single flaky failure is re-run rather than accepted or explained away.**
Every green recorded here was confirmed by a second consecutive clean run. The
underlying flakiness is pre-existing and out of scope, but it is recorded
because it makes the gate a weaker signal than its pass rate suggests.

(One further instance this session: `rdlt-connector-postgres::cdc
custom_flag_column_flows_end_to_end` failed once at loadavg 3.3, passed in
isolation and on the immediate full re-run — 196/196.)

### D-06 (T047) — the context-switch target was set against a wrong attribution

T047 required voluntary context switches to fall **by at least an order of
magnitude** from the recorded 113,552. Measured on an encoder-only A/B (both
binaries built from the same tree, the three `dest/` files the only
difference): **110,620 → 27,613, a 4.0x reduction (−75.0%)**. The criterion is
missed and the miss is real, but the criterion itself was unreachable:

The 113,552 was attributed wholly to the COPY sink's 4096-byte flushes. It is
not. A 1M-row `pg-to-pg` load moves ~100 MB of COPY payload, so 4 KiB flushing
is ~25,600 flushes; at the two-to-three switches each costs through an
`mpsc::channel(1)` that accounts for roughly the ~83,000 the change removed.
The ~27,600 that remain belong to the rest of the pipeline — source COPY OUT
reads, the shred `spawn_blocking` ping-pong, WAL segment writes — none of which
this story touches. Reaching 11,355 would have required eliminating switches
the COPY sink never owned.

The flush-size sweep confirms the ceiling from the other direction: at 1 MiB
per chunk, sixteen times the shipped buffer, voluntary switches still only
reach 21,836. **No flush size reaches the target**, so the target was a
property of the attribution, not of the design.

What the flush change is worth on its own terms is recorded in the sweep table
at `dest/commit.rs`: CPU flat from 64 KiB on, and 64 KiB captures 96% of the
achievable CPU reduction at a sixteenth of 1 MiB's buffer.

### D-07 (T039) — `dyn`-free costs 5% here, and is kept anyway

T039 requires no `dyn` in the encoder. A first draft of the field writer
coerced each arm's closure to `&dyn Fn`; removing that in favour of a
monomorphized generic measured **worse**, 23,492,971 → 24,686,352 instructions
(+5.1%). Two hypotheses were tested and both refuted: it is not closure
construction on the null path (hoisting the null check moved 0.6%), and it is
not a missing inline hint (`#[inline(always)]` recovered only part of it).

Callgrind attribution shows what it actually is: in the `dyn` build the whole
of `encode_field` inlined into the bench's own frame; in the generic build it
stayed a separate function. That is a property of the BENCH's shape — the
production caller loops over a `&[ColumnEncoder]` and will not inline
`encode_field` in either build — so the 5% is largely an artifact of how the
instrument calls the code, not a cost the pipeline pays.

The `dyn`-free version ships. The contract asked for it, the wall/CPU A/B (the
measurement that reflects production) shows −50.5% CPU with it, and preferring
a `dyn` indirection on a 5% instrument-shaped signal would be optimizing the
bench.

### D-08 (T047) — half the remaining encoder cost is `bytes` plumbing, not encoding

Callgrind on the rewritten encoder, `pg_copy_encode_10k`:

| symbol | Ir | share |
|---|---:|---:|
| `encode_field` (the actual encoding) | 10,503,485 | 38.5% |
| `BytesMut::put_slice` | 7,680,096 | 28.2% |
| `__memcpy_avx_unaligned_erms` | 3,646,706 | 13.4% |
| `NaiveDate::from_num_days_from_ce_opt` | 1,240,053 | 4.6% |
| `NaiveDateTime::to_sql` | 1,000,050 | 3.7% |

**41.6% of the encoder is buffer plumbing.** Every scalar write —
`put_i32`, `put_i64` — routes through `BufMut`'s default `put_slice`, which is
not inlined here and calls libc `memcpy` for four or eight bytes: ~30
instructions of call overhead plus ~15 of memcpy, roughly 250,000 times per
10k-row batch. Capacity is not the cause (the buffer is pre-reserved and
`reserve` early-exits); only reducing the CALL COUNT would help, by composing
each fixed-width field's length prefix and value into one stack array and
emitting them together.

Not taken here, deliberately. It would mean writing the scalar wire forms by
hand instead of through `ToSql`, which is exactly what T039 chose against and
what PI3 and the recorded off-the-shelf preference push away from. It is
recorded so the next reader knows the largest remaining item in this path is
`bytes`' call shape rather than anything about Postgres, and that ~5M
instructions (~20%) is the size of the prize.

### D-09 (US5) — the direct path moves constraint failures from publish to write

Under staging, a row that violated the TARGET's constraints landed happily in
the permissive stage table and failed later, at `INSERT … SELECT`. Writing
straight into the target means the server enforces the constraint during the
COPY, so the failure now surfaces from `write` — naming the offending row,
which the old path could not.

`dest_conformance::native_types::forced_db_failure_surfaces_server_message_and_sqlstate`
was re-pinned to the new phase. What SC-007 requires is unchanged: the server's
message and SQLSTATE still reach the caller, never a bare "db error". The test
also now asserts the session is usable after the failure, because a failed unit
must ROLLBACK — a statement error inside a transaction poisons the connection
until it does, and the engine may retry a transient failure on that same
session.

### D-10 (US5) — the "never observed empty" guarantee holds by LOCKING, not MVCC

T054 asked for an isolation-level pin. Writing it revealed that the
isolation-level reasoning does not describe what happens.

`clear_table` is `TRUNCATE`, which takes ACCESS EXCLUSIVE. A concurrent
`SELECT` needs ACCESS SHARE and conflicts. So a reader arriving mid-unit does
not read the old rows from an older snapshot — it **blocks** until the unit
commits, then reads the new ones. The first draft of the test asserted the
snapshot behaviour and hung; the pin now asserts the real one (the reader does
not complete within 750 ms, and returns the NEW contents once the unit
commits).

The guarantee itself is intact and holds at every isolation level. What
changed is the WINDOW: that lock used to be held for the publish alone — about
740 ms on a 1M-row load — and is now held from the first batch to the commit.
That, the retained `xmin` delaying vacuum database-wide, and the two together
under a stalled load, are recorded in the `dest` module doc as the price of
the change. Commit cadence is the control.

### D-11 (US5) — COPY itself got FASTER, against expectation

Server-side statement time, medians of 3 interleaved runs on `pg-to-pg-1m`:

| statement kind | before | after | Δ |
|---|---:|---:|---:|
| COPY | 1048.2 ms | 977.4 ms | −6.8% |
| `INSERT … SELECT` | 812.1 ms | **0.0 ms** | −100% |
| BEGIN/COMMIT | 26.6 ms | 7.4 ms | −72.2% |
| DDL | 19.4 ms | 11.6 ms | −40.4% |
| TRUNCATE | 2.3 ms | 0.6 ms | −72.8% |
| **total** | **1927.5 ms** | **999.0 ms** | **−48.2%** |

The COPY row should have gone the other way: rows now land in a LOGGED target
instead of an UNLOGGED stage, which means writing WAL for them. It is a real
effect, not noise — it reproduced across all three runs.

The mechanism is T051. The stage table carried `__rdlt_arrival BIGSERIAL`, so
staging a million rows meant a million `nextval()` calls. Non-merge tables no
longer have a stage, so that column and its sequence are gone with it. The
saved sequence traffic more than pays for the added WAL.

The DDL row is the same change seen from another angle: 29 statements before,
15 after — the stage table's `CREATE` and `ALTER`s, no longer issued. The
TRUNCATE row is two clears becoming one (target plus stage, versus target
alone).

### D-12 (US6) — the repository had no identity oracle at all, and would not have noticed

`_rdlt_id` is persisted and a destination merges against it, so a shift does
not corrupt loudly — it silently stops matching and every row looks new.
Before T059 nothing defended it: `grep -rEn '"[0-9a-f]{64}"' crates/` returned
no literal identity anywhere, and the closest thing, `shred_property.rs`'s
referential-integrity check, **provably cannot catch a shift** — child ids are
derived from the root id via `child_row_id`, so a moved root moves every child
consistently and referential integrity still holds. The whole suite stays
green while every persisted id changes.

`tests/fixtures/shred_identities.txt` now pins 23 cases verbatim. The corpus
was designed by adversarially attacking the planned changes, and each entry
defends a rule a plausible cleanup would break:

- `keyed_float_edges` — `10.0`, `10` and `"10"` share ONE keyed id
  (`98ae774b…`) because the keyed path renders floats with Rust's `Display`,
  while their keyless counterparts differ because that path goes through
  serde_json. Any float-formatter swap — `{:?}`, a faster float crate, or
  simply using the other one — moves these.
- `keyed_composite` — `{"a":1,"b":2}` = `330b6b64…` sits next to
  `{"a":12,"b":3}`. A scratch buffer cleared once per ROW instead of once per
  FIELD makes the first hash as the second does. Only a multi-field key whose
  fields both render through the buffer can catch it, and the repo had none.
- `keyed_null_absent_and_empty` — absent, explicit null, `{}` and `[]` all
  share one id; `""` does not.
- `child_null_slots_consume_positions` — surviving children sit at `pos` 1 and
  3, not 0 and 1. Any `filter().enumerate()` renumbers every sibling.
- `child_normalized_collision` — `"a-b"` and `"a b"` land in one table with
  both children at `pos` 0, which only holds while they stay separate
  observation entries.

A companion cross-view proptest lives in `src/shred/table.rs` (256 cases each
for the keyed and keyless arms) because the arena view is crate-private.

### D-13 (US6) — T062's single-pass scatter measures WORSE and is not taken

FR/T062 called for replacing `build_batch`'s column-major probe with a
single-pass scatter, on the strength of `build_batch` being 12.41% inclusive
of the flagship cell. It was implemented twice and measured three ways:

| variant | `shred_nested_10k` | vs baseline |
|---|---:|---:|
| column-major probe (shipping) | 325,653,221 | — |
| scatter into `Vec<Vec<Option<V>>>` | 338,147,276 | +3.8% |
| scatter into one flat column-major buffer | 336,511,908 | +3.3% |

And on the flagship cell itself, isolated with two binaries differing only in
this change: **CPU 0.78 s both ways, exactly flat** (wall −3.6%, within noise
at RSS and context-switch parity).

The reason is arithmetic, not implementation. The probe costs
`columns x rows x entries` comparisons; the scatter costs
`rows x entries x lookup`. It only wins if the lookup is cheaper than a scan
of the column list — and with `std`'s `HashMap` the lookup hashes a short
string with SipHash, which is not. Replacing the map with a linear scan makes
the two costs algebraically identical, so there is nothing left to win at this
column count. The first variant was additionally cache-hostile: pushing a
placeholder into every column's separate vector once per row touches N cache
lines per row.

Not landed. D1 says the measured-better option replaces the other; this one is
measured-worse, so the shipping code stays. The change is recorded rather than
silently dropped because the 12.41% figure will tempt the next reader.

### D-14 (US6) — the ≥10% cell-CPU floor is missed at −4.9%, and the floor assumed D-13

SC/T067 required flagship-cell CPU at least 10% below baseline. Measured on an
isolated interleaved A/B (n=7): **0.81 → 0.77 s, −4.9%**. Missed.

The gap is D-13. The 10% target was sized on recovering `build_batch`'s 12.41%
alongside the identity-path savings; that 12.41% turned out not to be
recoverable by the proposed change, or by the alternative implementation. What
did land:

| lever | `shred_nested_10k` | Δ |
|---|---:|---:|
| baseline | 347,094,870 | — |
| T060 shared identity scratch | 325,653,278 | −6.2% |
| T063 memoized child-table index | 313,658,843 | −3.7% |
| T064 pre-sized arena | 310,654,653 | −1.0% |
| **total** | | **−10.5%** |

Two further reasons the cell moves less than the microbench: shred is roughly
a quarter of that cell, and **T061 is invisible to both instruments** — the
keyed borrow only affects streams that declare a primary key, and both
`shred_nested_10k` (`fuzzing.rs` builds `StreamSpec::new("bench")`, so
`primary_key: None`) and the flagship cell are keyless. T061 is defended by
the pinned corpus, not by a number.

### D-15 (US6) — two negative results recorded so they are not chased twice

**`rdlt-core/src/identity.rs` is unchanged.** Reusing a `blake3::Hasher`
across rows is not pursued: `RowIdBuilder::update_lp` feeds each field's
LENGTH before its bytes, so the whole canonical document is one length-prefixed
field. Its length is not known until canonicalization finishes, which is why
"hash incrementally instead of materialising" (the original FR-029 wording) is
structurally impossible while identities are frozen. Only the ALLOCATION was
recoverable, and T060 recovered it.

**The canonical key-sort comparator is untouched.** `__memcmp` was 5.48% of
the profile, but T063 removes callers, so attributing it before re-measuring
would optimize a number that has already moved.

**`smallvec` was NOT evaluated and is NOT taken.** T065 made it conditional on
`shred_nested_10k` moving; the buffers it targeted were addressed differently
(the memo removed the repeated child-table resolution, pre-sizing removed the
arena's regrowth), and no measurement was taken that would justify a new
direct dependency. Stated plainly rather than implied: this is an untested
option declined under PI3's default, not a measured rejection.

### D-16 (US7) — the dictionary default, chosen by sweep

Parquet's own `dictionary_page_size_limit` default is 1 MiB. Writing 200k rows
with snappy, median of 5, at each candidate:

| limit | high-card µs | high-card KiB | low-card µs | low-card KiB |
|---|---:|---:|---:|---:|
| 4 KiB | 4397 | 1770 | 3664 | 14 |
| 16 KiB | 4280 | 1771 | 3545 | 14 |
| **64 KiB (shipped)** | **4322** | **1783** | **3571** | **14** |
| 256 KiB | 4936 | 1839 | 3558 | 14 |
| 1 MiB (library default) | 7280 | 2079 | 3569 | 14 |

The asymmetry T070 demanded be checked is real and holds: high-cardinality
encoding is flat from 4–64 KiB and degrades sharply above it (1 MiB costs 68%
more CPU AND produces a larger file, because the column interns nearly every
distinct value before giving up), while low-cardinality encoding is flat across
the entire range. A lower cap therefore takes nothing away from the columns
dictionary encoding exists to help.

64 KiB is the TOP of the flat region deliberately: 4 and 16 KiB are no faster,
and a smaller cap would abandon dictionary encoding for medium-cardinality
columns that 64 KiB still serves.

**On AC-3's "encoder CPU ≥ 25% below baseline", the answer depends on which
baseline, so both are recorded rather than the flattering one:**

- vs naively turning on snappy and leaving the limit alone (7280 µs):
  **−40.6%** — clears the bar, and this is the comparison the requirement was
  reasoning about, since it is what makes "compression without a CPU cost"
  true.
- vs the behaviour this feature replaces — uncompressed at the library default
  (4917 µs): **−12.1% CPU and −81.6% bytes** (9688 → 1783 KiB). Below 25% on
  CPU; the size result is the headline for that comparison.

### D-17 (US7) — parquet 58's row-group setter takes `Option`, and `None` means UNLIMITED

T071 named `set_max_row_group_row_count` as the replacement for the deprecated
`set_max_row_group_size`, which is correct (`properties.rs:726` carries
`#[deprecated(since = "58.0.0")]`). Reading the actual signature turned up
something the task did not say: it takes **`Option<usize>`**, and its own doc
states `None` means *unlimited* — not "use the default".

So passing the config field straight through would have silently replaced
parquet's 1,048,576-row default with unbounded row groups on every load that
did not set it: larger memory, worse read pushdown, and nothing to notice it.
The translator calls the setter only when the value is `Some`, and
`writer_props.rs` carries a test that fails if that ever changes
(`an_unset_row_group_count_keeps_the_library_default_not_unlimited`).

The `assert_ne!(value, Some(0))` at `:741` is confirmed, which is why zero is
refused during validation rather than allowed to panic inside the library.

### D-18 (US7) — incomplete, and what is missing

Landed: the SPI type, its defaults and validation, the file destination's
translation boundary, the CLI mirror (without which the whole story would be
unreachable from YAML), and the bench prefix collision.

**Not landed, and none of it is blocked — only unfinished:**

- **The iceberg half of T071.** `rdlt-connector-iceberg` still writes parquet
  with library defaults, so iceberg output stays uncompressed. Not a
  regression — it is what it always did — but it now differs from the file
  destination, which is worse than either state alone.
- **T074** (bytes-written in the artifact), **T076** (S3 unsigned payload),
  **T078** (re-derive the `s3jsonl-to-s3parquet-200k` bar, whose rdlt arm now
  pays compression CPU that dlt's arm already paid), **T079** (the recorded
  measurement).

T078 matters for honesty of the published numbers: that cell carries
`min_ratio = 45.0` against a 60.1x floor, and D4 adds compression CPU to
rdlt's side only. Until it is re-measured the bar is stale in the direction
that flatters rdlt.

## Deviations

### D-01 (T004) — CI at HEAD is not executing, and it is broader than the cold-start gap

Two separate problems were found; the owner directed that CI be set aside for
now, so this records the state rather than fixing it.

**The cold-start gap is real and confirmed.** `grep -rn hyperfine .github/`
returns nothing — no workflow installs it — while `ci.yml`'s perf-gate job runs
`make bench TARGET=iai`, which invokes `benches/check-cold-start.sh`, which
exits 1 without hyperfine (`:25-28`). The script's own header additionally
requires a quiet machine, which a hosted runner is not, so running it in CI was
never right. **Decision: the fix is the split in T032** (cold start moves to its
own verb, invoked by `make check` and the recorded session, not by CI) rather
than installing hyperfine in the job. Installing it would unbreak the job only
to have T032 remove it again, and would leave a quiet-machine measurement
running on a noisy runner in the meantime.

**But that is not why CI is failing.** Run `30147886242` at `270c903` shows all
four jobs — `check`, `perf-gate`, `semver`, `test` — failing **3 to 5 seconds
after start with zero steps recorded**:

```
check:     started 06:39:36  completed 06:39:40  steps=(none)
perf-gate: started 06:39:36  completed 06:39:41  steps=(none)
semver:    started 06:39:36  completed 06:39:40  steps=(none)
test:      started 06:39:36  completed 06:39:39  steps=(none)
```

Jobs that fail before any step runs did not fail on code — this is a
workflow- or runner-level failure. The referenced composite action exists
(`.github/actions/free-disk/action.yml`), so that is not the cause. Logs for
the run have expired, so the cause is not determinable from the API alone.

**Consequence for this feature**: CI cannot be the evidence for "the gate is
green" at any increment. The **local** gate (`make check` = lint + test + sweep
+ instruments) is the gate of record until CI is repaired, and each increment's
green must be demonstrated locally and recorded. Deferred by owner instruction.

## Project-setup verification (Phase 1)

- `.gitignore` — verified, no change needed. Already carries the Rust set:
  `/target` (`:1`), `**/*.rs.bk` (`:2`), `*.prof*` (`:3`), `.env*` (`:4`),
  `*.log` (`:6`), `.idea/` (`:7`).
- `.dockerignore` — **deliberately not created.** The only Dockerfile is
  `benches/competitors/dlt/Dockerfile`, and its build context is that directory
  (`podman build … benches/competitors/dlt/` in the `bench` target), which holds
  seven files: the Dockerfile, five pipeline scripts and `variants.toml`. A
  root-level `.dockerignore` would never be consulted, and a directory-level one
  would exclude nothing. Adding either would be noise.
- No Node, Python-package, Terraform, or Helm surface in this repo requiring an
  ignore file. The Python that exists (`benches/fixtures/*.py`,
  `benches/competitors/`) is script-only and not packaged.
