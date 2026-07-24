# Bench refinement — analysis & proposals (v3)

Status: analysis only. Nothing here is implemented.

**Governing principle (v3, owner-stated):** this benchmark is for **end-to-end
pipeline benchmarks only**, using sources and destinations that can be measured
**across all three products — rdlt, dlt, Airbyte — in the same conditions**.
No gated/scoreboard taxonomy. Everything else is cleaned out.

v1 surveyed the harness; v2 indexed cells on claims; v3 cuts harder: the
classification vocabulary itself goes, and comparability decides what lives.
v3.1 (owner): nothing is archived — it is **deleted**. Git history is the
archive; the working tree stays clean.

---

## 1. TL;DR — the entire benchmark after this lands

Five cells. Each is an e2e pipeline. Each runs on all three products from the
same data into the same destination instance. That is the whole matrix:

| Cell | Pipeline | Claim it answers |
|---|---|---|
| `pg-to-pg-1m` | Postgres 1M wide rows → Postgres | database replication: engine vs library vs platform |
| `pg-to-s3parquet-1m` | Postgres 1M → parquet on S3 (RUSTFS) | database → lakehouse files |
| `s3jsonl-to-pg-200k` | nested jsonl 200k on S3 → Postgres | files → database (the old flagship's shape claim, now 3-way) |
| `s3jsonl-to-s3parquet-200k` | jsonl on S3 → parquet on S3 | JSON→parquet / Arrow-native |
| `pg-to-pg-dedup-1m` | load 2 re-delivers 1M rows 50% changed, dedup by pk | keeping a table in sync |

Everything else — 19 of today's 24 cells, the `class` field, the suites, three
of the four run modes, 6 of the 9 fixtures — is deleted outright (§5, §6). Git
history keeps every number and every line of it; nothing lives half-dead in an
`archive/` directory.

Supporting changes, each small: the Airbyte `driver` competitor kind (§7), the
presentation rebuild (§8), bars set measurement-first after the first recorded
session (§4).

## 2. The rule: three tests, all required

A cell survives only if it passes all three:

1. **E2E pipeline.** A real source → product → real destination, measured end
   to end. No stage-level micros (shred-only), no SQL-level cells
   (merge-index), no startup micros (cold-start), no rdlt-internal comparisons
   (strategy pairs).
2. **Three-way comparable.** The source AND the destination must exist in
   rdlt, dlt, and Airbyte with equivalent semantics, measurable under the Same
   Conditions (§3). One product missing → no cell.
3. **Claim-worthy.** The row answers a question a user asks when choosing a
   tool. If nobody would print the sentence, there is no cell.

## 3. "Same conditions", defined

- Same machine, same session, baselines measured first (today's protocol,
  unchanged).
- Same source data: the same seeded Postgres table (1M × 12 cols,
  `seed_pg.sql`), or the same generated 200k nested jsonl in the same RUSTFS
  bucket (`raw`).
- Same destination instance: the same Postgres server (one database per
  product) or the same RUSTFS (one prefix per product, bucket `lake`).
- Same operation: full refresh of one table — or, for the dedup cell,
  full-redelivery + dedup by primary key in all three (§4 note).
- **Fastest documented configuration per product, honestly chosen**: dlt's
  Postgres extraction runs with `backend="connectorx"` — the fastest dlt
  actually has (a Rust reader). The 005-era "pure-dlt" scoping that gated
  against the slower pyarrow backend retires; pyarrow stays as recorded
  context. The multiples drop (≈2.2× instead of ≈7.8× on pg cells) and the
  claim gets *more* defensible, not less.
- Same verification: destination row count equals expected.
- Timing boundary per product, recorded honestly (unchanged from today): rdlt
  = CLI wall; dlt = in-process self-timed (the deliberate generosity); Airbyte
  = job wall as headline + sync-attempt time as context (§7.4).

## 4. Bars, without the taxonomy

`class = "gated" | "scoreboard"` is deleted — from the cell schema, the
artifacts, the quiet guard, and the vocabulary. What remains:

- There are **cells** (measured, reported) and there are **bars** (enforced by
  `rdlt-bench gate`). A cell with a bar blocks; a cell without one informs.
  That was already the de-facto semantics (BH6) — the label was redundant.
- The quiet guard becomes one rule: refuse/wait on a loaded machine for any
  run; `RDLT_BENCH_FORCE=1` runs annotated. No class branch.
- Cross-validation simplifies to: every bar references an existing cell.
- `bars.toml` starts **empty** for the new matrix. After the first recorded
  3-way session, at most one bar per cell is set measurement-first through the
  existing policy-entry mechanism (bar below the session floor, policy pointer,
  the 004 culture). Candidate shapes: `ratio_vs` the strongest baseline per
  cell; optionally one `rss_ratio_vs dlt` on `pg-to-pg-1m` (both sides are
  single processes — the Airbyte cluster RSS is never bar material).
- Retiring all current bars rides one policy-log entry in RESULTS.md ("matrix
  rebuilt 3-way; the dlt-era claims are preserved in git history and the
  Milestones section").

## 5. What dies — the cleanup list

### 5.1 Cells

| Today | Fate | Why |
|---|---|---|
| `jsonl-duckdb-200k` (the 13.5× flagship) | **deleted** | **Airbyte cannot do DuckDB under abctl** (verified: local file DBs unsupported in k8s, MotherDuck-only). See the consequence note below. |
| `shred-only-200k` | deleted | stage-level micro for a claim that no longer exists; the iai/criterion instruments already guard the shredder |
| `rest-pg-100k` | deleted | Airbyte's generic REST source is a low-code toy — no 3-way row. The dlt-era result survives in git history + Milestones |
| `parquet-passthrough` | **replaced** by `s3jsonl-to-s3parquet-200k` | local-file dlt-only cell becomes the 3-way S3 cell |
| `parquet-duckdb`, `pg-wide-duckdb-1m`, `pg-jsonb-duckdb-200k` | deleted | DuckDB not 3-way |
| `cold-start` | **moves to the instruments track** | startup micro, not an e2e pipeline comparison. Lives next to iai/criterion as a hyperfine absolute check (≤ 40 ms), keeping the embeddability claim guarded outside rdlt-bench |
| all 12 `merge`/`strategy`/SQL cells | deleted | rdlt-internal comparisons; the dedup claim returns as 3-way cell `pg-to-pg-dedup-1m` |
| both `cdc` cells | deleted | no dlt CDC counterpart → not 3-way |
| `file-s3-duckdb-200k`, `iceberg-polaris-200k` | deleted | DuckDB/Iceberg not 3-way today. A 3-way Iceberg cell (rdlt 016 / dlt `table_format="iceberg"` / Airbyte `destination-iceberg-v2`, all → Polaris+RUSTFS) is **possible but deferred** — version-compat across three Iceberg writers is a probe minefield; only if lakehouse becomes a headline claim |

Deletion semantics: one migration commit deletes the cells, fixtures, seeds,
and `benches/results/*.json` artifacts outright. **Git history is the
archive** — every recorded number remains checkout-able at the pre-migration
commit, and the migration commit message + RESULTS.md policy log cite the
final recorded values. No `cells/archive/` directory, no `--all` flag, no
frozen table, no re-activation triggers: if a future code change warrants
re-measuring one of these, the cell is re-added from history as a normal PR.

**Consequence, flagged for the owner:** the strongest single marketing number
(13.5× vs dlt on DuckDB) has no 3-way home, because Airbyte structurally can't
run that pipeline locally. Recommendation: accept the cut — one legacy
exception reopens the door this cleanup is closing, the 3-way rows will carry
their own strong numbers, and the 13.5× remains quotable from git history +
Milestones for as long as it's honest to quote it. If the number must stay
*live*, keep exactly one `legacy-jsonl-duckdb-200k` dlt-only cell and name it
as the deliberate exception. Owner decision; this document recommends **cut
clean**.

### 5.2 Fixtures

7 Postgres containers + Polaris + RUSTFS sidecar + mock-API sidecar →
**1 Postgres container** (source table + per-product destination databases) +
**1 RUSTFS** (bucket `raw` for jsonl, bucket `lake` for parquet) + generated
jsonl. `seed_merge_index.sql`, `seed_refine.sql`, the CDC/strat fixtures, the
mock API, and the Polaris bootstrap are deleted with their cells (git history
keeps them).

### 5.3 Harness code

- `class` deleted from cell schema + artifact (`format_version` 2) + all
  report/gate branches.
- `suite` deleted — five cells, one table, one marker pair.
- `Mode` collapses to `subprocess` + wall timing: the `hyperfine` arm leaves
  with cold-start, `stdout_ms`/`self_json_seconds` leave with the deleted
  SQL/shred cells, `library` mode was already dead (v2 S5) — all deleted,
  together with `library_mode.rs` and `parity_specs.yaml` (it exists to pin
  the harness's library-mode spec parser; the CLI-side parse pin moves to the
  facade's own tests).
- The quiet guard loses its class branch (§4).
- Kept untouched: the protocol (quiet guard, baseline-first, warmups/N/median),
  artifacts with fingerprints, `Missing{reason}` loud-skip semantics, the
  ru_maxrss discipline, marker-spliced report generation, the iai + criterion
  instruments (now also hosting cold-start).
- Contract: this amends BH1/BH2/BH3/BH6 wording (class/mode vocabulary) — a
  deliberate, recorded amendment; the *mechanisms* those clauses protect are
  unchanged. Continuity for retired bars rides the policy log (BH8 spirit).

### 5.4 dlt competitor module, slimmed

Image drops `duckdb` extras, **adds `s3fs`** (the long-deferred gap — now
required, since every dlt arm touches S3). Scripts: the five pipelines +
`normalize_only.py`/`cold_start.py` leave with their cells. Variants: `dlt`
(the connectorx-backed config for pg sources), `dlt-pyarrow` kept as context;
`dlt-sqlalchemy` deleted (marketing by selection).

## 6. The five cells, specified

| Cell | rdlt | dlt 1.29.0 | Airbyte (abctl) |
|---|---|---|---|
| `pg-to-pg-1m` | pg source → pg dest, replace | `sql_database` connectorx → postgres, replace | source-postgres (full refresh overwrite) → destination-postgres **Direct Load** |
| `pg-to-s3parquet-1m` | pg → file dest (parquet, s3) | `sql_database` connectorx → filesystem (parquet, s3) | source-postgres → destination-s3 (parquet) |
| `s3jsonl-to-pg-200k` | file source (jsonl, s3) → pg | filesystem (jsonl, s3fs) → postgres | source-s3 (jsonl) → destination-postgres |
| `s3jsonl-to-s3parquet-200k` | file → file | filesystem → filesystem | source-s3 → destination-s3 |
| `pg-to-pg-dedup-1m` | merge upsert, key `id` | `write_disposition="merge"`, delete-insert, key `id` | **Full Refresh Overwrite + Deduped**, primary key `id` |

Dedup-cell regime note (honesty, recorded in the cell): all three read the
full 1M-row table on load 2 (50% changed) and dedup by `id` — matching
semantics. Airbyte's cursor-based incremental is a *different, cheaper* regime
it shares with rdlt's CDC — not benched, because dlt has no counterpart.
`runs = 3` for Airbyte arms (per-competitor run counts already exist);
rdlt/dlt keep 5.

## 7. Airbyte machinery (driver kind) — condensed from v2, essentials stand alone

Verified facts (2026-07-24): `abctl local install` → kind cluster (10–30 min
install, 4 CPU/8 GB recommended, `--low-resource-mode` exists);
`abctl local credentials` → Client-Id/Secret; public API at
`http://localhost:8000/api/public/v1` (token from `/applications/token`);
`POST /v1/jobs {connectionId, jobType: sync|reset}`, `GET /v1/jobs/{id}` →
status/timing/`recordsSynced`/`bytesSynced` (exact fields pinned at probe).
destination-postgres 3.x Direct Load writes typed final tables — genuinely
comparable output. destination-duckdb impossible under k8s. destination-s3
certified, works against RUSTFS.

Harness delta (small):

1. Load `benches/competitors/*/variants.toml` (not the hardcoded dlt path);
   one flat variant namespace, collision = load-time error.
2. New variant field `kind = "self_timed_container" | "driver"` (default =
   today). A `driver` run executes the module's host-side `driver.py`
   (venv-managed, 016 pyiceberg precedent), which orchestrates abctl + the API
   and prints the same last-line JSON convention (`seconds`, `rows`, optional
   `peak_rss_kb`, plus an `extra` object carried into the artifact). Artifact,
   gate, and report paths need **zero changes**.
3. CPU/RSS from the kind node container's cgroup v2 via the existing
   `read_cgroup_via_exec` (recorded, never gated, "whole cluster ≠ one
   process" note attached).
4. abctl is a **machine prerequisite**, not a fixture: harness probes
   `abctl local status`; absent → `Missing{reason}` loud skip (BH4 semantics,
   testcontainers precedent). Idempotent committed `setup.py` installs if
   needed and creates the five connections via the API (ids cached in
   gitignored `state.json`).

Timing: headline `seconds` = job wall (trigger → terminal status — includes
orchestration, what a user experiences); `extra.sync_s` = attempt time
excluding queue/setup (scoreboard context, labeled); records/bytes from job
stats. If a bar is ever proposed, its policy entry names which statistic it
binds.

Fairness policy (recorded in RESULTS.md): Airbyte measured in its fastest
documented local configuration; orchestration included in the headline,
labeled; Airbyte's actual value (connector breadth, scheduling, UI) is not
what these cells measure — said in caveats; versions pinned in
`variants.toml`, bump → re-measure (same policy as dlt).

Probes before any harness code (016 T001 pattern): **(1) runtime** — this
machine is podman-based, abctl/kind wants Docker; rootless podman
(`KIND_EXPERIMENTAL_PROVIDER`) vs installing docker — the #1 feasibility risk;
**(2) networking** — kind pods reaching the host's postgres/RUSTFS
(`host.docker.internal` on Linux kind vs bridge gateway); **(3) API field
names** end-to-end; **(4) idle kind loadavg vs the quiet guard**; **(5)
`reset` + schema drop returns the destination to initial state (row counts
prove it).

## 8. Presentation (v2 plan, adjusted: no suites)

`benches/RESULTS.md`:

```
# Benchmark results
> methodology + pin policy (2 lines) · policy log (bar retirements, pin bumps)

## The matrix      ← GENERATED — the five rows, one table
| Cell | rdlt median | dlt 1.29.0 | Airbyte 1.x | vs dlt | vs Airbyte | Bar | Status |
   Status = gate verdict where a bar exists, — otherwise; spread as
   "1.31 s (1.28–1.36)" from runs_ms; per-cell claim as the caption line
   (hoisted from [cell.workload] note)

## Caveats (hand-written, curated)
## Trends (GENERATED from benches/history.jsonl — append one line per cell
          per invocation)
## Milestones (hand-written — this is where the deleted dlt-era claims
              live on as quotable history, with the pre-migration commit
              cited as their evidence)
```

Coverage/semver/classified-exclusion records move to `benches/GOVERNANCE.md`.
RESULTS.md: ~377 → ~120 lines, every number generated or frozen-with-citation.

## 9. Phasing (each phase independently mergeable, gate green at each)

| Phase | Content | New measurements? |
|---|---|---|
| **P0** | harness simplification (class/suite/mode collapse, §5.3) + legacy deletion (§5.1–5.2 — one migration commit; continuity = the commit message + policy log cite the final recorded values and the pre-migration commit) + cold-start → instruments + presentation rebuild (§8) + GOVERNANCE.md split + bars.toml retired via policy log | none |
| **P1** | §7 probes → spike doc (runtime decision first) | spike only |
| **P2** | new fixtures (1 pg + 1 RUSTFS) + 5 rdlt pipelines + slimmed dlt image (s3fs) + dlt scripts + connectorx baseline → **first recorded session, rdlt vs dlt**, no bars | yes, recorded-not-barred |
| **P3** | driver kind + airbyte module + arms → **first 3-way session** | yes, recorded-not-barred |
| **P4** | bars set measurement-first via policy entries (≤ 1 per cell). Conditional, only-if-headline-claim: 3-way Iceberg cell | per policy |

## 10. Non-goals (explicit)

- No gated/scoreboard (or any per-cell importance taxonomy) — deleted, not
  renamed. Don't reintroduce one.
- No product-specific cells: if all three can't run it, it's not in this
  matrix (docs and functional tests are the place for product-specific
  claims).
- No CI gating on wall time (quiet-machine requirement; gate stays a
  deliberate local act).
- No hosted services (Airbyte Cloud, MotherDuck) — unrepresentative by
  construction.
- No bars without recorded evidence + a policy entry.
- No dashboard. Markdown + JSONL is the right altitude.

---

### Appendix: proposal-to-file map

| Proposal | Files |
|---|---|
| Class/suite/mode collapse | `crates/rdlt-bench/src/{cells,artifact,runner,gate,report,main,protocol}.rs`, delete `library_mode.rs`, 012 contract amendment, `crates/rdlt-bench/tests/selftest.rs` |
| Legacy deletion | `benches/cells/` (5 remain), delete old `benches/results/*.json`, `benches/fixtures/` (slimmed), delete `benches/parity_specs.yaml` (pin moves to facade tests); Milestones entry cites the pre-migration commit |
| Cold-start → instruments | `benches/compare-iai.sh` side (or new `check-cold-start.sh`), Makefile |
| New matrix | `benches/cells/e2e.toml`, `cells/pipelines/*.yaml` ×5, `fixtures/fixtures.toml`, dlt `Dockerfile` (+s3fs, −duckdb) + 5 scripts |
| Airbyte | `crates/rdlt-bench/src/{competitors,main}.rs`, `benches/competitors/airbyte/{setup.py,driver.py,variants.toml,README.md}` |
| Presentation | `crates/rdlt-bench/src/report.rs`, `runner.rs` (history append), `benches/RESULTS.md`, new `benches/GOVERNANCE.md` |
