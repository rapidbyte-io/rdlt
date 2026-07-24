# Data Model: Benchmark Refinement — Three-Way E2E Matrix

No engine/connector persisted formats are touched. The entities are the
harness's own data shapes; artifact v2 is the one versioned-format change
(BR3).

## 1. Cell (declarative, benches/cells/e2e.toml — one file, no suites)

| Field | Notes |
|---|---|
| `id` | one of the five matrix ids |
| `fixtures` | subset of {`pg`, `rustfs`} |
| `pipeline` | rdlt pipeline spec path (cells/pipelines/*.yaml) |
| `expected_rows` | row-count verification target (BR4) |
| `note` | the cell's claim + regime caveats — rendered as the matrix caption |
| `competitors` | per-variant arm config (script/driver args, per-variant `runs` override) |
| *(removed)* | `class`, `suite`, `mode` — gone from the schema; unknown keys stay load-time errors |

## 2. Artifact (format_version = 2)

v1 → v2 delta: `class` REMOVED; optional `extra: object` ADDED (driver
pass-through, e.g. `sync_s`); `forced: bool` annotation when the quiet
guard was overridden; everything else (fingerprints, runs_ms, medians,
rss, verification) unchanged. Reader accepts v2 only; v1 → loud error
naming the archive commit.

## 3. History line (benches/history.jsonl, append-only)

`{ts, cell, variant, median_ms, rows}` — ts taken from the artifact's
recorded timestamp (no new clock source); one line per cell×variant per
recorded invocation; Trends section generated from it.

## 4. Variant (per-module benches/competitors/*/variants.toml)

| Field | Notes |
|---|---|
| `id` | flat namespace across modules; duplicate = load-time error naming both files |
| `kind` | `self_timed_container` (default, today's behavior) \| `driver` |
| `pin` | version pin (bump ⇒ re-measure); `[defaults]` table per module (017 mechanism) |
| `image` / `driver` | container image, or host-side driver script path (venv-managed) |
| `runs` | per-variant run count (Airbyte 3, rdlt/dlt 5) |

## 5. Driver result convention (unchanged last line, extended)

`{"seconds": f64, "rows": u64, "peak_rss_kb"?: u64, "extra"?: {…}}` —
`extra` carried verbatim into the artifact; headline = `seconds`
(Airbyte: job wall); labeled context lives in `extra`.

## 6. Bar (bars.toml, empty until P4)

`cell` (must exist — the only cross-validation), `metric`
(`ratio_vs <variant>` | `rss_ratio_vs <variant>` | absolute), `bound`,
`policy` (entry pointer citing the recorded session). ≤ 1 per cell;
cluster-wide statistics forbidden as metrics.

## 7. Fixtures (2 + exempt selftest)

- **pg**: one container (pin `postgres:16` per 017 defaults), database
  `src` seeded by seed_pg.sql (1M×12); empty `dest_rdlt`/`dest_dlt`/
  `dest_airbyte` databases; per-run reset = drop/recreate the measured
  arm's destination schema.
- **rustfs**: one container (pin `1.0.0-beta.11`), bucket `raw` (seeded
  200k nested jsonl via gen_jsonl.py, once per session), bucket `lake`
  (per-product prefixes, reset by prefix delete per run).

## 8. Spike record (specs/018-bench-refinement/spike/)

One file per probe: evidence (commands + outputs), decision
(go/no-go/owner-approval-required), and — for networking/API probes —
the pinned facts (address form, field names) driver.py will rely on.

## 9. Governance documents

- Constitution v1.1.0 (Amendment A applied verbatim, Sync Impact Report
  in header).
- 012 contract amendment note (Amendment B appended verbatim).
- RESULTS.md policy log: one entry for the matrix rebuild + bar
  retirement (final values + archive commit), one per future bar.
- GOVERNANCE.md: coverage/semver/exclusion records relocated from
  RESULTS.md.
