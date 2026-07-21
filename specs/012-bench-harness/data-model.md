# Data Model: Unified Benchmark Framework

## 1. Cell (`benches/cells/*.toml`, `[[cell]]`)

| Field | Type | Notes |
|---|---|---|
| `id` | string, unique across all cell files | e.g. `pg-wide-to-duckdb` |
| `class` | `gated` \| `scoreboard` | gated cells must have a bar in bars.toml (validated at load) |
| `mode` | `subprocess` \| `library` \| `hyperfine` | R3/R8; gated throughput cells are `subprocess` (FR-011); `hyperfine` only for cold-start |
| `fixture` | string → Fixture id | |
| `pipeline` | path (relative to benches/) | YAML pipeline-spec template; `{{conn}}`, `{{workdir}}`-style substitutions filled by the harness |
| `workload` | table | free-form knobs recorded verbatim into the artifact (row counts, change-mix, …) |
| `warmups`, `runs` | u32 | defaults 1 / 5 (the 004 protocol) |
| `competitors` | array of CompetitorVariant ids | may be empty |
| `verify` | optional table | destination row-count assertion post-run (`table`, `expected_rows`) |

Validation: unknown fields rejected (`deny_unknown_fields`); duplicate
ids typed error naming both files; `class = "gated"` without a matching
bars.toml entry is a load-time typed error (and vice versa: a bar whose
cell id doesn't exist).

## 2. Fixture (`benches/fixtures/`)

| Field | Type | Notes |
|---|---|---|
| `id` | string | e.g. `pg-wide-1m`, `jsonl-200k`, `mock-rest-100k` |
| `kind` | `postgres-container` \| `generated-files` \| `mock-rest` \| `none` | |
| `seed` | path / generator params | e.g. SQL file; generator settings for datasets |
| identity | computed | content hash(es) printed at seed time and recorded in the artifact (FR-005) |

Lifecycle: the harness owns create → seed → (shared across cells within
one invocation when isolation allows; destination schemas dropped
between runs) → teardown on exit (trap-equivalent).

## 3. CompetitorVariant (`benches/competitors/dlt/variants.toml`)

| Field | Type | Notes |
|---|---|---|
| `id` | string | `dlt-pyarrow`, `dlt-sqlalchemy`, `dlt-connectorx` |
| `pin` | string | dlt version — recorded in every artifact; one pin for the whole module |
| `image` | string | container image tag (built from `competitors/dlt/Dockerfile`) |
| `entry` | string | in-container script for the cell's workload |
| `role` | `baseline` \| `context` | `baseline` feeds gated ratios; `context` is scoreboard-only |

Missing image / failed run → the artifact's competitor block is
`{"status": "missing", "reason": ...}` — loud in report and gate output
(gate still evaluates rdlt-side absolute bars; ratio bars against a
missing baseline FAIL with "baseline missing", never pass silently).

## 4. Artifact (`benches/results/<cell-id>.json`, `format_version: 1`)

```text
{
  format_version, cell_id, class, mode, recorded_at,
  fingerprint: { cpu_model, kernel, rustc, competitor_pin,
                 dataset_hashes{}, loadavg_at_start },
  workload: { ...verbatim from cell },
  rdlt: {
    runs_ms: [..], median_ms, p95_ms,
    rows, bytes, rows_per_s, mb_per_s,          // from RunReport totals
    cpu: { mean_util, peak_util, user_ms, sys_ms } | null,
    rss: { peak_bytes } | null,                  // VmHWM (subprocess) — null+reason if unreadable
    streams: [ { stream, first_batch_ms, finished_ms, rows, bytes } ]  // library mode only
  },
  competitors: {
    "<variant-id>": { status: "ok"|"missing",
                      runs_ms, median_ms, self_timed: true,
                      cpu: {...}|null, rss: {...}|null,   // cgroup v2 delta
                      ratio_vs_rdlt } | { status:"missing", reason }
  },
  verify: { table, expected_rows, actual_rows, ok } | null
}
```

Committed. Raw time-series (sampler output) → `benches/results/raw/`
(gitignored).

## 5. Bar (`benches/bars.toml`, `[[bar]]`)

| Field | Type | Notes |
|---|---|---|
| `cell` | string → Cell id | must exist and be gated |
| `metric` | `wall_median` (this feature) | CPU/RSS/throughput bars out of scope (FR-006) |
| `kind` | `ratio_vs` \| `absolute_ms` \| `rss_ratio_vs` | `ratio_vs` names the competitor variant; `absolute_ms` is the 004 cold-start form; `rss_ratio_vs` covers the existing gated peak-RSS row |
| `min_ratio` / `max_ms` / `max_rss_ratio` | number | one per kind |
| `tolerance_pct` | number | jitter allowance before a violation is declared |
| `policy` | string | pointer to the evidence/version-policy record that set the bar |

Initial content = the eight currently-gated rows of RESULTS.md (R6).

## 6. ContinuityRecord (migration close-out, in the feature's evidence/)

Per migrated gated cell: recorded old median, new-harness median, delta
%, in-band verdict; out-of-band cells carry diagnosis + the
version-policy entry that re-derived the bar. Zero unexplained rows at
close-out.

## Relationships

Cell —(fixture)→ Fixture; Cell —(competitors[])→ CompetitorVariant;
Cell ←(cell)— Bar (bijective over gated cells); run(Cell) → Artifact;
gate(Bars × Artifacts) → verdict; report(Artifacts) → RESULTS.md tables.
