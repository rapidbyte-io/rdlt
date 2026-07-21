# Research: Unified Benchmark Framework

## R1 — Harness form: one dev-only crate, one binary

**Decision**: new workspace member `crates/rdlt-bench` with
`publish = false`, exposing one binary `rdlt-bench` with subcommands
`list`, `run`, `gate`, `report`. It depends only on existing workspace
dependencies: `rdlt` (all connector features, like the CLI), `serde`,
`serde_json`, `toml`, `tokio`. **Zero new dependencies, workspace-wide**
— verified: `toml 0.8` is already a workspace dep (config_schema
tooling), everything else ships today.

**Rationale**: FR-010 requires dev-only + conservative deps; it turns
out "conservative" collapses to "none new". No clap (the house CLI
hand-rolls args; the harness has four subcommands and a filter flag —
same treatment). No sysinfo/procfs crates: /proc and cgroup v2 are
line-oriented text files; parsing them is ~40 lines of safe std.

**Alternatives considered**: cargo-bench/criterion integration for e2e
cells (rejected in 004 already — criterion's model fits micro, not
container-lifecycle e2e); a Python driver (rejected: the whole point is
retiring ad-hoc scripting; Rust gives us the library seams in-process).

## R2 — Cell format: TOML files under `benches/cells/`

**Decision**: human-authored TOML, one file per suite
(`e2e.toml`, `pg.toml`, `cdc.toml`, `merge.toml`), each holding
`[[cell]]` entries. A cell names: `id`, `class` (`gated`|`scoreboard`),
`mode` (`subprocess`|`library`), `fixture`, `pipeline` (a YAML spec
template under `benches/cells/pipelines/`), `workload` knobs, `warmups`,
`runs`, `competitors = ["dlt-pyarrow", ...]`, and optional
`verify` (row-count check in the destination — measurement that also
proves the load happened, the run-pg.sh discipline). Machine-written
outputs are JSON (R5); human-authored inputs are TOML (matches
bars.toml and the house preference for comment-friendly config).

**Rationale**: cells-as-data is the feature (FR-001); TOML + serde gives
typed validation with named-offender errors for free, same two-layer
validation posture as connector config.

**Alternatives**: YAML cells (rejected — pipeline specs are YAML;
keeping the harness's own config TOML makes "which layer am I editing"
unambiguous); a single mega-file (rejected — suites grow independently).

## R3 — rdlt-side metrics without unsafe: procfs, not getrusage

**Decision**: subprocess cells spawn the release CLI via
`std::process::Command`; wall time via `Instant`. A sampler thread polls
`/proc/<pid>/status` (`VmHWM` — kernel-maintained peak-RSS high-water
mark) and `/proc/<pid>/stat` (utime/stime → CPU utilisation) every
~50 ms, keeping the last-seen values and a coarse time-series.
`getrusage`/`wait4` are libc FFI (unsafe) and are REJECTED per the
house safe-Rust rule; `VmHWM` makes sampling loss-proof for peak RSS
(the kernel tracks the high-water mark between samples — transient
spikes are captured by construction, satisfying the spec's
"spike visibility"). The final CPU sample undercounts by at most one
sampling interval; recorded as a measurement note, matching the
existing `/usr/bin/time` quantization honesty.

Library-mode cells run the pipeline in-process through the `rdlt` crate:
`RunReport` (crates/rdlt-core/src/report.rs) supplies per-table
`rows`/`bytes` → rows/s + MB/s exactly (no estimation), plus
`elapsed_ms`, `commits`, `retries`; the engine's `events()` stream
(`PipelineEvent::StreamStarted/BatchLoaded/Committed/StreamFinished`)
timestamps arrival → per-stream/per-phase attribution. No engine
changes; both seams exist today.

**Rationale**: FR-003 metric set with zero unsafe and zero deps.
`/usr/bin/time -v`'s 10 ms quantization (documented in RESULTS.md) goes
away as a side effect — `Instant` is the clock now.

## R4 — Competitor accounting: cgroup v2 via the container runtime

**Decision**: dlt runs keep their in-container **self-timing** for the
wall-time column (continuity: every recorded multiple was computed this
way — generous to the baseline, documented). Resource metrics come from
the container's cgroup v2: resolve the cgroup path via
`podman inspect`, read `memory.peak` and `cpu.stat` (`usage_usec`,
`user_usec`, `system_usec`) directly from `/sys/fs/cgroup/...` on the
host, deltaed around the timed pipeline invocation. The competitor
module (`benches/competitors/dlt/`) absorbs today's `baseline/`
(Dockerfile, pipeline scripts, seed SQL) with a variant registry:
`dlt-pyarrow` (the gated pairing), `dlt-sqlalchemy`, `dlt-connectorx`.

**Rationale**: same metric set both sides (FR-004) without touching the
dlt container's internals; cgroup files are the kernel's own accounting
for exactly that process tree. Fallback honesty: if the cgroup path is
unreadable (rootless quirks), resource fields are reported `null` with
a reason — never fabricated.

## R5 — Artifact schema: versioned JSON under `benches/results/`

**Decision**: every `rdlt-bench run` writes
`benches/results/<cell-id>.json` (`format_version: 1`): all per-run
wall times, medians/p95, derived throughput, CPU/RSS stats, per-stream
attribution (library mode), competitor blocks with ratios, and the
environment fingerprint (CPU model from /proc/cpuinfo, kernel from
/proc/sys/kernel/osrelease, `rustc -V`, dlt pin, dataset identity
hashes from seeding, quiet-machine load reading). Summary artifacts are
committed — git history IS the archive (one file per cell, overwritten
per accepted session; no timestamped filename sprawl). Raw time-series
go to a gitignored `benches/results/raw/`.

**Rationale**: FR-005; mirrors `perf-baselines.json`'s
format_version + toolchain pattern that already works.

## R6 — bars.toml: the gated set moves out of prose

**Decision**: `benches/bars.toml` holds every currently-gated bar,
transcribed from RESULTS.md at migration time: flagship jsonl→DuckDB
≥ 10× and peak-RSS ≤ 1/5 vs dlt, shred-only ≥ 10×, REST→PG ≥ 5×,
parquet passthrough ≥ 2×, pg→DuckDB ≥ 6×, pg→PG ≥ 6×, cold start
≤ 40 ms ABSOLUTE (the 004 lesson: bars that a competitor release can
flip are wrong — the schema supports both `ratio_vs` and `absolute_ms`
forms). Each bar carries its tolerance and a `policy` pointer to its
version-policy/evidence record. `rdlt-bench gate` evaluates bars
against the latest artifacts and exits nonzero naming cell, bar, and
measured value. The iai instruction gate stays in
`perf-baselines.json` + `compare-iai.sh` — different instrument,
untouched (FR-008).

## R7 — Continuity protocol for migration

**Decision**: migrated gated cells re-measure under the new harness
same-session paired (dlt first, then rdlt — the R12/004 discipline) on
the reference machine; a migrated median must land inside the
documented session-jitter band (±2–10%, per RESULTS.md history) of the
most recent recorded value. In-band → the artifact becomes the new
recorded number, bar unchanged. Out-of-band → STOP: diagnose (harness
overhead? protocol drift?), and only if the delta is explained and
accepted does a version-policy entry re-derive the bar — never a silent
renumbering. Expected harness deltas to watch: `Instant` vs
`/usr/bin/time` quantization (cold-start-adjacent cells) and container
warm-up handling.

## R8 — What is explicitly retained, and the Makefile wiring

**Decision**: criterion shred micro (`make bench`), iai + baselines +
`compare-iai.sh` (`TARGET=iai`), and the hyperfine 20-run cold-start
protocol are retained as-is; the cold-start cell is *declared* in
cells/e2e.toml with `mode = "hyperfine"` so it lists/gates/reports
through the framework while the measurement instrument stays hyperfine
(FR-008's "invoked by, not absorbed"). Makefile keeps intent verbs:
`TARGET=e2e make bench` → `rdlt-bench run --class gated`;
new `TARGET=matrix` → full scoreboard sweep; `make bench-gate` and
`make bench-report` (or TARGET= forms — implementation picks one,
consistent with the header comment style). The six run-*.sh scripts are
deleted in US3, replaced by cells + fixtures.
