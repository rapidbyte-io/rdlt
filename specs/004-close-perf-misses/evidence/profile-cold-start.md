# Evidence: Cold-Start Composition Profile (T013)

**Date**: 2026-07-20 | **Identity**: per [environment.md](environment.md)
(blashyrkh, Ryzen AI MAX+ 395, kernel 7.0.12-201.fc44, rustc 1.96.0
ac68faa20, hyperfine 1.20.0, strace 7.1; one-row pipeline exactly as
`benches/run-e2e.sh` cold cell: jsonl file source → DuckDB destination,
fresh workdir + fresh .duckdb per run)

## Invocations

- **Phase stamps**: throwaway `Instant`-stamp build of `crates/rdlt-cli`
  (stamps at main entry, tokio runtime created, spec parsed, DuckDB
  opened, pipeline built, `run()` returned, runtime done — instrumentation
  reverted after capture, never merged; `git status` clean afterwards).
  20 runs, fresh workdir/db each, external wall clocked per run with
  nanosecond shell timestamps around the exec.
- **Pre-main lens**: `/proc/self/stat` starttime vs `/proc/uptime` at main
  entry (10 ms tick resolution — printed 0.0, i.e. < 10 ms), refined by
  `external total − internal total` per run.
- **Syscall corroboration**: `strace -f -c -w` on one full run (observer
  effect ~2.3× — used for structure only, never for wall numbers, per the
  spec's observer-effect edge case).

## Phase table (medians of 20 instrumented runs)

| Phase | Median ms | Min–Max | Floor or reducible? |
|---|---|---|---|
| pre-main (dynamic link of libduckdb et al. + process exit; includes ~0.5–1 ms shell fork/exec in this lens) | 2.67 | 2.39–3.07 | **Floor** — static linking rejected (DuckDB is the deliberate dynamic dependency) |
| tokio runtime create | 0.65 | 0.57–0.89 | Floor |
| spec read + parse (TOML + YAML) | 0.06 | 0.04–0.07 | Floor |
| DuckDB open (embedded DB instantiate) | 4.41 | 4.19–5.03 | Floor in current architecture — deferral only moves it inside `run()`; overlap with source open is a possible ~2–4 ms win, **not taken** (see resolution) |
| pipeline build (catalog/state/WAL init) | 0.09 | 0.08–0.10 | Floor |
| `run()` — source read → shred → DuckDB table create + load → 2 commits | 17.38 | 15.46–19.57 | Floor — see syscall lens: not I/O-bound |
| report emit + runtime teardown | 0.03 | 0.02–0.04 | Floor |
| **Internal total** (main entry → runtime done) | **22.61** | 20.61–24.99 | |
| External total (this lens, incl. shell fork/exec) | 25.17 | 23.27–28.06 | |

Protocol-conformant total (hyperfine `-N`, 3 warmups + 20 runs, T014):
**23.57 ms median** (21.7–26.3 ms) — sits inside the instrumented
internal/external bracket, confirming the composition explains the whole
measured value.

## Syscall lens (strace -f -c -w, one run)

- **fsync/fdatasync: 12 calls, 55 µs total** — durability I/O is
  negligible on this reference SSD with a one-row payload; `run()`'s
  17.4 ms is compute + coordination, not disk wait.
- 64 `clone3` — tokio worker/blocking threads + DuckDB pool; futex wall
  time dominates the strace summary (idle-worker parking, summed across
  threads — an artifact of `-w` accounting, not runnable-path cost).
- 35 `openat`, 156 `mmap` — dynamic linking + file opens are all
  sub-millisecond.

## Floor composition (irreducible vs reducible)

**Irreducible floor = 23.6 ms** (the protocol-measured total; every phase
above is classified floor in the current architecture). Reducible
candidates surfaced, none material:

1. Overlap DuckDB open with source open: bounded by the 4.4 ms phase,
   realistic win ~2–4 ms.
2. Cap thread-pool size for tiny pipelines: micro win, complexity cost.
3. fsync batching: pointless (55 µs total).

None classified "worth taking" (T015's bar) — each is < 20% of total,
and the owner decision of 2026-07-20 takes no optimization work in this
feature. Recorded as backlog notes in
[resolution-cold-start.md](resolution-cold-start.md).
