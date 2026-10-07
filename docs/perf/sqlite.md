# SQLite destination

The reference SQLite destination stages a commit's rows, then publishes and deletes them by the
staging predicate of ADR 0006. This record keeps what an append costs at one commit and at many,
and what a merge into a table of a million rows costs.

## Method

- **Rows.** The passthrough bench's ten mixed columns, ids from zero, in batches of 10 000 rows for
  appends and 50 000 for the merge, a checkpoint after each batch. The sizes are the review's
  ablation's, fixed so the figures compare with it and with each other.
- **Cases.**
  - `append/1000000/once`: a million rows, one commit at the end.
  - `append/{200000,500000,1000000}/10000`: a commit every 10 000 rows.
  - `merge/1000000`: a million rows merged by `id` into a table a merge of a million rows made,
    a tenth of them updates of its first rows, their values changed, the rest new, every batch
    holding both. The table is written once a process; each run merges into a copy of it.
- **Destination.** `SqliteDestination`, a database of its own a run in a directory beside the
  build, on btrfs on an NVMe disk, with its build defaults (no page cache or `synchronous` set).
- **Checks.** Every run reads the table back for its rows (1 900 000 after the merge), checks its
  staging is empty, and that it committed at least once every 10 000 rows where it commits so.
- **Engine.** Within the four cores (2 runtime workers and 2 compute threads), a budget of 1 GiB.
- **Syscalls.** One run of each case in criterion's test mode under `strace -f -c`, its setup
  included; the merge's setup writes the table it merges into.
- **Running.** `just bench sqlite 0-3`, five rounds, each started at a one-minute load average
  under 1.0 under the measurement lock; release profile. SQLite's C builds optimised in dev
  builds too (`[profile.dev.package.libsqlite3-sys]`), so the bench's smoke run under `just test`
  takes seconds rather than minutes.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-07, commit `28726685db15`, governor `powersave`
with energy preference `performance`, one-minute load average 0.92–0.99 before the runs; the
median of five rounds and their range.

| Case | Time a run | Rounds within ±3 % | Rows a second | `pread64` | `pwrite64` | `fsync` |
|---|---|---|---|---|---|---|
| `append/1000000/once` | 2.04 s (2.01–2.05 s) | 5 of 5 | 490 k | 295 931 | 423 002 | 15 |
| `append/200000/10000` | 0.71 s (0.70–0.74 s) | 4 of 5 | 283 k | 206 471 | 79 579 | 68 |
| `append/500000/10000` | 2.65 s (2.63–2.79 s) | 4 of 5 | 189 k | 1 163 068 | 202 006 | 143 |
| `append/1000000/10000` | 8.19 s (8.12–8.50 s) | 4 of 5 | 122 k | 4 501 071 | 405 720 | 272 |
| `merge/1000000` | 12.57 s (12.43–14.17 s) | 3 of 5 | 79.5 k | 5 015 805 | 3 211 264 | 26 |

- Appends that commit every 10 000 rows grow faster than their rows: five times the rows take
  11.6 times as long, and a million rows four times as long as at one commit, with fifteen times
  the reads: each commit's publish and delete scan the rows staged for later commits, as the
  review traced.
- 272 syncs a million rows at that cadence, about 2.7 a commit.
- The merge, with the merge into an empty table its setup makes, reads 5.0 million times and
  writes 3.2 million.
- The review measured 10.8–11.2 s and 17.5–18.3 s for the 102-commit append and the merge on the
  efficient cores; these are the performance cores.

`perf stat` cannot difference runs of several seconds; the appends that run under three seconds
keep 0.83–0.88 CPUs busy at 4.4–5.0 instructions a cycle.
