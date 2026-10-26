# SQLite destination

The reference SQLite destination stages a commit's rows, then publishes and deletes them by the
staging predicate of ADR 0006. This record keeps what an append costs at one commit and at many,
what it costs when its source cannot read the rows again, so the engine logs them ahead, and what
a merge into a table of a million rows costs.

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
  build, on btrfs on an NVMe disk, with `synchronous=FULL` and no page cache set. A table's
  staging is indexed by who staged its rows while its writer appends.
- **Checks.** Every run reads the table back for its rows (1 900 000 after the merge), checks its
  staging is empty, and that it committed at least once every 10 000 rows where it commits so.
- **Engine.** Within the four cores (2 runtime workers and 2 compute threads), a budget of 1 GiB.
- **Syscalls.** One run of each case in criterion's test mode under `strace -f -c`, its setup
  included; the merge's setup writes the table it merges into.
- **Running.** `just bench sqlite 0-3`, three rounds, each interleaved with one of the parent
  commit's (`8b05f0a7`), each started at a one-minute load average under 1.0 under the
  measurement lock; release profile. SQLite's C builds optimised in dev
  builds too (`[profile.dev.package.libsqlite3-sys]`), so the bench's smoke run under `just test`
  takes seconds rather than minutes.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-12, this change on `8b05f0a7`, governor `powersave`
with energy preference `performance`, one-minute load average 0.92–0.98 before the runs; the median
of three rounds and their range.

| Case | Time a run | Rounds within ±3 % | Rows a second | `pread64` | `pwrite64` | `fsync` |
|---|---|---|---|---|---|---|
| `append/1000000/once` | 2.74 s (2.68–2.85 s) | 2 of 3 | 365 k | 371 130 | 508 688 | 16 |
| `append/200000/10000` | 0.57 s (0.55–0.58 s) | 2 of 3 | 353 k | 59 758 | 87 789 | 67 |
| `append/500000/10000` | 1.39 s (1.35–1.40 s) | 3 of 3 | 360 k | 151 880 | 221 896 | 142 |
| `append/1000000/10000` | 2.73 s (2.70–2.75 s) | 3 of 3 | 366 k | 305 800 | 445 937 | 267 |
| `merge/1000000` | 12.45 s (12.36–12.60 s) | 3 of 3 | 80.3 k | 5 015 856 | 3 211 324 | 26 |

- Appends that commit every 10 000 rows grow about linearly: a million rows take about as long a
  row as 200 000 (a per-row ratio of 0.965), and the rate holds near 360 k rows a second.
- Each commit reads only its own staged rows: `pread64` at a million rows is about 305 k.
- The one-commit append pays for the staging index as it stages, 2.74 s against 2.05 s on the
  parent commit, about a third more.
- 267 syncs a million rows at that cadence, about 2.7 a commit.
- The merge, with the merge into an empty table its setup makes, reads 5.0 million times and
  writes 3.2 million, as on the parent commit (12.45 s against 12.39 s), since a keyed table's
  staging carries no staging index.
- The review measured 10.8–11.2 s and 17.5–18.3 s for the 102-commit append and the merge on the
  efficient cores; these are the performance cores.

`perf stat` cannot difference runs of several seconds; the appends keep 0.82–0.89 CPUs busy at
4.4–4.6 instructions a cycle.

## Logged ahead

`append/1000000/10000/unreplayable` loads `append/1000000/10000`'s rows, committed every 10 000,
from a source that cannot read them again, so the engine logs each load ahead with `LocalWal` in a
directory of its own beside the database's, on the same btrfs disk, within the four cores (2
runtime workers and a compute thread a core) and a budget of 1 GiB; it is the baseline a commit
that keeps no log of its own is held to.

- **Phases.** Each append prints its last run's commit phases a commit: the flush, the horizon,
  the log, the destination's commit and the acknowledgement.
- **Syncs by file.** One run of each case in criterion's test mode under
  `strace -f -y -e trace=fsync,fdatasync`, which names the file each sync reaches, as `just bench`
  takes them: SQLite's files (`loaded.db` and its `-wal`) apart from the log's (`wal/...`), though
  both run in the engine's process.
- **Running.** `just bench sqlite 0-3`, three rounds of the two appends of a million rows
  committed every 10 000, interleaved with the other benches' rounds ([campaign.md](campaign.md)),
  each started once no other build ran, the one-minute load average 1.25 to 2.15 at their starts;
  `main` at `6365cc13` with the served bench's readings (`d2fc0f95`; the engine's code is
  `main`'s), 2026-10-25 and -26. The median of the three rounds and their range.

| Case | Time a run | Rows a second | Commits | Flush | Horizon | Log | Commit | Ack |
|---|---|---|---|---|---|---|---|---|
| `append/1000000/10000` | 2.836 s (2.805–2.851) | 353 k | 101 | 13.60 ms | 0.000 ms | 0.008 ms | 13.80 ms | 0.011 ms |
| `append/1000000/10000/unreplayable` | 3.068 s (3.007–3.086) | 326 k | 101 | 13.52 ms | 0.149 ms | 1.804 ms | 14.09 ms | 0.006 ms |

Syncs by file, one run of each in test mode, its setup included (`strace -f -y`):

| Case | `loaded.db` | `loaded.db-wal` | `loaded.db-journal` | The directory holding both | The log (`wal/...`) | All syncs | `openat` |
|---|---|---|---|---|---|---|---|
| `append/1000000/10000` | 57 | 226 | 2 | 2 | – | 287 | 34 |
| `append/1000000/10000/unreplayable` | 57 | 225 | 2 | 3 | 313 | 600 | 5 703 |

SQLite syncs its files alike in both, about 2.8 times a commit; the log adds 313 syncs of its own,
about 3.1 a commit, and the logged run makes 5 669 more `openat` calls.

Logging the load ahead takes the run 8.2 % longer at the median, the log's phase 1.8 ms of each
commit's 29.6 ms.
