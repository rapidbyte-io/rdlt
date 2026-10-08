# Instruction and allocation counts

Every pull request counts what each hot path below takes per iteration, on the change and on the
base it merges into, and fails where a case takes more than 2% more instructions or allocations.
This record keeps the method, each case's counts when the gate began, and what was measured of
the gate. The counts of a change are in its CI run's summary; nothing reads a committed count.

## Cases

The cases are those of the engine's `allocations` bench whose names hold a `/`.

| Case | One iteration |
|---|---|
| `passthrough/null_sink` | `bench::Passthrough::run` moving 8 batches of 8,192 rows from a replaying source to the null sink, on one runtime worker and one compute thread |
| `shred/nested`, `shred/with_arrays`, `shred/flat_narrow`, `shred/wide_200`, `shred/string_heavy` | shredding 1 MiB of the corpus in 1 MiB chunks on the calling thread |
| `normalize/keyless/nested`, `normalize/keyless/with_arrays`, `normalize/keyed/with_arrays` | normalizing, to depth 8, keyless or keyed on `id`, the batches 1 MiB of the corpus shreds into |
| `wal/encode` | `bench::sample_log` of one 8,192-row batch of the passthrough workload's columns |
| `wal/scan` | `bench::scan_log` of that log |
| `ipc/roundtrip` | `rdlt_wire`'s `Encoder` and `Decoder` over that batch: its schema and its frames encoded, then decoded |

The passthrough case fixes its layout whatever the host has: a count is the same on every machine
only on one layout, so this is a measuring fixture, not a deployment default; every timed
workload still takes its layout from the host. The shred bench's `sparse` corpus is not a case:
it is `nested` with one row in ten thousand holding one more key.

## Method

- `just instructions [base]` runs `cargo xtask instructions --base <base> --limit 2`, `base`
  `origin/main` unless given. It checks the base out in a temporary worktree outside the tree, so
  none of this tree's cargo configuration reaches the base's build, and builds the `allocations`
  bench of both trees side by side as `cargo bench` builds it (the release profile: fat LTO, one
  codegen unit), each into its own target directory under `target/instructions`, since two
  checkouts sharing one would take each other's builds as their own, and both with the toolchain
  `rust-toolchain.toml` names. It then runs each case under
  `valgrind --tool=callgrind --fair-sched=yes` at one and at three iterations.
- A case's count is the difference of the two runs over two, so what the process does once,
  making the case's inputs among it, cancels out. Callgrind counts every thread, so work moved
  between the caller, the runtime and the compute pool still counts. Allocations come from the
  bench's `stats_alloc` counting allocator, in the same runs.
- A case fails when either count is more than 2% above the base's, unless a commit between the
  base and the change has an `Instructions-Accepted: <case>` trailer; a trailer naming no case of
  either tree fails the run. Under 50, one more is more than 2%, so such a count is held exactly.
  A case only one side has is reported, not judged.
- In CI, a pull request's base is the first parent of the merge commit CI checks out, the commit
  the change merges into, and a push to `main` is compared with the commit before it.
- Linux only. CI installs the runner image's valgrind from Ubuntu's archive, unlocked
  (ADR 0048); the gate refuses one older than 3.22. Both trees run under the same valgrind, so
  its version moves both sides alike.
- The report, `target/instructions/report.md` and the CI job's summary, opens with both
  revisions, the toolchain, valgrind's version, the build profile, the load average, and how long
  building and counting took. Each run's callgrind output is kept in
  `target/instructions/{base,head}/callgrind/<case>.<iterations>.out`, for `callgrind_annotate`.

## Counts when the gate began

At `ebb7c848`, per iteration, under valgrind 3.25.1 and Rust 1.98.1; `passthrough/null_sink`'s
instructions move between runs of one tree, as below:

| Case | Instructions | Allocations |
|---|---:|---:|
| `ipc/roundtrip` | 2,150,799 | 153 |
| `normalize/keyed/with_arrays` | 22,506,381 | 225 |
| `normalize/keyless/nested` | 43,581,894 | 17,624 |
| `normalize/keyless/with_arrays` | 56,574,919 | 12,991 |
| `passthrough/null_sink` | 4,464,486 | 3,602 |
| `shred/flat_narrow` | 50,711,464 | 52 |
| `shred/nested` | 41,204,415 | 190 |
| `shred/string_heavy` | 35,802,719 | 4,830 |
| `shred/wide_200` | 47,633,638 | 1,851 |
| `shred/with_arrays` | 48,369,600 | 10,871 |
| `wal/encode` | 4,775,577 | 142 |
| `wal/scan` | 5,072,218 | 179 |

## What was measured of it

On the change that added the gate (#77), on an Intel Core Ultra X7 358H, valgrind 3.25.1, Rust
1.98.1, the release profile. Counts do not depend on load, and the counting runs shared the
machine (one-minute loads up to 32.4); the durations started below a one-minute load of 1.0.

- **Repeatability.** The same tree against itself: every case but `passthrough/null_sink` read
  +0.00% in both counts in every run. That case's allocations read +0.00% too; its instructions
  read -0.36% to +0.28% over eight runs on four revisions of the change. What differs between
  runs is what follows the scheduler: glibc's malloc merging free chunks, rayon's and tokio's
  idle waiting, and how often the engine's futures are polled; the compute thread's own work
  counts alike. The gate cannot leave the waiting out, since the compute pool's jobs run inside
  rayon's loop, so the spread accepted for that case is 0.5%, against the 2% limit.
- **Sensitivity.** Against a tree that writes identity's floats in place, today's code fails
  exactly `normalize/keyless/nested` (+4.33% instructions, 104 → 17,624 allocations) and
  `normalize/keyless/with_arrays` (+2.31%, 256 → 12,991). Against a tree that skips the plain
  Int64 rounding scan, it fails exactly `passthrough/null_sink` (+66.06% instructions). Every
  other case stayed within.
- **Threads.** One run of `passthrough/null_sink` under `--separate-threads=yes` counted
  149,823,238 instructions on the main thread, 3,505,439 on the runtime worker and 796,563 on the
  compute thread.
- **Duration.** Locally, on cores 0-3 with four build jobs: 295 s (built in 258 s, counted in
  35 s) with both trees building from nothing, and 165 s (built in 129 s, counted in 35 s) with
  dependencies built and the workspace's crates rebuilt, as CI's warm runs are. In CI the job
  took 9.9 minutes from a cold cache (built in 482 s, counted in 57 s) beside its run's
  slowest other job, `test (macos-15)` at 17.8 minutes. Warm runs, which rebuild only the workspace's crates, are held to
  10 minutes and to the slowest other job.
