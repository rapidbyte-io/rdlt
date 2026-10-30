# The ground-truth campaign

One measurement campaign, on one tree, took every baseline the next changes are held against and
read each pre-check whose rule was written before the run: whether a redesign is worth building,
by the rule it was given. This record states each rule as written, the figures with their ranges
and method, and the verdict; the baselines themselves are in their benches' records, linked below.

## Method

- **Tree.** `main` at `6365cc13` with the served bench's two readings added (each process's user
  share of its CPU time, and the production slice's largest frames from sixteen partitions within
  1 GiB): `d2fc0f95`. The engine's code is `main`'s.
- **Machine.** Intel Core Ultra X7 358H, governor `powersave` with energy preference
  `performance`; the host on the four performance cores (0–3), spawned connectors on the eight
  efficient ones (4–11), the four low-power ones idle; Rust 1.98.1, release profile.
- **Rounds.** Each bench ran as `just bench <bench> <cores> <filter> <connectors>` runs its timed
  pass: the bench's binary under `taskset`, criterion saving a baseline, built beforehand.
  Rounds of different benches alternated, the order reversed between rounds: served,
  passthrough, latency, SQLite, wide, normalized, shredding; then back; then forward again. Three
  rounds each; five of the wide and normalized benches, as their records' methods say, their
  fourth and fifth last. Each hold of the measurement
  lock was one round of one bench, started once no other build ran and the one-minute load was
  under 3.0 (others' commands wait while the lock is held); `uptime` is recorded before and after
  each.
- **Figures.** A round's figure is criterion's point estimate, or the bench's own printed median
  over the round's runs; a table gives the median of the rounds and their range.
- **Profiles.** `perf record -e cycles:u` at 999 Hz with frame-pointer call graphs, on the
  profiling build (`just profiling <bench>`), the bench's process and every process it spawned;
  `perf_event_paranoid` 2 samples user mode alone. libc's copy routines are named from its
  debug information. Frame-pointer unwinding loses the immediate caller of a libc function, which
  keeps no frame; where a rule counts such a caller, the record says how it was recognised.

## Decisions

| Question | Rule | Figure | Verdict |
|---|---|---|---|
| Are the boundary's copies worth a stream of their own for each data call? | gathers and encode copies ≥ 12 % of the pipeline's CPU s a GB | 26.3 % (25.7–26.5) | Passed: built; failed its adoption rule and closed |
| Do pressure flushes cost served throughput? | `64×80000` from 16 partitions ≥ 10 % faster within 1 GiB | 4.4 % slower | Failed: closed |
| Does a checkpoint-set flush cost the in-process run? | `1000x5120` at k = 6 ≥ 1.5× k = 1 from 1 partition and ≥ 1.2× from 16 | 0.77× and 1.46× | Failed: closed |
| Can one hot partition use more lanes? | four partitions ≥ 1.25× one through a writer a core | 0.98× | Failed: closed |
| Does acknowledgement cost a many-table commit? | its phase > 100 ms at the most served streams | 6.88 ms | Failed: closed |
| What do many followed reads keep? | < a quarter of the reads' share at their least budget | 0.36 % | Passed: built, the read kept lazily |
| Does the normalized hold cost? | 16 partitions ≥ 15 % slower within 256 MiB than 1 GiB | 9.9 % | Failed |
| Is the normalized split worth a window? | four cores < 1.5× one, and split jobs > 30 % of samples | 2.04×, 3.7 % | Failed; with the hold's, both steps closed |
| What does a wide table pay a column? | closes if under 10 % of allocations and time in every case | time 8.6 %, 18.3 %, 17.0 %; allocations 6.6 %, 34.3 %, 34.3 % | Not closed |
| Is a pure commit value worth reopening? | positions' encoding and decoding > a quarter of `commit/1024`'s instructions | at most 12.3 % | Not reopened |
| Why does served throughput fall with partitions? | diagnosed once | per-partition and per-writer work in every process | Diagnosed |

The baselines the later rules read, and their records, are listed after the decisions.

### A framed stream for each data call

- **Rule.** Build a stream of its own for each data call, read into place and written from the
  batch's own buffers, only if the receivers' gathers and the senders' encode copies, each
  weighted by its process's user share, come to at least 12 % of the pipeline's CPU seconds a GB
  in `both/64x80000`, from a per-thread profile of each process with its user and system split.
- **Figures.** 26.3 % (25.7–26.5 % over three rounds): the gathers 12.4 %, the encode copies
  14.0 %; counting only the gather's samples whose caller unwinding keeps, 15.5–16.0 %
  ([served.md](served.md), "The boundary's copies").
- **Verdict.** Passed: built. Built, it failed its adoption rule, as the 1 000-row cells through one
  writer moved 18–25 % less, and closed ([served.md](served.md), "One framed stream a data
  call").

### Freeing a consuming writer's write as it returns

- **Rule.** Stop holding a served lane's written batches until its flush only if
  `64×80000` from 16 partitions runs at least 10 % faster at the median within 1 GiB than within
  256 MiB, on the same tree, with the lane flushes counted.
- **Figures.** Through a writer a core, the rows the production path's later changes are held to:
  1 298 MB/s [1 280–1 351] within 1 GiB against 1 358 [1 261–1 374] within 256 MiB,
  4.4 % slower, pressure flushes 4 against 27. Through one writer: 1 346 [1 304–1 348] against
  1 449 [1 413–1 523], 7.1 % slower, pressure flushes 1 against 9 ([served.md](served.md), "Within
  1 GiB").
- **Verdict.** Failed: closed. The pressure flushes the larger budget removes are not what the
  case pays.

### Spilling a hot partition's writes to lanes with room

- **Rule.** Build only if four partitions move at least 1.25 times one partition, ranges apart, on
  `both/64x80000` through as many writers as lanes.
- **Figures.** 1 576 MB/s [1 523–1 758] from four partitions against 1 612 [1 559–1 875] from one,
  0.98 times, the ranges overlapping.
- **Verdict.** Failed: closed. A partition keeps writing a table through one lane.

### One acknowledgement call a commit

- **Rule.** Build only if the many-table cell of the most streams one served connection carries
  (192) shows the acknowledgement phase's median above 1 % of the streaming interval, 100 ms.
- **Figures.** 6.88 ms a commit (6.69–7.20).
- **Verdict.** Failed: closed.

### Sharing followed reads by what they keep

- **Rule.** Build only if, at the most streams served, followed reads keep under a quarter of the
  reads' share at their peak, at the cell's least budget. Then the read kept lazily, unless idle
  CPU passes 10 % of a core or acknowledgement passes a quarter of a commit's time after one call
  acknowledges a commit: then one read for several streams.
- **Figures.** 352 KiB kept at most of a 96.5 MiB share (0.36 %) at 192 streams within 386 MiB,
  their least budget. Idle CPU 0.008 of a core (host 0.006, source 0.002, destination 0.000).
  Acknowledgement is 61 % of a commit's 11.2 ms, through one call a stream.
- **Verdict.** Passed: built, the read kept lazily. The second condition for one read for several
  streams cannot be read as written: no call acknowledges a commit at once, since that change
  closed above, and the per-stream figure is what such a call would change. The rule's default,
  the lazily kept read, stands.

### Sealing a checkpoint only once its pushes are worth a write

- **Rule.** In process, checkpointing every 6th push against every push: `1000x5120` from one
  partition at least 1.5 times and from sixteen at least 1.2 times, ranges apart. If this fails,
  the in-process flush is not the cost, and the change closes; its served half waits for the
  framed stream's decision.
- **Figures.** From one partition 3 183 MB/s [3 172–3 187] against 4 113 [3 628–4 128], 0.77 times;
  from sixteen 6 677 [6 676–6 683] against 4 565 [4 449–4 571], 1.46 times, the ranges apart
  ([passthrough.md](passthrough.md), "Checkpoints every k pushes").
- **Verdict.** Failed: closed. Sixteen partitions gain, one partition loses, and the rule asks
  both.

### Normalized holds sized by the split, and splits in a window

- **Rules.** The hold: build its first step only if sixteen partitions within 256 MiB run at
  least 15 % slower than within 1 GiB (`normalized/partitions/16`). The window: build both steps
  only if four cores run `just bench normalized` under 1.5 times one core, and the split jobs
  (concatenation, key check, normalize) hold more than 30 % of a four-core profile's samples. The
  first step builds if either rule passes, the second only if the window's does.
- **Figures.** The hold: 49.8 ms (48.5–51.5) within 256 MiB against 45.3 (44.5–52.2) within 1 GiB,
  9.9 % slower. The window: on `normalized/keyless/50000`, the case whose flushes hold several
  units for a window to split together, four cores take 26.2 ms (25.9–28.6) against one core's
  53.4 (53.3–54.3), 2.04 times; 1.42 times at 10 000 rows a push and 0.83 times at 2 000. The split
  jobs hold 3.7 % of a four-core profile's samples ([normalized.md](normalized.md)).
- **Verdict.** Both failed: both steps closed. Four partitions within 256 MiB take 51 % longer than
  within 1 GiB, but the rule reads sixteen.

### What a wide table pays a column

- **Rule.** Close the first three steps (sharing the incoming schema and matching plans by
  pointer, measuring a fitting batch once, one null array a type a batch) if the per-batch
  schema-shaped work they change holds under 10 % of both the allocations and the time of
  `arrow/5000` and of `sparse` (Arrow and JSON).
- **Figures.** Time, from a four-core profile of each: `arrow/5000` 8.6 %, `sparse/arrow` 18.3 %,
  `sparse/json` 17.0 %, the null fill 18.0 % and 16.7 % of the sparse ones. Allocation calls,
  from heaptrack: 6.6 %, 34.3 % and 34.3 %, the null fill 33.7 % of each sparse case's
  ([wide.md](wide.md), "Where a wide run's time goes").
- **Verdict.** Not closed: `arrow/5000` is under 10 % of both, but the sparse table's null fill
  is over 10 % of its time and of its allocations. The steps stand for their own measured rules.

### A pure commit value

- **Rule.** Reopen building a commit as one pure value when `commit/1024` shows re-encoding and
  decoding positions above a quarter of its instructions.
- **Figures.** The coordinator's whole commit holds 12.3 % of the case's instructions, positions'
  encoding and decoding among it ([instructions.md](instructions.md), "Counts of the ground-truth
  cases").
- **Verdict.** Not reopened.

## Why served throughput falls with partitions

Diagnosed once, from the production slice's rows through one writer and through a writer a core
in the same rounds: both fall from 1 to 16 partitions, intake waits stay under 3 % of a run, the
same frames within 1 GiB move no more, and CPU a GB rises while CPUs busy fall. The decline is
work each process does a partition (the source's above all) and a writer, not the sink's single
writer and not the budget ([served.md](served.md), "Through a writer a core").

## Baselines a rule reads later

- **Jobs sized by rows, cores and a limit.** `just bench wide 0-3` re-recorded with a compute
  thread a core, the per-stage profile of `json/5000` (shredding 65 % of its time), `shred_cores`
  from one to four threads on the performance cores and from one to eight on the efficient ones,
  and the JSON cells in process on both: [wide.md](wide.md), [shred.md](shred.md),
  [passthrough.md](passthrough.md). Recorded; the sweep runs against them.
- **One tokenizer.** `shred_cliffs` and the one-core group on this tree, with per-function
  profiles of five corpora: [shred.md](shred.md). Recorded; the arms run against them.
- **A merge staging only each key's last row's children.** `normalized/merge/zipf` in process and
  served, with what each commit stages: [normalized.md](normalized.md). Recorded; the change's
  rule compares itself with it.
- **The latency cell**, both placements at both commit policies: [latency.md](latency.md).
- **The log's cost** on a durable destination, with syncs by file: [sqlite.md](sqlite.md), "A
  source that cannot read again".
- **`commit/1024`**, `shred/rows` and `wide/arrow_1000`, counted: [instructions.md](instructions.md).
- **The many-table cells**, in process and served: [passthrough.md](passthrough.md) and
  [served.md](served.md), "Many tables".

## Holds

Every hold of the measurement lock, in order, with the one-minute load average at its start and
end. The counts that do not depend on load (instructions, allocation calls by stage, syncs by
file) took the last hold, as a mutation run holds the lock; the allocation counts ran before it.

| Hold | Started | Minutes | Load before | Load after |
|---|---|---|---|---|
| Served, `process/socket/both`, round 1 | 10-25 22:04 | 12.0 | 1.15 | 1.05 |
| Profile of the spawned pipeline, dwarf call graphs (not read) | 10-25 22:20 | 0.3 | 2.48 | 2.33 |
| Profile of the spawned pipeline | 10-25 22:56 | 0.5 | 1.37 | 1.64 |
| Passthrough: chosen, JSON and many-table cells, round 1 | 10-25 22:57 | 10.8 | 1.17 | 0.68 |
| Latency, round 1 | 10-25 23:08 | 13.0 | 0.77 | 4.53 |
| SQLite: the two appends of a million rows, round 1 | 10-25 23:22 | 1.2 | 1.96 | 2.52 |
| Wide, round 1 | 10-25 23:24 | 1.5 | 1.86 | 1.50 |
| Normalized, four cores then one, round 1 | 10-25 23:26 | 2.0 | 1.15 | 2.47 |
| Shredding: one core, `0-3`, `4-11`; JSON cells on `4-11`, round 1 | 10-25 23:28 | 4.1 | 1.84 | 4.37 |
| Shredding: one core, `0-3`, `4-11`; JSON cells on `4-11`, round 2 | 10-25 23:35 | 4.0 | 2.02 | 6.15 |
| Normalized, four cores then one, round 2 | 10-25 23:45 | 1.9 | 2.33 | 2.31 |
| Profile of `normalized/keyless/50000` | 10-25 23:49 | 0.2 | 1.71 | 1.84 |
| Wide, round 2 | 10-26 00:06 | 1.5 | 2.30 | 1.96 |
| Profiles of wide cases (sparse) | 10-26 00:15 | 0.7 | 1.41 | 1.50 |
| SQLite: the two appends of a million rows, round 2 | 10-26 00:24 | 1.2 | 1.25 | 2.13 |
| Profiles of five shredding corpora | 10-26 00:26 | 1.0 | 1.90 | 2.13 |
| Profiles of wide cases (`arrow/5000`, `json/5000`) | 10-26 00:29 | 0.3 | 2.39 | 2.20 |
| Latency, round 2 | 10-26 00:38 | 12.8 | 1.12 | 4.56 |
| Passthrough: chosen, JSON and many-table cells, round 2 | 10-26 00:56 | 10.8 | 1.17 | 0.66 |
| Served, `process/socket/both`, round 2 | 10-26 01:14 | 12.1 | 2.69 | 1.55 |
| Served, `process/socket/both`, round 3 | 10-26 01:34 | 12.1 | 2.22 | 0.93 |
| Passthrough: chosen, JSON and many-table cells, round 3 | 10-26 01:56 | 10.7 | 1.73 | 0.37 |
| Latency, round 3 | 10-26 02:21 | 12.9 | 2.15 | 5.65 |
| SQLite: the two appends of a million rows, round 3 | 10-26 03:11 | 1.2 | 2.15 | 4.16 |
| Wide, round 3 | 10-26 03:14 | 1.5 | 2.31 | 2.50 |
| Normalized, four cores then one, round 3 | 10-26 03:51 | 2.0 | 2.10 | 3.88 |
| Shredding: one core, `0-3`, `4-11`; JSON cells on `4-11`, round 3 | 10-26 04:12 | 4.2 | 2.54 | 5.51 |
| Wide, round 4 | 10-26 04:20 | 1.4 | 2.38 | 4.25 |
| Normalized, four cores then one, round 4 | 10-26 04:42 | 1.9 | 2.27 | 2.56 |
| Normalized, four cores then one, round 5 | 10-26 05:02 | 2.0 | 2.29 | 2.39 |
| Wide, round 5 | 10-26 05:28 | 1.6 | 2.18 | 1.35 |
| Counts: instructions, heap profiles of four wide cases, syncs by file | 10-26 07:42 | 3.8 | 1.90 | 1.43 |
