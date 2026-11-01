# The engine before and after

One comparison of two engines on one machine: the engine at `41284517`, against `main` at
`10669fa9`, 346 commits later, with its durability, commit, normalize, JSON, accounting and served
paths changed. Each change's own record measured it against the `main` of its day; this record
measures what they come to together. It keeps the method, the instruction and allocation counts of
every case both trees have, the resident peaks, every timed cell both have under the same name and
meaning, what only one tree has, and what moved and why.

## Trees and machine

- **Before.** `41284517` (2026-10-23), kept on the remote as `baseline/engine-before-improvements`.
- **After.** `main` at `10669fa9` (2026-11-01): 924 files changed, 45 102 lines added and 54 154
  deleted, 39 255 and 47 032 of them under `crates/`.
- **Dependencies.** Both trees lock the same version of every crate both use, Arrow, tokio, hyper,
  tonic, rustls and serde_json among them, but `pulldown-cmark` (0.13.4 to 0.13.5), which only
  `xtask`'s code generation uses. `main` locks 116 crates fewer, `object_store` and the container
  clients among them.
- **Build.** Rust 1.98.1 and the release profile both trees declare alike: fat LTO and one codegen
  unit.
- **Machine.** Intel Core Ultra X7 358H, 16 CPUs: four performance cores (0–3, up to 4.7 GHz),
  eight efficient ones (4–11, up to 3.7 GHz) sharing their L3, and four low-power ones (12–15);
  mains power; governor `powersave` with energy preference `performance` on every CPU; Linux
  7.2.8; valgrind 3.25.1.
- **Dates.** 2026-11-01: the timed rounds from 08:45 to 12:00, the counts and resident runs after.

## Method

- **A shared machine.** The laptop ran a virtual machine and its owner's other work throughout
  the rounds; no other build or bench of this repository ran during them. Each hold of the timed
  lock waited for a one-minute load average under 1.0 before it started: the starts read 0.92 to
  0.99 over one minute and 1.18 to 2.81 over five, and the holds ended at one-minute loads of 1.14
  to 4.24, the benches' own threads among them. Absolute times are one shared morning's; what the
  timed cells give is direction. The instruction and allocation counts, which load does not move,
  are the measure of efficiency.
- **Cells.** Every cell both trees have under the same name, where the engine is asked for the same
  work: the same rows, batches, partitions, budget, commits and sink. Each bench's ids were listed
  on both trees and their sources compared. One pipeline is renamed: the base's
  `passthrough/logged/{tmpfs,disk}/8000x640/1p/2s/16th` moves a stream its source can read again
  beside one it cannot, the second's batches logged ahead into a write-ahead log on tmpfs or on the
  disk beside the build; on `main` the same pipeline is `passthrough/mixed/mixed/...`, both streams
  told their cursors once the commit holding their rows lands, and the base's both-readable
  `logged/unlogged/...` is `mixed/replayable/...`.
- **Cells ported to the base.** Three things `main` measures and the base has no cell for ran on
  the base through ports of `main`'s cells to the base's public bench API, kept outside both trees
  and built in the base's workspace with its lockfile and profile: the passthrough bench's JSON
  cells (`passthrough/json/<corpus>/<P>p`: the same pushes, partitions, budget, sink and writers,
  the engine the bench's `Passthrough` workload's, whose runner differs from the cell's only in the
  pipeline's name); the counted cases `commit/1024`, `shred/rows` and `wide/arrow_1000`, the
  same code on the base's types, `shred/rows` pushing what the base's `Emitter::rows` pushed, one
  JSON array of the records; and the served line of `just resident`. Each ported cell checks the
  rows it loads, as its counterpart does, and each JSON cell loads the same rows of the same bytes
  on both trees (739 058 rows of 134 217 778 bytes, and 50 175 of 134 219 570).
- **The harness made alike.** `main`'s bench harness shares the pushes a replay source makes, where
  the base's copied every batch's columns as each source connected, inside a timed run; and
  `main`'s wide workload keeps its replay shared, where the base's copied its batches each run, and
  builds its Arrow batches on the first batch's schema, as a source that builds its batches on one
  schema does, where the base gave each batch a schema of its own. The base's harness was given
  the same three changes, 43 lines added and 18 removed in its `bench` feature, a test's among
  them, and none in its engine, so both engines load the same inputs and both benches do the same
  work around them.
- **Timed rounds.** Each tree's bench executables were built beforehand with `cargo bench
  --no-run`, and each ran its criterion pass as `just bench` runs it: `taskset -c 0-3` for the
  host, each spawned connector held to 4–11 (`RDLT_BENCH_CONNECTOR_CORES`). A round is one bench
  on both trees in one hold of the measurement lock, the two trees back to back, the tree run first
  alternating from bench to bench and from round to round, `uptime` taken before and after each
  tree. Three rounds of every bench; five of the served, passthrough and shredding benches, whose
  ranges overlapped most after three. The lowering bench's cases take microseconds to a fraction of
  a second, and whole rounds of it put the tree run first behind ([wide.md](wide.md)), so each of
  its holds ran every case on the base, `main`, `main` and the base in turn: six figures a tree.
- **Figures.** A round's figure is criterion's point estimate, and for the served cases also the
  median over its samples of the CPU seconds every process took a GB moved. A table gives the
  median of the rounds and their range, `main`'s median over the base's, and a verdict: faster or
  slower (leaner or costlier for CPU) only where the two trees' ranges over every round are apart,
  level otherwise. Throughput is decimal MB a second of the logical bytes a cell moves, as
  [passthrough.md](passthrough.md) and [served.md](served.md) count them.
- **Counts.** `just instructions 41284517` on `main`: each tree's release `allocations` bench under
  `valgrind --tool=callgrind --fair-sched=yes` at one and three iterations, their difference over
  two, as [instructions.md](instructions.md) describes. The three ported cases ran on the base by
  the same commands; the same script on `main`'s bench read within 0.7 % of `just instructions`.
- **Resident.** Every line `just resident` runs, five times a tree, the trees alternating line by
  line, each in a process of its own.

## Instructions and allocations

Per iteration, as `just instructions` counts them; a case marked ported was counted on the base
through its port.

| Case | Instructions, base | `main` | Change | Allocations, base | `main` | Change |
|---|---:|---:|---:|---:|---:|---:|
| `commit/1024` (ported) | 232 584 434 | 221 296 118 | −4.85 % | 277 798 | 232 692 | −16.24 % |
| `passthrough/null_sink` | 4 031 560 | 3 489 053 | −13.46 % | 2 938 | 2 435 | −17.12 % |
| `wide/arrow_1000` (ported) | 169 770 503 | 129 976 849 | −23.44 % | 164 455 | 138 733 | −15.64 % |
| `shred/rows` (ported) | 62 661 543 | 37 885 519 | −39.54 % | 190 | 190 | 0.00 % |
| `shred/nested` | 38 669 641 | 37 901 235 | −1.99 % | 190 | 190 | 0.00 % |
| `shred/with_arrays` | 44 025 205 | 43 096 107 | −2.11 % | 327 | 327 | 0.00 % |
| `shred/flat_narrow` | 44 769 646 | 44 190 191 | −1.29 % | 52 | 52 | 0.00 % |
| `shred/wide_200` | 45 536 612 | 44 565 726 | −2.13 % | 1 851 | 1 851 | 0.00 % |
| `shred/string_heavy` | 33 904 481 | 33 731 968 | −0.51 % | 4 830 | 4 830 | 0.00 % |
| `shred/array_form` | 62 518 292 | 61 795 004 | −1.16 % | 190 | 190 | 0.00 % |
| `shred/drift_json` | 105 366 643 | 104 612 691 | −0.72 % | 5 552 | 5 552 | 0.00 % |
| `shred/drift_scalar` | 41 801 554 | 41 002 806 | −1.91 % | 209 | 209 | 0.00 % |
| `shred/big_numbers` | 85 092 375 | 85 087 926 | −0.01 % | 7 474 | 7 474 | 0.00 % |
| `normalize/keyed/with_arrays` | 17 948 335 | 5 661 453 | −68.46 % | 176 | 84 | −52.27 % |
| `normalize/keyless/nested` | 37 157 524 | 20 139 | −99.95 % | 103 | 27 | −73.79 % |
| `normalize/keyless/with_arrays` | 47 891 611 | 675 243 | −98.59 % | 207 | 77 | −62.80 % |
| `normalize/dropped/with_arrays` | 50 056 508 | 1 848 038 | −96.31 % | 285 | 132 | −53.68 % |
| `ipc/roundtrip` | 1 437 378 | 1 437 833 | +0.03 % | 148 | 150 | +1.35 % |
| `json/check` | 33 736 892 | 33 736 906 | +0.00 % | 2 | 2 | 0.00 % |
| `lowering/append_text` | 54 340 842 | 54 340 952 | +0.00 % | 88 | 87 | −1.14 % |
| `lowering/merge_unique` | 1 692 278 | 1 701 406 | +0.54 % | 50 | 25 | −50.00 % |
| `lowering/merge_duplicates` | 10 973 566 | 10 975 036 | +0.01 % | 88 | 63 | −28.41 % |
| `lowering/history` | 265 818 768 | 265 689 577 | −0.05 % | 372 | 115 | −69.09 % |
| `lowering/changes` | 6 008 846 | 5 659 899 | −5.81 % | 440 | 34 | −92.27 % |
| `lowering/split_json` | 12 608 943 | 12 664 044 | +0.44 % | 356 | 355 | −0.28 % |
| `lowering/one_bad_value` | 604 183 | 605 250 | +0.18 % | 260 | 261 | +0.38 % |
| `lowering/one_dropped_row` | 1 762 140 | 1 766 053 | +0.22 % | 293 | 296 | +1.02 % |
| `lowering/temporal_widening` | 168 845 | 168 894 | +0.03 % | 19 | 18 | −5.26 % |

- **No case takes more than 0.54 % more instructions or 1.35 % more allocations**: the largest
  rises are `lowering/merge_unique`'s instructions and `ipc/roundtrip`'s allocations, 148 to 150.
- **The normalize cases do less work inside the case.** On `main` a row no merge identifies by its
  key, every keyless row and a merge's children, carries its position, from which lowering builds
  its id, outside these cases; the base hashed every root and item inside them (ADR 0009;
  [normalized.md](normalized.md), "Ids by role"). Their falls are mostly work moved out of the
  case, not out of the run: 96–99.95 % for the keyless cases, and 68.46 % for
  `normalize/keyed/with_arrays`, whose roots' keys both trees hash. The end-to-end
  `normalized/keyless/*` cells below hold the whole of the keyless work.
- `passthrough/null_sink` is a case whose instructions move up to 0.5 % between runs of one tree
  ([instructions.md](instructions.md)); `commit/1024` moved 0.33 % between this record's two counts
  of `main`.
- `wal/encode` (3 388 115 instructions, 132 allocations) and `wal/scan` (5 092 255, 182) are the
  base's alone: `main` keeps no log.

## Resident memory

Five runs a line on each tree, MiB, the median and range. Growth is the process's peak resident
memory above its resident memory once its inputs are made; `peak_memory` is the most of its budget
the engine reserved; the limit is the budget, a fifth of it and 32 MiB, the frames held unadmitted,
the served transport's windows and a stack a thread, the same on both trees
([resident.md](resident.md)). A verdict compares growth's ranges.

| Line | `peak_memory`, base | `main` | Growth, base | `main` | Limit | Growth |
|---|---|---|---|---|---|---|
| `passthrough/2` | 86 [79–86] | 106 [106–106] | 11 [10–13] | 10 [10–12] | 355 | level |
| `passthrough/2/tunables` | 86 [86–86] | 106 [106–106] | 13 [12–13] | 12 [12–12] | 355 | level |
| `passthrough/4` | 86 [86–86] | 106 [106–106] | 12 [11–12] | 11 [10–11] | 359 | level |
| `passthrough/4/tunables` | 86 [79–86] | 106 [106–106] | 13 [12–13] | 12 [12–12] | 359 | level |
| `passthrough/16` | 86 [86–86] | 106 [106–106] | 11 [11–12] | 12 [11–12] | 395 | level |
| `passthrough/16/tunables` | 86 [86–86] | 106 [106–106] | 13 [13–13] | 12 [12–13] | 395 | level |
| `normalized/2` | 144 [144–144] | 163 [163–163] | 15 [15–16] | 14 [14–14] | 358 | lower |
| `normalized/2/tunables` | 144 [144–144] | 163 [163–163] | 18 [18–19] | 15 [15–15] | 358 | lower |
| `normalized/4` | 144 [144–144] | 163 [163–163] | 15 [15–16] | 14 [13–14] | 362 | lower |
| `normalized/4/tunables` | 144 [144–144] | 163 [163–163] | 23 [21–24] | 17 [16–17] | 362 | lower |
| `normalized/16` | 144 [144–144] | 163 [163–163] | 18 [17–21] | 16 [15–17] | 398 | level |
| `normalized/16/tunables` | 144 [144–144] | 163 [163–163] | 33 [32–34] | 21 [21–22] | 398 | lower |
| `served/.../2` | 94 [87–94] | 100 [100–107] | 181 [175–194] | 240 [224–266] | 522 | higher |
| `served/.../2/tunables` | 94 [87–94] | 107 [107–107] | 180 [169–255] | 262 [228–297] | 522 | level |
| `served/.../4` | 94 [94–94] | 107 [100–107] | 186 [174–219] | 257 [233–276] | 526 | higher |
| `served/.../4/tunables` | 94 [87–94] | 107 [100–107] | 228 [175–309] | 274 [249–330] | 526 | level |
| `served/.../16` | 101 [89–109] | 109 [107–115] | 255 [232–351] | 315 [289–367] | 562 | level |
| `served/.../16/tunables` | 101 [94–116] | 115 [107–122] | 380 [357–393] | 396 [364–403] | 562 | level |

`served/...` is `served/process/socket/both/64x80000/16-partitions`, its connectors spawned; the
base's ran through its port. Their spawned processes' peaks, in the same runs, MiB:

| Line | Source, base | `main` | Destination, base | `main` |
|---|---|---|---|---|
| `2` | 943 [917–951] | 918 [901–935] | 78 [70–91] | 81 [72–86] |
| `2/tunables` | 915 [904–932] | 920 [895–935] | 84 [72–91] | 79 [66–88] |
| `4` | 907 [893–921] | 930 [923–939] | 74 [62–91] | 89 [78–91] |
| `4/tunables` | 907 [903–930] | 918 [888–932] | 83 [66–102] | 79 [70–87] |
| `16` | 920 [890–936] | 885 [858–916] | 82 [67–97] | 82 [62–92] |
| `16/tunables` | 882 [868–946] | 870 [860–931] | 68 [63–83] | 79 [66–81] |

- **Every run on both trees stays within its limit**: the largest growth is `main`'s served line
  on 16 cores with the tunables, 403 of 562 MiB.
- **The engine reserves more of the same budget for the same work**: `peak_memory` 86 to 106 MiB
  for the passthrough workload, 144 to 163 for the normalized one. No record measured the rise;
  between the trees a push and a piece came to be charged the arrays and fields they hold beside
  their buffers (ADR 0039), and the log's share of the budget went to the others. The process's
  growth did not follow: level for the passthrough workload, and lower for the normalized one in
  five lines of six (33 to 21 MiB on 16 cores with the tunables).
- **The served host grows more at two and four cores**, 181 to 240 and 186 to 257 MiB without the
  tunables, its ranges apart; with them and at 16 cores the ranges overlap. The spawned processes'
  peaks overlap in every line but one: the source at four cores without the tunables, 907 to
  930 MiB, its ranges apart by 2 MiB.

## Timed cells

### The production path

`served/process/socket/both`: both connectors spawned over a socket pair, the replay source making
its batches in its own process and the IPC sink, declaring one writer, writing them in its own;
the passthrough bench's 441 MB a run in frames of 80 000, 10 000 or 1 000 rows, from 1, 4, 8 or 16
partitions; the engine within the default budget, one commit. Five rounds.

| Frames | Partitions | MB/s, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|---|
| 64 × 80 000 | 1 | 2 022 [1 841–2 084] | 2 081 [1 886–2 156] | 1.03 | level |
| 64 × 80 000 | 4 | 1 962 [1 687–1 970] | 1 975 [1 723–2 138] | 1.01 | level |
| 64 × 80 000 | 8 | 1 893 [1 566–1 943] | 1 894 [1 594–1 926] | 1.00 | level |
| 64 × 80 000 | 16 | 1 815 [1 488–1 819] | 1 617 [1 370–1 795] | 0.89 | level |
| 512 × 10 000 | 1 | 3 591 [3 573–3 662] | 3 104 [3 063–3 565] | 0.86 | slower |
| 512 × 10 000 | 4 | 3 037 [2 758–3 136] | 2 726 [2 596–2 845] | 0.90 | level |
| 512 × 10 000 | 16 | 2 691 [2 666–2 722] | 2 523 [2 450–2 573] | 0.94 | slower |
| 5 120 × 1 000 | 1 | 1 997 [1 979–2 065] | 1 903 [1 849–1 985] | 0.95 | level |
| 5 120 × 1 000 | 4 | 1 768 [1 754–1 773] | 1 668 [1 645–1 739] | 0.94 | slower |
| 5 120 × 1 000 | 16 | 1 607 [1 592–1 623] | 1 606 [1 587–1 636] | 1.00 | level |

CPU seconds a GB moved, every process's together and the host's alone:

| Frames | Partitions | All, base | `main` | Main ÷ base | Verdict | The host's, base | `main` | Verdict |
|---|---|---|---|---|---|---|---|---|
| 64 × 80 000 | 1 | 1.839 [1.829–1.925] | 1.820 [1.796–1.914] | 0.99 | level | 0.723 [0.687–0.770] | 0.674 [0.658–0.754] | level |
| 64 × 80 000 | 4 | 1.901 [1.817–2.033] | 1.923 [1.773–2.073] | 1.01 | level | 0.731 [0.712–0.839] | 0.717 [0.683–0.822] | level |
| 64 × 80 000 | 8 | 1.931 [1.921–2.153] | 2.006 [1.924–2.134] | 1.04 | level | 0.717 [0.701–0.878] | 0.753 [0.711–0.867] | level |
| 64 × 80 000 | 16 | 1.998 [1.987–2.207] | 2.092 [2.006–2.263] | 1.05 | level | 0.728 [0.716–0.883] | 0.822 [0.736–0.923] | level |
| 512 × 10 000 | 1 | 1.300 [1.292–1.314] | 1.416 [1.296–1.458] | 1.09 | level | 0.456 [0.450–0.459] | 0.529 [0.483–0.539] | costlier |
| 512 × 10 000 | 4 | 1.424 [1.409–1.545] | 1.512 [1.495–1.555] | 1.06 | level | 0.492 [0.482–0.529] | 0.540 [0.532–0.562] | costlier |
| 512 × 10 000 | 16 | 1.593 [1.579–1.599] | 1.623 [1.617–1.645] | 1.02 | costlier | 0.549 [0.549–0.563] | 0.587 [0.576–0.602] | costlier |
| 5 120 × 1 000 | 1 | 2.534 [2.438–2.548] | 2.598 [2.530–2.634] | 1.03 | level | 0.941 [0.912–0.948] | 0.971 [0.933–0.987] | level |
| 5 120 × 1 000 | 4 | 2.846 [2.840–2.858] | 2.891 [2.830–2.947] | 1.02 | level | 1.054 [1.049–1.055] | 1.089 [1.049–1.113] | level |
| 5 120 × 1 000 | 16 | 2.946 [2.892–2.970] | 2.918 [2.901–2.961] | 0.99 | level | 1.145 [1.125–1.148] | 1.128 [1.103–1.143] | level |

- **Frames of 80 000 rows are level** at every count of partitions: `main`'s medians are 0.89 to
  1.03 times the base's, the ranges overlapping, and so are their CPU seconds a GB.
- **Frames of 10 000 rows are slower from one partition and from sixteen**, 0.86 and 0.94 times the
  base's MB/s, the ranges apart; from four the ranges overlap (0.90 at the medians). In all three
  the host's process takes more CPU a GB, 1.07 to 1.16 times the base's, its ranges apart; every
  process's together is costlier, by 1.02 times, only from sixteen partitions.
- **Frames of 1 000 rows are slower from four partitions**, 0.94 times the base's, and level from
  one and from sixteen.
- **Not located.** No change's record measured these cells across the changes between the trees.
  The served read bounded by bytes was held to 3 % of its `main` on them and measured 10 000-row
  frames from one partition 2.4 % slower at its median ([served.md](served.md), "A served read
  bounded by bytes"); a change since closed fell below its `main`'s range on this cell alone
  ([served.md](served.md), "One charge a served frame"). A profile of the host on each tree
  (`perf record -e cycles:u` at 999 Hz with frame pointers, criterion's profiling mode) gave
  35–36 % of the host's samples to hashing the connector's binary as each run spawns it and most
  of the rest to making the runs' inputs, which leaves the runs' own work too small a share to say
  where the added CPU goes. Which change costs it is not resolved here.

### In process

#### Passthrough

The passthrough bench's chosen cells: 442 MB a run of the same ten columns in batches of 80 000,
8 000 or 1 000 rows, into the IPC sink or, as `/null`, a sink that drops them; and the paired cell
against its bare loop. Five rounds.

| Cell | MB/s, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|
| `paired/2+4` | 14 587 [13 719–15 316] | 14 887 [14 246–15 347] | 1.02 | level |
| `matrix/80000x64/1p/1s/end` | 14 848 [14 672–15 024] | 14 754 [13 810–14 981] | 0.99 | level |
| `matrix/80000x64/4p/1s/end` | 18 597 [18 021–19 207] | 18 105 [17 479–19 135] | 0.97 | level |
| `matrix/80000x64/16p/1s/end` | 18 195 [16 640–19 385] | 18 677 [18 425–19 550] | 1.03 | level |
| `matrix/80000x64/1p/4s/end` | 18 033 [15 409–19 072] | 18 255 [18 176–18 847] | 1.01 | level |
| `matrix/80000x64/1p/1s/16th` | 13 412 [13 051–14 033] | 14 334 [13 921–14 585] | 1.07 | level |
| `matrix/8000x640/1p/1s/end` | 13 474 [12 740–13 919] | 13 129 [12 270–13 250] | 0.97 | level |
| `matrix/8000x640/16p/1s/end` | 15 907 [15 512–16 163] | 16 838 [16 478–17 550] | 1.06 | faster |
| `matrix/8000x640/1p/1s/end/null` | 16 546 [15 976–16 867] | 18 233 [17 738–18 347] | 1.10 | faster |
| `matrix/1000x5120/1p/1s/end` | 4 393 [4 209–4 463] | 5 376 [5 264–5 616] | 1.22 | faster |
| `matrix/1000x5120/16p/1s/end` | 4 970 [4 427–5 036] | 5 354 [5 280–5 510] | 1.08 | faster |
| `matrix/1000x5120/16p/1s/16th` | 4 869 [4 618–5 013] | 5 355 [5 251–5 539] | 1.10 | faster |
| `matrix/1000x5120/1p/1s/end/null` | 4 374 [4 169–4 501] | 5 498 [5 361–5 684] | 1.26 | faster |

- **Runs of many small pushes are faster**: the 1 000-row batches, 5 120 pushes a run, move 1.08
  to 1.26 times as much; the 8 000-row batches are faster from sixteen partitions and into the
  null sink, and level from one into the IPC sink; the 80 000-row batches, 64 pushes a run, are
  level. The base measured each batch against its piece in doubling stretches, where `main`
  measures a batch that fits once ([wide.md](wide.md), "Schema-shaped work done once a schema"):
  of `passthrough/null_sink`'s 542 507 fewer instructions an iteration, 239 288 are the cost
  meter's walk (`Meter::typed`, `Meter::range` and `Measure::stretch`, callgrind's self counts).
- **The paired cell is level**: the engine's overhead over its bare loop is 18.0 % [16.2–22.1] on
  the base and 20.1 % [18.1–21.5] on `main`.

#### A pipeline of both stream kinds

640 batches of 8 000 rows, 320 from one partition of each of two streams, committed every
sixteenth of the rows, into the IPC sink. The base logs the second stream, which its source cannot
read again, into a write-ahead log on the disk beside the build or on tmpfs; `main` tells both
streams their cursors once the commit holding their rows lands. Five rounds.

| Cell | Time a run, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|
| `logged/disk/...` → `mixed/mixed/...` | 327.53 ms [319.10–341.52] | 24.92 ms [24.48–26.12] | 0.08 | faster |
| `logged/tmpfs/...` → `mixed/mixed/...` | 158.90 ms [155.17–162.31] | 24.92 ms [24.48–26.12] | 0.16 | faster |
| `logged/unlogged/...` → `mixed/replayable/...` | 27.11 ms [26.19–27.75] | 24.84 ms [23.83–25.28] | 0.92 | faster |

- **The pipeline with a stream that cannot be read again runs 13.1 times as fast as the base's on
  disk** and 6.4 times as fast as on tmpfs: the base logged 25.9 MiB a commit over 17 commits, its
  log phase taking 15.2 ms a commit on disk and 3.5 ms on tmpfs (its first round's readings), and
  `main` writes every row once, into the destination ([passthrough.md](passthrough.md), "A
  pipeline of both stream kinds"; [sqlite.md](sqlite.md), "A source that cannot read again"). On
  `main` it costs what the both-readable pipeline does, 24.92 against 24.84 ms.

#### JSON through the engine

Sixteen pushes of about 8 MiB of the `nested` and `wide_200` corpora, a checkpoint after each, from
one partition or sixteen, into the sink that drops them; on the base through the cell's port.
Five rounds.

| Cell | Time a run, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|
| `json/nested/1p` | 124.12 ms [122.06–128.40] | 119.82 ms [116.79–124.70] | 0.97 | level |
| `json/nested/16p` | 93.12 ms [89.78–96.75] | 90.57 ms [87.30–92.37] | 0.97 | level |
| `json/wide_200/1p` | 155.56 ms [151.66–158.53] | 145.92 ms [142.00–153.60] | 0.94 | level |
| `json/wide_200/16p` | 103.53 ms [103.13–106.26] | 99.11 ms [97.78–101.24] | 0.96 | faster |

- Level but for `wide_200` from sixteen partitions, 0.96 times the base's time. Sizing the
  shredder's jobs by rows, cores and a limit, and one tokenizer, were measured and closed
  ([shred.md](shred.md), "Jobs sized by rows, cores and a limit" and "One tokenizer"); the
  shredder's counts are within 2.13 % of the base's (above).

#### Wide tables

Eight pushes of about 8 MiB of rows of 1 000 or 5 000 columns, as Arrow or as JSON, into a sink
that drops them, within 1 GiB. Three rounds.

| Cell | Time a run, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|
| `wide/arrow/1000` | 16.03 ms [15.91–16.16] | 12.96 ms [12.79–13.23] | 0.81 | faster |
| `wide/arrow/5000` | 78.48 ms [76.33–80.31] | 62.19 ms [60.81–64.39] | 0.79 | faster |
| `wide/json/1000` | 177.61 ms [174.93–177.61] | 159.85 ms [159.13–165.69] | 0.90 | faster |
| `wide/json/5000` | 807.72 ms [802.38–826.12] | 700.30 ms [680.87–721.02] | 0.87 | faster |

- Arrow loads 1.24 and 1.26 times the base's rows a second, JSON 1.11 and 1.15, and
  `wide/arrow_1000` takes 23.44 % fewer instructions and 15.64 % fewer allocations. The schema's
  work done once a schema, its table schema shared and its plan found by pointer, a fitting batch
  measured once, accounts for 8.4 % and 9.4 % less time on the Arrow cells and 2.5 % and 1.5 % on
  the JSON ones against the tree before it ([wide.md](wide.md), "Schema-shaped work done once a
  schema"). The rest of the move has no record of its own; lowering a piece without building its
  batch's fields again (`ec181fa5`) is on the same path.

#### Normalized writes

The first 100 000 rows of the `orders` corpus, keyless nested JSON, normalized into three tables
(550 000 rows), in pushes of 2 000, 10 000 or 50 000 rows, within 1 GiB. Three rounds.

| Cell | Time a run, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|
| `normalized/keyless/2000` | 158.44 ms [154.58–163.78] | 66.74 ms [66.09–69.31] | 0.42 | faster |
| `normalized/keyless/10000` | 121.20 ms [118.41–123.59] | 36.55 ms [36.37–37.56] | 0.30 | faster |
| `normalized/keyless/50000` | 103.03 ms [101.90–106.71] | 24.87 ms [24.64–25.45] | 0.24 | faster |

- **2.4 to 4.1 times the base's rows a second**, end to end. A keyless row carries its position
  and lowering builds its id, where the base hashed every root and item with BLAKE3: 60 % less
  time a run at 50 000 rows on one core against the tree before it ([normalized.md](normalized.md),
  "Ids by role"). A normalized stream keeps its parts' schemas for batches of one schema, about 9 %
  fewer allocations a batch ([wide.md](wide.md), "Schema-shaped work done once a schema").

#### Shredding

The shred bench's groups, one core but `shred_cores` and `fast_path`'s Arrow decoder beside the
shredder. Five rounds.

| Cell | Time a run, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|
| `shred/nested` | 52.75 ms [50.13–53.34] | 50.73 ms [48.86–54.68] | 0.96 | level |
| `shred/sparse` | 51.18 ms [51.01–53.16] | 50.44 ms [49.91–55.17] | 0.99 | level |
| `shred/flat_narrow` | 64.95 ms [63.20–66.14] | 63.99 ms [61.50–68.56] | 0.99 | level |
| `shred/wide_200` | 65.93 ms [63.91–67.41] | 64.90 ms [63.34–69.15] | 0.98 | level |
| `shred/string_heavy` | 54.61 ms [51.49–56.15] | 55.46 ms [53.86–57.46] | 1.02 | level |
| `shred_cliffs/array_form` | 82.12 ms [79.28–85.45] | 80.50 ms [78.02–85.05] | 0.98 | level |
| `shred_cliffs/drift_json` | 157.09 ms [143.87–161.22] | 149.68 ms [140.38–156.80] | 0.95 | level |
| `shred_cliffs/drift_scalar` | 55.51 ms [52.80–58.34] | 54.84 ms [52.67–57.08] | 0.99 | level |
| `shred_cliffs/int_arrays` | 491.52 ms [462.85–502.12] | 483.12 ms [461.45–499.31] | 0.98 | level |
| `shred_cliffs/big_numbers` | 122.16 ms [117.66–126.92] | 125.57 ms [115.76–127.69] | 1.03 | level |
| `shred_cliffs/pretty` | 79.58 ms [78.52–83.60] | 81.21 ms [78.57–84.54] | 1.02 | level |
| `shred_cores/1` | 50.67 ms [50.36–53.15] | 50.67 ms [49.78–52.68] | 1.00 | level |
| `shred_cores/2` | 28.73 ms [28.18–29.89] | 28.54 ms [27.93–29.03] | 0.99 | level |
| `shred_cores/4` | 17.73 ms [17.39–18.81] | 17.18 ms [16.93–17.40] | 0.97 | level |
| `fast_path/shredder/flat_narrow` | 67.17 ms [66.64–73.16] | 69.29 ms [65.74–74.54] | 1.03 | level |
| `fast_path/arrow_json/flat_narrow` | 76.48 ms [74.67–85.62] | 81.09 ms [76.35–84.43] | 1.06 | level |
| `fast_path/shredder/wide_200` | 65.36 ms [65.07–73.53] | 68.83 ms [65.64–70.02] | 1.05 | level |
| `fast_path/arrow_json/wide_200` | 118.80 ms [117.35–131.49] | 124.05 ms [117.33–127.00] | 1.04 | level |
| `normalize/shred_only` | 61.89 ms [61.05–69.32] | 63.37 ms [62.54–65.51] | 1.02 | level |
| `normalize/keyed` | 103.78 ms [102.97–117.30] | 77.98 ms [75.71–79.42] | 0.75 | faster |
| `normalize/keyless` | 163.84 ms [160.91–167.50] | 67.56 ms [65.28–68.66] | 0.41 | faster |
| `json_check/plain` | 22.61 ms [22.45–23.68] | 22.69 ms [22.25–23.27] | 1.00 | level |
| `json_check/escaped` | 19.81 ms [19.53–20.69] | 20.39 ms [19.62–20.84] | 1.03 | level |

- **Shredding is level**, every cell's ranges overlapping, as its counts say: 0.01 % to 2.13 %
  fewer instructions and the same allocations.
- **Normalizing is faster inside the cell**, keyed 0.75 and keyless 0.41 times the base's time:
  ids by role took them from 113.5 to 82.0 ms and from 175.3 to 70.0 ms, the medians of its three
  rounds against the tree before it ([shred.md](shred.md), "Normalizing"). Both cells ask for less
  on `main`, their children's and keyless rows' ids built later, in lowering (above), so neither is
  a like-for-like figure.

#### Lowering

One batch of 65 536 rows prepared on one thread by a plan made once. Three holds, each case on
the base, `main`, `main` and the base in turn: six figures a tree.

| Cell | Time a batch, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|
| `append_native` | 1.48 µs [1.47–1.56] | 1.36 µs [1.33–1.43] | 0.92 | faster |
| `append_text` | 20.56 ms [20.11–21.82] | 20.27 ms [19.71–21.29] | 0.99 | level |
| `merge_unique` | 539.42 µs [506.14–575.49] | 530.99 µs [529.08–575.31] | 0.98 | level |
| `merge_duplicates` | 5.82 ms [5.56–6.46] | 5.79 ms [5.64–6.66] | 1.00 | level |
| `history` | 144.05 ms [139.40–159.96] | 144.05 ms [140.22–160.23] | 1.00 | level |
| `changes` | 1.88 ms [1.78–2.10] | 1.86 ms [1.83–1.99] | 0.99 | level |
| `split_json` | 4.61 ms [4.52–5.03] | 4.57 ms [4.53–5.18] | 0.99 | level |
| `one_bad_value` | 116.03 µs [113.96–117.61] | 116.38 µs [114.33–119.35] | 1.00 | level |
| `one_dropped_row` | 2.28 ms [2.25–2.35] | 487.28 µs [485.10–518.47] | 0.21 | faster |
| `temporal_widening` | 50.06 µs [48.80–50.87] | 50.94 µs [50.02–51.68] | 1.02 | level |

- **A row dropped by the schema policy costs a fifth of the base's time** with the same
  instructions (+0.22 % under callgrind): over ten seconds of the cell under `perf stat`, the base
  took 6 226 544 page faults and `main` 4 601. The change on this path, `424ea870` ("let a
  filtered piece's rows go as their copy is made"), has the steps that drop rows copy a column at
  a time and let the column they copied from go, where the base held every column through the
  copy; no record timed it.
- **Merges, history and change streams allocate 28 % to 92 % less a batch** (the counts above), at
  the same instructions but for change streams' 5.81 % fewer: `main` lowers a lone part as it is
  and builds a merge's and a history table's fields once (`ec181fa5`, "lower a piece without
  building its batch's fields again"); no record timed it.

#### SQLite destination

The reference SQLite destination: a million rows appended in one commit or one every 10 000, and
a million merged into a table of a million, on btrfs with `synchronous=FULL`. Three rounds.

| Cell | Time a run, base | `main` | Main ÷ base | Verdict |
|---|---|---|---|---|
| `append/1000000/once` | 2.833 s [2.773–2.838] | 2.616 s [2.483–3.165] | 0.92 | level |
| `append/200000/10000` | 565.11 ms [556.05–568.89] | 522.24 ms [508.31–602.02] | 0.92 | level |
| `append/500000/10000` | 1.377 s [1.367–1.387] | 1.308 s [1.282–1.470] | 0.95 | level |
| `append/1000000/10000` | 2.809 s [2.763–2.810] | 2.588 s [2.575–3.029] | 0.92 | level |
| `merge/1000000` | 13.802 s [13.016–16.871] | 12.910 s [11.613–13.432] | 0.94 | level |

- Level under the rule: `main`'s medians are 0.92 to 0.95 times the base's. On the appends two of
  its three rounds ran below every round of the base and its third above them, the hold that began
  at the highest five-minute load of the three (2.15); the merge's ranges overlap.

## Only one tree

Cells and capabilities `main` has and the base does not, each in its own record, reported here
as new rather than as a ratio:

- **Column exclusion.** A stream excludes columns it never loads (`StreamPlan::exclude`, ADR
  0005), the read's request carrying them as a projection: `wide/arrow/5000_keep_100` loads a
  table of 5 000 columns kept to 100 in 7.3 times less time than discarding their values did on
  the `main` before it, with 13 times fewer allocations ([wide.md](wide.md), "A table kept by
  excluding columns").
- **Live status.** `RunHandle::status()` reports what a run is in the middle of and since when,
  each partition and lane, and each stream's lag, without waiting (ADR 0004).
- **A commit retried in place.** A commit the destination fails transiently is sent again by the
  same key within the attempt, where the base ended the attempt ([latency.md](latency.md),
  "Transient commits"); a refused acknowledgement is owed to the next commit rather than failing a
  landed one ([latency.md](latency.md), "Refused reports").
- **Latency.** `just bench latency`, push to landed and push to acknowledged, in process and with
  both connectors spawned ([latency.md](latency.md)).
- **Many tables.** 16 to 256 followed streams and a snapshot of 256 tables, in process and served,
  their reads keeping what they need of a shared part of the budget
  ([passthrough.md](passthrough.md) and [served.md](served.md), "Many tables").
- **More cells.** The production slice through a writer a core and within 1 GiB
  ([served.md](served.md), "Through a writer a core"); sparse wide tables and a 100-column one
  ([wide.md](wide.md)); normalized loads from 4 and 16 partitions and a merge of hot keys
  ([normalized.md](normalized.md)); checkpoints every 6 and 97 pushes
  ([passthrough.md](passthrough.md)); and an append from a source that cannot read again into
  SQLite ([sqlite.md](sqlite.md)).

The base alone has its write-ahead log's bench, 13 cells, and its two counted cases (above): the
log is gone from `main`, every stream told its cursor once its commit lands.

## Summary

**By the counts**, which load does not move:

- **Leaner.** `main` executes 39.54 % fewer instructions shredding rows a source pushes through
  `Emitter::rows`, now pushed as lines ([shred.md](shred.md), "Rows pushed as lines"); 23.44 %
  fewer, with 15.64 % fewer allocations, loading a table of 1 000 Arrow columns, partly the
  schema's work done once a schema ([wide.md](wide.md)); 13.46 % fewer and 17.12 % fewer
  allocations moving the passthrough workload, 44 % of the difference the cost meter's walk of
  each batch; 4.85 % fewer and 16.24 % fewer allocations committing 1 024 partitions' seals, where
  no record isolates which of the commit's changes gives it; 0.01 % to 2.13 % fewer shredding each
  corpus; and 28 % to 92 % fewer allocations lowering merges, history and change streams.
  Normalizing takes 68 % to 99.95 % fewer inside its cases, most of it work that lowering now does
  from positions; the end-to-end keyless loads, timed below, hold the whole of it.
- **Level.** The wire codec (`ipc/roundtrip`, +0.03 % instructions), the JSON check, and lowering's
  instructions in every case, within +0.54 %.
- **Costlier.** No case by more than 0.54 % instructions or 1.35 % allocations.

**In memory:** in process, a run's growth is level for the passthrough workload and lower for the
normalized one in five lines of six; the served host grows more at two and four cores without the
tunables, 181 to 240 and 186 to 257 MiB at the medians, within limits of 522 and 526 MiB; and
the engine reserves more of the same budget for the same work, 86 to 106 MiB and 144 to 163.

**In time**, direction only, on a shared machine:

- **Faster.** A pipeline holding a stream that cannot be read again, 13.1 times on disk, the
  write-ahead log gone; keyless normalized loads, 2.4 to 4.1 times, ids by position; wide tables,
  1.11 to 1.26 times; small batches in process, 1.06 to 1.26 times; a row dropped by the schema
  policy in lowering, 4.7 times, its page faults 6.2 million to 4 601 in ten seconds.
- **Level.** Shredding; large batches in process and the paired cell; the SQLite destination;
  JSON through the engine but one cell; the production slice's frames of 80 000 rows, in MB/s and
  CPU seconds a GB, of 10 000 rows from four partitions and of 1 000 rows from one and sixteen.
- **Slower.** The production slice in frames of 10 000 rows from one and from sixteen partitions,
  0.86 and 0.94 times the base's MB/s, the host taking 1.07 to 1.16 times its CPU a GB; and in
  frames of 1 000 rows from four partitions, 0.94 times. No record measured these cells across
  the changes, and a profile did not locate the host's added work; which change costs it is not
  resolved here.
