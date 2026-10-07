# Lowering

`LoweringPlan::prepare` turns a pushed batch into rows of its table: exact conversions, the
destination's storage types, metadata columns, and a merge's compaction, a history table's
hashes or a change stream's flags. This record keeps its cost a batch in each mode, the
allocations it makes, and the heap-peak check that holds each mode to the charge the write path
reserves for it.

## Method

- **Cases.** One batch of 65 536 rows each, prepared on the calling thread by a plan made once,
  as a partition reuses its plan (`rdlt_engine::bench::Lowering`, built from the cases the heap
  tests share):
  - `append_native`: the ten mixed columns of the passthrough bench, each stored as it arrives.
  - `append_text`: the same and a UUID column, into a destination storing every column as text.
  - `merge_unique`, `merge_duplicates`: a merge by `id`, keys unique, or each held by two rows.
  - `history`: a history table of 50 columns whose versions begin at the instants of `at`.
  - `changes`: a change stream merged by key, 200 columns, each update flagging one column
    unchanged; each run splits the change batch and prepares it, as the write path does.
  - `split_json`: a column of JSON whose integers its own column reads and the rest its JSON
    variant holds (a string every tenth value, an object every hundredth).
  - `one_bad_value`: instants in seconds into a column of nanoseconds, the middle one beyond it,
    which the stream's `DiscardValue` policy nulls.
  - `temporal_widening`: instants in seconds into a column of microseconds, a tenth null.
- **Checks.** Every run asserts the rows the case keeps (half for `merge_duplicates`) and the
  values it discards (one for `one_bad_value`); the cases' unit tests pin what each prepares.
- **Throughput.** Rows a second of the pushed batch.
- **Allocations.** The `allocations` root prepares each case once to make its plan, then counts
  a second prepare.
- **Running.** `just bench lowering 0-3`, as every record is taken: the four performance cores,
  the release profile's fat LTO and one codegen unit, five rounds, each started at a one-minute
  load average under 1.0 while the machine's other agents held off under the measurement lock.
  `prepare` runs on one thread, so the cases' sizes are fixed rather than taken from the host.

## Charge

`cost/tests/prepared.rs` prepares every case, stored natively and as text, 2 048 rows, and fails
where the heap's peak passes what the write path reserves for the batch (`Rendering::lowering`
over the plan's stored types and row bytes, and a change row's bytes for a change batch) by more
than 8 KiB. Every mode is within its charge: the highest peaks are a merge with duplicates into
text, 0.86 of its charge, and a merge of unique keys into text, 0.71; history and changes into text
reach 0.59 and 0.55 (measured at 2 048, 16 384 and 65 536 rows alike). No cost term is missing.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-07, commit `28726685db15`, governor `powersave`
with energy preference `performance`, one-minute load average 0.89–1.07 and five-minute 1.34–2.55
before the runs; criterion's estimate, the median of five rounds and their range, and how many
rounds fall within ±3 % of the median.

| Case | Time a batch | Rounds within ±3 % | Rows a second | Instructions a cycle | A row | A batch |
|---|---|---|---|---|---|---|
| `append_native` | 1.49 µs (1.41–1.57 µs) | 3 of 5 | 44.0 G | 1.70 | 0 allocations, 0 bytes | 9 allocations, 928 bytes |
| `append_text` | 26.54 ms (25.52–27.75 ms) | 3 of 5 | 2.47 M | 5.82 | 2.001 allocations, 338 bytes | 131 162 allocations, 22.2 MB |
| `merge_unique` | 7.86 ms (7.68–8.56 ms) | 4 of 5 | 8.34 M | 3.92 | 0.167 allocations, 79 bytes | 10 971 allocations, 5.18 MB |
| `merge_duplicates` | 6.80 ms (6.61–7.01 ms) | 4 of 5 | 9.64 M | 3.56 | 0.085 allocations, 115 bytes | 5 560 allocations, 7.51 MB |
| `history` | 127.29 ms (125.22–128.53 ms) | 5 of 5 | 515 k | 3.99 | 14.006 allocations, 619 bytes | 917 882 allocations, 40.6 MB |
| `changes` | 18.44 ms (17.87–19.71 ms) | 2 of 5 | 3.55 M | 4.72 | 2.007 allocations, 92 bytes | 131 557 allocations, 6.01 MB |
| `split_json` | 20.29 ms (19.71–21.10 ms) | 3 of 5 | 3.23 M | 3.84 | 11.170 allocations, 1 793 bytes | 732 066 allocations, 117 MB |
| `one_bad_value` | 12.46 ms (11.90–15.53 ms) | 2 of 5 | 5.26 M | 2.30 | 4.001 allocations, 306 bytes | 262 178 allocations, 20.0 MB |
| `temporal_widening` | 657 µs (607–714 µs) | 2 of 5 | 99.7 M | 5.84 | 0 allocations, 8 bytes | 12 allocations, 525 kB |

Allocation counts are the same in every round. One CPU is busy throughout every case.

- A native append allocates nothing a row: its columns are shared, and the load's constant
  columns sliced.
- A text append makes two allocations a row; the same columns widened natively make none.
- `one_bad_value` makes four allocations a row where `temporal_widening`, which widens seconds
  with no value beyond their column, makes none.
- `history` and `split_json` allocate most: 14 and 11 allocations a row.

The wall times of the cases under 30 ms fall outside ±3 % in one or more of five rounds on this
shared machine, as the counts above say; the review's protocol compares changes in interleaved
pairs, which cancels that drift, and the allocation counts and instructions are exact.
