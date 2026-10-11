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
  - `one_dropped_row`: the same instants, the middle one's row dropped by the stream's
    `DiscardRow` policy, into a table holding the 32-bit `n` in 64 bits.
  - `temporal_widening`: instants in seconds into a column of microseconds, a tenth null.
- **Checks.** Every run asserts the rows the case keeps (half for `merge_duplicates`, all but
  one for `one_dropped_row`) and the values it discards (one for `one_bad_value`); the cases'
  unit tests pin what each prepares.
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

Intel Core Ultra X7 358H, mains power, 2026-10-11, commit `57d5f5066dc8`, governor `powersave`
with energy preference `performance`, one-minute load average 0.77–0.99 and five-minute 1.16–1.78
before the runs; criterion's estimate, the median of five rounds and their range, and how many
rounds fall within ±3 % of the median.

| Case | Time a batch | Rounds within ±3 % | Rows a second | Instructions a cycle | A row | A batch |
|---|---|---|---|---|---|---|
| `append_native` | 1.49 µs (1.45–1.50 µs) | 5 of 5 | 44.0 G | 1.69 | 0 allocations, 0 bytes | 9 allocations, 928 bytes |
| `append_text` | 25.19 ms (24.45–25.79 ms) | 5 of 5 | 2.60 M | 5.70 | 2.001 allocations, 338 bytes | 131 162 allocations, 22.2 MB |
| `merge_unique` | 7.88 ms (7.71–8.15 ms) | 4 of 5 | 8.32 M | 3.93 | 0.167 allocations, 79 bytes | 10 971 allocations, 5.18 MB |
| `merge_duplicates` | 6.71 ms (6.52–7.11 ms) | 4 of 5 | 9.77 M | 3.52 | 0.085 allocations, 115 bytes | 5 560 allocations, 7.51 MB |
| `history` | 138.78 ms (137.19–142.35 ms) | 5 of 5 | 472 k | 3.59 | 0.006 allocations, 75 bytes | 373 allocations, 4.93 MB |
| `changes` | 1.93 ms (1.91–2.02 ms) | 4 of 5 | 33.9 M | 5.14 | 0.007 allocations, 43 bytes | 448 allocations, 2.85 MB |
| `split_json` | 20.24 ms (19.75–21.36 ms) | 4 of 5 | 3.24 M | 3.60 | 11.170 allocations, 1 794 bytes | 732 066 allocations, 118 MB |
| `one_bad_value` | 110 µs (110–118 µs) | 3 of 5 | 596 M | 3.48 | 0.005 allocations, 33 bytes | 311 allocations, 2.13 MB |
| `one_dropped_row` | 523 µs (523–560 µs) | 4 of 5 | 125 M | 0.62 | 0.005 allocations, 135 bytes | 344 allocations, 8.83 MB |
| `temporal_widening` | 48.6 µs (48.1–52.4 µs) | 4 of 5 | 1.35 G | 3.16 | 0 allocations, 8 bytes | 19 allocations, 526 kB |

Allocation counts are the same in every round. One CPU is busy throughout every case.

- A native append allocates nothing a row: its columns are shared, and the load's constant
  columns sliced.
- A text append makes two allocations a row; the same columns widened natively make none.
- The temporal cases allocate a batch, not a row: `one_bad_value` nulls its one value in 311
  allocations, `one_dropped_row` drops its one row in 344, and `temporal_widening` widens its
  seconds in 19.
- Dropping a row costs about five times nulling a value, 523 µs against 110 µs, and allocates
  four times the bytes, 8.83 MB against 2.13 MB.
- `split_json` allocates most, 11 allocations a row; every other case but a text append makes
  under one allocation in five rows.

The wall times of the cases under 30 ms fall outside ±3 % in one or more of five rounds on this
shared machine, as the counts above say; the review's protocol compares changes in interleaved
pairs, which cancels that drift, and the allocation counts and instructions are exact.

## History columns

A history table's lowering adds four columns to each batch: when each version begins, its end,
whether it is current, and the hash of its data, null for a row that deletes. The hashes are
computed into one column and a delete's is nulled by a null buffer over them, so a deleted row
keeps its 32 bytes in the column's values; a row is charged 72 bytes in a metadata column of
bytes (`META_BYTES`), which holds them.

- **Signal.** User-space instructions (`perf stat -e cpu_core/instructions/u`) of one
  `history_columns` call over 65 536 rows of one `id` column, every other row deleting: a test's
  count with 20 calls less its count with none, over 20. Beside it, the heap's peak during a call
  over 4 096 such rows and what its columns hold after it, as `peak_alloc` counts them.
- **Probe.** A test appended to `table/lowering/history/tests.rs` for the run and never
  committed:

  ```rust
  #[test]
  fn history_probe() {
      let iterations: usize = std::env::var("ITER").map_or(0, |n| n.parse().unwrap());
      let heap = &crate::cost::tests::HEAP;
      let small = batch(vec![("id", Arc::new(Int64Array::from_iter_values(0..4_096)) as ArrayRef)]);
      heap.reset_peak_usage();
      let before = heap.current_usage();
      let columns =
          history_columns(&stream(), &small, arrived(UNIX_EPOCH), &|row| row % 2 == 1).unwrap();
      let (peak, held) = (heap.peak_usage() - before, heap.current_usage() - before);
      println!("PROBE history rows 4096 peak {peak} held {held}");
      drop(columns);
      let data = batch(vec![("id", Arc::new(Int64Array::from_iter_values(0..65_536)) as ArrayRef)]);
      for _ in 0..iterations {
          std::hint::black_box(
              history_columns(&stream(), &data, arrived(UNIX_EPOCH), &|row| row % 2 == 1)
                  .unwrap(),
          );
      }
  }
  ```

- **Build and run.** `cargo test --release` of rdlt-engine's library tests, the binary run on
  performance core 2 (`taskset -c 2`), three rounds, the first started at a one-minute load
  average of 0.84.

Intel Core Ultra X7 358H, 2026-10-09, commit `b149181b8695`, Rust 1.98.1, governor `powersave`;
the median of three rounds, which differ by under 0.001 %.

| Measurement | Figure |
|---|---|
| Instructions a call, 65 536 rows | 94 410 507 |
| Heap peak, 4 096 rows | 412 562 bytes |
| Held by the columns after the call, 4 096 rows | 215 496 bytes |
