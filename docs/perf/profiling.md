# Profiling

How to see where the engine spends cycles, instructions, memory and system calls, on Linux, with
the benches in `crates/rdlt-engine/benches`. Recorded figures come from `just bench` and live in
the other records here, such as [shred.md](shred.md) and [passthrough.md](passthrough.md); this
page holds the recipes.

## Cycles

`just profiling <bench>` builds a bench with the release profile's code, line tables and frame
pointers, in `target/frame-pointers` so its flags never rebuild the shared target directory, and
prints the binary's path last:

```sh
bin=$(just profiling shred 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1)
perf record -o shred.perf -e cycles:u --call-graph fp -F 999 -- \
    taskset -c 0 "$bin" --bench '^shred/nested$' --profile-time 10
perf report -i shred.perf --no-children --sort srcfile -g none --stdio
```

- `cycles:u` counts user-mode cycles, which `perf_event_paranoid` 2 allows without root. On a CPU
  that mixes core types, name the type: `cpu_core/cycles/u` on the performance cores.
- `--profile-time` runs the matching benchmarks for that many seconds without criterion's
  analysis.
- Frame pointers lose the caller of a libc function such as `memcpy` or `malloc`, which libc is
  built without. Where stacks pass through libc, record with `--call-graph dwarf,65000` instead,
  for a few seconds: each sample copies 65 000 bytes of stack.
- `perf script --inline` prints each sample's stack, inlined functions included, one sample to a
  paragraph. To classify samples by stack, count the paragraphs that hold a frame:

```sh
perf script --inline -i shred.perf |
    awk -v RS= '{ all++ } /shred::meter/ { hit++ } END { printf "%.1f %% of samples\n", 100 * hit / all }'
```

## Instructions

`just bench` prints `perf stat`'s instructions a cycle and CPUs busy for the benchmarks it runs.
For the instructions of one pass of a function, run the benchmark once, in criterion's test mode,
under callgrind, and read the function's inclusive count, which leaves out the corpus generation
and criterion's own threads:

```sh
valgrind --tool=callgrind --callgrind-out-file=nested.cg "$bin" --test '^shred/nested$'
callgrind_annotate --inclusive=yes nested.cg | grep ':rdlt_engine::bench::shred '
```

`just instructions` counts each hot path's instructions and allocations under callgrind, on
this tree and on `main` built in the same job: each case at one and at three iterations, its
count the difference over two, so what a run does once cancels out; the passthrough case runs on
one runtime worker and one compute thread. [instructions.md](instructions.md) holds the cases.

## Allocations

`just bench` prints each workload's allocations, reallocations and bytes a row and a batch, from
the `allocations` bench's counting allocator. For where they happen and the heap's peak, use
heaptrack, once on one pass and once on none:

```sh
heaptrack --record-only -o keyless-one "$bin" --test '^normalize/keyless$'
heaptrack --record-only -o keyless-none "$bin" --test '^nothing$'
heaptrack_print -f keyless-one.zst | grep -E '^(calls to allocation functions|peak heap memory consumption)'
heaptrack_print -f keyless-none.zst | grep -E '^(calls to allocation functions|peak heap memory consumption)'
```

- Without `--record-only`, heaptrack opens its viewer when the run ends and waits for it.
- Both runs build every corpus; the difference between them is one pass of the benchmark.

DHAT gives each allocation site's bytes and lifetimes:

```sh
valgrind --tool=dhat --dhat-out-file=keyless.dhat "$bin" --test '^normalize/keyless$'
```

DHAT keeps the main thread's stacks; stacks from scoped threads come back empty. The one-core
groups (`shred/*`, `normalize/*`) run on the main thread. Open the file in valgrind's
`dh_view.html`.

## Scheduling

`perf stat` counts no context switches or migrations at `perf_event_paranoid` 2. The scheduler
counts them per thread in `/proc/<pid>/task/*/sched`; read them as the process ends:

```sh
bin=$(just profiling passthrough 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1)
taskset -c 0-3 "$bin" --bench '^passthrough/' --profile-time 10 > /dev/null &
pid=$!
while kill -0 "$pid" 2> /dev/null; do
    now=$(grep -H -E '^(nr_voluntary_switches|nr_involuntary_switches|se.nr_migrations) ' \
        /proc/"$pid"/task/*/sched 2> /dev/null) && last=$now
    sleep 1
done
printf '%s\n' "$last"
```

Each line names its thread's id; `/proc/<pid>/task/<id>/comm` names the thread (`tokio-runtime-w`
for a runtime worker, `rdlt-compute-<n>` for a compute thread).

## System calls

`just bench` counts each benchmark's reads, writes, syncs and opens once, under a count-only
`strace -c` in criterion's test mode. For every call, or for a run of your own:

```sh
strace -f -c -o syscalls.txt -- "$bin" --test '^passthrough/'
strace -f -c -o setup.txt -- "$bin" --test '^nothing$'
```

`-c` counts calls. strace slows a run that makes many calls about tenfold, so it counts and never
times; subtract the run matching no benchmark, as above.

## Files a bench reads

On btrfs, `cp` makes a reflink: the copy shares the original's extents, and its first read may
go to disk. Read a copied file once before timing a bench that reads it, such as the corpus
`RDLT_SHRED_CORPUS` names: `cat "$RDLT_SHRED_CORPUS" > /dev/null`.
