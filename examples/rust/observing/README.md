# Observing a run, in Rust

A bulk load watched as it runs: each of its events as it happens, its report and metrics once it
has ended, and a clean stop, asked at Ctrl-C or from the program's own code, that ends the run
after a commit. [`src/main.rs`](src/main.rs) is the whole of it.

It loads the quick start's source at a larger size, `LogSource`'s seeded offset logs, four
partitions of 250,000 messages, into SQLite, read incrementally and committed every 50,000 rows.

- **Events:** `run.events()` subscribes to the run's events, each read in order as the run makes
  it, an `Event` whose `kind` says what happened: the attempt started, a table's schema changed, a
  commit landed with its rows and the time it took, a failure is retried, the run ended. A
  subscriber that falls behind is told what it missed as a `Lagged`, and slows neither the run nor
  another subscriber.
- **Its report:** the `Outcome` a run ends with holds its `Report`: its status, its rows, bytes and
  commits, which come from the destination's receipts, its attempts and a report per stream.
- **Its metrics** go through the `metrics` crate to whichever recorder the program installs.
  Here that is the Prometheus exporter's, with no HTTP listener, and the program prints the
  counters it renders, as a scrape reads them; a service would serve them, or export them over
  OTLP with the facade's `otel` feature.
- **A clean stop:** `RunControl::stop(StopMode::AfterCommit)` asks the run to stop reading,
  commit what it read and end, `Stopped`, and the next run reads on from there. Ctrl-C asks for it,
  as the command line does, and a second Ctrl-C asks for `StopMode::Now`, which ends the run at
  once, cancelled, its staging discarded at the next open. `rdlt::Interrupts` hears Ctrl-C, and
  the termination, hang-up and quit signals, from before the run starts, so the process stops the
  run rather than ending mid-load. Besides, a host that spawns connectors needs it to stop them,
  since a terminal's Ctrl-C does not reach their process groups.

## Running it

From `examples/rust`, naming the database, in a directory only you may write, as the SQLite
destination requires, and a count of commits after which the program asks for the stop itself:

```console
$ cd examples/rust
$ mkdir -p /tmp/observing-rust
$ cargo run -q -p observing -- /tmp/observing-rust/observing.db 3
attempt 1 started
table events at schema version 1
commit 1: 50000 rows in 315 ms
commit 2: 50000 rows in 308 ms
commit 3: 50000 rows in 226 ms
stopping after the next commit
commit 4: 40000 rows in 143 ms
run ended: Stopped
Stopped: committed 190000 rows in 4 commits
rdlt_bytes_committed_total{pipeline="observing",stream="events"} 7037410
rdlt_commits_total{pipeline="observing"} 4
rdlt_rows_committed_total{pipeline="observing",stream="events"} 190000
rdlt_rows_discarded_total{pipeline="observing",stream="events"} 0
rdlt_schema_changes_total{pipeline="observing"} 1
rdlt_values_discarded_total{pipeline="observing",stream="events"} 0
```

Your counts and times will differ: the commit in flight when the stop is asked lands, and then one
of what was read since, where anything was, so how many commits a run makes depends on how far it
read before the stop took hold. The commits' rows add up to the report's, the counters' and the
table's, which holds each message once.

Without a count it loads the rest, reading on from where this run stopped, or stops at Ctrl-C:

```sh
cargo run -q -p observing -- /tmp/observing-rust/observing.db
```

Once the logs are read to their heads, a run commits no row. The program exits as the command
line does:

- with a signal's status where one came, however the run ended: 130 for Ctrl-C, whether the run
  stopped after its commit or a second Ctrl-C cancelled it, 143 for a termination, 129 for a
  hang-up and 131 for a quit;
- 0 where the run ended of itself or stopped after the count;
- 1, printing the error, where the pipeline cannot be built or its run fails.

The Python program exits 0 after its first Ctrl-C instead: the package's `run.wait()` answers the
stopped run's outcome and keeps no sign of the signal.

## Tests

[`tests/it/main.rs`](tests/it/main.rs), from `examples/rust` with `cargo nextest run -p observing`:

- **Told as the README shows:** the program run as the console block runs it prints the block's
  lines, digits and all, but the commits', the report's and the counters of what landed. Each
  commit line, printed or shown, has its form and comes in turn: three before the stop note, and
  any others between it and the run's end. In the block as in the output, the commits' rows add
  up to the report's and the counter's, and their count to the commits both tell; the table holds
  the output's, each message once.
- **Read on:** a run stopped after three commits, then one with no count, land all 1,000,000
  messages once, and a third run commits no row.
- **Ctrl-C:** interrupted once its first commit has landed, the program says it stops after the
  next commit, its run ends `Stopped` with what it committed in the table, each message once, and
  the program exits 130. No test sends a second Ctrl-C, which would race the run's end; one stops
  the run at once, and it ends `Cancelled` and exits 130.

Next: the same run observed from Python is [`python/observing`](../../python/observing); the
repository README's [Run control and reporting](../../../README.md#run-control-and-reporting) says
what else a run's status, report and metrics hold.
