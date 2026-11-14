# Observing a run, in Python

The [Rust observing example](../../rust/observing)'s run, watched from Python: each of a bulk
load's events as it happens, its report once it has ended, and a clean stop, asked at Ctrl-C or
from the program's own code, that ends the run after a commit. [`main.py`](main.py) is the whole
of it.

It loads a stream function's events, a thousand pages of a thousand, each page followed by its
checkpoint, into SQLite through the reference SQLite connector, read incrementally and committed
every 50,000 rows.

- **Events:** `run.events()` subscribes to the run's events, read in order as the run makes them,
  here on a thread of their own. Each is an `rdlt.Event`, its `kind` the Rust event's name, such
  as `committed`, and its `detail` that event's fields as the facade writes them. A subscriber that
  falls behind is told what it missed as an `rdlt.Lagged`.
- **Its report:** `run.wait()` answers the run's `Outcome`, whose `Report` holds its status, its
  rows, bytes and commits, its attempts and a report per stream.
- **No metrics:** the package binds no recorder of the run's metrics, which are the Rust facade's,
  through the `metrics` crate, as the Rust example shows. The counters' totals are the events' and
  the report's here, and `run.status()` reads, at any moment, the status the gauges are sampled
  from.
- **A clean stop:** `run.stop()` asks the run to stop after its next commit: it stops reading,
  commits what it read, and ends `stopped`, and the next run reads on from there. `run.wait()`
  answers Ctrl-C as the command line does: the first asks for that stop and waits on, saying so on
  standard error, and a second cancels the run and raises `KeyboardInterrupt`.

## Running it

It needs the package, built into `.venv` by `just python-develop`, and the SQLite connector,
installed as the README's [From the command line](../../../README.md#from-the-command-line)
shows. Run it from the repository's root, with the connectors' directory, a directory for the
database and a count of commits after which the program asks for the stop itself:

```console
$ mkdir -p /tmp/observing-python
$ .venv/bin/python examples/python/observing/main.py "${XDG_DATA_HOME:-$HOME/.local/share}/rdlt/connectors" /tmp/observing-python 3
attempt 1 started
table events at schema version 1
commit 1: 50000 rows in 278 ms
commit 2: 50000 rows in 302 ms
commit 3: 50000 rows in 295 ms
stopping after the next commit
commit 4: 31000 rows in 124 ms
run ended: stopped
stopped: committed 181000 rows in 4 commits
```

Your counts and times will differ: the commit in flight when the stop is asked lands, and then one
of what was read since, where anything was. The commits' rows add up to the report's and the
table's, which holds each event once. Without a count it loads the rest, reading on from where
this run stopped, or stops at Ctrl-C. Once every page is read, a run commits no row.

A build or a run that fails raises its `rdlt.Error`, so the program exits 1, printing the error's
traceback; a run that ends stopped exits 0, after a first Ctrl-C too, where the Rust program and
the command line exit 130: `run.wait()` answers the stopped run's outcome and keeps no sign of the
signal. On Linux the SQLite connector runs in bubblewrap, which may write in the database's
directory alone.

## Tests

[`test_observing.py`](test_observing.py), with `just python-test examples/python/observing`:

- **Told as the README shows:** the program run as the console block runs it prints the block's
  lines, digits and all, but the commits' and the report's. Each commit line, printed or shown,
  has its form and comes in turn: three before the stop note, and any others between it and the
  run's end. In the block as in the output, the commits' rows add up to the report's, and their
  count to the commits it tells; the table holds the output's, each event once.
- **Read on:** a run stopped after three commits, then one with no count, land all 1,000,000
  events once, and a third run commits no row.
- **Ctrl-C:** interrupted once its first commit has landed, the program says it stops after the
  next commit, its run ends `stopped` with what it committed in the table, each event once, and
  the program exits 0.

Next: the repository README's
[Run control and reporting](../../../README.md#run-control-and-reporting), which says what else a
run's status and report hold, and its [Python](../../../README.md#python) section.
