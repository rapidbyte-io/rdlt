# rdlt for Python

The `rdlt` Python package runs rdlt's pipelines, which move data exactly once, with the same
words, checks, errors and events as Rust and the command line, and runs Python functions inside
the engine as sources and transforms. Python connectors are written on the Rust SDK and certified
in pytest. It needs CPython 3.11 or newer, or free-threaded CPython 3.14, on Linux or macOS.

No wheel is published yet. `pip` builds one from the repository, with Rust 1.98 or newer and a C
compiler:

```sh
pip install "rdlt @ git+https://github.com/rapidbyte-io/rdlt"
```

`pipeline.run()` answers Ctrl-C as rdlt's command line does: the first stops the run after its next
commit and waits for it to end, and a second cancels the run and raises `KeyboardInterrupt`. Under
`asyncio.run` the first Ctrl-C cancels the main task, and an await of the run cancelled with it asks
the run to stop after its next commit and raises at once, so the process exits without waiting for
that commit. Exactly-once still holds: what was committed stays, and the next run reads on from the
last committed cursor.

The repository's [README](https://github.com/rapidbyte-io/rdlt#python) has the quick start, and
[ADR 0055](https://github.com/rapidbyte-io/rdlt/blob/main/docs/adr/0055-python.md) the design.
