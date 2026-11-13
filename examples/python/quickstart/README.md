# Quick start, in Python

The smallest pipeline from Python: a function's rows, three pages of ten users, loaded into
SQLite through the reference SQLite connector. `@stream` makes the function a stream, which
yields rows and the checkpoint after each page; a pipeline reads it in process. Its cursor,
`Since`, is the last page committed, so each run starts where the last committed one ended.
[`main.py`](main.py) is the whole of it, and the repository's README shows it as its Python quick
start, which a test holds to this code.

It needs the package, built into `.venv` by `just python-develop`, and the SQLite connector,
installed as the README's [From the command line](../../../README.md#from-the-command-line)
shows. Run it twice from the repository's root, with the connectors' directory and a directory
for the database:

```console
$ .venv/bin/python examples/python/quickstart/main.py ~/.local/share/rdlt/connectors .
succeeded: committed 30 rows
$ .venv/bin/python examples/python/quickstart/main.py ~/.local/share/rdlt/connectors .
succeeded: committed 0 rows
```

The second run commits nothing: `users` resumes from the cursor the first run committed with its
rows. On Linux the SQLite connector runs in bubblewrap, which may write in the database's
directory alone; where no sandbox can be made, the build raises `rdlt.ConfigError` coded
`sandbox_missing` or `sandbox_unavailable`, as the README says.

[`test_quickstart.py`](test_quickstart.py) runs it twice and checks that it prints what the
console block above shows, and the repository's README's too, which shows the program whole.

Next: [the PokeAPI source](../pokeapi), a connector class of a real HTTP API.
