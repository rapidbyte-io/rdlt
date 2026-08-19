# rdlt-cli

The pipeline CLI over the [`rdlt`](https://docs.rs/rdlt) library.

```
rdlt run <pipeline.yaml> [--report <path>] [--events <path|->]
rdlt check <pipeline.yaml>
rdlt schema <connector> [--role source|destination]
```

ONE YAML document describes the whole pipeline: pipeline-wide settings, the
source (inline, or `config: path` to a reusable document), and the
destination. One file, one format, end to end.

**Everything the CLI does, the library does.** It adds zero engine
capability — it parses the document, renders the event feed, and emits the
report. The document model and its construction into a pipeline are shared
library code (`rdlt::document`), so a platform embedding rdlt builds the
same pipelines without shelling out.

Events stream to stderr in human-readable form — a live display on a
terminal, a line per event elsewhere; the run report's JSON goes to stdout
(a terminal gets a one-line hint instead), or to `--report <path>`.
`--output auto|plain|json` overrides the terminal's choice: `plain` logs a
line per event even on a terminal, `json` silences the feed and prints the
report JSON to stdout even on a terminal. `--events <path|->` also writes
every event as NDJSON (`-` needs `--report`, so the two machine outputs
never share stdout). `check` performs connectivity, discovery and plan
checks, without running: the build gates run for real (spawn + handshake),
then both connectors' reachability probes, stream discovery, and the run's
plan validation — no load session, nothing created or written anywhere.
`schema` prints a spawned connector's configuration JSON Schema.

## Exit codes

Stable and scriptable, mirroring the `rdlt::error::Error` taxonomy:

| Code | Meaning |
|---|---|
| 0 | success |
| 2 | configuration |
| 3 | schema contract refused the change |
| 4 | source |
| 5 | destination |
| 6 | WAL / disk |
| 7 | cancelled |
| 64 | usage (`EX_USAGE`) |
| 70 | internal defect (`EX_SOFTWARE`) — report it |
| 74 | file I/O (`EX_IOERR`) |

The distinction matters to a caller: 2 means *edit your YAML*, 70 means *this
is our bug*, 74 means *the file could not be read or written*. An engine
that collapsed those into one code would send every operator to the wrong
place.
