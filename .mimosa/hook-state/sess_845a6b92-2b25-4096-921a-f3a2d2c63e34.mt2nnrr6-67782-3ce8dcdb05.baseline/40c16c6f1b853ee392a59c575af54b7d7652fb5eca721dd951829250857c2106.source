# rdlt-cli

A thin development CLI over the [`rdlt`](https://docs.rs/rdlt) library.

```
rdlt run <pipeline.yaml> [--report <path>]
```

ONE YAML document describes the whole pipeline: pipeline-wide settings, the
source (inline, or `config: path` to a reusable document), and the
destination. One file, one format, end to end.

**Everything the CLI does, the library does.** It adds zero engine
capability — it parses the document, renders the event feed, and emits the
report. The document model and its construction into a pipeline are shared
library code (`rdlt::pipeline_spec`), so a platform embedding rdlt builds the
same pipelines without shelling out.

Events stream to stderr in human-readable form; the `RunReport` JSON goes to
stdout, or to `--report <path>`.

## Exit codes

Stable and scriptable, mirroring the `RdltError` taxonomy:

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
