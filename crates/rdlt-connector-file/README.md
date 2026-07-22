# rdlt-connector-file

The file family: read JSONL, CSV, and parquet from the local filesystem
or any S3-compatible object store, and write parquet or JSONL back to
either — with per-file incremental cursors, commit-atomic publication,
and typed errors naming the file, row, and location for everything that
can go wrong. One crate, both directions, one shared definition of what
each format and location is to rdlt.

Facade: `rdlt::connector::file`. Pre-015 spellings are frozen: the
`file:` source document and the `destination: parquet: {path}` CLI form
parse unchanged, `ParquetDir::open(dir)` still constructs the local
parquet destination, and persisted state (cursor documents, staging
layout, receipts) remains byte-compatible.

```yaml
# pipeline.yaml
source:
  file:
    streams:
      - name: events
        format: csv
        csv: {delimiter: ",", header: true}
        path: "landed/2026/*.csv.gz"
        location:
          s3:
            endpoint: "http://127.0.0.1:9000"
            bucket: raw
            access_key: "${S3_KEY}"
            secret_key: "${S3_SECRET}"
        primary_key: [id]
        type_hints: {amount: float64, created_at: timestamp_tz}
destination:
  file:
    path: warehouse
    format: parquet
    partition_by: created_day
    location:
      s3: {endpoint: "http://127.0.0.1:9000", bucket: lake,
           access_key: "${S3_KEY}", secret_key: "${S3_SECRET}"}
```

Entry points: `FileSource::from_yaml`/`from_json`/`from_value` (the
embedder seam), `FileDest::from_config(FileDestConfig)` with builder
methods, `config_schema()`/`dest_config_schema()` generated from the
structs (schema and parser cannot drift). All validation is eager and
typed at parse.

## Source — stream options

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Stream (root table) name. |
| `format` | `jsonl` \| `csv` \| `parquet` | required | Explicit — no extension guessing. `jsonl`/`csv` are record streams (primary_key, dedup, merge downstream); `parquet` is structured (Arrow batches, no per-row identity — contract S7). |
| `path` | string | required | Local path or glob; with a `location`, the key or prefix+glob inside the bucket. Deterministic lexicographic order. An explicitly named missing file is a typed error; an empty glob/prefix is an empty success; a file whose name contains glob metacharacters but exists is taken literally; a partial listing never passes as complete. |
| `location` | block | absent = local | `s3: {endpoint, bucket, region?, access_key, secret_key, path_style: true}`. Any S3-compatible store (AWS, R2, RUSTFS, …). Credentials are `Secret`-typed — never rendered in Debug/errors (grep-proof cell). Unreachable endpoints and wrong credentials are typed errors naming endpoint+bucket — never a silent empty load. |
| `csv` | block | `{delimiter: ",", header: true, quote: "\""}` | CSV reader options; only valid with `format: csv`. Single-ASCII-byte delimiter/quote. `header: false` names columns `c0..cN`. |
| `primary_key` | [column] | absent | Merge identity downstream (record formats only; typed error on parquet). |
| `type_hints` | map | `{}` | `bool`, `int64`, `float64`, `utf8`, `timestamp_tz`, `date`, `time`, `uuid`, `json`. For CSV, hints OVERRIDE inference and are enforced per cell — a violating value is a typed error naming file, row, and column. |
| `validate` | bool | `true` | JSONL only: skim-parse each line so malformed input fails naming the file + byte offset. |

### CSV semantics

Rows convert to records on a documented inference lattice, decided per
column over the whole file: `bool → int64 → float64 → utf8` (a column
that ever sees a non-numeric value is text for the whole file); empty
cells are null. Ragged rows (wrong field count) are typed errors naming
file + row. CSV files are **whole-file incremental units** (quoted
newlines make byte-offset resume unsafe): complete files are skipped on
re-runs, and a size change is a typed error — deliver new data as new
files.

### Compression

`*.gz` (gzip) and `*.zst` (zstd) decode transparently for jsonl and
csv. The codec is chosen by extension and verified by magic bytes — a
mismatch is a typed error naming the file, never garbage rows.
Compressed files are whole-file incremental units. Parquet carries its
own internal codecs; a compression extension on a parquet path is
rejected at parse.

### Incremental — one cursor rulebook, both location kinds

Per-file progress rides the engine checkpoint machinery
(`format_version 1` cursor documents, frozen):

- **Completed files are skipped** (`done == size`, identity unchanged).
- **Grown plain-jsonl files resume at the recorded offset** — only if
  the consumed range ended on a record boundary; otherwise loud.
- **Rewritten files fail loudly**, never a stale-offset read: same-size
  with a moved mtime (local) or a changed etag (object store) is a
  typed error naming the file.
- **Shrunk files fail loudly.**
- Whole-file formats (csv, compressed) re-read entirely after a
  mid-file crash — exactly-once under keyed merge/dedup, documented.
- Object parquet is fetched to temp files (correctness-first) with the
  cursor still keyed by the object.

## Destination — options (`FileDestConfig` / CLI `destination: file:`)

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | string | required | Output directory (local) or key prefix (bucket). |
| `location` | block | absent = local | Same vocabulary as the source. |
| `format` | `parquet` \| `jsonl` | `parquet` | Output format; identical layout/atomicity guarantees. |
| `partition_by` | column | absent | One prefix per rendered value: `<table>/<value>/part-….` NULLs land under `__null__`. The column must exist in the stream's schema at write time (typed, naming it). |

The CLI's `destination: parquet: {path}` spelling stays frozen and
means local + parquet + no partitioning.

### Commit discipline

Writes stage under `.rdlt-staging/<pipeline-scope>/<load>/` (a scope
hash isolates pipelines sharing one output location). Publication at
commit is the visibility event: atomic renames locally, server-side
COPY-then-DELETE per key on object stores — a reader can never observe
a partial object under a final name (probed live by a concurrent
lister). Final names are deterministic per
(load, commit, table, partition, n), so crash replay converges
idempotently; Replace-mode truncation is guarded durably to once per
load; state and receipts write last. Fail points
(`pq.*` preserved + `file.list`/`file.read`/`file.stage.put`/
`file.finalize.copy`/`file.finalize.delete`) are swept with armed-fire
pins on both location kinds.

Append/Replace only — `merge: false` by capability (per-row identity
lives in the SQL destinations).

## Error classification

Store/network failures are transient (the engine's retry budget);
malformed data, codec mismatches, identity tripwires, unauthorized or
missing locations, and config violations are fatal and always name
their subject (file, row/byte/row-group, key, bucket, endpoint).

## Verification records

`specs/015-file-completeness/matrix.md` — parameter traceability, zero
uncited rows. `specs/015-file-completeness/dlt-parity.md` — capability
mapping vs dlt 1.29.0's filesystem source/destination with deliberate
deviations named. Live cells run against a RUSTFS container (Apache-2.0
S3-compatible server) and skip visibly without a container runtime.
Pre-015 behavior is pinned by committed fixtures
(`tests/preservation.rs`).
