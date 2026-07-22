# Data Model: Filesystem/Object-Store Completeness

## 1. Location (shared by source and dest)

```
Location = Local                       # absent block = today's semantics
         | S3 { endpoint: String,      # e.g. http://127.0.0.1:9000
                bucket: String,
                region: Option<String>,     # default "us-east-1"
                access_key: Secret,
                secret_key: Secret,
                path_style: bool }          # default true (test servers)
```

Validation (eager, typed): endpoint parses as URL; bucket non-empty;
credentials present together. Secrets never render (014 grep-proof
discipline). Reachability is an OPEN-time check, not parse-time — but
unreachable/unauthorized at open is a typed error naming
endpoint/bucket (never a silent empty load).

## 2. File snapshot & identity

```
FileMeta = { name: String,            # local path | object key
             size: u64,
             identity: Mtime(ms) | Etag(String) }
```

Discovery resolves path/prefix+glob → lexicographically sorted,
COMPLETE list (continuation tokens fully drained or typed failure).
Empty match-set = success; explicitly named missing file = typed error.
A named existing file is literal even if it contains glob metacharacters
(existing rule).

## 3. Per-file cursor (CURSOR_FORMAT_VERSION 1 — unchanged shape)

```
FileProgress = { done: u64, size: u64, eol: bool,
                 mtime_ms: Option<u64>,      # local identity (existing)
                 etag: Option<String> }      # NEW, additive: object identity
```

Rules (one rulebook, both kinds): done==size → skip; grown + eol →
read tail from done (jsonl only); shrunk, or same-size with moved
mtime/changed etag → typed error naming the file; compressed files:
resume only at done==size. Pre-015 cursor documents parse unchanged
(new field defaults None).

## 4. Formats

```
Format = Jsonl                        # record stream (fast path, RS5-class)
       | Csv { delimiter: u8 = b',',
               header: bool = true,
               quote: u8 = b'"' }     # record stream via NDJSON conversion
       | Parquet                      # structured stream (S7), row-group units
Codec  = None | Gzip | Zstd           # by extension; jsonl/csv only
```

CSV inference lattice: bool → int64 → float64 → utf8; empty = null;
`type_hints` overrides per column (violations typed: file, row, column).
Headerless columns are `c0..cN`.

## 5. Source stream (additive over FileStream)

```
FileStream += { location: Option<Location>,   # absent = Local
                csv: Option<CsvOptions> }     # only with format: csv (typed)
```

`path` keeps its meaning (local path/glob | key prefix+glob under the
bucket). Existing fields (`format`, `primary_key`, `type_hints`,
`validate`) unchanged; `csv` block with a non-csv format is a typed
error; `primary_key` stays record-format-only (jsonl/csv), typed on
parquet (existing S7 rule).

## 6. Destination config (absorbing the parquet crate's)

```
FileDestConfig = { path: String,                  # dir (local) | prefix (s3)
                   location: Option<Location>,    # absent = Local
                   format: parquet | jsonl = parquet,
                   partition_by: Option<String> } # column name
```

CLI spellings: `destination: parquet: {path}` FROZEN (≡ file dest,
format parquet, local); `destination: file: {…}` NEW, full vocabulary.
Staging (`.rdlt-staging/<scope>/…`), receipts, state docs, pipeline
scoping: LAYOUT_FORMAT_VERSION 1, byte-identical local; same logical
layout as object keys. `partition_by` column must exist in the stream's
schema at write time (typed); rows with NULL partition values land under
a documented `__null__` partition prefix.

## 7. Fail points

Preserved: `pq.replace.truncate`, `pq.staged.sync`, `pq.part.rename`,
`pq.dir.fsync`, `pq.state.write`, `pq.receipt.write`.
New: `file.list`, `file.read`, `file.stage.put`, `file.finalize.copy`,
`file.finalize.delete`.

## 8. Error taxonomy (S3 posture unchanged)

Transient: network/timeout/5xx from the store, credential-expiry
mid-run. Fatal: malformed rows (file+row/byte), codec/extension
mismatch, identity tripwires, unauthorized/missing bucket or named
file, config violations (eager). RateLimited: store 429/backoff
signals with retry-after when present.
