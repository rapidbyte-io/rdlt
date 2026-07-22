# dlt parity record — the file family (015)

Reference: dlt 1.29.0 `filesystem` source (`readers`) and `filesystem`
destination. Verdict per row: PARITY (equivalent capability), DEVIATION
(deliberately different), or OUT (deferred with a trigger).

## Source (vs dlt `filesystem`/`readers`)

| dlt | rdlt file source | verdict |
|---|---|---|
| `read_jsonl` | `format: jsonl` (byte-level fast path, per-file byte cursors) | PARITY (rdlt adds tail resume + loud rewrite tripwires dlt lacks) |
| `read_csv` (pandas/duckdb backends) | `format: csv` — record stream via NDJSON, documented inference lattice, `type_hints` override, delimiter/header/quote options | PARITY for the declarative surface; DEVIATION: no dataframe-backend choice — one documented reader, typed errors naming file/row/column instead of backend-dependent coercion. |
| `read_parquet` | `format: parquet` (structured Arrow batches, row-group units) | PARITY |
| bucket URLs (s3://, gs://, az://…) via fsspec | `location: {s3: {…}}` — S3-compatible endpoint + static credentials | PARITY for S3-compatible; OUT: GCS/Azure native APIs and provider-native auth (IAM roles, workload identity) — deferred until a consumer needs them; any S3-compatible endpoint (R2, GCS interop mode) works today. |
| glob patterns | local globs + prefix+glob keys, COMPLETE-or-fail listings | PARITY (rdlt pins listing-pagination completeness with a 1100-key cell; a partial listing can never pass) |
| incremental by mtime (`apply_hints(incremental=…)`) | per-file cursors: completed-skip, grown-tail resume, identity tripwires (mtime local, etag object), whole-file units for csv/compressed | DEVIATION by design — file-level progress with loud failure on rewritten files instead of mtime-window heuristics; exactly-once outcomes ride the engine checkpoint machinery. |
| compression (gzip via fsspec) | gzip + zstd by extension, magic-byte mismatch typed | PARITY+ (zstd added; mismatch is typed, never garbage rows) |
| custom readers (any fsspec open) | — | OUT of the connector — arbitrary formats are future formats; the `formats/` module is the in-crate seam. |

## Destination (vs dlt `filesystem` destination)

| dlt | rdlt file dest | verdict |
|---|---|---|
| parquet output | `format: parquet` (default; pre-015 protocol frozen) | PARITY |
| jsonl output | `format: jsonl` | PARITY |
| csv output | — | OUT — no consumer; the format seam makes it additive when one appears. |
| bucket destinations | `location: {s3: {…}}`, staged keys → COPY+DELETE finalize, commit-atomic visibility (probed live) | PARITY for S3-compatible; same GCS/Azure deferral as the source. |
| layout templates (`{table_name}/{load_id}.{ext}` etc.) | fixed deterministic layout `<table>[/<col>=<val>]/part-<load>-<seq>-<n>.<ext>` | DEVIATION — the layout is a persisted-format identity (recovery converges BECAUSE names are deterministic), not a template option; partitioning is the supported axis. |
| partitioning (via layout placeholders) | `partition_by: <column>` — one prefix per rendered value, `__null__` for NULLs, missing column typed | PARITY for single-column; multi-column nesting deferred until asked for. |
| delta/iceberg table formats | — | OUT (spec Out-of-Scope; future feature). |
| merge/replace dispositions | Append/Replace (replace = durable once-per-load truncation); merge stays the SQL destinations' capability | DEVIATION recorded — dlt's filesystem "merge" is really file bookkeeping; rdlt keeps merge semantics where per-row identity exists. |

## Discipline dlt does not have

Commit-atomic visibility probed by a concurrent lister against a real
S3 server; crash points on both location kinds with armed-fire sweep
pins and exactly-once totals; the preservation net (pre-015 cursor and
commit-log bytes committed as fixtures); complete-or-fail listings;
secret-redacted credentials with a grep-proof cell; container cells
that skip-not-fail.
