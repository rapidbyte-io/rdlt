# Quickstart: Filesystem/Object-Store Completeness

## Read a bucket, land a lake

```yaml
# pipeline.yaml
pipeline: s3-demo
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
        type_hints: {amount: float64, created_at: timestamp_tz}
        primary_key: [id]
destination:
  file:
    path: warehouse/events
    format: parquet
    partition_by: created_day
    location:
      s3: {endpoint: "http://127.0.0.1:9000", bucket: lake,
           access_key: "${S3_KEY}", secret_key: "${S3_SECRET}"}
```

`rdlt run pipeline.yaml` — second run loads only new/grown files.
Pre-015 spellings still work verbatim: `source: file:` with local
globs, `destination: parquet: {path: out/}`.

## Verify

```bash
cargo nextest run -p rdlt-connector-file            # unit + local cells
cargo nextest run -p rdlt-connector-file -E 'binary(s3_live)'  # RUSTFS container (skips w/o podman)
cargo nextest run -p rdlt-connector-file --features failpoints -E 'binary(sweep)'
cargo llvm-cov nextest -p rdlt-connector-file       # ≥80% floor
TARGET='parquet-passthrough' make bench             # gated bar still in-band
TARGET='jsonl-duckdb-200k' make bench               # gated bar still in-band
```

## The rules

`contracts/file-family.md` (FF1–FF8): frozen behavior through the
merge, complete-or-fail discovery, one cursor rulebook, shared typed
formats, commit-atomic visibility, secret redaction, crash discipline,
matrix + parity + container-cell verification.
