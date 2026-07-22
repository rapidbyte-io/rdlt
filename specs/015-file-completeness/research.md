# Research: Filesystem/Object-Store Completeness

## R1 — Dependency survey (009 rule, per candidate)

**object_store — TAKE (the one new external dep).** Hand-rolling the
S3 surface means SigV4 request signing, XML list-objects with
continuation tokens, multipart/copy semantics, and error taxonomy —
well over a thousand lines of protocol code with real security-relevant
signing logic; the 009 "hand-roll when the dep buys little" rule cuts
the OTHER way here. `object_store` is the Apache-Arrow-project crate
(same governance as our pinned arrow), has no arrow version coupling
(independent release train), and with only the `aws` feature enabled
its tree is dominated by reqwest/rustls — already ours. Alternatives
rejected: `aws-sdk-s3` (huge smithy-generated tree, AWS-specific
posture), `rust-s3` (smaller community project, weaker maintenance
story), hand-roll (survey verdict above).

**csv — TAKE as a direct dep (already in the tree).** `csv` 1.4
(BurntSushi) is ALREADY in Cargo.lock via arrow-csv; declaring it
directly adds zero new code to the tree. Hand-rolling RFC-4180 quoting/
escaping is the classic false economy. Alternative rejected: arrow-csv
(would make CSV a structured stream — see R4 for why records win).

**flate2 + zstd — TAKE as direct deps (already in the tree).** Both are
in the lock today as parquet codec dependencies; direct declarations
add nothing new. gzip via flate2 (rust backend), zstd via the zstd
crate.

**RUSTFS is a container image, not a dependency.** Apache-2.0
S3-compatible object store (chosen because MinIO's license change makes
it unsuitable as the project's test server). Port 9000, static
access/secret key via env. Image/tag/env-var names are VERIFIED at the
environment-gate task (T001-class), not assumed here — same posture as
the podman-shim check.

## R2 — The weld: two moves and an absorption

**Decision**: US1 lands as three mechanical steps, each leaving the
suite green: (1) file source → `src/source/{mod,config,cursor}.rs` +
`formats/{jsonl,parquet}.rs` with `lib.rs` a thin façade re-exporting
every currently-public item; (2) the parquet crate's entire surface →
`src/dest/` (public types re-exported; `ParquetDir` name preserved as an
alias of the unified dest opened in parquet mode); (3) crate deletion +
workspace/facade/CLI/bench rewiring. Pins: pre-015 cells for BOTH crates
run unchanged beyond import paths; a persisted-format fixture (pre-015
cursor doc + staging dir + receipt files committed as test data) parses
and resumes; `parquet-passthrough` + `jsonl-duckdb-200k` gated bars
re-measured same-session. The `pq.*` fail-point names are persisted in
sweep tooling — they do NOT rename.

## R3 — File identity and cursors over object storage

**Decision**: the cursor document keeps `CURSOR_FORMAT_VERSION 1` and
its shape; the object-store entry reuses `mtime_ms`'s slot semantics
with etag: identity = (key, size, etag). Rules unchanged: completed ⇔
done==size (skip), grown → read tail from `done` (jsonl only, eol-flag
rules as today), size-shrunk OR same-size-different-etag → typed error
naming the object (rewritten in place). Range reads (`GET` with Range)
give tail resumption the same semantics as local seeks. Rationale: one
rulebook for both location kinds keeps the loud-failure story teachable;
etag is the store's content-identity and is returned by LIST — no extra
HEAD per file on the happy path.

## R4 — CSV is a record format, not a structured stream

**Decision**: CSV rows convert to NDJSON records and ride the record
path (shred, primary_key, dedup, merge, drift rules) exactly like
jsonl. Typing: inference per column over the file's prefix (documented
widening lattice: bool → int64 → float64 → utf8; empty field = null),
overridden per column by the existing `type_hints` vocabulary — a value
that cannot satisfy a DECLARED hint is a typed error naming file, row,
column. Header row provides names (`header: true` default); without it,
columns are `c0..cN`. Rationale: dlt parity (CSV is a record source
there), and the record path is where primary_key/merge live — CSV users
expect those. arrow-csv (structured) rejected: parquet's S7
"structured, no per-row identity" posture fits column stores, not CSVs
that users merge by key. Options: `delimiter` (default `,`), `header`
(default true), `quote` (default `"`); malformed row → typed error
naming file + 1-based row number.

## R5 — Compression

**Decision**: extension-driven (`.gz`, `.zst` suffix after the format
extension, e.g. `events.jsonl.gz`): the byte reader wraps in the codec
stream before format parsing; applies to jsonl and csv (parquet has
internal codecs — a compressed-parquet-file spelling is rejected
typed). A compressed file is a whole-file incremental unit: cursor
records decompressed-bytes progress but resume-at-offset is only valid
at done==size (completed-skip); a grown compressed file is a rewrite by
definition → loud failure. Magic-byte check on open: extension/codec
mismatch is a typed error naming the file. Rationale: transparent
decode is table stakes for landed logs; whole-file units keep the
exactly-once story honest (gzip streams are not seekable).

## R6 — Destination finalize on object storage

**Decision**: staging keys live under the existing
`.rdlt-staging/<scope>/…` prefix (same constants), uploaded via
object_store `put` (single or multipart as size dictates — puts are
atomic-per-key in S3 semantics: no reader ever sees a partial object).
Finalize at commit = server-side COPY staged→final + DELETE staged, per
file, in the deterministic (load, commit, table, n) order; the
state/receipt documents write LAST, exactly as the local protocol
orders rename-then-state today. Recovery: re-running finalize is
idempotent (copy overwrites identical content; delete of missing staged
key is success); incomplete multipart uploads are aborted by re-put
under the same staged key. The local path keeps its
rename+fsync protocol BYTE-IDENTICAL. Honesty note (mirrors 002 R18):
multi-file set-atomicity is per-key, not per-set — the receipt is the
set-commit witness, unchanged.

## R7 — Config vocabulary (additive)

**Decision**: source stream gains `location:` (absent = local, exactly
today's `path` semantics) — `location: {s3: {endpoint, bucket, region?,
access_key, secret_key, path_style: true}}` with `path` then meaning
the key prefix/glob inside the bucket; credentials are `Secret` fields
(014 discipline, grep-proof cell). `format:` gains `csv` + a
`csv: {delimiter, header, quote}` options block. Destination config
(today: `path`) gains the same `location:` block, `format:
parquet|jsonl` (default parquet — frozen behavior), and
`partition_by: <column>` (optional). CLI: `destination: parquet:` stays
frozen (byte-identical meaning); `destination: file:` is the NEW
spelling exposing the full vocabulary; `source: file:` unchanged with
the new fields available inside the config document. All growth
additive; schema round-trips extended.

## R8 — Container fixture

**Decision**: a `rustfs` fixture joins the bench/test container
registry alongside postgres: started via the podman shim, health-check
= S3 ListBuckets against the endpoint, seeded by the test itself
through object_store (no shell aws-cli dependency), torn down like the
postgres cells; `RDLT_S3=…` override envs mirror the postgres-cell
convention. Cells skip-not-fail without a container runtime. The
pagination-complete cell seeds >1000 keys (S3 default page size) to
force continuation-token traversal.

**VERIFIED at the T001 environment gate (2026-07-22)**: image
`docker.io/rustfs/rustfs:latest` — S3 API on container port 9000
(console on 9001), credentials via `RUSTFS_ACCESS_KEY` /
`RUSTFS_SECRET_KEY` env vars (honored: a SigV4-signed ListBuckets with
gate-chosen keys succeeded; anonymous requests get S3-style XML
`AccessDenied`), data volume `/data` (`RUSTFS_VOLUMES`), entrypoint
needs no arguments. Startup to ready ≈ 3s.

## R9 — Crash points

**Decision**: preserved: `pq.replace.truncate`, `pq.staged.sync`,
`pq.part.rename`, `pq.dir.fsync`, `pq.state.write`,
`pq.receipt.write` (names are part of sweep tooling). New:
`file.list`, `file.read` (source side), `file.stage.put`,
`file.finalize.copy`, `file.finalize.delete` (dest object path). The
sweep runs the local matrix always and the object matrix inside the
container cells; both assert armed-fire pins and exactly-once totals.

## R10 — Bench posture

**Decision**: existing gated bars untouched (`parquet-passthrough`,
`jsonl-duckdb-200k` merely re-proven in-band after the weld). New cell
`file-s3-duckdb-200k` (scoreboard): the flagship 200k nested dataset
seeded into RUSTFS as jsonl, read via the s3 location into duckdb —
recorded baseline-first per the 012 harness rules (a dlt filesystem
baseline rides the same container). Object-store throughput is
correctness-first this feature; a gated bar would gate on the TEST
SERVER's performance, which is not ours to promise (the 004
measurement-first rule: bars come from measured floors, and this floor
measures RUSTFS).
