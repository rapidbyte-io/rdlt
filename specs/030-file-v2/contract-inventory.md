# 030 — GENERATION-1 CONTRACT INVENTORY: rdlt-connector-file

Prepared for the true-greenfield rewrite (`030-file-v2`, main @ 8a10d0fe).
Source of record: `crates/rdlt-connector-file` (src 4,741 lines; tests 3,045
lines) plus consumers in `crates/rdlt`, `crates/rdlt-cli`, `crates/rdlt-engine`,
`Makefile`, `benches/`, `fuzz/`. All quoted spellings are EXACT (byte-level)
unless flagged. `\` inside quoted Rust format strings below indicates the
source used a string-literal line continuation — the rendered message has a
SINGLE space there.

Layout of gen 1: `lib.rs` façade over `source/{mod,config,cursor}.rs`,
`dest/{mod,config,session,layout,truncate,inspect,writer_props}.rs`,
`formats/{mod,jsonl,csv,parquet}.rs`, `location/{mod,types,s3}.rs`.
Crate root re-exports (lib.rs): `dest::ParquetDir`, `formats::Format`,
`source::{FileConfig, FileSource, FileStream, config, config_schema, cursor}`.

---

## 1. CONFIG VOCABULARY (frozen), both halves

### 1.1 Source document (`FileConfig`, src/source/config.rs)

Top level: `#[serde(deny_unknown_fields)]`, `#[non_exhaustive]`.

```yaml
streams:                    # Vec<FileStream>, min 1 (schemars length(min=1) AND validate())
  - name: <string>          # required
    format: jsonl|parquet|csv   # required, rename_all snake_case, NEVER inferred from extension
    location:               # Option<LocationOptions>, default None = local filesystem
      s3: { ... }           # see 1.3
    csv:                    # Option<CsvOptions>, default None; only with format: csv (typed refusal otherwise)
      delimiter: ","        # char, default ',' (must be single ASCII byte)
      header: true          # bool, default true; without it columns are c0..cN
      quote: "\""           # char, default '"' (must be single ASCII byte)
    path: <string>          # required; explicit file path or glob. Named-missing = error; empty glob = empty stream
    primary_key: [<string>] # Option<Vec<String>>, default None; jsonl+csv only; REFUSED on parquet
    type_hints: {col: hint} # BTreeMap<String, HintType>, default {}; csv reads directly, jsonl → shredder;
                            # on parquet ACCEPTED AND IGNORED (documented, not refused)
    validate: true          # bool, default true (fn default_validate); jsonl only — per-line JSON skim-parse;
                            # on csv/parquet ACCEPTED AND IGNORED (documented)
```

`HintType` (rename_all snake_case, non_exhaustive; "same human-friendly hint
names as the REST source"): `bool`, `int64`, `float64`, `utf8`, `timestamp_tz`,
`date`, `time`, `uuid`, `json`. `From<HintType> for LogicalType` 1:1.

All config structs are `deny_unknown_fields` + `#[non_exhaustive]`:
`FileConfig`, `FileStream`, `LocationOptions`, `S3Options`, `CsvOptions`,
`FileDestConfig`, and the SPI's `ParquetOptions` (deny_unknown_fields, NOT
non_exhaustive). No serde field renames anywhere in CONFIG (only enum
`rename_all = "snake_case"`); the ONLY wire renames are on the persisted
`FileProgress` (§3).

### 1.2 Source validation rules + EXACT messages (`FileConfig::validate`, config.rs:122-162)

- empty streams → `ConfigError::Invalid("at least one stream is required")`
- per stream, location present → `LocationOptions::validate(&format!("stream `{}`", stream.name))`
- csv block on non-csv format →
  `stream `{name}`: a `csv` options block requires `format: csv``
- csv options → `CsvOptions::validate(context)`:
  `{context}: csv.{name} `{value}` must be a single ASCII byte` (name ∈ delimiter|quote)
- parquet + primary_key →
  `stream `{name}`: parquet streams are structured and cannot declare \ primary_key (no per-row identity)`
- parquet + compression extension on path (codec_of(path) not plain) →
  `stream `{name}`: parquet carries its own internal codecs — a \ compression extension on a parquet path is not supported`

`ConfigError` (thiserror):
- `invalid file source YAML: {0}` (from serde_yaml::Error)
- `invalid file source JSON: {0}` (from serde_json::Error)
- `invalid file source config: {0}` (Invalid(String))

NOTE: duplicate stream names are NOT refused (see §9-S1).

### 1.3 Location vocabulary (location/mod.rs, location/s3.rs)

`LocationOptions` — a STRUCT, not enum, "so YAML stays the natural
`location: {s3: {...}}` and future kinds (gcs:, azure:) are ADDITIVE fields
with exactly-one-set validation". Fields: `s3: Option<S3Options>` (default).
Programmatic seam: `LocationOptions::s3(options)`.
Validation: absent s3 →
`{context}: `location` block declares no storage kind (expected `s3`)`.

`S3Options` (deny_unknown_fields, non_exhaustive):
```yaml
s3:
  endpoint: <string>        # required; must be http(s) URL with nonempty host
  bucket: <string>          # required, nonempty
  region: <string>          # Option, default None → "us-east-1" at build time
  access_key: <Secret>      # rdlt_connector::Secret (Debug renders ***; serde round-trips value)
  secret_key: <Secret>
  path_style: true          # bool, default TRUE (fn default_path_style) — test-server form
  unsigned_payload: false   # bool, default false; a SECURITY setting, never inferred
                            # (payload SHA-256 is 6.72% of the parquet-to-S3 cell)
```
Builders: `S3Options::new(endpoint, bucket, access_key, secret_key)` (region
None, path_style true, unsigned false), `with_unsigned_payload`, `with_region`,
`with_path_style`. Validation messages:
- `{context}: location.s3.endpoint `{endpoint}` must be an http(s) URL`
- `{context}: location.s3.bucket must not be empty`

### 1.4 Formats (formats/mod.rs)

`Format` enum (snake_case, non_exhaustive): `jsonl`, `parquet`, `csv`.
Compression codec decided PER FILE by extension (R5): `.gz` → Gzip, `.zst` →
Zstd, else Plain (`codec_of`). Magic bytes checked on open (`open_decoded`):
gzip `[0x1f,0x8b]` (2 bytes), zstd `[0x28,0xb5,0x2f,0xfd]` (4 bytes); mismatch →
`file `{path}` does not match its compression extension \ (magic bytes {:02x?}) — fix the name or the content`
(the `{:02x?}` renders the observed bytes). `zstd `{path}`: {e}` on decoder
construct. Shared slab constant `SLAB_BYTES = 8 << 20` (8 MiB).

CSV inference lattice (formats/csv.rs — the JOIN, total and commutative):
`Empty` bottom; `bool` disjoint from numeric chain; `Int → Float` widening;
any mix involving bool, or anything×text → `Text`. `kind_of`: "true"/"false" →
Bool; parses i64 → Int; parses f64 → Float; else Text. Empty cells → JSON null.
Column that never saw a value (Empty) → Text. Headerless columns named `c0..cN`.
Whole file is one incremental unit (quoted newlines make byte resume unsafe);
TWO passes (infer then convert). Hints override inference per column.

### 1.5 Destination document (`FileDestConfig`, src/dest/config.rs)

```yaml
path: <string>              # required; output dir (local) or key prefix (object store)
location:                   # Option<LocationOptions>, default None = local
  s3: { ... }
format: parquet             # DestFormat: parquet (DEFAULT) | jsonl, snake_case, non_exhaustive
partition_by: <string>      # Option; ONE column; BARE value layout `<table>/<value>/part-...`
                            # (NOT Hive `col=value`); NULL → `__null__`; column must exist at write time
parquet:                    # Option<ParquetOptions> (SPI type, re-exported); absent = defaults (COMPRESSED)
  compression: snappy       # uncompressed|snappy|gzip|lz4_raw|zstd|brotli (default snappy)
  compression_level: <i32>  # Option; only gzip/zstd/brotli take one (typed refusal otherwise)
  dictionary_enabled: true
  dictionary_page_size_limit: 65536   # measured default, 16x below library 1 MiB
  data_page_size_limit: <usize>       # Option
  max_row_group_rows: <usize>         # Option; 0 refused (library setter panics on Some(0))
```

Builders: `FileDestConfig::new(path)`, `with_location`, `with_format`,
`with_partition_by`, `with_parquet`; accessor `parquet_options()` (configured
or default — callers never read the raw field). Validation
(`validate(context)`, context = `"file destination"` from `from_config`):
- `{context}: `path` must not be empty`
- location → LocationOptions::validate(context) (SAME context, nested)
- `{context}: `partition_by` must name a column` (empty string)
- parquet block under non-parquet format:
  `{context}: a `parquet` block is set but `format` is `{fmt}` — \ these settings only apply to parquet output; remove the block, \ or set `format: parquet``
- then `ParquetOptions::validate()` mapped `{context}: {message}`.

SPI `ParquetOptions::validate` messages (crates/rdlt-connector/src/parquet.rs,
thiserror, `OptionsError` non_exhaustive):
- `` `compression_level` is set but `{codec}` has no compression level — remove the level, or choose a codec that takes one (gzip, zstd, brotli)``
- `` `max_row_group_rows` is 0 — a row group must hold at least one row; remove the setting to use the default, or give a positive count``
- `` `dictionary_page_size_limit` is 0 while dictionary encoding is enabled — a dictionary page cannot be zero bytes; raise the limit, or set `dictionary_enabled: false` to disable dictionary encoding outright``

Codec spellings round-trip as written: `uncompressed`, `snappy`, `gzip`,
`lz4_raw`, `zstd`, `brotli`; `takes_level()` = {gzip, zstd, brotli}.
The serde-default trap is documented on the type: every field names its own
default fn (a bare `#[serde(default)]` would give `false`/`0`).

### 1.6 Parse surface + what the facade/CLI use

Source: `FileConfig::{from_yaml, from_json, from_value}` (all parse → validate),
`FileSource::{from_yaml, from_json, from_value, new(FileConfig)}`,
`config_schema()` = `schemars::schema_for!(FileConfig)` (schema GENERATED from
the parse structs — cannot drift). Dest: `FileDest::open(path)` (plain-path,
local+parquet+no-partitioning; PathBuf used AS-IS, config mirror is
`to_string_lossy` informational only), `FileDest::from_config(FileDestConfig)`,
`dest_config_schema()`. `pub type ParquetDir = FileDest;` — CANONICAL
local-parquet spelling, "supported by name, not a deprecated alias" (bench,
CLI, crash-sweep tooling consume it).

Facade (crates/rdlt/src/lib.rs): line 53 `pub use rdlt_connector_file as file;`
line 62 `pub use rdlt_connector_file as parquet;` (module alias so
`rdlt::connector::parquet::ParquetDir` keeps resolving). Features
(crates/rdlt/Cargo.toml): `file = ["dep:rdlt-connector-file"]`,
`parquet = ["file"]` — the parquet FEATURE spelling frozen for build scripts.

Pipeline spec (crates/rdlt/src/pipeline_spec.rs):
- `SourceSpec::File { config }` (line ~85): `config` is a PATH to the file
  source YAML/JSON document; resolved by extension: `.json` →
  `FileSource::from_json(&text)` else `from_yaml` (lines 343-350).
- `DestSpec::Parquet { path }` (line ~173): frozen `parquet: {path}` spelling →
  `ParquetDir::open(path)`, error context `opening parquet dir: {e}` (451-455).
- `DestSpec::File(Box<FileDestConfig>)` (line ~192): the connector config IS
  the document shape, EMBEDDED not mirrored (a hand mirror failed silently:
  fields configurable in the library, invisible from YAML). Error context
  `file destination: {e}` (457-460).
Spec enums use `serde_yaml::with::singleton_map`.

---

## 2. FROZEN MESSAGE SPELLINGS, CLASSIFICATION, CRASH POINTS

### 2.1 Classification rulebook

ONE rulebook, both halves, via the SPI's `rdlt_connector::store::is_recoverable`:
- `store_err(e: object_store::Error) -> DestinationError`
  (location/mod.rs:35-41): recoverable → `transient(e.to_string())`, else
  `fatal(e.to_string())`. Test-pinned semantics: Generic transport → transient;
  NotFound/InvalidPath/NotSupported/NotImplemented/UnknownConfigurationKey →
  fatal; AlreadyExists + Precondition → TRANSIENT (S3 `OperationAborted` on
  unconditional requests — the store's own retry loop doesn't cover it).
- Source S3 `classify(action, subject, error)` (s3.rs:285-307): severity from
  the ONE rulebook, the match only chooses WORDING. Subject prefix:
  `{action} `{subject}` (s3 `{endpoint}` bucket `{bucket}`)` with actions
  `object` | `listing` | `reading`. NotFound → `{name}: not found`;
  Unauthenticated|PermissionDenied → `{name}: unauthorized — check credentials/bucket`;
  else `{name}: {error}`. Recoverable → `SourceError::transient("{name}: {error}")`.
- io seam: `classify_read_error(context, e)` (location/mod.rs:473-479):
  `ErrorKind::ConnectionReset` → transient `{context}: {e}`, else fatal.
  `S3Reader::read_full` maps recoverable stream errors to ConnectionReset kind,
  message `reading {subject}: {e}` (subject =
  `{name} (s3 `{endpoint}` bucket `{bucket}`)`).
- EVERYTHING else in the crate is `fatal`. No RateLimited anywhere.

### 2.2 Source-side operator-facing messages (exact)

resolve/list (source/mod.rs):
- `invalid glob `{pattern}`: {e}`
- `expanding glob `{pattern}`: {e}; refusing to load a partial file list`
- `` `{pattern}` is a directory, not a file — name a file, or use a \ pattern such as `{pattern}/*.jsonl` ``
- `file `{pattern}` does not exist`
- `stat `{path}`: {e}`
- `unknown stream {name}` (read() on an undeclared stream)
- `temp dir for object fetch: {e}` / `temp file for `{key}`: {e}` /
  `writing temp for `{key}`: {e}` / context `fetching `{key}`` (via classify_read_error)
- S3 literal head miss (no glob):
  `object `{pattern}` (s3 `{endpoint}` bucket `{bucket}`): not found`
- `s3 location `{endpoint}` bucket `{bucket}`: {e}` (build_store failure)

cursor/planner (source/cursor.rs):
- `unreadable file cursor: {e}`
- shrunk: `file `{path}` shrank or was rewritten (recorded {done} of {size} \ bytes, now {now}); refusing to read from a stale \ offset — clear it from the pipeline state or restore the file`
- etag rewrite: `file `{path}` was rewritten in place (same size, different etag); \ refusing to trust recorded progress — clear it from the \ pipeline state or restore the object`
- mtime rewrite: `file `{path}` was rewritten in place (same size, but modified \ since the last run); refusing to trust recorded progress — \ clear it from the pipeline state or restore the file`
- unterminated growth: `file `{path}` grew after a run that consumed an unterminated \ final line; the recorded offset {done} points mid-record — \ clear it from the pipeline state or restore the file`
- whole-file size change: `file `{path}` changed size ({old} → {new}) — whole-file formats \ (csv, compressed) never grow in place; deliver new data \ as a new file, or clear this file from the pipeline state`

jsonl (formats/jsonl.rs):
- tail-hash mismatch: `file `{path}` was rewritten before the resume offset (the content \ preceding byte {start} changed since the last run); refusing to \ read a stale tail — clear it from the pipeline state or \ restore the file`
- wrong-kind resume check (jsonl.rs:163) — DEFECT: the literal contains ~18
  embedded spaces mid-sentence (no `\` continuation); renders as
  `file `{path}` recorded a row-group integrity value but is being read as a record[18 spaces]stream; refusing to resume without a check this reader can evaluate —[18 spaces]clear it from the pipeline state` (§9-S2)
- `malformed JSON in `{path}` at byte offset {line_start_offset}: {e}`
- `opening `{path}`: {e}` / `reading `{path}`: {e}` / `seek `{name}`: {e}`

csv (formats/csv.rs):
- `reading CSV header of `{path}`: {e}`
- pass-1 parse: `malformed CSV in `{path}` at row {row+1}: {e}`
- pass-2 parse: `malformed CSV in `{path}`: {e}` (NO row — inconsistent with pass 1, §9-S7)
- width: `malformed CSV in `{path}` at row {row}: {n} fields, {m} columns`
- hint violation: `` `{path}` row {row} column `{col}`: value does not satisfy the \ declared {expected} hint`` (expected = bool|int64|float64|json)
- two-pass race: `` `{path}` row {row} column `{col}`: value no longer parses as the \ inferred {expected} — the file changed between the inference and \ conversion passes; retry the run``
- non-finite float: `` `{path}` row {row} column `{col}`: non-finite value \ `{cell}` has no JSON representation — declare a utf8 \ type hint to load it as a string``

parquet (formats/parquet.rs):
- `reading parquet `{path}`: {e}` (builder/footer)
- `corrupt parquet `{path}` (row group {group}): {e}`
- past-end: `file `{path}` records progress through row group {start} but now holds only {total} — \ refusing to resume past the end; clear it from the pipeline state or \ restore the file`
- footer not self-consistent: `parquet `{path}`: row group {last} has {what}; refusing to verify a resume \ offset against a footer that does not describe itself`
  (what ∈ `a negative page offset` | `a negative chunk size` | `an overflowing extent`)
- prefix window unreadable: `parquet `{path}`: the {window} bytes ending the consumed prefix could not be \ read ({e}); refusing to trust a resume offset the file no longer covers`
- rewritten prefix: `file `{path}` was rewritten before the resume offset (the content of the {n} row \ groups preceding it changed since the last run); refusing to read from a \ stale offset — clear it from the pipeline state or restore the file`

### 2.3 Destination-side messages (exact)

- Merge refusal (session.rs ensure_table):
  `file destination does not support Merge (capabilities.merge = false)`
- `write before ensure_table for `{table}``
- partition column missing: `partition_by column `{column}` does not exist in stream `{table}` \ (columns: {:?})` (Debug list of field names)
- `partition value at row {row}: {e}`
- commit-log future version (layout.rs): `commit log `{file}` format v{v} is newer than this build supports \ (v{LAYOUT_FORMAT_VERSION}); upgrade rdlt instead of resetting`
- non-UTF-8 walk (location/mod.rs): `output directory contains a non-UTF-8 name under `{dir}`; \ rename it or move it outside the destination`
- listing violation (tail_of_key): `listing returned key `{key}`, which is not under the prefix `{root}` it was listed by`
- `count_rows is synchronous; use count_rows_async for object stores`
- writer_props.rs: `` `compression_level` {level} is negative — {what} levels start at 0``;
  `` `compression_level` {level} is not valid for {what}: {e}``;
  `internal: {what} level requested without a value`;
  `compression `{name}` is not supported by the file destination` (non_exhaustive SPI codec refused by name, never silently uncompressed)
- All injected-crash messages: `injected crash at {point}`.

Capabilities (dest/mod.rs:119-128): `merge(false)`, `structs(true)`,
`scalar_lists(true)`, `json_type(false)`, `decimal(true)`,
`ident_rules(IdentRules::default())`. ConnectorSpec name `"file"` +
CARGO_PKG_VERSION, config_schema attached, BOTH halves.

### 2.4 Crash points — IDs, arming spelling, placement

Arming spelling everywhere: `crash_point!` (`rdlt_connector::core::crash_point`;
imported as `use rdlt_connector::core::crash_point;` in location/ and dest/,
fully-qualified `rdlt_connector::core::crash_point!` in source/mod.rs).
Registries (all `#[cfg(feature = "failpoints")] #[doc(hidden)]`):

`source::FAIL_POINTS = &["file.list", "file.read"]` (source/mod.rs:21)
- `file.list` — source/mod.rs:160, AFTER resolve_inputs (listing + any S3
  parquet up-front fetch), BEFORE plan_tasks.
- `file.read` — source/mod.rs:170, top of the per-task loop, each task.

`dest::FAIL_POINTS = &["pq.replace.truncate", "pq.staged.sync",
"pq.part.rename", "pq.dir.fsync", "pq.state.write", "pq.receipt.write"]`
(dest/mod.rs:56-63). "These spellings are frozen" — the ENGINE's crash sweep
drives them against `ParquetDir` (crates/rdlt-engine/tests/crash_sweep.rs:154,
213). Placement — LOCAL protocol only, gated by `filesystem_protocol()`:
- `pq.replace.truncate` — session.rs:159, before any deletion, once per load.
- `pq.staged.sync` — location/mod.rs:227, in publish_part local arm, before
  the staged file's `sync_all`.
- `pq.part.rename` — location/mod.rs:233, between fsync and `fs::rename`.
- `pq.dir.fsync` — session.rs:203, before the touched-directory fsync pass.
- `pq.state.write` — session.rs:237, before the state doc write.
- `pq.receipt.write` — session.rs:248, after state, before the commit-log write.

`dest::S3_FAIL_POINTS = &["file.stage.put", "file.finalize.copy",
"file.finalize.delete"]` (dest/mod.rs:67-71) — cannot fire on a local store:
- `file.stage.put` — location/mod.rs:190, S3 arm of stage_put, before the PUT.
- `file.finalize.copy` — location/mod.rs:240, before COPY staged→final.
- `file.finalize.delete` — location/mod.rs:247, between COPY and DELETE staged.

Registry-vs-sources pin: tests/sweep.rs:183 `the_registry_matches_the_sources`
runs `rdlt_testkit::assert_registry_matches_sources(src, &[dest::FAIL_POINTS,
dest::S3_FAIL_POINTS, source::FAIL_POINTS])` — THREE registries over one tree,
checked against their union (024 rule: never "simplify" this).

---

## 3. THE CURSOR RULEBOOK (source)

### 3.1 Persisted cursor JSON — FROZEN wire shape

`FileCursor` (source/cursor.rs): `CURSOR_FORMAT_VERSION = 1`.
```json
{
  "format_version": 1,          // u32, serde default = 1
  "files": {                    // BTreeMap<String path, FileProgress>
    "<path>": {
      "done": <u64>,            // Rust: done_units  — #[serde(rename = "done")]
      "size": <u64>,            // Rust: size_units  — #[serde(rename = "size")]
      "eol":  true,             // Rust: ended_at_record_boundary — rename "eol", default = true
      "mtime_ms": 1750000000000,        // Option<u64>, #[serde(default)] — SERIALIZED (null when None)
      "etag": "...",            // Option<String>, default + skip_serializing_if None
      "tail_hash": "...",       // Option<String>, default + skip — blake3 hex, jsonl only
      "row_groups_hash": "..."  // Option<String>, default + skip — parquet only, additive at v1
    }
  }
}
```
Wire keys `done`/`size`/`eol` frozen (WR1); Rust names renamed on top. Neither
struct denies unknown fields (forward-compat both ways). Pre-tripwire docs
(no eol/mtime) decode with defaults (pinned: cursor.rs test
`decodes_pre_tripwire_cursors_with_defaults`; tests/preservation.rs PRE_015_CURSOR).
Encode: `Cursor::new(serde_json::to_value(self).expect("cursor serialization"))`.
Decode of None → empty v1 cursor; parse failure → `unreadable file cursor: {e}`.

Unit polymorphism: `done_units`/`size_units` are BYTES for plain jsonl, ROW
GROUPS for parquet, whole-file BYTES for csv/compressed. Complete ⇔
`done_units == size_units`.

RETENTION: entries retained for the LIFE of the pipeline state — deliberately
never pruned (a pruned path that reappears re-reads from zero and DUPLICATES
under Append). ~150-250 bytes/path; operator levers: narrow the pattern or
clear state. Pinned by `every_path_ever_recorded_is_retained`.

### 3.2 Listing semantics (complete-or-fail)

Local (`resolve_files`, source/mod.rs:59-111): a path naming an EXISTING file
is ALWAYS literal, even with glob metacharacters (`events[prod].jsonl`); else
if pattern contains `*?[` → glob::glob, only `is_file()` entries; glob
expansion error (unreadable dir) → fatal "partial file list" refusal; an
existing DIRECTORY → typed "is a directory" error suggesting `{pattern}/*.jsonl`;
else missing-file error. Result sorted lexicographically; each stat'd for
size + mtime_ms (ms since epoch); etag None.

S3 (`S3Location::list`, s3.rs:325-376): ONE HEAD decides literal-vs-glob (same
rule: existing object with metacharacters is a key, not a character class).
Miss without glob → typed not-found; with glob → `glob::Pattern` with
`require_literal_separator: true` (`*`/`?` NEVER cross `/` — staged keys under
`.rdlt-staging/` must never match a data glob). Listing prefix =
`prefix_of(pattern)` (chars before last `/` preceding first metachar);
CONTINUATION PAGES FULLY DRAINED or typed failure. Meta: size bytes, etag from
listing, mtime_ms ALWAYS None on S3 (etag is the S3 rewrite tripwire). Sorted
lexicographically. object_store prefix listing is SEGMENT-scoped (listing `a`
never returns `ab/*`) — pinned by tests/prefix_semantics.rs.

Snapshot ONCE per run: stable list; files created after listing arrive next run.

### 3.3 Planning (delta detection) — two planners

`plan` (byte/row-group tails — plain jsonl, parquet), per matched file:
1. no recorded entry → `FileTask::fresh` (start 0, no check);
2. `meta.size < recorded.size || recorded.done > meta.size` → shrunk error;
3. `rewritten_in_place` tripwire: SAME size AND (etag pair present+different
   OR mtime pair present+different) → typed rewrite error;
4. `done < size`: if `!eol` → unterminated-growth error; else resume task at
   `start = done` with `resume_check` derived from the record (only when
   done > 0);
5. `done == size` (+ tripwires silent) → skip.

`plan_whole` (csv + compressed jsonl): ANY size change → typed error (whole-file
formats never grow in place); rewritten_in_place same; incomplete
(`done < size`) → re-read WHOLE from zero (crash re-delivery; exactly-once
under keyed merge/dedup — documented); complete → skip.

jsonl glob may match plain AND compressed files: partitioned by `codec_of`,
each follows its own rule, tasks re-sorted by path (plan_tasks, source/mod.rs:331-349).

### 3.4 TAIL-HASH resume integrity

`TAIL_WINDOW = 4096` bytes. Record streams (plain jsonl): every checkpoint
records `tail_hash` = blake3 hex of the last `min(done_units, TAIL_WINDOW)`
consumed bytes (rolling buffer `roll_tail`). On resume with
`ResumeCheck::TailBytes {window = min(done, 4096), hash}`: open at
`start - window`, read exactly `window` bytes from the SAME reader, compare
count AND blake3; mismatch → typed "rewritten before the resume offset" error;
match → continue from the open reader (no reopen). A genuine append resumes; a
grown REWRITE fails loudly on BOTH location kinds.

Parquet analogue: `row_groups_hash` = `prefix_digest` (parquet.rs:78-109) =
blake3 over (every column's path string + physical_type Debug + logical_type
Debug, NUL-separated) + `groups.to_le_bytes()` + the last
`min(end_of_prefix, 4096)` BYTES of the consumed prefix (content, not layout —
footer quantities are position-determined; schema folded in because a rename
changes what the prefix MEANS). `end_of_prefix` = max over chunks of
(dictionary_page_offset|data_page_offset + compressed_size), CHECKED arithmetic
(footer is untrusted input; `byte_range()` deliberately not used — it asserts).
Recorded per row-group checkpoint; verified before resuming. `start > total_groups`
→ typed past-end refusal; `start == total_groups` reads nothing (not an error).

`resume_check_for` (cursor.rs:185-202): record holding BOTH hashes → None
(written by two different readers; verifying either would verify the wrong
thing); one → the matching variant; none → None (first-upgrade resume carries
no check). A RowGroupPrefix check reaching the RECORD reader is REFUSED, not
ignored (jsonl.rs:158-167). done_units == 0 arms no check.

### 3.5 Read loop mechanics + checkpoints

Plain jsonl (`read_task`): SlabReader assembles slabs of COMPLETE lines
(8 MiB target; a longer line grows the buffer until its newline/EOF; tail after
last newline carries over; EOF emits the final unterminated line). Optional
`validate_lines` skim-parse (`serde_json::from_slice::<IgnoredAny>` per
non-whitespace line) fails naming file + LINE-START byte offset. Push =
`out.raw_json(Bytes::from(slab))` zero-copy; closed channel → `Ok(false)` =
cancellation (never an error). After EVERY slab: `cursor.record` (done=offset,
size=max(listing,offset), eol = slab ends with `\n`, mtime/etag from task,
tail_hash) then `out.checkpoint(cursor.encode())` — checkpoint covers exactly
the rows pushed before it. Final completion record: done=offset,
size=`offset.max(task.start)` (see §9-S3), then checkpoint.

Compressed jsonl (`read_task_whole`): decode through codec (not seekable),
same slab/line discipline, offset counts DECOMPRESSED bytes for error context,
ONE completion checkpoint (done=size=task.size_units, eol=true, tail_hash None).

CSV (`csv::read_task`): pass 1 infer + headers; pass 2 convert to NDJSON into
slab, flush at SLAB_BYTES via raw_json; ONE completion checkpoint
(whole-file unit, tail_hash None).

Parquet (`parquet::read_task`): per remaining row group, a FRESH
ParquetRecordBatchReaderBuilder `.with_row_groups(vec![group])` (each group
independently readable; footer re-parse is microseconds); batches pushed via
`out.arrow(batch)` (STRUCTURED passthrough, run-level provenance only — spec
`with_structured()`; jsonl/csv get `with_primary_key` + `with_type_hint`);
checkpoint per row group with row_groups_hash.

### 3.6 Object-store staging of reads

S3 parquet: fetched UP FRONT to temp files (correctness over streaming) during
resolve_inputs; cursor stays keyed by the OBJECT key; `FileTask.read_path`
points at the temp copy. Skip-fetch optimisation (`recorded_completion`,
source/mod.rs:319-326): an object whose recorded progress is COMPLETE and whose
etag is PRESENT AND EQUAL on both sides is not downloaded — its unit count is
recovered from the record; conservative in three ways (both etags present;
progress complete; everything else falls through to fetch + ordinary
tripwires). Proven engaged by the `SKIPPED_FETCHES` relaxed counter +
`skipped_fetches()` doc(hidden) accessor.
S3 csv + compressed jsonl: fetched POST-plan (only non-skipped tasks) in
`stage_s3_fetches`. Plain S3 jsonl streams directly (range GET
`GetRange::Offset(start)`).
Temp dirs: `std::env::temp_dir()/rdlt-file-{pid}-{seq}-{stream}` (process-wide
AtomicU64 seq — concurrent in-process pipelines never share);
`FetchDir` removes itself on Drop (every exit incl. error/cancel), best-effort.

---

## 4. THE DEST PROTOCOL

### 4.1 Layout vocabulary — FROZEN (dest/layout.rs; WR1)

- `LAYOUT_FORMAT_VERSION = 1`
- `STAGING_DIR = ".rdlt-staging"` (location/mod.rs:24)
- pipeline scope: `ident_hash(pipeline.as_str(), 12)` (12-hex; SPI naming)
- state file: `_rdlt_state.{scope}.json`
- commit log: `_rdlt_commits.{scope}.json`
- staging tail: `.rdlt-staging/{scope}/{load}/{name}`
- staged part NAME: `{load}-{table}-{slug}-{index}.{extension}` where slug =
  partition value (path-safe) or `all`; index counted PER (table, partition)
- final tail: `{table}/part-{load}-{seq}-{index}.{ext}` or
  `{table}/{partition}/part-{load}-{seq}-{index}.{ext}` — `n` per
  TABLE+PARTITION so cross-table arrival order cannot change a final name
  (pinned: tests/recovery.rs `final_names_independent_of_cross_table_arrival_order`)
- `path_safe(value)`: keep ascii-alphanumeric + `-_.`, else `_`; empty result →
  `__empty__`; NULL partition → `__null__` (applied at split time, stored
  sanitized). Partition dirs are BARE values, not Hive `col=value`.
- CommitLog JSON: `{"format_version": u32 (default 0), "receipts": [["<load_id>", <seq>], ...]}`
  — receipts retained for the LIFE of the destination, deliberately (the SPI
  commit contract is UNCONDITIONAL; trimming re-truncates Replace targets on a
  redelivered trimmed load — bounding needs a persisted watermark + typed
  refusal, "a design, not a trim"). Future version (`>` strictly) → typed
  upgrade-not-reset error; v0 (absent, pre-versioning) accepted.

### 4.2 Session lifecycle

`open(ctx)`: compute scope; `prepare_staging(scope, load_id)` — clause D4:
LOCAL removes `.rdlt-staging/{scope}` entirely then creates `{scope}/{load}`;
S3 deletes every key under `.rdlt-staging/{scope}`. Scoped: a SIBLING pipeline
sharing the output keeps its staged data (pinned:
`open_does_not_destroy_another_pipelines_staging_or_state`). Writer properties
resolved ONCE per session (translation can fail; a load must not get halfway
in before finding out).

`ensure_table`: Merge → typed refusal; local → `create_dir_all(root/table)`;
records (schema, mode) in `tables`.

`write(table, batch)`: refuse before ensure. `split_partitions`: no
partition_by → one (None, batch) group; else locate column (typed if missing),
ONE `ArrayFormatter` for the column (`FormatOptions::default().with_display_error(true)`,
matching `array_value_to_string` — hoisted out of the row loop, 020 D-41),
group row indices by path-safe rendered value (BTreeMap = deterministic order),
`take_record_batch` per group. Encode per format: parquet = `ArrowWriter` with
session WriterProperties into Vec<u8> (one part per batch×partition); jsonl =
`arrow::json::LineDelimitedWriter`. part_index = count of already-staged parts
with same (table, partition). `stage_put` (local: `fs::write`; S3: PUT, crash
point first). Staged names deterministic so crash-recovery WAL replay
reproduces both staged and final names identically.

### 4.3 Commit — four named phases (session.rs:318-358)

Read commit log (verbatim bytes → serde), `check_readable`, key =
(load_id, commit_seq).
1. REPLAY DEDUP (D3): `log.receipts.contains(&key)` → discard this session's
   staged parts (best-effort remove) and return the prior receipt WITHOUT
   republishing. Pinned by the PLANTED commit-log fixture
   (tests/preservation.rs `pre_015_commit_log_fixture_drives_receipt_dedup`:
   plant `{"format_version": 1, "receipts": [["load-x", 1]]}`, commit
   ("load-x",1), assert 0 rows published) and
   `a_redelivered_commit_is_recognised_after_later_loads_have_run`.
2. REPLACE TRUNCATION, guarded DURABLY: "has any earlier commit of THIS load
   landed?" read from the RECEIPT LOG, not session memory —
   `log.receipts.iter().any(|(load, _)| load == meta.load_id)`; if none,
   truncate every Replace-mode table ONCE per load (a crash-recovery session
   never re-truncates files a prior commit of this load published; if no
   receipt landed, re-truncating is convergent under WAL re-delivery).
3. PUBLISH: each staged part → deterministic final name via
   `Location::publish_part` — LOCAL: create parent dirs, crash `pq.staged.sync`,
   open+`sync_all` staged file, crash `pq.part.rename`, `fs::rename` (per-file
   atomic); S3: crash `file.finalize.copy`, COPY staged→final, crash
   `file.finalize.delete`, idempotent DELETE staged (NotFound = success — a
   replayed finalize tolerates a missing staged object). Then LOCAL-only D2
   durability: crash `pq.dir.fsync`, fsync every touched partition dir AND
   table dir (deduped). NO set-atomic multi-file publish; recovery converges
   because names are deterministic.
4. RECORD: crash `pq.state.write`; write state doc `_rdlt_state.{scope}.json`
   (= `meta.state`, StateDoc); set log.format_version = 1, push receipt; crash
   `pq.receipt.write`; write commit log. Receipt lands LAST = the durable
   idempotency guard. Local doc writes are atomic-durable:
   temp (`.json.tmp` via with_extension) + fsync + rename + parent-dir fsync
   ("metadata must not be LESS durable than the parquet parts it describes");
   S3 doc writes = single PUT of `serde_json::to_vec_pretty`.

`read_state(pipeline)`: read state file for THAT pipeline's scope; parse;
`filter(|s| &s.pipeline == pipeline)` (scope-hash collision safety net).

### 4.4 Replace truncation — ownership precision (dest/truncate.rs)

Two rules, UNION, over the ONE `Location::keys_of_table` listing (local and S3
cannot diverge):
- OWNED-PARTS rule (ALWAYS): tail depth 1 or 2 (one partition dir max) whose
  file name has the EXACT written shape `part-<load>-<seq>-<index>.<ext>` with
  `<ext>` ∈ `DestFormat::ALL` extensions ({parquet, jsonl} — completeness
  enforced by construction: `in_all` exhaustive match breaks compilation on a
  new variant), numeric nonempty seq+index, nonempty load remainder. Reads NO
  configuration — a load that switched format or dropped partition_by still
  clears its predecessors. A bare `part-` prefix is NEVER enough:
  `part-0.parquet` / `part-00000-<uuid>-c000.snappy.parquet` are the default
  basenames of pyarrow/Spark/Hive/Delta — a prefix test would delete a user's
  dataset (pinned: `a_foreign_dataset_is_never_ours_to_delete`,
  `replace_never_deletes_a_foreign_dataset`, s3_live
  `s3_replace_never_deletes_user_files`).
- FROZEN plain-parquet rule (ADDED only when `location.is_local() && format ==
  Parquet && partition_by.is_none()`): TOP-LEVEL `*.parquet` of ANY name —
  exact pre-015 behavior. Local-only by its stated scope; on object stores a
  user-placed *.parquet under the prefix is never ours.
`every_name_this_destination_writes_is_owned` builds names from `final_tail`
itself so writer and rule cannot drift.

Ownership listing: `keys_of_table` — local: recursive walk, symlinks SKIPPED
(never staged by us; descending one lets truncation unlink outside the root),
non-UTF-8 names TYPED error; S3: list under `{prefix}/{table}`, strip the
EXACT root (`tail_of_key` — STRIPPED never searched: a partition value can
spell the table's own name; a key outside the prefix is a TYPED listing
violation, not skipped — skipping under-reports ownership and Replace leaves
data behind; the ONE non-violation: a zero-byte directory MARKER whose key is
the table root itself → skipped `Ok(None)`).

### 4.5 Inspection (dest/inspect.rs)

`count_rows(table)` sync, local only (typed refusal on S3);
`count_rows_async(table)` both. Count over the ownership listing:
`.parquet` → footer num_rows; `.jsonl` → newline count; anything else 0.
Partitions included, staged excluded.

### 4.6 writer_props (dest/writer_props.rs)

THE one boundary turning SPI `ParquetOptions` into the parquet crate's
`WriterProperties` (Principle III). Sets compression (+ level via
GzipLevel/BrotliLevel::try_new(u32), ZstdLevel::try_new(i32 — zstd defines
negative fast levels)), dictionary_enabled, dictionary_page_size_limit; sets
data_page_size_limit only when Some; sets max_row_group_row_count ONLY when
Some (the setter's `None` means UNLIMITED, not default — passing through would
replace the 1,048,576-row default; and it panics on Some(0), refused earlier).
Defaults produce SNAPPY-compressed output (pinned — the library default is
uncompressed: 210 MB vs dlt's 74).

---

## 5. THE LIBRARY BOUNDARY

Libraries behind boundaries:
- `object_store` (workspace pin, `aws` feature) — used ONLY in
  `location/{mod,s3}.rs` plus the tests. APIs: `AmazonS3Builder` (endpoint,
  bucket_name, region default "us-east-1", access_key_id/secret_access_key via
  `Secret::reveal()` — "Secrets are revealed HERE only",
  `with_virtual_hosted_style_request(!path_style)`, `with_unsigned_payload`,
  `with_allow_http(true)` ALWAYS — §9-S8); `store.head`, `get`,
  `get_opts(GetOptions{range: GetRange::Offset(start)})` + `into_stream`,
  `put`, `copy`, `delete`, `list(Some(prefix))` (futures::StreamExt drained
  fully). Error wrapping: every `object_store::Error` passes through
  `store_err` (write half) or `classify` (read half), both consulting the
  SPI's `rdlt_connector::store::is_recoverable` — the SINGLE recoverability
  decision. Mid-stream errors carried through the io seam with a retryable
  ErrorKind. The SPI dep declares features `["schema", "object-store"]`
  (Secret/schemars + the store module); the crate ALSO depends on object_store
  directly (workspace-pinned same version).
- `parquet` — formats/parquet.rs (SerializedFileReader,
  ParquetRecordBatchReaderBuilder, ParquetMetaData) + dest session
  (ArrowWriter) + writer_props (WriterProperties, levels) + inspect (footer
  num_rows). Errors → fatal with file named.
- `arrow` — session (json::LineDelimitedWriter, util::display::ArrayFormatter/
  FormatOptions, compute::take_record_batch, UInt32Array), e2e tests.
- `csv` (ReaderBuilder: delimiter/quote as u8, has_headers,
  `flexible(false)`), `flate2` (GzDecoder), `zstd` (stream Decoder), `glob`
  (local walk + S3 Pattern matching), `memchr` (memrchr/memchr_iter newline
  scan), `blake3` (tail + prefix digests), `bytes`, `futures`.
Full dep list §8.

---

## 6. TESTS CENSUS

`env -u RUSTUP_TOOLCHAIN cargo nextest list -p rdlt-connector-file` → 109
tests listed (default features); + 3 in `sweep` (compiled only with
`--features failpoints`; `#![cfg(feature = "failpoints")]`). Per binary:

| binary | tests | covers |
|---|---|---|
| lib (unit) | 36 | layout commit-log version pin (1); truncate ownership rules (6); writer_props translation (6); csv two-pass bool (1); location tail_of_key (4); location validation + secrets + shared recoverability rulebook (4); cursor planner/roundtrip/retention/pre-tripwire decode (6); skip-fetch decision matrix (5); temp-dir uniqueness/self-removal (2); a few more in source (1) |
| config_schema | 5 | generated schema ⇄ parser agreement both halves; documented example; unknown-field parity; dest corpus; the FileDestConfig field-set pin `["format","location","parquet","partition_by","path"]` |
| conformance | 1 | testkit source conformance suite over a 3-file jsonl glob |
| csv | 13 | lattice/nulls; hints + typed violations; options matrix (delimiter/header/quote); header-only empty; malformed rows; non-finite floats; csv-block-requires-csv-format; codec mismatch magic; compressed csv + jsonl whole-file; compressed-parquet parse refusal; directory-vs-missing error |
| dest_conformance | 2 | testkit destination conformance over ParquetDir; merge capability rejection at plan time |
| dest_options | 7 | dest config typed validation; jsonl parts; partition split + missing column; Replace clears other-shape predecessors / spares user files / never deletes foreign datasets |
| e2e_copy | 1 | parquet → parquet through the engine (structured passthrough), type preservation + resume |
| e2e_duckdb | 1 | jsonl → DuckDB through the engine, incremental across two runs |
| jsonl | 12 | US1 acceptance: glob load order, resume tail-only, unchanged-run reads nothing, tail-hash grown-rewrite, same-size rewrite, shrunk, unterminated growth, malformed line naming file+offset, empty glob success, literal path w/ metacharacters, missing named file, unreadable-dir partial-list refusal — driven directly through the SPI records channel |
| parquet_resume | 6 | resume integrity: genuine append resumes; rewritten/re-encoded/prepended/schema-changed prefixes refused; hashless cursor recovers rather than poisons |
| prefix_semantics | 1 | object_store list-prefix is segment-based (InMemory probe) — the assumption per-table listing rests on |
| preservation | 4 | THE WELD PROOFS (015 T004/FF1): committed pre-015 cursor doc (`done`/`size`/`eol` keys) parses + plans identically; artifact names + commit-log shape frozen (part-load-a-1-0.parquet, `_rdlt_state.{scope}.json`, receipts `[["load-a",1]]`); planted pre-015 commit log drives receipt dedup; pre-015 config spellings parse |
| recovery | 4 | Replace once-per-load durable guard across crash-recovery sessions; redelivered commit recognised after later loads; final names independent of cross-table arrival order; sibling-pipeline staging/state isolation |
| s3_live | 16 | container-gated RUSTFS cells (skip-not-fail): deterministic seeded listing + pagination; glob no-`/`-crossing; literal metachar key; missing-object typed / empty-prefix success; range read; tail-hash + etag rewrites typed; engine loads jsonl + parquet + quickstart csv.gz-with-hints delta; dest atomic publish, jsonl partitions, replace user-file safety; unreachable endpoint + wrong credentials typed |
| sweep | 3 (failpoints) | source points × 3 actions (`return`, `panic`, `1*off->return`) local jsonl→duckdb, armed-fire pin + exactly-once (TOTAL_ROWS=4); S3_FAIL_POINTS × 3 container-gated against the bucket; registry-vs-sources scan |

RUSTFS container mechanics (tests/common/s3.rs): image
`docker.io/rustfs/rustfs` TAG PINNED `1.0.0-beta.11` (floating latest rejected —
bump deliberately with live cells green); exposed port 9000 tcp, host port
dynamic via testcontainers (podman socket); `WaitFor::message_on_stdout("Starting")`;
env `RUSTFS_ACCESS_KEY=rdlt-test-access`, `RUSTFS_SECRET_KEY=rdlt-test-secret`;
bucket `raw`; label `rdlt_testkit::gate::RECLAIM_LABEL` (chained LAST —
with_label returns ContainerRequest). Skip-not-fail via
`rdlt_testkit::gate::runtime_available()`; eprintln
`SKIP: no container runtime socket — s3 live cell not run`. Health check = a
signed HEAD retried 100×150ms (NotFound counts as ready). Bucket created by a
hand-rolled SigV4 PUT in PYTHON3 (object_store has no bucket admin op) — a
python3 runtime dependency of the fixture (never product code). Seed `put`
retried 3× with backoff. Helpers: `location_options()`, `location()`,
`location_yaml()`.

Committed fixtures are INLINE consts (no fixture files): PRE_015_CURSOR JSON
(preservation.rs:15-24) and the planted commit-log bytes
`{"format_version": 1, "receipts": [["load-x", 1]]}` (preservation.rs:145).

Sweep shape + Makefile: `make test TARGET=sweep` includes line 123:
`cargo nextest run -p rdlt-connector-file --features failpoints -E 'binary(sweep)'`.
The dest's LOCAL points are swept by the ENGINE's sweep
(crates/rdlt-engine/tests/crash_sweep.rs:154/160/213 — drives
`rdlt_connector_file::dest::FAIL_POINTS` against ParquetDir;
rdlt-engine dev-deps rdlt-connector-file with failpoints). No nextest group
membership for this crate (.config/nextest.toml has only iceberg-live).
Fuzz (Makefile:65 FUZZ_TARGETS): `cursor_decode` (FileCursor decode/encode
roundtrip stability), `file_config` (FileSource::from_yaml never panics).

---

## 7. CONSUMERS — exact locations

- Workspace: root Cargo.toml members; version/edition/lints from workspace.
- Facade `crates/rdlt/src/lib.rs`: 53 `pub use rdlt_connector_file as file;`
  62 `pub use rdlt_connector_file as parquet;` (alias so
  `rdlt::connector::parquet::ParquetDir` keeps resolving).
- Facade features `crates/rdlt/Cargo.toml`: 21 `file = ["dep:rdlt-connector-file"]`,
  28 `parquet = ["file"]` (FROZEN feature spelling for build scripts — 015 FF1
  comment), 37 optional dep.
- `crates/rdlt/src/pipeline_spec.rs`: ~85 `SourceSpec::File { config }` (path
  to source doc; .json → from_json else from_yaml, 343-350); ~173
  `DestSpec::Parquet { path }` → `ParquetDir::open` (451-455, error
  `opening parquet dir: {e}`); ~192 `DestSpec::File(Box<FileDestConfig>)`
  EMBEDDED config (457-460, error `file destination: {e}`).
- `crates/rdlt-cli/src/main.rs`: spec-level tests referencing
  `DestSpec::Parquet` (540-546) and the full `destination: file:` vocabulary
  incl. `DestFormat::Jsonl` (558-590); CLI itself consumes via the facade.
- `crates/rdlt-engine`: dev-dep with failpoints (Cargo.toml:53);
  tests/crash_sweep.rs 154/160/213-220 sweep + registry over
  `dest::FAIL_POINTS`.
- Makefile: 123 (crate sweep line), 65 (FUZZ_TARGETS: cursor_decode,
  file_config), 285-291 (whole-workspace nextest run includes this crate's
  binaries; only the snowflake sweep is excluded).
- Bench: `benches/cells/e2e.toml` cells `pg-to-s3parquet-1m` (line 40),
  `s3jsonl-to-pg-200k` (77), `s3jsonl-to-s3parquet-200k` (99) via
  `benches/cells/pipelines/{pg-to-s3parquet,s3jsonl-to-pg,s3jsonl-to-s3parquet}.yaml`
  (pipeline documents using the file source/dest vocabulary + the bench MinIO
  at 127.0.0.1:19110).
- Fuzz: `fuzz/Cargo.toml:17`; `fuzz/fuzz_targets/file_config.rs`,
  `fuzz/fuzz_targets/cursor_decode.rs`.
- `rdlt-testkit`: NOT a consumer of this crate (this crate dev-deps testkit).

---

## 8. DEPENDENCIES (crates/rdlt-connector-file/Cargo.toml, all workspace-pinned)

[dependencies] `rdlt-connector` (features **["schema", "object-store"]**),
`arrow`, `blake3`, `schemars`, `async-trait`, `bytes`, `memchr`, `glob`, `csv`,
`flate2`, `zstd`, `object_store`, `futures`, `parquet`, `serde`, `serde_json`,
`serde_yaml`, `thiserror`, `tokio`.
[dev-dependencies] `jsonschema`, `object_store`, `testcontainers`,
`rdlt-testkit`, `rdlt-engine`, `rdlt-connector-duckdb`, `tempfile`.
[features] `failpoints = ["rdlt-connector/failpoints"]`. [lints] workspace.
Package: description "rdlt file connectors: JSONL/Parquet/CSV source and
destination over local or S3 locations, with resumable cursors"; keywords
elt/parquet/jsonl/csv/s3; categories database/encoding.

NOTE for the rewrite: under the 027 one-dependency rule the v2 crate depends
on `rdlt-connector-sdk` alone (SPI via its `spi` re-export; failpoints/schema/
object-store forwarded by the sdk). Gen 1 predates the sdk entirely — no
`config::Document`, hand-rolled from_yaml/from_json/from_value triple, direct
`Source`/`Destination` SPI impls, session choreography (write-before-ensure
refusal, existing_receipt→replay→publish) hand-implemented in commit().

---

## 9. SUSPICIOUS ITEMS (candidate inherited defects — review-loop input, NOT fixed)

S1. **Duplicate stream names are not refused** — src/source/config.rs:122-162
    validates per stream but never uniqueness; `stream_config`
    (src/source/mod.rs:48-50) takes the FIRST match, silently shadowing the
    second. Two streams may also share one physical table name at the dest.
    029's review found the analogous shared-table case to be SILENT CORRUPTION
    and refused it at the config gate.
S2. **Malformed message literal with embedded space runs** —
    src/formats/jsonl.rs:163: the RowGroupPrefix-on-record-reader refusal
    string was joined without `\` continuation; the rendered message contains
    two runs of ~18 literal spaces mid-sentence (verified with cat -A).
S3. **Mid-run truncation marked complete** — src/formats/jsonl.rs:241-253: the
    completion record after the read loop uses `size_units: offset.max(task.start)`
    with `done_units: offset`. A file that SHRANK between listing and read hits
    EOF early and is recorded done==size (complete); the next run sees recorded
    size == current size and SKIPS — bytes between the observed EOF and the
    listing size are never read and no tripwire fires (the shrink tripwire
    compares against the RECORDED size, which was already capped down).
S4. **`path_safe` collisions silently merge partitions** —
    src/dest/layout.rs:68-84: distinct values `a/b`, `a_b`, `a?b` all sanitize
    to `a_b`; rows from different partition values land in one directory with a
    SHARED part-index sequence; nothing refuses or disambiguates. Similarly
    `__null__`/`__empty__` collide with real values spelled that way.
S5. **Partition rendering swallows formatter errors as text** —
    src/dest/session.rs:103 `.with_display_error(true)`: an arrow formatting
    error renders as an error STRING which then becomes a partition VALUE
    (path_safe'd), not a typed failure.
S6. **`prepare_staging` reclaims the whole pipeline scope on open** —
    src/location/mod.rs:161-178: two CONCURRENT sessions of the SAME pipeline
    (two engines, one output) destroy each other's staging; the rule assumes
    dead sessions only. Analogue of 028/029 coexistence findings; at minimum a
    standing owner record.
S7. **CSV row-number context inconsistent between passes** —
    src/formats/csv.rs:114-119 (pass 1: `at row {row + 1}` — off-by-one naming
    relative to pass 2's hint/two-pass errors which use `{row}` after
    increment) vs csv.rs:148 (pass 2 malformed: NO row at all).
S8. **`with_allow_http(true)` unconditionally** — src/location/s3.rs:144: plain
    HTTP endpoints always accepted, even with `unsigned_payload: true`, whose
    doc says that combination removes the last integrity check. No refusal or
    warning ties the two.
S9. **`type_hints` and `validate` accepted-and-ignored off their formats** —
    src/source/config.rs:46-59 documents both (parquet hints; csv/parquet
    validate) as ACCEPTED AND IGNORED rather than refused — contrary to the
    crate's own refuse-don't-ignore convention (csv block and parquet
    primary_key ARE refused). Candidate for typed refusal in v2 (behavior
    change — owner call).
S10. **S3 rewrite tripwire has no mtime leg** — src/location/s3.rs lists and
    heads never populate `mtime_ms` (etag-only), and HEAD (literal path,
    s3.rs:332-339) returns `mtime_ms: None` — fine where etags exist, but an
    S3-compatible store that omits etags (both sides None) has NO same-size
    rewrite detection at all: `rewritten_in_place` needs both sides present.
S11. **Local read taken from `path` not re-verified against listing size for
    whole-file formats mid-run** — plan_whole plans from the snapshot, but
    csv/compressed reads reopen the live file (local): a file replaced between
    listing and read with SAME size passes silently (mtime recorded at listing
    is stamped into progress, so the NEXT run trips, but this run loads the
    new content against the old file's plan).
S12. **`count_rows`/`count_rows_async` count files the destination does NOT
    own** — src/dest/inspect.rs:17-26 counts ANY `.parquet`/`.jsonl` under the
    table dir (user files included), unlike truncation's ownership rules.
    Test-helper only, but conformance probes ride it.
S13. **jsonl completion checkpoint duplicates the last per-slab record** —
    src/formats/jsonl.rs:241-256: after the final slab the completion record +
    checkpoint re-emit an identical cursor (harmless but every load ends with
    a redundant checkpoint; the rewrite can decide deliberately).
S14. **SKIPPED_FETCHES is process-global and monotonic** —
    src/source/mod.rs:291: shared across concurrent in-process pipelines;
    `skipped_fetches()` readings interleave across tests/pipelines.

Standing (documented, deliberate — carry as records, not defects): unbounded
cursor retention (§3.1); unbounded commit-log growth (§4.1); whole-file crash
re-delivery = exactly-once ONLY under keyed merge/dedup (Append duplicates);
12-hex `ident_hash` pipeline scope (029 N-series analogue); no set-atomic
multi-file publish (convergence via deterministic names).
