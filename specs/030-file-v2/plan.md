# 030 — FILE FAMILY SECOND GENERATION (`rdlt-connector-file-v2`)

Owner goal: "design and plan and rewrite rdlt-connector-file in
rdlt-connector-file-v2 (greenfield/clean layout/from scratch clean
implementation) — similarly current to postgres/rest."

Branch `030-file-v2` off main @ 8a10d0fe (001-029 merged: the sdk trio
plus the snowflake and iceberg second generations are all live).

THE DISCIPLINE (binding, learned across 025/026/028/029): TRUE
greenfield — generation 1 is the reference implementation for its
CONTRACT only; near-verbatim transcription is copying and is rejected
(memory `rdlt-rewrite-means-no-copying`). Frozen spellings are
identical because they are contracts. The review loop afterward treats
"found an inherited defect" as its success condition.

## The authoritative contract inventory

`specs/030-file-v2/contract-inventory.md` (committed; an exhaustive
read of generation 1 at 8a10d0fe) is D3's substance: both halves'
config vocabulary with exact spellings, the frozen message inventory,
the cursor rulebook with the PERSISTED v1 wire keys, the destination's
4-phase commit protocol and staging/receipt layout, crash points,
tests census (109 default + 3 sweep across 15 binaries), consumers,
dependencies, and 14 suspicious items reserved for the review loop.

THE LOAD-BEARING FACTS the design bends around:
- The source's resume integrity is the TAIL-HASH rulebook: persisted
  cursor v1 with FROZEN wire keys (`done`/`size`/`eol` + additive
  `mtime_ms`/`etag`/`tail_hash`/`row_groups_hash`), a 4096-byte blake3
  tail verification before trusting any offset, whole-file planning
  for csv/compressed, and S3 skip-fetch only on provably-unchanged.
- The destination is a REAL direct-to-storage exactly-once: staging
  under `.rdlt-staging/{scope}/{load}/`, a durable receipt log
  (`_rdlt_commits.{scope}.json` — the planted-commit-log weld proof),
  state in `_rdlt_state.{scope}.json`, once-per-load Replace
  truncation with the ownership-precise shape rule, and a 4-phase
  commit (replay dedup → truncate → publish → state-then-receipt).
- `ParquetDir` is a frozen ALIAS of the destination; the facade keeps
  `DestSpec::Parquet{path}`, the `parquet = ["file"]` feature, and the
  `rdlt::connector::parquet` module alias.

## Decisions

**D1 — Born on the sdk, BOTH halves.** `SourceConnector` + `Feed` and
`DestinationConnector` + `Backend` in one crate (the postgres shape):
`source::Shell` and `destination::Shell` aliases; SPI only via
`rdlt_connector_sdk::spi`; the one-dependency rule (sdk alone —
`object_store` arrives through the sdk's `object-store` forward, no
sqlcore, external deps for formats/compression as gen 1). sdk
`test_dependency_rule` gains
`("rdlt-connector-file-v2", &["rdlt-connector-sdk"])`.

**D2 — Typed ConfigError** with generation 1's frozen framings and
validation spellings (the inventory quotes all of them); the Document
gate is the only parse path; `config_schema()` for both halves from
the same structs.

**D3 — Frozen surfaces.** The contract inventory in full. Notably: the
persisted cursor v1 wire keys and their semantics (a PERSISTED DATA
format — the hardest freeze in the crate); the staging/receipt/state
file names and layout incl. `ident_hash(pipeline, 12)` scoping; final
part naming `{table}/[{part}/]part-{load}-{seq}-{index}.{ext}`;
partition directory encoding (bare values, NULL → `__null__`); the
truncation shape rule UNION frozen local top-level `*.parquet`; every
crash-point id and placement (`file.list`/`file.read`, the six `pq.*`
points the ENGINE's sweep owns, the three S3 points the crate's sweep
owns); every message spelling; the CSV join lattice and format
vocabulary; the 9 shared type-hint names.

**D4 — Fresh design.** lib.rs façade; modules by noun:
- `location/` — the Local|S3 vocabulary, store construction (the ONE
  object_store boundary), and path/listing primitives both halves
  share.
- `format.rs` — the format vocabulary (jsonl|csv|parquet) and shared
  per-format options; READERS live with the source, WRITERS with the
  destination (gen 1's shared formats/ mixed the directions; the
  halves' needs diverge and the vocabulary is the only true sharing).
- `source/` — `config.rs` (Document), `list.rs` (complete-or-fail
  listing + prefix semantics), `cursor.rs` (the rulebook: v1 wire
  format, planners, tail-hash verification), `read.rs` (per-format
  readers + gzip/zstd), `connector.rs` (`File` + Feed choreography,
  `source::Shell`).
- `destination/` — `config.rs` (Document; `partition_by`, kinds),
  `layout.rs` (staging/final naming, partition encoding), `stage.rs`
  (part writing: parquet/jsonl writers + writer props), `truncate.rs`
  (the ownership-precise Replace rule), `load.rs` (the Backend:
  4-phase commit mapped onto the sdk hooks per D7), `connector.rs`
  (`File`, capabilities, connect = staging reclaim obligation,
  FAIL_POINTS, testhook, the `ParquetDir` alias), `destination::Shell`.

**D5 — Parity = coverage + needles.** Fresh tests answer the census
(109 + 3 across 15 binaries → the house layout); the WELD PROOFS carry
verbatim (the pre-015 persisted-cursor const and the planted commit
log must read back identically — they prove the persisted formats
survived the rewrite); both sdk conformance kits certify the Shells;
the 14 suspicious items open the review docket — the headliner being
gen 1's duplicate-stream-name gap, the exact analogue of 029's
shared-table silent corruption.

**D6 — Coexistence.** `publish = false`, consumed by nothing; the swap
(delete gen 1, rename, port the facade's `DestSpec::File`/`Parquet`
arms and the module alias) is the owner's decision.

**D7 — The receipt mapping.** Unlike iceberg (029 D7), this
destination HAS a load-level receipt store: `existing_receipt` READS
`_rdlt_commits.{scope}.json` for (load_id, commit_seq); `replay`
discards the redelivered staging (the phase-1 dedup of gen 1's
commit); `publish` runs truncate → publish → state → receipt-LAST with
the same crash points at the same edges. The engine-owned `pq.*`
points must keep their declaration site and sweep ownership (the 024
scanner locates ENGINE_POINTS by shape — coordinate, don't move).

## STATUS

- Branch created; contract inventory committed; this plan written.
- NEXT: build in the 029 rhythm (incremental commits, offline tests
  green at each step): location/ + format vocabulary → source half →
  destination half → connectors/Shells → fresh suite (local-FS cells
  anywhere, RUSTFS cells skip-not-fail, both conformance kits, weld
  consts honored, sweep wired beside gen 1) → review rounds to
  terminus → gates twice clean (baseline 1024; counts predicted and
  verified; volume-count check in the gate prep per the 029 lesson).
