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
  FAIL_POINTS, testhook), with the `ParquetDir`/`Shell` aliases in the
  destination TOC, `destination::Shell`.

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
- BUILD COMPLETE (both halves + Shells + ParquetDir + registries +
  testhook + the sdk ADOPTED entry) — incremental commits, offline
  tests green at every step; clippy clean including --all-targets.
- SUITE COMPLETE under the house layout: 91 offline tests + the
  failpoints crash_sweep binary (11 points × 3 actions: 2 source + 6
  pq.* local, 3 file.* against RUSTFS skip-not-fail), wired into
  `make test TARGET=sweep` beside generation 1. BOTH sdk conformance
  kits certify the Shells on the local filesystem. The WELD PROOFS
  carried verbatim; RUSTFS live cells 4/4; registry pin ungated + the
  gated twin.
- TWO DEFECTS THE SUITE CAUGHT IN THE FRESH CODE, both fixed + pinned:
  (1) four cursor-planner refusals had been PARAPHRASED — the weld
  proof failed on the frozen spellings (shrink, both same-size
  rewrites, unterminated growth, whole-file size change; now the
  inventory's exact text). (2) THE PART-INDEX DEFECT (S3 sweep,
  file.finalize.delete / 1*off->return, measured 6 rows where 4 were
  loaded): the fresh Backend counted part indices per SESSION; a
  crash-recovery session resumes from committed state, stages fewer
  parts, and re-publishes the pending commit under different indices,
  orphaning the crashed attempt's already-copied finals. The index is
  per COMMIT (the pending-staged count per (table, partition)) — the
  inventory's "already-staged parts" means exactly this.
## REVIEW ROUNDS

**Round 1** — three parallel lenses (docket audit S1-S14, fresh-eyes
correctness, contract fidelity + anti-transcription):
- Docket: S2/S5 resolved by construction, S7's off-by-one half
  resolved; 11 items inherited. TAKEN in the fix pass: S1 (duplicate
  stream names refused at the gate — the headliner, 029's shared-table
  precedent), S3 (mid-read shrink refuses instead of recording
  complete), S7's row-less pass-2 CSV error, S8 (unsigned_payload over
  plain http refused), S10 (S3 listing/HEAD stamp last_modified — the
  second rewrite-tripwire leg; etag-less stores were blind), S11
  (whole-file local reads re-stat against the listing snapshot), S12
  (row counting filters through the ownership shape rule), S13
  (redundant completion checkpoint suppressed), S14 (skip-fetch
  counter per instance). STANDING OWNER RECORDS (not fixed, recorded):
  S4 path_safe collisions silently merge partitions (fixing changes
  the FROZEN partition-dir naming — a persisted layout); S6 two
  CONCURRENT sessions of the same pipeline destroy each other's
  staging (inherent to scope-wide reclaim; lease design is owner
  scope); S9 type_hints/validate accepted-and-ignored off their
  formats (typed refusal = behavior change, owner call). Fidelity
  notes recorded: the write-before-ensure spelling is now the sdk's
  longer one (the same 027-era supersession snowflake/iceberg v2
  shipped); v2's truncation spares a file literally named `.parquet`
  (safer than gen 1's exact rule; deliberate).
- Fresh-eyes: FIVE fresh-code bugs, all fixed + pinned (cb342f03) —
  the unverified-resume tail-hash poisoning (a healthy append refused
  as rewritten, with advice that duplicates), wildcards matching
  dot-prefixed names through `**` (a recursive glob over a shared
  prefix read UNCOMMITTED staged parts — require_literal_leading_dot
  on both listing paths), the parquet reader silently ignoring a
  byte-window resume check, no future-version gate on the cursor
  (decoded as EMPTY and re-read everything), S3 patterns matched
  un-normalized against normalized keys (leading slash = silent empty
  stream).
- Anti-transcription: VERDICT NOT CLEAN — the read stack, location
  layer, and writer-props translation carried gen-1 statement
  sequences beyond what the frozen contract forces. Executed at
  7b55ea68: all nine files re-derived (Fill enum, Slabs, ValueShape
  lattice with a shared record-puller, LocalCopies, thin Location
  dispatchers over a disk-helper section) with the 100-test suite
  holding behavior; every rendered message verified byte-identical by
  literal extraction; digest input order untouched (persisted format).

**Round 2** — verification: fresh-eyes HELD every focus area
(tail-window arithmetic hand-traced across six resume shapes; lattice;
digest order; crash placements; per-commit index) but caught the S11
message's botched wrap (14 embedded spaces — gen 1's §9-S2 defect
class reborn) and five round-1 fixes without pins; the transcription
recheck passed all eight source pairs but flagged the RUSTFS fixture
as a light paraphrase plus four single-sentence comment residues.
Fixed at ed4c153a (fixture rewritten from its verified facts; comments
reworded) and d89e9169 (message evidence names the tripwire that
fired; all five fixes pinned — the skip-fetch wiring proven LIVE with
one instance, two runs, counter exactly 1).

**Round 3/terminus** — the last two commits verified clean (SigV4
script, readiness/retry loops, pin premises all held; 105/105).

**Round 4 — /code-review (owner-directed loop)** — the skill's
5-lens process (CLAUDE.md audit, shallow bug scan, git-history,
prior-work, comment compliance) with per-issue confidence scoring.
Eleven findings; fixed:
- STAGED-NAME COLLISION (shallow scan; verified deterministic): the
  gen-1 flat staged name `{load}-{table}-{slug}-{index}` was AMBIGUOUS
  under dashed tables/slugs — (`events`, `us-east`) and (`events-us`,
  `east`) shared one staging file; second write overwrote the first,
  publish promoted wrong rows then failed NotFound. AMENDED (recorded
  deviation from the inventory's frozen staged shape — staging is
  transient, reclaimed wholesale, never re-interpreted across
  versions): the table is its own path segment,
  `{load}/{table}/{slug}-{index}.{ext}`; injectivity pinned in layout
  and live (both dashed tables publish their own rows).
- `..`/`.` PARTITION ESCAPE (verified on the filesystem): path_safe
  passed `..` through and the part published OUTSIDE its table dir,
  invisible to counting and truncation. Now `__dot__`/`__dotdot__`
  sentinels; pinned unit + live (nothing escapes to the root).
- DUPLICATE CSV HEADERS (scored 100): name-keyed rows silently dropped
  the earlier column, convertible under the wrong per-index shape.
  Typed refusal in the survey pass; pinned.
- The two unpinned round-1 fixes pinned: the S3-side dot-key glob
  exclusion (live planted `.rdlt-staging` key) and the S12 ownership
  filter on count_rows (foreign unreadable parquet + foreign jsonl
  neither break nor inflate the count).
- mod.rs purity (house layout): source FAIL_POINTS → connector.rs,
  testhook → destination/connector.rs, read/mod.rs's entry checks →
  read/checks.rs; every mod.rs a pure TOC again.
- Comment/doc drift: the pre-S10 mtime rationale on FileProgress, the
  "two facts" validate doc, the "every local read" overclaim (resolved
  by the checks.rs extraction), and D4's testhook location below.
Recorded, not changed: the registries are unconditional `pub const`
(sibling 028/029 shape; gen 1's were failpoints-gated — the spellings
are the frozen thing and are pinned).

- NEXT: gates twice clean (baseline 1024; predicted main count 1126 =
  1024 + the crate's 102 offline tests; volume-count check in the gate
  prep per the 029 lesson — 135 leaked volumes pruned before the first
  gate).
