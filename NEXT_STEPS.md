# NEXT_STEPS — full-codebase audit, 2026-07-26

Audited at `main @ 634222e` (feature 019 complete, all four bars PASS). Method:
11 parallel analysis lenses over the whole workspace (~62k LoC Rust) + spec
corpus; 175 raw findings; every claimed bug adversarially verified against the
actual code — **29 of 47 bug claims confirmed, 18 refuted** (refutations
recorded in Appendix A so they are not re-litigated). Items deduplicated and
tagged `(impact/effort)` — impact high/medium/low, effort S/M/L.

Recorded standing for context (3-way session 2026-07-25): rdlt vs dlt
**13.2x / 1.7x / 95.0x / 63.6x / 2.6x** — no losses, no parities remain. The
engine's quality story is gate-driven; the gate of record is currently LOCAL
because CI is dead (see P0-1).

---

## P0 — Do first

1. **Restore CI: the root cause is GitHub org billing, not the workflows.** (high/S)
   019's D-01 recorded all four jobs failing in 3–5 s with zero steps and could
   not determine the cause (logs expired). The cause is now determined from the
   check-run *annotation* (annotations outlive logs): *"The job was not started
   because recent account payments have failed or your spending limit needs to
   be increased."* Every push and scheduled run on `rapidbyte-io/rdlt` has
   failed this way since at least 2026-07-23, including nightly/weekly
   `deep-checks`. Fix billing/spending limit in the org settings, re-run CI on
   current main, and watch the first real run — the workflows themselves have
   not executed since the 017 workflow changes, so residual breakage may
   surface after billing is fixed. Until green, the local gate remains the only
   enforcement surface for a project whose entire method is gate-driven.

2. **Add the LICENSE file.** (high/S) Every crate inherits
   `license = "Apache-2.0"` and README claims it, but no LICENSE/COPYING exists
   anywhere (`git ls-files | grep -i license` is empty). Apache-2.0 §4 requires
   shipping the text; GitHub can't detect the license; the recorded 0.2→0.3
   publish window makes this a blocker. Commit the Apache-2.0 text as
   `LICENSE` (+ optional NOTICE).

3. **Refresh CLAUDE.md — it actively misleads every future session.** (high/S)
   The 019 block still says "PLANNED, not yet implemented" while 019 is merged
   with a recorded session; the 018 block still quotes superseded medians
   (1.95/1.62/1.15/0.99/14.60 s) and calls the dedup cell a "0.9x LOSS /
   optimization target" when it is now a 2.6x barred win. Rewrite the 019 block
   in the COMPLETE style of 013–018: recorded standing per cell, the honest
   misses (US2 wall −14.3% vs ≥15%, US6 cell-CPU −4.9% vs ≥10%, T047 4.0x vs
   10x), the US9 re-scope (T089–T095 not built; Amdahl 1.29x vs SC-005's
   1.5x), and next-round pointers (D-08 prize, D-05 allocator, merge-arm
   EXPLAIN owed).

4. **Close 019's paperwork — the record currently contradicts itself.** (medium/S)
   One doc commit: `close-out.md:7` header "IN PROGRESS" → COMPLETE;
   `close-out.md:958` still says "T098/T099 are NOT done" while line 1055
   records that exact session with all bars PASS; the PI5 contract row says
   "Remaining: T094" but T094 was re-scoped away; `spec.md:7` still "Draft".
   Also: dispose FR-016 in place (the offload MUST was measured to cost +7.0%
   wall (D-03), re-scoped to US9, and US9 wasn't built — the requirement is now
   orphaned; record the inversion and the re-trigger condition), and give
   PERF_ANALYSIS.md the "EXECUTED as 019" banner REFACTORING.md/BENCH_REFINMENT.md
   already have — three of its claims are now recorded false (F3's under-one-core,
   §F8's allocator wall cost, F6's 12.41% recoverability) and the next perf
   effort must not plan against them.

5. **Re-run cargo-mutants — the committed run predates features 006–019 entirely.** (high/M)
   `mutants.out` was produced 2026-07-20 at `f58570b`; since then 017 renamed
   most mutated files (graph.rs→run.rs), 019 rewrote the WAL and COPY encoder.
   The 29-missed/7-timeout report describes code that no longer exists, and the
   mutation-closure tests written against it have never been verified to kill
   their current targets. `TARGET=mutants make test` in distrobox
   (`--iterate` keeps it incremental), triage survivors, commit the refreshed
   record. The specific pins in §3 below are worth writing regardless.

---

## 1. Bugs — confirmed by adversarial verification

### High

- **Silent NULLing of u64 values above `i64::MAX` on the shred path.** (high/M)
  `shred/infer.rs:49` — a JSON integer like a full-range hash id leaves the
  column Int64; at build, `scalar_int64` matches only `Kind::Int` and appends
  NULL for `Kind::UInt` — value lost, zero Discarded accounting. Contradicts
  the module's own "counted, never silent" rule and the passthrough path,
  which *rejects* UInt64 for exactly this reason. Fix: treat `Kind::UInt` as a
  Utf8 observation (monotone on the lattice; canonical rendering keeps digits).
  Schema-affecting — affected columns become text instead of silently-null Int64.

- **Schema policies only hold within one run — Freeze is bypassed at every run
  boundary.** (high/L) `runtime/run.rs:506` — SchemaRegistry starts empty each
  run, so run N+1's first batch is a CreateTable, unconditionally forced to
  Evolve; a widened/new column between runs sails through a Freeze contract.
  `StateDoc.schema_hashes` is persisted apparently for cross-run drift
  detection but is write-only (no reader anywhere). The design doc promises
  "Freeze turns a would-be delta into a typed error before any row is written"
  with no per-run qualifier. Either seed the registry from persisted schema at
  run start (needs design — hashes alone can't diff) or explicitly document
  Freeze/Discard as within-run and mark schema_hashes diagnostics-only.

### Medium — wrong data / hangs

- **Iceberg `reconcile()` compares struct types including nested field IDs.** (medium/M)
  `iceberg/dest/ensure.rs:163` — wanted-schema IDs are assigned depth-first;
  REST catalogs normalize with the Java convention (level-order). For any
  schema with a struct followed by another column, the second `ensure_table` of
  an *unchanged* stream can fail fatal with "contradictory drift". No live test
  ever ensures a struct-bearing table, so nothing pins this either way. Compare
  structurally (names + primitive kinds, ignore IDs) and add the live pin
  (§3, struct/list cell).

- **Grown parquet rewrite resumes from a stale row-group offset undetected.** (medium/M)
  `file/source/cursor.rs:45` — same-size rewrites are caught by etag/mtime,
  shrunk by the size check, but a *grown* rewrite passes: parquet never records
  `tail_hash` (`formats/parquet.rs:73`), so resume trusts the recorded
  row-group index into a different file — silently skipped/wrong rows. JSONL
  closed exactly this hole with TAIL-HASH. Fix: hash the footer's per-group
  (offset, size) entries for groups `0..done` into FileProgress and verify on
  resume (a genuine append preserves them; blunt etag=fatal would break
  legitimate parquet appends).

- **Replace truncation keeps stale parts after a format or `partition_by`
  change.** (medium/S) `file/dest/truncate.rs:22` — ownership is parameterized
  by the *current* config (extension, partitioned flag); switch jsonl→parquet
  or drop partition_by and old parts survive a Replace — the table is silently
  a mix of two loads. The files are unmistakably rdlt-written (`part-*.jsonl|parquet`);
  own both extensions and always include depth-2 partition dirs.

- **`keys_of_table` mis-splits when a partition value equals the table name.** (medium/S)
  `file/location/mod.rs:304` — the S3 arm uses `name.rfind("{table}/")`; key
  `…/events/events/part-….parquet` splits wrong, Replace truncation then
  deletes a nonexistent key, `delete_key` doesn't tolerate NotFound → commit
  fails fatal and the real object is never truncated. Fix: `strip_prefix` on
  the known computed prefix instead of searching.

- **Type-hint pins are silently overridden by object/array values.** (medium/S)
  `shred/infer.rs:112` — `pinned` lives in ScalarState but `ColState::observe`
  replaces the whole state with `ColState::Json` on an object/array *before*
  the pin check runs; a hinted column drifts to Json under Evolve or aborts
  under Freeze. Contradicts both in-code contracts ("hints win over
  inference"). Thread pinned-ness up to ColState.

- **`parse_decimal` ignores declared precision.** (medium/S)
  `shred/build.rs:444` — over-scale fractions are nulled ("inexact") but a
  value exceeding `precision` digits is stored unvalidated into Decimal128;
  destinations fail later with a confusing NUMERIC overflow, or a parquet file
  carries an out-of-spec decimal. Reject `|result| >= 10^precision` → None,
  consistent with the scale rule.

- **Hinted-column value misfits become silent NULLs with no accounting.** (medium/M)
  `shred/build.rs:308` — pinned columns never observe, so no policy fires;
  unparseable strings under timestamp/date/time hints, objects under Utf8
  hints, lossy casts under Float64 all null out uncounted — the one hole in
  the crate's "counted, never silent" discipline (`value_fits` even claims any
  string fits TimestampNaive/Date/Time). Decide: count as discarded, or
  validate at observe time; at minimum document on `with_type_hint` and pin.

- **Freeze bypass for child tables appearing mid-run.** (medium/M)
  `shred/mod.rs:156` — under Freeze, a new scalar field aborts the run but a
  new `items: [{…}]` field silently creates and loads a whole new child table
  (CreateTable forced to Evolve). Resolve policy for post-first-drain child
  tables via `policy.action_for` — the table-level ContractViolation variant
  already exists. Untested asymmetry (us4 tests cover only scalars/widenings).

- **REST clients have no request timeout — a stalling server hangs the
  pipeline forever.** (medium/S) `rest/source/client/mod.rs:56` — reqwest has
  no default timeout; `Client::new()` also panics (rather than errors) on TLS
  init failure, and the OAuth2 token fetch builds a fresh client per fetch
  with the same gaps. Add a defaulted `request_timeout_secs` config, build via
  `Client::builder().timeout(…)`, map build errors to typed ConfigError.

- **POST pagination silently no-ops for non-object bodies.** (medium/S)
  `rest/source/read/driver.rs:315` — page params inject only into
  `Value::Object` bodies; an array/scalar body template sends the identical
  wire request every page, and the same-request guard doesn't fire (it hashes
  `page_params`, which change) — up to `max_pages` (default 10,000) duplicate
  pages, then a misleading fatal. Reject the config combination at validation.

- **Tracing span guards held across `.await` in `stream_task` and
  `Loader::process`.** (medium/S) `runtime/run.rs:498`, `load/mod.rs:120` —
  the canonical tracing misuse: per-stream attribution corrupts under
  concurrent streams, and the `rdlt.shred`/`rdlt.passthrough` spans aren't
  reliably parented. No `.instrument` usage exists in rdlt-engine. Use
  `Instrument` for the async bodies; keep `enter()` only in spawn_blocking
  closures.

### Low — hardening, latent, or embedder-only paths

- **Crash-before-first-checkpoint leaks WAL residue without bound.** (low/S)
  `runtime/run.rs:317` — `Scan::Nothing` (returned for a checkpointless span)
  clears nothing; cron-driven retries against a broken source grow the
  manifest and orphan segments forever. Clear the dir when a manifest exists
  but nothing is replayable.
- **Decimals nested inside preserved structs / scalar-list items are never
  lowered.** (low/M) `load/lowering.rs:50-59` — with `structs=true,
  decimal=false` a nested Decimal128 reaches a destination that declared it
  can't take it. Latent for in-tree connectors (all declare decimal:true) but
  the SPI is public: recurse, or reject the capability combination loudly.
- **Embedder-supplied type hints are unvalidated — `Decimal{precision:200}`
  panics in build.** (low/S) `shred/build.rs:375` — hints bypass the lattice
  that justifies the `expect`. Validate at TapeShredder construction → typed
  Config error.
- **Postgres `column_wire` uses the ensured schema's decimal scale, not the
  array's.** (low/S) `postgres/dest/encode.rs:42` — the i128 values are stored
  at the *array's* scale; any divergence silently multiplies/divides values by
  10^diff. Read the scale from the Decimal128 DataType (the fallback arm
  already does) or fail typed on disagreement.
- **Out-of-range Time64 values wrap instead of erroring.** (low/S)
  `postgres/dest/encode.rs:246` — negative/>24h values can wrap into
  NaiveTime's accepted range → silently wrong time, against FR-021. Adjacent:
  the Date epoch shift can overflow i32 under overflow-checks; a
  `unwrap_or(i16::MAX)` substitutes a wrong numeric weight where `expect`
  would be truthful. Bounds-check before casting.
- **DuckDB probes/DDL map all errors fatal, bypassing the crate's own
  classifier.** (low/S) `duckdb/dest/commit.rs:95` — a transient file lock
  during ensure/commit probes aborts the run while the same lock during
  write() retries. `map_err(classify)` at those sites.
- **`Retry-After` HTTP-date form silently ignored.** (low/S)
  `rest/source/client/mod.rs:164` — only delta-seconds parses; date-form
  servers fall back to generic backoff, against the recorded "honored,
  bounded" posture. Parse the date form, still clamped by `retry_after_cap`.
- **OAuth2 401 hook can evict a concurrently-refreshed fresh token.** (low/S)
  `rest/source/client/auth.rs:70` — under fan-out, B's stale 401 drops the
  token A just fetched; bounded (one retry per send) but burns token-endpoint
  round trips. Generation counter on CachedToken.
- **Parent placeholder values substituted into URL paths without
  percent-encoding.** (low/S) `rest/source/read/resolve.rs:72` — a parent
  record id of `../admin` or `x?y=1` restructures the child request URL.
  Percent-encode path substitutions (query params are already encoded).
- **CSV inferred-Bool cells coerce to `false` on the two-pass race.** (low/S)
  `file/formats/csv.rs:244` — Int/Float arms fail loudly with the typed
  `two_pass` error; the Bool arm maps any changed cell to `false`. Mirror the
  declared-hint arm.
- **`is_recoverable` treats deterministic object_store failures as
  transient.** (low/S) `file/location/s3.rs:303` — InvalidPath/NotSupported/
  UnknownConfigurationKey burn the retry budget and report as transient
  exhaustion. Add them to the non-recoverable set in the one rulebook.
- **`normalize_ident` breaks its max_len contract when `max_len < 9`.** (low/S)
  `rdlt-core/src/naming.rs:49` — result is 9 chars, longer than the bound;
  latent (in-tree uses 63) but it's core vocabulary with a pub field. Clamp or
  shorten the hash; boundary test.
- **Forced bench runs enter the Trends table as evidence.** (low/S)
  `rdlt-bench/src/report.rs:249` — artifacts stamp `forced: true` precisely so
  a forced number is never mistaken for evidence, but `append_history` drops
  the flag and Trends renders it unmarked. Skip or annotate.
- **`RdltError::Internal` exits the CLI with the config code (2).** (low/S)
  `rdlt-cli/src/main.rs:102` — the `_ =>` fallback tells scripts to fix their
  YAML for an engine bug; `#[non_exhaustive]` means future variants join it
  silently. Map Internal/catch-all to EX_SOFTWARE (70) and document.
- **`tools/interop/.gitignore` pattern is a no-op.** (low/S) — slash-anchored
  `tools/interop/.venv/` inside that directory matches nothing; only Python
  3.13's auto-written ignore is protecting the venv. Change to `.venv/`.

---

## 2. Testing

- **The mutants re-run is P0-5.** The pins below are worth writing regardless
  of what the fresh run says:
- **Pin `LoadItem::byte_size`** (medium/S) — its ONLY consumer is the
  shred→load byte budget; zeroing it disables backpressure invisibly. And
  `mutation_closures.rs:24-25` *falsely claims* the counters test covers it
  (counters call `get_array_memory_size()` directly) — correct the comment
  (Principle VI).
- **Pin WAL segment sequencing** (medium/S) — mutate `segment_seq += 1` and
  every batch writes the SAME file (File::create truncates) while the manifest
  accumulates N lines naming it: replay would deliver the last batch N times.
  The test *named* for this (`…segments_are_sequential`, resume.rs:387) never
  asserts it.
- **Pin `lower_batch` under mixed capabilities** (medium/S) — each guard is
  only ever tested with its own flag off; a batch with BOTH struct and decimal
  under caps(true,false)/(false,true) kills the two live match-guard mutants.
- **Decimal edge-case table** (medium/S) — build.rs has no test module at all;
  parse/render decimal grammar (".5", "5.", "+5", "1e5", whitespace,
  over-scale, precision/i128 boundaries, scale=38 with i128::MIN) is the
  thinnest-tested value path in the shredder. Pairs with the precision bug above.
- **Iceberg struct/list live cell** (medium/M) — capabilities advertise
  structs/scalar_lists but no test (unit or live) ever creates one; nested-ID
  assignment, Polaris create parsing, align() round-trip and re-ensure are all
  unexercised. Ensure twice to pin the reconcile bug's fix.
- **Container reaper/labeling convention** (medium/M) — recorded twice in 017
  (188 stopped containers + 1117 volumes, 168 GB; then 16 orphans + 851 GB
  target/) and it turned a gate red once already. Stamp `rdlt-test=1` on every
  testkit/bench container + a labeled prune verb.
- **Pin the Polaris image** (medium/S) — 017 D16's "later increment with a
  live-verified tag" never happened; `apache/polaris:latest` still floats in
  the iceberg fixture while rustfs got pinned at all 3 sites for exactly this
  drift mode.
- **Crash-sweep ack-loss variant** (medium/M) — D-23: the sweep passed 23/23
  through two real exactly-once defects because no crash point can model
  "server committed, client never learned". Add a drops-connection-after-COMMIT
  action, or record pin-only coverage as the deliberate boundary.
- **Turn container-flake watching into data** (medium/M) — six distinct flakes
  recorded across 017/019 with only procedural workarounds; have testkit
  tag/count skips so a dominant offender emerges and gets a targeted fix.
- **Make `saw_cancelled` precedence deterministically testable** (medium/M) —
  the mutant that commits trailing work for a cancelled run survives because
  the existing closure is sleep-timed. Testkit source that completes pushes,
  then parks until cancellation.
- Smaller pins (all low/S): EverySeconds commit-policy boundary; lowered-field
  nullability rules; `render_decimal` zero + minus-vs-divide boundary;
  `scalar_of`/ContractViolation from/to fields; clean-run-removes-WAL-dir;
  DestSpec::File↔FileDestConfig parity pin (or take the refactor in §4);
  convert the 7 timeout-kills to assertion-kills; add rdlt-connector-sqlcore
  to the mutation scope; consider a fuzz target for WAL v2 Arrow-IPC replay;
  record equivalent/untestable mutant residuals so triage isn't repeated.
- **Operator work: the second recorded session** (low/M) — bars.toml queues
  it: tighten two bars whose floors sit well under the recorded medians, and
  decide the deliberately-unbarred `pg-to-s3parquet-1m` at 1.7x (one session
  on a newly-comparable cell was rightly not the basis for a bar).

---

## 3. Performance — measurement-first queue (per project rule: measure, take only what wins)

- **Run the owed `EXPLAIN (ANALYZE, BUFFERS)` on the merge arm.** (high/S)
  The dedup cell's merge arm is 77.1% of the cell (3,982.8 ms server-side) and
  the narrowest barred margin (2.6x vs ≥2x). PERF_ANALYSIS §7 said it
  "deserves an EXPLAIN before it is written off"; none exists anywhere in the
  019 record. US9's parallel-staging lever is Amdahl-blocked (1.29x < 1.5x),
  so server-side SQL is the only place a merge-cell win can come from. MERGE
  and UPDATE-then-INSERT are already killed on first principles — anything
  taken must beat the *plan*, not the intuition.
- **Re-attribute blocked time on the post-019 pipeline.** (medium/S) The one
  experiment PERF_ANALYSIS named as its own top gap (off-CPU profile /
  tokio-console; needs `perf_event_paranoid ≤ 1` on the host) was never run,
  and every premise it would test has moved (under-one-core is recorded false;
  throughput went 362k → 1.19M rows/s). Output decides whether the next round
  buys wall or only headroom.
- **D-08 fixed-width COPY fast path.** (medium/M) 019's own close-out records
  the prize: 41.6% of encoder instructions are `put_slice`/memcpy plumbing for
  4–8-byte fields (~250k calls per 10k rows), chrono round-trips add 8.3%
  where wire forms are trivial offsets; ~20% of encoder cost named. Declined
  under PI3 then; the phrasing now is a throwaway build gated on the
  byte-identity fixture + `pg_copy_encode_10k` iai (baseline 31,924,625) + an
  interleaved cell CPU A/B, with D-13/D-21 as the standing warning that
  counting arguments lose.
- **D-05 allocator follow-up is now due — its precondition (US4+US6 landed)
  is met.** (medium/S) Step 1 free: re-profile; if libc malloc/free is under
  ~10% of cycles, record the negative and stop. Step 2 only if it ranks:
  mimalloc A/B, adopt only with RSS within a few percent (the 5–6x memory edge
  over dlt is a headline result; the dependency cuts against the small-engine
  rule).
- **Measure the WAL's residual cost; then the all-Replace skip / spec-level
  opt-out if it pays.** (medium/M) PERF_ANALYSIS F2 options 3/4 were never
  taken; pre-019 the no-WAL control was ~6% ahead, but US4/US5 halved the cell
  since. A/B `workdir=None` vs default on both 1M cells; ≥3% → implement
  skip-WAL-when-all-streams-Replace (a Replace span is provably discardable;
  the engine already knows write modes; crash sweep gates), plus an explicit
  opt-out not spelled `workdir: null`. Under noise → record beside D-03.
- **File-dest whole-part buffering: close D18 with a recorded disposition.** (medium/M)
  017's D18 trigger ("perf follow-up with before/after evidence") fired — 019
  US7 touched these files — but D18 was neither taken nor re-recorded, and
  US2's missed RSS floor was explicitly assigned to this buffering
  (`FileSession::encode` still materializes each part in a `Vec<u8>`; flagship
  cell peaks at 208 MB RSS). Heap-profile first (DHAT/heaptrack); if the
  buffer dominates, evaluate streaming a single staged part via multipart
  upload (keeps the one-named-part-per-batch replay protocol the 019 review
  protected). D-03 (+7% from offloading the WAL encode) is the counter-evidence
  requiring the A/B either way.
- **Stop re-downloading complete, unchanged S3 parquet objects every run.** (medium/M)
  `file/source/mod.rs:234` — resolve_inputs fetches EVERY matched object
  before planning, including cursor-complete ones; an incremental pipeline
  re-downloads its whole history each run. Etag + recorded progress are both
  available pre-fetch; skip and synthesize FileMeta. Structural, but measure
  the cell before/after per the rule.
- **WAL recovery path blocks the executor (the engine-side D18 residual).** (low/M)
  `wal/resume.rs:66-99,186-242` — replay opens and fully decodes every segment
  twice inside async fns; the only *unbounded* blocking site left (time-based
  commit policies make spans arbitrarily large). Async-hygiene for embedders
  sharing a runtime, not a throughput claim; spawn_blocking the passes and
  record the disposition so D18 finally closes. (Encode-side is
  closed-with-evidence: D-03 measured offload at +7% wall; manifest appends
  are ~100 B — leave unless measured.)
- Smaller measured candidates (all low): netem 2 ms RTT run, then coalesce the
  commit preamble's serial round trips if it pays (the JSONL cell commits 5x);
  canonical-JSON per-object allocation probe with D-13/D-21 as the explicit
  null hypothesis; price the merge stage's 1M `nextval()` calls before
  believing `__rdlt_arrival` is free; micro-gate the partitioned-write
  per-row String rendering before partition_by ships at scale; hash the POST
  body template once per sequence instead of Debug-rendering per page
  (`rest/read/driver.rs:121`); DuckDB full loads still write every row twice —
  the deferral lives only in a code comment (`duckdb/dest/commit.rs:423`),
  absent from any close-out; dedupe the reqwest 0.12/0.13 double tree in the
  shipped CLI (binary size); US2's unexplained ~4-point wall gap vs
  PERF_ANALYSIS's −18.3% remains a recorded open question.

---

## 4. Refactoring

- **D17: fold the engine's byte-budget channel into the SPI's — its recorded
  trigger has fired.** (medium/M) 017 confirmed the duplication
  (`rdlt-connector/src/channel.rs` vs `rdlt-engine/src/runtime/channel.rs` —
  same semaphore budget, same oversized-degrades-to-drain, same
  permit-in-item release) with fix shape "one generic core in the SPI" and
  trigger "next feature touching either"; 019 touched rdlt-connector
  (output.rs/ParquetOptions). Backpressure accounting is a correctness
  invariant hand-maintained in two places. Parameterize the msg caps (64 vs
  256) and sender Clone-ability; DELETE the engine copy per greenfield rule.
- **Mechanize the `lower_column`/`flatten_array` parity — the hand-maintained
  duplication has already drifted.** (medium/S) 017 deferred it in place
  ("revisit if a third site appears"); no third site, but the batch-side
  decimal arm hardcodes `nullable=true` while the schema side computes it —
  exactly the drift the deferral gambled against (latent today; verified
  unreachable for in-tree destinations). Fix the arm, then add the parity
  test: for each caps combination, `lower_batch(batch).schema()` must equal
  the arrow schema of `lower_schema(schema)`.
- **DestSpec::File is a hand-mirror whose own comment names the failure
  mode.** (medium/M) `rdlt/src/pipeline_spec.rs:134` — "a field added to the
  destination config and NOT added here compiles fine and is simply
  unreachable". The Iceberg variant already shows the fix (embed
  `Box<Config>`); do the same for File (and pg/duckdb where feasible), or at
  minimum add the round-trip parity pin.
- **D19: the config-plumbing trio is still triplicated** (low/M) — recorded in
  017, and US7's config additions re-fired it (`rest/source/config.rs:453`).
- Sqlcore consolidation (low): route `flagged_roots` through the dialect dedup
  seam instead of hardcoding DISTINCT ON (`sqlcore/plan/arms.rs:155`); move
  `create_index_sql` + duplicate-merge-key diagnosis from duckdb into sqlcore;
  extract the shared `ensure_table` merge choreography into a sqlcore plan
  (golden pins must stay byte-identical throughout).
- Deduplicate the shred/passthrough forward blocks in `stream_task` and unify
  send-failure handling (low/S; also fixes the overstating comment at
  `run.rs:585-590` — see Appendix A items 1–2).

---

## 5. Cleanup

- **Reclassify internal-invariant failures from Config to Internal.** (medium/S)
  Five sites raise impossible-unless-engine-bug conditions as
  `RdltError::Config`, misdirecting operators into a config hunt (Principle V:
  one variant = one operator action). `load/lowering.rs:117` et al.
- **Per-file cursor entries accumulate forever for rotated-out files.** (medium/M)
  `file/source/cursor.rs:161` — every file ever seen rides every checkpoint
  and StateDoc; pruning has real semantics (a pruned file that reappears
  re-reads fully), so it needs a decided, documented retention rule — the
  current rule is unbounded growth, undocumented. Related: the file dest's
  commit log also grows without bound and is rewritten wholesale each commit
  (`dest/layout.rs:94`).
- **Dependency hygiene** (all low/S): `arrow-schema` unused in rdlt-core (and
  the lib.rs charter comment claiming it — this is the semver-sacred crate);
  `futures` unused in rdlt-engine; `bytes`+`futures` unused in rdlt-testkit;
  `tokio` demotable to dev-dep in the rdlt facade; dev-deps repeating regular
  deps.
- **Silent-failure logging** (all low/S): log the tokio-postgres connection
  driver's terminal error (`postgres/tls/connect.rs:79`); surface dropped
  events on EventStream lag (`engine/lib.rs:92`); CLI report-write I/O failure
  is misclassified as Usage (`cli/main.rs:196`); CLI swallows event-feed task
  panics (`cli/main.rs:190`); bench runner conflates corrupt report.json with
  absent (`bench/runner.rs:254`) and a failed podman inspect with container
  exit (`bench/competitors.rs:367`).
- Recorded-but-never-executed sweeps (low/S): strip the T0xx/SMx history tags
  from duckdb tests (017 deferral to increment 6, never done); 017's eight
  verified-but-cut review residuals remain recorded and unscheduled — triage
  into this list or close them.
- Odds and ends (low/S): `WalRecord::Segment.rows` is write-only — use it as a
  replay cross-check or drop the field (019 recorded the deliberate
  not-built; decide which side wins); `new_load_id`'s uniqueness comment
  overclaims (epoch-fallback + pid reuse); `_ctx` underscore on a used
  parameter (`postgres/dest/mod.rs:412`); decide a retention story for
  `_rdlt_cleared`/`_rdlt_commits` growth; extend the REST credential-header
  blocklist beyond authorization/x-api-key; iceberg reconcile ignores
  nullability drift (surfaces late as an align error); temp fetch dirs leak on
  planning/staging failure (`file/source/mod.rs:154`); `resolve_files` reports
  an existing directory as "does not exist" (`file/source/mod.rs:77`); prelude
  omits `PipelineBuilder` despite claiming crate-root parity; delete or
  justify unused bench schema surface (`Cell::primary_fixture`, non-Wall
  Timing variants); unify PgFixture/CdcPgFixture and dedupe client()/seed();
  archive the executed root working documents (PERF_ANALYSIS.md,
  REFACTORING.md, BENCH_REFINMENT.md — note the filename typo) once their
  banners point at close-outs.

---

## 6. Build / CI / release readiness

The 0.2→0.3 publish window is recorded as the next publish, and the deferral
list for it is confirmed EMPTY (017 R10 applied everything under greenfield) —
these are the remaining mechanics:

- **CI billing + first green run** — P0-1.
- **Publish metadata** (medium/M): no crate sets `readme`/`keywords`/
  `categories`; every crates.io page would ship with an empty long
  description. Verify with `cargo package --list -p rdlt`.
- **Stale crates.io descriptions** (medium/S): rdlt-cli says "TOML" (it parses
  YAML and is the *shipped* artifact); rdlt-connector-file says "file source:
  JSONL and Parquet" (it's the whole source+dest family incl. CSV/S3 since
  015). Re-verify the rest while there.
- **Feature-matrix / packaging job** (medium/M): whole-workspace builds mask
  per-crate breakage via feature unification; the facade's narrowed features,
  connector-without-schema, testkit-without-containers, and each crate as
  `cargo publish` would build it are never compiled. `cargo hack check
  --each-feature` on the publishable crates, or per-crate `publish --dry-run`.
- **Rustdoc is never built; no missing_docs anywhere** (medium/S):
  `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` in the lint gate;
  `#![warn(missing_docs)]` on rdlt/rdlt-core/rdlt-connector before publishing.
- **Deep-tier scheduling truth** (medium/M): Makefile:18 claims `TARGET=deep`
  equals "everything scheduled CI runs", but memory_bound and spark_deep run
  in *no* schedule at all; deep-checks.yml never invokes TARGET=deep. Add
  memory_bound to nightly (its pg service exists) or correct the header and
  record where the heavy legs run.
- **Semver gate scope** (low/S): only rdlt-core/rdlt-connector are checked;
  extend to the other published crates before 0.3.
- Smaller (low/S): pin GH Actions to commit SHAs and stop compiling
  iai-callgrind-runner from source each run; run the deterministic bars gate
  (`make bench TARGET=gate`) in CI; honor `CARGO_TARGET_DIR` in
  `rdlt-bench/src/paths.rs:38`; bench-setup.sh unbounded `pg_isready` wait +
  hardcoded mise kubectl fallback; `fuzz/Cargo.lock` still records parquet as
  an rdlt-engine dep (stale since US2); document the hyperfine prerequisite
  `make check` hard-fails without.

---

## 7. Docs

- **Document the 8.43x concurrent-pipeline scaling — the close-out's own
  instruction, still undone.** (medium/S) US9's re-scope concluded this
  documentation IS the deliverable that replaced T089–T095, and no README/
  docs/GOVERNANCE text mentions it: ~1.19M rows/s single pipeline,
  near-linear to 8 concurrent (10M rows/s aggregate), and the deliberate
  trade (a full-refresh load is one transaction on one connection by
  construction; aggregate throughput comes from pipeline-level concurrency).
- CLAUDE.md + 019 status lines + PERF_ANALYSIS banner — P0-3/P0-4.
- Contract-truth fixes (low/S each): `partition_by` doc claims Hive-style
  `col=value` dirs, code writes bare `value` (`file/dest/config.rs:52`);
  file-source config carries silently-ignored knobs (primary_key, validate,
  type_hints) and stale comments; `json_type` capability contract vs lowering
  behavior; state the WAL-before-validation invariant for merge-key NULL
  checks (replay has no such check); document iceberg replay detection's
  snapshot-retention dependency; fix the stale module doc in
  `direct_publish_guarantees.rs` claiming an ignored unfixed defect;
  `benches/README.md` says format_version 2, artifacts are 3; `bars.toml`
  header still claims the dedup cell "carries NO bar" while the file defines
  one; Makefile header omits the coverage verb and the recipe's scope
  contradicts its comment; document `primary_key` declaration as the free
  JSONL perf lever it measured to be; GOVERNANCE note that the 0.3 window
  holds only the standing publish-time bump.

---

## 8. Features (fit the small-engine rule; all recorded doors, not new scope)

- **Iceberg phase-2, when demand materializes** (medium/L): re-survey
  iceberg-rust ≥0.10 for client-middleware signing (Glue/SigV4) — the
  AuthOptions seam is already additive-ready; re-probe Replace/overwrite on
  every iceberg-rust upgrade (typed-unsupported today was a designed
  narrowing, and `commit_with_retry` already generalizes to arbitrary
  transactions). The 018 policy log's deferred Iceberg 3-way bench cell shares
  the same re-trigger.
- **Bench artifact provenance** (medium/S): fingerprints record cpu/kernel/
  rustc/competitor pins but not the rdlt git SHA or fixture images — bars
  rest on floors nothing ties to a build. Additive serde-defaulted fields;
  consider pinning `postgres:16` to an exact tag in fixtures.toml (testkit
  already pins `16-alpine` citing exactly this).
- **Facade completeness** (medium/S): re-export `EventStream` and
  `CancellationToken` at the rdlt root — `Pipeline::events()` returns a type
  an embedder cannot name without depending on the internal engine crate.
  Decide and document whether the connector SPI is reachable through the
  facade (low/S).
- **CLI**: `rdlt --version` (a shipped binary without it), and consider a
  `check`/`validate` subcommand for pipeline specs (low/M).

---

## Appendix A — bug claims checked and REFUTED (do not re-litigate without new evidence)

1. `?`-exits in stream_task bypass close+join — mechanics true, but every
   error on those paths is non-retryable, so the claimed retry-overlap is
   unreachable; the fix is the comment/dedup cleanup in §4.
2. Stream failure unobserved until drain completes — mechanics confirmed, but
   committed progress is preserved and resume is correct; it's a
   failure-latency *design choice* that deserves a stated rationale at
   `drain_loader`, not a bug.
3. WAL misses directory fsync — true, but the WAL is explicitly not the source
   of truth; exactly-once rests on destination idempotence + atomic cursors.
4. Decimal→Utf8 batch lowering hardcodes nullable=true — real drift, covered
   by the §4 parity item; unreachable for in-tree destinations.
5. Trailing discards never commit (2 claims) — mechanics true, but nothing in
   the workspace consumes `CommitMeta.counters`; RunReport totals are correct.
6. Passthrough DiscardRow semantics differ from shred — contracted design
   (SPI contract E7: column-projection discards; value-level discards are a
   typed error).
7. Negative-scale clamp should be a typed error — dead defensive code; every
   route to lower_batch rejects negative scales earlier.
8. Zero-row Replace direct-path divergence — unreachable: no Delta → no
   registration → neither path clears.
9. OffsetLimit short-page vs total_count ordering — reordering changes
   behavior on zero inputs; both arms return Done identically.
10. Cross-origin NextUrl credential leak — the party controlling `next` IS the
    API server that already holds the credential.
11. NextUrl relative-resolution doc contradiction — README + pinned test
    define base_url resolution; the one stale doc line on
    `PageDecision::NextUrl` is a docs nit.
12. catalog.props credential override — documented, recorded (016 matrix)
    escape-hatch behavior.
13. Iceberg vended-credential expiry misclassified — traced: the 401/403-fatal
    arm only fires on catalog-auth status contexts; storage expiry funnels
    through opendal as transient, matching the recorded posture.
14. Staged part-name collisions — require a `-` in a table name;
    normalize_ident's charset makes that unreachable on every production path.
15. Bench runner swallows RunReport errors — the CLI propagates write failures
    as fatal + non-zero exit, which the runner rejects before parsing.
16. Manifest-open I/O error treated as absence — any persistent open failure
    fails typed moments later at `Wal::open` in the same run.
17. Swallowed mtime error disarms the rewrite tripwire — designed, pinned
    (Some,Some) opportunistic pairing that lets one rulebook serve both
    location kinds; mtime can't fail on the supported platform.
