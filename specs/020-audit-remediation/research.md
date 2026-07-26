# Phase 0 Research: Audit Remediation

**Feature**: 020-audit-remediation | **Date**: 2026-07-26 | **Spec**: [spec.md](spec.md)

Twelve research tasks were run against the real tree at `634222e`, one per
unknown cluster, followed by an adversarial pass over the three riskiest design
clusters. **All three were found defective and all three corrections are
adopted below as the decisions of record** — the original proposals are kept
only as rejected alternatives, because each failed for a reason worth
remembering.

Every fact below was read out of the repository or out of
`~/.cargo/registry/src/`. Where a claim could not be established from the
repository, it is listed in *Unresolved* with the probe that settles it.

---

## R0 — The three reversals

These are recorded first because they are the difference between a plan that
ships correct code and one that ships confident code.

### R0.1 — The schema-baseline design would have fired Freeze on its own established columns

The first design governed only the `CreateTable` arm against a persisted
baseline. Every *other* change still resolved against the within-run registry,
which is empty at run start (`runtime/run.rs:503-506`) and accumulates across
drains (`tape.rs:89-96`). So:

- Run N establishes `t` = {…, `extra`}; the baseline persists it.
- Run N+1 under Freeze, drain 1 has no `extra` → `CreateTable` → exempt.
- Drain 2 carries `extra` → the registry emits `AddColumn{extra}` →
  `policy.action_for("t", Some("extra"))` → **Freeze → `RdltError::Schema`**,
  on a column this pipeline established and the destination physically has
  (`ADD COLUMN IF NOT EXISTS`, pg `commit.rs:544-554`).

That is the audited defect reproduced in mirror image, and FR-028 forbids
exactly it (a documented promise the loop does not keep). Three further
failures came with it: the design would have **panicked on the first drain of
every second run** (an empty governed set means nothing reaches `kept`, so
`registry.apply` returns `None` and `registry.get(...).expect("schema
registered before building")` at `shred/mod.rs:201-203` fires); the
`STATE_FORMAT_VERSION` 1→2 bump was **inert** because nothing in rdlt-engine
ever assigns `format_version`; and per-table policy **does not reach child
tables** (`policy.action_for` resolves `per_table` by exact `TableName`,
`policy.rs:87`), so freezing `t` would not have frozen `t__items` — FR-030
unmet for the exact policy shape `us4_policies.rs:44` uses.

**Adopted correction**: two diffs on every drain, not one special-cased arm
(R6.2). Plus version stamping (R6.4) and child-table policy inheritance (R6.5).

### R0.2 — The misfit counter would have panicked on an ordinary nullable list column

The proposed counter was a difference of totals:
`misfits += non_null_inputs - non_null_outputs`. That is sound only if
`non_null_outputs <= non_null_inputs`, and it is not. In the `ScalarList` arm
(`shred/build.rs:178-187`) an explicitly-null list value takes the `_` arm and
pushes `validity.push(value.is_some())` — **true**, because `obj_get` returns
`Some(node)` for an explicit JSON null (`arena.rs:189-197`, `view.rs:103-105`).
So `[{"tags":["a","b"]},{"tags":null}]` yields `non_null_inputs = 1` and
`non_null_outputs = 2`: `1u64 - 2u64` → **panic in debug** (the full local
gate), or in release a wrap to `u64::MAX` that flows into
`report.discarded_values` and into destination-persisted commit metadata.

No hint, no policy, default capabilities — `scalar_lists: true` on every
destination but Postgres. Nothing in the suite catches it: `shred_property.rs`'s
list keys (`:60-63`) are disjoint from its scalar keys and always emit arrays.

**Adopted correction**: a positional count (R2.4). Also uncovered: the audit's
own framing of the type-hint defect is **factually wrong** and must not be
copied into the close-out (R2.2).

### R0.3 — The parquet resume hash would have poisoned itself on first upgrade

Part A was defined over row groups `0..done`, but the only site that walked the
prefix was *verification*, which runs only when a check is present — and on the
first upgraded resume there is no recorded hash, so no check, so no walk. The
checkpoint would then record a value covering `start..done` instead of
`0..done`. Run 3 sees a genuine append, verifies over `0..done`, mismatches,
and returns a fatal "rewritten before the resume offset" — **permanently**, for
100% of pre-existing parquet cursors that resume mid-file. The design's own
risk note called this exposure "bounded and self-closing"; it is neither.

Two more: the `CURSOR_FORMAT_VERSION` 1→2 bump contradicts the in-tree
precedent (`etag` and `tail_hash` were both added to `FileProgress` in `91eab01`
with `#[serde(default, skip_serializing_if)]` and **no** bump; the constant has
one commit ever, `1a32b3a`) and, combined with a separately-proposed
`found > supported` gate, would make the bump increment **non-revertible**;
and the design omitted jsonl's `start > 0` arming filter (`jsonl.rs:155`), so a
hand-edited cursor with `done: 0` plus a hash reaches `done - 1` and
**underflows** instead of failing typed.

**Adopted correction**: build the descriptor unconditionally, no version bump,
mirror the arming filter (R3.3–R3.5).

---

## R1 — The record and the license (US1)

- **R1.1 LICENSE.** No license file is tracked (`git ls-files | grep -i
  license` is empty) while all 12 publishable crates inherit
  `license = "Apache-2.0"` (`Cargo.toml:25`). Add exactly one root `LICENSE`
  with the verbatim Apache-2.0 text and the boilerplate appendix filled in.
  **No NOTICE** — nothing in the tree is vendored or carries an attribution
  obligation. Per-crate packaging inclusion is verified in US9 with
  `cargo package --list`, because a root `LICENSE` is *not* automatically
  included in each `.crate` tarball (R5.4).
- **R1.2 CLAUDE.md.** Rewrite the 019 block in the COMPLETE style of 013–018
  with the recorded post-019 standing, the honest misses, and the US9
  re-scope; correct the 018 block's superseded medians and its "0.9x LOSS /
  optimization target" framing. The `<!-- SPECKIT START -->` / `<!-- SPECKIT
  END -->` markers are at `CLAUDE.md:1` and `:163`; the plan pointer inside
  them is what `/speckit-plan` rewrites, and the feature-history prose sits
  inside the same block.
- **R1.3 019's record.** Header `close-out.md:7` → COMPLETE; the paragraph at
  `:958` asserting T098/T099 are not done is superseded by the session
  recorded at `:1055` and becomes past tense pointing at it; the PI5 row at
  `:179` closes (T094 was re-scoped away with US9, `tasks.md:234`);
  `spec.md:7` Draft → the terminal status prior features use.
- **R1.4 FR-016 (019).** Amend in place with the measured inversion (+7.0%
  wall, 6.7 ms/batch, D-03 `close-out.md:358-386`) and the standing re-trigger:
  the offload becomes worth re-measuring only when a freed thread has work.
- **R1.5 PERF_ANALYSIS.md.** Add the EXECUTED banner in the exact shape
  `REFACTORING.md` and `BENCH_REFINMENT.md` already carry, naming the claims
  019 falsified: F3's under-one-core (measured CPU/wall 1.6), §F8's allocator
  wall-cost claim (D-05 factorial), F6's 12.41% recoverability (D-13), and the
  ~3.5× headroom (T088).
- **R1.6 Concurrency documentation (FR-018).** Goes in README plus
  `benches/RESULTS.md`: ~1.19M rows/s single pipeline, 8.43× at 8 concurrent
  pipelines, and the deliberate trade — a full-refresh load is one transaction
  on one connection by construction.
- **R1.7 The doc-truth list.** 19 verified edits. Those describing code this
  feature changes move to the increment that changes it; the rest land here.
  Notable: `bars.toml`'s header still says the dedup cell "carries NO bar"
  while the file defines one; `benches/README.md:39` says artifacts are
  format_version 2 (they are 3); `partition_by`'s doc claims Hive-style
  `col=value` directories while `final_tail` (`dest/layout.rs:61-64`) writes a
  bare value; four Makefile header claims contradict their recipes.

---

## R2 — Value fidelity on the shred path (US2)

- **R2.1 `Kind::UInt` observes `Utf8`.** `widen`'s catch-all `_ => Utf8`
  (`rdlt-core/src/types.rs:77`) makes Utf8 the join target for every
  non-Binary/Json type, so the change is monotone in both arrival orders and
  the `joined == Float64` escalation (`infer.rs:74`) becomes unreachable after
  any UInt observation. **No range condition** — a range-conditional rule would
  make the resolved type depend on value order, which is the bug class this
  lattice exists to prevent. Drop the `saw_inexact_int` assignment.
  **Row identity is unaffected**: `row_identity` runs on the raw node
  (`tape.rs:156-160`) and `render_scalar` already handles UInt
  (`canon.rs:42`), so `shred_identities.txt` stays byte-identical — including
  its `keyless_int_boundaries` case, which already carries `u64::MAX`.
  **Greenfield consequence**: `scalar_float64`'s `Some(Kind::UInt(u)) =>
  Some(u as f64)` (`build.rs:253`) becomes unreachable and is an inexact
  conversion of exactly the class the escalation refuses — delete it in the
  same change.
- **R2.2 The type-hint pin — and a correction to the audit.** Guard the
  shape-conflict arm on pinned-ness in `ColState::observe`. **The audit's
  stated consequence is wrong**: no child table is or ever was created, because
  `TapeShredder::new` seeds a hinted column as `ColState::Scalar(pinned)`
  (`tape.rs:75-77`) and `Kind::Object | Kind::Array => *self = ColState::Json`
  (`infer.rs:113`) fires before `is_child_table()` is read. The true
  before/after is *worse* and points the other way: today the array is
  **preserved verbatim as a Json column** (`build.rs:293-306`); after the fix
  the pin holds and the value is **NULLed and counted**. The close-out records
  it as verbatim-JSON → NULL, names `type_hints: {c: json}` as the escape
  hatch, and the pin asserts the stored value, not just the resolved type.
- **R2.3 Decimal precision.** Change `parse_decimal` to take `precision` and
  reject any value whose scaled magnitude reaches `10^precision`, at one point
  covering both the integer and string arms. Verified upstream: arrow's
  `with_precision_and_scale` validates only the *pair*, never a value
  (`arrow-array-58.3.0/.../primitive_array.rs:1615-1621`), and the value-level
  `validate_decimal_precision` is never called by rdlt.
- **R2.4 Misfit counting — positional, not differential.** Per top-level
  column, after building the array, count positions where a present, non-null
  input produced a NULL cell:

  ```rust
  let nulls = array.nulls();
  misfits += values.iter().enumerate()
      .filter(|(i, v)| v.is_some_and(|v| !v.is_null())
                    && nulls.is_some_and(|n| n.is_null(*i)))
      .count() as u64;
  ```

  Exact, cannot underflow, touches no builder; `validity.len() ==
  values.len()` in every arm so the index is always in range. **Recorded
  residual, deliberately not fixed here**: a non-array non-null value in a
  `ScalarList` column still becomes a valid empty list and is still not
  counted. That combination is unreachable through the shred path
  (`ColState::ScalarList` degrades to `Json` on any non-array,
  `infer.rs:146`), and changing `build.rs:184` would additionally turn today's
  explicit-null-becomes-`[]` into NULL — a data-visible change needing its own
  pin and its own close-out line. Do not smuggle it in.
- **R2.5 The discard reason must be typed or absent.** `reason` exists only as
  a free-form `String` on the transient `PipelineEvent::Discarded`
  (`event.rs:38-44`), while `TableReport` and `CommitCounters` merge both
  producers into one `discarded_values`. Introducing "policy discard" vs
  "unrepresentable value" as two *strings* would make substring-matching the
  only way to separate them — the pattern Principle V forbids. **Decision**:
  emit the new producer with the existing string and record explicitly that
  representability misfits are not separable in the report. A typed
  `DiscardReason` enum is the better end state but is a breaking public change;
  it is recorded as a named deferral with the trigger "the next feature that
  opens the version window for another reason". All four
  `LoadItem::Discarded` construction sites (`shred/mod.rs:135`, `:297`,
  `passthrough.rs:97`, and the new one) are updated together.
- **R2.6 Hint validation** lands in `validate_streams` (`runtime/run.rs:238-293`)
  as `RdltError::config` naming stream and column, enforcing
  `1 <= precision <= 38` and `0 <= scale <= precision`. The `scale as i8` wrap
  corner is real — `Decimal{precision:38, scale:200}` slips arrow's validation
  and nulls every row via `10i128.checked_pow(200) == None`.
- **R2.7 Identity corpus.** Reuse the 019 US6 mechanism unchanged: require
  `crates/rdlt-engine/tests/fixtures/shred_identities.txt` byte-identical
  before and after. Add no cases to the corpus in this increment.
- **Reachability caveat**: no shipped connector can put `LogicalType::Decimal`
  in `StreamSpec.type_hints` (file `source/config.rs:61-71`, rest
  `source/config.rs:407-417`), so R2.3/R2.6 pins are embedder-shaped and their
  red-before-green evidence is **synthetic** — recorded as such.

---

## R3 — Parquet resume integrity (US3)

- **R3.1 The rulebook.** Three planners over one guard sequence. The hole is
  one predicate: `rewritten_in_place` returns `None` when sizes differ
  (`cursor.rs:46-48`), parquet sets `ended_at_record_boundary: true` and
  `tail_hash: None` (`parquet.rs:70,73`), so a grown-and-rewritten file passes
  every tripwire.
- **R3.2 What is hashed.** One blake3 hex string over the consumed prefix
  only: per row group `0..done`, the loop index, `num_rows`, `total_byte_size`,
  `num_columns` (a `usize`, not `u32`), then per column chunk
  `dictionary_page_offset`, `data_page_offset`, `compressed_size`. All
  reachable from the footer the code already parses — **zero additional footer
  parses**. `byte_range()` is deliberately not used: it carries
  `assert!(col_start >= 0 && col_len >= 0)` and would panic on a hostile
  footer.
- **R3.3 Build it unconditionally** (the R0.3 correction). At the top of
  `parquet::read_task`, before the group loop, walk `0..task.start` from the
  already-held metadata and fold Part A into one `blake3::Hasher` — on every
  task with `start > 0`, whether or not a check is present. Verification, when
  a check *is* present, forks that hasher and compares. Stated as a live
  invariant at the site (Principle VI).
- **R3.4 No version bump** (the R0.3 correction). Add
  `row_groups_hash: Option<String>` at `CURSOR_FORMAT_VERSION` 1 with
  `#[serde(default, skip_serializing_if = "Option::is_none")]`, exactly as
  `etag` and `tail_hash` were added in `91eab01` (`types.rs:40-45`). Neither
  `FileCursor` nor `FileProgress` sets `deny_unknown_fields`, so old→new
  decodes to `None` and new→old ignores the key; jsonl and csv cursor
  documents stay byte-identical — which a bump would *not* have preserved,
  since `format_version` is serialized unconditionally (`cursor.rs:30-31`).
  Migration note: additive optional field, no version change, parquet entries
  carry no integrity value until the next checkpoint rewrites them.
- **R3.5 Arm and defend.** Emit the check only when `done_units > 0` (jsonl's
  filter, `jsonl.rs:155`). `read_task` returns a typed fatal — never an
  arithmetic operation — on `groups == 0` with a check present,
  `start > total_groups`, a negative offset or size in the footer, `end` past
  the file length, or a short read. The currently-silent empty loop at
  `parquet.rs:46` becomes a typed fatal in the same change.
- **R3.6 Recorded narrowing (FR-002).** The hash covers absolute offsets, so a
  grow-by-rewrite performed by a *different* writer or different
  `WriterProperties` (pyarrow, Spark, DuckDB, a compression change) preserves
  the logical prefix but re-encodes it and will now be refused. That is
  defensible — a whole-file re-encode is not an append — but it is a
  behavioural narrowing and is recorded as a deviation, with pin P5 bounding
  it deliberately rather than leaving it to be discovered.
- **R3.7 Pins.** P1 legitimate append resumes; P2 prefix rewrite refuses
  typed; P3 no-check first upgrade; **P4** the R0.3 regression — pre-fix
  cursor, then two successive appends, must reach 9 rows (red under the
  original design, green under the correction); P5 the R3.6 narrowing.
  `ArrowWriter::flush` early-returns when `in_progress` is `None`
  (`parquet-58.3.0/.../arrow_writer/mod.rs:439-442`), so `flush()` + `close()`
  does not emit a trailing empty row group and the fixtures are exact.

---

## R4 — File destination ownership, classification, retention (US3)

- **R4.1 `keys_of_table`**: strip against the known table root instead of
  searching; a listed key not under the listed prefix becomes a typed fatal
  (silently dropping it would make the ownership listing incomplete). The
  local arm does not share the defect.
- **R4.2 Ownership**: replace `owns_tail(tail, ext, partitioned)` with
  `owns_part(tail)` — last segment starts `part-` and ends with *any*
  extension this destination can write, at depth 1 or 2, unconditionally.
  Source the extension set from a new `DestFormat::ALL` beside `extension()`
  so the exhaustive match forces a new variant to be considered, and pin
  `ALL`'s completeness against the schemars-generated enum.
- **R4.3 Commit log**: bound it. Both readers key on the session's own load
  id, so retain the current load's receipts plus the one immediately preceding
  load. No `LAYOUT_FORMAT_VERSION` bump.
- **R4.4 Cursor entries**: do **not** prune and do **not** add a knob — take
  FR-038's documented-growth branch, state the rule and its cost, and pin it
  so an accidental future prune fails loudly instead of silently duplicating
  rows under Append.
- **R4.5 `is_recoverable`**: invert to an allow-list —
  `object_store::Error::Generic` and nothing else — then route
  `S3Location::classify`'s severity through it so exactly one place decides
  transient-vs-fatal for all three call sites.
- **R4.6** CSV inferred-Bool mirrors the declared-hint arm; `resolve_files`
  distinguishes "is a directory" from "does not exist" with an actionable
  hint; the temp fetch directory becomes an RAII guard and the manual cleanup
  is deleted in the same change.

---

## R5 — REST robustness (US4)

- **R5.1 Timeout**: one field, `request_timeout_secs: u64`, `#[serde(default)]`
  = 300, rejecting 0 in `validate` (0 must not mean "disabled" — SC-007
  forbids any configuration producing an unbounded wait). Take
  `ClientBuilder::timeout` (the total request deadline) and only that.
- **R5.2 One client**: build exactly one `reqwest::Client` in
  `RestSource::build`, pass a clone to the auth provider. `build` becomes
  fallible; all four callers already return `Result<_, ConfigError>`.
  `Client::new()` at `client/mod.rs:56` and the per-fetch client at
  `auth.rs:113` are both deleted (FR-041).
- **R5.3 POST pagination**: reject at config validation — `method == Post` AND
  a non-object body AND a paginator among the four that produce non-empty page
  params.
- **R5.4 Retry-After date form**: `httpdate = "1"`. **Registry facts**:
  `httpdate 1.0.3` is *already in `Cargo.lock`* (pulled by `hyper 1.10.1`,
  which reqwest already requires), so the direct edge costs **zero new tree
  entries**; it is a new edge for `rdlt-connector-rest` only. Hand-rolling was
  priced and rejected — the crate is in the tree either way, and date parsing
  has real edge cases. Both forms pass through one site, so the
  `retry_after_cap` clamp applies identically.
- **R5.5 Token generation**: reuse the existing single-flight mutex; replace
  `Mutex<Option<CachedToken>>` with `Mutex<TokenState { generation, cached }>`
  and thread `Option<u64>` through `attach`/`send`. No new synchronisation
  primitive.
- **R5.6 Path encoding**: encode at exactly one site (the path), hand-rolled,
  RFC 3986 unreserved pass-through. Query and body values stay raw because
  serde and reqwest already encode those.
- **R5.7 Credential blocklist**: extend to 13 exact, case-insensitive names.
  No substring rule; not extended to query params.
- **R5.8 The POST-body hash is not a performance item** — delete the term.
  `format!("{:?}", body)` at `driver.rs:121` hashes a value that is provably
  constant across every page of a sequence into a set scoped to that same
  sequence, so it cannot change a single guard outcome. Ship it with no
  performance claim and no measurement.

---

## R6 — Schema contracts (US5) — the corrected design

- **R6.1 Take (A), extend enforcement across runs**, via a **read-only
  baseline carried alongside** the still-within-run registry — never by seeding
  the registry. `StateDoc` gains `schemas: BTreeMap<TableName, TableSchema>`
  and **loses** `schema_hashes`, which nothing reads (`apply.rs:31-34` writes;
  grep finds no reader) and which could only ever prove *inequality* — it is a
  blake3 digest of the whole canonical schema (`rdlt-core/src/schema.rs:98-104`)
  and cannot produce the `AddColumn`/`WidenColumn` the policy layer resolves
  on. (B) — narrowing the contract — was rejected because essentially every
  drift a user cares about appears at a run boundary, so a contract that resets
  every run is close to worthless while the destinations quietly apply the
  drift as additive DDL.
- **R6.2 Two diffs on every drain** (the R0.1 correction). Delete the
  special-cased `establishment_changes`. On both paths:

  ```text
  emit      = registry.diff(&observed)                       // drives LoadItems, unchanged
  established = union(registry.get(&table), baseline.get(&table))
  governed  = diff_against(established, &observed)           // drives policy
  ```

  `governed.is_empty()` → nothing is policed and every change in `emit` is
  Evolve (this also removes the panic path). `governed == [CreateTable]` →
  exempt iff bootstrapping, else `policy.action_for(&table, None)`. Otherwise
  per governed change as today. Run N+1 drain 2 re-sighting a baseline column
  yields `governed == []` → the `AddColumn` is emitted and the destination's
  `ADD COLUMN IF NOT EXISTS` no-ops. Within-run drift is preserved because the
  union includes the registry, so `us4_policies.rs:39-64` still passes.
- **R6.3 The baseline is a monotone union**, not "the last run's schema":
  `apply_delta` merges rather than overwrites — baseline columns first in
  baseline order with types joined, then new columns appended. Without the
  union the trap merely moves one run later (run1 {id,v} → run2 {id} would
  overwrite down to {id}, and run3 would report `v` as new). The union is also
  the truthful model of destinations, whose DDL is additive and never drops.
  `nullable` and `provenance` participate in the content hash
  (`schema.rs:27-31`), so the merge rule for them is stated explicitly rather
  than left to chance.
- **R6.4 Stamp the version** (the R0.1 correction). Nothing in rdlt-engine
  ever assigns `format_version`, so a 1→2 bump would be inert and a v1 engine
  would hit a serde "missing field" routed through `fatal` — a rendered-string
  failure, not the typed `UnsupportedStateVersion`. Assign
  `STATE_FORMAT_VERSION` where the recovered document is adopted
  (`run.rs:349-350` and the replay leg at `:387-389`) and fix the hardcoded
  `format_version: 1` in `rdlt-testkit/src/fixtures.rs:51`.
- **R6.5 Child-table policy inheritance** (the R0.1 correction). Resolve a
  child table through its parent chain before falling to `default`: try
  `per_table[child]`, then `per_table[root]`, then `default`. Without it,
  freezing `t` does not freeze `t__items` and FR-030 is unmet.
- **R6.6 The v1-document hole is decided, not footnoted.** A recovered
  document with an empty `schemas` under a non-Evolve policy is
  indistinguishable from "never established". Refuse with a typed variant
  naming the pipeline and telling the operator to re-establish once under
  Evolve; the alternative is a silent one-run Freeze bypass on first upgrade.
- **R6.7 Table-level discard**: `enforce_discards` skips changes with no
  column (`shred/mod.rs:246-248`), so a refused table creation silently
  degenerates to Evolve today. The fix closes it explicitly.
- **R6.8 Cost and risk.** No new dependency. **`StateDoc` is a public field of
  a public type in the semver-sacred `rdlt-core`, so deleting `schema_hashes`
  is a breaking change** — see the plan's version-window decision. Size: a
  serialized `ColumnDef` is ~100 bytes, so a 20-table/50-column pipeline puts
  ~100 KB in a document rewritten on every commit; for Iceberg that document
  is a table property, so it is metadata growth per commit. The plan records a
  measured byte count rather than a threshold gate.

---

## R7 — Iceberg nested types (US6)

- **R7.1 The audit's claim is CONFIRMED and stronger than "plausible".** For
  any catalog whose create path normalizes IDs through
  `TableMetadataBuilder::from_table_creation`, the divergence is **guaranteed**
  for every schema in which a struct or list column is followed by another
  top-level column. `NestedField` derives `PartialEq` over all fields including
  `pub id: i32` (`iceberg-0.10.0/src/spec/datatypes.rs:529-550`); `StructType`'s
  eq is `self.fields == other.fields` (`:503-507`); `ListType` derives it over
  `element_field` (`:689-695`). The spec's US6 prose is amended accordingly.
- **R7.2 Structural comparison** lives in `dest/schema.rs` — the module that
  assigns the IDs, so the invariant and its insensitivity sit together — with
  the drift *policy* staying in `ensure.rs`. Recursive, because the engine's
  `ColumnType::Struct` is recursive.
- **R7.3 Nullability drift confirmed ignored**, and it detonates later inside
  `align` as a generic batching error (`session.rs:263-264`). The rule is
  **asymmetric**: `live.required && !wanted.required` is drift (the write
  cannot honour it); the reverse is tolerated.
- **R7.4 Red-before-green without a container.** A skipping test is green, so
  a skip-not-fail live cell can never be *demonstrated* to fail on the pre-fix
  build and is inadmissible as the FR-001 pin. `dest/test_support.rs:20-51`
  already builds its fixture through the very normalizer that causes the
  defect — parameterize it and capture the red pin as a container-free unit
  test. The live Polaris cell is confirmation, not evidence.
- **R7.5 The live cell** is a new `tests/nested_types.rs` running the engine
  **twice** against the same config, plus the existing pyiceberg read-back leg.
- **R7.6 The Polaris tag cannot be determined from the repository** — one
  site, nothing recorded anywhere, no local cache. Pin it at T001 by live
  probe exactly as 017 did for rustfs (pull, read
  `org.opencontainers.image.version` and the digest off the pulled image, edit
  the single site, prove by running the suite twice green). Pin by digest if
  no immutable version tag is published. **Do not invent a tag.**
- **R7.7 Both phase-2 doors are still closed, with registry evidence.**
  `Transaction` in iceberg 0.10.0 exposes exactly eight actions and the module
  directory contains no overwrite/rewrite/delete action file
  (`src/transaction/mod.rs:135-170`). Record as re-probed; do not open scope.
- **R7.8 Nested additive evolution** (a struct that gains a child) is refused
  after the fix — a strict improvement over today, where *every* struct
  re-ensure is refused, but a real ceiling for JSON sources, since the engine
  widens structs by appending children. Recorded as a named deferral with a
  fired-trigger condition, and the typed error says precisely what happened.

---

## R8 — Engine hardening (US7)

- **R8.1** Postgres `encode.rs`: **delete** the Decimal arm at `:42-44` (and
  the redundant Time arm at `:47-49`) so both fall through to the
  representation match at `:63-66`, which reads the scale off the array — the
  scale the payload is actually stored at — and already applies the
  negative-scale typed fatal. Deleting is truer than patching: the arm has no
  reason to exist.
- **R8.2** The promise being kept is feature 019's **FR-021**
  (`019/spec.md:568-570`). Three edits inside the `field!` closure so a
  rejected value aborts before any length prefix is backfilled: Time64 bounds,
  the date epoch shift in i64/checked arithmetic, and `unwrap_or(i16::MAX)` →
  `expect`.
- **R8.3** DuckDB: `fatal` → `classify` at nine enumerated sites in
  `dest/commit.rs`; deterministic classes still come out fatal through the
  classifier.
- **R8.4** Tracing: `Instrument` is reachable with **zero** new dependencies
  (`tracing-0.1.44/src/lib.rs:956`; `tracing-attributes` already in
  `Cargo.lock:5688`). Exactly four span sites workspace-wide; the two async
  ones migrate, the two `spawn_blocking` `enter()` calls stay. No test pins
  span attribution today — verified by inspection and recorded as such.
- **R8.5** WAL residue: split the outcome. `Scan::Nothing` keeps meaning "no
  manifest"; a new sibling variant is returned when a manifest *was* read but
  produced nothing replayable, and `recover_wal` clears — the same call the
  `Damaged` and `Unsupported` arms already make.
- **R8.6** Exactly five sites reclassify to `RdltError::internal`
  (`lowering.rs:97,117,129`, `passthrough.rs:157`, `shred/mod.rs:211`). Every
  other `RdltError::config` in the engine stays.
- **R8.7** CLI: `Internal => 70` (EX_SOFTWARE) *and* `_ => 70`, because an
  unknown variant is "this binary cannot classify what happened", never "edit
  your YAML". Split file I/O out of `Usage` into a new `Io` → 74 (EX_IOERR).
- **R8.8** Nested decimals: **recurse** — make lowering total for `decimal` in
  both `lower_column` and `flatten_array`. The test vehicle is the testkit
  memory destination with custom capabilities.
- **R8.9** `normalize_ident`: shorten the suffix to fit the bound rather than
  changing `IdentRules`; the public `ident_hash` keeps its 4..64 clamp.
- **R8.10** Silent failures: log via `tracing::warn!`, propagate none, except
  the bench runner, where a read/parse failure on an *existing* report becomes
  a hard error so "absent" means genuinely absent.

---

## R9 — The gate (US8)

- **R9.1 The mutation cache is 100% dead** across the 017 renames — plan a
  **fresh full run**, not an iteration. `rm -rf mutants.out mutants.out.old`
  then `RDLT_TESTKIT_FORCE_NO_CONTAINERS=1 TARGET=mutants make test` in
  distrobox. `--iterate` stays in the recipe: it is a no-op on a fresh run and
  is the right resume mechanism if the run is killed, which is the recorded
  failure mode.
- **R9.2 Budget 60–90 min** (80–115 with sqlcore), expect **~70 survivors**
  (band 50–120) plus 10–15 timeouts. Triage order: every mutant in a file this
  feature changed gets a terminal disposition, no exceptions; then the named
  ones; the remainder may be recorded as a named deferral with its trigger
  rather than left untriaged.
- **R9.3** Add sqlcore to `examine_globs` (+~205 mutants, +21 min).
- **R9.4–R9.13** Each named pin has an exact shape. Notable: `byte_size` is
  pinned by its **consequence** (drive a real byte channel and assert
  backpressure), not its value; the misnamed WAL test is **split**, not
  patched; the decimal grammar table lands in US8 *after* US2 ships the
  precision refusal, and the precision rows belong to US2; `saw_cancelled` is
  unit-tested on `drain_loader` directly rather than via a new testkit source;
  `SchemaPolicy::freeze()` is **kept** and pinned, not deleted.
- **R9.14 Labels exist today** — `testcontainers 0.23.3` exposes
  `ImageExt::with_label` (`src/core/image/image_ext.rs:76`, impl `:214`). Label
  every start site and add a reclaim verb.
- **R9.15 Ack-loss is cheap, but the audit's shape is wrong.** Do not build a
  "drops connection after COMMIT" fail-point action. Add a new **crash point**
  `pg.tx.acked` immediately after the unit commit succeeds and before the unit
  is taken (`dest/commit.rs:894-901`), registered in `FAIL_POINTS`.
- **R9.16 Flake data**: a nextest `[profile.flake]` with retries and JUnit
  output, plus a small tool that appends flaky classifications to a committed
  log — not a testkit counter.

---

## R10 — Publish readiness (US9)

- **R10.1** 12 of 13 members are publishable (only `rdlt-bench` sets
  `publish = false`), all at `0.2.0`.
- **R10.2** All 12 are missing `keywords`, `categories`, `readme`,
  `documentation`. **Do not** put `readme` in `[workspace.package]` — an
  inherited `readme` resolves relative to the *workspace* root, not the crate.
- **R10.3** Two descriptions are wrong (rdlt-cli says TOML; it parses YAML at
  `main.rs:135`. rdlt-connector-file says "file source"; it has been
  source+dest since 015), one incomplete, one stylistically inconsistent; the
  other nine are accurate.
- **R10.4** A root `LICENSE` satisfies the repository and GitHub detection but
  **not** the `.crate` tarballs — per-crate inclusion is verified with
  `cargo package --list`.
- **R10.5** `#![warn(missing_docs)]` (warn, not deny; per-crate source
  attribute, not `[workspace.lints]`) on the three semver-sacred crates.
- **R10.6** Nothing builds documentation today. Add a `docs` verb running
  `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features`
  and wire it into `check`.
- **R10.7** By inspection the facade is genuinely narrowing-safe; the one
  confirmed per-crate breakage is a rustdoc link, not a compile error.
- **R10.8** The semver job covers 2 of 12 crates; changing `-p rdlt-core -p
  rdlt-connector` to `--workspace` is exactly FR-070 and cannot rot.
  **Verification is CI-blocked and recorded as unperformed.**
- **R10.9** `fuzz/Cargo.lock` staleness confirmed (fuzz/ is a standalone
  workspace); the `tools/interop/.gitignore` no-op confirmed;
  `CARGO_TARGET_DIR` is ignored at two sites; `bench-setup.sh` has an
  unbounded readiness wait; four Makefile header claims contradict their
  recipes.

---

## R11 — Recorded deferrals (US10)

- **R11.1 D17**: the generic core goes in `rdlt-connector` (not `rdlt-core`),
  and `crates/rdlt-engine/src/runtime/channel.rs` is **deleted** in the same
  change. Six additive public items; the parameterization covers the message
  cap, sender `Clone`, `ByteSized` sizing, and the close-wake.
- **R11.2 Lowering parity**: fix `lowering.rs:138` (hardcoded `true`) to the
  schema side's rule, then pin the parity as **exact `arrow::datatypes::Schema`
  equality** over generated schemas × the four capability combinations, using
  zero-row batches. Achievable with no allowances. Severity recorded honestly
  as latent-unreachable.
- **R11.3 D19 is REJECTED with a recorded reason** — its premise changed (it
  is now a quartet, and the code it names is not a correctness invariant) —
  with the shape that would close it and a new trigger.
- **R11.4 `DestSpec::File` embedding is TAKEN**; the pg and duckdb variants
  **cannot** be (both connectors are builder-shaped, with no deserializable
  destination config) and are re-recorded with a trigger.
- **R11.5** sqlcore: take `flagged_roots` through the dedup seam and move
  `create_index_sql` + the duplicate-merge-key diagnosis (adding the golden
  pin they lack today); **re-record** the `ensure_table` choreography
  extraction with the trigger "the next feature that adds a third SQL
  destination".
- **R11.6 All dependency-hygiene claims hold.**
- **R11.7 `WalRecord::Segment.rows` gets a consumer** rather than being
  deleted: a pass-1 replay cross-check that warns and degrades to
  re-extraction on mismatch. No `WAL_FORMAT_VERSION` bump.
- **R11.8** 017's eight residuals: take three, fold one, re-record four.

---

## R12 — The performance queue (US11)

Every item is measure-then-take. Instruments verified available in this
environment unless noted.

- **R12.1 EXPLAIN** is manual via `auto_explain` on the fixture database;
  rdlt-bench is **not** extended.
- **R12.2 Blocked-time**: **tokio-console is rejected** (not reachable without
  a new dependency and a rebuilt binary) and sched-tracepoint off-CPU profiling
  is **verified unavailable** from this container. The primary instrument
  becomes a throwaway build instrumenting the ~6 await sites; host-side `perf`
  is an optional confirmation.
- **R12.3 D-08**: throwaway prototype, iai gate, byte-identity oracle, then a
  cell A/B with a stated take threshold.
- **R12.4 D-05**: step 1 is a free `perf record` (verified to work here) with
  an explicit stop condition; mimalloc is priced but not committed.
- **R12.5 019's D2 still binds.** It rejected precisely "skipping the log
  entirely for all-Replace runs", and its ground was not the size of the win
  but that recovery becomes a full source re-extraction — expensive against a
  rate-limited or paid-per-request source. The all-Replace narrowing does
  **not** escape that. The A/B is still run to record the residual cost; an
  automatic skip is not taken.
- **R12.6 S3 skip-fetch** is structural, not a tuning guess: thread the
  already-decoded cursor into `resolve_inputs` and skip etag-matched complete
  objects. **Sequenced after R3** so resume integrity lands first.
- **R12.7 D18**: `valgrind --tool=dhat` needs nothing new (valgrind is already
  a hard prerequisite of the iai gate). Heaptrack is not installed and is not
  worth installing.
- **R12.8 WAL recovery blocking**: verified with a starvation test, not a
  timing — there is no throughput claim.
- **R12.9 The reqwest 0.12/0.13 double tree cannot be deduplicated** —
  verified impossible without an upstream version change. Recorded as
  rejected-with-reason plus a re-trigger.
- **R12.10** The remaining smaller items each get an explicit disposition;
  three need no new measurement.
- **R12.11** All measurements are recorded in `close-out.md` as D-entries
  following the 019 pattern, with negatives carrying a site comment so they
  are not retried a third time.

---

## Unresolved

Carried into implementation with the probe that settles each; none blocks
planning.

1. **The Polaris image tag** (R7.6) — settled at T001 by live probe. Do not
   invent one.
2. **The actual mutation survivor count and wall time** (R9.2) — settled by
   running it; the triage rule already covers a larger-than-expected list.
3. **The `missing_docs` item count** (R10.5) — a heuristic scan sized it; the
   real number appears when the lint first runs.
4. **Whether host `perf_event_paranoid` can be lowered** (R12.2) — settled on
   the host; the throwaway-instrumentation fallback does not depend on it.
5. **The state-document size for the widest pipeline** (R6.8) — recorded as a
   number, not gated on a threshold.
6. **Every performance outcome** — that is the point of US11; FR-075 requires
   the number, not a prediction.
