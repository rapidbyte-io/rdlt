# 031 — DUCKDB SECOND GENERATION (`rdlt-connector-duckdb-v2`)

Owner goal: "design and plan and rewrite rdlt-connector-duckdb in
rdlt-connector-duckdb-v2 (greenfield/clean layout/from scratch clean
implementation) — similarly current to postgres/rest."

Branch `031-duckdb-v2` off main @ 961d0b77 (001-030 merged: the sdk
trio and the postgres/rest/snowflake/iceberg/file second generations
are all live under their real names).

THE DISCIPLINE (binding, learned across 025-030): TRUE greenfield —
generation 1 is the reference implementation for its CONTRACT only;
near-verbatim transcription is copying and is rejected (memory
`rdlt-rewrite-means-no-copying`). Frozen spellings are identical
because they are contracts. The review loop treats "found an inherited
defect" as its success condition.

## The authoritative contract inventory

`specs/031-duckdb-v2/contract-inventory.md` (committed; an exhaustive
read of generation 1 at 961d0b77) is D3's substance. THE HEADLINE
CORRECTION it makes: duckdb classification is NOT structured
code/extended_code (the 017-era claim) — the structured channel is
degenerate (`ErrorCode::Unknown`), and both classification keys are
MESSAGE PREFIXES (`"IO Error"` → transient, `"Constraint Error"` → the
duplicate-merge-key diagnosis), probe-pinned with named escape hatches.

THE LOAD-BEARING FACTS the design bends around:
- Everything merge-shaped is SQLCORE'S, executed not owned: the
  session drives `plan_commit`/`CommitContext` with
  `FullLoadPublish::Staged` (DirectToTarget is a recorded deferral),
  `ensure::schema_steps`/`merge_steps`, `build_merge_plan`/
  `render_arm`; `DuckDialect` overrides exactly TWO hooks
  (`arrival_order` = `rowid`, `clear_table` = `DELETE FROM`),
  everything else trait defaults backed by six probes.
- Exactly-once: TEMP-table stages (die with the connection — orphan
  reclaim for free; hence `stage_name` = `_rdlt_stage_{16-hex}` of the
  table alone, NOT pipeline-scoped — safe ONLY because stages are
  session-scoped); meta tables `_rdlt_state`/`_rdlt_commits` ensured
  at open with byte-frozen DDL; receipt + durable Replace guard read
  from `_rdlt_commits` INSIDE the one publish transaction; replay =
  RunScript (stage truncation only) + re-marks `single_unit_done`.
- Gen 1 predates the sdk: no config document, no config_schema, no
  write-before-ensure refusal. v2 changes all three DELIBERATELY (the
  sdk choreography's refusal supersedes the appender error — the
  028/029/030 precedent, recorded).
- The legacy `rdlt_ix_`→`rdlt_ux_` unique-index DROP shim is
  PERSISTED-FORMAT migration (user databases carry the old name) and
  must survive the rewrite.

## Decisions

**D1 — Born on the sdk.** `DestinationConnector` + `Backend`
(destination-only; a future source slots beside). Dependencies: the
sdk (SPI via its `spi` re-export) + `rdlt-connector-sqlcore` (THE
recorded exception, as postgres/snowflake) + `duckdb` itself
(workspace pin `version = "1"`, `bundled` + `appender-arrow`; the
workspace arrow major is COUPLED to it). sdk `test_dependency_rule`
gains `("rdlt-connector-duckdb-v2", &["rdlt-connector-sdk",
"rdlt-connector-sqlcore"])`.

**D2 — A config DOCUMENT is born.** Gen 1's configuration surface is
a builder + the facade's `DestSpec::Duckdb` YAML arm. v2 derives
`destination::Config` (sdk `config::Document`, parse-then-validate,
schema attached — closes docket S11) covering EXACTLY that
vocabulary: `path` (required), `memory_limit`, `merge_strategy`
(sqlcore's enum), `tables` (sqlcore `TableOptions`), `extensions`,
`settings`. Typed `ConfigError` with the sdk from-text framings
(`invalid duckdb destination YAML/JSON/config: {0}` — a NEW surface;
gen 1 never parsed text). The bare-identifier refusal and
setting/extension validation move INTO the Document gate where they
can (spellings frozen); eager application stays at connect. Builder
parity via `Config` with_* methods; the facade arm ports to
`destination::Config` + `Shell::new` at swap (D6).

**D3 — Frozen surfaces.** The inventory in full. Notably: every §2
message spelling; the classification rulebook verbatim (prefix keys,
fatal default, no RateLimited); crash IDs `duck.append` /
`duck.tx.commit` at the same placements including the `!replayed`
guard on the commit point; the meta-table DDL byte-identical (a
persisted format); the legacy unique-index DROP shim; the §3.2 type
lowering table including Json's VARCHAR stage leg; the two-phase
ensure shapes (Append+Staged always-both-legs; widen = `SET DATA
TYPE` no USING; validity columns no NOT NULL) golden-pinned AS DATA;
`stage_name`; the dialect's two overrides; capabilities
merge/structs/scalar_lists/json_type/decimal all true +
IdentRules::default(), now WITH config_schema (recorded delta);
`_rdlt_state` keyed by the RAW pipeline string (no hash scope).

**D4 — Fresh design.** lib.rs façade; modules by noun under
`destination/`:
- `config.rs` — the Document + validation (D2).
- `client.rs` — THE duckdb-rs boundary (028's precedent): the shared
  database handle (`Arc<Mutex<Connection>>`), session-setup replay on
  every clone, classify/is_constraint_violation/fatal, execute/query
  seams. Library types stop here.
- `schema.rs` — type lowering, `create_table_sql`, and the two ensure
  phases rendered as data (the golden-pin seam).
- `dialect.rs` — `DuckDialect`, exactly the two overrides.
- `load.rs` — the Backend: the ONE-transaction commit mapped onto the
  sdk hooks per D7.
- `connector.rs` — `DuckDb` (DestinationConnector), capabilities,
  FAIL_POINTS, the testhook (count_rows/query_string + the sqlgen
  pin seam).
- `mod.rs` pure TOC + `Shell` alias + the sqlcore vocabulary
  re-export (facade parity).

**D5 — Parity = the census as a fresh suite.** 51 default + 2 sweep
tests across 11 binaries → the house layout (integration.rs +
cases/test_<noun>.rs + the sweep binary). Carried: the golden ensure
pins (as data), the six dialect probes + settings-replay probe, the
cross-destination DIFFERENTIAL oracle vs postgres (container-gated
skip-not-fail; it lives in THIS crate deliberately), the recovery
pins (durable Replace guard, replay re-marks), the strategy matrix,
native-JSON proof, classification probes with their escape-hatch
wording. NEW: the sdk conformance kit certifies the Shell. The
scanner census row is `("rdlt-connector-duckdb-v2", 2)` during
coexistence (the swap renames it back).

**D6 — Coexistence.** `publish = false`, consumed by nothing; the
swap (delete gen 1, rename, port the facade's `DestSpec::Duckdb` arm
to `destination::Config` + `Shell::new`, re-point the engine sweep
and file's e2e, collapse the Makefile line, rename the census row) is
the owner's decision.

**D7 — The receipt mapping.** The sdk choreography calls
`existing_receipt` BEFORE publishing: v2 answers it by reading
`_rdlt_commits` (`receipt_exists_sql`). The `replay` hook carries gen
1's replay disposition — RunScript: `plan_commit` with
`replayed=true` inside a transaction (stage truncation and nothing
else) and the re-marking of `single_unit_done` from the script's
marks. `publish` is the fresh path: the ONE transaction (in-tx
receipt re-probe as defense in depth, durable Replace guard,
full-feed stage probes, planner-owned steps, `duck.tx.commit`, then
marks applied only after commit). `read_state` verbatim.

## STATUS

- Branch created; contract inventory committed; plan written.
- BUILD COMPLETE (config Document / client boundary / dialect /
  schema seam / Load Backend / connector + Shell + testhook + sdk
  ADOPTED entry), incremental commits, clippy clean.
- SUITE COMPLETE: 59 offline + the 24-cell failpoints crash_sweep (4
  strategies × 2 points × 3 actions, armed-fire matrix pinned), wired
  into TARGET=sweep beside gen 1; the sdk conformance kit CERTIFIES
  the Shell (new over gen 1); the cross-destination differential
  oracle ran LIVE (6 cells vs postgres containers); golden ensure
  pins as data; the six dialect probes; census row + ungated registry
  twin. THE SUITE'S TWO CATCHES: (1) a second READ-WRITE instance
  beside a live one replays AND TRUNCATES the live WAL (measured — a
  mid-test count made the next session's committed row invisible);
  testhook oracles went READ-ONLY at once, the full fix below. (2)
  sqlcore validate_merge never checked merge-key columns (a ghost key
  column surfaced as a raw Binder error).

## REVIEW ROUNDS

**Round 1** — two lenses (docket audit S1-S13 + the suite catches;
fresh-eyes + fidelity + anti-transcription), then one fix pass, all
guards red-proven:
- N2 (the WAL-truncation hazard, PUBLIC surface): a second in-process
  open of one database file is refused TYPED via a process-global
  canonicalized-path registry (pre-open check refuses before the
  truncating act; post-open claim closes the not-yet-created race;
  clones share the claim, last drop releases; sequential re-open
  legal). Cross-process stays the transient lock refusal — recorded.
- A2: the commit probes moved INSIDE the one transaction (D7 as
  written); replay/publish share run_commit, the in-tx receipt probe
  decides replayed; the 24-cell sweep is the net.
- S2 (+A3 disposition): uniform commit-path classification — the
  three fatal read sites classify; recorded as a deliberate v2
  refinement over gen 1 (a locked-file IO Error mid-commit rides the
  retry budget instead of aborting).
- S1 partial: re-ensure that DROPS ensured columns refused typed
  (append-only evolution makes a column regression = colliding
  streams or a harness defect). The FULL normalized-name collision
  gate needs the engine's rename knowledge — STANDING OWNER RECORD
  (engine scope), same family as 029/030's headliners.
- S4: the positional stage append guarded — batch field names must
  prefix-match the ensured order; refusal names the first divergent
  position.
- N1 (sqlcore, shared with postgres): validate_merge refuses a merge
  key column absent from the schema; identity/child tables skipped
  (lineage keys are legitimate); postgres 232/232 live after.
- S7 checked narrowing; S6 = the sdk refusal supersession recorded
  (028-030 precedent); S3 (within-run widen) and S5 (IO-prefix
  transience incl. deterministic paths) standing records; S8/S10/S11/
  S13 resolved by construction; S12 n/a (no examples).
- Anti-transcription: two comment residues reworded, create_table_sql
  re-flowed (output byte-identical, goldens green); the rest of the
  crate judged genuinely re-derived.

**Round 2** — terminus verification of the fix commit (record below).

- NEXT: gates twice clean (baseline 1024 + this crate's offline
  tests; counts predicted and verified; hygiene by test image/label
  only). Crate coexists unconsumed; swap = owner decision (D6).
