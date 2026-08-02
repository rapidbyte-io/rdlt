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

- Branch created; contract inventory committed; this plan written.
- NEXT: build in the established rhythm (incremental commits, offline
  tests green at each step): config → client → schema/dialect → load
  → connector/Shell → fresh suite (kit + goldens + probes +
  differential + recovery + strategies + sweep beside gen 1) → review
  rounds to terminus (docket S1-S13; headliner S1 = shared-table, the
  029/030 analogue) → gates twice clean (baseline 1024; counts
  predicted and verified; container hygiene by test image/label only —
  never the dev toolbox).
