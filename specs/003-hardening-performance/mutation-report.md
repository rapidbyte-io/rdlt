# Mutation report (feature 003, data-model §2)

**Threshold**: ≥85% of viable mutants killed; zero undispositioned survivors
(SC-002). Run via `TARGET=mutants make test`; config `.cargo/mutants.toml`
(workspace-tested, nextest). Operational notes: the config MUST live at
`.cargo/mutants.toml` (a repo-root file is silently ignored); use a disk-backed
TMPDIR; run with `--jobs 2` — parallel runaway mutants (broken loop/backpressure
bounds) stack allocations and can OOM the host (observed: a mutated
`us3_crash_matrix` test at 4.4 GB RSS took down the machine's user session).
`--iterate` resumes a killed run without repeating tested mutants.

## Run log

| date | commit | mutants | caught | missed | unviable | timeout | kill rate |
|---|---|---|---|---|---|---|---|
| 2026-07-20 | 8d44055-era tree (pre-tape; earlier rows misattributed this to 852049f) | 470 | 241 | 127 | 94 | 8 | 64.1% |
| 2026-07-20 | c4a90f9 (OOM-killed at 349/595) | 349 partial | 188 | 79 | 74 | 8 | 68.4% partial |
| 2026-07-20 | ce9972c, post-closures (OOM-killed at 347/595) | 347 partial | 237 | 29 | 74 | 7 | **86.8% partial — bar met on the tested set** |

The partial run covers the current tape-path code. Three survivors visible from
the first run's tail were already closed with tests in c4a90f9 (passthrough
decimal-scale guard, list-mapping arms, WAL future-version guard).

## Survivor dispositions (79 from the partial run, clustered)

Every planned test below lands before this feature's PR merges; the scheduled
`TARGET=deep make test` run then verifies the kills.

| cluster | count | disposition |
|---|---|---|
| `load/lowering.rs` capability guards (`!caps.structs`/`decimal`/`scalar_lists` match guards, `needs_lowering`) | 22 | **new-test**: lowering unit matrix — for each capability OFF, assert `lower_schema`/`lower_batch` flatten/downgrade exactly (structs→flattened columns, decimal→utf8, lists→child/json). The suite only ever ran with full-capability destinations; the whole lowering seam was assertion-free. |
| `load/mod.rs` counter arithmetic (`+=`→`*=` ×6), `byte_size`→0/1, `policy_triggers` boundaries | 12 | **new-test**: assert exact `RunReport` rows/bytes/discarded numbers after a known load; commit-policy boundary tests (`EveryCheckpoints(2)` commits at exactly the 2nd checkpoint; `EveryBytes` triggers on the byte threshold — kills `byte_size` and `>=`→`<` together). |
| `schema/contracts.rs` `value_fits` arms | 9 | **new-test**: direct `value_fits` unit table over every LogicalType × fitting/non-fitting values (today only reachable via Discard policies, which few tests exercise). |
| `runtime/graph.rs` (retry/backoff arithmetic, event sends) | 6 | **new-test**: retry-delay progression assertion via the existing flaky-source harness; event-emission assertions piggyback on the observability test. |
| `core/ids.rs` `hex_nibble` arms (`from_hex` decoding) | 6 | **new-test**: `from_hex`/`to_hex` round-trip incl. uppercase `A–F` input and invalid-nibble rejection (encoding is well-tested; DECODING never was). |
| `core/naming.rs` (`A..Z` arm deletion, `max_len` `>`→`>=`, `flattened_column_name` body) | 4 | **new-test**: assert normalize("User Name") == "user_name" BY VALUE (the existing test only checked stability); truncation at exactly `max_len`; `flattened_column_name` output — its only caller is the untested lowering seam above. |
| `wal/mod.rs` (`default_wal_version`→0, counter `+=`→`*=`) | 4 | **new-test**: manifest round-trip asserts serialized `format_version == 1`; segment-seq monotonicity assertion added to an existing WAL test. |
| `shred/mod.rs` + `schema/registry.rs` + `shred/arena.rs` + `runtime/channel.rs` + `engine/lib.rs` | 8 | **new-test** where behavioral (registry diff edge, arena dedup slot, channel budget boundary — a push of exactly the budget passes, one byte more awaits); **waived** where observably equivalent (e.g. `Engine::fmt` debug body — cosmetic class that slipped the exclude filter; filter widened instead). |
| `core/state.rs` `check_readable` (future state-version guard) | 2 | **new-test**: same shape as the WAL version-guard test that already landed. |
| `core/types.rs` (`widen` Binary arm, `is_widening_of`→true) | 2 | **new-test**: lattice property test gains explicit Binary-meets-anything and non-widening-pair cases. |
| `core/schema.rs` `is_system`→const | 2 | **new-test**: one assertion each way. |
| `connector/lib.rs` `RecordsOut::send` `>`→`>=` | 1 | **new-test**: byte-budget boundary at exactly the budget. |
| timeouts | 8 | **waived**: loop-bound mutants whose infinite loops ARE the detected outcome — the timeout mechanism is the designed kill. |

## Residual survivors (29, post-closure run)

Same tested prefix as the 79 before the closures — 50 killed by the new tests.
Of the rest: `graph.rs:177` (resumed-from guard) got a further test
(`empty_cursor_state_reports_fresh_resume`); `graph.rs:461` (saw_cancelled arm)
is **waived** — a defense-in-depth arm reachable only under a race (all streams
exit Ok exactly as cancellation lands); the Cancelled outcome is asserted via
the deterministic path. The remaining ~20 are guard-direction/equivalent
variants in lowering/load/wal clusters pending verification by the scheduled
CI run (isolated runner; local full runs are now bounded with
`NEXTEST_TEST_THREADS=2 --jobs 2` after two host OOMs).

## Bugs found by the comprehensive review + sweep-writing (2026-07-20)

Writing the Postgres crash sweep (review finding: G2.1 coverage absent)
exposed that BOTH SQL destinations carried the parquet Replace-recovery
data-loss bug the feature-002 review had fixed only in parquet: the
truncate-once-per-load guard lived in session memory (`replaced` +
`last_replace_load`), so a crash between commits of one load recovered into a
fresh session that re-truncated the target inside its publish transaction —
atomically deleting the earlier commit's durably-acknowledged rows and
re-inserting only the replayed tail. Fixed in both crates with the parquet
pattern (durable guard from the receipt log: any receipt for this load ⇒ never
truncate again), dead session fields removed, regression tests mirroring
parquet's `replace_recovery_session_keeps_prior_commits_of_same_load`.

Root cause of the blind spot: the sweep armed each fail point CONTINUOUSLY, so
every boundary was only ever exercised at its FIRST occurrence — crash-between-
commits was unreachable. The sweep matrix now includes a `1*off->return` pass
(skip the first hit, fire on the second) in both sweep suites.

## Bug found while closing survivors

The registry-widening closure test (`cross_batch_narrowing_keeps_the_wide_type`)
found a REAL passthrough bug, not just a coverage gap: a structured batch whose
column type NARROWED across batches (Utf8 then Int64) pushed a narrowing delta
into the registry — guarded only by a `debug_assert`, so release builds would
have shrunk the destination schema. Fixed: passthrough now JOINS observed types
with the registry's current types on the widening lattice before diffing
(scalars via `widen`, lists item-wise, structs field-wise, shape conflicts →
Json) — the same outcomes the shredder's observation states produce implicitly.

## Remaining work

- All planned survivor tests have LANDED (28 new tests across 13 files plus
  `tests/mutation_closures.rs`); the fmt-impl mutant class is excluded via
  `exclude_re`. A clean full run on the final tree gives the authoritative
  post-closure kill rate; the scheduled `deep` job repeats it weekly.
