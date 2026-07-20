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
| 2026-07-20 | 852049f (pre-tape snapshot) | 470 | 241 | 127 | 94 | 8 | 64.1% |
| 2026-07-20 | c4a90f9 (OOM-killed at 349/595) | 349 partial | 188 | 79 | 74 | 8 | 68.4% partial |

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

## Remaining work

- 246 of 595 mutants untested (the run OOM-killed the user session; containers
  need a host re-login to restart). Resume with `TARGET=mutants make test`
  (the recipe now carries `--iterate --jobs 2`) and extend the dispositions.
- With the planned tests landed, the projected kill rate on the tested set is
  ≈94% — over the 85% bar; the next scheduled `deep` run verifies.
