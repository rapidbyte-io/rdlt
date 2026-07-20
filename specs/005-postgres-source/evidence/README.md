# Evidence directory — feature 005 (formats per the 004 house rules)

Artifacts justify the feature's measured claims. Every artifact opens
with the environment-identity header (machine, CPU, kernel, rustc, tool
versions, dataset identity) — the 004 rule carries over.

- `bench-pg.md` — the US3 measurement record: dataset identities (seed
  content hashes), baseline-first same-session pairs (dlt pyarrow gated;
  sqlalchemy + connectorx scoreboard), rdlt cells, medians + raw runs,
  measurement-first bar derivations.
- Traceability + verification-sweep records are appended below at
  feature close (T030).

## Traceability walk (T030, 2026-07-20)

Spec claims → records, walked:
- **US1/SC-001** (faithful typed replication) → conformance suite: every
  type-mapping contract row asserted through real Postgres → DuckDB
  (`tests/conformance.rs`); differential proptest pins the decoder to an
  independent driver reference; fuzz target `pg_copy_decode` registered.
- **US2/SC-003** (incremental semantics + exactly-once) → incremental
  suite (boundary matrix, dedup, NULL policies, regressing clocks,
  PK-less hash keys) + armed crash sweep (all 4 fail points × 3 actions,
  both occurrence passes) + in-run transient resume from a MID-TABLE
  checkpoint (`report.retries` asserted) + real container kill.
  Deviation: US2-AS5 Merge blocked by engine clause B4 — asserted as a
  boundary test, recorded in tasks.md notes + README; backlog item.
- **SC-002** (memory bounded, ≥10×) → `tests/memory_bound.rs`: 6.86 GB
  table (ratio measured in-test via pg_total_relation_size) through a
  256 MiB `prlimit --data` ceiling; 39 MB peak RSS observed.
- **US3/SC-004** (measured cells, honest bars) → [bench-pg.md](bench-pg.md):
  dataset identities, baseline-first same-session pairs, worst-case-run
  bar derivations; matrix rows + policy entry in `benches/RESULTS.md`;
  design-doc §8 rows added. Gate: `pg_copy_decode_10k` added as a NEW
  baseline entry; existing entries untouched (FR-007/P5).
- **FR-012** (no SPI breakage) → cargo-semver-checks vs origin/main:
  rdlt-core and rdlt-connector both "no semver update required".
  `unsafe_code = "deny"` stands; zero new exceptions.
- No dangling links found; deviations all carry a record + backlog entry.

## Verification sweep (T030, 2026-07-20)

`make check` green on the final tree (exit 0): fmt + clippy -D warnings,
full workspace nextest (incl. the new conformance/incremental/
differential/drift/memory suites), doc-tests, crash sweeps — NOW
GENUINELY ARMED (the fail/failpoints workspace fix landed in this
feature; the probe test guards it) — and the iai gate 5/5 within ±3%
(pg_copy_decode_10k +0.00%, shred +0.67% jitter, others ±0.0x%).
