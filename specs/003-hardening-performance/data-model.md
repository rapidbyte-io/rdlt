# Data Model: Hardening & Performance (feature 003)

This feature persists no pipeline data; its "entities" are test/CI artifacts with
defined formats.

## 1. Fault-point registry (crash sweep)

Every `fail_point!()` site registers a stable string name, namespaced by crate:

```text
wal.segment.write | wal.segment.fsync | wal.manifest.append | wal.manifest.fsync
pq.replace.truncate | pq.staged.sync | pq.part.rename | pq.dir.fsync
pq.state.write | pq.receipt.write
duck.append | duck.tx.commit
session.after_ensure | session.after_write | session.after_commit
```

- **Invariants**: names are stable identifiers (renaming one is a test-breaking
  change, reviewed as such); the sweep test asserts its enumerated list matches
  the registry exactly — an instrumented-but-unswept point fails the suite.
- **Sweep matrix**: each point × {error-return, panic} × {first run, during
  recovery (composed with one prior kill)}. Expected outcome for every cell:
  restart → exactly-once totals.

## 2. Mutation report (`specs/003-hardening-performance/mutation-report.md`)

Per run: tool version, commit, crates covered, totals (generated / viable /
killed / survived / timeout), kill rate vs the 85% threshold, and one table row
per survivor:

| mutant (file:line, mutation) | disposition | reference |
|---|---|---|
| `…` | `new-test` \| `dead-code-removed` \| `waived` | test name / commit / reason |

- **Invariant**: zero survivors without a disposition row (SC-002).

## 3. Fuzz corpus (`fuzz/corpus/<target>/`)

Seed inputs plus minimized reproducers of every past finding. Findings graduate
to named unit tests in the owning crate; the corpus entry stays (regression
against re-introduction under fuzzing).

## 4. Perf baselines (`benches/perf-baselines.json`)

```json
{
  "format_version": 1,
  "toolchain": "<rustc version the counts were recorded with>",
  "benches": {
    "shred_nested_10k":      { "instructions": 0 },
    "passthrough_10k":       { "instructions": 0 },
    "identity_keyed_10k":    { "instructions": 0 },
    "identity_keyless_10k":  { "instructions": 0 }
  }
}
```

- **Invariants**: updated only deliberately in-diff (lockfile discipline); CI
  fails on regression >3% per bench; improvements SHOULD update the file in the
  same PR (drift hides later regressions); a toolchain bump that moves counts
  re-records all baselines in a dedicated commit.

## 5. Benchmark results rows (extends `benches/RESULTS.md`)

Same schema as existing rows: dataset, baseline command + self-timed number,
rdlt command + self-timed number, multiple, target, status, caveats. New rows:
REST→Postgres, shred-stage-only, cold start, plus the re-measured flagship RSS
row. Honesty rules unchanged (no multiple without both columns).

## 6. Hash decision record (design doc §5.4 amendment)

Fields: candidates, microbench numbers (keyed/keyless), flagship e2e numbers,
threshold applied (>10% e2e), decision, date, consequence note (dev-state reset
if switched). Written before any release tag (FR-008).

## 7. Streaming-shred equivalence gate

Not a persisted artifact but a defined relation: for any input batch sequence,
`old_path(batches) == new_path(batches)` over (rows, `_rdlt_*` values, schema
sequence, discard counts). Enforced by `shred_equivalence.rs` (proptest) and by
running the ENTIRE existing engine suite against the new path. The old path is
deleted only after one full feature cycle with the new path as default.
