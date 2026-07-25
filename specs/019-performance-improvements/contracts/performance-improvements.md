# Contract: Performance Improvements (PI1–PI8)

Clauses this feature must satisfy; the close-out cites these IDs. Per
constitution Principle V they never appear in user-facing strings.

## PI1 — Evidence or it did not happen

Every increment that claims a performance improvement lands with a recorded
before/after measurement on the affected benchmark cells, taken through the
harness on a machine that passed the quiet guard. The baseline is
re-established locally before the first increment; where the local baseline
differs from the figures in `spec.md` the local figure becomes the comparator
and the difference is recorded, never silently substituted.

An increment whose measurement shows no improvement is REPORTED as such and
either dropped or justified on non-performance grounds. A measurement is not
retried until it is favourable; the first clean measurement on a quiet machine
is the result.

Instrument-track counts (`benches/perf-baselines.json`) that shift for a
KNOWN reason — a deliberate build-profile change, a rewritten hot path — are
re-recorded in the same change that causes the shift, with the reason in the
commit message. A shifted count is never absorbed by widening the tolerance.

## PI2 — Greenfield replacement, superseded code deleted

Where this feature replaces an implementation, the replaced implementation is
DELETED in the same change that introduces its replacement. No compatibility
shim, no alias, no second code path, no feature flag that keeps the slow path
reachable, no `#[deprecated]` bridge.

At close-out, a search of the shipped tree for each replaced implementation
returns zero hits. Where an old implementation must survive briefly as a test
oracle (PI4), it lives in `#[cfg(test)]` code, is named as an oracle, and is
deleted when its pin is captured.

## PI3 — Off the shelf unless a fact says otherwise

No component is hand-written where a crate in the dependency tree, or a
feature flag on a crate already in the tree, already provides it. This applies
to hashing, wire-value encoding, compression, container formats, parsing, and
anything else this feature touches.

Every hand-written component this feature introduces or RETAINS carries a
recorded justification that is a fact, not a preference — one of:

- no such crate exists (with the search recorded),
- the available crate cannot represent the required domain (e.g. a decimal
  crate whose mantissa is narrower than the values carried),
- the available crate would allocate per value on a path this feature exists
  to de-allocate,
- it is protocol framing rather than an algorithm.

Symmetrically, no dependency is added without a version that resolves against
the workspace pins, the feature path that reaches it, and a statement of what
it costs the dependency tree. A dependency that is already in `Cargo.lock`
still counts as an addition to any crate that did not previously depend on it,
and is justified the same way. Principle I applies: the default answer to a
new dependency is no.

## PI4 — Frozen values stay frozen; one authorised version bump

These are byte-frozen across the whole feature and are pinned by tests that
fail on any drift:

- every emitted `_rdlt_id` value, for roots and children at every depth;
- the binary wire bytes the relational destination sends for every supported
  column type, null and non-null, at representable boundaries;
- the statement text the merge strategies emit, except where an increment
  deliberately changes it, in which case the golden pins are re-pinned in the
  same change and the diff is reviewable.

Exactly ONE persisted format may change: the recovery-log segment format, with
its version incremented. A log written by an unsupported version is refused
with the reason logged and recovery degrades to source re-extraction. No
reader for the displaced format survives.

Where PI2 requires deleting an implementation whose output PI4 freezes, the
byte-identity pin is captured FIRST, from the old implementation, and committed
as fixture data before the old implementation is removed.

## PI5 — Exactly-once survives every increment

Every increment touching the recovery log, the commit protocol, publish
atomicity, or concurrency runs the crash-point sweep suite with duplicate-free
verification. The recorded crash points are:
`wal.segment.write`, `wal.segment.fsync`, `wal.manifest.append`,
`wal.manifest.fsync`, `session.after_ensure`, `session.after_write`,
`session.after_commit`, `pg.stage.copy`, `pg.publish.begin`, `pg.tx.commit`.

An increment that changes what a crash point MEANS updates its self-contained
comment in the same change. An increment that introduces a new failure window
adds a crash point for it. An increment that removes a window removes its
crash point and says so.

The canonical redelivery window — a crash between the recovery log becoming
durable and the destination acknowledging the commit — replays to exactly one
published copy under every increment, including under concurrency.

## PI6 — The benchmark measures what it claims

A benchmark cell delivers exactly the streams it declares. The harness rejects
a cell whose delivered stream set differs from its declared set, naming the
surplus or missing streams, before any measurement is recorded.

Row-count verification alone is not sufficient evidence that two arms are
comparable: a cell whose arms move different volumes is not a comparison, and
the corrected value supersedes the old one in the results page with a
governance entry naming what was wrong and what the superseded number was.

Where two arms produce artifacts that are not equivalent (different encoding,
different compression), the cell either equalises them or states the
difference in its own note, and records the volume each arm wrote alongside
its elapsed time.

Bars follow Principle VIII unchanged: at most one per cell, set below a
recorded session floor, each with a policy-log entry.

## PI7 — Configuration is expressible and validated

Every intent this feature's users need is expressible in configuration:

- deliver declared query streams and discover no tables;
- write output files with a chosen compression, dictionary policy, and
  row-group and page sizing.

A configuration that expresses nothing to do — no tables and no queries — is
rejected at configuration time, not run as a silent no-op. A contradictory or
unsupported combination of output-format settings is rejected at configuration
time with a typed error naming the offending setting. No rejection message
carries a clause ID or a spec citation.

Where a configuration spelling changes meaning under PI2 (greenfield), the new
meaning is the only meaning; the old spelling is not retained alongside it.

## PI8 — The version window is opened deliberately or not at all

The seam crates `rdlt-core` and `rdlt-connector` are checked against `main` by
a blocking release-compatibility gate. If an increment changes the destination
session interface incompatibly, then in the SAME change:

- the workspace version moves to the recorded next window (0.2 → 0.3),
- every implementor of the interface in the tree is updated — the four bundled
  destinations, the testkit's memory and crash destinations, and the test
  implementors,
- the break is recorded where the window was named, so the record shows the
  window was opened here rather than drifting open.

If the design lands WITHOUT an incompatible interface change, that outcome is
recorded explicitly and the window stays closed. Discovering the break late,
in CI, is a process failure: the decision is made when the design is fixed.
