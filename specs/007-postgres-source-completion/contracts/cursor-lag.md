# Contract: Cursor Lag, NULL Policy, End Bound

Extends the 005 incremental-cursor contract
(`specs/005-postgres-source/contracts/source-config.md`). All three
surfaces are read-side only: checkpoint/state formats and watermark
semantics are UNCHANGED.

## Lag (attribution window)

```yaml
cursor:
  column: updated_at
  lag: "5m"        # duration for time cursors; magnitude for numeric; days for date
```

| # | Rule |
|---|---|
| L1 | With a saved watermark W and lag Δ, the resumed read's lower bound is `W - Δ` (closed, `>=`), computed by the DATABASE in the cursor's type (`timestamptz - interval`, `int8 - int8`, `date - int`). The SAVED watermark is never lowered — it advances exactly as without lag. Run 1 (no watermark) ignores lag. |
| L2 | Open-time validation, all typed naming the column: lag requires `boundary: closed`; the cursor type must have defined subtraction (time, date, integer, decimal — text/uuid rejected); the unit form must match the cursor family (sub-day durations rejected for `date`); the stream must have a primary key (reflected or declared). |
| L3 | Delivery within the window is AT-LEAST-ONCE: rows in `[W-Δ, W]` re-deliver on every run. Exact destination totals are guaranteed under keyed Merge write mode (feature-006 merge-structured path — the conformance mode). Under Append the re-delivery is a documented property, never silent (README + config docs state it). |
| L4 | Watermark-equal dedup (005 boundary keys) continues to apply unchanged on top of the lag window. |
| L5 | Rows with cursor values older than `W - Δ` at commit time are outside the guarantee (the window bounds the promise) — unchanged, documented. |

## NULL-cursor policy: `error`

```yaml
cursor:
  column: updated_at
  nulls: error     # exclude (default) | include | error
```

| # | Rule |
|---|---|
| N1 | Under `error`, the first NULL cursor value fails the run with a typed FATAL error naming stream and column, raised at decode time (no pre-flight query; zero cost when the column has no NULLs). |
| N2 | The failure respects the commit protocol: nothing past the last committed checkpoint publishes; retries do not duplicate (the error is fatal, not transient — the engine does not retry it). |
| N3 | `exclude` and `include` behavior and the `exclude` default are byte-for-byte unchanged. |

## End bound: `inclusive`

```yaml
cursor:
  column: id
  end_value: "1000"
  end_bound: inclusive   # exclusive (default) | inclusive
```

| # | Rule |
|---|---|
| E1 | `inclusive` makes the upper predicate `<=` under `direction: max` (`>=` under min); rows exactly AT `end_value` load. `exclusive` (default) is unchanged. |
| E2 | The end bound is a read filter only — it never participates in watermark/resume state. Boundary rows load exactly once across re-runs (watermark-equal dedup applies as everywhere). |

## Conformance (incremental.rs additions)

- Late-arrival capture under Merge: seed, sync, insert a row behind
  the watermark inside Δ, sync → row present, totals exact; three
  further runs → totals STILL exact (idempotent window re-merge).
- Beyond-window row → not loaded (L5 pin).
- Rejection matrix: lag+open, lag on text cursor, lag on keyless
  stream, sub-day lag on date — each a typed error naming the column.
- `nulls: error` fails typed; same table under exclude/include pins
  unchanged behavior.
- Inclusive end: boundary row loads once; beyond-bound row does not.
