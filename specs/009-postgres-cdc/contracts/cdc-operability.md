# Contract: CDC Operability — Preflights, Lifecycle, Lag, TOAST

## Rules

| # | Rule |
|---|---|
| O1 | Preflight per CDC table: replica identity usable (PK-default, FULL, or usable unique index) — else typed error naming the table AND the fix. Publication must cover every CDC table — else typed error naming the gaps (or created via create_if_missing). |
| O2 | Slot lifecycle: created only under `create_if_missing` (idempotent); NEVER dropped by rdlt. Distinguished typed errors (each its own, with recovery path): missing slot; existing slot with a different plugin; invalidated slot / WAL-retention overrun (names the condition, prescribes fresh snapshot); slot actively held by another consumer (single-consumer rule, names the pid). |
| O3 | TOAST policy (FR-007): unchanged out-of-line values substitute from the old image under REPLICA IDENTITY FULL; without FULL, the first unchanged-TOAST marker is a typed error naming table + column and advising the ALTER — never silent nulling. |
| O4 | Identity dropped mid-stream (update/delete record without usable key data): typed error; never a mis-applied change. Source table dropped/renamed mid-stream: typed error. Additive column drift follows the existing D5 rules. |
| O5 | Replication lag appears in every completed run's report: LSN delta (bytes behind) and wall-clock delta when the server exposes it. |
| O6 | Crash discipline: fail points `cdc.slot.create`, `cdc.snapshot.copy`, `cdc.stream.peek`, `cdc.ack.advance` — registry-pinned, swept with both occurrence passes and armed-fire pins, plus a container-kill mid-catch-up cell. The ack-after-commit ordering has its own pin. |
| O7 | Performance: snapshot throughput rides the existing gated pg-source bars unchanged; change-apply throughput and catch-up latency are scoreboard entries under a written 5-run-median protocol. No new gates without a version-policy entry. |
