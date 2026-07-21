# Contract: CDC Protocol — Boundary, Passes, Ordering, Acks

## Rules

| # | Rule |
|---|---|
| P1 | The change feed is consumed WITHOUT side effects until acknowledged: reads peek; nothing is consumed by reading. Every pass over a WAL range is therefore independently resumable. |
| P2 | Snapshot boundary (spec refinement 1, research R4): the slot exists BEFORE the snapshot; the snapshot covers ALL CDC tables under ONE consistent view; the stream replays from the slot's consistent point ≤ the snapshot's point. NO GAP ever; the window between the points applies twice and CONVERGES (upsert-by-key + idempotent deletes). Conformance pins the overlap cell: a row changed inside the window appears exactly once with its final state. |
| P3 | Bounded catch-up pins `target_lsn` at run start; each CDC stream's pass covers exactly `(its cursor, target_lsn]` filtered to its table; a COMPLETED run has every stream at target_lsn — completed-run consistency (FR-003). |
| P4 | Checkpoints land ONLY at transaction-commit positions for the stream's table: resume never tears a table's transaction; large transactions may span multiple pushed batches between checkpoints (bounded memory, FR-012). |
| P5 | Within a table, changes apply in source commit order. PK-changing updates emit delete(old key) then insert(new key), in that order, in the same batch. |
| P6 | The slot's acknowledged position advances at most once per run, to min(committed cursor across ALL CDC streams), only after every stream's resume cursor is known. A run that dies early acks nothing — always safe (acking is retention hygiene, never correctness). `ack: off` disables advancement entirely. |
| P7 | Tail mode (spec refinement 2, research R6): a chunked loop of bounded catch-ups with `idle_wait` between quiet chunks; cancellation honored at commit boundaries; checkpoints flow every chunk; a later run (either mode) resumes exactly where the loop stopped. |
| P8 | Exactly-once OUTCOMES hold on keyed-merge destinations (upsert + hard-delete composition); destinations without keyed merge receive the operation flag as data — documented at-least-once, never claimed otherwise. |
