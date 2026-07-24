# Probe 5 — Reset fidelity: GO

**Question**: can the bench driver return the destination to a clean state
between runs (reset job vs plain schema/table drop) and re-sync cleanly?

**Answer: GO. Both paths work. For a full-refresh-overwrite bench, a plain
`DROP TABLE` (or `DROP SCHEMA … CASCADE` when a dedicated schema exists)
is the recommended per-run reset — faster than a reset job and sufficient
for a clean re-sync.**

## Evidence

Starting state: sync #1 landed 5 rows in `dst.public.probe` (see
spike/03). Connection `efb53231-c2d6-45f8-95e3-8bd85f9394c3`.

1. **Reset job** — `POST /api/public/v1/jobs {connectionId,
   jobType:"reset"}` → `{"jobId":2,"status":"pending",...}`; polled to
   terminal:
   ```json
   {"jobId":2,"status":"succeeded","jobType":"reset","startTime":"2026-07-24T22:20:52Z","connectionId":"efb53231-c2d6-45f8-95e3-8bd85f9394c3","lastUpdatedAt":"2026-07-24T22:21:14Z","duration":"PT22S","bytesSynced":0,"rowsSynced":0}
   ```
   After reset: table `public.probe` **still exists** but is emptied →
   `count(*) = 0`. The reset job truncates data + clears platform-side
   connection state; it does NOT drop the table/schema.

2. **Plain drop** (the faster path) — Airbyte created no dedicated schema
   (it reused the pre-existing `public`; no `airbyte_internal`/raw schema
   under Direct-Load), so the drop unit is the table:
   `DROP TABLE public.probe;` → table gone (`information_schema` count 0).

3. **Re-sync after the plain drop** — `POST /jobs {jobType:"sync"}`
   (jobId 3) → `succeeded`, `duration":"PT17S"`, `rowsSynced":5`; dst
   `count(*) = 5`. The connection recreates the table and lands 5/5 rows
   with no reset job in between. Proves the connection survives a manual
   drop.

## Per-run reset recipe (recommended for the bench driver)

For full-refresh-overwrite cells (the only mode in scope), **prefer the
plain drop**:
- Postgres dest: `DROP TABLE IF EXISTS <namespace>.<stream> CASCADE;`
  (or `DROP SCHEMA <ns> CASCADE` if the product writes a dedicated
  schema — postgres Direct-Load does not, it reuses the configured
  `schema`). File/lake dests (rustfs): delete the product's prefix/path.
- Rationale: no cursor/incremental state exists in full-refresh-overwrite,
  so the reset job's extra work (state wipe) buys nothing; the plain drop
  is ~immediate vs the reset job's ~22s orchestration. Overwrite mode
  recreates the table on the next sync (verified).
- Keep the reset job (`jobType:"reset"`) in the toolbox only if a future
  cell uses incremental/dedup and needs platform state cleared too; it is
  NOT needed for the in-scope full-refresh cells.

Whichever path, the driver must run the drop against the SAME
per-product database/schema/prefix the sync targets (bench isolation), and
re-verify `count(*) == 0` before the timed run.

**Decision: GO. Reset fidelity confirmed both ways; plain drop is the
recommended per-run reset for the full-refresh-overwrite bench.**
