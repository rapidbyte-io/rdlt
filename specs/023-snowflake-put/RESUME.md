# Where 023 stands — resume point

Paused 2026-07-29. Branch `023-snowflake-put`, tree clean at `f87ce798`,
10 commits ahead of `main`. **39 tasks done, 3 deferred by owner decision,
12 open.** Delete this file when the feature closes.

## Run this first

The crash sweep was KILLED mid-run and never reached its cleanup, so it left
scratch schemas behind on the qual account. They are named `SWEEP_*` in the
configured database. Nothing reads them and they cost storage:

```sql
SHOW SCHEMAS LIKE 'SWEEP%';   -- then DROP the ones this run created
```

Not dropped automatically: `DROP SCHEMA` is destructive and was not worth
running unattended.

## What is proven

Live, against the account:

- Rows land through the internal stage with nothing configured. Exact totals,
  replay publishes nothing, awkward and multi-byte values survive, an empty
  load still commits its position.
- Uploading does not commit an open transaction; creating the staging object
  inside a unit is safe and dropping it is not; the listing exposes a part's
  age; stale parts are reclaimed and fresh ones are spared.
- The account names three S3 hosts as `STAGE`, none under
  `snowflakecomputing.com` — the evidence behind the README's egress
  prerequisite.
- A key pair fails silently and fatally both ways: material this host cannot
  parse, and a generated key the account never registered.

Offline: 299/299 on the other SQL destinations, golden pins byte-identical.
`make lint` clean including the distribution gate.

## What is open

| task | what it needs |
|---|---|
| T023, T025 | **Re-run the crash sweep** — see the finding below before deciding how. |
| T035, T045 | Story gates, both waiting only on the sweep. |
| T047–T049 | The recorded ingestion session against 022's figures (582 rows/s INSERT, 2,191 bucket at 250k; 1,941 at 1M). ~17 min of warehouse time. |
| T050–T052, T054 | Coverage, semver note, close-out finalisation, final gate. |

Deferred by owner decision 2026-07-29, already written up as close-out D-33:
T036, T037, T039 — password and OAuth stay UNPERFORMED. The legs are written
and announce their skip; adding the credential entries turns them green with
no code change.

## The finding that needs a decision

**SC-012 asked for the sweep's cell count AND wall clock to fall. Only the
cell count does.**

- Cells: 30 → 27, with Merge newly covered at the publish.
- Wall clock: 72 min → projected ~140 min, from 19 of 27 cells in 90 minutes
  before the run was stopped.

The reason is structural rather than a defect. The sweep's loads are 40 rows.
At that size a statement is one round trip, while an upload is a stage-info
call, an HTTPS transfer to object storage, and a `COPY` — plus two more round
trips per open for the new remote reclaim. The path that ships is faster where
the data is; the sweep exercises it where the data is not. The criterion
conflated "smaller matrix" with "less time", and only the first was ever in
this feature's control.

**Correction to an earlier reading of the cost.** `make check` does NOT run
this sweep — `Makefile` has no Snowflake line in its sweep target and never
has. 022 ran it by hand, twice, at 4,308 s (71.8 min) each. So T054's
"twice clean" gate is ordinary length, and the sweep's cost is its own
separate decision, exactly as it was in 022.

That leaves the recording question only. The miss is recorded either way; what
is NOT taken is amending SC-012 to match the outcome. Changing a criterion
because it was missed has to be a deliberate decision on the argument above,
not a convenience — so it is noted here and left to the owner.
