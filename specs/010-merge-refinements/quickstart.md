# Quickstart: Merge Refinements

## Ordered survivor (`dedup_sort`)

Your feed can deliver several versions of one row in a single load, and
arrival order isn't trustworthy? Name the column that is:

```toml
[destination.postgres.tables.events]
dedup_sort = { column = "updated_at", order = "desc" }
```

The newest `updated_at` wins — including the hard-delete decision (a
survivor flagged deleted deletes; an older flagged version loses to a
newer unflagged one). Rows with NULL `updated_at` lose to rows with a
value; full ties keep the old deterministic last-wins. Without the
option nothing changes.

## Scope replacement (`merge_key`)

Re-delivering a window ("all of yesterday", "this tenant") where the
batch is the complete truth for its scope:

```toml
[destination.postgres.tables.events]
merge_key = ["day"]
```

Every `day` present in the batch is replaced wholesale — rows the batch
no longer carries disappear; days the batch doesn't mention are
untouched. Rows with a NULL scope only merge by identity. The scoped
TABLE's feed must arrive in ONE commit unit (the batch is "the complete
truth for its scope" only if it lands atomically — a later unit staging
scoped rows is a typed error advising the engine commit thresholds;
units where the table stages nothing are fine, so other streams'
checkpoints never trigger it — same rule as scd2 retire). The scope
columns get a supporting index automatically. Composes with
`hard_delete`, `dedup_sort`, and both delete-insert/upsert strategies.

One recorded caveat: a scoped stream should checkpoint only at feed
end. A stream that checkpoints MID-feed and crashes in the window
between a committed partial unit and the split-detection error resumes
as a new load with a partial feed — which the destination cannot
distinguish from a fresh one (contract MR5, recorded residual).

## Verify

```bash
cargo nextest run -p rdlt-postgres -E 'binary(dest_conformance)'   # MR matrices
cargo nextest run -p rdlt-postgres --features failpoints -E 'binary(dest_crash_sweep)'
benches/run-merge-refinements.sh                                    # scoreboard cells
```
