# Data Model: Postgres CDC

Zero engine/connector entities change. LSN cursors are ordinary
`Cursor` values; CDC streams are ordinary keyed structured streams.

## CdcConfig (new block on `PostgresConfig`)

| Field | Type | Default | Rules |
|---|---|---|---|
| `slot` | String | — (required) | slot name; single-consumer (R9) |
| `publication` | String | — (required) | must cover every CDC table (preflight) |
| `create_if_missing` | bool | false | creates slot + publication idempotently; rdlt NEVER drops them |
| `mode` | catchup \| tail | catchup | tail = chunked loop (refinement 2) |
| `idle_wait` | duration string | "1s" | tail-mode quiet wait; reuses the 007 duration vocabulary |
| `flag_column` | String | `_rdlt_deleted` | deletion flag; collision-checked against every CDC table's columns (typed) |
| `ack` | auto \| off | auto | `off` = never advance the slot (debugging/fan-in staging); WAL-retention warning documented |

Validation: CDC and `cursor:` on the same table are mutually exclusive
(typed, names the table); `tables:` entries covered by CDC must have
usable replica identity (preflight, R8); all fields in the generated
schema (SC-007).

## Cursor semantics

| Item | Value |
|---|---|
| Cursor | LSN as u64; human-rendered `X/Y` in errors/reports |
| First run | no cursor → snapshot pass; cursor initialized to the slot's consistent point |
| Later runs | change pass over `(cursor, target_lsn]`, checkpoints ONLY at transaction-commit LSNs |
| Ack | once per run, `pg_replication_slot_advance(slot, min(committed cursor across CDC streams))`, only after all streams reported (R5) |

## Change record (per table, structured batch rows)

| Column set | Content |
|---|---|
| table's columns | new row image (insert/update); key columns only for delete (non-key NULL) |
| `flag_column` | NULL for insert/update; TRUE for delete |
| ordering | commit order within the table; PK-changing update = delete(old key) then insert(new key) |

TOAST (R7): unchanged-TOAST marker + REPLICA IDENTITY FULL →
substitute from old image; otherwise typed error naming table+column
advising `REPLICA IDENTITY FULL`.

## Distinguished error taxonomy (R9 — each its own typed error)

| Condition | Detection | Message must include |
|---|---|---|
| identity unusable | `relreplident` preflight | table + the fix |
| slot missing | catalog check | slot + create_if_missing hint |
| slot wrong plugin | `pg_replication_slots.plugin` | both plugin names |
| slot invalidated / WAL overrun | `wal_status` in lost/unreserved | condition + fresh-snapshot recovery |
| concurrent consumer | `active` = true | pid + single-consumer rule |
| publication missing/gap | catalog check | publication + missing tables |
| unchanged TOAST w/o FULL | decode-time marker | table + column + ALTER advice |
| identity dropped mid-stream | update/delete without key data | table; never mis-applies |

## Run report additions

Replication lag per completed run: `target_lsn − min committed LSN`
(bytes) and wall-clock delta when the server exposes commit
timestamps; present in both modes (FR-011/SC-006).

## Fail-point registry additions

`cdc.slot.create`, `cdc.snapshot.copy`, `cdc.stream.peek`,
`cdc.ack.advance` — registry-pinned, swept both passes, armed-fire.
