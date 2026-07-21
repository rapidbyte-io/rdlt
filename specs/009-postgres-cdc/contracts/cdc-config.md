# Contract: CDC Configuration + Pipeline Composition

## The block

```yaml
cdc:
  slot: my_pipeline_slot
  publication: my_pub
  create_if_missing: true      # rdlt creates idempotently, NEVER drops
  mode: catchup                # catchup (default) | tail
  idle_wait: "1s"              # tail-mode quiet wait
  flag_column: _rdlt_deleted   # collision-checked per table
  ack: auto                    # auto | off
tables:
  - name: orders               # CDC tables: no cursor: block (mutually exclusive)
  - name: customers
```

## Rules

| # | Rule |
|---|---|
| C1 | `cdc:` and `cursor:` are mutually exclusive per table (typed, names the table). CDC streams are keyed structured streams; the key preflight is the replica-identity check (cdc-operability O1). |
| C2 | `flag_column` collides with no CDC table's columns (typed at open, names table + column — same discipline as SCD2 validity names). |
| C3 | Recommended pipeline shape is DOCUMENTED and validation-warned when absent: `write_mode = merge{key = <PK>}`, destination `merge_strategy = upsert`, `hard_delete = <flag_column>`. Without hard-delete support the flag lands as data (soft delete, documented). |
| C4 | All fields ride the generated config schemas (schemars; examples validate; unknown fields fail both layers; the 007 duration vocabulary for `idle_wait`). |
| C5 | Connection behavior is inherited unchanged: TLS/mTLS policy, conn-string portability, application_name. |
