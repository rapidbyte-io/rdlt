# Data Model: Snowflake Internal-Stage Ingestion (023)

Entities, their lifecycle, and the rules each must satisfy. Nothing persisted
changes: receipts, state documents and the other destinations' emitted SQL are
untouched. What changes is where a batch lives between leaving the engine and
landing in a table.

## 1. Load session

Unchanged from 022 in every respect except what backs its staging.

| aspect | rule |
|---|---|
| Identity | one pipeline, one load |
| Schema work | all of it before any unit opens — schema statements commit an open unit |
| Unit | pure data manipulation; publish, receipt and position commit together |
| Replay | a receipted unit publishes nothing and returns the prior receipt |

**New constraint from a measured fact**: creating the staging object does *not*
commit an open unit, but **dropping it does**. Teardown therefore never happens
inside a unit. Creation may be treated as schema work regardless, since it
belongs with the other schema statements that precede the unit.

## 2. Staging area

A named, schema-scoped object owned by one pipeline.

| aspect | rule |
|---|---|
| Form | named object — not the per-user area (no scoping) and not the per-table area (loads only its own table) |
| Naming | derived from the pipeline, deterministic, so a later run finds the same object |
| Scope | one pipeline; a sibling pipeline in the same schema has its own |
| Lifetime | created before the unit; contents removed after the unit commits |
| Teardown | outside any unit, because dropping it commits one |

**Contents are not transactional.** A rolled-back unit leaves its uploaded parts
behind — measured. Cleanup is therefore explicit and must be idempotent, since a
crash can occur either side of it.

## 3. Part

One batch of rows, encoded once and moved once.

| field | meaning |
|---|---|
| local path | where the part is built; exists only until the upload returns |
| staged name | what the load statement names; derived from the *reported* upload target |
| rows | how many rows it holds, recorded at build time |

**Rules**:

- **Rows are recorded at build time**, because that is the only moment the count
  is free. Reading it back from the file to verify the load would be checking
  the service against itself.
- **One part in flight.** A part is built, uploaded, and its local file deleted
  before the next is built. Peak local usage is one part — not proportional to
  the unit, and not to the dataset.
- **Names are load-scoped.** Two loads of one pipeline must never derive the
  same name. This is not a hypothetical: the deleted bucket path shipped without
  it and the crash sweep found the collision, where one session's reclamation
  deleted a part another was about to load.
- **The staged name is the reported target, never the local name and never the
  listing's name.** The service appends a compression suffix when it compresses;
  the listing lower-cases and re-prefixes. Both were measured to break a file
  list.

**Open**: the maximum size a part may reach is source-dependent and the transfer
has a per-file ceiling. See the plan's risk table.

## 4. Upload outcome

The result of moving one or more parts into the staging area.

| field | meaning |
|---|---|
| source | the local file |
| target | the staged name — **the value a file list must use** |
| status | success or failure, **per part** |
| message | the reason, when it failed |

**The rule that matters**: an overall success does **not** mean every part
uploaded. A transfer of several parts where some fail returns success with a
mixed result — measured. Failure is returned only when *every* part failed.
Therefore: **inspect every row's status; any non-success fails the unit.**

This mirrors the load's own verification, which already compares rows loaded
against rows staged and abandons the unit on a mismatch.

## 5. Configuration

Shrinks. What remains describes *where to connect and what to write*, with
nothing describing *how rows travel*.

| retained | account, user, auth, database, schema, warehouse, role, table type, session parameters, query tag, host override, and the shared destination options |
| removed | the entire storage vocabulary — bucket, prefix, region, endpoint, addressing style, access keys, storage integration |

**Removal is by deletion, not deprecation.** A document still carrying the
storage block is refused by name through the configuration's existing rejection
of unknown fields. No tombstone field is kept: that would be a compatibility
shim, and it would leave the removed vocabulary in the generated schema.

## 6. Local working area

New, and the only genuinely new entity.

| aspect | rule |
|---|---|
| Location | the platform's temporary directory, per load |
| Ownership | one load; concurrent loads of one pipeline cannot collide |
| Lifetime | one part at a time; deleted as soon as the upload returns |
| Reclamation | a later run removes its own load's leftovers unconditionally, another load's only when demonstrably stale |
| Failures | typed by condition — out of space, read-only, permission — never a bare I/O error |

The reclamation rule is inherited deliberately from the deleted path, which
arrived at it by having the naive version break under the crash sweep. The
lesson transfers even though the storage does not: *this load's residue is
unreachable and may go; another load's is indistinguishable from live work
until it is old.*

## State transitions

```
                    schema work (outside any unit)
                              │
                    staging object ensured
                              │
        ┌─────────────────────┴──────────────────────┐
        │                  UNIT OPEN                 │
        │                                            │
        │   for each batch:                          │
        │     build part locally ──► upload ──► drop │
        │                             │        local │
        │                             ▼              │
        │                    verify EVERY row's      │
        │                    status; any failure     │
        │                    abandons the unit       │
        │                                            │
        │   load staged parts into the target        │
        │   verify rows loaded == rows staged        │
        │   write receipt + position                 │
        └─────────────────────┬──────────────────────┘
                              │ commit
                    remove staged contents
                       (idempotent; a crash
                        either side is safe)
```

Crash behaviour: a unit that does not commit publishes nothing. Its staged parts
survive — they are not transactional — and are reclaimed by name, since a part
is named by exactly one load and no receipt refers to it.
