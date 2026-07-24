# Data Model: Workspace Refactoring Program

This feature introduces no persisted data and no wire formats (WR1 forbids
both). The "entities" are (a) the tracking artifact and (b) the shared
abstractions being extracted — their shapes are design commitments the tasks
implement against.

## 1. Close-out matrix (tracking artifact)

File: `specs/017-workspace-refactoring/close-out.md` (created at increment 1,
completed at close-out).

| Field | Type | Rules |
|---|---|---|
| `item` | catalogue ID (`B1`…`B12`, `R1`…`R13` + Part 3 sub-item suffix, `D1`…`D15`, `P5-low-*`) | every catalogue ID appears exactly once |
| `increment` | 1–12 or `close-out` | matches plan.md's increment table |
| `disposition` | `applied` \| `shimmed` \| `deferred` \| `overtaken` | terminal states only |
| `evidence` | citation (test name, commit, sweep command output, or deferral target) | never empty (FR-023); `deferred` must name the target window; `overtaken` must cite the contradicting code |

Header records the pre-feature coverage baseline (D-15) and the red-run
evidence index for B-items (D-14).

## 2. Shared abstractions (extraction targets)

### 2.1 `rdlt_connector::secret::Secret` (R3)

- Newtype over `String`; `Debug`/`Display` render a fixed mask; serde is
  transparent; `From<String>`/`From<&str>`; schemars impl behind SPI feature
  `schema`.
- Invariant: no code path renders the inner value except an explicit
  `reveal()` (audit surface — grep-provable, the existing per-crate
  guarantee, now in one place).
- Consumers: rest (auth), file (S3 credentials), iceberg (catalog
  credential); old paths remain as re-exports.

### 2.2 `DestError::RateLimited` (R8)

- New variant on the `#[non_exhaustive]` SPI enum, mirroring
  `SourceError::RateLimited` (carries optional retry-after).
- State transition rule: engine retry loop treats it as Transient-with-hint;
  budget accounting identical to the source path.

### 2.3 sqlcore commit protocol (R2)

- `commit_script(tables: &[TablePlan], options: &DestOptions, replayed: bool)
  -> Vec<Step>` — pure function, no driver types (Principle III).
- `Step` (closed enum): `CheckReplay`, `GuardSingleUnit`, `MarkSingleUnit`,
  `Publish(MergeArm)`, `TruncateStage`, `UpsertState`, `InsertReceipt`.
- Invariants: single-unit discipline and scope-replacement ordering are
  planner decisions — executors may not reorder; SQL text emitted through
  the existing dialect seam; golden pins freeze the script per dialect.

### 2.4 Engine apply helpers (R6)

- `apply_delta(session, registry, delta) -> Result<SchemaHash>` — the
  lower_schema → ensure_table → record-hash triple, one home.
- `apply_batch(session, schema, batch) -> Result<Counters>` — the
  lower_batch → write pair.
- Consumers: `Loader::process` and `wal::resume::replay` (two-pass per D-08:
  pass 1 validate, pass 2 stream).

### 2.5 Unified file location (R7)

- One `Location` enum (`Local` | `S3`) exposing read half (open, list,
  metadata) and write half (put, delete, staged-part naming).
- One classification fn: store error → Transient/Fatal (the source-side
  rulebook becomes canonical; B9).
- One ownership helper: `keys_of_table(root, table)` — lists with the
  `"{table}/"` tail so prefix-sharing table names cannot collide (B2); both
  counting and truncation call it.
- `FileMeta`/`FileTask`/`FileProgress` live under `location/` (kills the
  upward import).

### 2.6 Testkit containers & fixtures (D1–D5)

- `containers::runtime_available() -> bool` — superset probe (env override →
  docker/podman sockets → `podman ps`); the only probe any crate uses.
- `containers::PgFixture::start() -> Option<PgFixture>` — `None` = skip
  visibly (posture rule: missing runtime never panics); image tag, port,
  conn-string template defined once. `CdcPgFixture` variant for logical
  replication.
- `fixtures::{batch_of, schema_for, meta_for}` — the canonical
  single-`id`-column schema, its Arrow batch, and `CommitMeta`/`StateDoc`
  builders; the 6 duplicate sites consume these.

### 2.7 Context structs (R11)

- Engine `ShredCtx { registry, load_id, mode, policy }` — one field order,
  both construction sites.
- Postgres CDC `TableCtx` — the 6-arg prefix shared by the 5 suppressed
  functions.
- Bench: `return_side` args collapse into the yield-then-build restructure.

## 3. Validation rules added by this feature

- Ordering violation in cursor streams → typed Fatal (B4) — replaces
  `debug_assert!`; key-format failures propagate.
- Structured-code classification: duckdb `code`/`extended_code` (B5),
  iceberg `status` context (B6) — each with probe-pinned assumptions.
- Parity test pinning CLI vs bench spec models until increment 12 retires
  the copies (D-01).
- Version-agreement check: `iai-callgrind` lib pin vs CI runner pin (D-13).
