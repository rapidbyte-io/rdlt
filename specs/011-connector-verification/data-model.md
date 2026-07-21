# Data Model: Connector Verification

No runtime entities change. The feature's entities are audit artifacts.

## Traceability matrix (`matrix.md`)

One table per config block (R4 inventory). Row schema:

| Column | Content |
|---|---|
| parameter | field name (or enumerated value, e.g. `nulls: error`) |
| default | the documented default, or `required` |
| behaviors | documented values/behaviors to prove (one row per distinguishable behavior where they differ materially) |
| validation | typed-error rules for this parameter |
| cells | `file::test_name` citations (≥1 per behavior AND per validation rule) |
| class | unit \| live \| sweep \| heavy |

**Interaction rows** (FR-003 minimum set, each its own row):

1. cursor `boundary` × `lag` (lag requires closed — parse AND open),
   `lag` × write mode (merge exact totals vs append re-delivery),
   `lag` × cursor type family (duration vs magnitude vs date whole-days).
2. `end_value` × `end_bound` (inclusive/exclusive observable row sets).
3. `nulls` (3 policies) × resume (NULL rows re-included every run under
   `include`; typed failure names stream+column under `error`).
4. `type_hints` × cursor capability (hint enables/disables cursor use).
5. `included_columns`/`excluded_columns` × cursor column exclusion ×
   CDC key coverage.
6. `cdc` × any `cursor` (C1 exclusivity), `cdc.flag_column` collision,
   CDC × `primary_key` override (FULL wins; default/index mismatch
   rejects), CDC recommended composition warning legs (CLI).
7. `tls` block × conn-string sslmode (precedence + contradiction),
   client cert both-or-neither, `sslrootcert=system`.
8. destination strategy × per-table options: `hard_delete` flag-type
   semantics (bool vs non-bool), `dedup_sort` survivor drives
   hard_delete/upsert/scd2, `merge_key` × strategies + single-unit rule,
   scd2 `absent` × single-unit rule, explicit `merge_strategy` × 
   non-merge write mode (R5 — new typed rejection).
9. CLI: `write_mode` three forms; postgres source inline XOR
   `{config: path}` (mixing = loud error); `workdir` resume behavior;
   `pipeline` id keys state.

## Coverage record (in `benches/RESULTS.md`)

| Field | Content |
|---|---|
| crate | rdlt-postgres |
| command | the exact `make coverage` / cargo-llvm-cov invocation |
| baseline | total % + date (T001, before new cells) |
| final | total % + date (≥ 80) |
| per-file | table of file → line % (final) |
| exclusions | cluster → reason (e.g. subprocess CLI path, defensive unreachable arms) |

## Close-out record (tasks.md implementation notes)

Every mismatch found: `parameter → observed vs documented → resolution`
(fix + pinned cell | doc correction), including the R5 footnote.
