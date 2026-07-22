# Contract: Shared Merge Core Rules (SM1–SM8)

| # | Rule |
|---|---|
| SM1 | ONE core: strategy arms, dedup/survivor ordering, scope replacement, hard-delete decision (on the deduped survivor), scd2 lifecycle, per-table single-commit-unit rules, and option validation live in rdlt-connector-sqlcore. No destination re-implements a shape. |
| SM2 | Dialects own SQL TEXT only. A dialect hook receives the shape's full semantic inputs and returns SQL; it never decides ordering, survivorship, or unit rules. |
| SM3 | No silent approximations: a dialect that cannot honor a shape's exact semantics returns a typed capability gap naming option + destination; the open-time validator surfaces it before any data moves. |
| SM4 | Postgres provably unchanged: golden-SQL pins captured BEFORE extraction must match byte-for-byte after; existing postgres suites/sweeps pass without behavioral edits; semver "no update required" for rdlt-core/rdlt-connector; every gated bar within tolerance. |
| SM5 | One vocabulary: option names, YAML shapes, serde spellings, defaults (incl. explicit-vs-default merge_strategy), and typed-error posture are identical across destinations; the shared types are the single source of truth, re-exported at the existing postgres paths. |
| SM6 | Differential equivalence: identical feeds through both destinations yield identical canonical per-table rows and identical typed-error classes, modulo the documented type-affinity table — the ONLY sanctioned differences, each recorded. |
| SM7 | Crash discipline: every new DuckDB strategy arm is swept under the crate's fail points with armed-fire pins, proving crash/rerun convergence and the single-unit rules; the D4 recovery clauses extend to the new arms. |
| SM8 | Governance: zero SPI change, zero new external runtime deps (sqlcore uses existing workspace deps only), WriteMode frozen, behavior unchanged when options absent, duckdb bench cells scoreboard-only, verification to the 011 standard (matrix zero uncited rows; coverage ≥80% with classified exclusions, baseline first). |
