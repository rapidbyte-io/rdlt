# Contract: Parameter Verification Rules

## Rules

| # | Rule |
|---|---|
| PM1 | Every parameter row in the inventory (research R4) appears in `matrix.md` with ≥1 citing cell per documented behavior AND per validation rule. Zero unresolved rows at close-out. |
| PM2 | Cells prove BEHAVIOR: server-visible effects assert server state; typed errors assert the named offender (table/column/field). A parse-success-only cell is valid ONLY where no behavior is observable beyond parsing (and the row says so). |
| PM3 | Defaults are OBSERVED, not inferred: a default's cell exercises the behavior with the field absent. |
| PM4 | Existing suites are citations — cells are written only for genuine gaps; healthy tests are not rewritten. Every citation must actually pin the row's claim (spot-audit, SC-002). |
| PM5 | Coverage: `make coverage` reproduces the recorded number; the floor is ≥80% line coverage for rdlt-postgres; uncovered clusters are classified with reasons in the record. Coverage-only tests (no behavioral claim) are forbidden. |
| PM6 | Mismatches (behavior ≠ docs ≠ validation) are resolved in this feature: code fix + pinned regression cell (crash-swept when on a publish/read path), or explicit doc correction. Both are listed in the close-out. |
| PM7 | The R5 resolution ships here: EXPLICITLY configured `merge_strategy` (destination-wide or per-table) under an append/replace write mode is a typed error naming table + mode; the unconfigured default never rejects. |
| PM8 | Governance unchanged: SPI frozen (semver-checks "no update required"), no new runtime dependencies, `make check` semantics untouched, gated bars within tolerance. |
