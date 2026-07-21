<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/011-connector-verification/plan.md` (feature: postgres connector
verification — every config parameter of source/dest/CLI proven by
behavioral cells via the traceability matrix (contracts/
parameter-matrix.md PM1-PM8; matrix.md is the worklist), ≥80% measured
line coverage for rdlt-postgres via cargo-llvm-cov (`make coverage`,
baseline BEFORE new cells, exclusions classified), every mismatch
resolved incl. the R5 fix: EXPLICIT merge_strategy under append/replace
becomes a typed error while the unconfigured default never rejects.
Audit-first: cite existing suites, write cells only for genuine gaps,
never coverage-only tests. Zero SPI change; dev-tooling deps only).
Previous feature 010 for reference: `specs/010-merge-refinements/plan.md`
(merge refinements for
the postgres destination — `dedup_sort` ordered survivor selection (ONE
ORDER BY rewrite in the shared dedup shape; values beat NULL; ties keep
deterministic last-wins) and `merge_key` scope replacement (scope delete
before the strategy arm; PER-TABLE single-commit-unit rule shared with
scd2 absent-retire — the receipts design was killed by this feature's
own crash sweep, research R2 amendment; a mid-feed-checkpointing scoped
stream's crash window is the recorded residual). Both options are
keyed-structured-only, two-layer validated (parse shape + open-time
existence/collisions, each its own typed error), behavior-unchanged when
absent, dlt-1.29.0 parity with three recorded deviations; contract:
contracts/merge-refinements.md MR1-MR8. Zero SPI change, zero new
dependencies). Features 005-009 (`specs/00{5,6,7,8,9}-*/`) are the
merged base being composed; 004's benchmark governance and 003's
hardening nets remain in force. The established architecture is feature
001: `specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as
amended by features 002 and 006) remain authoritative; the approved
technical design is `2026-07-18-rdlt-engine-design.md` at the repo root.
Run tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
