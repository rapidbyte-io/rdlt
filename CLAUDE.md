<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/004-close-perf-misses/plan.md` (feature: close or re-baseline the two
benchmark misses — shred-only profiling toward ≥20× or an evidence-backed bar
adjustment, cold-start conversion to an absolute gated bar; spec, research,
data model, and the measurement-protocol contract live alongside it; feature
003's hardening nets in `specs/003-hardening-performance/` — crash sweep,
mutation/fuzz/property suites, blocking perf gate — remain in force). The
established architecture is feature 001:
`specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as amended by
feature 002) remain authoritative; the approved technical design is
`2026-07-18-rdlt-engine-design.md` at the repo root. Run tests with
`cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
