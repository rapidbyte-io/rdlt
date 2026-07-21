<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/012-bench-harness/plan.md` (feature: unified benchmark framework
— one dev-only crate `crates/rdlt-bench` (binary: list/run/gate/report,
ZERO new dependencies, publish=false) replacing the six run-*.sh
scripts with declarative TOML cells (benches/cells/, source×dest×
workload matrix, gated|scoreboard classes); metrics from existing seams
only: RunReport rows/bytes → rows/s + MB/s, engine events() →
per-stream attribution (library mode = scoreboard detail; GATED numbers
bind to CLI-subprocess wall time), safe-Rust procfs sampler
(VmHWM/utime — getrusage FFI rejected), cgroup v2 for the dlt
competitor module (self-timed wall kept for continuity); committed
versioned JSON artifacts with environment fingerprint under
benches/results/; the 8 gated bars move to benches/bars.toml enforced
by `rdlt-bench gate`; RESULTS.md tables generated between markers,
narrative preserved; migration must prove continuity — medians in the
±2–10% band of recorded numbers or an explicit version-policy entry,
run-*.sh deleted only after in-band re-measure; iai gate + criterion
shred + hyperfine cold-start protocol retained unchanged; CPU/RSS
recorded NOT gated. Contract: contracts/bench-harness.md BH1-BH8. Zero
SPI change, zero runtime-crate manifest changes).
Previous feature 011 for reference:
`specs/011-connector-verification/plan.md` (postgres connector
verification — traceability matrix PM1-PM8, 88.98% measured line
coverage for rdlt-postgres, R5 typed rejection of explicit
merge_strategy under non-merge modes). Features 005-010
(`specs/0{05,06,07,08,09,10}-*/`) are the merged base being composed;
004's benchmark governance and 003's hardening nets remain in force. The established architecture is feature
001: `specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as
amended by features 002 and 006) remain authoritative; the approved
technical design is `2026-07-18-rdlt-engine-design.md` at the repo root.
Run tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
