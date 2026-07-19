# Quickstart: rdlt

**Date**: 2026-07-19 · **Phase**: 1 · Two audiences: embedders (using the library) and
contributors (working in this repo).

## For embedders

```toml
# Cargo.toml
[dependencies]
rdlt = { version = "0.1", features = ["rest", "duckdb"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use rdlt::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RdltError> {
    let mut pipeline = Pipeline::builder("github_issues")
        .source(RestSource::from_yaml(include_str!("github.yaml"))?)
        .destination(DuckDb::open("issues.duckdb")?.dataset("raw"))
        .write_mode(WriteMode::Merge { key: &["id"] })
        .build()?;                        // config errors die here, pre-I/O

    let report = pipeline.run().await?;   // resumable: rerun after a crash and it continues
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
```

What you get without declaring any schema: typed columns inferred from data, nested
objects preserved (or flattened collision-safely, per destination), lists of objects as
linked child tables, `_rdlt_*` lineage columns, incremental resume from the last committed
cursor, and a machine-readable `RunReport`. Watch progress with `pipeline.events()`.

Dev CLI equivalent: `rdlt run pipeline.toml`.

## For contributors

```bash
# Toolchain is pinned by rust-toolchain.toml; just build:
cargo build --workspace

# Tests — ALWAYS via nextest (workspace policy); doc-tests are separate:
cargo nextest run
cargo test --doc

# Lints exactly as CI runs them:
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check

# Semver gates (seam crates only; CI-enforced):
cargo semver-checks -p rdlt-core -p rdlt-connector

# The four test tiers (research.md R11):
cargo nextest run -p rdlt-core                 # semantic laws (proptest)
cargo nextest run -p rdlt-engine               # shredder round-trips + unit
cargo nextest run -p rdlt-testkit              # crash-injection + conformance
cargo nextest run --workspace --features integration   # DuckDB/Postgres/wiremock
                                               # (Postgres needs Docker: testcontainers)

# Benchmarks (baseline first — see benches/README):
cargo bench -p rdlt-engine                     # shredder microbench (criterion, per-PR)
./benches/run-e2e.sh                           # pinned-dlt container vs rdlt, full matrix
```

### Where things live

| I want to change… | Go to |
|---|---|
| A vocabulary type, the lattice, hashing, naming | `crates/rdlt-core` (semver-sacred — expect `semver-checks` to interrogate you) |
| The Source/Destination/LoadSession contract | `crates/rdlt-connector` + its conformance tests in `rdlt-testkit` (contract clauses S*/D*/E* in `specs/001-rdlt-ingestion-engine/contracts/connector-spi.md`) |
| Shredding, WAL, recovery, scheduling | `crates/rdlt-engine` (all `pub(crate)` — change freely, keep the crash suite green) |
| A connector | `crates/rdlt-{source,dest}-*` (SPI only; if you need engine internals, the SPI is wrong — raise it) |

### Ground rules

- Crash-injection suite green = mergeable; it is the definition of "correct" here.
- No new `pub` items in seam crates without updating the matching contract doc.
- Every discard/widening/retry must surface in `RunReport` — silent failure is a bug class,
  not a style issue.
