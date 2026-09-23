set shell := ["bash", "-euo", "pipefail", "-c"]

nightly := "nightly-2026-09-20"

# List the recipes
default:
    @just --list

# Format Rust and TOML sources
fmt:
    cargo fmt --all
    taplo fmt

# Run every static check CI runs
lint:
    cargo fmt --all --check
    taplo fmt --check
    typos
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo xtask lint
    cargo xtask deps
    cargo machete
    cargo deny check
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
    cargo hack check --workspace --each-feature --no-dev-deps

# Run the test suite; extra arguments go to nextest
test *args:
    cargo nextest run --workspace --all-features {{ args }}
    cargo test --workspace --all-features --doc

# Run the simulation suite; pass a seed to replay one run, or an empty seed and a count
sim seed="" seeds="1000":
    RDLT_SIM_SEED="{{ seed }}" RDLT_SIM_SEEDS="{{ seeds }}" cargo nextest run --package rdlt-sim --all-features

# Measure line and branch coverage and apply the CI gate
coverage:
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    cargo +{{ nightly }} llvm-cov nextest --branch --package rdlt-engine --all-features --json --summary-only --output-path target/coverage.json
    cargo xtask coverage-gate target/coverage.json --lines 90 --branches 85

# Mutation testing; extra arguments go to cargo-mutants, for example --in-diff pr.diff
mutants *args:
    cargo mutants --package rdlt-engine {{ args }}

# Everything the pull-request gate runs
ci: lint test coverage (sim "" "10000")
