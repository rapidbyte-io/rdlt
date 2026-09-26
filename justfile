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
    cargo xtask codegen --check
    cargo machete
    cargo deny check
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
    cargo hack check --workspace --each-feature --no-dev-deps
    actionlint
    pinact run --check

# Run the test suite; extra arguments go to nextest
test *args:
    cargo nextest run --workspace --all-features {{ args }}
    cargo test --workspace --all-features --doc

# Run the simulation suite; pass a seed to replay one run, or an empty seed and a count
sim seed="" seeds="1000":
    RDLT_SIM_SEED="{{ seed }}" RDLT_SIM_SEEDS="{{ seeds }}" cargo nextest run --package rdlt-sim --all-features --cargo-profile sim

# Run the simulation on many threads and the real clock, where races the paused single thread
# never meets can happen; its failures name their seed but do not replay exactly
stress seeds="20":
    RDLT_SIM_SEEDS="{{ seeds }}" cargo nextest run --package rdlt-sim --all-features --cargo-profile sim --run-ignored ignored-only -E 'test(many_threads)'

# Measure how much of the engine and connector code the simulation alone reaches, test code and
# the connector's test kit and SQL planner left out, and hold it above its floor
sim-coverage seeds="1000":
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    RDLT_SIM_SEEDS="{{ seeds }}" cargo +{{ nightly }} llvm-cov nextest --branch --workspace --all-features --json --summary-only --output-path target/sim-coverage.json --ignore-filename-regex '(rdlt-sim|rdlt-testkit|rdlt-connector-reference|xtask)/|/tests?(\.rs|/)|differential|reference\.rs|/bench|/testing|sqlgen|encodings\.rs' -E 'package(rdlt-sim) & test(through_faults)'
    cargo xtask coverage-gate target/sim-coverage.json --lines 82 --branches 73

# Measure line and branch coverage and apply the CI gate
coverage:
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    cargo +{{ nightly }} llvm-cov nextest --branch --package rdlt-engine --package rdlt-connector --package rdlt-wire --all-features --json --summary-only --output-path target/coverage.json --ignore-filename-regex '/generated/'
    cargo xtask coverage-gate target/coverage.json --lines 90 --branches 85

# Mutation testing; extra arguments go to cargo-mutants, for example --in-diff pr.diff
mutants *args:
    cargo mutants --package rdlt-engine --package rdlt-connector --package rdlt-wire {{ args }}

# Mutation testing over this branch's changes, including uncommitted ones, against `base`, in
# `jobs` parallel builds; incremental builds are what make rebuilding per mutant cheap
mutants-diff base="origin/main" jobs="4":
    mkdir -p target
    git diff --src-prefix=a/ --dst-prefix=b/ "$(git merge-base {{ base }} HEAD)" > target/mutants.diff
    CARGO_INCREMENTAL=1 cargo mutants --package rdlt-engine --package rdlt-connector --package rdlt-wire --in-diff target/mutants.diff -j {{ jobs }}

# Fuzz one target for a number of seconds, for example `just fuzz state_record 60`
fuzz target seconds="60":
    rustup toolchain install {{ nightly }} --profile minimal
    cargo +{{ nightly }} fuzz run {{ target }} --target "$(rustc -vV | sed -n 's/host: //p')" -- -max_total_time={{ seconds }}

# Everything the pull-request gate runs
ci: lint test coverage (sim "" "10000")

# Everything to run before pushing: the pull-request gate and mutation testing of the change
ready: ci mutants-diff
