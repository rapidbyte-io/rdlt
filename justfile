set shell := ["bash", "-euo", "pipefail", "-c"]

nightly := "nightly-2026-09-20"

# The crates mutation testing mutates, and the crates whose tests may catch a mutant: the
# protocol's served end is tested from the host, where a client exists, and the reference
# connectors from the engine and certification too
mutated := "--package rdlt-engine --package rdlt-connector --package rdlt-adopt --package rdlt-wire --package rdlt-host --package rdlt-certify --package rdlt-connector-reference --test-package rdlt-engine --test-package rdlt-connector --test-package rdlt-adopt --test-package rdlt-wire --test-package rdlt-host --test-package rdlt-certify --test-package rdlt-connector-reference"

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
    RUSTFLAGS="-D warnings" cargo hack check --workspace --each-feature --no-dev-deps
    actionlint
    pinact run --check

# Run the test suite; extra arguments go to nextest
test *args:
    cargo nextest run --workspace --all-features {{ args }}
    cargo test --workspace --all-features --doc

# Run the simulation suite; pass a seed to replay one run, or an empty seed, a count and the first
# seed of a shard
sim seed="" seeds="1000" from="0":
    RDLT_SIM_SEED="{{ seed }}" RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_SEEDS_FROM="{{ from }}" cargo nextest run --package rdlt-sim --all-features --cargo-profile sim

# Crash a pipeline run in a process of its own at every durability step, and kill it or its
# spawned connectors as it loads, then check every row landed once; a seed draws the same kill points
crashes seed="":
    cargo build --package rdlt-engine --features failpoints --examples
    RDLT_KILL_SEED="{{ seed }}" cargo nextest run --package rdlt-engine --features failpoints --test crashes

# Run the simulation on many threads and the real clock, where races the paused single thread
# never meets can happen; its failures name their seed but do not replay exactly
stress seeds="20":
    RDLT_SIM_SEEDS="{{ seeds }}" cargo nextest run --package rdlt-sim --all-features --cargo-profile sim --run-ignored ignored-only -E 'test(many_threads)'

# Measure how much of the engine, connector and host code the simulation alone reaches, and hold
# it above its floor: left out are test code, the connector's test kit and SQL planner, generated
# code, and what the simulation never runs (process placement, a served binary's entry, and the
# write-ahead log's local-directory and in-memory stores: the simulation keeps logs in its own)
sim-coverage seeds="1000":
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    RDLT_SIM_SEEDS="{{ seeds }}" cargo +{{ nightly }} llvm-cov nextest --branch --workspace --all-features --json --summary-only --output-path target/sim-coverage.json --ignore-filename-regex '(rdlt-sim|rdlt-testkit|rdlt-connector-reference|rdlt-adopt|xtask)/|/tests?(\.rs|/)|differential|reference\.rs|/bench|/testing|sqlgen|encodings\.rs|/generated/|rdlt-host/src/(local|registry|connect|kills|sink|wire)|rdlt-certify/|serve/(args|binary)|wal/(local|memory)\.rs' -E 'package(rdlt-sim) & test(through_faults)'
    cargo xtask coverage-gate target/sim-coverage.json --lines 81 --branches 73

# Measure line and branch coverage and apply the CI gate. Spawned connectors write profiles too, and
# one killed as its test ends leaves a truncated profile, which the merge skips
coverage:
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    cargo +{{ nightly }} llvm-cov nextest --failure-mode all --branch --package rdlt-engine --package rdlt-connector --package rdlt-adopt --package rdlt-wire --package rdlt-host --package rdlt-certify --all-features --json --summary-only --output-path target/coverage.json --ignore-filename-regex '/generated/' -E 'not (package(rdlt-engine) & binary(crashes))'
    cargo xtask coverage-gate target/coverage.json --lines 90 --branches 85

# Run the audited crate's test of its `unsafe` code under Miri (§20.14): the workspace's only
# `unsafe` code
miri:
    rustup toolchain install {{ nightly }} --profile minimal --component miri
    cargo +{{ nightly }} miri test --package rdlt-adopt --lib -- --exact tests::an_owned_descriptor_is_its_socket

# Mutation testing; extra arguments go to cargo-mutants, for example --in-diff pr.diff
mutants *args:
    cargo mutants {{ mutated }} {{ args }}

# Mutation testing over this branch's changes, including uncommitted ones, against `base`, in
# `jobs` parallel builds; incremental builds are what make rebuilding per mutant cheap. Each changed
# crate's mutants run the tests that can catch them, and the nightly full pass runs every crate's
# tests against every mutant (ADR 0025)
mutants-diff base="origin/main" jobs="4":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/mutants
    git diff --src-prefix=a/ --dst-prefix=b/ "$(git merge-base {{ base }} HEAD)" > target/mutants.diff
    # The packages whose tests can catch a crate's mutants: its own, and those that drive it.
    # A function rather than an associative array, which the bash macOS ships lacks.
    catching() {
        case "$1" in
            rdlt-connector) echo "rdlt-connector rdlt-connector-reference rdlt-engine rdlt-host rdlt-certify" ;;
            rdlt-adopt) echo "rdlt-adopt rdlt-host" ;;
            rdlt-connector-reference) echo "rdlt-connector-reference rdlt-engine" ;;
            rdlt-wire) echo "rdlt-wire rdlt-host" ;;
            rdlt-host) echo "rdlt-host rdlt-certify" ;;
            *) echo "$1" ;;
        esac
    }
    failed=0
    for crate in rdlt-engine rdlt-connector rdlt-adopt rdlt-connector-reference rdlt-wire rdlt-host rdlt-certify; do
        grep -q "^+++ b/crates/$crate/" target/mutants.diff || continue
        tests=()
        for package in $(catching "$crate"); do tests+=(--test-package "$package"); done
        CARGO_INCREMENTAL=1 cargo mutants --package "$crate" "${tests[@]}" \
            --in-diff target/mutants.diff -j {{ jobs }} --output "target/mutants/$crate" || failed=1
    done
    exit "$failed"

# Fuzz one target for a number of seconds, for example `just fuzz state_record 60`
fuzz target seconds="60":
    rustup toolchain install {{ nightly }} --profile minimal
    cargo +{{ nightly }} fuzz run {{ target }} --target "$(rustc -vV | sed -n 's/host: //p')" -- -max_total_time={{ seconds }}

# Everything the pull-request gate runs
ci: lint test coverage miri (sim "" "10000")

# Everything to run before pushing: the quick checks of the pull-request gate and mutation testing
# of the change; CI runs the rest of the gate (coverage, Miri, the simulation, macOS)
ready base="origin/main": lint test (mutants-diff base)
