set shell := ["bash", "-euo", "pipefail", "-c"]

nightly := "nightly-2026-09-20"

# The crates mutation testing mutates, and the crates whose tests may catch a mutant: the
# protocol's served end is tested from the host, where a client exists, and the reference
# connectors from the engine and certification too
mutated := "--package rdlt-engine --package rdlt-connector --package rdlt-adopt --package rdlt-wire --package rdlt-host --package rdlt-certify --package rdlt-connector-reference --package rdlt-log-store --test-package rdlt-engine --test-package rdlt-connector --test-package rdlt-adopt --test-package rdlt-wire --test-package rdlt-host --test-package rdlt-certify --test-package rdlt-connector-reference --test-package rdlt-log-store"

# List the recipes
default:
    @just --list

# Format Rust and TOML sources
fmt:
    cargo fmt --all
    taplo fmt

# Run every static check CI runs
lint: checks pins

# Fail when the workspace's lockfile is behind its manifests. It runs before any other cargo
# command, xtask's included, since each would bring the lockfile up to date instead; the other
# workspaces' lockfiles are held by `cargo xtask deny`
locked:
    cargo metadata --locked --format-version 1 > /dev/null

# Every static check that needs no credential: these build and run the workspace's and its
# dependencies' code
checks: locked
    cargo fmt --all --check
    taplo fmt --check
    typos
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo xtask lint
    cargo xtask deps
    cargo xtask codegen --check
    cargo xtask tools
    cargo xtask shipped
    cargo machete
    cargo xtask deny
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
    RUSTFLAGS="-D warnings" cargo hack check --workspace --each-feature --no-dev-deps
    RUSTFLAGS="-D warnings" cargo check --package rdlt-engine --lib --tests
    actionlint

# Check that each action is pinned to the commit of the version beside it; GITHUB_TOKEN, where
# set, keeps the lookups within GitHub's rate limit. CI runs this in a job of its own, the only
# one given the token, so it builds nothing
pins:
    pinact run --check --verify-comment

# Check every workspace's locked dependencies, the fuzzing crate's included, for advisories, bans,
# licenses and sources; a lockfile behind its manifest fails
deny: locked
    cargo xtask deny

# Run the test suite; extra arguments go to nextest. Each benchmark then runs once as a test, in
# a process of its own beside the others, and the allocation counts once, so every bench's setup,
# checks and counts run on every change; the last run is of a connector built without
# certification's probes, which a build with every feature never is: it must serve none
test *args:
    cargo nextest run --workspace --all-features {{ args }}
    cargo test --workspace --all-features --doc
    cargo nextest run --workspace --all-features --benches -E 'kind(bench) & not binary(allocations)'
    cargo test --workspace --all-features --bench allocations
    cargo nextest run --package rdlt-connector --features serve -E 'test(serve::probes)'

# Run the simulation suite; pass a seed to replay one run, or an empty seed, a count and the first
# seed of a shard
sim seed="" seeds="1000" from="0":
    RDLT_SIM_SEED="{{ seed }}" RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_SEEDS_FROM="{{ from }}" cargo nextest run --package rdlt-sim --all-features --cargo-profile sim

# Run a count of seeds from a first seed as shards side by side, one a core by default; each
# shard's output is in `target/sim-shards/<shard>.log`, and a failing shard names its seed there
sim-shards $seeds="100000" $shards=`nproc 2>/dev/null || sysctl -n hw.ncpu` $from="0":
    #!/usr/bin/env bash
    set -euo pipefail
    # A count, a number of shards and a first seed, each small enough that no sum below overflows.
    [[ $seeds =~ ^[1-9][0-9]{0,11}$ ]] || { echo "seeds must be an integer from 1 to 999999999999" >&2; exit 2; }
    [[ $shards =~ ^[1-9][0-9]{0,3}$ ]] || { echo "shards must be an integer from 1 to 9999" >&2; exit 2; }
    [[ $from =~ ^(0|[1-9][0-9]{0,11})$ ]] || { echo "from must be a seed from 0 to 999999999999" >&2; exit 2; }
    cargo nextest run --package rdlt-sim --all-features --cargo-profile sim --no-run
    rm -rf target/sim-shards && mkdir -p target/sim-shards
    per=$(( (seeds + shards - 1) / shards ))
    pids=()
    for shard in $(seq 0 $(( shards - 1 ))); do
        first=$(( from + shard * per ))
        count=$(( from + seeds - first ))
        (( count > per )) && count=$per
        (( count > 0 )) || break
        RDLT_SIM_SEED="" RDLT_SIM_SEEDS="$count" RDLT_SIM_SEEDS_FROM="$first" \
            cargo nextest run --package rdlt-sim --all-features --cargo-profile sim --test-threads 1 \
            > "target/sim-shards/$shard.log" 2>&1 &
        pids+=("$!")
    done
    failed=0
    for shard in "${!pids[@]}"; do
        if wait "${pids[$shard]}"; then
            echo "shard $shard passed"
        else
            echo "shard $shard failed: $(grep -m1 'failing seed' "target/sim-shards/$shard.log" || echo 'see its log')"
            failed=1
        fi
    done
    exit "$failed"

# Crash a pipeline run in a process of its own at every durability step, and kill it or its
# spawned connectors as it loads, then check every row landed once; a seed draws the same kill points
crashes seed="":
    cargo build --package rdlt-engine --features failpoints --examples
    RDLT_KILL_SEED="{{ seed }}" cargo nextest run --package rdlt-engine --features failpoints --test crashes

# Run the S3 log store's tests on S3 servers in containers, which need Docker: its contract, its
# probe, and loads killed as they commit; two containers at most at once
containers *args:
    cargo nextest run --package rdlt-log-store --all-features --ignore-default-filter -E 'binary(containers)' {{ args }}

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
    RDLT_SIM_SEEDS="{{ seeds }}" cargo +{{ nightly }} llvm-cov nextest --branch --workspace --all-features --json --summary-only --output-path target/sim-coverage.json --ignore-filename-regex '(rdlt-sim|rdlt-testkit|rdlt-connector-reference|rdlt-adopt|rdlt-log-store|xtask)/|/tests?(\.rs|/)|differential|reference\.rs|/bench|/testing|sqlgen|encodings\.rs|/generated/|rdlt-host/src/(local|registry|connect|kills|sink|wire)|rdlt-certify/|serve/(args|binary)|wal/(local|memory)\.rs' -E 'package(rdlt-sim) & test(through_faults)'
    cargo xtask coverage-gate target/sim-coverage.json --lines 81 --branches 73

# Measure line and branch coverage and apply the CI gate. Spawned connectors write profiles too, and
# one killed as its test ends leaves a truncated profile, which the merge skips
coverage:
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    cargo +{{ nightly }} llvm-cov nextest --failure-mode all --branch --package rdlt-engine --package rdlt-connector --package rdlt-adopt --package rdlt-wire --package rdlt-host --package rdlt-certify --all-features --json --summary-only --output-path target/coverage.json --ignore-filename-regex '/generated/' -E 'not (package(rdlt-engine) & binary(crashes))'
    cargo xtask coverage-gate target/coverage.json --lines 90 --branches 85

# Run the audited crate's test of its `unsafe` code under Miri: the workspace's only
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
            rdlt-connector) echo "rdlt-connector rdlt-connector-reference rdlt-engine rdlt-host rdlt-certify rdlt-log-store" ;;
            rdlt-adopt) echo "rdlt-adopt rdlt-host" ;;
            rdlt-connector-reference) echo "rdlt-connector-reference rdlt-engine" ;;
            rdlt-wire) echo "rdlt-wire rdlt-host" ;;
            rdlt-host) echo "rdlt-host rdlt-certify rdlt-log-store" ;;
            *) echo "$1" ;;
        esac
    }
    failed=0
    for crate in rdlt-engine rdlt-connector rdlt-adopt rdlt-connector-reference rdlt-wire rdlt-host rdlt-certify rdlt-log-store; do
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

# Build an engine bench for profiling, for example `just profiling shred`: release's code with
# line tables and frame pointers, in a target directory of its own so the flags never rebuild the
# shared one; cargo prints the binary's path last
profiling bench:
    CARGO_TARGET_DIR=target/frame-pointers RUSTFLAGS="-C force-frame-pointers=yes" cargo bench --package rdlt-engine --features bench --profile profiling --bench {{ bench }} --no-run

# Measure a bench as every recorded figure is measured: on the CPUs `cores` lists, every CPU
# this process may use by default, saved as criterion's baseline named after the commit, then each
# benchmark under `perf stat` for the instructions a cycle and the CPUs kept busy, then the
# bench's allocation counts, with the commit, load, governor and thread counts. Linux only, which
# can hold a process to chosen CPUs; the record is in `target/bench`
bench $name $cores="" $filter="":
    #!/usr/bin/env bash
    set -euo pipefail
    [[ "$(uname -s)" == Linux ]] || { echo "just bench runs only on Linux, which can hold a process to chosen CPUs" >&2; exit 2; }
    [[ -n $cores ]] || cores=$(taskset -cp $$ | sed 's/.*: //')
    [[ $name != allocations && -f "crates/rdlt-engine/benches/$name.rs" ]] || { echo "$name is not a timed bench of rdlt-engine" >&2; exit 2; }
    [[ $cores =~ ^[0-9]{1,4}(-[0-9]{1,4})?(,[0-9]{1,4}(-[0-9]{1,4})?)*$ ]] || { echo "cores must be a CPU list such as 0-3" >&2; exit 2; }
    command -v perf > /dev/null || { echo "just bench needs perf" >&2; exit 2; }
    mkdir -p target/bench
    changed=$(git status --porcelain --untracked-files=no)
    commit=$(git rev-parse --short=12 HEAD)${changed:+-changed}
    record="target/bench/$name-$commit.txt"
    cargo bench --package rdlt-engine --features bench --bench "$name" --bench allocations --no-run 2> target/bench/build.log || { cat target/bench/build.log >&2; exit 1; }
    built() { sed -n "s|^ *Executable benches/$1\.rs (\(.*\))\$|\1|p" target/bench/build.log; }
    exe=$(built "$name")
    counter=$(built allocations)
    cpus=()
    IFS=, read -ra parts <<< "$cores"
    for part in "${parts[@]}"; do mapfile -t -O "${#cpus[@]}" cpus < <(seq "${part%-*}" "${part#*-}"); done
    {
        echo "bench $name${filter:+ matching $filter}, commit $(git rev-parse HEAD)${changed:+ with uncommitted changes}"
        echo "$(rustc -V), $(uname -sr)"
        echo "CPUs $cores ($(taskset -c "$cores" nproc) of $(nproc --all)): $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        for cpu in "${cpus[@]}"; do
            policy=/sys/devices/system/cpu/cpu$cpu/cpufreq
            echo "governor $(cat "$policy/scaling_governor" 2>/dev/null || echo unknown), energy preference $(cat "$policy/energy_performance_preference" 2>/dev/null || echo unknown)"
        done | sort | uniq -c | sed 's/^ *\([0-9]*\) /CPUs: \1, /'
        echo "load before: $(cut -d' ' -f1-3 /proc/loadavg)"
    } | tee "$record"
    taskset -c "$cores" "$exe" --bench --noplot --save-baseline "$commit" ${filter:+"$filter"} | tee -a "$record"
    mapfile -t ids < <(taskset -c "$cores" "$exe" --list --format terse ${filter:+"$filter"} | sed -n 's/: benchmark$//p')
    declare -A wall
    for id in "${ids[@]}"; do
        for seconds in 5 15; do
            started=$(date +%s%N)
            taskset -c "$cores" perf stat -x, -o "target/bench/perf-$seconds.csv" -e cycles:u,instructions:u,task-clock "$exe" --bench --exact --profile-time "$seconds" "$id" > /dev/null
            wall[$seconds]=$(( $(date +%s%N) - started ))
        done
        awk -F, -v id="$id" -v wall="$(( wall[15] - wall[5] ))" '
            FNR == 1 { run++ }
            $1 ~ /^[0-9.]+$/ && $3 ~ /cycles/ { cycles[run] += $1 }
            $1 ~ /^[0-9.]+$/ && $3 ~ /instructions/ { instructions[run] += $1 }
            $1 ~ /^[0-9.]+$/ && $3 ~ /task-clock/ { busy[run] += $1 }
            END { printf "%s: %.2f instructions a cycle, %.2f CPUs busy (perf stat, 15 s of it less 5 s)\n", id, (instructions[2] - instructions[1]) / (cycles[2] - cycles[1]), (busy[2] - busy[1]) * 1e6 / wall }
        ' target/bench/perf-5.csv target/bench/perf-15.csv | tee -a "$record"
    done
    taskset -c "$cores" "$counter" "$name" | tee -a "$record"
    echo "load after: $(cut -d' ' -f1-3 /proc/loadavg)" | tee -a "$record"

# Everything the pull-request gate runs
ci: lint test coverage miri (sim "" "10000")

# Everything to run before pushing: the quick checks of the pull-request gate and mutation testing
# of the change; CI runs the rest of the gate (coverage, Miri, the simulation, macOS)
ready base="origin/main": lint test (mutants-diff base)
