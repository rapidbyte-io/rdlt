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

# Run the test suite; extra arguments go to nextest. Each benchmark, and each bench's allocation
# counts, then runs once as a test in a process of its own beside the others, so every bench's
# setup, checks and counts run on every change; the last run is of a connector built without
# certification's probes, which a build with every feature never is: it must serve none
test *args:
    cargo nextest run --workspace --all-features {{ args }}
    cargo test --workspace --all-features --doc
    cargo nextest run --workspace --all-features --benches -E 'kind(bench)'
    cargo nextest run --package rdlt-connector --features serve -E 'test(serve::probes)'

# Build the simulation's integration tests in the `sim` profile, optimised with its overflow
# checks and debug assertions kept, into the archive `sim`, `stress` and `sim-rate` run
sim-archive:
    cargo nextest archive --package rdlt-sim --all-features --cargo-profile sim --test it --archive-file target/sim.tar.zst

# Run the simulation's two sweeps from its archive, each sweep's seeds side by side on the host's
# cores, or on `cores` when given; pass a seed to replay one run, or an empty seed, a count and the
# first seed of a shard. Each seed's timing is a line of `target/sim-timings/<sweep>.jsonl`
sim seed="" seeds="1000" from="0" cores="": sim-archive
    RDLT_SIM_SEED="{{ seed }}" RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_SEEDS_FROM="{{ from }}" RDLT_SIM_CORES="{{ cores }}" cargo nextest run --archive-file target/sim.tar.zst --workspace-remap . -E 'test(through_faults)'

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
stress seeds="20" cores="": sim-archive
    RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_CORES="{{ cores }}" cargo nextest run --archive-file target/sim.tar.zst --workspace-remap . --run-ignored ignored-only -E 'test(many_threads)'

# Measure how many seeds a second the exactly-once sweep runs over seeds 0 to 999, from the
# simulation's archive, and fail below `floor`; docs/perf/sim.md records the rate and the floor
sim-rate floor="7" cores="": sim-archive
    RDLT_SIM_SEED="" RDLT_SIM_SEEDS=1000 RDLT_SIM_SEEDS_FROM=0 RDLT_SIM_CORES="{{ cores }}" cargo nextest run --archive-file target/sim.tar.zst --workspace-remap . -E 'test(=exactly_once::every_row_lands_exactly_once_through_faults_crashes_and_concurrent_runs)'
    cargo xtask sim-rate target/sim-timings/exactly_once.jsonl --floor {{ floor }}

# Measure how much of the engine, connector and host code the simulation alone reaches, and hold
# it above its floor: left out are test code, the connector's test kit and SQL planner, generated
# code, and what the simulation never runs (process placement, a served binary's entry, and the
# write-ahead log's local-directory and in-memory stores: the simulation keeps logs in its own).
# Each sweep's seeds run side by side on the host's cores, or on `cores` when given
sim-coverage seeds="1000" cores="":
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_CORES="{{ cores }}" cargo +{{ nightly }} llvm-cov nextest --branch --workspace --all-features --json --summary-only --output-path target/sim-coverage.json --ignore-filename-regex '(rdlt-sim|rdlt-testkit|rdlt-connector-reference|rdlt-adopt|rdlt-log-store|xtask)/|/tests?(\.rs|/)|differential|reference\.rs|/bench|/testing|sqlgen|encodings\.rs|/generated/|rdlt-host/src/(local|registry|connect|kills|sink|wire)|rdlt-certify/|serve/(args|binary)|wal/(local|memory)(\.rs|/)|/conformance\.rs|rdlt-connector/src/required\.rs|rdlt-engine/src/fixtures(\.rs|/)' -E 'package(rdlt-sim) & test(through_faults)'
    cargo xtask coverage-gate target/sim-coverage.json --lines 79 --branches 69

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

# Builds this tree and `base` with one toolchain and runs each case under valgrind, so Linux
# only; a commit's `Instructions-Accepted: <case>` trailer accepts that case's growth, and the
# report is `target/instructions/report.md`.
# Count each hot path's instructions and allocations here and on `base`, failing past 2% more
instructions base="origin/main":
    cargo xtask instructions --base "{{ base }}" --limit 2

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

# Build a bench for profiling, for example `just profiling shred`: release's code with line
# tables and frame pointers, in a target directory of its own so the flags never rebuild the
# shared one; cargo prints the binary's path last
profiling bench:
    #!/usr/bin/env bash
    set -euo pipefail
    found=$(just --quiet bench-package "{{ bench }}")
    read -r package features <<< "$found"
    CARGO_TARGET_DIR=target/frame-pointers RUSTFLAGS="-C force-frame-pointers=yes" cargo bench --package "$package" $features --profile profiling --bench "{{ bench }}" --no-run

# The package whose manifest declares the bench `name`, and the features it builds with
[private]
bench-package name:
    #!/usr/bin/env bash
    set -euo pipefail
    manifest=$(awk -v name="{{ name }}" '/^\[\[bench\]\]/ { bench = 1; next } /^\[/ { bench = 0 } bench && $0 == "name = \"" name "\"" { print FILENAME }' crates/*/Cargo.toml)
    [[ "{{ name }}" != allocations && -n $manifest ]] || { echo "{{ name }} is not a timed bench of any crate" >&2; exit 2; }
    features=$(grep -q '^bench = ' "$manifest" && echo "--features bench" || true)
    echo "$(basename "$(dirname "$manifest")") $features"

# Measure a bench as every recorded figure is measured: on the CPUs `cores` lists, every CPU
# this process may use by default, saved as criterion's baseline named after the commit, then each
# benchmark under `perf stat` for the instructions a cycle and the CPUs kept busy, and once under
# a count-only `strace -c` for the syscalls a run makes, then the bench's allocation counts, with
# the commit, load, governor and thread counts. Connectors a bench serves in processes of their
# own run on the CPUs `connectors` lists, by default every online CPU outside `cores`; their CPU
# time is in the CPUs kept busy and their syscalls are counted. Linux only, which can hold a
# process to chosen CPUs; the record is in `target/bench`
bench $name $cores="" $filter="" $connectors="":
    #!/usr/bin/env bash
    set -euo pipefail
    [[ "$(uname -s)" == Linux ]] || { echo "just bench runs only on Linux, which can hold a process to chosen CPUs" >&2; exit 2; }
    [[ -n $cores ]] || cores=$(taskset -cp $$ | sed 's/.*: //')
    found=$(just --quiet bench-package "$name")
    read -r package features <<< "$found"
    list='^[0-9]{1,4}(-[0-9]{1,4})?(,[0-9]{1,4}(-[0-9]{1,4})?)*$'
    [[ $cores =~ $list ]] || { echo "cores must be a CPU list such as 0-3" >&2; exit 2; }
    [[ -z $connectors || $connectors =~ $list ]] || { echo "connectors must be a CPU list such as 4-11" >&2; exit 2; }
    for tool in perf strace; do command -v "$tool" > /dev/null || { echo "just bench needs $tool" >&2; exit 2; }; done
    mkdir -p target/bench
    changed=$(git status --porcelain --untracked-files=no)
    commit=$(git rev-parse --short=12 HEAD)${changed:+-changed}
    record="target/bench/$name-$commit.txt"
    { cargo bench --package "$package" $features --bench "$name" --no-run && cargo bench --package rdlt-engine --features bench --bench allocations --no-run; } 2> target/bench/build.log || { cat target/bench/build.log >&2; exit 1; }
    built() { sed -n "s|^ *Executable .* (\(.*/deps/$1-[0-9a-f]*\))\$|\1|p" target/bench/build.log | tail -1; }
    exe=$(built "$name")
    counter=$(built allocations)
    expand() { local part parts; IFS=, read -ra parts <<< "$1"; for part in "${parts[@]}"; do seq "${part%-*}" "${part#*-}"; done; }
    mapfile -t cpus < <(expand "$cores")
    if [[ -z $connectors ]]; then
        connectors=$(expand "$(cat /sys/devices/system/cpu/online)" | grep -vxF -f <(printf '%s\n' "${cpus[@]}") | paste -sd, || true)
    fi
    mapfile -t apart < <([[ -z $connectors ]] || expand "$connectors")
    [[ -z $connectors ]] || export RDLT_BENCH_CONNECTOR_CORES=$connectors
    {
        echo "bench $name${filter:+ matching $filter}, commit $(git rev-parse HEAD)${changed:+ with uncommitted changes}"
        echo "$(rustc -V), $(uname -sr)"
        echo "CPUs $cores ($(taskset -c "$cores" nproc) of $(nproc --all)): $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        echo "connector processes, where the bench spawns any: CPUs ${connectors:-$cores}"
        for cpu in "${cpus[@]}" "${apart[@]}"; do
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
        # A run of several seconds runs about as often in both, which leaves no difference to
        # measure, so a difference of less than five seconds of wall time is reported as such.
        awk -F, -v id="$id" -v wall="$(( wall[15] - wall[5] ))" '
            FNR == 1 { run++ }
            $1 ~ /^[0-9.]+$/ && $3 ~ /cycles/ { cycles[run] += $1 }
            $1 ~ /^[0-9.]+$/ && $3 ~ /instructions/ { instructions[run] += $1 }
            $1 ~ /^[0-9.]+$/ && $3 ~ /task-clock/ { busy[run] += $1 }
            END {
                if (cycles[2] - cycles[1] <= 0 || instructions[2] - instructions[1] <= 0 || wall < 5e9) { printf "%s: a run outlasts what perf stat can difference\n", id; exit }
                printf "%s: %.2f instructions a cycle, %.2f CPUs busy (perf stat, 15 s of it less 5 s)\n", id, (instructions[2] - instructions[1]) / (cycles[2] - cycles[1]), (busy[2] - busy[1]) * 1e6 / wall
            }
        ' target/bench/perf-5.csv target/bench/perf-15.csv | tee -a "$record"
    done
    for id in "${ids[@]}"; do
        taskset -c "$cores" strace -f -c -U name,calls -o target/bench/strace.txt "$exe" --test --exact "$id" > /dev/null
        awk -v id="$id" '
            BEGIN { count = split("read write readv writev recvfrom sendto recvmsg sendmsg pread64 pwrite64 fsync fdatasync openat", names, " ") }
            { calls[$1] = $2 }
            END {
                line = ""
                for (i = 1; i <= count; i++) if (calls[names[i]] > 0) line = line (line == "" ? "" : ", ") calls[names[i]] " " names[i]
                printf "%s: %s a run, its setup included (strace -c, one run in test mode)\n", id, line
            }
        ' target/bench/strace.txt | tee -a "$record"
    done
    if "$counter" --list | grep -qx "$name: test"; then taskset -c "$cores" "$counter" "$name" | tee -a "$record"; fi
    echo "load after: $(cut -d' ' -f1-3 /proc/loadavg)" | tee -a "$record"

# Everything the pull-request gate runs, instruction and allocation counts against main among it.
# Linux only: the counts need valgrind
ci: lint test coverage miri instructions (sim "" "10000")

# Everything to run before pushing: the quick checks of the pull-request gate and mutation testing
# of the change; CI runs the rest of the gate (coverage, Miri, the simulation, macOS, instruction
# and allocation counts against main)
ready base="origin/main": lint test (mutants-diff base)
