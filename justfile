set shell := ["bash", "-euo", "pipefail", "-c"]

nightly := "nightly-2026-09-20"

# The crates mutation testing mutates, and the crates whose tests may catch a mutant: the
# protocol's served end is tested from the host, where a client exists, and the reference
# connectors from the engine and certification too
mutated := "rdlt-engine rdlt-connector rdlt-adopt rdlt-wire rdlt-host rdlt-certify rdlt-connector-reference"

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
# checks and debug assertions kept, into the archive `sim`, `sim-replay`, `stress` and `sim-rate`
# run
sim-archive:
    cargo nextest archive --package rdlt-sim --all-features --cargo-profile sim --test it --archive-file target/sim.tar.zst

# Run the simulation's two sweeps from its archive, each sweep's seeds side by side on the host's
# cores, or on `cores` (by default `RDLT_SIM_CORES`) when given; pass a seed to replay one run, or
# an empty seed, a count and the first seed of a shard. Each seed's timing is a line of
# `target/sim-timings/<sweep>.jsonl`
sim seed="" seeds="1000" from="0" cores=env("RDLT_SIM_CORES", ""): sim-archive
    RDLT_SIM_SEED="{{ seed }}" RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_SEEDS_FROM="{{ from }}" RDLT_SIM_CORES="{{ cores }}" cargo nextest run --archive-file target/sim.tar.zst --workspace-remap . -E 'test(through_faults)'

# Run each seed of both oracles twice from the simulation's archive, and fail where a seed's two
# runs differ in what any of its phases' runs reported or failed with, in their simulated time
# (but over the network), or in what they left; pass a seed to replay one, or an empty seed, a
# count and the first seed of a shard
sim-replay seed="" seeds="1000" from="0" cores=env("RDLT_SIM_CORES", ""): sim-archive
    RDLT_SIM_SEED="{{ seed }}" RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_SEEDS_FROM="{{ from }}" RDLT_SIM_CORES="{{ cores }}" cargo nextest run --archive-file target/sim.tar.zst --workspace-remap . -E 'test(=determinism::every_seed_replays_alike)'

# Crash a pipeline run in a process of its own at every durability step, and kill it or its
# spawned connectors as it loads, then check every row landed once; a seed draws the same kill points
crashes seed="":
    cargo build --package rdlt-engine --features failpoints --examples
    RDLT_KILL_SEED="{{ seed }}" cargo nextest run --package rdlt-engine --features failpoints --test crashes

# Run the simulation on many threads and the real clock, where races the paused single thread
# never meets can happen; its failures name their seed but do not replay exactly
stress seeds="20" cores=env("RDLT_SIM_CORES", ""): sim-archive
    RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_CORES="{{ cores }}" cargo nextest run --archive-file target/sim.tar.zst --workspace-remap . --run-ignored ignored-only -E 'test(many_threads)'

# Build what `soak` runs: the host's examples, which it spawns as connectors, and its tests
soak-build:
    cargo build --package rdlt-host --all-features --examples
    cargo nextest run --package rdlt-host --all-features --test it --no-run

# Start connectors in processes of their own through `Local` past the groups a process may own,
# racing on real threads, which no simulated seed interleaves, and load through ones killed,
# stalled and crashing for `seconds`; real processes on the real clock, so a failure names its round
soak seconds="60": soak-build
    RDLT_SOAK_SECONDS="{{ seconds }}" cargo nextest run --package rdlt-host --all-features --test it --run-ignored ignored-only -E 'test(/^soak::/)'

# Measure how many seeds a second the exactly-once sweep runs over seeds 0 to 999, from the
# simulation's archive, and fail below `floor`; docs/perf/sim.md records the rate and the floor
sim-rate floor="2" cores=env("RDLT_SIM_CORES", ""): sim-archive
    RDLT_SIM_SEED="" RDLT_SIM_SEEDS=1000 RDLT_SIM_SEEDS_FROM=0 RDLT_SIM_CORES="{{ cores }}" cargo nextest run --archive-file target/sim.tar.zst --workspace-remap . -E 'test(=exactly_once::every_row_lands_exactly_once_through_faults_crashes_and_concurrent_runs)'
    cargo xtask sim-rate target/sim-timings/exactly_once.jsonl --floor {{ floor }}

# Measure how much of the engine, connector, host and data-plane code the simulation alone
# reaches, and hold it above its floor: left out are test code, modules built only for tests and
# benches among it, the connector's test kit and SQL planner, generated code, and what the
# simulation never runs (what process placement leaves to the operating system, files the seeds
# reach in under half their lines, a served binary's entry, and the write-ahead log's
# local-directory and in-memory stores: the simulation keeps logs in its own).
# Each sweep's seeds run side by side on the host's cores, or on `cores` (by default
# `RDLT_SIM_CORES`) when given
sim-coverage seeds="1000" cores=env("RDLT_SIM_CORES", ""):
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    RDLT_SIM_SEEDS="{{ seeds }}" RDLT_SIM_CORES="{{ cores }}" cargo +{{ nightly }} llvm-cov nextest --branch --workspace --all-features --json --summary-only --output-path target/sim-coverage.json --ignore-filename-regex '(rdlt-sim|rdlt-testkit|rdlt-connector-reference|rdlt-adopt|xtask)/|/tests?(\.rs|/)|differential|reference(\.rs|/)|/bench|/testing|sqlgen|encodings\.rs|/generated/|rdlt-engine/src/(conformance|env/inline|shred/numbers)\.rs|rdlt-connector/src/(strategy|serve/live)\.rs|rdlt-host/src/(kills\.rs|local/(bubblewrap|sandbox)|local/grants/opened\.rs|local/process\.rs|local/process/(command|group)|connect|sink)|rdlt-certify/|serve/(args|binary)|wal/(local|memory)(\.rs|/)|rdlt-connector/src/required\.rs|rdlt-engine/src/fixtures(\.rs|/)' -E 'package(rdlt-sim) & test(through_faults)'
    cargo xtask coverage-gate target/sim-coverage.json --lines 81 --branches 71

# Measure line and branch coverage and apply the CI gate. Spawned connectors write profiles too, and
# one killed as its test ends leaves a truncated profile, which the merge skips
coverage:
    rustup toolchain install {{ nightly }} --profile minimal --component llvm-tools-preview
    cargo +{{ nightly }} llvm-cov nextest --failure-mode all --branch --package rdlt-engine --package rdlt-connector --package rdlt-adopt --package rdlt-wire --package rdlt-host --package rdlt-certify --all-features --json --summary-only --output-path target/coverage.json --ignore-filename-regex '/generated/' -E 'not (package(rdlt-engine) & binary(crashes))'
    cargo xtask coverage-gate target/coverage.json --lines 96 --branches 93

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

# Run each resident-memory workload once a process, by cores and with glibc's malloc tunables
# and without, the served one with its connectors spawned, each reporting its own peak, and fail
# where a run raises the host's peak past its budget's bound (ADR 0039); Linux only
resident:
    #!/usr/bin/env bash
    set -euo pipefail
    [[ "$(uname -s)" == Linux ]] || { echo "just resident runs only on Linux, which has /proc" >&2; exit 2; }
    cargo bench --package rdlt-engine --features bench --bench resident --no-run
    cargo bench --package rdlt-host --bench served --no-run
    tunables="glibc.malloc.trim_threshold=268435456:glibc.malloc.mmap_threshold=33554432"
    status=0
    for workload in passthrough normalized; do
        for cores in 2 4 "$(nproc)"; do
            cargo bench --package rdlt-engine --features bench --bench resident -- "$workload" "$cores" || status=1
            GLIBC_TUNABLES="$tunables" cargo bench --package rdlt-engine --features bench --bench resident -- "$workload" "$cores" || status=1
        done
    done
    for cores in 2 4 "$(nproc)"; do
        cargo bench --package rdlt-host --bench served -- --resident "$cores" || status=1
        GLIBC_TUNABLES="$tunables" cargo bench --package rdlt-host --bench served -- --resident "$cores" || status=1
    done
    exit "$status"

# Mutation testing of a crate: its mutants against the tests of every mutated package that links
# it, which are all the tests that can catch one, the crate's own first, where most are caught.
# Extra arguments go to cargo-mutants, for example --shard 1/4 or --in-diff pr.diff
mutants crate *args:
    #!/usr/bin/env bash
    set -euo pipefail
    [[ " {{ mutated }} " == *" {{ crate }} "* ]] || { echo "{{ crate }} is not a crate mutation testing mutates" >&2; exit 2; }
    linking=$(cargo tree --all-features --workspace --invert "{{ crate }}" --edges normal,dev --prefix none --format '{p}' | awk '{ print $1 }' | sort -u)
    tests=()
    for package in $linking; do
        if [[ " {{ mutated }} " == *" $package "* ]]; then tests+=(--test-package "$package"); fi
    done
    CARGO_INCREMENTAL=1 cargo mutants --package "{{ crate }}" "${tests[@]}" {{ args }} -- --profile "mutants-{{ crate }}"

# Mutation testing over this branch's changes, including uncommitted ones, against `base`, in
# `jobs` parallel builds; incremental builds are what make rebuilding per mutant cheap. Each changed
# crate's mutants run the tests of the packages that drive it, and the nightly full pass runs every
# mutant against the tests of every package that links its crate (ADR 0003)
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
            --in-diff target/mutants.diff -j {{ jobs }} --output "target/mutants/$crate" \
            -- --profile "mutants-$crate" || failed=1
    done
    exit "$failed"

# Fuzz one target for a number of seconds, for example `just fuzz state_record 60`, from its
# corpus, which keeps what it finds, and the seeds `fuzz/seeds/<target>` holds for it, if any
fuzz target seconds="60":
    #!/usr/bin/env bash
    set -euo pipefail
    rustup toolchain install {{ nightly }} --profile minimal
    corpora=("{{ justfile_directory() }}/fuzz/corpus/{{ target }}")
    mkdir -p "${corpora[0]}"
    seeds="{{ justfile_directory() }}/fuzz/seeds/{{ target }}"
    if [[ -d "$seeds" ]]; then corpora+=("$seeds"); fi
    cargo +{{ nightly }} fuzz run {{ target }} --target "$(rustc -vV | sed -n 's/host: //p')" "${corpora[@]}" -- -max_total_time={{ seconds }}

# Prove the Kani harnesses of `crate`, or of every crate that holds any, or only those of the
# proofs of `crate`'s `module`, a path such as `budget::admits`, with Kani's release bundle as
# mise.lock holds it: rustup installs the nightly the bundle was built with, a link in the bundle
# names it, and its driver runs as `cargo kani` would run it, reading the crate's own
# `[package.metadata.kani]`, where it turns on what its proofs use; Linux x64 and macOS arm64
kani crate="" module="":
    #!/usr/bin/env bash
    set -euo pipefail
    bundle=$(mise where github:model-checking/kani)
    toolchain=$(cat "$bundle/rust-toolchain-version")
    rustup toolchain install "$toolchain" --profile minimal
    ln -sfn "$(rustc +"$toolchain" --print sysroot)" "$bundle/toolchain"
    if [[ "$("$bundle/toolchain/bin/rustc" --version)" != "$(cat "$bundle/rustc-version")" ]]; then
        echo "$toolchain is not the rustc Kani was built with: $(cat "$bundle/rustc-version")" >&2
        exit 2
    fi
    crates="{{ crate }}"
    module="{{ module }}"
    [[ -z $module || -n $crates ]] || { echo "a module is proved with its crate named" >&2; exit 2; }
    [[ -n $crates ]] || crates=$(just --quiet proving | cut -d: -f1 | sort -u)
    [[ -n $crates ]] || { echo "no crate holds a Kani harness" >&2; exit 2; }
    for crate in $crates; do
        RUSTUP_TOOLCHAIN="$toolchain" PATH="$bundle/bin:$PATH" bash -c 'exec -a cargo-kani "$0" "$@"' "$bundle/bin/kani-driver" --manifest-path "crates/$crate/Cargo.toml" --package "$crate" ${module:+--harness "$module::proofs::"} --output-format terse
    done

# The modules that hold Kani harnesses, those with a module only Kani compiles, one a line as
# `<crate>:<module>`, such as `rdlt-engine:budget::admits` for `src/budget/admits.rs`, or as
# `<crate>` alone for a crate whose root holds them, which stands for all of the crate's
[private]
proving:
    #!/usr/bin/env bash
    set -euo pipefail
    files=$(grep -rlE --include='*.rs' '^[[:space:]]*#\[cfg\(kani\)\]$' crates || true)
    modules=$(for file in $files; do
        [[ $file =~ ^crates/([^/]+)/src/(.+)\.rs$ ]] || { echo "$file holds Kani harnesses outside a library's src/" >&2; exit 2; }
        module=${BASH_REMATCH[2]//\//::}
        if [[ $module == lib ]]; then echo "${BASH_REMATCH[1]}"; else echo "${BASH_REMATCH[1]}:$module"; fi
    done | sort -u)
    for module in $modules; do
        [[ $module == *:* ]] && grep -qx "${module%%:*}" <<< "$modules" && continue
        echo "$module"
    done

# Check every spec under `specs/`: typecheck it, run its tests, and simulate it against its
# invariants (each a `val` whose name begins with a capital letter) as its `// quint steps:` and
# `// quint run:` lines say. It runs Quint's TypeScript simulator, and fails if Quint fetched
# anything
quint:
    #!/usr/bin/env bash
    set -euo pipefail
    export QUINT_HOME="$PWD/target/quint-home"
    rm -rf "$QUINT_HOME"
    mkdir -p "$QUINT_HOME"
    for spec in specs/*.qnt; do
        name=$(basename "$spec" .qnt)
        steps=$(sed -n 's|^// quint steps: ||p' "$spec")
        samples=$(sed -n 's|^// quint run: ||p' "$spec")
        invariants=$(just --quiet properties val "$spec")
        if [[ -z $invariants ]]; then
            echo "$spec names no invariant" >&2
            exit 1
        fi
        quint typecheck "$spec"
        # A spec names the modules to test and simulate, or has only the one named for it.
        mains=$(sed -n 's|^// quint main: ||p' "$spec")
        for main in ${mains:-$name}; do
            quint test --backend typescript --main "$main" "$spec"
            # shellcheck disable=SC2086
            quint run --backend typescript --main "$main" "$spec" $steps ${samples:---max-samples 10000} --invariants $invariants
        done
    done
    fetched=$(find "$QUINT_HOME" -mindepth 1 -maxdepth 1)
    if [ -n "$fetched" ]; then
        echo "quint fetched a tool into $QUINT_HOME: $fetched" >&2
        exit 1
    fi

# The names of `spec`'s invariants (`kind` val) or temporal properties (`kind` temporal): each
# definition of that kind whose name begins with a capital letter, its type given or not; fails
# on such a definition whose name it cannot read
[private]
properties kind spec:
    #!/usr/bin/env bash
    set -euo pipefail
    names=$(sed -nE 's/^[[:space:]]*{{ kind }}[[:space:]]+([A-Z][A-Za-z0-9_]*)[[:space:]]*(:[^=]*)?=.*/\1/p' "{{ spec }}")
    defined=$(grep -cE '^[[:space:]]*{{ kind }}[[:space:]]+[A-Z]' "{{ spec }}" || true)
    if [[ $(grep -c . <<< "$names" || true) != "$defined" ]]; then
        echo "{{ spec }} defines $defined capitalised {{ kind }}s, of which only these are read: $names" >&2
        exit 1
    fi
    echo $names

# Model-check every spec under `specs/`: its invariants by Apalache over every run of up to
# `steps` steps, then its invariants and temporal properties, each a `temporal` whose name begins
# with a capital letter, by TLC over every state it reaches; Apalache and the JDK as mise.lock
# holds them, never one Quint would fetch: Quint runs with a fresh `QUINT_HOME` holding only the
# link to the locked distribution, the recipe refuses an `apalache-mc` that does not report the
# locked version, fails if anything else appears in the directory, and fails a check that ran on
# an Apalache server it did not start, which Quint would use unchecked; the servers write their
# output under `target/apalache`
quint-verify steps="10":
    #!/usr/bin/env bash
    set -euo pipefail
    export QUINT_HOME="$PWD/target/quint"
    apalache=$(mise where github:apalache-mc/apalache)
    version=$(basename "$apalache")
    reported=$("$apalache/bin/apalache-mc" version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n 1)
    if [[ $reported != "$version" ]]; then
        echo "apalache-mc reports $reported, mise.lock holds $version" >&2
        exit 2
    fi
    rm -rf "$QUINT_HOME"
    mkdir -p "$QUINT_HOME/apalache-dist-$version"
    ln -sfn "$apalache" "$QUINT_HOME/apalache-dist-$version/apalache"
    printf '{"maxHeap": "-Xmx4G", "workers": 4}\n' > "$QUINT_HOME/tlc.json"
    unfetched() {
        local extra
        extra=$(find "$QUINT_HOME" -mindepth 1 -maxdepth 1 ! -name "apalache-dist-$version" ! -name tlc.json
                find "$QUINT_HOME/apalache-dist-$version" -mindepth 1 -maxdepth 1 ! -name apalache)
        if [[ -n $extra || ! -L "$QUINT_HOME/apalache-dist-$version/apalache" ]]; then
            echo "quint fetched into $QUINT_HOME: $extra" >&2
            exit 1
        fi
    }
    started() {
        if ! grep -qF "APALACHE version: $version " "$out/quint.log"; then
            echo "quint used an Apalache server it did not start" >&2
            exit 1
        fi
    }
    specs=("$PWD"/specs/*.qnt)
    out="$PWD/target/apalache"
    rm -rf "$out"
    mkdir -p "$out"
    cd "$out"
    for spec in "${specs[@]}"; do
        invariants=$(just --quiet properties val "$spec" | tr ' ' ,)
        if [[ -z $invariants ]]; then
            echo "$spec names no invariant" >&2
            exit 1
        fi
        temporal=$(just --quiet properties temporal "$spec" | tr ' ' ,)
        # A spec names the checker that fits it: `tlc` for a finite model TLC explores whole,
        # `apalache` for one too large to enumerate, both when it names none (ADR 0052).
        checker=$(sed -nE 's#^// quint verify: (tlc|apalache).*$#\1#p' "$spec")
        bound=$(sed -nE 's#^// quint verify: (apalache )?--max-steps ([0-9]+)$#\2#p' "$spec")
        if [[ $checker != tlc ]]; then
            quint verify "$spec" --apalache-version "$version" --max-steps "${bound:-{{ steps }}}" --invariant "$invariants" 2>&1 | tee "$out/quint.log"
            started
            unfetched
        fi
        if [[ $checker != apalache ]]; then
            quint verify "$spec" --apalache-version "$version" --backend tlc --tlc-config "$QUINT_HOME/tlc.json" --invariant "$invariants" ${temporal:+--temporal "$temporal"} 2>&1 | tee "$out/quint.log"
            started
            unfetched
        fi
    done

# The formal checks of this branch's changes against `base`, uncommitted ones included: each part
# `formal-parts` names, in turn
formal-diff base="origin/main":
    #!/usr/bin/env bash
    set -euo pipefail
    parts=$(just --quiet formal-parts "{{ base }}")
    for part in $(tr -d '[]"' <<< "$parts" | tr , ' '); do just formal-part "$part"; done

# The parts of the formal checks that this branch's changes against `base`, uncommitted ones
# included, need, as a JSON list: `kani:<crate>:<module>` for each module of a changed crate that
# holds Kani harnesses, `kani:<crate>` where the crate's root holds them, and `quint` where
# `specs/` changed; every part where the tools, the recipes, the pull-request workflow or any
# input of every crate's build changed
[private]
formal-parts base="origin/main":
    #!/usr/bin/env bash
    set -euo pipefail
    changed=$(git diff --name-only "$(git merge-base "{{ base }}" HEAD)")
    everything=$(grep -xE 'mise\.(toml|lock)|justfile|\.github/workflows/ci\.yml|Cargo\.(toml|lock)|rust-toolchain\.toml|\.cargo/.*' <<< "$changed" || true)
    proving=$(just --quiet proving)
    [[ -n $proving ]] || { echo "no crate holds a Kani harness" >&2; exit 2; }
    parts=()
    for module in $proving; do
        if [[ -n $everything ]] || grep -q "^crates/${module%%:*}/" <<< "$changed"; then parts+=("\"kani:$module\""); fi
    done
    if [[ -n $everything ]] || grep -q '^specs/' <<< "$changed"; then parts+=('"quint"'); fi
    (IFS=,; echo "[${parts[*]}]")

# Run one part that `formal-parts` names: `kani:<crate>:<module>` proves the module's harnesses,
# `kani:<crate>` the crate's, `quint` checks the specs
[private]
formal-part part:
    #!/usr/bin/env bash
    set -euo pipefail
    part="{{ part }}"
    case $part in
        kani:*:*) module=${part#kani:}; just kani "${module%%:*}" "${module#*:}" ;;
        kani:*) just kani "${part#kani:}" ;;
        quint) just quint ;;
        *) echo "no formal part is named $part" >&2; exit 2 ;;
    esac

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
    [[ "{{ name }}" != allocations && "{{ name }}" != resident && -n $manifest ]] || { echo "{{ name }} is not a timed bench of any crate" >&2; exit 2; }
    features=$(grep -q '^bench = ' "$manifest" && echo "--features bench" || true)
    echo "$(basename "$(dirname "$manifest")") $features"

# Measure a bench as every recorded figure is measured: on the CPUs `cores` lists, every CPU
# this process may use by default, saved as criterion's baseline named after the commit, then each
# benchmark under `perf stat` for the instructions a cycle and the CPUs kept busy, and once under
# a count-only `strace -c` for the syscalls a run makes, and where a run syncs files once more
# under `strace -y` for the files it syncs, each named by its place under the temporary directory
# that holds it, then the bench's allocation counts at full size (`--bench`), with the commit,
# load, governor and thread counts. Connectors a bench
# serves in processes of their own run on the CPUs `connectors` lists, by default every online CPU
# outside `cores`; their CPU time is in the CPUs kept busy and their syscalls are counted. Linux
# only, which can hold a process to chosen CPUs; the record is in `target/bench`
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
        awk '($1 == "fsync" || $1 == "fdatasync") && $2 > 0 { synced = 1 } END { exit !synced }' target/bench/strace.txt || continue
        taskset -c "$cores" strace -f -y --seccomp-bpf -e trace=fsync,fdatasync -o target/bench/synced.txt "$exe" --test --exact "$id" > /dev/null
        # A file under a directory tempfile made, `.tmp` and a suffix, is named by the first part
        # of its path below it, `.` for that directory itself; any other by its whole path.
        awk '
            match($0, /(fsync|fdatasync)\([0-9]+</) {
                path = substr($0, RSTART + RLENGTH)
                sub(/>.*/, "", path)
                if (match(path, /\/\.tmp[^\/]*/)) {
                    path = substr(path, RSTART + RLENGTH)
                    sub(/^\//, "", path)
                    sub(/\/.*/, "", path)
                    if (path == "") path = "."
                }
                synced[path]++
            }
            END { for (path in synced) print synced[path], path }
        ' target/bench/synced.txt | sort -k 2 | awk -v id="$id" '
            { files = files (NR > 1 ? ", " : "") $1 " " $2 }
            END { printf "%s: fsyncs and fdatasyncs by file: %s (strace -f -y, one run in test mode)\n", id, files }
        ' | tee -a "$record"
    done
    if "$counter" --list | grep -qx "$name: test"; then taskset -c "$cores" "$counter" --bench "$name" | tee -a "$record"; fi
    echo "load after: $(cut -d' ' -f1-3 /proc/loadavg)" | tee -a "$record"

# Everything the pull-request gate runs, instruction and allocation counts against main among it.
# Linux only: the counts need valgrind
ci: lint test (formal-diff "origin/main") coverage miri instructions (sim "" "10000")

# Everything to run before pushing: the quick checks of the pull-request gate, and the formal
# checks and mutation testing of the change; CI runs the rest of the gate (coverage, Miri, the
# simulation, macOS, instruction and allocation counts against main) and the formal checks again
ready base="origin/main": lint test (formal-diff base) (mutants-diff base)
