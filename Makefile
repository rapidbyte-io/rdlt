# rdlt — canonical entry points for contributors AND CI.
# CI invokes these verbs; never duplicate their commands inline anywhere else.
# (CI runs lint/test/perf as PARALLEL jobs rather than calling `make check`;
#  `check` is the contributor-local composition of the same verbs. The 80%
#  coverage floor is enforced at feature close-out, not per-push.)
#
#   make build                 debug build, whole workspace
#   make release               optimized CLI with all bundled connectors
#   make dist                  the SHIPPED CLI (release + symbols stripped)
#   make lint                  format check + clippy (warnings are errors)
#   make test                  fast suite (nextest + doc-tests)
#     TARGET=unit make test      nextest only
#     TARGET=e2e  make test      end-to-end integration tests only
#     TARGET=sweep make test     crash-point sweep (failpoints feature)
#     TARGET=prop make test      extended property runs (4096 cases)
#     TARGET=fuzz make test      fuzz all targets (nightly toolchain; FUZZ_SECONDS each)
#     TARGET=mutants make test   mutation pass (slow)
#     TARGET=deep make test      every heavy suite: prop + sweep + mutants + fuzz.
#                                NOTE: no CI schedule invokes this verb —
#                                deep-checks.yml runs prop, sweep and fuzz
#                                nightly and mutants weekly, each individually.
#   make bench                 shred microbench (criterion)
#     TARGET=iai make bench      instruction-count benches + baseline comparison
#     TARGET=cold make bench     cold-start check (<=40ms); needs hyperfine and
#                                a QUIET machine, so it is local/session-only
#     TARGET=setup make bench    one-shot competitor setup: dlt image + Airbyte
#                                connections (skips Airbyte with guidance when
#                                no abctl cluster is reachable)
#     TARGET=e2e make bench      the e2e cell matrix (rdlt-bench; quiet machine!)
#     TARGET=matrix make bench   the full cell matrix (alias of e2e; long)
#     TARGET=gate make bench     evaluate benches/bars.toml vs committed artifacts
#     TARGET=report make bench   regenerate RESULTS.md tables from artifacts
#     TARGET=<cell-or-glob> make bench   one cell or a slice, e.g.
#                                TARGET=pg-to-pg-1m or TARGET='pg-*'
#                                (cells: cargo run -p rdlt-bench -- list)
#   make check                 everything a PR must pass (lint + docs + test + e2e
#                                + sweep + semver + perf gates)
#                                PREREQUISITES, and they hard-fail rather than skip
#                                (a missing instrument must never read as a passing
#                                gate): `hyperfine` and `python3` for the cold-start
#                                leg, and `valgrind` for the iai instruction-count
#                                leg. Install them or run the legs individually;
#                                do NOT soften the failure.
#   make docs                  build the public documentation with warnings as
#                                errors — a broken intra-doc link is a defect in
#                                the published surface, not a cosmetic detail
#   make coverage              line coverage over the whole workspace; the recorded
#                                floor is 80%, enforced at feature close-out
#   make semver                public-surface comparison for the semver-sacred
#                                crates against the pinned baseline
#   make counts                report per-binary tests-run/skipped counts in the
#                                committed baseline's shape (reports, never fails)
#   make reclaim               remove every container AND volume this workspace
#                                started (label rdlt-test=1). Safe to run any
#                                time: it can only match our own label, never
#                                anything else on the machine. Needed because a
#                                suite killed mid-run never reaches
#                                testcontainers' Drop, and the orphaned
#                                anonymous volumes filled the disk twice in 017.
#
# Suites are selected by TARGET; the tools behind them are implementation details.

TARGET ?=
MUTANTS_TMPDIR ?= $(CURDIR)/target/mutants-tmp
FUZZ_SECONDS ?= 600
# The nightly run bounds each target by wall time; the PR leg overrides
# FUZZ_RUN_BOUND with a small `-runs=` count (deterministic, no flaky
# timeout) so a fuzz-invariant break — including one a seed already
# triggers, since libFuzzer replays the corpus first — surfaces on the
# PR, not the next night.
FUZZ_RUN_BOUND ?= -max_total_time=$(FUZZ_SECONDS)
# The RUN set. These fuzz the engine and the wire decoders; connector
# fuzz harnesses belong to the rdlt-connectors repository (a standing
# owner record there). arrow_ipc_decode is BUILT (below) but not RUN:
# it targets the client's hardened decode seat (decode_one_batch under
# catch_unwind, commit 11a396ed), and that containment is real in
# PRODUCTION (panic=unwind) and pinned by the client's own
# embedded-reproducer unit test — but libfuzzer-sys installs a panic
# hook that abort()s the instant a panic STARTS, before catch_unwind
# can run, so arrow-ipc's internal crafted-frame panic (047 M7 — the
# panic this target found) still reads as a libFuzzer crash under the
# harness. The target stays compiled (the reproducer's home and the
# coverage door); its containment proof lives in the client suite.
# wal_segment_decode (047 3L6) is EXCLUDED for the same reason,
# measured, not assumed: within 60 s it found arrow-buffer 58.3's own
# panic on a crafted record-batch buffer length (one flipped byte in a
# real segment), which the WAL replay seat contains under caught_decode
# in production — pinned by the engine's own one-byte-flip unit test —
# but which abort()s under the libfuzzer hook before containment can
# run. Its corpus carries real writer-produced segments plus that
# crash input, so a future arrow bump can be probed by hand:
#   cd fuzz && cargo +nightly fuzz run wal_segment_decode
FUZZ_TARGETS := jsonl_slab arrow_schema_map shred_push \
	wire_frame_decode handshake_line wal_manifest_line

.PHONY: build release connector-bins dist lint docs test bench check coverage counts semver reclaim

build:
	cargo build --workspace

release:
	cargo build --release -p rdlt-cli

# The reference connector BINARY, release — what this repo's own gates
# (the cold-start instrument's `connector:` arms) spawn on PATH. This
# engine repo builds only the connector it gates on; the first-party
# connectors' release bins come from the sibling rdlt-connectors repo's
# own verbs.
connector-bins:
	cargo build --release -p rdlt-connector-reference --features bin-serve --bin rdlt-connector-reference

# The shipped artifact: release plus symbol stripping. Separate from `release`
# so day-to-day builds keep their symbols for profiling and backtraces.
dist:
	cargo build --profile dist -p rdlt-cli

# check-git-deps.sh is the distribution gate. It runs AHEAD of clippy
# deliberately: it costs under a second, builds nothing, and answers a question
# no other check here asks — whether this workspace can still be published. A
# manifest that cannot ship should not wait behind a multi-minute compile to
# say so. Needs python3 3.11+ (stdlib tomllib).
lint:
	cargo fmt --all --check
	tools/check-git-deps.sh
	cargo clippy --workspace --all-targets -- -D warnings
	# `serve` (038) is OFF by default, so the workspace clippy line above
	# never compiles serve/ at all — a connector that never runs
	# out-of-process must not pay for it, not even a clippy pass. Enabled
	# here for the one crate that owns it; turning it on workspace-wide
	# would pull tonic into every OTHER crate's default clippy run too.
	cargo clippy -p rdlt-connector-sdk --all-targets --features serve -- -D warnings

# THE FAST SUITE, stated once and expanded in BOTH gate blocks — the
# default verb and TARGET=unit (the 024 both-blocks discipline: a suite
# spelled twice is a suite one block eventually loses; the spelling
# lives here alone). One module per NEXTEST invocation throughout:
# nextest fails only a FULLY empty selection, so an OR filter with a
# renamed module beside a live one passes green (measured) — separate
# lines make each module fail its own line.
define fast-suite
cargo nextest run --workspace
# The sdk's `schema` feature gates schema_of and its test; no workspace
# run enables it, so without this line that test had never executed
# (the 024 zero-second-pass class). `-E 'test(schema_of)'` so an empty
# selection — a renamed test — fails rather than passing vacuously.
cargo nextest run -p rdlt-connector-sdk --features schema -E 'test(schema_of)'
# Same class, `serve`: OFF by default. A plain workspace run reaches
# serve/ and its tests only through feature unification (in-tree
# dev-dependencies enable it), which a dependency edit could silently
# undo — these lines are same-shape explicit guards that do not depend
# on it. FIVE SEPARATE `-E` invocations, not one combined `or`
# expression: measured that a combined `test(a) or test(b) or ...`
# fails CLOSED only when EVERY
# clause goes empty at once — renaming just one of the five modules
# still selects the other four and exits 0, silently dropping
# coverage. Each side's own unit tests first (`serve::wire::tests`,
# `serve::source::tests`, `serve::destination::tests`), then the
# tonic-over-UDS integration suites for each half. Same binary each
# time (already built after the first), so this costs nothing beyond
# the five short nextest startups.
cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(serve::wire)'
cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(serve::source)'
cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(serve::destination)'
cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(test_serve_source)'
cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(test_serve_destination)'
# Same class once more, the connector BINARY: behind `bin-serve` +
# `required-features`, so NO workspace command ever compiles it — built
# here explicitly so a bin that stops compiling fails the gate rather
# than rotting unseen (the never-compiled-file lesson from 024). The
# one-module rule applies to the NEXTEST lines (empty-selection
# semantics), not to builds.
cargo build -p rdlt-connector-reference --features bin-serve --bin rdlt-connector-reference
# The certifier bin rides the same discipline: behind `bin` +
# `required-features`. Its OWN invocation — a different feature set
# (`bin`) does not batch with the bin-serve group.
cargo build -p rdlt-certify --features bin --bin rdlt-certify
# rdlt-runtime's spawn-bins suite drives the BUILT reference bin
# through the provider — the T6 smoke (test_spawned_bins) plus the T8
# headline e2e (test_e2e_spawned: a full engine run over the spawned
# reference connector on both sides, and its one crash arm). The env
# var tells the shared helper to (re)build the bins itself, so each
# suite stays honest run alone.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-runtime --features spawn-bins -E 'test(test_spawned_bins)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-runtime --features spawn-bins -E 'test(test_e2e_spawned)'
# The D1 swap's live halves, on the reference bin: the facade's
# `connector:` documents resolve to SPAWNED binaries, so its acceptance
# arm (spawned_pipeline: discovery over the search path, no path:
# override), its load-bearing e2e (the `e2e` binary: Pipeline::from_file +
# persisted cursor across sessions), and the CLI's run/validate/events
# contract pins all need the real bin.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt --features spawn-bins -E 'binary(spawned_pipeline)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt --features spawn-bins -E 'binary(e2e)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-cli --features spawn-bins -E 'test(test_spawned)'
# The reference connector's OWN certification cells: the spawned
# reference bin faces the full clause suite over the wire, BOTH roles —
# hermetic on tempdirs, no container runtime, never skips. (The
# first-party connectors' spawn/certify/kill suites run in the
# rdlt-connectors repo's own gate, beside their crates.)
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-reference --features spawn-bins -E 'test(test_certify_wire)'
# The CERTIFIER's spawn suite (040): rdlt-certify's gated cases drive
# the REAL reference bin through the certification stack — source,
# destination, and the kill matrix (SIGKILL at every K boundary,
# convergence by re-run). The crate's UNGATED tests (report pins, the
# in-process rogue suites) carry no required-features, so the bare
# workspace line at the top already runs them — no line here.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-certify --features spawn-bins -E 'test(test_certify_reference_source)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-certify --features spawn-bins -E 'test(test_certify_reference_destination)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-certify --features spawn-bins -E 'test(test_kill_matrix)'
# The CLI suite additionally enables `bin`: it spawns the certifier
# bin itself (cargo builds it for `CARGO_BIN_EXE_`), pinning the
# stdout/stderr/exit-code contract end to end. (The shell probe's own
# unit pins live in the LIBRARY — `probe::tests` — so the bare
# workspace line already runs them.)
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-certify --features spawn-bins,bin -E 'test(test_cli)'
# The Python proof connector (040): ZERO Rust — the SAME certifier
# bin, the SAME clause vocabulary, against a pure-Python jsonl source
# over the real wire. One compound block so the skip guard covers all
# three steps; skips ONLY when python3 is absent — a broken venv, stub
# drift, or a failed clause FAILS. The venv is cached under
# target/py-certify-venv and rebuilt when requirements.txt changes
# (hash check); the certifier bin was built by the explicit build line
# above; prepending the venv's bin makes the launcher's `python3`
# resolve to the pinned deps.
@if command -v python3 >/dev/null 2>&1; then \
	set -e; \
	req=connectors/python/rdlt-connector-pyjsonl/requirements.txt; \
	venv=target/py-certify-venv; \
	if ! sha256sum --status -c "$$venv/.requirements.sha256" 2>/dev/null; then \
		rm -rf "$$venv"; \
		python3 -m venv "$$venv"; \
		"$$venv/bin/pip" install --quiet -r "$$req"; \
		sha256sum "$$req" > "$$venv/.requirements.sha256"; \
	fi; \
	tools/check-python-stubs.sh; \
	PATH="$$(pwd)/$$venv/bin:$$PATH" $${CARGO_TARGET_DIR:-target}/debug/rdlt-certify --role source \
		--config connectors/python/rdlt-connector-pyjsonl/fixtures/config.json \
		connectors/python/rdlt-connector-pyjsonl/rdlt-connector-pyjsonl; \
else \
	echo "SKIP: python3 absent — the Python proof-connector certification needs it"; \
fi
endef

test:
ifeq ($(TARGET),)
	$(fast-suite)
	cargo test --doc --workspace
else ifeq ($(TARGET),unit)
	$(fast-suite)
else ifeq ($(TARGET),e2e)
	# ONE e2e binary answers this name: the rdlt facade's, spawn-gated
	# behind `spawn-bins` (the load-bearing `binary(e2e)` name —
	# Pipeline::from_file over the spawned reference connector, persisted
	# cursor across sessions). The feature + env var are what keep the
	# selection NON-EMPTY: without them nextest compiles the binary
	# empty, selects zero tests and fails — the 024 empty-selection
	# discipline.
	RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt --features spawn-bins -E 'binary(e2e)'
else ifeq ($(TARGET),sweep)
	# No `--no-tests=pass`, and that distinction is the point: `--no-tests`
	# governs which tests the runner SELECTS, not whether they then skip.
	# An empty SELECTION only ever means a binary was renamed, deleted, or
	# misspelled here — which must fail. nextest's default is already
	# `fail`; relying on it is deliberate. The engine sweep is the one
	# sweep this repo owns; the connector sweeps run in the rdlt-connectors
	# repo's own gate, beside their crates.
	cargo nextest run -p rdlt-engine --features failpoints -E 'binary(crash_sweep)'
else ifeq ($(TARGET),prop)
	# `binary(...)`, not `test(...)`: shred_property is the BINARY; the test
	# inside it is shred_invariants_hold. A test-name filter matched nothing
	# here, and the run reported success having executed zero cases.
	PROPTEST_CASES=4096 cargo nextest run -p rdlt-engine -E 'binary(shred_property)'
else ifeq ($(TARGET),fuzz)
	# Build EVERY target first — the compile gate covers arrow_ipc_decode
	# even while it stays out of the RUN set (the 024 never-compiled-file
	# lesson) — then run only FUZZ_TARGETS under the active bound.
	cd fuzz && cargo +nightly fuzz build
	cd fuzz && for t in $(FUZZ_TARGETS); do \
		cargo +nightly fuzz run $$t -- -timeout=10 $(FUZZ_RUN_BOUND) || exit 1; \
	done
else ifeq ($(TARGET),mutants)
	# --jobs 2 + 2 test threads: runaway mutants (broken backpressure bounds)
	# balloon EVERY parallel test at once — two host OOMs taught this. --iterate resumes.
	#
	# TMPDIR is pinned onto the repo's own filesystem. cargo-mutants builds a
	# full workspace per job in its scratch dir, and the default /tmp is
	# commonly a tmpfs sized well under that — overflowing it aborts the run
	# with a bare "Disk quota exceeded" (EDQUOT, not ENOSPC) mid-build, which
	# reads like a host problem rather than a too-small scratch dir.
	#
	# TMPFS WAS TRIED ON PURPOSE AND MEASURED WORSE — do not try it a third
	# time. Under [profile.mutants] the scratch tree is 9.5 GiB (vs 50-60 GiB
	# under dev), so two jobs DO fit in a 32 GiB tmpfs. But tmpfs pages are RAM:
	# available memory fell 48 GiB -> 24 GiB, `shared` rose to 24 GiB, swap came
	# back into use, and throughput did NOT improve. The writes it was supposed
	# to save were already being absorbed by ~50 GiB of page cache, so the trade
	# was pure RAM starvation for rustc. Same lesson as the 019 allocation
	# removals: measure, because the obvious win can be a loss.
	mkdir -p $(MUTANTS_TMPDIR)
	#
	# --jobserver-tasks caps build concurrency ACROSS both jobs. Without it each
	# job's cargo build claims every core, so two concurrent builds oversubscribe
	# the machine ~3x — and the mutant TEST phase, which is what the timeout
	# actually measures, starves. Measured here: a 9s baseline auto-set a 28s
	# timeout, and 73% of mutants then "timed out" at exactly 28s while builds
	# ran. Those are FALSE results, indistinguishable in the report from a
	# genuinely uncaught mutant.
	#
	# --minimum-test-timeout is the floor that stops a merely-loaded test run
	# from being recorded as a hang. It costs real time on a GENUINE hang, which
	# is the right trade for a gate whose entire purpose is finding mutants the
	# suite fails to catch.
	#
	# --profile mutants: no debug info, no incremental, max codegen units (see
	# [profile.mutants] in Cargo.toml). A mutation run rebuilds the workspace
	# once per mutant, so build time IS the run, and nothing here needs a
	# debuggable binary — the only question asked is whether the suite passes.
	#
	# mold as the linker, scoped to THIS recipe via RUSTFLAGS rather than
	# .cargo/config.toml, so ordinary builds and the gate of record keep the
	# stock toolchain.
	#
	# `--threads=4` is NOT optional. mold defaults to every core and does NOT
	# participate in cargo's jobserver, so `--jobserver-tasks` above caps rustc
	# while leaving linkers unbounded: observed FIVE concurrent ld.mold at
	# 400-613% CPU each, load 44, and wall throughput collapsing to 0.33
	# mutants/min against a per-mutant cost of only ~39s of actual work. The
	# machine was thrashing on linker threads, not linking faster.
	# systemd-run confines the run to its OWN memory cgroup, and this is the
	# mitigation the "two host OOMs" note above asked for but never got.
	#
	# The failure is intrinsic to what this gate DOES: a mutant that breaks the
	# byte-budget backpressure makes the channel queue without limit, and the
	# facade e2e test then grows until the kernel intervenes. Observed twice in
	# one day, the same binary each time, at 24 GB anon-rss — and because it was
	# a GLOBAL oom-kill it took the whole run's session with it, which reads as
	# "something killed my run" rather than "a mutant did exactly what it was
	# supposed to do".
	#
	# Bounded, the kernel kills only inside this scope: the runaway test dies,
	# its mutant is recorded CAUGHT (a test that dies is a test that failed),
	# and the run continues. That is strictly better than surviving the mutant —
	# unbounded memory growth is precisely the defect the byte budget exists to
	# prevent, so killing it IS the correct verdict.
	#
	# MemoryMax is sized from LEGITIMATE peak (two rustc plus two test suites,
	# comfortably under 10G), NOT from the pathology. A first attempt set it to
	# 24G because that is where the runaway was observed — which meant the
	# cgroup had to reach 24G before the kernel acted, starving the host on the
	# way there. Cap just above normal use and a runaway dies early and cheaply.
	#
	# OOMPolicy=continue is what keeps the RUN alive: without it systemd stops
	# the whole scope when any process in it is OOM-killed, so containing the
	# blast radius still ended the run. With it, only the runaway test dies.
	# NOT --quiet: when this silently fails to create the scope the run proceeds
	# UNBOUNDED, which looks identical to success until the host OOMs. Observed:
	# launched from inside a tmux session the processes stayed in the caller's
	# `tmux-spawn-*.scope` and no limit applied. If that happens, bound the live
	# cgroup instead:
	#   systemctl --user set-property <scope> MemoryMax=12G MemorySwapMax=2G
	# reading <scope> from /proc/<pid>/cgroup.
	systemd-run --user --scope \
	  -p MemoryMax=12G -p MemorySwapMax=2G -p OOMPolicy=continue \
	  env RUSTFLAGS="-C link-arg=-fuse-ld=mold -C link-arg=-Wl,--threads=4" \
	  TMPDIR=$(MUTANTS_TMPDIR) NEXTEST_TEST_THREADS=2 \
	  cargo mutants --iterate --jobs 2 --jobserver-tasks 16 \
	    --minimum-test-timeout 180 --profile mutants
else ifeq ($(TARGET),deep)
	# The RDLT_HEAVY memory_bound claim lives with the postgres crate in
	# the rdlt-connectors repo, where `TARGET=deep make test` runs it
	# (that verb installs the release rdlt CLI at the repo's locked rdlt
	# revision and builds the release connector bins the cell spawns).
	$(MAKE) test TARGET=prop
	$(MAKE) test TARGET=sweep
	$(MAKE) test TARGET=mutants
	$(MAKE) test TARGET=fuzz
else
	$(error unknown test TARGET '$(TARGET)' — see header comment)
endif

bench:
ifeq ($(TARGET),)
	cargo bench -p rdlt-engine --bench shred
else ifeq ($(TARGET),iai)
	# The postgres iai bench (iai_pg) moved to rdlt-connectors with its
	# crate; its recorded baselines stay in benches/perf-baselines.json
	# as the dated reference (compare-iai.sh knows they are not run here).
	cargo bench -p rdlt-engine --bench iai_hotpath -- --save-summary=json
	benches/harness/compare-iai.sh
else ifeq ($(TARGET),cold)
	# Cold start is a WALL-CLOCK measurement: it needs hyperfine and a quiet
	# machine, neither of which a shared CI runner provides. It rides `make
	# check` locally and the recorded measurement session, never the CI perf
	# gate — where it silently required a tool no workflow installs. The
	# measured pipeline spawns the reference bin on both sides, so it is
	# built alongside the CLI.
	$(MAKE) release
	$(MAKE) connector-bins
	benches/harness/check-cold-start.sh
else ifeq ($(TARGET),setup)
	benches/harness/bench-setup.sh
else ifeq ($(TARGET),e2e)
	$(MAKE) release
	# Every cell spawns connector bins the CLI build does not produce —
	# build them here, not as a manual preflight.
	$(MAKE) connector-bins
	sh -c 'E=$$(command -v podman || command -v docker); "$$E" build -q -t rdlt-baseline benches/competitors/dlt/'
	cargo run -q -p rdlt-bench -- run
else ifeq ($(TARGET),matrix)
	$(MAKE) release
	# Same as e2e: the cells spawn bins `release` never builds.
	$(MAKE) connector-bins
	sh -c 'E=$$(command -v podman || command -v docker); "$$E" build -q -t rdlt-baseline benches/competitors/dlt/'
	cargo run -q -p rdlt-bench -- run
else ifeq ($(TARGET),gate)
	cargo run -q -p rdlt-bench -- gate
else ifeq ($(TARGET),report)
	cargo run -q -p rdlt-bench -- report
else
	# Anything else is a cell id or glob: TARGET=pg-wide-pg-1m, TARGET='pg-*'.
	# The harness errors loudly when nothing matches (typos stay visible);
	# `cargo run -p rdlt-bench -- list` shows the matrix.
	$(MAKE) release
	# Unconditionally, exactly as `release` above: a cell named here would
	# otherwise seed its whole fixture (up to 1M rows) before dying at spawn
	# on an absent binary. The build is a sub-second no-op once warm.
	$(MAKE) connector-bins
	cargo run -q -p rdlt-bench -- run --filter '$(TARGET)'
endif

# `-D warnings` promotes rustdoc's lints to errors: a dead intra-doc link is a
# defect in what consumers read, and nothing else in the gate looks at rustdoc.
# --all-features so cfg-gated public items are documented too.
docs:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# Measured line coverage; the recorded floor is 80%, enforced at feature
# close-out rather than per-push (no CI gate). The run is WORKSPACE-WIDE —
# `cargo llvm-cov nextest` takes no package filter here — so the floor is read
# against the whole tree. Numbers + exclusions live in benches/GOVERNANCE.md.
coverage:
	# Recorded pre-044 figures were measured over a tree that still
	# carried the connector crates — read them against their own
	# denominator, not this one.
	cargo llvm-cov nextest --features failpoints

# Public-surface comparison for the two semver-sacred crates.
#
# The baseline is PINNED, not a moving branch. A baseline that advanced with every
# merge would silently forgive the break it just accepted; and the automated
# pipeline compares against `origin/main`, which trails local main by dozens of
# commits, so its verdict mixes intended history with a genuine break and can be
# read neither way.
#
# Re-derive the sha with: git merge-base main 024-gate-integrity
SEMVER_BASELINE ?= 34ccd379b3f8c7adcd19ecf827fed3ed133073d9

# Per-binary counts of tests run and skipped, in the shape of the committed
# baseline, so drift is a diff rather than a manual read.
#
# It REPORTS; it does not fail on a difference. Adding a test legitimately changes
# the numbers, and a check that failed on every legitimate addition would train
# everyone to update the baseline without reading it — which is exactly how a
# pinned number stops pinning. Read a difference by direction: run-count up means
# a test was added; run-count down with skip-count up means a suite lost its
# resource or a probe regressed; run-count down with skip-count flat means tests
# disappeared.
counts:
	@cargo nextest run --workspace --no-fail-fast 2>&1 \
	  | grep -oE '[A-Za-z0-9_-]+::[A-Za-z0-9_]+ ' \
	  | awk '{print $$1}' | sed 's/::.*//' | sort | uniq -c \
	  | awk '{printf "%-32s %s\n", $$2, $$1}'

semver:
	cargo semver-checks check-release -p rdlt-core -p rdlt-connector \
	  --baseline-rev $(SEMVER_BASELINE)

check: lint
	$(MAKE) docs
	$(MAKE) test
	# e2e is reachable from no other composed verb (it is not in `deep`
	# either) — without this line its suite would be gated by nothing
	# while looking like coverage in the tree.
	$(MAKE) test TARGET=e2e
	$(MAKE) test TARGET=sweep
	$(MAKE) semver
	$(MAKE) bench TARGET=iai
	$(MAKE) bench TARGET=cold

# Reclaim leaked test containers and their volumes.
#
# Scoped by the `rdlt-test=1` label that every start site in this workspace
# applies (rdlt-testkit::gate::RECLAIM_LABEL, and `--label` at the two
# sites that shell out to the CLI). Volumes are removed SEPARATELY because an
# anonymous volume outlives the container that created it — reaping containers
# alone is what let the disk fill twice during 017.
#
# Whichever engine is present wins; `docker` here may itself be podman's
# compat CLI, which is why the socket-probing order matches the testkit's.
reclaim:
	@engine=""; \
	for candidate in podman docker; do \
	  if $$candidate ps >/dev/null 2>&1; then engine=$$candidate; break; fi; \
	done; \
	if [ -z "$$engine" ]; then \
	  echo "reclaim: no working container engine (podman or docker) — nothing to do"; \
	  exit 0; \
	fi; \
	echo "reclaim: using $$engine"; \
	containers=$$($$engine ps -aq --filter label=rdlt-test=1); \
	if [ -n "$$containers" ]; then \
	  echo "$$containers" | xargs $$engine rm -f -v; \
	else \
	  echo "reclaim: no labelled containers"; \
	fi; \
	volumes=$$($$engine volume ls -q --filter label=rdlt-test=1); \
	if [ -n "$$volumes" ]; then \
	  echo "$$volumes" | xargs $$engine volume rm -f; \
	else \
	  echo "reclaim: no labelled volumes"; \
	fi; \
	echo "reclaim: done"
