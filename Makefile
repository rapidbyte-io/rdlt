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
#     TARGET=deep make test      every heavy suite: prop + sweep + mutants + fuzz,
#                                PLUS the RDLT_HEAVY memory_bound claim and the
#                                RDLT_DEEP Spark read-back. NOTE: no CI schedule
#                                invokes this verb — deep-checks.yml runs prop,
#                                sweep and fuzz nightly and mutants weekly, each
#                                individually, so memory_bound and spark_deep run
#                                HERE or nowhere.
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
#   make check                 everything a PR must pass (lint + docs + test + sweep
#                                + perf gate)
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
FUZZ_TARGETS := jsonl_slab cursor_decode file_config arrow_schema_map shred_push pg_copy_decode pg_pgoutput_decode

.PHONY: build release dist lint docs test bench check coverage reclaim

build:
	cargo build --workspace

release:
	cargo build --release -p rdlt-cli

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
	# The snowflake crash sweep is `#![cfg(feature = "failpoints")]` and no gate
	# command enabled that feature for this crate, so the file was never compiled
	# by any pipeline — it once broke against deleted APIs with every gate green.
	# Type-checked here, not RUN: the sweep itself costs 101.5 min and needs live
	# credentials. The feature is enabled for this crate ALONE, because turning it
	# on workspace-wide changes what compiles in seven others.
	cargo clippy -p rdlt-connector-snowflake --all-targets --features failpoints -- -D warnings

test:
ifeq ($(TARGET),)
	cargo nextest run --workspace
	cargo test --doc --workspace
else ifeq ($(TARGET),unit)
	cargo nextest run --workspace
else ifeq ($(TARGET),e2e)
	cargo nextest run --workspace -E 'binary(/e2e/)'
else ifeq ($(TARGET),sweep)
	# No `--no-tests=pass` on any line below, and that distinction is the point:
	# `--no-tests` governs which tests the runner SELECTS, not whether they then
	# skip. These binaries are always selected and self-skip internally when a
	# container runtime or credentials are absent, so an empty SELECTION only
	# ever means a binary was renamed, deleted, or misspelled here — which must
	# fail. nextest's default is already `fail`; relying on it is deliberate.
	cargo nextest run -p rdlt-engine --features failpoints -E 'binary(crash_sweep)'
	# Postgres sweeps self-skip without a container runtime (G2.1).
	cargo nextest run -p rdlt-connector-postgres --features failpoints -E 'binary(crash_sweep) or binary(dest_crash_sweep) or binary(cdc_crash_sweep)'
	cargo nextest run -p rdlt-connector-duckdb --features failpoints -E 'binary(sweep)'
	cargo nextest run -p rdlt-connector-rest --features failpoints -E 'binary(sweep)'
	cargo nextest run -p rdlt-connector-file --features failpoints -E 'binary(sweep)'
	cargo nextest run -p rdlt-connector-iceberg --features failpoints -E 'binary(sweep)'
else ifeq ($(TARGET),prop)
	# `binary(...)`, not `test(...)`: shred_property is the BINARY; the test
	# inside it is shred_invariants_hold. A test-name filter matched nothing
	# here, and the run reported success having executed zero cases.
	PROPTEST_CASES=4096 cargo nextest run -p rdlt-engine -E 'binary(shred_property)'
else ifeq ($(TARGET),fuzz)
	cd fuzz && for t in $(FUZZ_TARGETS); do \
		cargo +nightly fuzz run $$t -- -timeout=10 -max_total_time=$(FUZZ_SECONDS) || exit 1; \
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
	# RDLT_HEAVY=1: the memory-bound claim must RUN here — missing prereqs
	# (prlimit, release CLI) hard-fail instead of silently skipping. Not on
	# sweep: sweep is part of the PR gate, which stays container-optional.
	RDLT_HEAVY=1 cargo nextest run -p rdlt-connector-postgres -E 'binary(memory_bound)'
	# Spark read-back (016): heavyweight JVM leg, deep tier only.
	RDLT_DEEP=1 cargo nextest run -p rdlt-connector-iceberg -E 'binary(spark_deep)'
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
	cargo bench -p rdlt-engine --bench iai_hotpath -- --save-summary=json
	cargo bench -p rdlt-connector-postgres --bench iai_pg -- --save-summary=json
	benches/compare-iai.sh
else ifeq ($(TARGET),cold)
	# Cold start is a WALL-CLOCK measurement: it needs hyperfine and a quiet
	# machine, neither of which a shared CI runner provides. It rides `make
	# check` locally and the recorded measurement session, never the CI perf
	# gate — where it silently required a tool no workflow installs.
	$(MAKE) release
	benches/check-cold-start.sh
else ifeq ($(TARGET),setup)
	benches/bench-setup.sh
else ifeq ($(TARGET),e2e)
	$(MAKE) release
	sh -c 'E=$$(command -v podman || command -v docker); "$$E" build -q -t rdlt-baseline benches/competitors/dlt/'
	cargo run -q -p rdlt-bench -- run
else ifeq ($(TARGET),matrix)
	$(MAKE) release
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
	cargo run -q -p rdlt-bench -- run --filter '$(TARGET)'
endif

# Measured line coverage; the recorded floor is 80%, enforced at feature
# close-out rather than per-push (no CI gate). The run is WORKSPACE-WIDE —
# `cargo llvm-cov nextest` takes no package filter here — so the floor is read
# against the whole tree. Numbers + exclusions live in benches/RESULTS.md.
# `-D warnings` promotes rustdoc's lints to errors: a dead intra-doc link is a
# defect in what consumers read, and nothing else in the gate looks at rustdoc.
# --all-features so cfg-gated public items are documented too.
docs:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

coverage:
	# The snowflake crash sweep is excluded, and that exclusion is what the
	# recorded coverage figure was actually measured with — it existed only as
	# prose in a close-out until now, so reproducing the number required knowing
	# to look for it. The sweep costs 101.5 min against live credentials and is
	# run separately; every other crate's sweep still runs here, at seconds each.
	cargo llvm-cov nextest --features failpoints \
	  -E 'not (package(rdlt-connector-snowflake) and binary(crash_sweep))'

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
	# e2e was reachable from NO target: it is not in `deep` either, so its two
	# suites were gated by nothing while looking like coverage in the tree.
	$(MAKE) test TARGET=e2e
	$(MAKE) test TARGET=sweep
	$(MAKE) semver
	$(MAKE) bench TARGET=iai
	$(MAKE) bench TARGET=cold

# Reclaim leaked test containers and their volumes.
#
# Scoped by the `rdlt-test=1` label that every start site in this workspace
# applies (rdlt-testkit::containers::RECLAIM_LABEL, and `--label` at the two
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
