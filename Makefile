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

lint:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings

test:
ifeq ($(TARGET),)
	cargo nextest run --workspace
	cargo test --doc --workspace
else ifeq ($(TARGET),unit)
	cargo nextest run --workspace
else ifeq ($(TARGET),e2e)
	cargo nextest run --workspace -E 'binary(/e2e/)' --no-tests=pass
else ifeq ($(TARGET),sweep)
	cargo nextest run -p rdlt-engine --features failpoints -E 'binary(crash_sweep)' --no-tests=pass
	# Postgres sweeps self-skip without a container runtime (G2.1).
	cargo nextest run -p rdlt-connector-postgres --features failpoints -E 'binary(crash_sweep) or binary(dest_crash_sweep) or binary(cdc_crash_sweep)' --no-tests=pass
	cargo nextest run -p rdlt-connector-duckdb --features failpoints -E 'binary(sweep)' --no-tests=pass
	cargo nextest run -p rdlt-connector-rest --features failpoints -E 'binary(sweep)' --no-tests=pass
	cargo nextest run -p rdlt-connector-file --features failpoints -E 'binary(sweep)' --no-tests=pass
	cargo nextest run -p rdlt-connector-iceberg --features failpoints -E 'binary(sweep)' --no-tests=pass
else ifeq ($(TARGET),prop)
	PROPTEST_CASES=4096 cargo nextest run -p rdlt-engine -E 'test(shred_property)' --no-tests=pass
else ifeq ($(TARGET),fuzz)
	cd fuzz && for t in $(FUZZ_TARGETS); do \
		cargo +nightly fuzz run $$t -- -timeout=10 -max_total_time=$(FUZZ_SECONDS) || exit 1; \
	done
else ifeq ($(TARGET),mutants)
	# --jobs 2 + 2 test threads: runaway mutants (broken backpressure bounds)
	# balloon EVERY parallel test at once — two host OOMs taught this. --iterate resumes.
	#
	# TMPDIR is pinned onto the repo's own filesystem because cargo-mutants
	# builds one FULL debug workspace per job in its scratch directory, and the
	# default /tmp is commonly a tmpfs sized well under that (32 GiB here, ~15
	# GiB per copy). Overflowing it aborts the run with a bare "Disk quota
	# exceeded" mid-build, which reads like a host problem rather than a
	# too-small scratch dir. The path is inside target/, so it is already
	# gitignored and `cargo clean` reclaims it.
	mkdir -p target/mutants-tmp
	TMPDIR=$(CURDIR)/target/mutants-tmp NEXTEST_TEST_THREADS=2 cargo mutants --iterate --jobs 2
else ifeq ($(TARGET),deep)
	# RDLT_HEAVY=1: the memory-bound claim must RUN here — missing prereqs
	# (prlimit, release CLI) hard-fail instead of silently skipping. Not on
	# sweep: sweep is part of the PR gate, which stays container-optional.
	RDLT_HEAVY=1 cargo nextest run -p rdlt-connector-postgres -E 'binary(memory_bound)'
	# Spark read-back (016): heavyweight JVM leg, deep tier only.
	RDLT_DEEP=1 cargo nextest run -p rdlt-connector-iceberg -E 'binary(spark_deep)' --no-tests=pass
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
	cargo llvm-cov nextest --features failpoints

check: lint
	$(MAKE) docs
	$(MAKE) test
	$(MAKE) test TARGET=sweep
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
