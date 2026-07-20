# rdlt — canonical entry points for contributors AND CI (feature 003, FR-014 / gate G4).
# CI invokes these verbs; never duplicate their commands inline anywhere else.
#
#   make build                 debug build, whole workspace
#   make release               optimized CLI with all bundled connectors
#   make lint                  format check + clippy (warnings are errors)
#   make test                  fast suite (nextest + doc-tests)
#     TARGET=unit make test      nextest only
#     TARGET=e2e  make test      end-to-end integration tests only
#     TARGET=sweep make test     crash-point sweep (failpoints feature)
#     TARGET=prop make test      extended property runs (4096 cases)
#     TARGET=fuzz make test      fuzz all targets (nightly toolchain; FUZZ_SECONDS each)
#     TARGET=mutants make test   mutation pass (slow)
#     TARGET=deep make test      everything scheduled CI runs (prop+sweep+mutants+fuzz)
#   make bench                 shred microbench (criterion)
#     TARGET=iai make bench      instruction-count benches + baseline comparison (perf gate)
#     TARGET=e2e make bench      full end-to-end benchmark script
#   make check                 everything a PR must pass (lint + test + sweep + perf gate)
#
# Suites are selected by TARGET; the tools behind them are implementation details.

TARGET ?=
FUZZ_SECONDS ?= 600
FUZZ_TARGETS := jsonl_slab cursor_decode file_config arrow_schema_map shred_push pg_copy_decode

.PHONY: build release lint test bench check

build:
	cargo build --workspace

release:
	cargo build --release -p rdlt-cli

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
	# Postgres sweep self-skips without a container runtime (G2.1).
	cargo nextest run -p rdlt-dest-postgres --features failpoints -E 'binary(crash_sweep)' --no-tests=pass
else ifeq ($(TARGET),prop)
	PROPTEST_CASES=4096 cargo nextest run -p rdlt-engine -E 'test(shred_property)' --no-tests=pass
else ifeq ($(TARGET),fuzz)
	cd fuzz && for t in $(FUZZ_TARGETS); do \
		cargo +nightly fuzz run $$t -- -timeout=10 -max_total_time=$(FUZZ_SECONDS) || exit 1; \
	done
else ifeq ($(TARGET),mutants)
	# --jobs 2 + 2 test threads: runaway mutants (broken backpressure bounds)
	# balloon EVERY parallel test at once — two host OOMs taught this. --iterate resumes.
	NEXTEST_TEST_THREADS=2 cargo mutants --iterate --jobs 2
else ifeq ($(TARGET),deep)
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
	benches/compare-iai.sh
else ifeq ($(TARGET),e2e)
	benches/run-e2e.sh
else
	$(error unknown bench TARGET '$(TARGET)' — see header comment)
endif

check: lint
	$(MAKE) test
	$(MAKE) test TARGET=sweep
	$(MAKE) bench TARGET=iai
