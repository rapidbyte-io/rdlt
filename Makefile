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
#   make certify-snowflake     live snowflake destination certification, BY HAND
#                                only (never part of check/test — it talks to a
#                                real account, the snowflake-sweep discipline).
#                                Reads config from
#                                ~/.config/rdlt/snowflake/certify.json; without
#                                that file it announces the skip and exits 0.
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

.PHONY: build release connector-bins dist lint docs test bench check coverage reclaim certify-snowflake

build:
	cargo build --workspace

release:
	cargo build --release -p rdlt-cli

# The connector BINARIES the `-remote` benchmark cells spawn over the wire.
# `release` builds the CLI alone, so without this the five remote twins'
# `connector: path: {{bins}}/…` overrides point at a file the matrix never
# built and the run dies MID-SESSION, after the fixtures are up. Release
# unconditionally, matching what `{{bins}}` resolves to (rdlt-bench
# paths.rs): a measured cell must spawn the shipped shape, and a debug bin
# beside the release engine would measure the wire's overhead wrong.
connector-bins:
	cargo build --release -p rdlt-connector-postgres --features bin-serve --bin rdlt-connector-postgres
	cargo build --release -p rdlt-connector-file --features bin-serve --bin rdlt-connector-file

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
	# The snowflake crash sweep is `#![cfg(feature = "failpoints")]` and no gate
	# command enabled that feature for this crate, so the file was never compiled
	# by any pipeline — it once broke against deleted APIs with every gate green.
	# Type-checked here, not RUN: the sweep itself costs 101.5 min and needs live
	# credentials. The feature is enabled for this crate ALONE, because turning it
	# on workspace-wide changes what compiles in seven others.
	cargo clippy -p rdlt-connector-snowflake --all-targets --features failpoints -- -D warnings

# THE SPAWN-SUITE MATRIX, stated once (round-4 fix: twelve-plus lines
# were duplicated between the full gate and TARGET=unit, and a suite
# added to one block could silently miss the other). Expanded in BOTH
# gate blocks — the 024 both-blocks discipline stands, the spelling
# lives here alone. One module per invocation throughout: nextest
# fails only a FULLY empty selection, so an OR filter with a renamed
# module beside a live one passes green (measured) — separate lines
# make each module fail its own line.
define spawn-suite-matrix
# Same class once more, the connector BINARIES (039 T6): behind
# `bin-serve` + `required-features`, so NO workspace command ever
# compiles them — built here explicitly so a bin that stops
# compiling fails the gate rather than rotting unseen (the
# snowflake-crash-sweep lesson). Then rdlt-runtime's spawn-bins
# suite drives the BUILT bins through the provider — the T6 smoke
# (test_spawned_bins) plus the T8 headline e2e (test_e2e_file: a
# full engine run over spawned connectors on both sides, and its
# one crash arm); the env var tells the shared helper to (re)build
# the bins itself, so the suite stays honest run alone. ONE module
# per invocation applies to the NEXTEST lines below (empty-selection
# semantics), not to builds: the seven bin-serve bins batch into ONE
# cargo invocation (round-10 — eight sequential invocations paid
# resolution, the target-dir lock and process startup eight times per
# gate block). The BARE `--features bin-serve` spelling is deliberate
# and measured: cargo applies it to every selected package (each
# defines the feature; one that dropped it would fail the line), while
# the package-prefixed `rdlt-connector-postgres/bin-serve` form does
# NOT register for the one crate whose workspace dependency entry pins
# `default-features = false` — its bin then fails required-features. A
# build failure still names its package.
cargo build \
  -p rdlt-connector-file -p rdlt-connector-snowflake -p rdlt-connector-postgres \
  -p rdlt-connector-rest -p rdlt-connector-duckdb -p rdlt-connector-iceberg \
  -p rdlt-connector-oracle \
  --features bin-serve \
  --bin rdlt-connector-file --bin rdlt-connector-snowflake --bin rdlt-connector-postgres \
  --bin rdlt-connector-rest --bin rdlt-connector-duckdb --bin rdlt-connector-iceberg \
  --bin rdlt-connector-oracle
# The certifier bin rides the same discipline: behind `bin` +
# `required-features`, built here explicitly so a CLI that stops
# compiling fails the gate rather than rotting unseen. Its OWN
# invocation — a different feature set (`bin`) does not batch with
# the bin-serve group.
cargo build -p rdlt-certify --features bin --bin rdlt-certify
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-runtime --features spawn-bins -E 'test(test_spawned_bins)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-runtime --features spawn-bins -E 'test(test_e2e_file)'
# The postgres bin's OWN spawn suite (041) — the crate's gated cases
# drive the built bin through the provider's Spec RPC (identity,
# version, exit codes), same env-var discipline as the runtime lines.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-postgres --features fixtures,spawn-bins -E 'test(test_spawned_bin)'
# CDC over the wire (041 Task 1): the spawned pg bin against a live
# logical-replication container — snapshot, cursor JSON round-trip,
# resumed change pass across two processes, slot persistence parity.
# Skip-not-fail without a container runtime, own line per the
# one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-postgres --features fixtures,spawn-bins -E 'test(test_cdc_wire)'
# The certification cells (041 Task 3): the spawned pg bin faces the
# FULL clause suite over the wire against a live container, both
# roles — S1/S2/S4 + P1-P7 (source, certified twice in a row) and
# D1-D6 + D8 LIVE + P1-P10 (destination). Skip-not-fail without a
# container runtime, own line per the one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-postgres --features fixtures,spawn-bins -E 'test(test_certify_wire)'
# The kill matrix (041 Task 4): the spawned pg bin SIGKILLed at
# every K boundary against a live container — the first kill matrix
# against a REAL database (the certify crate's own cell below is
# hermetic on the file connector). Skip-not-fail without a container
# runtime, own line per the one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-postgres --features fixtures,spawn-bins -E 'test(test_kill_wire)'
# The rest bin's OWN spawn suite (042 Task 5), the first SOURCE-ONLY
# port: spawn smoke (identity, --version, exit 2 — including
# --role=destination), then certification (S1/S2/S4 + P1-P7, twice
# in a row) and the source kill matrix (K-S1..K-S3) over the real
# wire against a LOCAL wiremock stub — NEVER the live PokeAPI (that
# cell stays behind RDLT_NET and is never a kill subject). No
# container runtime involved, so these cells never skip; own line
# per the one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-rest --features spawn-bins -E 'test(test_spawned_bin)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-rest --features spawn-bins -E 'test(test_certify_wire)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-rest --features spawn-bins -E 'test(test_kill_wire)'
# The duckdb bin's OWN spawn suite (042 Task 6), the first
# SINGLE-WRITER destination port: spawn smoke (identity, --version,
# exit 2 — including --role=source on a destination-only crate, plus
# the cross-process lock-conflict FATAL refusal, D-042-2), then
# certification (D1-D6 + D8 live + ALL TEN P-clauses incl. P11/P12,
# the first destination port certifying against them) and the
# destination kill matrix (K-D1..K-D6), all hermetic on tempdir
# database files — no container runtime, so these cells never skip;
# own line per the one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-duckdb --features spawn-bins -E 'test(test_spawned_bin)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-duckdb --features spawn-bins -E 'test(test_certify_wire)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-duckdb --features spawn-bins -E 'test(test_kill_wire)'
# The support module's OWN pin (the shared count_at probe helper's
# absence-vs-broken-read rule) lives at cases::support::probe, which
# none of the three case-module filters above matches — without this
# line it is compiled behind `spawn-bins` yet selected by NOTHING
# (the 024 zero-coverage class). Own line per the
# one-module-per-invocation rule; an empty selection (module renamed)
# fails the line.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-duckdb --features spawn-bins -E 'test(support::probe)'
# The iceberg bin's OWN spawn suite (042 Task 7), the first CATALOG
# destination port: spawn smoke (identity, --version, exit 2 —
# including --role=source on a destination-only crate; offline, never
# skips), then certification and the destination kill matrix
# (K-D1..K-D6, all six arms run live — D-042-3) against the
# Polaris/RUSTFS fixture. The two live cells are skip-not-fail
# without a container runtime and ride the `iceberg-live` nextest
# group by package filter; own line per the
# one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-iceberg --features spawn-bins -E 'test(test_spawned_bin)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-iceberg --features spawn-bins -E 'test(test_certify_wire)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-iceberg --features spawn-bins -E 'test(test_kill_wire)'
# The oracle bin's OWN spawn suite (042 Task 8), the port with the
# PRE-SPAWN CLIENT PROBE: the driver dlopens an Oracle Client at
# RUNTIME, so the bin probes BEFORE the handshake line and a missing
# client is a typed stderr refusal + exit 1 with stdout EMPTY —
# never an opaque spawn death. The spawn smoke pins BOTH probe arms
# (each skips, announced, where the other has the subject) plus
# identity/--version/exit 2; certification (S1/S2/S4 + P1-P7, twice
# in a row) and the source kill matrix (K-S1..K-S3) run against the
# live Oracle Free container with DOUBLE skip-not-fail — no
# container runtime AND no client each announce their own reason.
# The whole package rides the `oracle-live` nextest group (the ~75 s
# boots, bounded at 3); own line per the one-module-per-invocation
# rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-oracle --features spawn-bins -E 'test(test_spawned_bin)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-oracle --features spawn-bins -E 'test(test_certify_wire)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-oracle --features spawn-bins -E 'test(test_kill_wire)'
# Same class, the CERTIFIER's spawn suite (040): rdlt-certify's gated
# cases drive the REAL file bin through the certification stack —
# source, destination, and the kill matrix (SIGKILL at every K
# boundary, convergence by re-run) — behind `spawn-bins` +
# RDLT_BUILD_CONNECTOR_BINS=1 exactly like the runtime lines above
# (same bin, already built by the build line; the env var keeps the
# suite honest run alone). ONE module per invocation, as everywhere
# in this block. The crate's UNGATED tests (report pins, the
# in-process rogue suites) carry no required-features, so the bare
# workspace line at the top already runs them — no line here.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-certify --features spawn-bins -E 'test(test_certify_file_source)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-certify --features spawn-bins -E 'test(test_certify_file_destination)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-certify --features spawn-bins -E 'test(test_kill_matrix)'
# The CLI suite additionally enables `bin`: it spawns the certifier
# bin itself (cargo builds it for `CARGO_BIN_EXE_`), pinning the
# stdout/stderr/exit-code contract end to end.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-certify --features spawn-bins,bin -E 'test(test_cli)'
endef

test:
ifeq ($(TARGET),)
	cargo nextest run --workspace
	# The sdk's `schema` feature gates schema_of and its test; no workspace
	# run enables it, so without this line that test had never executed
	# (the 024 zero-second-pass class). `-E 'test(schema_of)'` so an empty
	# selection — a renamed test — fails rather than passing vacuously.
	cargo nextest run -p rdlt-connector-sdk --features schema -E 'test(schema_of)'
	# Same class, `serve` feature (038): OFF by default so a plain
	# workspace run compiles neither serve/ nor its tests. FOUR
	# SEPARATE `-E` invocations, not one combined `or` expression (038
	# T5 review, F6): measured that a combined `test(a) or test(b) or
	# ...` fails CLOSED only when EVERY clause goes empty at once —
	# renaming just one of the four modules still selects the other
	# three and exits 0, silently dropping coverage exactly like the
	# 024 zero-second-pass class. One invocation per module means a
	# rename of ANY of them fails ITS OWN line: `serve::common::tests`
	# and `serve::destination::tests` (each side's own unit tests — the
	# destination one added at 038 T5 review round 1, the
	# `part_close_reason_str` serde-parity pin), then `cases::
	# test_serve_source` and `cases::test_serve_destination` (the
	# tonic-over-UDS integration suites for each half). Same binary
	# each time (already built after the first), so this costs nothing
	# beyond the four short nextest startups.
	cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(serve::common)'
	cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(serve::destination)'
	cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(test_serve_source)'
	cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(test_serve_destination)'
	$(spawn-suite-matrix)
	# The Python proof connector (040): ZERO Rust — the SAME certifier
	# bin, the SAME clause vocabulary, against a pure-Python jsonl
	# source over the real wire. One compound block so the skip guard
	# covers all three steps; skips ONLY when python3 is absent — a
	# broken venv, stub drift, or a failed clause FAILS. The venv is
	# cached under target/py-certify-venv and rebuilt when
	# requirements.txt changes (hash check); the certifier bin was
	# built by the explicit build line above; prepending the venv's
	# bin makes the launcher's `python3` resolve to the pinned deps.
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
	cargo test --doc --workspace
else ifeq ($(TARGET),unit)
	cargo nextest run --workspace
	cargo nextest run -p rdlt-connector-sdk --features schema -E 'test(schema_of)'
	cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(serve::common)'
	cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(serve::destination)'
	cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(test_serve_source)'
	cargo nextest run -p rdlt-connector-sdk --features serve -E 'test(test_serve_destination)'
	$(spawn-suite-matrix)
	# The Python proof-connector certification — the same block as the
	# full gate above (skip ONLY on absent python3; venv cached by
	# requirements hash; stub drift and failed clauses FAIL).
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
	cargo nextest run -p rdlt-connector-postgres --features failpoints -E 'binary(source_crash_sweep) or binary(destination_crash_sweep) or binary(cdc_crash_sweep)'
	cargo nextest run -p rdlt-connector-duckdb --features failpoints -E 'binary(crash_sweep)'
	cargo nextest run -p rdlt-connector-rest --features failpoints -E 'binary(sweep)'
	cargo nextest run -p rdlt-connector-file --features failpoints -E 'binary(crash_sweep)'
	cargo nextest run -p rdlt-connector-iceberg --features failpoints -E 'binary(crash_sweep)'
	# Oracle self-skips without a container runtime; ~15 s fixture boot.
	cargo nextest run -p rdlt-connector-oracle --features failpoints -E 'binary(crash_sweep)'
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
	# The iceberg spark leg died with generation 1 (the second generation's
	# interop consolidation is recorded in the iceberg crate's history; a
	# fresh spark deep-tier cell is owed if the owner re-opens the tier).
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
	# The matrix includes the `-remote` cells, which spawn connector bins the
	# CLI build does not produce — build them here, not as a manual preflight.
	$(MAKE) connector-bins
	sh -c 'E=$$(command -v podman || command -v docker); "$$E" build -q -t rdlt-baseline benches/competitors/dlt/'
	cargo run -q -p rdlt-bench -- run
else ifeq ($(TARGET),matrix)
	$(MAKE) release
	# Same as e2e: the `-remote` cells spawn bins `release` never builds.
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
	# Unconditionally, exactly as `release` above: this arm cannot inspect the
	# filter, and a `-remote` cell named here would otherwise seed its whole
	# fixture (up to 1M rows) before dying at spawn on an absent binary. The
	# build is a sub-second no-op once warm.
	$(MAKE) connector-bins
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

# Live snowflake destination certification (040 T9) — BY HAND only, the same
# discipline as the snowflake crash sweep: it talks to a real account, so no
# check/test block ever invokes it. The config file may hold real credentials;
# this recipe passes its PATH to --config and never echoes its contents.
# Without the file it announces the skip and exits 0, so an uncredentialed
# machine can run it harmlessly. Expected shape with credentials: the
# handshake and P-clauses run against the real service; the read-back
# D-clauses may render Skip (the certifier bin carries no --probe yet).
certify-snowflake:
	@set -e; \
	config="$$HOME/.config/rdlt/snowflake/certify.json"; \
	if [ ! -f "$$config" ]; then \
		echo "SKIP: no snowflake credentials (~/.config/rdlt/snowflake/certify.json)"; \
		exit 0; \
	fi; \
	cargo build -p rdlt-certify --features bin --bin rdlt-certify; \
	cargo build -p rdlt-connector-snowflake --features bin-serve --bin rdlt-connector-snowflake; \
	$${CARGO_TARGET_DIR:-target}/debug/rdlt-certify --role destination --config "$$config" \
		$${CARGO_TARGET_DIR:-target}/debug/rdlt-connector-snowflake

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
