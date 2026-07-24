# Quickstart: Benchmark Refinement — Three-Way E2E Matrix

Commands from the repo root (inside the distrobox). Measurement phases
need a quiet machine + podman socket; P0/P1 need neither.

## P0 verification (no measurements)

```sh
# Gate
cargo nextest run && cargo test --doc && make lint

# BR1 vocabulary sweep — expect ZERO hits (harness code, cells, docs)
grep -rn -iE '"(gated|scoreboard)"|class *=|suite *=|Mode::Library|Mode::Hyperfine' \
  crates/rdlt-bench/src benches/cells benches/RESULTS.md

# Amend-then-delete order (BR2): constitution v1.1.0 in the log BEFORE the migration commit
git log --oneline -- .specify/memory/constitution.md benches/cells | head

# Instruments track still guards embeddability (FR-006)
benches/check-cold-start.sh   # exits non-zero above 40 ms

# Report regenerates from the new shape, diff-clean
cargo run -q -p rdlt-bench -- report && git diff --stat benches/RESULTS.md
```

## P1 (spike only)

```sh
# Probe order = risk order; each writes evidence into specs/018-bench-refinement/spike/
# 1 runtime: KIND_EXPERIMENTAL_PROVIDER=podman abctl local install --low-resource-mode
#    (docker fallback requires recorded owner approval BEFORE any install)
# 2 networking: from a kind pod, curl host postgres + RUSTFS fixture ports
# 3 api: create throwaway connection, sync, GET /v1/jobs/{id} — pin fields
# 4 quiet guard: idle loadavg with cluster up vs guard threshold
# 5 reset: sync → reset+drop → row counts equal initial
```

## P2 (first recorded session, rdlt vs dlt)

```sh
make release
podman build -q -t rdlt-baseline benches/competitors/dlt/
cargo run -q -p rdlt-bench -- run          # 5 cells × (rdlt + dlt [+pyarrow context])
cargo run -q -p rdlt-bench -- report       # matrix renders; Trends gains lines
# BR4: every arm's verification = rowcount PASS in the run summary
```

## P3 (first 3-way session)

```sh
python3 benches/competitors/airbyte/setup.py   # idempotent; ids → gitignored state.json
cargo run -q -p rdlt-bench -- run              # Airbyte arms runs=3; absent ⇒ Missing{reason}
cargo run -q -p rdlt-bench -- report
```

## P4 (bars, measurement-first)

```sh
# After the recorded 3-way session: add ≤1 bar per cell to bars.toml, each
# below the session floor, each with a policy entry in RESULTS.md; then
cargo run -q -p rdlt-bench -- gate   # green against the SAME session
```

## Close-out audit

```sh
# BR1 sweep again; BR8: every bar cites a policy entry + existing cell
grep -c "policy" benches/bars.toml
# Milestones cite the archive commit for every retired claim
grep -n "pre-migration\|$(git log --format=%h -1 main -- benches/cells 2>/dev/null | head -1)" benches/RESULTS.md | head
```
