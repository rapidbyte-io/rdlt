# Quickstart: Workspace Refactoring Program

How to build, verify, and audit this feature's increments. Commands run from
the repo root (inside distrobox: `distrobox enter my-distrobox --`).

## Baseline (once, before increment 1)

```sh
# Coverage baseline — record the number in close-out.md's header (WR7)
make coverage

# Confirm the gate is green on the merge base
cargo nextest run && cargo test --doc
```

## Per-increment gate (every merge)

```sh
cargo nextest run                      # unit + integration (containers skip-not-fail)
cargo test --doc                       # doc-tests
cargo nextest run --features failpoints  # crash-point sweeps (increments 1,5,6,8,10)
make lint                              # clippy + fmt via workspace lints
```

Container-backed suites (postgres, RUSTFS, Polaris) run when a runtime is
present; after increment 4 they all skip through
`rdlt_testkit::containers::runtime_available()` — verify the posture by
running the gate once with the runtime stopped: expect visible skips, zero
panics (WR5, D2).

## Sweep commands (WR2 — run after increment 2 and at close-out)

```sh
# Citation IDs in user-facing strings: expect zero hits in error/warning
# construction sites (test code excluded)
grep -rn --include='*.rs' -E '\(contract [A-Z]+[0-9]+\)|review finding|specs/[0-9]{3}-' crates/*/src

# Former duplicate definitions gone (WR3 spot checks)
grep -rn --include='*.rs' 'struct Secret' crates | grep -v rdlt-connector/src
grep -rn 'fn start_pg' crates/*/tests
```

## Red-first evidence (WR6, per B-item)

```sh
# Order the commits test-before-fix, then capture the red run:
git stash        # stash the fix, keep the test
cargo nextest run -E 'test(<regression_test_name>)'   # expect FAIL — capture output
git stash pop    # restore the fix
cargo nextest run -E 'test(<regression_test_name>)'   # expect PASS
```

Paste the failing output reference into the item's close-out row.

## Close-out audit (last increment)

```sh
# Matrix completeness: every row has a disposition and evidence (WR7)
grep -c '| B\|| R\|| D' specs/017-workspace-refactoring/close-out.md   # row count vs catalogue
grep -n '|\s*|' specs/017-workspace-refactoring/close-out.md            # empty cells: expect none

# Coverage vs baseline
make coverage

# Perf gate + bench tooling neutrality (WR8)
make bench TARGET=gate
```
