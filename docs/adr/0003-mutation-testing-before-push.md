# ADR 0003: Mutation testing runs before pushing until production

Status: accepted, 2026-09-23; amended 2026-09-25 (faster local runs).

## Context

The spec runs `cargo mutants --in-diff` on every pull request and requires zero surviving mutants.
The job's cost grows with the size of the change: the M1 pull request tested 533 mutants one at a
time and took more than 20 minutes, while every other check finished in under two. Until rdlt is
production ready, the speed of development matters more than a second, redundant run of the same
check on a CI runner.

## Decision

- Contributors run `just ready` before pushing. It runs the pull-request gate (`just ci`) and
  `just mutants-diff`, which tests the mutants in the branch's changes against `origin/main`,
  uncommitted changes included. The zero-surviving-mutants rule is unchanged.
- The pull-request workflow has no `mutants` job, and `mutants` is not a required check on `main`.
- The weekly workflow still runs the full mutation suite, so a mutant that slips through a local
  run is found within a week.
- Mutants build with the `mutants` cargo profile: no debug info, incremental builds, and four
  jobs in `just mutants-diff`. On M3e's 313 mutants that took a run from 27m48s (two jobs of
  debug builds, incremental off) to 8m35s, catching the same 271: debug info cost a quarter of
  the time, two more jobs a third of the rest, and incremental builds a third of that.

## Consequences

- Nothing enforces the local run; a survivor can reach `main` and stay there until the weekly run.
- Restoring the gate when rdlt is production ready takes two steps:
  1. Add the job back to `.github/workflows/ci.yml`:

     ```yaml
     mutants:
       if: github.event_name == 'pull_request'
       runs-on: ubuntu-24.04
       timeout-minutes: 45
       steps:
         - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
           with:
             fetch-depth: 0
         - uses: jdx/mise-action@c2a87611a18de5b3828c5652fe268e992400cb5c # v4.3.0
           timeout-minutes: 5
           with:
             install_args: just cargo-binstall cargo:cargo-mutants cargo:cargo-nextest
             cache_key_prefix: mise-v1-mutants
         - uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6 # v2.9.2
           with:
             save-if: false
         - run: git diff "origin/${BASE_REF}...HEAD" > pr.diff
           env:
             BASE_REF: ${{ github.base_ref }}
         - run: just mutants --in-diff pr.diff
     ```

     For large changes, split it across a matrix with `--shard k/n`.
  2. Add `mutants` back to the required status checks of the `main` ruleset.
