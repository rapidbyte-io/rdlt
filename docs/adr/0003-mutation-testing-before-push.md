# ADR 0003: Mutation testing runs before pushing until production

Status: accepted, 2026-09-23.

## Context

The spec runs `cargo mutants --in-diff` on every pull request and requires zero surviving
mutants. The job's cost grows with the size of the change, while every other check finishes in
minutes. Until rdlt is production ready, the speed of development matters more than a second,
redundant run of the same check on a CI runner.

## Decision

- Contributors run `just ready` before pushing: lint, tests and `just mutants-diff`, which tests
  the mutants in the branch's changes against `origin/main`, uncommitted changes included. No
  mutant may survive.
- `just mutants-diff` tests each changed crate's mutants against the packages whose tests can
  catch them: the crate's own and those that drive it, as the recipe's `catching` lists them.
- The pull-request workflow has no `mutants` job, and `mutants` is not a required check on
  `main`. CI's pull-request gate runs the rest: coverage, Miri, the simulation, macOS, and the
  instruction and allocation counts against `main`.
- The nightly workflow runs the full pass: every crate's mutants against the tests of every
  package that links the crate (`just mutants <crate>`), the crate's own first, through a
  nextest profile of its own, where most mutants are caught. The whole pass is some sixty
  runner-hours, so each crate's mutants are cut into shards that finish within their limit; the
  large crates' shards are taken a quarter a night and the small crates run whole every night,
  so a missed mutant is found within four nights. A mutant it finds is fixed the next day, and
  one a crate's `catching` packages missed adds the package that caught it.
- Mutants build with the `mutants` cargo profile: no debug info, dependencies optimised, and
  incremental builds, which make rebuilding per mutant cheap. `just mutants-diff` runs four jobs
  by default; the nightly runs two mutants at once on two test threads each, linked by mold.
- A test's limit in the `mutants` nextest profile sits far enough above what it takes that only
  a test hung on its mutant reaches it: twenty seconds for any test, a minute or more for the
  reference connectors' tests that sync to disk and the others the profile names. A mutant a
  loaded machine counts as caught by a test timing out proves nothing, so mutation results are
  taken from an otherwise idle machine.
- Rejected: a RAM disk for mutation runs, which the development machine's memory cannot spare.

## Consequences

- Nothing enforces the local run; a survivor can reach `main` until the nightly pass finds it.
- Restoring the gate when rdlt is production ready takes two steps: a pull-request job that
  installs cargo-mutants and nextest from the lockfile (ADR 0048) and runs
  `just mutants-diff "origin/${BASE_REF}"`, and `mutants` added to the required status checks of
  the `main` ruleset.
