# Working on rdlt

Notes for coding agents. The code standard is in `CONTRIBUTING.md` and enforced by `just lint`;
decisions and their reasons are in `docs/adr/`. Read those before changing code.

## Before you start

- Check the latest nightly run (`gh run list --workflow nightly.yml --limit 1`). A failing nightly
  is fixed before other work: its gates (the simulation's seeds and coverage, fuzzing, stress and
  mutation testing) run nowhere else.

## Before you push

- Run `just ready`: lint, tests, and mutation testing of your change. CI runs the rest of the
  pull-request gate (coverage, Miri, the simulation, macOS) but not mutation testing (ADR 0003), so
  a missed mutant is caught here or by the nightly pass.
- `just mutants-diff` tests each changed crate's mutants against the packages whose tests can
  catch them; the nightly workflow runs the full pass, every crate's tests against every mutant
  (ADR 0025). Fix a mutant the nightly finds the next day, and when the crate's packages in
  `mutants-diff` missed it, add the package that caught it.
- Test mutants on an otherwise idle machine: a test that times out counts its mutant as caught, so
  load hides survivors. Mutation builds carry no debug info, and four jobs' build directories
  (about 2 GB each) fit a 16 GB `/tmp`; use fewer jobs when memory is short. Do not set
  `CARGO_INCREMENTAL=0`: incremental builds make a run about a third faster.
- `main` is protected: changes land through pull requests, rebase-merged, with every review thread
  resolved. Commit messages follow Conventional Commits with body lines of at most 72 characters
  (`committed` checks them).

## Engine code

- Time, randomness, tasks and the count of cores come only from `Env` and `TaskScope`, so the
  engine runs under deterministic simulation. Clippy bans the direct calls in `rdlt-engine`; do
  not work around it.
- Tests that involve time run on tokio's paused clock and must fail rather than hang: bound every
  wait.
- A simulation failure prints its seed; replay it with `just sim <seed>`.

## Lints

`cargo xtask lint` checks comments and code beyond clippy: banned words, one-sentence doc
summaries, `biased;` in every `select!`, no `mod.rs`. The workspace's clippy lints require
`#[expect(.., reason = "..")]` instead of `allow`. Run `just lint` rather than guessing what they
accept.

`unsafe` code lives in `rdlt-adopt` alone, which holds its two audited files and nothing else
(ADR 0048). Two checks hold that, and neither is enough alone:

- The compiler: every target's root file (a library's, a binary's, an example's, a test's, a
  bench's, a fuzz target's) starts with `#![forbid(unsafe_code)]`, and `cargo xtask lint` fails on
  one that does not. The compiler does not look inside a macro's definition, nor at what another
  crate's macro expands to.
- The lint: `cargo xtask lint` fails on `unsafe` wherever it is a token outside `rdlt-adopt`, a
  `macro_rules!` or `quote!` body included, on `include!`, and on a `#[path]` that is not a `.rs`
  file beneath its crate, so no code comes from a file the lint did not read.
