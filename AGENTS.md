# Working on rdlt

Notes for coding agents. The code standard is in `CONTRIBUTING.md` and enforced by `just lint`;
decisions and their reasons are in `docs/adr/`. Read those before changing code.

## Before you push

- Run `just ready`: the pull-request gate plus mutation testing of your change. CI does not run
  mutation testing on pull requests (ADR 0003), so a missed mutant is caught here or not at all.
- Run full mutation passes with `just mutants -j 4`. Mutation builds carry no debug info, so
  four jobs' build directories (about 2 GB each) fit a 16 GB `/tmp`. Do not set
  `CARGO_INCREMENTAL=0`: incremental builds make a run about a third faster.
- `main` is protected: changes land through pull requests, rebase-merged, with every review thread
  resolved. Commit messages follow Conventional Commits with body lines of at most 72 characters
  (`committed` checks them).

## Engine code

- Time, randomness and tasks come only from `Env` and `TaskScope`, so the engine runs under
  deterministic simulation. Clippy bans the direct calls in `rdlt-engine`; do not work around it.
- Tests that involve time run on tokio's paused clock and must fail rather than hang: bound every
  wait.
- A simulation failure prints its seed; replay it with `just sim <seed>`.

## Lints

`cargo xtask lint` checks comments and code beyond clippy: banned words, one-sentence doc
summaries, `biased;` in every `select!`, no `mod.rs`. The workspace's clippy lints require
`#[expect(.., reason = "..")]` instead of `allow`. Run `just lint` rather than guessing what they
accept.
