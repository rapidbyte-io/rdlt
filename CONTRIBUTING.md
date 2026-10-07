# Contributing to rdlt

This is the code standard for every line in this repository. Tooling enforces most of it
(`just lint`); reviewers enforce the rest. Where this document and the code disagree, this document
wins and the code is fixed.

## Workflow

- Work on a branch and open a pull request; `main` accepts changes only through pull requests
  whose checks pass.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org) with a crate scope:
  `fix(engine): release reservations on lane errors`. Subjects are imperative, lowercase, at most
  72 characters. Details go in the body.
- Keep commits small and focused. Every fixed defect gets a regression test in the same change.
- Run `just ready` before pushing: lint, tests, and mutation testing of your change, which CI does
  not repeat; CI runs the rest of the pull-request gate.

## Tools

`mise.toml` names every tool and its version; `mise.lock` holds, for Linux x64 and macOS arm64,
the release archive each installs and its checksum. `mise install --locked` installs only what the
lockfile holds, and fails on an archive whose checksum differs. CI installs the same way, with a
mise binary held to the version and checksum in the workflows' `SETUP_MISE_*` variables.

To add or update a tool:

1. Edit its version in `mise.toml`. Use a backend that installs a release archive (`aqua:`,
   `github:`, or a short name from mise's registry): `cargo:` resolves its download at install
   time, so nothing can be locked.
2. Run `mise lock --platform linux-x64,macos-arm64`.
3. Where a release publishes no digest, `mise lock` records none. Download the archive at the
   locked `url`, and add `checksum = "sha256:<sha256sum of it>"` to that platform's table.
4. Run `cargo xtask tools`, which fails on a tool with no download or checksum for a platform,
   then `mise install --locked`, and commit both files.

To update mise in CI, set `SETUP_MISE_VERSION` in both workflows, which Renovate proposes, and
the two checksums to those of the `mise-v<version>-linux-x64` and `mise-v<version>-macos-arm64`
binaries in the release's `SHASUMS256.txt`.

Actions are pinned to commit SHAs with their version in a comment; `just pins`, part of
`just lint`, checks with `pinact` that each SHA is the commit of the version beside it. A GitHub
token in `GITHUB_TOKEN` keeps it within the API's rate limit.

Renovate proposes each update seven days after its release. It does not update transitive
crates: run `cargo update` for those, and when the nightly advisory check names one. A lockfile
behind its manifests fails `just lint`, so commit the lockfile cargo resolves with the change.

## Vocabulary

Use only these words for these concepts, in code, docs, logs and errors.

| Term | Meaning |
|---|---|
| pipeline | A named configuration of one source, one destination, selected streams and their policies |
| run / attempt | One `start()` of a pipeline / one try within it; each retry is a new attempt |
| load | The work of one attempt, identified by a `LoadId` |
| stream | A source-side named sequence of records |
| partition | A source-defined slice of a stream; the unit of parallel reading and of cursor state |
| push | One delivery from a source into the engine: an Arrow batch, JSON bytes or a change batch |
| batch | An Arrow `RecordBatch` |
| checkpoint | A source-emitted cursor marking a resumable position in a partition |
| segment | A partition's data between two consecutive checkpoints; the unit of publication |
| barrier | An engine request asking partitions to checkpoint at their next safe point |
| commit | The atomic publication of sealed segments with the state they imply |
| receipt | The destination's acknowledgment of a commit |
| epoch | The fencing token incremented at every destination open |
| cursor / state | A partition's resume position / the pipeline's committed cursors, schemas, name maps and epoch |
| staging | Written but unpublished destination data |
| table | A destination-side relation |
| lane | One concurrent destination writer inside the engine |
| limit | A numeric bound (not "ceiling", "cap" or "gate") |
| budget | Only the byte-denominated memory backpressure mechanism |

`cargo xtask lint` rejects a list of banned words in comments; the list is in
`xtask/src/rules.rs`.

## Comments

The default is no comment. Names, types and structure carry the meaning. Write a comment only for:

1. A contract the type system cannot express ("re-committing the same key returns the stored
   receipt").
2. An ordering or durability invariant ("fsync the log before the destination commit").
3. A non-obvious reason, in one line.
4. A workaround for an upstream bug, with a link to the issue.
5. `// SAFETY:` on any `unsafe` block.
6. The unit and meaning of a public limit.

Never write history (what used to be, what was tried), tracker or spec references, measurements,
restatements of the code, emphasis (upper-case words, "deliberately", "honestly"), commented-out
code, or `TODO` without an issue number (`TODO(#123)`).

A doc comment's first paragraph is one sentence. Doc comments are usually one to five lines, plus
an example on entry-point types. `//` comments are at most three lines; longer reasoning belongs in
an ADR under `docs/adr/`.

An ADR states a decision in force and why, in the present tense. When the decision changes, its
ADR is rewritten to state the new decision, with no note that it changed and no account of what
it replaced; a new ADR is written for a new decision, never for a change to an old one.

## Structure

- Modules use `foo.rs` plus a `foo/` directory, never `mod.rs`.
- One concept per module. Aim for at most 400 production lines per file and 60 lines per
  function. A function that needs many arguments needs a struct.
- File order: module doc, `use` declarations, public types, their impls, private helpers, then
  `#[cfg(test)] mod tests;`.
- Unit tests live in a sibling `tests.rs` (`foo/tests.rs` for `foo.rs`). A crate's integration
  tests are one binary, `tests/it/main.rs`, with `tests/it/<area>.rs`, but where they need what
  the rest must not have: `rdlt-engine`'s `crashes` (the `failpoints` feature) is a binary of
  its own, and `rdlt-log-store`'s integration tests are `containers` (Docker). `rdlt-wire`'s
  one binary is `tests/decoded.rs`.
- Crate roots hold a crate doc with one example, `mod` declarations and an explicit `pub use`
  list. No glob re-exports.
- Every numeric limit a crate enforces lives in that crate's `limits.rs`.

## Naming

Constructors are `new` (infallible), `try_new` or `parse` (fallible), `with_*` (builder setters)
and `from_*` (conversions). Accessors have no `get_` prefix. Types read well out of context
(`RemoteSource`, not `remote::Source`). APIs take enums, not booleans. Identifiers are newtypes,
and durations are `Duration`, never integer milliseconds.

## Errors

- One error type per crate boundary, derived with `thiserror`, marked `#[non_exhaustive]`, with a
  `kind()` accessor.
- Keep causes as `#[source]`; never flatten them with `to_string()`.
- No `Result<_, String>` or `Result<_, &'static str>` outside tests.
- Messages are lowercase, one line, without a trailing period, and name their subject:
  `stream "orders": cursor exceeds the 4 MiB limit`.
- `expect` is for invariants only, and its message states the invariant. No `unwrap` outside
  tests. Panics are for bugs, and public functions document them under `# Panics`.

## Async, concurrency and determinism

- Inside `rdlt-engine` every task has an owner: spawn through a `TaskScope`, never
  `tokio::spawn`, and dropping any future cleans up everything it started. Clippy bans the
  direct calls there.
- Nothing blocks the async runtime. CPU-heavy work runs on `Env::compute`; blocking file I/O runs
  on a dedicated task that owns the file.
- Inside `rdlt-engine`, time, randomness, scheduling and the count of cores come only from `Env`.
  Clippy bans the direct calls, and iteration order must never depend on a hash map's random state.
- Every `select!` starts with `biased;` and documents which branch wins.
- Timeouts are per operation and configurable. Locks come from `parking_lot` and are never held
  across `.await`.

## API design

- Validate at construction: a value that exists is valid.
- Items are `pub(crate)` unless they are part of the public API. Test and benchmark seams live
  behind features, not `#[doc(hidden)] pub`.
- Public data types derive `Debug`, `Clone` and `PartialEq`; wire and state types also derive
  `Serialize` and `Deserialize`. Types holding secrets implement a redacting `Debug`.
- Every public item is documented, and entry-point types have a runnable example.

## Tests

- Test behavior, never wording: assert on error kinds, codes and data, not message text.
  Snapshot-test only rendered user output.
- Test names state the property: `resumed_run_does_not_duplicate_committed_rows`.
- One test per behavior; inputs that differ only in data go in a table-driven test.
- Law-like code (lattices, codecs, naming) gets property tests. Anything persisted or evolved is
  tested across at least two runs.
- Simulation tests run their seeds through `rdlt_sim::for_each_seed`, which runs them side by side; a sweep takes its seeds from `rdlt_sim::seeds`. They run under nextest, one process a test. A failing seed replays with `just sim <seed>`, a failing stress seed reruns, not exactly, with `RDLT_SIM_SEED=<seed> just stress`, and `just sim "" <count> <first>` runs `<count>` seeds of each sweep from `<first>`, as each shard does; the fixed-seed and replay tests run in `just test` alone.
- Snapshot tests (`insta`) hold output a person reads, such as `rdlt-certify`'s help and reports. When it changes on purpose, record it again with `INSTA_UPDATE=always`, and review the snapshots' diff before committing.

## Definition of done

A change is done when:

1. `just ready` passes, with no lint allowance lacking a reason (use `#[expect(.., reason = "..")]`).
2. Every public item is documented and no comment breaks the rules above.
3. Its tests are behavioral, cover every defect it fixes, and include property tests where the
   code is law-like.
4. It has no detached tasks, no blocking on the runtime and no string errors.
5. It uses the vocabulary above.
6. A reviewer who has not seen it can explain what it does from its docs alone.
