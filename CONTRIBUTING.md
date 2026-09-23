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
- Run `just ready` before pushing: the pull-request gate plus mutation testing of your change,
  which CI does not repeat.

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
an ADR under `docs/adr/`. ADRs are short and immutable: a changed decision gets a new ADR.

## Structure

- Modules use `foo.rs` plus a `foo/` directory, never `mod.rs`.
- One concept per module. Aim for at most 400 production lines per file and 60 lines per
  function. A function that needs many arguments needs a struct.
- File order: module doc, `use` declarations, public types, their impls, private helpers, then
  `#[cfg(test)] mod tests;`.
- Unit tests live in a sibling `tests.rs` (`foo/tests.rs` for `foo.rs`). Each crate has one
  integration test binary, `tests/it/main.rs`, with `tests/it/<area>.rs`.
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

- Every task has an owner: spawn through a `TaskScope`, never `tokio::spawn`. Dropping any future
  cleans up everything it started.
- Nothing blocks the async runtime. CPU-heavy work runs on `Env::compute`; blocking file I/O runs
  on a dedicated task that owns the file.
- Inside `rdlt-engine`, time, randomness and scheduling come only from `Env`. Clippy bans the
  direct calls, and iteration order must never depend on a hash map's random state.
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
- Simulation tests use `rdlt_sim::seeds`; a failing seed replays with `just sim <seed>`.

## Definition of done

A change is done when:

1. `just ready` passes, with no lint allowance lacking a reason (use `#[expect(.., reason = "..")]`).
2. Every public item is documented and no comment breaks the rules above.
3. Its tests are behavioral, cover every defect it fixes, and include property tests where the
   code is law-like.
4. It has no detached tasks, no blocking on the runtime and no string errors.
5. It uses the vocabulary above.
6. A reviewer who has not seen it can explain what it does from its docs alone.
