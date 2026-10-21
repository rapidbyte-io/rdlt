# Rust style

How Rust is written in rdlt. This adopts [Google's Rust style
guide](https://google.github.io/styleguide/rustguide.html), adapted to this codebase: an async
engine on tokio that runs under deterministic simulation, takes untrusted input from connectors,
and keeps `unsafe` in one audited crate. Where this document is silent, follow the official [Rust
Style Guide](https://doc.rust-lang.org/style-guide/) and the [Rust API
Guidelines](https://rust-lang.github.io/api-guidelines/).

`CONTRIBUTING.md` holds the rest of the code standard: vocabulary, comments, structure,
constructor names, errors, async and determinism, API design and tests. This document points at
those sections rather than repeating them. Where Google's guide and rdlt differ, the rdlt rule is
stated here and wins.

Most rules are enforced by a tool, named beside the rule: rustfmt, clippy (the workspace enables
`pedantic`, and `just lint` denies warnings) or `cargo xtask lint`. Reviewers hold the rest.

## Library and application code

Google's API rules bind library code and guide application code. The library crates are
`rdlt-connector`, `rdlt-engine`, `rdlt-host`, `rdlt-wire`, `rdlt-log-store`, `rdlt-adopt` and
`rdlt-connector-macros`: connector authors and embedders build on their public items. The
application and tooling crates are `rdlt-certify`, `rdlt-connector-reference`, `rdlt-sim`,
`rdlt-testkit`, `xtask` and `fuzz`, which favor simplicity over generality.

rdlt keeps no compatibility promise: a public API changes when its design does, and its callers in
the workspace change with it. Google's rules whose reason is compatibility (`const fn` held
forever, `#[non_exhaustive]` everywhere) are adapted below.

## Code organization

- **Modules.** `foo.rs` with a `foo/` directory, never `mod.rs`, one concept per module
  (CONTRIBUTING.md § Structure; `cargo xtask lint` rejects `mod.rs`).
- **A parent module owns its concept.** Google keeps non-leaf modules to declarations and
  re-exports; in rdlt a parent such as `table.rs` holds the concept's own types and the code that
  ties its parts together, keeps its parts in private submodules, and re-exports what callers use
  (`pub(crate) use model::{Creation, Model};`), so callers write `table::Model`.
- **File names.** A module is named after its primary item or concept in snake case
  (`scope.rs` for `TaskScope`).
- **Crates.** The crate graph follows the dependency rule `cargo xtask deps` checks; a crate is not
  split for size alone.
- **No glob imports**, with these exceptions:
  - `use proptest::prelude::*;` in property tests.
  - `use rdlt_connector::prelude::*;` in connectors. It is the one prelude rdlt defines, for
    connector authors; define no other.
  - `use super::*;` in an inline `mod` inside a test file.
  - Generated code (`rdlt-wire/src/generated`).
- **One module per `use`.** Braces hold items of one module: `use std::sync::{Arc, LazyLock};`,
  never `use crate::{table::Model, error::Error};`. rustfmt's `imports_granularity = "Module"` is
  unstable, so reviewers hold it.
- **Import groups.** `std`, then other crates (workspace crates included), then `crate`/`super`,
  separated by blank lines, each sorted as rustfmt sorts it.
- **Qualify free functions with their module.** `mem::take`, `fs::read`, `ptr::eq`, after
  `use std::{fs, mem, ptr};`, never a bare imported `take`. Exceptions: a crate's own functions a
  parent module re-exports; a function whose module repeats its name (`arrow_select::take::take`);
  and `pin!`, `ready!` and `poll_fn` in hand-written futures.
- **Import types and traits, and use them unqualified** (`Arc`, `RecordBatch`). Qualify a type
  whose name collides in the file (`tokio::sync::Mutex` beside `parking_lot::Mutex`, `io::Error`),
  or rename it on import (`use std::fmt::Write as _;` for a trait wanted only for its methods).
- **Traits used in one function** may be imported inside that function.

## Formatting

- `cargo fmt` with `rustfmt.toml` (style edition 2024, 100 columns); `taplo fmt` for TOML. CI
  checks both.
- `#[rustfmt::skip]` only for manual alignment that carries meaning, such as a table of seeds and
  what each one found. Never for a personal preference. Generated code is not skipped:
  `cargo xtask codegen` formats what it writes.
- Hex digits are lowercase (`0x9e37_79b9`), long literals grouped in fours with `_`. Invisible
  characters in literals are written as escapes (`'\u{a0}'`, `"\u{200d}"`). rustfmt's
  `hex_literal_case` is unstable, so reviewers hold it.

## Ownership and borrowing

- **Unique ownership and borrows first.** Within a task or a function, values have one owner and
  are lent.
- **`Arc` for what tasks share.** Spawned tasks and the compute pool need `'static` data, and
  Arrow's own types (`ArrayRef`, `SchemaRef`, buffers) are reference counted, so `Arc` is the
  normal way to share across tasks. Google's "avoid `Rc` and `Arc`" applies within one task's
  data. Clone a shared pointer with `Arc::clone(&x)`, so it reads differently from a deep copy.
- **Borrow before cloning.** `.clone()` is for a copy the code needs. Cloning an Arrow array or a
  `Bytes` copies a handle, not the data; cloning a `Vec`, `String` or `BTreeMap` copies it all.
  The shredder's, the lowering's and the write-ahead log's hot paths are measured by
  `just instructions`, which fails a change that adds more than 2% instructions or allocations
  unless its commit names the case in an `Instructions-Accepted` trailer (AGENTS.md).
- **Byte payloads are `Bytes`.** Pushes, cursors and state that cross tasks are sliced, not
  copied.

## Function inputs and outputs

- **Take the most general borrow:** `&str`, `&[T]`, `&T` and `Option<&T>`, not `&String`,
  `&Vec<T>`, `&Box<T>` or `&Option<T>` (clippy `ptr_arg`, `borrowed_box`, `ref_option`).
- **Take ownership when the function keeps the value**, rather than borrowing and cloning inside
  (clippy `needless_pass_by_value` flags the reverse).
- **Iterators or collections.** A library function that only iterates its input takes
  `impl IntoIterator`; one whose result callers will mostly iterate returns `impl Iterator`.
  Google's exceptions hold: return a `Vec` when the function builds one anyway, when callers index
  it, or when the result is the point. Application crates take slices and return `Vec`s where that
  is simpler.
- **`impl Into<T>`** only in library constructors whose callers hold the value in several forms,
  such as error messages and names taking `impl Into<String>`. Nowhere else.
- **Enums, not booleans** (CONTRIBUTING.md § Naming).
- **`Option`, `Result` or a panic.** `Option` when absence explains itself; `Result` when the
  caller needs to know why (CONTRIBUTING.md § Errors); a panic only for a bug in rdlt. Anything a
  connector, the wire, a file or an object store hands rdlt is untrusted: malformed or hostile
  input is an error, never a panic (ADRs 0037 and 0050).

## Types

- **Newtypes over aliases** when a value has meaning of its own, an API of its own, or could be
  confused with another of the same representation: identifiers, positions, epochs, sizes
  (CONTRIBUTING.md § Naming). A `type` alias only shortens a long type.
- **Named fields over tuples**, except a newtype's one field and a tuple used within a few lines.
- **Valid at construction** (CONTRIBUTING.md § API design).
- **Checked conversions.** Convert integers with `From` and `TryFrom`, not `as` (clippy's
  pedantic `cast_*` lints). An `as` cast that cannot lose a value carries an `#[expect]` saying
  why.

## Traits

- **Derive what can be derived.** A manual impl is for behavior a derive cannot express, such as a
  redacting `Debug` on a type holding secrets (CONTRIBUTING.md § API design).
- **Implement the standard trait** rather than a method of the same purpose: `FromStr`, not
  `fn from_str`; `Default`, not a `new()` without arguments (clippy `should_implement_trait`,
  `new_without_default`).
- **Generics or `dyn`.** Per-row and per-value code is concrete or generic, so it inlines. A
  boundary that holds implementations chosen at run time is `dyn`: connectors and their sessions,
  log stores, `Env`. These are called once per batch or per operation, and `dyn` keeps the engine
  compiled once rather than once per connector.
- **A trait needs a reason:** generic code, a `dyn` boundary (a simulation or test double counts),
  or an extension trait. Don't define a trait to organize methods; use an inherent `impl`.
- **Extension traits** end in `Ext` (`ResultExt`) and live in the crate that owns the extended type
  or its domain.
- **Error kinds are `Copy`.** Error types carry their causes and are not `Copy`; their `kind()`
  returns a `Copy` enum (CONTRIBUTING.md § Errors).
- **`Default`** for a struct of options whose all-default value is valid. A type that must be
  configured to be valid has no `Default`.
- **Equality and order.** Derive `PartialEq` for data types (CONTRIBUTING.md § API design), `Eq`,
  `Hash` and `Ord` when the type is a map key or sorted. Map keys in `rdlt-engine` and
  `rdlt-testkit` go in `BTreeMap`, since a hash map's order breaks deterministic simulation
  (clippy `disallowed-types` there).

## Concurrency

- **Blocking locks are `parking_lot`'s** (`Mutex`, `RwLock`, `Condvar`). They do not poison, so no
  code reaches past a poisoned lock and no code handles `PoisonError`. `std::sync`'s `Mutex`,
  `RwLock` and `Condvar` are not used. Google's Fuchsia rule (`fuchsia_sync::Mutex`) has this
  form in rdlt.
- **A blocking lock is never held across `.await`** (clippy `await_holding_lock`). A lock a task
  must hold across `.await`, such as a destination session's, is `tokio::sync::Mutex`.
- **A guard stays inside its type.** A method takes the lock, does the work and releases it;
  public APIs do not return guards.
- **Async is the default for I/O**, on tokio, with the rules of CONTRIBUTING.md § Async,
  concurrency and determinism: every task owned, `biased;` in every `select!`, time and randomness
  from `Env` inside `rdlt-engine`.
- **Threads.** Inside `rdlt-engine`, CPU work runs on `Env::compute` and no thread is started
  (clippy `disallowed-methods`). Elsewhere a thread is started only for a blocking wait that has
  no async form, through `std::thread::Builder` with a name, and is owned and joined.
- **`catch_unwind` only contains a panic at a boundary**, turning it into an error: a task in a
  `TaskScope`, a job on the compute pool, Arrow's readers decoding a hostile frame, a simulation
  seed. A crate that catches a panic refuses to compile with `panic = "abort"` (`cargo xtask lint`
  checks its root). A caught panic is still a bug and fails the run, call or seed it happened in;
  nothing relies on it for correctness.

## Macros

Prefer, in order: functions, types and traits; `macro_rules!`; code generated by `cargo xtask
codegen`; procedural macros. rdlt has one procedural-macro crate, `rdlt-connector-macros`, for
the `#[source]` and `#[destination]` attributes connector authors use. A new macro must remove
more repetition than it costs in tooling and readability. No macro body contains `unsafe`
(`cargo xtask lint`).

## Naming

- **Conversions:** `as_` (free, borrowed), `to_` (costly or owned), `into_` (consumes `self`)
  (clippy `wrong_self_convention`). Constructors are `new`, `try_new`, `parse`, `with_*` and
  `from_*` (CONTRIBUTING.md § Naming). `with_*` is a builder setter in rdlt, never Google's
  closure accessor.
- **Accessors:** `foo()`, `foo_mut()`, `set_foo()`, `take_foo()` and `replace_foo()`. No `get_`
  prefix, except to implement a foreign trait that has one.
- **`len` comes with `is_empty`** (clippy `len_without_is_empty`).
- **Lifetimes** are `'a`, `'b` when they relate a function's inputs to its output or a type to
  what it borrows. A type with more than two lifetimes names them for what they borrow.
- **Generic parameters** are short and taken from their main bound: `S: SourceConnector`,
  `F: Future`. `T` and `U` are for unconstrained types.
- **No abbreviations** that a reader outside rdlt would not know, and none made by dropping
  letters: `index`, not `idx`; `config`, not `cfg`; `message`, not `msg`. Common abbreviations
  stay (`id`, `io`, `tls`, `url`, `sql`, `json`). Domain words come from CONTRIBUTING.md
  § Vocabulary.

## Documentation

CONTRIBUTING.md § Comments rules all comments: no comment by default, one-sentence doc summaries,
no history. In addition:

- Every public item is documented, enum variants included (rustc `missing_docs`, stricter than
  Google's guide).
- A function that can panic says when under `# Panics` (clippy `missing_panics_doc`); an `unsafe`
  function states its contract under `# Safety`.
- Entry-point types have a runnable example; examples elsewhere are optional.
- `///` for items, `//!` for modules and crates, Markdown, intra-doc links (rustdoc denies broken
  ones). Write documentation directly, not from macros.

## Other language features

- **`unsafe`** lives in `rdlt-adopt` alone, every block under a `// SAFETY:` comment (clippy
  `undocumented_unsafe_blocks`), and every other target root forbids it (AGENTS.md § Lints,
  ADR 0048).
- **Stable Rust only.** The toolchain is fixed in `rust-toolchain.toml`; no `#![feature]`.
- **`#[non_exhaustive]`** on error types and their kinds (CONTRIBUTING.md § Errors), and on a
  public enum or struct with public fields that connector authors or embedders match on or build
  and that will grow. A closed set (a three-way policy, an ontology) is exhaustive. Within the
  workspace, match every variant rather than a wildcard where the code should change when a
  variant is added.
- **`const fn`** wherever it can be; there is no compatibility promise to keep.
- **Operators** are overloaded only for their mathematical or logical meaning.
- **`#[must_use]`** on functions whose result is the point of the call and on types that do
  nothing unless used: guards, builders (clippy `return_self_not_must_use`), lazily evaluated
  values. Clippy's `must_use_candidate` is off as too broad; reviewers judge the rest.
- **`#[doc(hidden)]`** items of other crates are private: don't use them. rdlt defines none
  (CONTRIBUTING.md § API design).
- **Nothing runs before `main`:** no `ctor`, `inventory` or `linkme`. Process-wide state is a
  `static` built on first use (`LazyLock`, `OnceLock`).

## Implementation

- **Annotate types a reader cannot infer**, on the binding rather than as a turbofish
  (`let rows: Vec<Row> = ….collect();`). A turbofish is fine mid-chain, with `_` for what is
  obvious.
- **Match instead of check-then-unwrap:** `if let Some(x) = …`, `let … else`, not `is_some()`
  followed by `unwrap()` (clippy `unnecessary_unwrap`).
- **`expect` states the invariant; no `unwrap` outside tests** (CONTRIBUTING.md § Errors, clippy
  `unwrap_used`).
- **Assertions say why.** Give `assert!` a message when its condition does not explain itself,
  saying whether a failure is the caller's misuse or a bug here, with the values that broke it.
  `debug_assert!` is for checks too costly for release builds: the simulation profile keeps debug
  assertions and overflow checks on, so it trips them.
- **Loops over long chains.** Short iterator chains are fine. A chain with branches or side
  effects becomes a `for` loop; a chain in a `for` header is bound to a named variable first; no
  `for_each` (clippy `needless_for_each`).
- **`format!` and `write!`** to build strings, not `+` chains or `str::replace` on templates.

## Tests

Google's rule is to use the codebase's framework. In rdlt that is `#[test]` and
`#[tokio::test(start_paused = true)]` for anything that waits, `proptest` for law-like code,
`rdlt_sim::for_each_seed` for simulation, `rdlt_testkit::cancel::at_every_point` for cancellation,
and `insta` for output a person reads, all run under nextest (CONTRIBUTING.md § Tests). Tests may
`unwrap` and print (`clippy.toml`).
