# ADR 0052: Formal verification as a layer

Status: accepted, 2026-10-18.

## Context

The simulation, the mutation tests and fuzzing find a defect by running into it: a seed that
interleaves a cancellation with a send, a change no test catches, an input the fuzzer reaches.
Some properties are stated over every input or every interleaving, and a run meets a sample of
them: the credit a sender may spend, a budget's arithmetic, a log on object storage whose creates
land late. A bounded model checker proves a property of the code for every input up to a bound;
a specification checked by a model checker proves a protocol's invariant over every interleaving
up to a bound. Neither runs the program: each holds only what it is given, the code's arithmetic
or the protocol's design.

## Decision

### Which layer catches which class of defect

| Class | Caught by |
|---|---|
| Cancellation safety: state changed before an `.await` | a harness cancelling at every await point; the simulation's leak oracle |
| Check-then-act, or state initialised in parts | the simulation's hostile peers; Kani where the state is pure |
| Resources held but not charged | the simulation's real-memory and bounds oracles; property tests of the budget's ledger |
| A wait with no deadline | the simulation's hung and stalled faults |
| Redaction out of order or bypassed | the simulation's secret canary; a property test of `Redactions::scrubbed` |
| Trust in the file system | regression tests; the simulation, where process placement enters it |
| Conversion fidelity | the simulation's encodings; differential tests |
| A contract's documentation, fixtures, or an API's precondition | Kani over the limits a handshake accepts; tests |
| Arithmetic of credit, budgets, positions and frame lengths | Kani; property tests where the code takes a lock (below) |
| A protocol across processes and a store: the log on object storage, exactly-once, re-planning | Quint |

A formal layer is added to the layers of its class, never in place of one.

### Proofs: Kani over the code

- A module's proofs live in `src/<module>/proofs.rs`, declared `#[cfg(kani)] mod proofs;` beside
  its tests. `kani` is a configuration only Kani sets; the workspace's `unexpected_cfgs` lint
  names it in its `check-cfg`, so every other build compiles none of it and warns of none of it.
  Kani provides the `kani` crate itself: a crate adds nothing to its manifest.
- A proof is a `#[kani::proof]` function, named as a test is, over `kani::any()` inputs narrowed
  by `kani::assume`, and states its bound in a paragraph of its doc comment that begins `Bound:`:
  the steps a sequence takes and the range each input is drawn from, or none, for a proof of one
  step from any state an invariant admits, which covers every sequence. A loop's unwinding,
  `#[kani::unwind]`, is one more than its steps.
- `just kani <crate>` proves a crate's harnesses; `just kani` proves every crate's that holds any.
- Kani compiles every crate with `panic = "abort"`. A crate that contains a panic by unwinding
  refuses an aborting build with `#[cfg(all(panic = "abort", not(kani)))] compile_error!(..)`:
  Kani's build runs nothing it compiles, so no caught panic is at stake in it, and every other
  aborting build is still refused. `cargo xtask lint`'s unwinding rule reads `kani` as false.
- Kani's compiler builds `rdlt-wire` and `rdlt-engine` with their default features, Arrow,
  tonic, sonic-rs and stacker among them. `rdlt-wire`'s four harnesses take about five minutes,
  288 seconds from a build that had compiled its dependencies and 299 warm, so verification and
  not compilation is the time, and at most 0.7 GB for the largest process. `rdlt-engine`'s three
  harnesses take about 225 seconds warm, nearly all of it verification. A crate's proofs check
  its own code, in the crate. Kani's output is not clean: its toolchain warns of the
  `register_tool` feature it forces and of deprecated methods in the engine, and it drops the
  arguments of an `unreachable!`, which warns of an unused variable; none comes from a proof.
- A target whose proof does not finish within its job's limit at the least bound that still
  states its property becomes a property test, with the reason here; no target is left with part
  of its property proved.
- Kani 0.68.0 stops with an internal compiler error (`kani-compiler/src/intrinsics.rs:243`) on
  any harness that reaches a `parking_lot` lock, in the lock's slow path. Stubbing the lock's
  waits out (`#[kani::stub]` under `-Z stubbing`; sound, as a proof runs on one thread and never
  finds the lock taken) gets past the error but not to a proof: dropping a reservation takes the
  lock and admits waiters, and CBMC unrolls that through the drops of the waiters' channels and
  of what they hold. The ledger's harness that only reserves and releases ran 10 minutes without
  an answer; bounded to 8 unwindings it, and the lent reservation's harness at 3, filled 10 GB
  within three minutes. Code that takes such a lock is held by property tests.
- Mutation testing leaves out `proofs.rs` (`.cargo/mutants.toml`): no mutant's tests compile it.
  Coverage needs no rule: no build it measures sets `kani`.
- Wire credit is proved (`rdlt-wire/src/flow/proofs.rs`): a receiver opens within its bounds and
  stays within them, its window between its floor and its limit, growing without overflow and
  never shrinking, for any state those bounds admit; a grant of no bytes is refused; and over
  every call of up to 6 steps a sender's credit is what was granted
  less what was spent, above minus its largest frame, and the window once every frame sent is
  taken.
- The memory budget is proved where it takes no lock and held by property tests where it does.
  `rdlt-engine/src/budget/ledger/proofs.rs` proves, for every capacity, that a budget's shares
  add up to it, its data is what pushes and one request take, a piece is within a request, and a
  share's bytes beside one request of its class never wrap (18 seconds).
  `budget/admits/proofs.rs` proves, for every capacity and 1 to 24 reads at once, that each limit
  the budget admits fits the share it draws on (82 seconds), and that every budget from the least
  up admits a peer (121 seconds); `least` itself returns a budget that admits a peer while the one
  a byte below does not. 24 reads hold both of what sets the least budget, a frame up to
  16 reads and the dictionaries from 20, and keep the two proofs within the 15 minutes of
  `rdlt-engine`'s part of the `formal` check. Above 24 reads admission is not proved to be
  monotone, and a known counterexample lies past 524,288 reads. What a budget admits does not
  grow with it byte by byte, since its shares' rounding moves a frame by a few bytes: with one
  read, a budget of 53,690,302 bytes admits a peer and one of 53,690,314 does not. The ledger's
  own properties are property tests in `budget/ledger/tests.rs`, since each of its paths takes
  its `parking_lot` lock: every share stays within it through any asks, lets and presses, and none
  is held once all is let go; waiting requests are admitted in turn only where they fit, with
  abandoned ones given back; and a lent reservation gives back all it held, in any order.
- Kani's further targets: acknowledgements against the committed cursor, and the horizon; a log
  frame's length arithmetic and the parse of its preamble and head; the limits a handshake
  accepts and the identifier rules; a bounded body's length prefix and its window. Each is
  proved, or held by a property test with the reason here.

### Specifications: Quint over the design

- A protocol's specification is `specs/<name>.qnt`, and opens by naming the record it specifies.
  Its invariants are `val`s whose names begin with a capital letter, its temporal properties
  `temporal`s named so; where the simulation checks the same property, the invariant takes the
  check's name, and a check added later for an invariant takes the invariant's.
  A specification's names never reuse one that a standard TLA+ module defines, such as `Head`,
  because TLC reads the translation and rejects the clash.
- `just quint` typechecks each, runs its tests (its `run` definitions whose names end in `Test`),
  and simulates 10,000 runs of up to 20 steps against its invariants. `just quint-verify` checks
  each invariant with Apalache over every run of up to ten steps, and each invariant and temporal
  property with TLC over every state the specification reaches, which needs every domain finite.
- Quint runs Apalache from the distribution mise installs: `QUINT_HOME` is `target/quint`, where
  a link names it under its version, which `--apalache-version` passes, so Quint never fetches
  one. The recipe starts from an empty `QUINT_HOME`, refuses an `apalache-mc` that does not
  report the locked version, and fails if anything but the link appears there; the nightly job
  runs with GitHub's release hosts blocked. Quint uses any Apalache server that answers on its
  port, whatever its version, and starts one only when none does, so each check must print the
  banner of the server it started, naming the locked version, or the recipe fails. The servers
  run in `target/apalache`, where they write their output. Quint's recipes pass `--backend typescript`, since
  its default evaluator is a binary fetched from GitHub at run time, which the lock does not
  hold. The tools warn on every run, of a vulnerable protobuf type in Apalache and of the
  garbage collector in TLC. TLC is the jar inside Apalache's distribution, on the
  Temurin JDK mise installs.
- The log on object storage is specified (`specs/wal_objects.qnt`, ADR 0051): open marks created
  once with a token, chunk heads created where their name is free and kept only while the mark
  is there, either create losing its answer and landing later, and removal, mark first; the
  invariant `NoReopenAfterRemove`, that a removed log is never open again with chunks. Each
  operation is one step. A test takes a retried open, which the log never makes, and the
  invariant fails, as ADR 0051 says it would. `just quint` takes 8 seconds; `just quint-verify`
  takes about 44 minutes, Apalache's ten steps 35 to 43 of them and TLC 48 seconds over 123,904
  distinct states. Apalache checks the invariant to a bound and TLC over every state: the
  safety property is held both ways, and a temporal property would need TLC.
- Traces will tie a specification to the code: `quint run --out-itf` writes runs in the
  Informal Trace Format, and a driver among `rdlt-sim`'s tests will replay each against the real
  store over the simulation's object store, through the `itf` crate, a dev-dependency of
  `rdlt-sim` alone. A specification without a driver is tied to the code by its invariants'
  names where a simulation check carries them, and is otherwise a design alone. No check
  carries `NoReopenAfterRemove`'s name, so `wal_objects.qnt` is a design alone until its
  driver exists.

### Tools

- Quint 0.33.0, Apalache 0.62.4, a Temurin 21 JDK and Kani 0.68.0 are locked in `mise.lock` as
  every tool is (ADR 0048), each a release archive with its checksum for Linux x64 and macOS
  arm64. Apalache's one archive serves both, and its checksum is the one its release's
  `sha256sum.txt` states.
- Kani's release bundle holds its driver, its compiler and its solvers. `cargo-kani`, the
  frontend that installs a bundle, is built from crates.io alone, so `just kani` does what its
  setup does: rustup installs the nightly the bundle names, as it installs the repository's own
  toolchains; a link named `toolchain` in the bundle's install names that toolchain; a toolchain
  whose `rustc --version` is not the bundle's is refused; and the driver runs as `cargo-kani`,
  with `RUSTUP_TOOLCHAIN` set to the nightly, so the repository's toolchain file steers nothing
  beneath it.

### Where they run

- `just ready` runs `just formal-diff`: Kani's proofs of each crate the change touches that
  holds any, and `just quint` when it touches `specs/`; both in full when it touches `mise.toml`,
  `mise.lock`, the justfile or the pull-request workflow.
- The pull-request workflow runs the parts of `just formal-diff` side by side, on Linux. Its
  `formal-plan` job asks `just formal-parts` which parts the change needs against its base: a
  `kani:<crate>` part for each crate to prove, and a `quint` part for the specs. A `formal-part`
  job for each runs it with `just formal-part`, within 15 minutes of its own; the `formal` job,
  the check the ruleset requires, passes once every part has, or where the change needs none, so
  it always reports. On GitHub's runners `rdlt-wire`'s part took 276 to 481 seconds over four
  runs, nearly all of it its credit harness (271 to 450 seconds), `rdlt-engine`'s 261 to 459
  over four, and `quint` 16 seconds, each beside a setup of about 25 seconds: each part is within
  its limit with six minutes to spare, where the three in turn exceed 15 minutes on a slow
  runner.
- The nightly workflow's `kani` job proves every harness, and its `quint-verify` job model-checks
  every specification.

### What a formal layer does not replace

A proof holds the code it compiles for the inputs its bound admits; a specification holds the
design it states. Neither replaces the simulation, which runs the engine through faults, the
mutation tests, which hold that the tests notice a change, or fuzzing, which feeds the parsers
what no bound names.

## Rulings

- **Kani's frontend is not installed.** It is a crates.io crate with no release archive, which
  the tool lock cannot hold, and mise keeps a bundle extracted rather than as the archive the
  frontend's `--use-local-bundle` takes. Cost if wrong: a Kani release that changes how its
  driver finds its toolchain breaks `just kani` until the recipe follows, which the nightly
  shows.
- **Kani is exempted by each guard.** Kani has no unwinding build. Cost: a build that sets
  `--cfg kani` by hand and aborts compiles where it should not; nothing does.
- **The formal checks filter inside the workflow.** A check that a workflow's path filter skips
  never reports, so it could not be required; a filtering action would be one more pinned
  dependency. Cost: `formal-plan` checks out the whole history on every change, and each part's
  job installs Quint and Kani, from mise's cache after the first, and restores its own build.
- **Quint is `quint-co/quint`,** the repository's name; `informalsystems/quint` redirects there.
- **`itf` comes with the first trace driver,** with a ban that lets `rdlt-sim` alone depend on
  it. `deny.toml` keeps no list of admitted crates, so nothing stands in the way until then.

## Consequences

- A crate that adds proofs adds `#[cfg(kani)] mod proofs;`. If it catches panics, its guard
  against aborting builds exempts `kani`, as `rdlt-wire`'s does.
- `just ready` costs Kani's build and proofs of each crate under proof whenever the change
  touches it: on a laptop, about 300 seconds for `rdlt-wire` and 225 for `rdlt-engine`, warm, so
  a change that touches the tools, the justfile or the workflow, which runs both, spends about
  525 seconds proving. The `formal` check waits for its longest part alone.
- A contributor's `mise install` installs about 1.1 GB more on disk, and the first `just kani`
  fetches a nightly toolchain.
- Updating Kani is its version and `mise lock`; its nightly comes with its bundle. Updating
  Apalache is its version, `mise lock`, and its checksum compared with its `sha256sum.txt`.
- `formal` is a new check: the repository's ruleset has to require it beside `lint`.
