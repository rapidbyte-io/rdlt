# ADR 0052: Formal verification as a layer

Status: accepted, 2026-10-18.

## Context

The simulation, the mutation tests and fuzzing find a defect by running into it: a seed that
interleaves a cancellation with a send, a change no test catches, an input the fuzzer reaches.
Some properties are stated over every input or every interleaving, and a run meets a sample of
them: the credit a sender may spend, a budget's arithmetic, attempts that crash and race over one
destination. A bounded model checker proves a property of the code for every input up to a bound;
a specification checked by a model checker proves a protocol's invariant over every interleaving
up to a bound. Neither runs the program: each holds only what it is given, the code's arithmetic
or the protocol's design.

## Decision

### Which layer catches which class of defect

| Class | Caught by |
|---|---|
| Cancellation safety: state changed before an `.await` | a harness cancelling at every await point; the simulation's leak oracle |
| Check-then-act, or state initialised in parts | the simulation's hostile peers; starts racing on real threads for a process's last group, in the soak; Kani where the state is pure |
| Resources held but not charged | the simulation's real-memory and bounds oracles; property tests of the budget's ledger |
| A wait with no deadline | the simulation's hung and stalled faults |
| Redaction out of order or bypassed | the simulation's secret canary, said by connectors in process, over the network and in played processes, and the soak's spawned connectors, which say one; a property test of `Redactions::scrubbed` |
| Trust in the file system | regression tests; every played placement opens, checks and hashes its binary |
| Conversion fidelity | the simulation's encodings; differential tests |
| A contract's documentation, fixtures, or an API's precondition | Kani over the limits a handshake accepts; tests |
| Arithmetic of credit, budgets, positions and frame lengths | Kani; property tests where a proof does not fit (below) |
| A protocol across processes: exactly-once, re-planning | Quint |

A formal layer is added to the layers of its class, never in place of one.

### Proofs: Kani over the code

- A module's proofs live in `src/<module>/proofs.rs`, declared `#[cfg(kani)] mod proofs;` beside
  its tests. `kani` is a configuration only Kani sets; the workspace's `unexpected_cfgs` lint
  names it in its `check-cfg`, so every other build compiles none of it and warns of none of it.
  Kani provides the `kani` crate itself: a crate adds no dependency for it.
- A proof is a `#[kani::proof]` function, named as a test is, over `kani::any()` inputs narrowed
  by `kani::assume`, and states its bound in a paragraph of its doc comment that begins `Bound:`:
  the steps a sequence takes and the range each input is drawn from, or none, for a proof of one
  step from any state an invariant admits, which covers every sequence. A loop's unwinding,
  `#[kani::unwind]`, is one more than its steps.
- `just kani <crate>` proves a crate's harnesses; `just kani` proves every crate's that holds any;
  `just kani <crate> <module>` proves those of one module's proofs, such as `budget::admits`'s.
  Kani reads the crate's own manifest: an unstable feature its proofs use, such as `stubbing`,
  is turned on in its `[package.metadata.kani.unstable]`, for every harness of the crate.
- Kani compiles every crate with `panic = "abort"`. A crate that contains a panic by unwinding
  refuses an aborting build with `#[cfg(all(panic = "abort", not(kani)))] compile_error!(..)`:
  Kani's build runs nothing it compiles, so no caught panic is at stake in it, and every other
  aborting build is still refused. `cargo xtask lint`'s unwinding rule reads `kani` as false.
- Kani's compiler builds `rdlt-wire` and `rdlt-engine` with their default features, Arrow, tonic,
  sonic-rs and stacker among them. Each module's proofs are a part of their own, which compiles for
  seconds when warm, so verification and not compilation is the time, as the bullets below give it
  harness by harness. Warm on a laptop, `rdlt-wire`'s nine harnesses take about 460 seconds, 319 of
  them `flow`'s credit harness, and `rdlt-engine`'s three about 250 seconds. A crate's
  proofs check its own code, in the crate. Kani's output is not clean: its toolchain warns of the
  `register_tool` feature it forces and of deprecated methods in the engine, and it drops the
  arguments of an `unreachable!`, which warns of an unused variable; none comes from a proof.
- A target whose proof alone does not finish within its job's limit at the least bound that
  still states its property becomes a property test, with the reason here; no target is left
  with part of its property proved. Proofs that only together come near the limit are split
  into jobs of their own instead (below).
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
- The limits a handshake accepts are proved (`rdlt-wire/src/limits/proofs.rs`) for every value
  of every limit: a peer's limits are read as sent, or as the protocol's defaults where it left
  them unset, but the dictionary limit, taken as sent; a limit set a handshake admits, sent or
  not, meets every minimum and every rule between its limits, and one it refuses names the first
  limit below its minimum; what two ends agree is, limit by limit, the lesser of the two ends',
  and meets every minimum both meet; and a value is admitted exactly within its own limit. They
  take about 15 seconds together: 0.3, 5.5, 4.5 and 4.8. The identifier rules are a property
  test (`rdlt-connector/src/wire/tests.rs`): rules from the wire are accepted exactly within the
  bounds the protocol documents, refused as the first they break, and those accepted keep what
  was sent. Their bounds are on thousands of strings, up to 4096 reserved words of up to 256
  bytes each, past any useful Kani bound.
- The memory budget is proved where it takes no lock and held by property tests where it does.
  `rdlt-engine/src/budget/ledger/proofs.rs` proves, for every capacity, that a budget's shares add
  up to it, its data is what pushes and one request take, a piece is within a request, and a share's
  bytes beside one request of its class never wrap, but the data beside a push, which the ledger
  adds saturating (7 seconds). `budget/admits/proofs.rs` proves, for every capacity and 1 to 24
  reads at once, that each limit the budget admits fits the share it draws on, and that the parts
  what reads keep is divided into fit the reads' share, each twice the least dictionary wherever the
  share holds one (96 seconds); and that every budget from the least up admits a peer while the one
  a byte below does not (198 seconds). What a budget admits does not grow with it byte by byte,
  since its shares' rounding moves a frame by a few bytes, so halving for the least could end
  beside such a step: over budgets up to 2^40 it ends at the least for up to 24 reads, which the
  proof holds. A frame sets the least for any count of reads, since a part never holds less than
  twice the least dictionary where the share holds one; from 18 reads the share holds 18 parts at
  the least, and more reads each keep no more, which leaves a frame no less, so `least` halves for
  at most 24, which keeps the proofs within the 15 minutes of their part of the `formal` check. The
  proof of the same for any count of reads above 24 ran 20 minutes without an answer, since what a
  read keeps is the reads' share divided by a count the proof leaves free, a product the solver must
  undo; a property test in `budget/admits/tests.rs` holds it instead, drawing counts up to 2^43 and
  budgets from the least to far above it. The ledger's own properties are property tests in
  `budget/ledger/tests.rs`, since each of its paths takes its `parking_lot` lock: every share stays
  within it through any asks, lets and presses, and none is held once all is let go; waiting
  requests are admitted in turn only where they fit, with abandoned ones given back; and a lent
  reservation gives back all it held, in any order.
- A bounded body's length prefix is proved (`rdlt-wire/src/bounded/proofs.rs`), and its property
  test of the same name stays beside it (`rdlt-wire/src/bounded/tests.rs`): from any bytes up to 16
  and any wire bound, a prefix is read as nothing until it is whole, then as the length it declares
  within the bound or a refusal beyond it (120 seconds). A refusal formats the length and the bound
  into its message, and Kani 0.68 does not finish formatting one number in five minutes, so the
  proof replaces `std::fmt::format` with a function that formats nothing, by stubbing, which
  `rdlt-wire`'s manifest turns on: what is read or refused never depends on a message's text. What a
  body lacks is a property test: its proof, with formatting stubbed out and bounded to 6 bytes
  arrived and 6 waiting unread, which still holds a whole prefix and a byte past it, had not
  reported when ended at 900 seconds, past the 10 minutes a proof may take, most likely since a
  first growth of the empty arriving buffer writes up to 63 bytes before its first aligned one. With
  up to 16 bytes arrived and up to 16 waiting unread, the property test holds that what a body lacks
  never reaches past its message's end, the length read from what arrived or, before anything has,
  from what waits unread.
- A bounded body's growth rule is a property test (`rdlt-wire/src/bounded/tests.rs`): one
  arrival of what a body lacks, onto any arriving buffer the rule admits, holds what arrived and
  keeps the buffer's room at what arrived until the prefix is whole, then within the message's
  length and four times what arrived. Its proof does not fit: the buffer starts what it holds
  up to 63 bytes into its allocation, by the allocation's address, which Kani takes as any
  value, and moving what is held by any such amount as the buffer grows took the harness past
  10 GB in about 70 seconds, with up to 8 bytes held and arriving and with up to 6.
- A bounded body's window is a property test (`rdlt-wire/src/bounded/tests.rs`): a message whose
  whole prefix waits takes room in its connection's window before anything past its prefix is
  copied, holds none of its bytes while it waits, though the room it has gathered stays taken
  (ADR 0042), goes on once room comes back, and gives its room back once passed on or once its
  body is dropped. Its proof does not compile: waiting for room reads tokio's cooperative
  budget, a thread-local whose value has a destructor, and Kani 0.68 stops with an internal
  error on any such thread-local, at the `catch_unwind` intrinsic. A harness that takes and
  gives back room without waiting compiles, and took past 10 GB in about 7 minutes.
- Acknowledgement and done markers are property tests, not proofs, because Kani does not run
  the one and does not usefully bound the other. That a source is told only cursors a landed
  commit covers is an order of the async coordinator's calls (`coordinator/tests/acks.rs`), and
  that the done markers are exactly what commits record is a property of maps of JSON-keyed
  records (`coordinator/done/tests.rs`).
- Where a property is over strings, sets, files or draining, a property test holds it, beside
  the code: a name map stays injective and append-only, tables named in turn and salted naming
  never share an identifier, and nothing said of a secret, `*` and all, survives scrubbing and
  what follows it.

### Specifications: Quint over the design

- A protocol's specification is `specs/<name>.qnt`, and opens by naming the record it specifies.
  Its invariants are `val`s whose names begin with a capital letter, its temporal properties
  `temporal`s named so; where the simulation checks the same property, the invariant takes the
  check's name, and a check added later for an invariant takes the invariant's.
  A specification's names never reuse one that a standard TLA+ module defines, such as `Head`,
  because TLC reads the translation and rejects the clash.
- `just quint` typechecks each, runs its tests (its `run` definitions whose names end in `Test`),
  and simulates runs against its invariants, as many and as long as its header says, in the
  module named for the file or in each that a `// quint main:` line names.
  `just quint-verify` checks the module named for each specification's file with the checker
  that fits it, named by a `// quint verify:` line in its header. `tlc` checks each invariant
  and temporal property over every state a finite specification reaches, which covers every run
  a bounded check could explore. `apalache [--max-steps <n>]` checks each invariant symbolically
  over every run of up to ten steps, or `<n>`, for a specification whose states are too many to
  enumerate. A specification that names neither is checked by both, and `--max-steps <n>` alone
  bounds Apalache's runs in that check.
- Quint runs Apalache from the distribution mise installs: `QUINT_HOME` is `target/quint`, where
  a link names it under its version, which `--apalache-version` passes, so Quint never fetches
  one. The recipe starts from an empty `QUINT_HOME`, refuses an `apalache-mc` that does not
  report the locked version, and fails if anything but the link appears there; the nightly job
  runs with GitHub's release hosts blocked. Quint uses any Apalache server that answers on its
  port, whatever its version, and starts one only when none does, so each check must print the
  banner of the server it started, naming the locked version, or the recipe fails. The servers
  run in `target/apalache`, where they write their output. Quint's recipes pass
  `--backend typescript`, since its default evaluator is a binary fetched from GitHub at run
  time, which the lock does not hold. The tools warn on every run, of a vulnerable protobuf type
  in Apalache and of the garbage collector in TLC. TLC is the jar inside Apalache's
  distribution, on the Temurin JDK mise installs.
- Exactly-once delivery is specified (`specs/exactly_once.qnt`, ADRs 0004 and 0030) as the engine
  orders it: an attempt, its load, opens a session, which fences every session of its pipeline
  opened before, reads each partition from where the destination holds it, commits what it sealed
  under the session's epoch, and tells each partition's source the position its commit landed with
  only once the commit lands. A source that reads again serves any position; one that cannot
  forgets what precedes the position it was last told, refuses a read from before it, and serves
  what follows it to every reader that asks. Attempts crash, commits fail or lose their answer,
  tellings fail or lose theirs and leave the attempt reading, owing what they carried to its next
  telling, which a commit with nothing to publish makes too, and attempts start beside one another,
  two of them reading one partition whose source cannot read again; a retry is an attempt of its
  own, and the model does not group attempts into runs. Its invariants, each with the check in the
  code it stands for: every row a partition's position lies past is held once and none past it
  (`ExactlyOnce`, the contents and cursor checks of the simulation's `check_exactly_once`); no
  source stands past the position the destination holds (`Acknowledged`, the simulation's
  `check_acknowledged` and its source's check that no acknowledged offset lies beyond the committed
  one); a source stands only where it was told, at a position a commit landed with
  (`PositionOnlyInCommitted`, ADR 0030's `S-ACK` clause as `rdlt-certify` checks it); a position an
  attempt owes a source is one a commit landed with (`OwedOnlyLanded`, the acknowledgement property
  test's check that every report, made again or not, carries only cursors a landed commit covers);
  no commit lands from a fenced session (`FencedNeverCommits`, the destination's fence, which the
  model's destination enforces and no simulation check covers).
  Each has a test that breaks its rule: `Acknowledged`'s tells a source the position of what it
  sealed before its commit lands. Once faults stop, every row lands once (`Converges`), which TLC
  checks under weak fairness of every step; a position never moves back, so the load stays so. The
  protocol is a module of constants. The checkers explore one pipeline reading a partition that
  reads again, of one row, and one that does not, of two; five attempts and two faults: a fault
  ends one attempt at most, so three leave one that meets none, and the others start beside a live
  one or once the load converged. The checkers are held to 20 minutes for TLC and 10 for Apalache
  on the nightly runner, and half that on four workers of a developer's machine: TLC checks every
  state, 298,944 of them, in about 25 seconds there, and Apalache every run of up to eight steps
  in about 235 seconds, where nine take past 5 minutes. Two rows of each partition take TLC
  3,846,313 states and about 6 minutes there. A second pipeline sharing the destination with a
  partition of its own is the module `exactly_once_pair`, which `just quint` tests and simulates
  and no checker explores; a test shows that its sessions fence none of the first pipeline's.
  `just quint` simulates 2,000 runs of 60 steps of each module, and its tests and runs of this
  spec take about 22 seconds on a developer's machine.
- Re-planning in a continuous run is specified (`specs/replan.qnt`, ADR 0031): partitions the source
  names appear, change between bounded and unbounded, and disappear between plans, a read keeping
  the kind its partition had when it started; a running read the plan drops stops, and a later
  commit takes what it sealed; a read of an unbounded partition may end on its own, the rows after
  its last checkpoint read again by the next; a partition named again resumes where it was
  committed; a bounded one that ended is read again once its end commits; rows arrive as the run
  reads, and the run may crash. No partition is lost or doubled: no row is skipped or held twice
  (`NoPartitionLostOrDoubled`: one read of a partition at most, each read's next segment starting
  where the destination holds the partition, every row below a position held once), and the reads
  follow the source (`PlansFollowTheSource`: a partition the source names comes to be read, again
  once a read of it stopped or ended, and one it no longer names to have no read running), which TLC
  checks under weak fairness of every plan and every commit; a dropped partition drains, its sealed
  rows committed unless the run crashes first (`RemovedDrains`), which TLC checks under weak
  fairness of every commit. Each invariant has a test that breaks its rule, and a plan that never
  starts a partition the source adds, never stops one it dropped, or never reads again one whose
  read ended fails `PlansFollowTheSource` in TLC. Its header names `tlc`: the model is finite, and
  TLC checks every state, 1,095,033 of them, at three partitions, one of two rows and two of one,
  and two changes or crashes; that covers every run a bounded check could explore. On four workers
  of a developer's machine the invariant and `RemovedDrains` take about 2 minutes and, with
  `PlansFollowTheSource`, about 6; the time on the nightly runner is not measured, and the nightly
  job's limit is 120 minutes. `just quint` tests and simulates 2,000 runs of 60 steps of each
  module, among them `replan_rows2`, at two rows of every partition and three changes or crashes,
  which no checker explores. In the simulation, a churning source (ADR 0031) retires, adds and names
  again partitions while runs follow it, through the seeds' faults, and checks named for the
  invariants hold its runs. `no_partition_lost_or_doubled` holds, in the engine's process, that no
  run reads a partition twice at once. Beside it, in every placement, unless a reset races the runs,
  the destination holds that no row of an incremental stream lands below where it held the
  partition, and the contents check holds that every row lands once. `plans_follow_the_source`
  holds, in the engine's process on the single thread, that a read whose partition a later plan
  dropped sends no more than its partition's channel holds and one in flight. Its other half, that a
  partition named comes to be read, holds only at each phase's end, where every partition is named
  again and every row must land. `removed_drains` holds, in the engine's process, that an
  undisturbed run that succeeded in one attempt, with every run beside it, commits every checkpoint
  the engine took from a read of a partition its plans dropped. Beside them, the engine's tests
  `a_following_run_starts_the_partitions_its_source_adds`,
  `a_following_run_reads_a_bounded_stream_again_as_it_grows`,
  `a_run_for_a_while_follows_its_source_and_loads_each_message_once` and
  `a_partition_a_plan_drops_and_names_again_reads_on_from_where_it_was_committed` exercise
  `NoPartitionLostOrDoubled` on the code, each holding that every row lands once;
  `a_partition_read_again_takes_its_place_and_a_new_one_a_retired_place` and
  `replans_naming_new_partitions_take_the_places_of_those_they_dropped` read no rows and hold the
  places reads take: a partition read again takes its own, a new one a retired one, and a run
  tracks no more partitions than its plans name; the first two, which read an added partition
  and a bounded one again, and `a_following_run_stops_the_partitions_its_source_no_longer_plans`
  exercise `PlansFollowTheSource`; and that last one exercises `RemovedDrains`: the retired
  partition's first rows land and are committed, and it reads no further than a plan past its
  retirement. `a_partition_a_replan_drops_keeps_its_position_across_runs` holds that a run records
  a dropped partition's position where it committed it, and that the next run reads on from there
  with no row loaded twice.

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
  part for each module that holds proofs in each crate the change touches, such as
  `kani:rdlt-engine:budget::admits`, which proves the harnesses beneath the module's `proofs`
  (Kani's `--harness budget::admits::proofs::`), a `kani:<crate>` part where a crate's root holds
  them, and a `quint` part for the specs. A `formal-part` job for each runs it with
  `just formal-part`, within 15 minutes of its own, from a build cache the crate's parts share;
  the `formal` job, the check the ruleset requires, passes once every part has, or where the
  change needs none, so it always reports.
- On GitHub's runners `rdlt-wire`'s `flow` part took 276 to 481 seconds over four runs, nearly all
  of it its credit harness (271 to 450 seconds), beside a setup of about 25 seconds.
  `rdlt-engine`'s `budget::admits` part takes about 7 minutes and `budget::ledger`'s about 2.
- A part that comes near its limit is split further, by module: its harnesses are divided
  between modules, each with proofs of its own and so a part of its own. Only a harness that
  alone would pass the limit becomes a property test.
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
- **A crate's Kani flags are its own.** `just kani` names the crate's manifest to Kani, which
  reads its `[package.metadata.kani]`, rather than taking flags for each module's part: Kani
  compiles with the features it turns on, and a crate's parts share one build cache because
  they build alike. Cost: a feature one module needs is on for all of the crate's harnesses;
  stubbing changes nothing for a harness that names no stub.
- **The formal checks filter inside the workflow.** A check that a workflow's path filter skips
  never reports, so it could not be required; a filtering action would be one more pinned
  dependency. Cost: `formal-plan` checks out the whole history on every change, and each part's
  job installs Quint and Kani, from mise's cache after the first, and restores its own build.
- **Quint is `quint-co/quint`,** the repository's name; `informalsystems/quint` redirects there.
- **Quint runs its TypeScript simulator,** for the reason given under *Specifications*.
  Cost: simulation is slower, so `just quint` samples fewer runs than a faster evaluator would
  allow.
- **The exactly-once spec is tied to the code by its invariants' names alone,** each named with its
  check under *Specifications*. Checking the simulation's runs against it was tried: a seed's
  sessions, commits and positions told, written as observations a checker searches the spec for. It
  was held to four criteria: reach, 25 seeds within what the spec models among the first 500;
  admission, each of those admitted within 10 minutes; teeth, a seeded engine bug caught where the
  simulation's oracle passes; and size, a hook of at most 300 lines. Reach held, as 68 of the first
  500 seeds lay within what the spec models, and the other three failed. Of the first 25 seeds in
  scope, in file-name order, TLC admitted 19 in 7 to 163 seconds, rejected none, and ran out of 10
  minutes on 6, as Apalache did. No seeded engine bug was caught where the oracle passes: one that
  tells sources that read again before the destination's commit was refused by the observer only on
  seeds the oracle fails too, and one that replays every commit whole was refused on none. The
  hook was 393 lines. The observer's last form admitted more than its first: replays of held
  commits, tellings of commits that move no source of their kind, taken unobserved, and commits
  with no ends dropped, fenced and failed ones among them. Cost: a design defect the simulation
  does not meet is found only by the spec's own checks.
- **The re-planning spec is tied to the code by its invariants' names, the simulation's checks that
  take them, and the engine's tests.** A read placed apart ends only once its stop crosses the wire,
  and sends into what the wire holds meanwhile. A stopped one's sends are dropped there. So the
  simulation holds reads to their plans in the engine's process alone, and holds every placement to
  the destination's and the contents checks. On threaded runs it does not bound a read's late sends
  even there: between the source's answer and the engine's stop, another worker's reads go on for an
  unbounded time, so no bound on a late send holds; two reads at once and undrained checkpoints are
  judged there as on one thread. Within a following run of half a second to two, a named partition
  may wait for a read slot, or for a commit the policy makes due later. So that a named partition
  comes to be read is held only at a phase's end, by initial planning. A churned source names starts
  in the recorded phase, which the engine must not start from. With the fix that starts a partition
  added mid-run from its beginning undone, churning seeds fail. Hand-planted defects fail each
  check: a partition read again from its beginning, a dropped read left running, a stopped read's
  seals dropped, and a stopped read not counted as unsettled. Cost: a re-plan that never starts a
  partition its source adds, while the next run's initial planning does, is found only by the
  engine's tests and the spec's own checks.
- **Process placement enters the simulation as a played launch.** `rdlt-host`'s `sim` feature gives
  `Local` a launch whose process a script plays in the host's own process. The host checks the
  binary, drains and scrubs the output, witnesses the exit, and stops and kills the process as it
  does a spawned one, through `Process`. The script serves the simulated connector over an in-memory
  socket. What only the operating system does stays out: descriptors, process groups and their
  reaper, the registry of groups, which every seed's thread would share, and bubblewrap. Those keep
  their tests against real processes: a nightly soak loads through connectors spawned through
  `Local` as real processes (killed, stopped with SIGSTOP, crashing, flooding their output, saying a
  secret that ends in whitespace) and does not replay, so a failure names its round. A start's check
  and its listing are a section no single-threaded seed interleaves, so the soak races starts on
  real threads. Evidence: a played seed replays alike over 64 seeds, and over a sweep of 1 000 once
  a process's exit and the close of its standard error wake their waiters in the order they waited,
  where two seeds of the sweep had replayed apart; a played seed took 206 ms and a networked seed
  432 ms, the means over the 185 played seeds and over the networked seeds of one 1 000-seed sweep,
  timed before that change; with a drained line trimmed before it is scrubbed, 37 of the first 200
  seeds fail on the canary in the exactly-once sweep and 38 in the change sweep, each a played seed;
  and 1 000 seeds reach 58 % of `local.rs`, 52 % of `local/provide.rs`, 50 % of `local/binary.rs`,
  55 % of `local/grants.rs`, 94 % of `local/process/output.rs`, 76 % of `local/process/played.rs`,
  98 % of `kills/landing.rs` and 80 % of `supervise.rs`, `supervise::spawn` among it. Two criteria
  set before it was built were passed over. The seam adds 312 lines outside tests where 280 were
  allowed, about 30 of them moved, not new. `kills.rs` is reached in 49 % of its lines and
  `local/process.rs` in 45 %, short of half: the first's rest is the kill path of a remote
  placement, which the simulation's network does not wire up, and the second's is the spawn that
  only the operating system runs. Both stay out of the simulation's coverage (ADR 0019). Cost if
  wrong: a feature's worth of code beside `Process`, removed with the feature.

## Consequences

- A crate that adds proofs adds `#[cfg(kani)] mod proofs;`. If it catches panics, its guard
  against aborting builds exempts `kani`, as `rdlt-wire`'s does.
- `just ready` costs Kani's build and proofs of each crate under proof whenever the change touches
  it: on a laptop, about 460 seconds for `rdlt-wire` and 1,400 to 1,850 for `rdlt-engine`, warm, so
  a change that touches the tools, the justfile or the workflow, which runs both, spends about 1,900
  to 2,300 seconds proving. The `formal` check waits for its longest part alone.
- A contributor's `mise install` installs about 1.1 GB more on disk, and the first `just kani`
  fetches a nightly toolchain.
- Updating Kani is its version and `mise lock`; its nightly comes with its bundle. Updating
  Apalache is its version, `mise lock`, and its checksum compared with its `sha256sum.txt`.
- `formal` is a new check: the repository's ruleset has to require it beside `lint`.
