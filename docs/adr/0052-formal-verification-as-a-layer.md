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
| Arithmetic of credit, budgets, positions and frame lengths | Kani; property tests where a proof does not fit (below) |
| A protocol across processes and a store: the log on object storage, exactly-once, re-planning | Quint |

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
  them `flow`'s credit harness, and `rdlt-engine`'s ten about 1,000 to 1,250 seconds. A crate's
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
  `rdlt-engine/src/budget/ledger/proofs.rs` proves, for every capacity, that a budget's shares
  add up to it, its data is what pushes and one request take, a piece is within a request, and a
  share's bytes beside one request of its class never wrap (18 seconds).
  `budget/admits/proofs.rs` proves, for every capacity and 1 to 24 reads at once, that each limit
  the budget admits fits the share it draws on (94 seconds), and that every budget from the least
  up admits a peer while the one a byte below does not (165 seconds). What a budget admits does
  not grow with it byte by byte, since its shares' rounding moves a frame by a few bytes: with
  one read, a budget of 53,690,302 bytes admits a peer and one of 53,690,314 does not. So halving
  for the least ends beside whichever change its steps meet: over budgets up to 2^40 it meets the
  last for up to 24 reads, which the proof holds, and over every `u64` it would end at 53,690,299
  for one read. 24 reads hold both of what sets the least budget, a frame up to 16 reads and the
  dictionaries from 17, and keep the proofs within the 15 minutes of their part of the
  `formal` check; past them each read keeps less, which leaves a frame no less, so `least`
  halves for at most 24 and takes the larger of that and what the reads' dictionaries need, twice
  the least dictionary for each in a quarter of the budget, exact as what a read keeps grows with
  the budget; from 2^43 reads no budget holds that, and there is no least. The proof of the same
  for any count of reads above 24 ran 20 minutes without an answer, since what a read keeps is
  the reads' share divided by a count the proof leaves free, a product the solver must undo; a
  property test in `budget/admits/tests.rs` holds it instead, drawing counts up to 2^43 and
  budgets from the least to far above it. The ledger's own properties
  are property tests in `budget/ledger/tests.rs`, since each of its paths takes its
  `parking_lot` lock: every share stays within it through any asks, lets and presses, and none
  is held once all is let go; waiting requests are admitted in turn only where they fit, with
  abandoned ones given back; and a lent reservation gives back all it held, in any order.
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
- A log frame's length is proved (`rdlt-engine/src/wal/frame/len/proofs.rs`): a frame is its
  payload and its head, saturating at the largest length rather than wrapping, for every payload
  length (under a second). What `listed` counts of a list of numbers is held by tests in
  `wal/frame/tests.rs` instead: `an_end_frame_s_length_counts_every_width_of_number` checks a
  number at both ends of every width a `u64` takes, and the property test
  `an_end_frame_s_length_is_what_it_encodes_to` draws lists of up to 19 numbers, commas included.
  Its proof within a bound of one number took 424 to 595 seconds on a laptop and 786 on GitHub's
  runner, its job 833 of the 900 seconds it may take; over two numbers it took 627 seconds and
  over three 640, past the 10 minutes a part may take on a laptop.
- A log frame's preamble and head are proved (`rdlt-engine/src/wal/frame/proofs.rs`): from any bytes
  up to 16, a head's length is nothing until its five bytes have arrived, then the little-endian
  length they announce (under a second), and a chunk is accepted only where the bytes start with
  this format's own preamble (8 to 10 seconds); summing a frame's checksum in two pieces cut
  anywhere accepts exactly the frames of up to four bytes of payload that reading it whole accepts
  (163 to 306 seconds). A refusal's message is formatted as nothing in these proofs, by stubbing,
  which `rdlt-engine`'s manifest turns on, as no property reads it. The checksum is a bitwise
  CRC-32C, proved in a part of its own (`wal/frame/crc/proofs.rs`) to give the algorithm's standard
  check value and to sum in pieces as it does whole (293 to 410 seconds), stubbed in for the
  crate's, whose CPU detection is inline assembly, which Kani does not run. That a frame reads back
  as framed, and that bytes near it, cut or with a byte changed, read only where they are a frame's
  framing, is a property test (`a_frame_reads_back_as_framed_and_no_other_bytes_read` in
  `wal/frame/tests.rs`, payloads of up to 63 bytes). Its proof does not fit: with the reader beside
  the writer the harness took past 10 GB in 41 to 304 seconds in every form tried, even one byte of
  payload of a given length, as the writer's and the reader's bitwise checksums must be shown equal;
  the writer alone verifies in 4 seconds.
- A commit's horizon is proved (`rdlt-engine/src/coordinator/horizon/proofs.rs`). With up to
  three loads listed, any of them the load itself, and the load's oldest commit among its first
  four, the horizon is the earlier of that oldest commit and another listed load's first, and no
  later than any of them (190 to 270 seconds). A later commit's horizon is never earlier, where
  the later listing holds only loads listed before or loads newer than the load itself
  (15 to 23 seconds). The ids are drawn at any of the first 256 milliseconds: ids order by their
  time first, so these order, tie and differ as any ids do. Acknowledgement and positions are
  property tests, not proofs, because Kani does not run the one and does not usefully bound the
  other. That a source is told only cursors a landed commit covers is an order of the async
  coordinator's calls (`coordinator/tests/acks.rs`), and that a log's positions are exactly what
  commits record is a property of maps of JSON-keyed records (`wal/positions/tests.rs`).
- Where a property is over strings, sets, files or draining, a property test holds it, beside
  the code: a name map stays injective and append-only, tables named in turn and salted naming
  never share an identifier, a log's local and object-store names read back only as written
  and never hold `..` or an empty segment, nothing said of a secret, `*` and all, survives
  scrubbing and what follows it, and a torn local log is replayed exactly or refused with a
  typed error.

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
- The log on object storage is specified (`specs/wal_objects.qnt`, ADR 0051) at the level of the
  store's requests, in the order the store makes them: a writer opening its log with one create
  of its mark, publishing a chunk whole and one in parts, and deleting what its newest no longer
  needs; a replay removing leftovers, fencing the log at its next chunk, reading what its tail
  needs, releasing and removing it. Every request may succeed, fail, lose its answer once done,
  or go unanswered and land later, and a create may meet a racing one; either load may crash,
  and an intruder may leave a name the store never writes. Its invariants: a removed log is never
  open again with its load's chunks (`NoReopenAfterRemove`); every chunk the writer was told it
  published lies below every fence a replay holds (`OneHolder`); a replay reads a chunk from its
  head or the body its head names by its token (`OwnTokenReplay`); a listing holding a stray name
  refuses its operation (`StraysRefused`); a replay that read a log through read exactly what its
  tail needs (`CrashExactOrRefused`). Each has a test that breaks its rule and sees it fail. TLC
  checks every one over every state, 2,020,064 of them in about 8 minutes on four workers of a
  developer's machine, with two attempts a request, two chunks, two fences a replay and two
  faults a behavior: three faults take it past 13 minutes there, its states still growing. Its
  time on the nightly runner is not measured, and the job's limit is 120 minutes. At two faults
  TLC does not reach a fence read back from an earlier start's object; a test covers that
  alone. Its header names `tlc`: its model is finite and TLC enumerates it whole, while
  Apalache's bounded run cannot hold its state and runs out of memory at the first step. `just
  quint` simulates 500 runs of 80 steps.
  The specification has no temporal property: the log's guarantees are safety, that it is never
  reopened, has one holder, is read by its own tokens, refuses strays and replays exactly, and a
  liveness claim needs fairness assumptions on a store that may answer nothing forever. A
  property that needs one will add it.
- Exactly-once delivery is specified (`specs/exactly_once.qnt`, ADRs 0004, 0029 and 0030) as the
  engine orders it: an attempt, its load, opens its log, fences and replays the others in a session
  it opens for the first commit it replays, then opens its own session and commits what it reads,
  logged first, sources that cannot read again told once the log holds the commit, the destination's
  commit keyed by load and number under the session's epoch, sources that read again told after.
  Attempts crash, logs refuse commits, commits fail or lose their answer, tellings fail, and
  attempts start beside one another; a retry is an attempt of its own, and the model does not group
  attempts into runs. Its invariants, each with the check in the code it stands for: every row a
  partition's position lies past is held once and none past it (`ExactlyOnce`, the contents and
  cursor checks of the simulation's `check_exactly_once`); a source stands at a position only once
  its commit landed, or for one that cannot read again once a log holds it (`Acknowledged`, the
  simulation's `check_acknowledged` and its source's check that no acknowledged offset lies beyond
  the committed one); a source stands only where it was told, at a committed position
  (`PositionOnlyInCommitted`, ADR 0030's `S-ACK` clause as `rdlt-certify` checks it); no commit
  lands from a fenced session (`FencedNeverCommits`, the destination's fence, which the model's
  destination enforces and no simulation check covers). Each has a test that breaks its rule. Once
  faults stop, every row lands once and no log is left (`Converges`), which TLC checks under weak
  fairness of every step. A behavior need not end so: an attempt started once the load converged
  opens a log, and a fault that ends the last attempt the bounds start leaves it open, which the
  engine's next attempt would replay; a test shows such an end, where no step is left. The protocol
  is a module of constants. The checkers explore one pipeline reading a partition that reads again,
  of one row, and one that does not, of two; five attempts and two faults, as a fault may end two
  attempts started together and five leave one that meets none. The checkers are held to 20 minutes
  for TLC and 10 for Apalache on the nightly runner, and half that on four workers of a developer's
  machine, which sized these bounds: TLC checks every state, 477,460 of them, in about a minute
  there, and Apalache every run of up to seven steps in about 90 seconds, where eight take about 8
  minutes. Two rows of each partition, or a second pipeline, take TLC past 10 minutes there, its
  states still growing. A second pipeline sharing the destination with a partition of its own is the
  module `exactly_once_pair`, which `just quint` tests and simulates and no checker explores; a test
  shows that its sessions fence none of the first pipeline's. `just quint` simulates 2,000 runs of
  60 steps of each module, and its tests and runs of this spec take about 45 seconds on a
  developer's machine. A replay commits a logged commit again whole where the destination stands
  where its load left it, and otherwise, where another commit landed since, only its seals of
  partitions that cannot read again, each where its partition stands where the seal starts (ADR
  0029). Within these bounds the second case decides nothing a whole replay would not: replaying
  every commit whole leaves TLC's 477,460 states as they were, so no replay the bounds reach passes
  over a seal. That case guards resets and phase transitions (ADRs 0033, 0034), which the spec
  leaves out.
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
  which no checker explores. The simulation never adds or retires a partition, so no seed re-plans a
  changed set and no simulation check holds these properties: the engine's
  tests `a_following_run_starts_the_partitions_its_source_adds`,
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
- Traces tie a specification to the code. A spec's header names each trace by name, seed and
  the goal its run reaches; `just quint` writes each from its seed, which reproduces it exactly,
  and fails where it differs from the one `specs/traces/<spec>/` holds, so a trace cannot drift
  from its spec; `just quint write` rewrites them, and `just quint-seed` finds a seed reaching a
  goal. A driver among `rdlt-sim`'s tests replays each trace of the log on object storage against
  `ObjectStoreWal` over the simulation's store in memory, request by request: each request the
  store makes waits for its step, must be the request the step makes, and meets the fault the
  step names; a request left unanswered lands at the step the trace lands it, on the paused
  clock; the code's answers and what the store holds must be the trace's at every step. The
  traces are read with the `itf` crate, a dev-dependency of `rdlt-sim` alone, which `deny.toml`
  bans everywhere else. The engine's fence is stated by the spec and run by the driver as the
  store calls it makes, so the driver holds the store to the spec and the simulation holds the
  engine to the store. A specification without a driver is tied to the code by its invariants'
  names until it has one, or this record says it is a design alone.

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
  of it its credit harness (271 to 450 seconds), and `quint`, all three specs, 241 seconds, each
  beside a setup of about 25 seconds. `rdlt-engine`'s five harnesses took 838 seconds in one job,
  against a limit of 900: `budget::admits`'s two 228 and 95 seconds, `budget::ledger`'s one 22, and
  `coordinator::horizon`'s two 28 and 394, beside about 70 seconds of compiling and setup. As parts
  of their own, `coordinator::horizon`'s takes about 8 minutes, `budget::admits`'s about 7 and
  `budget::ledger`'s about 2. `wal::frame`'s three harnesses took 201 seconds in a job of 252,
  and `wal::frame::crc`'s one 545 in a job of 589.
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
- **`itf` is a dev-dependency of `rdlt-sim` alone,** held there by a ban with that one wrapper.
- **Quint runs its TypeScript simulator,** for the reason given under *Specifications*.
  Cost: simulation is slower, about fifty runs of 80 steps a second for the log's spec, so
  `just quint` samples fewer runs than a faster evaluator would allow.
- **A spec's traces are committed.** The driver then runs in every test run, on Linux and macOS,
  with no Quint, and every change to the code meets them. Cost: about 2 MB of generated JSON for
  the log's spec, rewritten when the spec changes.
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
  seeds the oracle fails too, and one that replays every commit whole was refused on none. The hook was 393 lines.
  The observer's last form admitted more than its first: replays of held commits, tellings of
  commits that move no source of their kind, taken unobserved, and commits with no ends dropped,
  fenced and failed ones among them. Cost: a design defect the simulation does not meet is found
  only by the spec's own checks.
- **The re-planning spec is tied to the code by its invariants' names and the engine's tests
  alone.** The simulation never adds or retires a partition, so no seed re-plans a changed set,
  and checking its runs against a spec was ruled out for the exactly-once spec. Cost: a design
  defect in re-planning that the engine's tests do not meet is found only by the spec's own
  checks.

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
