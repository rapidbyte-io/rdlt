# ADR 0014: Faults, concurrency, replay and coverage in the simulation

Status: accepted, 2026-09-26.

## Context

M3h is the third of the three simulation milestones (ADR 0012, ADR 0013). The simulation injected
transient and rate-limited failures into connector calls, crashed, stopped and raced runs, and ran
on one thread with a paused clock. It never met:

- a permanent failure or a panic;
- a failure while a schema change applies, a writer opens or a session closes;
- a run stopped at once;
- a destination two pipelines share;
- a scheduler that runs tasks in another order;
- real threads.

Nothing checked that a seed replays alike, or measured how much of the engine the simulation
reaches. Two gaps were also carried from M3g:

- **No float edges.** No test drew a NaN or an infinity: proptest's `any::<f64>()` never yields
  them.
- **Unmodelled shredding.** The model inferred a JSON push's types push by push, while the engine
  shreds a checkpoint's worth of pushes together.

The owner chose to keep all of this in one milestone.

## Decision

- **Faults beyond connector calls.**
  - The simulated connectors fail before and after a schema change applies, when a writer opens,
    when a session closes and when a stream lists its partitions.
  - Besides transient and rate-limited failures, one fault in ten is permanent and one in twenty
    is a panic.
  - A run may be stopped at once (`StopMode::Now`).
  - Now and then a call the engine bounds never returns until its caller gives up on it: two
    calls in a thousand at every fault point but a read, while faults are on. The hangs are drawn
    from a generator of their own, so the faults a seed met before fall as they did. The engine
    then waits 5 to 20 s for each connector call, drawn apart too, so a run meeting several hangs
    ends within the oracle's limit. A read never hangs: a source may be silent as long as it has
    nothing to send. The stress run on the real clock draws no hang.
  - A run with neither faults nor disruptions may fail only with a refusal the model predicts, so
    a failure only faults explain is a finding once they are off.
  - Each phase draws, apart from the seed's generator, how many of its first runs have faults
    injected: four to sixteen. Runs past the fourth draw their disruptions from a generator of
    their own, so a seed's generator draws alike whatever the count.
  - A crash, stop or racing run lands within the length of the last run that succeeded, or a
    following run's deadline; within two seconds before any run has succeeded.
- **Nothing a run began outlives it.** The simulated connectors count the sessions, writers and
  reads they hold. Once a phase's runs and the engine's tasks have ended, every count must fall
  to zero, and, over the network, every connector's host must run no task, within the longest a
  served connection whose host left behind a partition lives: its pings go unanswered for the
  connector's keep-alive interval and patience, then the writes that close it take none of their
  bytes for the connector's send limit; a margin is added. A destination that holds more sessions
  at once than one connection may (`ListenLimits::connection_sessions`), counted from before each
  opens until its close ends, is a violation.
- **A destination two pipelines share.** Where a seed turns it on, the workload's streams split
  between two pipelines that run at once against one store.
  - The simulated destination keeps each pipeline's epoch, state, staging and completed reads
    apart, as the contract's `OpenContext.pipeline` says.
  - One pipeline's open neither fences the other's session nor discards its staging.
  - Each phase converges only once both pipelines have succeeded.
- **Float edges everywhere.** The test kit draws a NaN, an infinity, a signed zero or an extreme
  one time in ten, for every float the engine's property tests and the simulation draw.
- **Pushes shredded together.** The engine gathers a partition's JSON pushes up to a checkpoint
  and infers one shape for them. The model knows the span of pushes the engine may gather with
  each one: the batches between two checkpoints, or the whole read where the stream checkpoints on
  demand.
  - Where every push in a span arrives alike, the model is exact.
  - Where they differ, a value its own push rules out is surely discarded or surely lands in a
    variant column. A value only the span's join rules out is *perhaps* discarded: its cell is
    optional, a row perhaps dropped is held at most once, and discard counts are ranges.
  - Such a column's refusals are predicted only as possible.
- **Seeded scheduling perturbation.** A perturbed `SimEnv` stretches each sleep by up to a tenth
  and a millisecond, and runs one compute job in four on a task of its own after up to eight turns
  of the scheduler.
  - Its draws come from a generator of their own, so a perturbed seed still replays exactly, and
    the engine's own random draws are unchanged.
  - A swarm feature turns perturbation on for about half the seeds.
- **A multi-threaded stress run.** `stress(seed)` runs the same oracle on the real clock, on the
  four worker threads the simulation's eight cores suggest, with compute jobs on a pool of a
  thread for each of the eight.
  - Races the paused single thread never meets can happen there.
  - A failure names its seed but does not replay exactly.
  - `just stress` runs it; the nightly workflow runs 200 seeds, as many at once as the runner's
    cores hold, eight cores to a seed. The contention between them is intended.
- **Same-seed replay.** `check_exactly_once` and `check_changes` return a digest of everything the
  destination holds at the end (its tables' columns and rows, each pipeline's state, and the
  receipts), a trace (every report and failure of the phases' runs, in the order the runs ended,
  hashed; not those of the runs that then check the engine's reports, whose times h2 moves over the
  network, ADR 0019), and the simulated time they took. A test runs each seed of both twice, one run
  after the other, and requires the same digests, traces, waits, features, reached paths and
  simulated time (the end time of a networked seed, or of one whose connectors are played in
  processes, apart, ADR 0019). The test job runs twenty seeds, and
  the nightly workflow a thousand through `just sim-replay`; a mismatch replays with
  `just sim-replay <seed>`.
- **Kept seeds name their path.** A seed kept because it found a defect names the path its defect
  was on, one of the paths the simulation records as its runs reach them (a connector the network
  stopped, a crash, a replay of a log left behind, a reset refused, a hostile host's attack and
  the rest), and its test fails where the seed no longer reaches it. The simulation grows around
  its seeds; a seed that no longer reaches its path is replaced by one that does, one that fails
  with the defect's fix undone where such a seed is found. Where none is, the seed keeps only its
  path, as its comment says.
- **Cancellation at every wait, beside the simulation.** A seed drops a call only where its faults
  and timing happen to. So each object a caller may call again after dropping a call to it is
  also tested by `rdlt_testkit::cancel::at_every_point`: the partition sink and the emitter, the
  remote writer, and the data plane's reader with its bounded body. The harness runs an operation
  to its end, then once for each time it waits, dropping it there. Each run builds afresh on a
  current-thread runtime of its own, its clock paused, so where the operation and its peers wait
  alike on every run (every `select!` biased, no tokio `watch` with several waiters), run *n*
  waits as the first did up to its *n*th wait. After each run a check calls the object again and
  asserts its model. An operation that never waits, waits more than 256 times or returns before
  the wait it is cut at when run again fails its test, and so does a run longer than an hour of
  the paused clock. A run that waits elsewhere as often, and a panic in a task the operation
  spawns, go unseen: a spawned fake reports its own failures. The project writes the harness
  rather than taking a crate: `tokio-test` polls a future by hand with no runtime to run the
  object's peers between polls, and `futures-test` adds spurious waits but never drops a future.
- **Seeds side by side.** Every simulation test runs its seeds through `for_each_seed`: each seed
  on a thread of its own, with its own runtime or turmoil simulation, its own generators and its
  own world, as many at once as the cores hold, the host's or as many as `RDLT_SIM_CORES` says.
  Determinism is per seed, so a seed replays alike whatever runs beside it. No two runs of one
  seed are in flight in one process: they share their world's name, and registering a name twice
  panics. The simulation's tests therefore run under nextest, one process a test; `cargo test`,
  which runs a binary's tests on threads of one process, would put two tests that check one seed
  side by side.
- **The simulation's own coverage.** `just sim-coverage` measures the engine, connector, host and
  data-plane code the simulation alone reaches over 1 000 seeds, leaving out test code and what
  the simulation never runs, and the nightly workflow holds it to the floor ADR 0019 states.
- **Found and fixed in the engine.**
  - A connector that panicked while an attempt opened or planned panicked the caller awaiting the
    run: its `open`, its listing of partitions, or its applying of a declared schema all run on
    the run's own task. An attempt now contains a panic and fails with an internal error, "an
    attempt panicked", like a panic in any of its tasks.
  - A seed did not replay alike, since M2a. Tokio's `watch` channel, which carried the
    coordinator's barriers to the partitions and the memory budget's pressure, spreads its
    waiters over eight queues, picked by a generator tokio seeds at random. Partitions woken by
    one barrier therefore ran in a random order. The replay test found it, and a sweep of `main`
    showed 3 seeds in 200 replaying differently.
    - The engine now uses a watch of its own (`watch.rs`), a value and version behind a mutex
      and one `Notify`, which wakes waiters in the order they began to wait, and only when its
      value changes.
    - The engine's clippy configuration bans awaiting tokio's `watch` receivers.
    - The connector's sink keeps tokio's channel, as it only reads the value.

## Consequences

- The engine already handled float edges and the new fault points: 200 000 cases of the lowering
  differential with NaN and infinities drawn found nothing.
- Tokio draws from its random generator in two places: an unbiased `select!` and a waiting
  `watch` receiver. Every `select!` in the engine is biased, and clippy now bans the waiting
  receiver, so neither can quietly break replay again.
- Loom and turmoil stay out of M3h:
  - §20.5's loom models of the memory budget, credit windows, seal bookkeeping and the
    coordinator's handshake check every interleaving of those primitives, where the stress run
    only samples some. ADR 0004 defers the budget's to M9, and the others belong with it.
  - Turmoil simulates networks, and belongs to the milestone that runs connectors over the
    network (M4).
- The simulation reaches least of JSON shredding's rarer paths, temporal text, plan validation
  and state records other than cursors.
- The nightly workflow runs the stress seeds, the replay's thousand seeds and the coverage run
  beside the shards; [docs/perf/sim.md](../perf/sim.md) records how long each takes.
