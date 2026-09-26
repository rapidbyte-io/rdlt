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
  - A run with neither faults nor disruptions may fail only with a refusal the model predicts, so
    a failure only faults explain is a finding once they are off.
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
  - Where every push in a span arrives alike, the model is exact, as before.
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
- **A multi-threaded stress run.** `stress(seed)` runs the same oracle on four worker threads and
  the real clock, with compute jobs on a pool of four threads.
  - Races the paused single thread never meets can happen there.
  - A failure names its seed but does not replay exactly.
  - `just stress` runs it; the nightly workflow runs 200 seeds.
- **Same-seed replay.** `check_exactly_once` returns a digest of everything the destination holds
  at the end: its tables' columns and rows, each pipeline's state, and the receipts. A test runs
  twenty seeds twice and requires the same digests.
- **The simulation's own coverage.** `just sim-coverage` measures the engine and connector code
  the simulation alone reaches, leaving out test code and the connector's test kit and SQL
  planner. Over 1 000 seeds it reaches 83.6 % of lines and 75.6 % of branches, and the engine
  alone about 86 % of lines. The recipe holds a floor of 82 % and 73 %, and the nightly workflow
  runs it.
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
      and one `Notify`, which wakes waiters in the order they began to wait.
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
- The simulation reaches least of JSON shredding's rarer paths, temporal text, plan validation,
  state records other than cursors, and change streams, which the engine does not load yet.
- The nightly workflow runs longer: 200 stress seeds take about ten minutes, and the coverage run
  about as long.
