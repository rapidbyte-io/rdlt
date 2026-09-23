# ADR 0004: Engine core

Status: accepted, 2026-09-23.

## Context

M2 of the spec is the in-process engine: planning, segments, barriers, the commit coordinator,
fencing, the memory budget, lanes, write modes, schemas, retries and reports. It is split in two:
M2a delivers the exactly-once core over streams with declared schemas; M2b adds merge, schema
evolution, name maps, nested data, `sqlgen` and the `sqlite` and `files` reference connectors.
Building M2a surfaced decisions the spec leaves open or gets wrong.

## Decision

- **API.** `Engine::new(EngineConfig, Arc<dyn Env>)` and `Engine::run(PipelinePlan, source,
  destination) -> RunHandle`. The handle is a future; `control()` returns a `RunControl` that stops
  the run after committing or at once, and dropping the handle cancels every task the run started.
  Events and `status()` arrive with the surface in M7.
- **One writer per table per lane.** Each attempt creates its writers before any partition reads,
  and routes a partition's batches for a table to one lane by an FNV hash, so they stay in order.
- **Barriers wait only for partitions that are reading.** A partition waiting for a read slot, or
  already ended, owes no answer, so many queued partitions never make every commit wait out
  `barrier_wait`.
- **Rows stay due until they commit.** A commit subtracts only the rows it published from the
  pending counts, so rows whose barrier went unanswered make the next commit due as soon as they
  seal, even with a rows-only policy.
- **End of partition.** A partition that reads to its end seals with its last cursor, so an
  incremental read resumes past it next run. Rows after the last checkpoint have no cursor that
  resumes past them, so committing them marks the partition `Done`. A partition that read nothing
  and never checkpointed records no position, so a stream that starts empty still loads rows added
  later. The spec said `Done` always, which would stop incremental streams from ever growing.
- **Full reads are cycles.** A `full` read records its generation when it first commits and
  deletes the previous cycle's partition entries; its last commit records it as `completed`. Each
  run remembers the cycle it started or resumed per stream, and state keeps the stream's sixteen
  most recently completed cycles, so a retry after a landed final commit (a lost response, a
  failed acknowledgement) does not read the stream again, even when another run completed a cycle
  in between. Without this, `full` + `append` appended a second copy. `TableRef` carries the
  generation `replace` writes fill, and the contract gains `StateKey::Completed`.
- **Every commit records its own receipt** in state. A run credits a commit whose response was lost
  to the attempt that made it once a later attempt reads that receipt back, so the report matches
  what the destination published.
- **The budget is charged before a push enters its partition channel.** The contract's
  `admitted_partition_channel` waits for admission of each push's bytes, so data a source has
  handed over but the engine has not read stays within the budget (spec §7.5).
- **One table per stream.** Until name maps (M2b), a stream's table is named after the stream, so
  a plan whose streams would share a table is refused.
- **Declared schemas and Arrow only.** Until the shredder (M3), a stream must declare its schema,
  JSON and change pushes are refused, and every batch must match the declared schema exactly.
  Until schema evolution (M2b), a committed schema that differs from the declared one is refused.
  Each stream loads one table named after the stream; naming arrives with name maps in M2b.
- **The lattice never rounds.** `Int64 ∨ Float` joins to `Json`: `Float64` holds integers exactly
  only up to 2^53. `Int8`, `Int16` and `Int32` still join floats as `Float64`.
- **Tests fail instead of hanging.** Engine tests run on tokio's paused clock with time limits,
  and mutation testing uses a nextest profile that terminates any test still running after ten
  seconds, so a mutant that makes the engine spin fails its tests rather than timing out. The
  simulation suite builds with a `sim` profile (optimised, overflow checks and debug assertions
  kept), which runs 10 000 seeds in about half a minute.
- **Reports** add `peak_memory`, the most in-flight bytes the run held, so budget accounting is
  observable.
- **Deferred:** the loom model of the memory budget (M9); the configuration size limit, which
  needs the byte boundary of documents (M7) and the wire (M4); the simulator's JSON, change,
  manifest and WAL workloads, which arrive with those features.

## Consequences

The simulation oracle checks every stream mode M2a supports through faults, crashes, stops and
concurrent runs. A source that ends with rows after its last checkpoint is read once; a
well-behaved incremental source checkpoints after its last row.
