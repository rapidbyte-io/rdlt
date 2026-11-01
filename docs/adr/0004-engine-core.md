# ADR 0004: Engine core

Status: accepted, 2026-09-23.

## Context

The engine runs a pipeline in process: planning, segments, barriers, the commit coordinator,
fencing, the memory budget, lanes, write modes, schemas, retries and reports. It delivers every
stream exactly once through failed attempts, lost answers, crashes and concurrent runs, and what
a run reports and tells must show what it did without changing what it does.

## Decision

- **API.** `Engine::new(EngineConfig, Arc<dyn Env>)` and `Engine::run(PipelinePlan, source,
  destination) -> RunHandle`. The handle is a future; `control()` returns a `RunControl` that stops
  the run after committing or at once, and dropping the handle cancels every task the run started.
- **A run tells what happens as it happens.** `Env::observe` is told each `Event` where it becomes
  true: an attempt started, a commit landed with its counts, phases, discards by table, column and
  reason, and its report's outcome, a lost commit credited, a schema change the destination applied,
  a retention reset, partitions forgotten, a retry of the attempt or of a commit sent again in place
  with its error and delay, and the run's end. `SystemEnv::with_observer` hands them to the
  embedder's function; an environment that observes nothing (`Env::observes`) has no event built for
  it. The engine calls the observer on its own tasks, outside the locks of its state, and holds no
  event for it, so nothing waits outside the memory budget. A run's events are told one at a time
  under a lock of their own, which the run's end closes: a task the end left running tells nothing
  after it, and nothing it notes changes the status then. The observer must not block, panic or call
  into the run, and hands each event to a channel. A panic fails the attempt it was told in, as any
  panic of the engine's tasks does; one told between attempts or at the run's end reaches the task
  awaiting the run. Each list an event holds names its first 128 entries and counts the rest, and
  every text a connector chose is shown as error reports show it. Export is the embedder's: the
  engine renders no metrics.
- **A run's status can be pulled.** `RunHandle::status()` never waits. It names what the run is in
  the middle of and since when (opening, reading, a commit's barrier, flush, commit with how many
  times it was sent, or report, backing off until its next attempt, stopping, ended), its partitions
  reading, waiting for a slot and ended, the one quiet longest and since when, the oldest wait for
  room in each share of the budget, each lane's call into the destination in flight, each stream's
  last landed commit, lag and retention headroom, and the report so far, assembled as the run's end
  assembles it. The run writes it at attempt and commit-step boundaries; a partition's stamp and a
  lane's call are relaxed atomic stores, around each destination call and each message a partition
  sends but its writes. Its times are the run's start on the wall clock and the monotonic time
  since.
- **Cores.** A `Cores` is the count of cores a run may use. The compute pool gets a thread for
  each, through `RayonPool::try_new`, and nothing else sizes a pool. A process that builds its own
  tokio runtime gives it `Cores::runtime_workers`: half the cores, rounded down, and two at least
  where there are two, since one worker runs the engine's per-row work and every lane's writes
  alone. The pool's threads overlap the workers' cores: the workers run the per-row work and the
  writes, the pool shreds and lowers, and with a thread a core the pool can take every core the
  workers leave idle, which JSON workloads need. A run of plain Arrow leaves the pool's threads
  asleep: its cuts and lowerings read and write no value, so they run on the partition's task
  (ADR 0008), and only a load's first lowering, which builds its constant columns, reaches the
  pool. The runtime does not grow to match: more workers slow a run of one partition
  (`docs/perf/passthrough.md`). `Cores::try_from_host` and `SystemEnv::try_from_host` take the
  cores the process may run on (its affinity mask and cgroup quota honoured); an embedder that
  fixes a count passes `Cores::new` to `SystemEnv::try_new`. `Env::cores` returns the count, and lanes default to one per core. The
  engine reads the host's cores in `Cores::try_from_host` and nowhere else (clippy bans
  `available_parallelism` in the rest of `rdlt-engine`), and the simulation declares a fixed
  count, so a simulated run sizes itself alike on every machine.
- **One writer per table per lane.** A lane opens a table's writer when it first writes to the
  table, since normalized streams add child tables as their rows arrive. Each attempt routes a
  partition's batches for a table to lane `(place + table) mod lanes`, the place being the
  partition's index in the attempt. A partition read again takes its ended read's place, and a
  new one a place whose seals all committed, so a partition's writes to a table stay in order on
  one lane, and as many partitions as lanes take a lane each.
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
  later. Marking it `Done` always would stop incremental streams from ever growing.
- **Full reads are cycles.** A `full` read records its generation when it first commits and
  deletes the previous cycle's partition entries; its last commit records it as `completed`. Each
  run remembers the cycle it started or resumed per stream, and state keeps the stream's sixteen
  most recently completed cycles, so a retry after a landed final commit (a lost response, a
  failed acknowledgement) does not read the stream again, even when another run completed a cycle
  in between. Without this, `full` + `append` would append a second copy. `TableRef` carries the
  generation `replace` writes fill, and the contract has `StateKey::Completed`.
- **Every commit is keyed and expects the key landed before it.** A commit's key is its
  session's epoch and its place among the session's commits, from 1, and it names `after`, the
  key it expects the destination to hold landed: the open's landed key for a session's first
  commit, the commit before it for the rest. The destination keeps the landed key with its time,
  rows and bytes in the pipeline's fence row, which every commit reads under its write lock, and
  answers by one decision (`rdlt_connector::answer`): the landed key with its receipt, publishing
  nothing; a commit of another epoch fenced; one whose `after` is not the landed key fenced and
  coded `commit_stale`, publishing nothing; the rest applied, writing their key in the same
  transaction (clause `D-LANDED`). The open returns the landed key's receipt, so a run credits a
  commit whose answer was lost to the attempt that made it once the next open finds its key
  landed, and the report matches what the destination published; an attempt is named in the
  report by its epoch.
- **A commit the destination answers transiently is sent again in place.** A try the
  destination's connector answers `Transient` or `RateLimited`, with none of the host's codes, is
  sent again on the same session: the same commit, by the same key and expecting the same key
  landed. It is tried as many times at most as the run makes attempts, each wait drawn as the
  run's backoff draws it, a rate limit's own wait held to the policy's delays, and the session
  locked for each try alone, so writers open and schema changes apply between tries; partitions
  read on, and what they seal waits for the next commit. A lost connection, the host's deadline or
  the engine's wait for the call, a fence and a permanent error end the attempt, as does the last
  try: one session owns one connection, and after such an end nothing tells whether the commit
  applied, so the next open credits it by its landed key. A try answered transiently may still
  land later: the key landed answers its retry, and `after` refuses it once a later commit landed
  (clause `D-RETRY`). Without progress a commit is sent at most the square of the attempts, 25
  times at the default policy. The wait races the attempt's cancel, which ends the attempt, then a
  stop after commit, which ends it with the commit's error, so the run ends stopped as when a stop
  meets the wait between attempts. Once another task's failure cancels the attempt, a commit in
  flight has the close wait (60 s by default) to answer: one that lands then is in the report
  though no attempt may follow to find it landed, as when a refusal ends the run for good, and one
  that hangs ends the attempt at that wait rather than the connector wait; the commit's reports
  race the cancel. A landed commit goes uncounted only where its attempt ended without hearing it
  land and no later attempt opened to find it landed: in a run stopped at once (`StopMode::Now`),
  which opens nothing after, where it answered past the close wait, where the last try the retry
  policy allows was answered transiently though the commit landed (at one attempt, or on the
  run's last), or where its answer was lost.
- **The report's counters are written, never read.** What a run waited for and spent its time on
  is counted as it happens and summed into its report; no decision of the engine reads a
  counter, an event or a status, so measuring a run never changes what it does.
- **The budget is charged before a push enters its partition channel.** The contract's
  `admitted_partition_channel` waits for admission of each push's bytes, so data a source has
  handed over but the engine has not read stays within the budget.
- **One table per stream.** A stream's table is named after the stream, so a plan whose streams
  would share a table is refused.
- **The lattice never rounds.** `Int64 ∨ Float` joins to `Json`: `Float64` holds integers exactly
  only up to 2^53. `Int8`, `Int16` and `Int32` still join floats as `Float64`.
- **Tests fail instead of hanging.** Engine tests run on tokio's paused clock with time limits,
  and mutation testing uses a nextest profile whose time limits end a test still running long
  past what it takes unmutated, so a mutant that makes the engine spin fails its tests rather
  than timing out. The simulation suite builds with a `sim` profile (optimised, without
  release's fat LTO and single codegen unit, and with overflow checks and debug assertions
  kept); [docs/perf/sim.md](../perf/sim.md) records how fast it runs.
- **Reports** add `peak_memory`, the most of the memory budget the run reserved at once across
  all its shares, and `counters.kept_peak`, the most its reads kept at once beside their events,
  so budget accounting is observable.

## Consequences

The simulation oracle checks every stream mode through faults, crashes, stops and concurrent
runs. A source that ends with rows after its last checkpoint is read once; a well-behaved
incremental source checkpoints after its last row.
