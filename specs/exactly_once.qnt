// Exactly-once delivery with the write-ahead log and acknowledged positions, as ADRs 0004, 0029
// and 0030 decide it. Each run of a pipeline opens a log of its own where a partition's source
// cannot read again, then fences and replays every other log of its pipeline, in a session it
// opens for the first commit it replays: a logged commit with no receipt is committed again whole
// where the destination stands where its load left it, and otherwise only its seals of partitions
// that cannot read again, each where its partition stands where the seal starts. It then opens its
// own session, which fences every session of the pipeline opened before, reads each partition
// from where the destination holds it, checkpoints, and commits what it sealed: logged first, the
// source that cannot read again told the commit's position once the log holds it, then the
// destination's commit, keyed by load and number, under the session's epoch; the source that reads
// again is told after. A run may crash, its log may refuse a commit, its commit may fail or land
// with its answer lost, a telling may fail, and runs may start while another runs. A second
// pipeline may share the destination.
//
// quint steps: --step simulated --max-steps 60
// quint run: --max-samples 2000
module exactly_once_protocol {
  // ---- What a destination's pipelines read

  /// The pipelines sharing the destination.
  const PIPES: Set[str]
  /// The partitions they read.
  const PARTS: Set[str]
  /// The pipeline reading each partition.
  const OWNER: str -> str
  /// The partitions whose source reads again; every other forgets what it was told is committed.
  const REREADS: Set[str]
  /// The rows each partition holds, from offset 0.
  const ROWS: str -> int
  /// The runs each pipeline may start, its loads 1 onward.
  const LOADS: int
  /// Faults a behavior meets: crashes, commits a log refused, failed or lost commits, failed
  /// tellings, and runs started beside a running one.
  const FAULTS: int

  type Pipe = str
  type Part = str

  pure def ownerOf(part: Part): Pipe = OWNER.get(part)

  pure def rereads(part: Part): bool = REREADS.contains(part)

  pure def rowsOf(part: Part): int = ROWS.get(part)

  pure def partsOf(pipe: Pipe): Set[Part] = PARTS.filter(part => ownerOf(part) == pipe)

  /// Whether a pipeline keeps a log: a partition of it cannot read again.
  pure def logsAhead(pipe: Pipe): bool = partsOf(pipe).exists(part => not(rereads(part)))

  // ---- What the destination, the logs and the sources hold

  /// A commit's key: its load and its number; (0, 0) is none.
  type Key = (int, int)

  pure val NONE: Key = (0, 0)

  /// A segment: a partition's rows from one offset to the next.
  type Seg = { part: Part, from: int, to: int }

  /// A commit: whose, its key, the epoch of the session making it, its segments, and the commit
  /// the destination last received before it, as its load knew it.
  type Commit = { pipe: Pipe, key: Key, epoch: int, segs: Set[Seg], prev: Key }

  pure val NOCOMMIT: Commit = { pipe: "", key: NONE, epoch: 0, segs: Set(), prev: NONE }

  /// A load's log: open until removed, fenced once a replay took it, and its commits.
  type Log = { open: bool, fenced: bool, commits: Set[Commit] }

  pure val NOLOG: Log = { open: false, fenced: false, commits: Set() }

  /// What the destination holds of a pipeline: its newest session's epoch, each partition's
  /// position and how often each row of it is held, each commit's receipt, and the last commit
  /// whose state it holds.
  type Dest = { epoch: int, cursor: Part -> int, rows: Part -> (int -> int), receipts: Set[Key], last: Key }

  /// Where a run stands: replaying other loads' logs, reading, its commit logged, the source
  /// that cannot read again told, its commit landed.
  type Stage = | Replaying | Reading | Logged | Told | Landed

  /// A run: its pipeline and load, the epoch of the session it holds (0 before its first), where
  /// it stands, each partition's rows read, sealed by a checkpoint and committed, its commits'
  /// count, the commit in flight, and the commit the destination last received as its log opened.
  type Run = {
    pipe: Pipe,
    load: int,
    epoch: int,
    stage: Stage,
    read: Part -> int,
    sealed: Part -> int,
    committed: Part -> int,
    seq: int,
    inFlight: Commit,
    opened: Key,
  }

  /// The destination, by pipeline.
  var dest: Pipe -> Dest
  /// Each load's log, by pipeline and load.
  var logs: (Pipe, int) -> Log
  /// Where each partition's source stands: the position it was last told is committed.
  var kept: Part -> int
  /// The runs running, by pipeline and load.
  var runs: (Pipe, int) -> Run
  /// The runs each pipeline started.
  var started: Pipe -> int
  /// Faults left to meet.
  var faults: int
  /// Each position a partition's commit landed at the destination with.
  var landed: Set[(Part, int)]
  /// Whether a commit landed from a session the destination had fenced.
  var fencedLanded: bool

  // ---- Helpers

  pure def zero(parts: Set[Part]): Part -> int = parts.mapBy(_ => 0)

  pure def none(part: Part): int -> int = 0.to(rowsOf(part) - 1).mapBy(_ => 0)

  pure def fresh(pipe: Pipe): Dest =
    { epoch: 0, cursor: zero(partsOf(pipe)), rows: partsOf(pipe).mapBy(part => none(part)), receipts: Set(), last: NONE }

  /// The destination `d` once it applied `segs` of the commit `key`, the commit's state included
  /// where `whole`.
  pure def applied(d: Dest, key: Key, segs: Set[Seg], whole: bool): Dest = {
    val rows = segs.fold(d.rows, (rs, s) =>
      rs.set(s.part, s.from.to(s.to - 1).fold(rs.get(s.part), (counts, o) => counts.set(o, counts.get(o) + 1))))
    val cursor = segs.fold(d.cursor, (c, s) => c.set(s.part, s.to))
    { ...d, rows: rows, cursor: cursor, receipts: d.receipts.union(Set(key)), last: if (whole) key else d.last }
  }

  /// The positions `segs` land with.
  pure def positions(segs: Set[Seg]): Set[(Part, int)] = segs.map(s => (s.part, s.to))

  /// What replaying the logged commit `c` stages where the destination holds `d`: the whole
  /// commit where the destination stands as its load left it; otherwise its seals of partitions
  /// that cannot read again, each where its partition stands where the seal starts.
  pure def decided(d: Dest, c: Commit): Set[Seg] =
    if (d.last == c.prev) c.segs
    else c.segs.filter(s => not(rereads(s.part)) and d.cursor.get(s.part) == s.from)

  /// The open logs of `pipe` other than `load`'s.
  def others(pipe: Pipe, load: int): Set[int] =
    1.to(started.get(pipe)).filter(l => l != load and logs.keys().contains((pipe, l)) and logs.get((pipe, l)).open)

  /// The least of `ns`; -1 where there is none.
  pure def least(ns: Set[int]): int = ns.fold(-1, (best, n) => if (best == -1 or n < best) n else best)

  /// The highest position a log still open holds a commit of `part` at.
  def logged(part: Part): int =
    logs.keys().filter(k => logs.get(k).open).fold(0, (best, k) =>
      logs.get(k).commits.fold(best, (b, c) =>
        c.segs.filter(s => s.part == part).fold(b, (bb, s) => if (s.to > bb) s.to else bb)))

  /// Whether a log still open holds a commit of `part` ending at `pos`.
  def heldAt(part: Part, pos: int): bool =
    logs.keys().exists(k => logs.get(k).open and logs.get(k).commits.exists(c =>
      c.segs.exists(s => s.part == part and s.to == pos)))

  /// The runs running.
  val live: Set[(Pipe, int)] = runs.keys()

  def runOf(k: (Pipe, int)): Run = runs.get(k)

  /// The next state: every variable as given.
  action keepAll(
    d: Pipe -> Dest, l: (Pipe, int) -> Log, k: Part -> int, r: (Pipe, int) -> Run, s: Pipe -> int, f: int,
    landedNow: Set[(Part, int)], fenced: bool
  ): bool = all {
    dest' = d,
    logs' = l,
    kept' = k,
    runs' = r,
    started' = s,
    faults' = f,
    landed' = landedNow,
    fencedLanded' = fenced,
  }

  /// The runs without `k`.
  def without(k: (Pipe, int)): (Pipe, int) -> Run = runs.keys().exclude(Set(k)).mapBy(x => runs.get(x))

  // ---- Actions

  action init = all {
    dest' = PIPES.mapBy(pipe => fresh(pipe)),
    logs' = Map(),
    kept' = zero(PARTS),
    runs' = Map(),
    started' = PIPES.mapBy(_ => 0),
    faults' = FAULTS,
    landed' = Set(),
    fencedLanded' = false,
  }

  /// A run of `pipe` starts: its log opens, noting the commit the destination last received; it
  /// holds no session yet. A run started while another of the pipeline runs meets a fault.
  action start(pipe: Pipe): bool = {
    val load = started.get(pipe) + 1
    val concurrent = runs.keys().exists(k => k._1 == pipe)
    val d = dest.get(pipe)
    val job = {
      pipe: pipe, load: load, epoch: 0, stage: Replaying,
      read: zero(partsOf(pipe)), sealed: zero(partsOf(pipe)), committed: zero(partsOf(pipe)),
      seq: 0, inFlight: NOCOMMIT, opened: d.last,
    }
    all {
      load <= LOADS,
      not(concurrent) or faults > 0,
      keepAll(
        dest,
        if (logsAhead(pipe)) logs.put((pipe, load), { ...NOLOG, open: true }) else logs,
        kept,
        runs.put((pipe, load), job),
        started.set(pipe, load),
        if (concurrent) faults - 1 else faults,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The run `k` takes its next step at replaying the oldest other open log: fencing it, then
  /// committing again each commit it holds with no receipt, meeting `outcome`, in a session it
  /// opens for the first, then removing it; with none left, it opens its own session, which fences
  /// every older one, and plans from where the destination holds each partition.
  action replay(k: (Pipe, int), outcome: Outcome): bool = {
    val job = runOf(k)
    val d = dest.get(job.pipe)
    val left = others(job.pipe, job.load)
    val l = least(left)
    val log = logs.get((job.pipe, l))
    val unreceipted = log.commits.filter(c => not(d.receipts.contains(c.key)))
    val c = unreceipted.fold(NOCOMMIT, (best, x) => if (best == NOCOMMIT or x.key._2 < best.key._2) x else best)
    val segs = decided(d, c)
    all {
      live.contains(k),
      job.stage == Replaying,
      if (left == Set()) {
        // A source that cannot read again refuses a read from before where it stands, and the
        // run ends.
        val planned = partsOf(job.pipe).forall(part => rereads(part) or kept.get(part) <= d.cursor.get(part))
        val at = d.cursor
        val epoch = d.epoch + 1
        all {
          outcome == Ok,
          keepAll(
            dest.set(job.pipe, { ...d, epoch: epoch }), logs, kept,
            if (planned) runs.set(k, { ...job, epoch: epoch, stage: Reading, read: at, sealed: at, committed: at })
            else without(k),
            started, faults, landed, fencedLanded,
          ),
        }
      }
      else if (not(log.fenced)) all {
        outcome == Ok,
        keepAll(dest, logs.set((job.pipe, l), { ...log, fenced: true }), kept, runs, started, faults, landed, fencedLanded),
      }
      else if (unreceipted == Set()) all {
        outcome == Ok,
        keepAll(dest, logs.set((job.pipe, l), NOLOG), kept, runs, started, faults, landed, fencedLanded),
      }
      // The replay's session opens as its first commit needs it.
      else if (job.epoch == 0) all {
        outcome == Ok,
        keepAll(
          dest.set(job.pipe, { ...d, epoch: d.epoch + 1 }), logs, kept,
          runs.set(k, { ...job, epoch: d.epoch + 1 }), started, faults, landed, fencedLanded,
        ),
      }
      else committing(k, job, d, c.key, segs, d.last == c.prev, outcome),
    }
  }

  /// The run `k` commits `segs` of the commit `key` at the destination, which holds `d`, meeting
  /// `outcome`: a session the destination fenced is refused; a commit it holds the receipt of
  /// answers with it; otherwise it lands, but where it failed, and its answer is lost where it
  /// landed so.
  action committing(k: (Pipe, int), job: Run, d: Dest, key: Key, segs: Set[Seg], whole: bool, outcome: Outcome): bool = {
    val fenced = d.epoch != job.epoch
    val held = d.receipts.contains(key)
    val lands = not(fenced) and not(held) and outcome != Failed
    val answered = not(fenced) and (held or outcome == Ok)
    all {
      Set(Ok, Failed, Lost).contains(outcome),
      outcome == Ok or faults > 0,
      keepAll(
        if (lands) dest.set(job.pipe, applied(d, key, segs, whole)) else dest,
        logs, kept,
        if (answered) runs.set(k, { ...job, stage: if (job.stage == Replaying) Replaying else Landed }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        if (lands) landed.union(positions(segs)) else landed,
        fencedLanded or (lands and fenced),
      ),
    }
  }

  /// The run `k` reads the next row of `part`.
  action read(k: (Pipe, int), part: Part): bool = {
    val job = runOf(k)
    all {
      live.contains(k),
      job.stage == Reading,
      partsOf(job.pipe).contains(part),
      job.read.get(part) < rowsOf(part),
      keepAll(dest, logs, kept, runs.set(k, { ...job, read: job.read.set(part, job.read.get(part) + 1) }), started, faults, landed, fencedLanded),
    }
  }

  /// The run `k` checkpoints `part` where it read it: the rows read so far are sealed.
  action checkpoint(k: (Pipe, int), part: Part): bool = {
    val job = runOf(k)
    all {
      live.contains(k),
      job.stage == Reading,
      partsOf(job.pipe).contains(part),
      job.sealed.get(part) < job.read.get(part),
      keepAll(dest, logs, kept, runs.set(k, { ...job, sealed: job.sealed.set(part, job.read.get(part)) }), started, faults, landed, fencedLanded),
    }
  }

  /// The run `k` commits what it sealed: its log takes the commit first, which a log a replay
  /// fenced refuses, meeting `outcome`; a pipeline with no log goes straight on.
  action log(k: (Pipe, int), outcome: Outcome): bool = {
    val job = runOf(k)
    val segs = partsOf(job.pipe).filter(part => job.sealed.get(part) > job.committed.get(part))
                                .map(part => { part: part, from: job.committed.get(part), to: job.sealed.get(part) })
    val key = (job.load, job.seq + 1)
    val c = { pipe: job.pipe, key: key, epoch: job.epoch, segs: segs, prev: if (job.seq == 0) job.opened else (job.load, job.seq) }
    val own = if (logsAhead(job.pipe)) logs.get((job.pipe, job.load)) else NOLOG
    val refused = logsAhead(job.pipe) and (own.fenced or not(own.open))
    val takes = not(refused) and outcome == Ok
    all {
      live.contains(k),
      job.stage == Reading,
      segs != Set(),
      Set(Ok, Failed).contains(outcome),
      outcome == Ok or faults > 0,
      keepAll(
        dest,
        if (takes and logsAhead(job.pipe)) logs.set((job.pipe, job.load), { ...own, commits: own.commits.union(Set(c)) }) else logs,
        kept,
        if (takes) runs.set(k, { ...job, stage: if (logsAhead(job.pipe)) Logged else Told, inFlight: c }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The run `k` tells the source of a partition that cannot read again its commit's position,
  /// the log holding the commit, meeting `outcome`: a telling that fails may have moved the
  /// source all the same.
  action tellLogged(k: (Pipe, int), outcome: Outcome): bool = {
    val job = runOf(k)
    val told = job.inFlight.segs.filter(s => not(rereads(s.part))).fold(kept, (kk, s) => kk.set(s.part, s.to))
    all {
      live.contains(k),
      job.stage == Logged,
      Set(Ok, Failed, Lost).contains(outcome),
      outcome == Ok or faults > 0,
      keepAll(
        dest, logs,
        if (outcome == Failed) kept else told,
        if (outcome == Ok) runs.set(k, { ...job, stage: Told }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The run `k` commits at the destination.
  action land(k: (Pipe, int), outcome: Outcome): bool = {
    val job = runOf(k)
    all {
      live.contains(k),
      job.stage == Told,
      committing(k, job, dest.get(job.pipe), job.inFlight.key, job.inFlight.segs, true, outcome),
    }
  }

  /// The run `k` tells the sources of partitions that read again the positions its commit
  /// landed with, meeting `outcome`, and reads on.
  action tellLanded(k: (Pipe, int), outcome: Outcome): bool = {
    val job = runOf(k)
    val told = job.inFlight.segs.filter(s => rereads(s.part)).fold(kept, (kk, s) => kk.set(s.part, s.to))
    all {
      live.contains(k),
      job.stage == Landed,
      Set(Ok, Failed, Lost).contains(outcome),
      outcome == Ok or faults > 0,
      keepAll(
        dest, logs,
        if (outcome == Failed) kept else told,
        if (outcome == Ok) runs.set(k, { ...job, stage: Reading, committed: job.sealed, seq: job.seq + 1, inFlight: NOCOMMIT }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The run `k` read every partition to its end and committed all of it: it ends, its log
  /// removed, every commit in it holding its receipt.
  action close(k: (Pipe, int)): bool = {
    val job = runOf(k)
    all {
      live.contains(k),
      job.stage == Reading,
      partsOf(job.pipe).forall(part => job.read.get(part) == rowsOf(part) and job.committed.get(part) == rowsOf(part)),
      keepAll(
        dest,
        if (logsAhead(job.pipe)) logs.set((job.pipe, job.load), NOLOG) else logs,
        kept, without(k), started, faults, landed, fencedLanded,
      ),
    }
  }

  /// The run `k` crashes: what it read and staged is lost; its log and what landed stay.
  action crash(k: (Pipe, int)): bool = all {
    live.contains(k),
    faults > 0,
    keepAll(dest, logs, kept, without(k), started, faults - 1, landed, fencedLanded),
  }

  /// What becomes of a commit or a telling: done and answered; failed, nothing done; or done
  /// with its answer lost.
  type Outcome = | Ok | Failed | Lost

  pure val OUTCOMES: Set[Outcome] = Set(Ok, Failed, Lost)

  /// What a step may meet: anything while faults are left, success alone after.
  val outcomes: Set[Outcome] = if (faults > 0) OUTCOMES else Set(Ok)

  action step = any {
    all {
      PIPES.exists(x => started.get(x) < LOADS),
      nondet pipe = PIPES.filter(x => started.get(x) < LOADS).oneOf()
      start(pipe),
    },
    all {
      live != Set(),
      nondet k = live.oneOf()
      nondet outcome = outcomes.oneOf()
      nondet part = PARTS.oneOf()
      any {
        replay(k, outcome),
        read(k, part),
        checkpoint(k, part),
        log(k, outcome),
        tellLogged(k, outcome),
        land(k, outcome),
        tellLanded(k, outcome),
        close(k),
        crash(k),
      },
    },
  }

  /// The steps `quint run` takes: `step`'s, with faults seldom, so a run of sixty steps reaches
  /// every partition's end.
  action simulated = {
    nondet rare = 1.to(6).oneOf()
    any {
      all {
        PIPES.exists(x => started.get(x) < LOADS),
        nondet pipe = PIPES.filter(x => started.get(x) < LOADS).oneOf()
        all { rare == 1 or runs.keys().forall(k => k._1 != pipe), start(pipe) },
      },
      all {
        live != Set(),
        nondet k = live.oneOf()
        nondet outcome = (if (rare == 1) outcomes else Set(Ok)).oneOf()
        nondet part = PARTS.oneOf()
        any {
          replay(k, outcome),
          read(k, part),
          checkpoint(k, part),
          log(k, outcome),
          tellLogged(k, outcome),
          land(k, outcome),
          tellLanded(k, outcome),
          close(k),
          all { rare == 1, crash(k) },
        },
      },
    }
  }

  // ---- Invariants, named as the simulation's checks where one checks the same

  /// Every row a partition's position lies past is held once, and none past it.
  val ExactlyOnce = PIPES.forall(pipe => partsOf(pipe).forall(part => {
    val d = dest.get(pipe)
    0.to(rowsOf(part) - 1).forall(o => d.rows.get(part).get(o) == (if (o < d.cursor.get(part)) 1 else 0))
  }))

  /// A source is told a position only once it is safe: a source that reads again once its
  /// commit landed, and one that cannot once a log holds the commit until it lands.
  val SAck = PARTS.filter(part => PIPES.contains(ownerOf(part))).forall(part => {
    val at = dest.get(ownerOf(part)).cursor.get(part)
    if (rereads(part)) kept.get(part) <= at
    else kept.get(part) <= (if (logged(part) > at) logged(part) else at)
  })

  /// No commit lands from a session the destination fenced.
  val FencedNeverCommits = not(fencedLanded)

  /// A source's position moves only as it is told a committed position, and then to that one: a
  /// position a commit landed with, or one a log holds.
  val PositionOnlyInCommitted = PARTS.forall(part =>
    kept.get(part) == 0 or landed.contains((part, kept.get(part))) or heldAt(part, kept.get(part)))

  /// Every pipeline holds every row of each of its partitions, and no log is left.
  val converged = PIPES.forall(pipe => partsOf(pipe).forall(part => dest.get(pipe).cursor.get(part) == rowsOf(part)))
    and logs.keys().forall(k => not(logs.get(k).open))

  /// Every variable.
  val vars = (dest, logs, kept, runs, started, faults, landed, fencedLanded)

  /// Once faults stop, the load converges: every row lands once, and no log is left.
  temporal Converges = weakFair(step, vars).implies(eventually(converged))
}

/// The protocol at the bounds every checker explores: the first pipeline reads `a`, whose source
/// reads again, and `u`, whose source does not, two rows each; the second reads `b`, one row.
module exactly_once {
  import exactly_once_protocol(
    PIPES = Set("p", "q"),
    PARTS = Set("a", "u", "b"),
    OWNER = Map("a" -> "p", "u" -> "p", "b" -> "q"),
    REREADS = Set("a", "b"),
    ROWS = Map("a" -> 2, "u" -> 2, "b" -> 1),
    LOADS = 4,
    FAULTS = 2,
  ).*

  // ---- Tests

  /// The first pipeline's run `load`.
  pure def p(load: int): (Pipe, int) = ("p", load)

  /// The steps a run of the first pipeline takes to read, seal and commit a row of each of its
  /// partitions, everything answered.
  action rowEach(load: int): bool =
    read(p(load), "a").then(checkpoint(p(load), "a")).then(read(p(load), "u")).then(checkpoint(p(load), "u"))
      .then(log(p(load), Ok)).then(tellLogged(p(load), Ok)).then(land(p(load), Ok)).then(tellLanded(p(load), Ok))

  /// A source told a position before the log holds the commit, which a run never does.
  action toldEarly(k: (Pipe, int)): bool = {
    val job = runOf(k)
    all {
      live.contains(k),
      job.stage == Reading,
      keepAll(dest, logs, kept.set("u", job.sealed.get("u")), runs, started, faults, landed, fencedLanded),
    }
  }

  /// A destination that lands a commit whatever its session's epoch, which it never does.
  action landedFenced(k: (Pipe, int)): bool = {
    val job = runOf(k)
    val d = dest.get(job.pipe)
    all {
      live.contains(k),
      job.stage == Told,
      keepAll(
        dest.set(job.pipe, applied(d, job.inFlight.key, job.inFlight.segs, true)),
        logs, kept, runs.set(k, { ...job, stage: Landed }), started, faults,
        landed.union(positions(job.inFlight.segs)),
        fencedLanded or d.epoch != job.epoch,
      ),
    }
  }

  /// A replay that commits a logged commit again though the destination holds its receipt, and
  /// whole, which a replay never does.
  action replayedAgain(k: (Pipe, int), l: int): bool = {
    val job = runOf(k)
    val d = dest.get(job.pipe)
    val c = logs.get((job.pipe, l)).commits.fold(NOCOMMIT, (_, x) => x)
    all {
      live.contains(k),
      job.stage == Replaying,
      keepAll(dest.set(job.pipe, applied(d, c.key, c.segs, true)), logs, kept, runs, started, faults,
              landed.union(positions(c.segs)), fencedLanded),
    }
  }

  /// A source that moves to where it was read, not where it was told, which a source that
  /// passes certification never does.
  action readMoving(k: (Pipe, int), part: Part): bool = {
    val job = runOf(k)
    all {
      live.contains(k),
      job.stage == Reading,
      job.read.get(part) < rowsOf(part),
      keepAll(dest, logs, kept.set(part, job.read.get(part) + 1),
              runs.set(k, { ...job, read: job.read.set(part, job.read.get(part) + 1) }), started, faults, landed, fencedLanded),
    }
  }

  /// A source that stands at `pos` of `part` with no commit having given it that position.
  action movedTo(part: Part, pos: int): bool =
    keepAll(dest, logs, kept.set(part, pos), runs, started, faults, landed, fencedLanded)

  /// A run reads each partition to its end and commits it a row at a time, then ends with its log
  /// removed.
  run aRunCommitsEveryRowOnceAndEndsTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(rowEach(1))
      .then(rowEach(1))
      .then(close(p(1)))
      .expect(all {
        dest.get("p").cursor == Map("a" -> 2, "u" -> 2),
        kept == Map("a" -> 2, "u" -> 2, "b" -> 0),
        logs.keys().forall(k => not(logs.get(k).open)),
        ExactlyOnce,
        SAck,
        PositionOnlyInCommitted,
      })

  /// A commit the log holds, whose source was told and whose run crashed before it landed, is
  /// committed by the next run's replay, whole, and its rows land once.
  run aLoggedCommitIsReplayedAfterACrashTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(log(p(1), Ok))
      .then(tellLogged(p(1), Ok))
      .expect(kept.get("u") == 1 and dest.get("p").cursor.get("u") == 0 and SAck)
      .then(crash(p(1)))
      .then(start("p"))
      .then(5.reps(_ => replay(p(2), Ok)))
      .expect(all {
        dest.get("p").cursor.get("u") == 1,
        dest.get("p").receipts == Set((1, 1)),
        not(logs.get(("p", 1)).open),
        runOf(p(2)).stage == Reading,
        runOf(p(2)).read.get("u") == 1,
        ExactlyOnce,
        SAck,
      })

  /// A commit that landed with its answer lost holds its receipt, so the next run's replay lands
  /// nothing again.
  run aCommitWhoseAnswerWasLostLandsOnceTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(log(p(1), Ok))
      .then(tellLogged(p(1), Ok))
      .then(land(p(1), Lost))
      .expect(not(live.contains(p(1))) and dest.get("p").cursor.get("u") == 1)
      .then(start("p"))
      .then(3.reps(_ => replay(p(2), Ok)))
      .expect(dest.get("p").rows.get("u").get(0) == 1 and runOf(p(2)).stage == Reading and ExactlyOnce)

  /// A run started beside a running one fences its log, then its session as the replay's opens:
  /// the older run's commit is refused at the destination, and the newer run's replay lands what
  /// the older one logged.
  run aFencedRunNeverCommitsTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "a"))
      .then(checkpoint(p(1), "a"))
      .then(log(p(1), Ok))
      .then(tellLogged(p(1), Ok))
      .then(start("p"))
      .then(2.reps(_ => replay(p(2), Ok)))
      .then(land(p(1), Ok))
      .expect(not(live.contains(p(1))) and dest.get("p").cursor.get("a") == 0 and FencedNeverCommits)
      .then(3.reps(_ => replay(p(2), Ok)))
      .expect(dest.get("p").cursor.get("a") == 1 and runOf(p(2)).stage == Reading and ExactlyOnce)

  /// A run of the second pipeline opens its own session: the first pipeline's run, opened before,
  /// still commits.
  run aSecondPipelineFencesNoneOfTheFirstsTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(start("q"))
      .then(replay(("q", 1), Ok))
      .then(rowEach(1))
      .then(read(("q", 1), "b"))
      .then(checkpoint(("q", 1), "b"))
      .then(log(("q", 1), Ok))
      .then(land(("q", 1), Ok))
      .then(tellLanded(("q", 1), Ok))
      .expect(all {
        dest.get("p").cursor == Map("a" -> 1, "u" -> 1),
        dest.get("q").cursor == Map("b" -> 1),
        ExactlyOnce,
        FencedNeverCommits,
      })

  /// A source told a position before the log held its commit would stand past every copy of the
  /// rows: the invariant refuses it.
  run aSourceToldBeforeTheLogWouldStandPastTheRowsTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(toldEarly(p(1)))
      .expect(not(SAck))

  /// A destination that took a fenced session's commit would land it: the invariant refuses it.
  run aDestinationTakingAFencedCommitWouldLandItTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "a"))
      .then(checkpoint(p(1), "a"))
      .then(log(p(1), Ok))
      .then(tellLogged(p(1), Ok))
      .then(start("p"))
      .then(2.reps(_ => replay(p(2), Ok)))
      .then(landedFenced(p(1)))
      .expect(not(FencedNeverCommits))

  /// A replay that committed again a commit whose receipt the destination holds would hold its
  /// rows twice: the invariant refuses it.
  run aReplayIgnoringItsReceiptWouldHoldRowsTwiceTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(log(p(1), Ok))
      .then(tellLogged(p(1), Ok))
      .then(land(p(1), Lost))
      .then(start("p"))
      .then(replay(p(2), Ok))
      .then(replayedAgain(p(2), 1))
      .expect(not(ExactlyOnce))

  /// A source that moved to where it was read would stand where no commit put it: the invariant
  /// refuses it.
  run aSourceMovingAsItReadsWouldStandWhereNoCommitPutItTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(readMoving(p(1), "u"))
      .expect(not(PositionOnlyInCommitted))

  /// A position below the end of a commit a log holds is still one no commit gave: the invariant
  /// refuses it.
  run aSourceBetweenTheEndsOfALoggedCommitWouldStandWhereNoCommitPutItTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "u"))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(log(p(1), Ok))
      .expect(PositionOnlyInCommitted)
      .then(movedTo("u", 1))
      .expect(not(PositionOnlyInCommitted))
}
