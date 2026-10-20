// Exactly-once delivery with the write-ahead log and acknowledged positions, as ADRs 0004, 0029 and
// 0030 decide it. Each attempt of a pipeline, its load, opens a log of its own where a partition's
// source cannot read again, then fences and replays every other log of its pipeline, in a session
// it opens for the first commit it replays: a logged commit with no receipt is committed again
// whole where the destination stands where its load left it, and otherwise only its seals of
// partitions that cannot read again, each where its partition stands where the seal starts. It then
// opens its own session, which fences every session of the pipeline opened before, reads each
// partition from where the destination holds it, checkpoints, and commits what it sealed: logged
// first, the source that cannot read again told the commit's position once the log holds it, then
// the destination's commit, keyed by load and number, under the session's epoch; the source that
// reads again is told after. An attempt may crash, its log may refuse a commit, its commit may fail
// or land with its answer lost, a telling may fail, and an attempt may start while another of its
// pipeline is live, as a second run's would. A retry is an attempt of its own: the model does not
// group attempts into runs. The destination may be shared by several pipelines: `quint test` and
// `quint run` take two, and the model checkers one.
//
// quint main: exactly_once exactly_once_pair
// quint steps: --step simulated --max-steps 60
// quint run: --max-samples 2000
// quint verify: --max-steps 7
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
  /// At least the most rows a partition holds; a range over rows is bounded by it, as Apalache
  /// takes a range only between constants.
  const MAXROWS: int
  pure val rowsFit = PARTS.forall(part => ROWS.get(part) <= MAXROWS)
  assume RowsWithinMax = rowsFit
  /// The attempts each pipeline may make, its loads 1 onward.
  const LOADS: int
  /// Faults a behavior meets: crashes, commits a log refused, failed or lost commits, failed
  /// tellings, and attempts started beside a live one.
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

  /// Where an attempt stands: replaying other loads' logs, reading, its commit logged, the source
  /// that cannot read again told, its commit landed.
  type Stage = | Replaying | Reading | Logged | Told | Landed

  /// An attempt: its pipeline and load, the epoch of the session it holds (0 before its first),
  /// where it stands, each partition's rows read, sealed by a checkpoint and committed, its
  /// commits' count, the commit in flight, and the commit the destination last received as its own
  /// session opened.
  type Attempt = {
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
  /// The attempts live, by pipeline and load.
  var attempts: (Pipe, int) -> Attempt
  /// The attempts each pipeline started.
  var started: Pipe -> int
  /// Faults left to meet.
  var faults: int
  /// Each position a partition's commit landed at the destination with.
  var landed: Set[(Part, int)]
  /// Whether a commit landed from a session the destination had fenced.
  var fencedLanded: bool

  // ---- Helpers

  pure def zero(parts: Set[Part]): Part -> int = parts.mapBy(_ => 0)

  pure def none(part: Part): int -> int = 0.to(MAXROWS - 1).filter(o => o < rowsOf(part)).mapBy(_ => 0)

  pure def fresh(pipe: Pipe): Dest =
    { epoch: 0, cursor: zero(partsOf(pipe)), rows: partsOf(pipe).mapBy(part => none(part)), receipts: Set(), last: NONE }

  /// The destination `d` once it applied `segs` of the commit `key`, the commit's state included
  /// where `whole`.
  pure def applied(d: Dest, key: Key, segs: Set[Seg], whole: bool): Dest = {
    val rows = segs.fold(d.rows, (rs, s) =>
      rs.set(s.part, 0.to(MAXROWS - 1).filter(o => s.from <= o and o < s.to).fold(rs.get(s.part), (counts, o) => counts.set(o, counts.get(o) + 1))))
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
    1.to(LOADS).filter(l => l <= started.get(pipe) and l != load and logs.keys().contains((pipe, l)) and logs.get((pipe, l)).open)

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

  /// The attempts live.
  val live: Set[(Pipe, int)] = attempts.keys()

  def attemptOf(k: (Pipe, int)): Attempt = attempts.get(k)

  /// The next state: every variable as given.
  action keepAll(
    d: Pipe -> Dest, l: (Pipe, int) -> Log, k: Part -> int, r: (Pipe, int) -> Attempt, s: Pipe -> int, f: int,
    landedNow: Set[(Part, int)], fenced: bool
  ): bool = all {
    dest' = d,
    logs' = l,
    kept' = k,
    attempts' = r,
    started' = s,
    faults' = f,
    landed' = landedNow,
    fencedLanded' = fenced,
  }

  /// The attempts without `k`.
  def without(k: (Pipe, int)): (Pipe, int) -> Attempt = attempts.keys().exclude(Set(k)).mapBy(x => attempts.get(x))

  // ---- Actions

  action init = all {
    dest' = PIPES.mapBy(pipe => fresh(pipe)),
    logs' = Map(),
    kept' = zero(PARTS),
    attempts' = Map(),
    started' = PIPES.mapBy(_ => 0),
    faults' = FAULTS,
    landed' = Set(),
    fencedLanded' = false,
  }

  /// An attempt of `pipe` starts: its log opens; it holds no session yet. An attempt started while
  /// another of the pipeline is live meets a fault.
  action start(pipe: Pipe): bool = {
    val load = started.get(pipe) + 1
    val concurrent = attempts.keys().exists(k => k._1 == pipe)
    val attempt = {
      pipe: pipe, load: load, epoch: 0, stage: Replaying,
      read: zero(partsOf(pipe)), sealed: zero(partsOf(pipe)), committed: zero(partsOf(pipe)),
      seq: 0, inFlight: NOCOMMIT, opened: NONE,
    }
    all {
      load <= LOADS,
      not(concurrent) or faults > 0,
      keepAll(
        dest,
        if (logsAhead(pipe)) logs.put((pipe, load), { ...NOLOG, open: true }) else logs,
        kept,
        attempts.put((pipe, load), attempt),
        started.set(pipe, load),
        if (concurrent) faults - 1 else faults,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The attempt `k` takes its next step at replaying the oldest other open log: fencing it, then
  /// committing again each commit it holds with no receipt, meeting `outcome`, in a session it
  /// opens for the first, then removing it; with none left, it opens its own session, which fences
  /// every older one, and plans from where the destination holds each partition and the commit it
  /// last received.
  action replay(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val left = others(attempt.pipe, attempt.load)
    val l = least(left)
    val log = logs.get((attempt.pipe, l))
    val unreceipted = log.commits.filter(c => not(d.receipts.contains(c.key)))
    val c = unreceipted.fold(NOCOMMIT, (best, x) => if (best == NOCOMMIT or x.key._2 < best.key._2) x else best)
    val segs = decided(d, c)
    all {
      live.contains(k),
      attempt.stage == Replaying,
      if (left == Set()) {
        // A source that cannot read again refuses a read from before where it stands, and the
        // attempt ends.
        val planned = partsOf(attempt.pipe).forall(part => rereads(part) or kept.get(part) <= d.cursor.get(part))
        val at = d.cursor
        val epoch = d.epoch + 1
        all {
          outcome == Ok,
          keepAll(
            dest.set(attempt.pipe, { ...d, epoch: epoch }), logs, kept,
            if (planned) attempts.set(k, { ...attempt, epoch: epoch, stage: Reading, read: at, sealed: at, committed: at, opened: d.last })
            else without(k),
            started, faults, landed, fencedLanded,
          ),
        }
      }
      else if (not(log.fenced)) all {
        outcome == Ok,
        keepAll(dest, logs.set((attempt.pipe, l), { ...log, fenced: true }), kept, attempts, started, faults, landed, fencedLanded),
      }
      else if (unreceipted == Set()) all {
        outcome == Ok,
        keepAll(dest, logs.set((attempt.pipe, l), NOLOG), kept, attempts, started, faults, landed, fencedLanded),
      }
      // The replay's session opens as its first commit needs it.
      else if (attempt.epoch == 0) all {
        outcome == Ok,
        keepAll(
          dest.set(attempt.pipe, { ...d, epoch: d.epoch + 1 }), logs, kept,
          attempts.set(k, { ...attempt, epoch: d.epoch + 1 }), started, faults, landed, fencedLanded,
        ),
      }
      else committing(k, attempt, d, c.key, segs, d.last == c.prev, outcome),
    }
  }

  /// The attempt `k` commits `segs` of the commit `key` at the destination, which holds `d`,
  /// meeting `outcome`: a session the destination fenced is refused; a commit it holds the receipt
  /// of answers with it; otherwise it lands, but where it failed, and its answer is lost where it
  /// landed so.
  action committing(k: (Pipe, int), attempt: Attempt, d: Dest, key: Key, segs: Set[Seg], whole: bool, outcome: Outcome): bool = {
    val fenced = d.epoch != attempt.epoch
    val held = d.receipts.contains(key)
    val lands = not(fenced) and not(held) and outcome != Failed
    val answered = not(fenced) and (held or outcome == Ok)
    all {
      Set(Ok, Failed, Lost).contains(outcome),
      outcome == Ok or faults > 0,
      keepAll(
        if (lands) dest.set(attempt.pipe, applied(d, key, segs, whole)) else dest,
        logs, kept,
        if (answered) attempts.set(k, { ...attempt, stage: if (attempt.stage == Replaying) Replaying else Landed }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        if (lands) landed.union(positions(segs)) else landed,
        fencedLanded or (lands and fenced),
      ),
    }
  }

  /// The attempt `k` reads the next row of `part`.
  action read(k: (Pipe, int), part: Part): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      partsOf(attempt.pipe).contains(part),
      attempt.read.get(part) < rowsOf(part),
      keepAll(dest, logs, kept, attempts.set(k, { ...attempt, read: attempt.read.set(part, attempt.read.get(part) + 1) }), started, faults, landed, fencedLanded),
    }
  }

  /// The attempt `k` checkpoints `part` where it read it: the rows read so far are sealed.
  action checkpoint(k: (Pipe, int), part: Part): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      partsOf(attempt.pipe).contains(part),
      attempt.sealed.get(part) < attempt.read.get(part),
      keepAll(dest, logs, kept, attempts.set(k, { ...attempt, sealed: attempt.sealed.set(part, attempt.read.get(part)) }), started, faults, landed, fencedLanded),
    }
  }

  /// The attempt `k` commits what it sealed: its log takes the commit first, which a log a replay
  /// fenced refuses, meeting `outcome`; a pipeline with no log goes straight on.
  action log(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    val segs = partsOf(attempt.pipe).filter(part => attempt.sealed.get(part) > attempt.committed.get(part))
                                .map(part => { part: part, from: attempt.committed.get(part), to: attempt.sealed.get(part) })
    val key = (attempt.load, attempt.seq + 1)
    val c = { pipe: attempt.pipe, key: key, epoch: attempt.epoch, segs: segs, prev: if (attempt.seq == 0) attempt.opened else (attempt.load, attempt.seq) }
    val own = if (logsAhead(attempt.pipe)) logs.get((attempt.pipe, attempt.load)) else NOLOG
    val refused = logsAhead(attempt.pipe) and (own.fenced or not(own.open))
    val takes = not(refused) and outcome == Ok
    all {
      live.contains(k),
      attempt.stage == Reading,
      segs != Set(),
      Set(Ok, Failed).contains(outcome),
      outcome == Ok or faults > 0,
      keepAll(
        dest,
        if (takes and logsAhead(attempt.pipe)) logs.set((attempt.pipe, attempt.load), { ...own, commits: own.commits.union(Set(c)) }) else logs,
        kept,
        if (takes) attempts.set(k, { ...attempt, stage: if (logsAhead(attempt.pipe)) Logged else Told, inFlight: c }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The attempt `k` tells the source of a partition that cannot read again its commit's position,
  /// the log holding the commit, meeting `outcome`: a telling that fails may have moved the
  /// source all the same.
  action tellLogged(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    val told = attempt.inFlight.segs.filter(s => not(rereads(s.part))).fold(kept, (kk, s) => kk.set(s.part, s.to))
    all {
      live.contains(k),
      attempt.stage == Logged,
      Set(Ok, Failed, Lost).contains(outcome),
      outcome == Ok or faults > 0,
      keepAll(
        dest, logs,
        if (outcome == Failed) kept else told,
        if (outcome == Ok) attempts.set(k, { ...attempt, stage: Told }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The attempt `k` commits at the destination.
  action land(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Told,
      committing(k, attempt, dest.get(attempt.pipe), attempt.inFlight.key, attempt.inFlight.segs, true, outcome),
    }
  }

  /// The attempt `k` tells the sources of partitions that read again the positions its commit
  /// landed with, meeting `outcome`, and reads on.
  action tellLanded(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    val told = attempt.inFlight.segs.filter(s => rereads(s.part)).fold(kept, (kk, s) => kk.set(s.part, s.to))
    all {
      live.contains(k),
      attempt.stage == Landed,
      Set(Ok, Failed, Lost).contains(outcome),
      outcome == Ok or faults > 0,
      keepAll(
        dest, logs,
        if (outcome == Failed) kept else told,
        if (outcome == Ok) attempts.set(k, { ...attempt, stage: Reading, committed: attempt.sealed, seq: attempt.seq + 1, inFlight: NOCOMMIT }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The attempt `k` read every partition to its end and committed all of it: it ends, its log
  /// removed, every commit in it holding its receipt.
  action close(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      partsOf(attempt.pipe).forall(part => attempt.read.get(part) == rowsOf(part) and attempt.committed.get(part) == rowsOf(part)),
      keepAll(
        dest,
        if (logsAhead(attempt.pipe)) logs.set((attempt.pipe, attempt.load), NOLOG) else logs,
        kept, without(k), started, faults, landed, fencedLanded,
      ),
    }
  }

  /// The attempt `k` crashes: what it read and staged is lost; its log and what landed stay.
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
        all { rare == 1 or attempts.keys().forall(k => k._1 != pipe), start(pipe) },
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
    0.to(MAXROWS - 1).filter(o => o < rowsOf(part)).forall(o => d.rows.get(part).get(o) == (if (o < d.cursor.get(part)) 1 else 0))
  }))

  /// A source stands at a position only once it is safe: a source that reads again once its
  /// commit landed, and one that cannot once a log holds the commit until it lands.
  val Acknowledged = PARTS.forall(part => {
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
  val vars = (dest, logs, kept, attempts, started, faults, landed, fencedLanded)

  /// Once faults stop, the load converges: every row lands once, and no log is left. It need not
  /// stay so to a behavior's end: a later attempt opens a log, which a fault ending the last
  /// attempt the bounds start leaves open, for an attempt the engine would start next to replay.
  temporal Converges = weakFair(step, vars).implies(eventually(converged))
}

/// The protocol at the bounds every checker explores: one pipeline reads `a`, whose source reads
/// again, one row, and `u`, whose source does not, two rows. A fault may end two attempts, so the
/// pipeline may start one attempt more than twice the faults: whatever the faults end, an attempt
/// remains that meets none.
module exactly_once {
  import exactly_once_protocol(
    PIPES = Set("p"),
    PARTS = Set("a", "u"),
    OWNER = Map("a" -> "p", "u" -> "p"),
    REREADS = Set("a"),
    ROWS = Map("a" -> 1, "u" -> 2),
    MAXROWS = 2,
    LOADS = 5,
    FAULTS = 2,
  ).*

  // ---- Tests

  /// Quint's simulator does not check the assumption on the bound, so a test does.
  run everyPartitionFitsTheBoundTest = init.expect(rowsFit)

  /// The first pipeline's attempt `load`.
  pure def p(load: int): (Pipe, int) = ("p", load)

  /// The steps an attempt of the first pipeline takes to read, seal and commit a row of each of its
  /// partitions, everything answered.
  action rowEach(load: int): bool =
    read(p(load), "a").then(checkpoint(p(load), "a")).then(read(p(load), "u")).then(checkpoint(p(load), "u"))
      .then(log(p(load), Ok)).then(tellLogged(p(load), Ok)).then(land(p(load), Ok)).then(tellLanded(p(load), Ok))

  /// The steps an attempt takes to read, seal and commit a row of `u`, everything answered.
  action rowOfU(load: int): bool =
    read(p(load), "u").then(checkpoint(p(load), "u"))
      .then(log(p(load), Ok)).then(tellLogged(p(load), Ok)).then(land(p(load), Ok)).then(tellLanded(p(load), Ok))

  /// A source told a position before the log holds the commit, which an attempt never does.
  action toldEarly(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      keepAll(dest, logs, kept.set("u", attempt.sealed.get("u")), attempts, started, faults, landed, fencedLanded),
    }
  }

  /// A destination that lands a commit whatever its session's epoch, which it never does.
  action landedFenced(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    all {
      live.contains(k),
      attempt.stage == Told,
      keepAll(
        dest.set(attempt.pipe, applied(d, attempt.inFlight.key, attempt.inFlight.segs, true)),
        logs, kept, attempts.set(k, { ...attempt, stage: Landed }), started, faults,
        landed.union(positions(attempt.inFlight.segs)),
        fencedLanded or d.epoch != attempt.epoch,
      ),
    }
  }

  /// A replay that commits a logged commit again though the destination holds its receipt, and
  /// whole, which a replay never does.
  action replayedAgain(k: (Pipe, int), l: int): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val c = logs.get((attempt.pipe, l)).commits.fold(NOCOMMIT, (_, x) => x)
    all {
      live.contains(k),
      attempt.stage == Replaying,
      keepAll(dest.set(attempt.pipe, applied(d, c.key, c.segs, true)), logs, kept, attempts, started, faults,
              landed.union(positions(c.segs)), fencedLanded),
    }
  }

  /// A source that moves to where it was read, not where it was told, which a source that
  /// passes certification never does.
  action readMoving(k: (Pipe, int), part: Part): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      attempt.read.get(part) < rowsOf(part),
      keepAll(dest, logs, kept.set(part, attempt.read.get(part) + 1),
              attempts.set(k, { ...attempt, read: attempt.read.set(part, attempt.read.get(part) + 1) }), started, faults, landed, fencedLanded),
    }
  }

  /// A source that stands at `pos` of `part` with no commit having given it that position.
  action movedTo(part: Part, pos: int): bool =
    keepAll(dest, logs, kept.set(part, pos), attempts, started, faults, landed, fencedLanded)

  /// An attempt reads each partition to its end and commits it a row at a time, then ends with its
  /// log removed.
  run anAttemptCommitsEveryRowOnceAndEndsTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(rowEach(1))
      .then(rowOfU(1))
      .then(close(p(1)))
      .expect(all {
        dest.get("p").cursor == Map("a" -> 1, "u" -> 2),
        kept == Map("a" -> 1, "u" -> 2),
        logs.keys().forall(k => not(logs.get(k).open)),
        ExactlyOnce,
        Acknowledged,
        PositionOnlyInCommitted,
      })

  /// A commit the log holds, whose source was told and whose attempt crashed before it landed, is
  /// committed by the next attempt's replay, whole, and its rows land once.
  run aLoggedCommitIsReplayedAfterACrashTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(log(p(1), Ok))
      .then(tellLogged(p(1), Ok))
      .expect(kept.get("u") == 1 and dest.get("p").cursor.get("u") == 0 and Acknowledged)
      .then(crash(p(1)))
      .then(start("p"))
      .then(5.reps(_ => replay(p(2), Ok)))
      .expect(all {
        dest.get("p").cursor.get("u") == 1,
        dest.get("p").receipts == Set((1, 1)),
        not(logs.get(("p", 1)).open),
        attemptOf(p(2)).stage == Reading,
        attemptOf(p(2)).read.get("u") == 1,
        ExactlyOnce,
        Acknowledged,
      })

  /// A commit that landed with its answer lost holds its receipt, so the next attempt's replay
  /// lands nothing again.
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
      .expect(dest.get("p").rows.get("u").get(0) == 1 and attemptOf(p(2)).stage == Reading and ExactlyOnce)

  /// An attempt's commits follow the commit the destination last received as its own session
  /// opened, one its replay landed included: a crash after its first commit is logged leaves a
  /// commit the next attempt's replay lands whole, its rows that read again included.
  run aReplayedCommitPrecedesTheNextAttemptsFirstTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "a"))
      .then(checkpoint(p(1), "a"))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(log(p(1), Ok))
      .then(crash(p(1)))
      .then(start("p"))
      .then(5.reps(_ => replay(p(2), Ok)))
      .expect(dest.get("p").last == (1, 1) and attemptOf(p(2)).stage == Reading)
      .then(read(p(2), "u"))
      .then(checkpoint(p(2), "u"))
      .then(log(p(2), Ok))
      .expect(attemptOf(p(2)).inFlight.prev == (1, 1))
      .then(crash(p(2)))
      .then(start("p"))
      .then(5.reps(_ => replay(p(3), Ok)))
      .expect(all {
        dest.get("p").last == (2, 1),
        dest.get("p").cursor == Map("a" -> 1, "u" -> 2),
        attemptOf(p(3)).stage == Reading,
        ExactlyOnce,
      })

  /// An attempt started beside a live one fences its log, then its session as the replay's opens:
  /// the older attempt's commit is refused at the destination, and the newer attempt's replay lands
  /// what the older one logged.
  run aFencedAttemptNeverCommitsTest =
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
      .expect(dest.get("p").cursor.get("a") == 1 and attemptOf(p(2)).stage == Reading and ExactlyOnce)

  /// Two attempts started together each replay the other's log, empty, and remove it, so neither
  /// has a log to commit to: one fault ends both.
  run anAttemptStartedBesideAnotherMayEndBothTest =
    init
      .then(start("p"))
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(replay(p(2), Ok))
      .then(replay(p(1), Ok))
      .then(replay(p(2), Ok))
      .then(replay(p(1), Ok))
      .then(replay(p(2), Ok))
      .then(read(p(1), "a"))
      .then(checkpoint(p(1), "a"))
      .then(log(p(1), Ok))
      .then(read(p(2), "a"))
      .then(checkpoint(p(2), "a"))
      .then(log(p(2), Ok))
      .expect(live == Set() and faults == FAULTS - 1 and dest.get("p").cursor.get("a") == 0)

  /// A behavior may end converged no longer: once the load converged, a fault ending the last
  /// attempt leaves its log open, and no step is left: no attempt is live, and the bounds start no
  /// more.
  run aFaultEndingTheLastAttemptLeavesItsLogTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(rowEach(1))
      .then(rowOfU(1))
      .then(close(p(1)))
      .expect(converged)
      .then((LOADS - 2).reps(i => start("p").then(replay(p(i + 2), Ok)).then(close(p(i + 2)))))
      .expect(converged)
      .then(start("p"))
      .then(crash(p(LOADS)))
      .expect(all {
        not(converged),
        logs.get(("p", LOADS)).open,
        dest.get("p").cursor == Map("a" -> 1, "u" -> 2),
        ExactlyOnce,
      })
      .expect(live == Set() and PIPES.forall(x => started.get(x) == LOADS))

  /// A source told a position before the log held its commit would stand past every copy of the
  /// rows: the invariant refuses it.
  run aSourceToldBeforeTheLogWouldStandPastTheRowsTest =
    init
      .then(start("p"))
      .then(replay(p(1), Ok))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(toldEarly(p(1)))
      .expect(not(Acknowledged))

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

/// The protocol with a second pipeline sharing the destination, which only `quint test` and
/// `quint run` explore: the checkers' bounds hold one.
module exactly_once_pair {
  import exactly_once_protocol(
    PIPES = Set("p", "q"),
    PARTS = Set("a", "u", "b"),
    OWNER = Map("a" -> "p", "u" -> "p", "b" -> "q"),
    REREADS = Set("a", "b"),
    ROWS = Map("a" -> 2, "u" -> 2, "b" -> 1),
    MAXROWS = 2,
    LOADS = 5,
    FAULTS = 2,
  ).*

  run everyPartitionFitsTheBoundTest = init.expect(rowsFit)

  /// An attempt of the second pipeline opens its own session: the first pipeline's attempt, opened
  /// before, still commits.
  run aSecondPipelineFencesNoneOfTheFirstsTest =
    init
      .then(start("p"))
      .then(replay(("p", 1), Ok))
      .then(start("q"))
      .then(replay(("q", 1), Ok))
      .then(read(("p", 1), "a")).then(checkpoint(("p", 1), "a"))
      .then(read(("p", 1), "u")).then(checkpoint(("p", 1), "u"))
      .then(log(("p", 1), Ok)).then(tellLogged(("p", 1), Ok)).then(land(("p", 1), Ok)).then(tellLanded(("p", 1), Ok))
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
}
