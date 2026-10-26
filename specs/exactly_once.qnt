// Exactly-once delivery with acknowledged positions, as ADRs 0004 and 0030 decide it. Each attempt
// of a pipeline, its load, opens a session at the destination, which fences every session of the
// pipeline opened before, reads each partition from where the destination holds it, checkpoints,
// and commits what it sealed under the session's epoch; once the commit lands, it tells each
// partition's source the position the commit landed with. Each commit is keyed by its session's
// epoch and its place among the session's commits, and names the key it expects the destination
// to hold landed; the destination keeps the last key that landed, answers a key it holds with its
// receipt, refuses a commit of another epoch or one expecting another key, and applies the rest.
// An attempt whose commit's answer it never heard credits it to the run once the next open finds
// its key landed. A source that reads again serves any
// position; one that cannot forgets what precedes the position it was last told, refuses a read
// from before it, and serves everything after it to every reader that asks. An attempt may crash,
// its commit may fail or land with its answer lost, and a telling may fail or lose its answer: the
// attempt then reads on, owing what it carried to its next telling, which a commit with nothing to
// publish makes too. An attempt may start while another of its pipeline is live, as a second run's
// would, both reading a partition whose source cannot read again. A retry is an attempt of its
// own: the model does not group attempts into runs. The destination may be shared by several
// pipelines: `quint test` and `quint run` take two, and the model checkers one.
//
// quint main: exactly_once exactly_once_pair
// quint steps: --step simulated --max-steps 60
// quint run: --max-samples 2000
// quint verify: --max-steps 8
module exactly_once_protocol {
  // ---- What a destination's pipelines read

  /// The pipelines sharing the destination.
  const PIPES: Set[str]
  /// The partitions they read.
  const PARTS: Set[str]
  /// The pipeline reading each partition.
  const OWNER: str -> str
  /// The partitions whose source reads again; every other forgets what precedes the position it
  /// was told is committed.
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
  /// Faults a behavior meets: crashes, failed or lost commits, failed tellings, and attempts
  /// started beside a live one.
  const FAULTS: int

  type Pipe = str
  type Part = str

  pure def ownerOf(part: Part): Pipe = OWNER.get(part)

  pure def rereads(part: Part): bool = REREADS.contains(part)

  pure def rowsOf(part: Part): int = ROWS.get(part)

  pure def partsOf(pipe: Pipe): Set[Part] = PARTS.filter(part => ownerOf(part) == pipe)

  // ---- What the destination and the sources hold

  /// A segment: a partition's rows from one offset to the next.
  type Seg = { part: Part, from: int, to: int }

  /// A commit's key: its session's epoch and its place among the session's commits, from 1.
  type Key = (int, int)

  /// No key: before the first commit lands.
  pure val NONE: Key = (0, 0)

  /// Whether `key` comes after `other`: of a later epoch, or later in one epoch.
  pure def above(key: Key, other: Key): bool =
    key._1 > other._1 or (key._1 == other._1 and key._2 > other._2)

  /// What the destination holds of a pipeline: its newest session's epoch, each partition's
  /// position and how often each row of it is held, the key of the last commit that landed, and
  /// whether a commit ever applied a key that is not above the one landed before it, as a key
  /// applying twice must.
  type Dest = { epoch: int, cursor: Part -> int, rows: Part -> (int -> int), landed: Key, again: bool }

  /// How the destination answers a commit: the commit landed already, and its receipt is the
  /// answer; a newer session fenced it; it expects a key other than the one landed; it applies.
  type Answer = AlreadyLanded | FencedEpoch | StaleAfter | Apply

  /// How the destination `d` answers the commit keyed `key` that expects `after` landed.
  pure def answer(d: Dest, key: Key, after: Key): Answer =
    if (d.landed == key) AlreadyLanded
    else if (d.epoch != key._1) FencedEpoch
    else if (d.landed != after) StaleAfter
    else Apply

  /// What a run knows of a pipeline's commit whose answer it never heard: its key, none where no
  /// commit is in flight; whether it landed, which the run does not know; whether the last open
  /// credited one to the run; and whether an open ever credited one that had not landed.
  type Ledger = { inFlight: Key, applied: bool, credited: bool, miscredited: bool }

  /// Where an attempt stands: before its session opens, reading, its commit landed.
  type Stage = | Opening | Reading | Landed

  /// An attempt: its pipeline, the epoch of the session it holds (0 before it opens one), where
  /// it stands, each partition's rows read, sealed by a checkpoint and committed, the segments
  /// of the commit that landed and whose sources it has yet to tell, the position each
  /// partition's source is owed by a telling that failed, 0 where none is, its next commit's
  /// place among its session's, and the key it expects landed.
  type Attempt = {
    pipe: Pipe,
    epoch: int,
    stage: Stage,
    read: Part -> int,
    sealed: Part -> int,
    committed: Part -> int,
    landing: Set[Seg],
    owed: Part -> int,
    seq: int,
    after: Key,
  }

  /// The destination, by pipeline.
  var dest: Pipe -> Dest
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
  /// What each pipeline's run knows of the commit whose answer it never heard.
  var ledger: Pipe -> Ledger

  // ---- Helpers

  pure def zero(parts: Set[Part]): Part -> int = parts.mapBy(_ => 0)

  pure def none(part: Part): int -> int = 0.to(MAXROWS - 1).filter(o => o < rowsOf(part)).mapBy(_ => 0)

  pure def fresh(pipe: Pipe): Dest = {
    epoch: 0, cursor: zero(partsOf(pipe)), rows: partsOf(pipe).mapBy(part => none(part)),
    landed: NONE, again: false,
  }

  /// The destination `d` once it applied `segs`, the commit keyed `key`.
  pure def applied(d: Dest, segs: Set[Seg], key: Key): Dest = {
    val rows = segs.fold(d.rows, (rs, s) =>
      rs.set(s.part, 0.to(MAXROWS - 1).filter(o => s.from <= o and o < s.to).fold(rs.get(s.part), (counts, o) => counts.set(o, counts.get(o) + 1))))
    val cursor = segs.fold(d.cursor, (c, s) => c.set(s.part, s.to))
    { ...d, rows: rows, cursor: cursor, landed: key, again: d.again or not(above(key, d.landed)) }
  }

  /// No commit in flight.
  pure val SETTLED: Ledger = { inFlight: NONE, applied: false, credited: false, miscredited: false }

  /// The ledger `l` once an open finds `register` landed: the commit in flight is credited where
  /// its key is the one landed, and is in flight no longer either way.
  pure def resolved(l: Ledger, register: Key): Ledger =
    if (l.inFlight == NONE) { ...l, credited: false }
    else {
      val credits = l.inFlight == register
      { ...SETTLED, credited: credits, miscredited: l.miscredited or (credits and not(l.applied)) }
    }

  /// The positions `segs` land with.
  pure def positions(segs: Set[Seg]): Set[(Part, int)] = segs.map(s => (s.part, s.to))

  /// The sources of `kept` once each partition of `segs` is told the position it lands with.
  pure def told(kept: Part -> int, segs: Set[Seg]): Part -> int = segs.fold(kept, (k, s) => k.set(s.part, s.to))

  /// What a telling carries: the position each partition of `segs` lands with, and of every other
  /// partition the position `owed`, 0 where none is.
  pure def reportOf(owed: Part -> int, segs: Set[Seg]): Part -> int = segs.fold(owed, (r, s) => r.set(s.part, s.to))

  /// The sources of `kept` once each partition `report` holds a position of is told it.
  pure def toldOf(kept: Part -> int, report: Part -> int): Part -> int =
    report.keys().fold(kept, (k, part) => if (report.get(part) > 0) k.set(part, report.get(part)) else k)

  /// Whether `owed` holds a position of any partition.
  pure def owes(owed: Part -> int): bool = owed.keys().exists(part => owed.get(part) > 0)

  /// The attempts live.
  val live: Set[(Pipe, int)] = attempts.keys()

  def attemptOf(k: (Pipe, int)): Attempt = attempts.get(k)

  /// The next state: every variable as given, the ledger as it was.
  action keepAll(
    d: Pipe -> Dest, k: Part -> int, r: (Pipe, int) -> Attempt, s: Pipe -> int, f: int,
    landedNow: Set[(Part, int)], fenced: bool
  ): bool = keepAllWith(d, k, r, s, f, landedNow, fenced, ledger)

  /// The next state: every variable as given.
  action keepAllWith(
    d: Pipe -> Dest, k: Part -> int, r: (Pipe, int) -> Attempt, s: Pipe -> int, f: int,
    landedNow: Set[(Part, int)], fenced: bool, l: Pipe -> Ledger
  ): bool = all {
    dest' = d,
    kept' = k,
    attempts' = r,
    started' = s,
    faults' = f,
    landed' = landedNow,
    fencedLanded' = fenced,
    ledger' = l,
  }

  /// The attempts without `k`.
  def without(k: (Pipe, int)): (Pipe, int) -> Attempt = attempts.keys().exclude(Set(k)).mapBy(x => attempts.get(x))

  // ---- Actions

  action init = all {
    dest' = PIPES.mapBy(pipe => fresh(pipe)),
    kept' = zero(PARTS),
    attempts' = Map(),
    started' = PIPES.mapBy(_ => 0),
    faults' = FAULTS,
    landed' = Set(),
    fencedLanded' = false,
    ledger' = PIPES.mapBy(_ => SETTLED),
  }

  /// An attempt of `pipe` starts, holding no session yet. An attempt started while another of
  /// the pipeline is live meets a fault.
  action start(pipe: Pipe): bool = {
    val load = started.get(pipe) + 1
    val concurrent = attempts.keys().exists(k => k._1 == pipe)
    val attempt = {
      pipe: pipe, epoch: 0, stage: Opening,
      read: zero(partsOf(pipe)), sealed: zero(partsOf(pipe)), committed: zero(partsOf(pipe)),
      landing: Set(), owed: zero(partsOf(pipe)), seq: 1, after: NONE,
    }
    all {
      load <= LOADS,
      not(concurrent) or faults > 0,
      keepAll(
        dest,
        kept,
        attempts.put((pipe, load), attempt),
        started.set(pipe, load),
        if (concurrent) faults - 1 else faults,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The attempt `k` opens its session, which fences every older one, credits the commit its
  /// run never heard the answer of where the key landed is its key, and plans each partition from
  /// where the destination holds it, its first commit expecting that key landed; a source that
  /// cannot read again refuses a read from before where it stands, and the attempt ends.
  action open(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val planned = partsOf(attempt.pipe).forall(part => rereads(part) or kept.get(part) <= d.cursor.get(part))
    val epoch = d.epoch + 1
    all {
      live.contains(k),
      attempt.stage == Opening,
      keepAllWith(
        dest.set(attempt.pipe, { ...d, epoch: epoch }),
        kept,
        if (planned) attempts.set(k, { ...attempt, epoch: epoch, stage: Reading, read: d.cursor, sealed: d.cursor, committed: d.cursor, after: d.landed })
        else without(k),
        started, faults, landed, fencedLanded,
        ledger.set(attempt.pipe, resolved(ledger.get(attempt.pipe), d.landed)),
      ),
    }
  }

  /// The attempt `k` reads the next row of `part`, which its source serves whatever another
  /// attempt read of it.
  action read(k: (Pipe, int), part: Part): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      partsOf(attempt.pipe).contains(part),
      attempt.read.get(part) < rowsOf(part),
      keepAll(dest, kept, attempts.set(k, { ...attempt, read: attempt.read.set(part, attempt.read.get(part) + 1) }), started, faults, landed, fencedLanded),
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
      keepAll(dest, kept, attempts.set(k, { ...attempt, sealed: attempt.sealed.set(part, attempt.read.get(part)) }), started, faults, landed, fencedLanded),
    }
  }

  /// The segments the attempt `attempt` sealed since its last commit.
  pure def sealedSegs(attempt: Attempt): Set[Seg] =
    attempt.sealed.keys().filter(part => attempt.sealed.get(part) > attempt.committed.get(part))
      .map(part => { part: part, from: attempt.committed.get(part), to: attempt.sealed.get(part) })

  /// The attempt `k` commits what it sealed at the destination under its next key, meeting
  /// `outcome`: the destination answers as `answer` does, and a commit it applies lands, but where
  /// it failed, and its answer is lost where it landed so. An attempt that hears no landing ends,
  /// its commit in flight for its run. No source is told anything yet.
  action land(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val segs = sealedSegs(attempt)
    val key = (attempt.epoch, attempt.seq)
    val answered = answer(d, key, attempt.after)
    val lands = answered == Apply and outcome != Failed
    val heard = outcome == Ok and (answered == Apply or answered == AlreadyLanded)
    all {
      live.contains(k),
      attempt.stage == Reading,
      segs != Set(),
      outcome == Ok or faults > 0,
      keepAllWith(
        if (lands) dest.set(attempt.pipe, applied(d, segs, key)) else dest,
        kept,
        if (heard) attempts.set(k, { ...attempt, stage: Landed, landing: segs, seq: attempt.seq + 1, after: key })
        else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        if (lands) landed.union(positions(segs)) else landed,
        fencedLanded or (lands and d.epoch != attempt.epoch),
        if (heard) ledger
        else ledger.set(attempt.pipe, { ...ledger.get(attempt.pipe), inFlight: key, applied: lands }),
      ),
    }
  }

  /// The attempt `k` tells the source of every partition its commit landed with that position,
  /// and of every other partition it owes a position that one, meeting `outcome`, and reads on.
  /// A telling that fails leaves every source as it was, and one whose answer is lost has moved
  /// them all the same; either leaves the attempt owing what it carried.
  action tell(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    val report = reportOf(attempt.owed, attempt.landing)
    all {
      live.contains(k),
      attempt.stage == Landed,
      outcome == Ok or faults > 0,
      keepAll(
        dest,
        if (outcome == Failed) kept else toldOf(kept, report),
        attempts.set(k, {
          ...attempt, stage: Reading, committed: attempt.sealed, landing: Set(),
          owed: if (outcome == Ok) zero(partsOf(attempt.pipe)) else report,
        }),
        started,
        if (outcome == Ok) faults else faults - 1,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The attempt `k`, reading, tells the sources it owes a position that one, as a commit with
  /// nothing to publish does, meeting `outcome` as a telling after a commit does.
  action tellOwed(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      owes(attempt.owed),
      outcome == Ok or faults > 0,
      keepAll(
        dest,
        if (outcome == Failed) kept else toldOf(kept, attempt.owed),
        if (outcome == Ok) attempts.set(k, { ...attempt, owed: zero(partsOf(attempt.pipe)) }) else attempts,
        started,
        if (outcome == Ok) faults else faults - 1,
        landed,
        fencedLanded,
      ),
    }
  }

  /// The attempt `k` read every partition to its end and committed all of it: it ends.
  action close(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      partsOf(attempt.pipe).forall(part => attempt.read.get(part) == rowsOf(part) and attempt.committed.get(part) == rowsOf(part)),
      keepAll(dest, kept, without(k), started, faults, landed, fencedLanded),
    }
  }

  /// The attempt `k` crashes: what it read and staged is lost; what landed stays.
  action crash(k: (Pipe, int)): bool = all {
    live.contains(k),
    faults > 0,
    keepAll(dest, kept, without(k), started, faults - 1, landed, fencedLanded),
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
        open(k),
        read(k, part),
        checkpoint(k, part),
        land(k, outcome),
        tell(k, outcome),
        tellOwed(k, outcome),
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
          open(k),
          read(k, part),
          checkpoint(k, part),
          land(k, outcome),
          tell(k, outcome),
          tellOwed(k, outcome),
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

  /// A source stands at a position only once the destination holds every row before it: no
  /// source is told past what is committed.
  val Acknowledged = PARTS.forall(part => kept.get(part) <= dest.get(ownerOf(part)).cursor.get(part))

  /// No commit lands from a session the destination fenced.
  val FencedNeverCommits = not(fencedLanded)

  /// No key a commit applies is at or below the key landed before it, so a landed key never
  /// applies twice.
  val LandedKeyAppliesOnce = PIPES.forall(pipe => not(dest.get(pipe).again))

  /// A run credits only a commit that landed.
  val CreditedOnlyLanded = PIPES.forall(pipe => not(ledger.get(pipe).miscredited))

  /// A source's position moves only as it is told a committed position, and then to that one: a
  /// position a commit landed with.
  val PositionOnlyInCommitted = PARTS.forall(part =>
    kept.get(part) == 0 or landed.contains((part, kept.get(part))))

  /// A position an attempt owes a source is one a commit landed with, so telling it later moves
  /// the source to no other.
  val OwedOnlyLanded = attempts.keys().forall(k => {
    val owed = attempts.get(k).owed
    owed.keys().forall(part => owed.get(part) == 0 or landed.contains((part, owed.get(part))))
  })

  /// Every pipeline holds every row of each of its partitions.
  val converged = PIPES.forall(pipe => partsOf(pipe).forall(part => dest.get(pipe).cursor.get(part) == rowsOf(part)))

  /// Every variable.
  val vars = (dest, kept, attempts, started, faults, landed, fencedLanded, ledger)

  /// Once faults stop, the load converges: every row lands once. A position never moves back, so
  /// it stays so.
  temporal Converges = weakFair(step, vars).implies(eventually(converged))
}

/// The protocol at the bounds every checker explores: one pipeline reads `a`, whose source reads
/// again, one row, and `u`, whose source does not, two rows. A fault ends one attempt at most, so
/// of five attempts three leave one that meets none, whatever two faults end; the others start
/// beside a live attempt, two then reading `u` at once, the newer fencing the older, or once the
/// load converged.
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
      .then(land(p(load), Ok)).then(tell(p(load), Ok))

  /// The steps an attempt takes to read, seal and commit a row of `u`, everything answered.
  action rowOfU(load: int): bool =
    read(p(load), "u").then(checkpoint(p(load), "u")).then(land(p(load), Ok)).then(tell(p(load), Ok))

  /// A source told the position of what an attempt sealed before its commit lands, which an
  /// attempt never does.
  action toldBeforeLanding(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      keepAll(dest, told(kept, sealedSegs(attempt)), attempts, started, faults, landed, fencedLanded),
    }
  }

  /// An attempt owing the positions of what it sealed before its commit lands, which an attempt
  /// never does.
  action owedBeforeLanding(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Reading,
      keepAll(
        dest, kept, attempts.set(k, { ...attempt, owed: reportOf(attempt.owed, sealedSegs(attempt)) }),
        started, faults, landed, fencedLanded,
      ),
    }
  }

  /// A destination that lands a commit whatever its session's epoch, which it never does.
  action landedFenced(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val segs = sealedSegs(attempt)
    all {
      live.contains(k),
      attempt.stage == Reading,
      keepAll(
        dest.set(attempt.pipe, applied(d, segs, (attempt.epoch, attempt.seq))),
        kept, attempts.set(k, { ...attempt, stage: Landed, landing: segs }), started, faults,
        landed.union(positions(segs)),
        fencedLanded or d.epoch != attempt.epoch,
      ),
    }
  }

  /// A commit that lands again rows the destination holds, which no attempt makes: each reads
  /// from where the destination holds its partitions as its session opens.
  action landedAgain(k: (Pipe, int), part: Part): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val segs = Set({ part: part, from: 0, to: d.cursor.get(part) })
    all {
      live.contains(k),
      d.cursor.get(part) > 0,
      keepAll(dest.set(attempt.pipe, applied(d, segs, (attempt.epoch, attempt.seq))), kept, attempts, started, faults, landed, fencedLanded),
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
      keepAll(dest, kept.set(part, attempt.read.get(part) + 1),
              attempts.set(k, { ...attempt, read: attempt.read.set(part, attempt.read.get(part) + 1) }), started, faults, landed, fencedLanded),
    }
  }

  /// A source that stands at `pos` of `part` with no commit having given it that position.
  action movedTo(part: Part, pos: int): bool =
    keepAll(dest, kept.set(part, pos), attempts, started, faults, landed, fencedLanded)

  /// An attempt reads each partition to its end and commits it a row at a time, each source told
  /// once its commit landed, then ends.
  run anAttemptCommitsEveryRowOnceAndEndsTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(rowEach(1))
      .then(rowOfU(1))
      .then(close(p(1)))
      .expect(all {
        dest.get("p").cursor == Map("a" -> 1, "u" -> 2),
        kept == Map("a" -> 1, "u" -> 2),
        live == Set(),
        ExactlyOnce,
        Acknowledged,
        PositionOnlyInCommitted,
      })

  /// A commit landed and not yet told leaves every source where it stood, the one that cannot
  /// read again included; an attempt that crashes then leaves the next to read from where the
  /// destination holds each partition, which every source serves.
  run aCommitThatLandedIsToldOnlyAfterTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Ok))
      .expect(kept.get("u") == 0 and dest.get("p").cursor.get("u") == 1 and Acknowledged)
      .then(crash(p(1)))
      .then(start("p"))
      .then(open(p(2)))
      .expect(all {
        attemptOf(p(2)).stage == Reading,
        attemptOf(p(2)).read.get("u") == 1,
        ExactlyOnce,
        Acknowledged,
      })
      .then(rowOfU(2))
      .expect(kept.get("u") == 2 and dest.get("p").rows.get("u") == Map(0 -> 1, 1 -> 1))

  /// A commit that landed with its answer lost tells no source, and the next attempt reads on
  /// from where it landed: its rows land once.
  run aCommitWhoseAnswerWasLostLandsOnceTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Lost))
      .expect(not(live.contains(p(1))) and dest.get("p").cursor.get("u") == 1 and kept.get("u") == 0)
      .then(start("p"))
      .then(open(p(2)))
      .then(rowOfU(2))
      .expect(dest.get("p").rows.get("u") == Map(0 -> 1, 1 -> 1) and kept.get("u") == 2 and ExactlyOnce)

  /// A telling whose answer is lost may move a source all the same, and only to the position its
  /// commit landed with; the attempt reads on, owing it.
  run aTellingLostStillMovesTheSourceOnlyToALandedPositionTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Ok))
      .then(tell(p(1), Lost))
      .expect(all {
        attemptOf(p(1)).stage == Reading,
        attemptOf(p(1)).owed.get("u") == 1,
        kept.get("u") == 1,
        Acknowledged,
        PositionOnlyInCommitted,
        OwedOnlyLanded,
      })

  /// A telling that fails leaves every source and the destination as they were, and the attempt
  /// reads on owing the position; its next commit's telling carries the newer position in its
  /// place.
  run aFailedTellingLeavesStateAsItWasAndTheNextCommitTellsItTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Ok))
      .then(tell(p(1), Failed))
      .expect(all {
        live.contains(p(1)),
        attemptOf(p(1)).stage == Reading,
        attemptOf(p(1)).owed == Map("a" -> 0, "u" -> 1),
        kept == Map("a" -> 0, "u" -> 0),
        dest.get("p").cursor == Map("a" -> 0, "u" -> 1),
        Acknowledged,
        OwedOnlyLanded,
      })
      .then(rowOfU(1))
      .expect(all {
        kept.get("u") == 2,
        not(owes(attemptOf(p(1)).owed)),
        ExactlyOnce,
        Acknowledged,
        PositionOnlyInCommitted,
      })

  /// A position owed is told with nothing more to publish, as an interval's commit tells it.
  run anOwedTellingIsMadeWithNothingToPublishTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "a"))
      .then(checkpoint(p(1), "a"))
      .then(land(p(1), Ok))
      .then(tell(p(1), Failed))
      .then(tellOwed(p(1), Failed))
      .expect(kept.get("a") == 0 and owes(attemptOf(p(1)).owed) and faults == FAULTS - 2)
      .then(tellOwed(p(1), Ok))
      .expect(all {
        kept.get("a") == 1,
        not(owes(attemptOf(p(1)).owed)),
        dest.get("p").cursor.get("a") == 1,
        Acknowledged,
        PositionOnlyInCommitted,
      })

  /// An attempt that owed a position of a commit that never landed would move its source past
  /// the destination once it told it: the invariant refuses it.
  run aPositionOwedBeforeItsCommitLandedIsRefusedTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(owedBeforeLanding(p(1)))
      .expect(not(OwedOnlyLanded))

  /// Two attempts read `u`, whose source cannot read again and serves its rows to both: the
  /// newer's session fences the older's, whose commit the destination refuses, and the newer's
  /// lands each row once and tells the source only once it landed.
  run twoAttemptsReadingASourceThatCannotReadAgainLandItsRowsOnceTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(start("p"))
      .then(open(p(2)))
      .then(read(p(2), "u"))
      .then(read(p(2), "u"))
      .then(checkpoint(p(2), "u"))
      .expect(attemptOf(p(1)).read.get("u") == 1 and attemptOf(p(2)).read.get("u") == 2)
      .then(land(p(1), Ok))
      .expect(not(live.contains(p(1))) and dest.get("p").cursor.get("u") == 0 and kept.get("u") == 0)
      .then(land(p(2), Ok))
      .then(tell(p(2), Ok))
      .expect(all {
        dest.get("p").cursor.get("u") == 2,
        dest.get("p").rows.get("u") == Map(0 -> 1, 1 -> 1),
        kept.get("u") == 2,
        faults == FAULTS - 1,
        ExactlyOnce,
        Acknowledged,
        FencedNeverCommits,
        PositionOnlyInCommitted,
      })

  /// An older attempt whose commit lands before a newer one opens tells its source, which then
  /// serves the newer attempt from that position on: the newer reads past it and fences the older,
  /// and every row lands once.
  run anOlderAttemptsLandedCommitIsWhereTheNewerReadsFromTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(start("p"))
      .then(land(p(1), Ok))
      .then(tell(p(1), Ok))
      .then(open(p(2)))
      .expect(attemptOf(p(2)).read.get("u") == 1 and kept.get("u") == 1)
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Ok))
      .expect(not(live.contains(p(1))) and dest.get("p").cursor.get("u") == 1)
      .then(rowOfU(2))
      .expect(all {
        dest.get("p").rows.get("u") == Map(0 -> 1, 1 -> 1),
        kept.get("u") == 2,
        ExactlyOnce,
        Acknowledged,
        FencedNeverCommits,
      })

  /// An attempt started once the load converged reads nothing and leaves it converged.
  run aLoadThatConvergedStaysSoTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(rowEach(1))
      .then(rowOfU(1))
      .then(close(p(1)))
      .expect(converged)
      .then((LOADS - 1).reps(i => start("p").then(open(p(i + 2))).then(close(p(i + 2)))))
      .expect(all {
        converged,
        live == Set(),
        PIPES.forall(x => started.get(x) == LOADS),
        ExactlyOnce,
      })

  /// The negative trace `tell_before_land`: a source told the position of what an attempt sealed
  /// before its commit lands would stand past every row the destination holds, and the invariant
  /// refuses it.
  run tellBeforeLandBreaksAcknowledgedTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .expect(Acknowledged)
      .then(toldBeforeLanding(p(1)))
      .expect(not(Acknowledged))

  /// A source told before its commit landed, whose attempt then crashed, refuses the next
  /// attempt's read from where the destination holds its partition: the attempt ends.
  run aSourceToldBeforeLandingRefusesTheNextReadTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(toldBeforeLanding(p(1)))
      .then(crash(p(1)))
      .then(start("p"))
      .then(open(p(2)))
      .expect(live == Set() and dest.get("p").cursor.get("u") == 0 and kept.get("u") == 1)

  /// A destination that took a fenced session's commit would land it: the invariant refuses it.
  run aDestinationTakingAFencedCommitWouldLandItTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "a"))
      .then(checkpoint(p(1), "a"))
      .then(start("p"))
      .then(open(p(2)))
      .then(landedFenced(p(1)))
      .expect(not(FencedNeverCommits))

  /// A commit landing again rows the destination holds would hold them twice: the invariant
  /// refuses it.
  run aCommitLandingHeldRowsAgainWouldHoldThemTwiceTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Lost))
      .then(start("p"))
      .then(open(p(2)))
      .then(landedAgain(p(2), "u"))
      .expect(not(ExactlyOnce))

  /// A source that moved to where it was read would stand where no commit put it: the invariant
  /// refuses it.
  run aSourceMovingAsItReadsWouldStandWhereNoCommitPutItTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(readMoving(p(1), "u"))
      .expect(not(PositionOnlyInCommitted))

  /// A position below the end of a commit that landed is still one no commit gave: the invariant
  /// refuses it.
  run aSourceBetweenTheEndsOfALandedCommitWouldStandWhereNoCommitPutItTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Ok))
      .then(tell(p(1), Ok))
      .expect(PositionOnlyInCommitted)
      .then(movedTo("u", 1))
      .expect(not(PositionOnlyInCommitted))

  /// The destination's answer to the commit keyed `key` were it never to compare the key landed
  /// with the one the commit expects, which it always does.
  pure def answerWithoutAfter(d: Dest, key: Key): Answer =
    if (d.landed == key) AlreadyLanded else if (d.epoch != key._1) FencedEpoch else Apply

  /// A copy of the commit of `segs` keyed `key`, expecting `after` landed, that the attempt `k`
  /// sent before arrives at the destination again, which compares the key landed with `after` or,
  /// where `compares` is false, does not: no attempt sends a commit twice, but a transport may
  /// deliver one late.
  action copyArrives(k: (Pipe, int), segs: Set[Seg], key: Key, after: Key, compares: bool): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val answered = if (compares) answer(d, key, after) else answerWithoutAfter(d, key)
    all {
      live.contains(k),
      keepAll(
        if (answered == Apply) dest.set(attempt.pipe, applied(d, segs, key)) else dest,
        kept, attempts, started, faults,
        if (answered == Apply) landed.union(positions(segs)) else landed,
        fencedLanded,
      ),
    }
  }

  /// A run that credits its commit in flight wherever a commit of the same session landed, which
  /// no run does: it credits only the key that landed.
  action creditedByEpoch(pipe: Pipe): bool = {
    val l = ledger.get(pipe)
    all {
      l.inFlight != NONE,
      l.inFlight._1 == dest.get(pipe).landed._1,
      keepAllWith(
        dest, kept, attempts, started, faults, landed, fencedLanded,
        ledger.set(pipe, { ...SETTLED, credited: true, miscredited: l.miscredited or not(l.applied) }),
      ),
    }
  }

  /// The first segment of `u`, as the first commit of an attempt reading it a row at a time holds
  /// it.
  pure val FIRST_OF_U: Set[Seg] = Set({ part: "u", from: 0, to: 1 })

  /// The destination answers a key it holds landed with its receipt, then refuses another epoch,
  /// then a commit expecting another key landed, and applies the rest.
  run theDestinationAnswersInTheOrderOfItsChecksTest = {
    val d = { ...fresh("p"), epoch: 2, landed: (1, 3) }
    init.expect(all {
      answer(d, (1, 3), (1, 2)) == AlreadyLanded,
      answer(d, (1, 4), (1, 3)) == FencedEpoch,
      answer(d, (2, 1), (1, 2)) == StaleAfter,
      answer(d, (2, 1), NONE) == StaleAfter,
      answer(d, (2, 1), (1, 3)) == Apply,
      answer(fresh("p"), (0, 1), NONE) == Apply,
    })
  }

  /// Each commit of an attempt expects the one before it landed: the first the key its session
  /// found landed, the next the first's.
  run anAttemptsCommitsEachExpectTheOneBeforeTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .expect(attemptOf(p(1)).after == NONE and attemptOf(p(1)).seq == 1)
      .then(rowOfU(1))
      .expect(all {
        dest.get("p").landed == (1, 1),
        attemptOf(p(1)).after == (1, 1),
        attemptOf(p(1)).seq == 2,
      })
      .then(rowOfU(1))
      .expect(dest.get("p").landed == (1, 2) and LandedKeyAppliesOnce)
      .then(start("p"))
      .then(open(p(2)))
      .expect(attemptOf(p(2)).after == (1, 2) and attemptOf(p(2)).seq == 1)

  /// A copy of the commit the destination holds landed is answered with its receipt and applies
  /// nothing.
  run aCopyOfTheLandedCommitAppliesNothingTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(rowOfU(1))
      .then(copyArrives(p(1), FIRST_OF_U, (1, 1), NONE, true))
      .expect(all {
        dest.get("p").rows.get("u") == Map(0 -> 1, 1 -> 0),
        dest.get("p").landed == (1, 1),
        ExactlyOnce,
        LandedKeyAppliesOnce,
      })

  /// A copy of a commit that a later commit of its session overtook expects a key other than the
  /// one landed: the destination refuses it, and every row stays held once.
  run aCopyALaterCommitOvertookIsRefusedTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(rowOfU(1))
      .then(rowOfU(1))
      .then(copyArrives(p(1), FIRST_OF_U, (1, 1), NONE, true))
      .expect(all {
        dest.get("p").cursor.get("u") == 2,
        dest.get("p").landed == (1, 2),
        ExactlyOnce,
        Acknowledged,
        LandedKeyAppliesOnce,
        PositionOnlyInCommitted,
      })

  /// The negative trace `commit_without_after`: a destination that never compared the key landed
  /// with the one a commit expects would apply that copy again, moving the partition back below
  /// where its source was told it stands and holding its row twice; the source was told no
  /// position of the copy, so it still stands where a commit landed.
  run commitWithoutAfterTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(rowOfU(1))
      .then(rowOfU(1))
      .then(copyArrives(p(1), FIRST_OF_U, (1, 1), NONE, false))
      .expect(all {
        dest.get("p").cursor.get("u") == 1,
        not(LandedKeyAppliesOnce),
        not(ExactlyOnce),
        not(Acknowledged),
        PositionOnlyInCommitted,
      })

  /// A commit that landed with its answer lost is credited to the run once the next open finds
  /// its key landed.
  run aLostAnswerIsCreditedOnceTheNextOpenFindsItsKeyTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Lost))
      .expect(ledger.get("p") == { ...SETTLED, inFlight: (1, 1), applied: true })
      .then(start("p"))
      .then(open(p(2)))
      .expect(all {
        ledger.get("p") == { ...SETTLED, credited: true },
        CreditedOnlyLanded,
      })

  /// A commit that failed is not credited: the next open finds another key landed.
  run aFailedCommitIsNotCreditedTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(rowOfU(1))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Failed))
      .expect(ledger.get("p") == { ...SETTLED, inFlight: (1, 2) })
      .then(start("p"))
      .then(open(p(2)))
      .expect(ledger.get("p") == SETTLED and CreditedOnlyLanded)

  /// A commit a newer session fenced is not credited: the key landed is the newer session's.
  run aFencedCommitIsNotCreditedTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(start("p"))
      .then(open(p(2)))
      .then(rowOfU(2))
      .then(land(p(1), Ok))
      .expect(ledger.get("p") == { ...SETTLED, inFlight: (1, 1) } and dest.get("p").landed == (2, 1))
      .then(start("p"))
      .then(open(p(3)))
      .expect(ledger.get("p") == SETTLED and CreditedOnlyLanded)

  /// A run crediting its commit in flight wherever a commit of the same session landed would
  /// credit one that failed after another landed: the invariant refuses it.
  run creditingByEpochWouldCreditAFailedCommitTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(rowOfU(1))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Failed))
      .then(creditedByEpoch("p"))
      .expect(not(CreditedOnlyLanded))
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
      .then(open(("p", 1)))
      .then(start("q"))
      .then(open(("q", 1)))
      .then(read(("p", 1), "a")).then(checkpoint(("p", 1), "a"))
      .then(read(("p", 1), "u")).then(checkpoint(("p", 1), "u"))
      .then(land(("p", 1), Ok)).then(tell(("p", 1), Ok))
      .then(read(("q", 1), "b"))
      .then(checkpoint(("q", 1), "b"))
      .then(land(("q", 1), Ok))
      .then(tell(("q", 1), Ok))
      .expect(all {
        dest.get("p").cursor == Map("a" -> 1, "u" -> 1),
        dest.get("q").cursor == Map("b" -> 1),
        kept == Map("a" -> 1, "u" -> 1, "b" -> 1),
        ExactlyOnce,
        FencedNeverCommits,
      })
}
