// Exactly-once delivery with acknowledged positions, as ADRs 0004 and 0030 decide it. Each attempt
// of a pipeline, its load, opens a session at the destination, which fences every session of the
// pipeline opened before, reads each partition from where the destination holds it, checkpoints,
// and commits what it sealed under the session's epoch; once the commit lands, it tells each
// partition's source the position the commit landed with. A source that reads again serves any
// position; one that cannot forgets what precedes the position it was last told, refuses a read
// from before it, and serves everything after it to every reader that asks. An attempt may crash,
// its commit may fail or land with its answer lost, a telling may fail, and an attempt may start
// while another of its pipeline is live, as a second run's would, both reading a partition whose
// source cannot read again. A retry is an attempt of its own: the model does not group attempts
// into runs. The destination may be shared by several pipelines: `quint test` and `quint run` take
// two, and the model checkers one.
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

  /// What the destination holds of a pipeline: its newest session's epoch, and each partition's
  /// position and how often each row of it is held.
  type Dest = { epoch: int, cursor: Part -> int, rows: Part -> (int -> int) }

  /// Where an attempt stands: before its session opens, reading, its commit landed.
  type Stage = | Opening | Reading | Landed

  /// An attempt: its pipeline, the epoch of the session it holds (0 before it opens one), where
  /// it stands, each partition's rows read, sealed by a checkpoint and committed, and the
  /// segments of the commit that landed and whose sources it has yet to tell.
  type Attempt = {
    pipe: Pipe,
    epoch: int,
    stage: Stage,
    read: Part -> int,
    sealed: Part -> int,
    committed: Part -> int,
    landing: Set[Seg],
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

  // ---- Helpers

  pure def zero(parts: Set[Part]): Part -> int = parts.mapBy(_ => 0)

  pure def none(part: Part): int -> int = 0.to(MAXROWS - 1).filter(o => o < rowsOf(part)).mapBy(_ => 0)

  pure def fresh(pipe: Pipe): Dest =
    { epoch: 0, cursor: zero(partsOf(pipe)), rows: partsOf(pipe).mapBy(part => none(part)) }

  /// The destination `d` once it applied `segs`.
  pure def applied(d: Dest, segs: Set[Seg]): Dest = {
    val rows = segs.fold(d.rows, (rs, s) =>
      rs.set(s.part, 0.to(MAXROWS - 1).filter(o => s.from <= o and o < s.to).fold(rs.get(s.part), (counts, o) => counts.set(o, counts.get(o) + 1))))
    val cursor = segs.fold(d.cursor, (c, s) => c.set(s.part, s.to))
    { ...d, rows: rows, cursor: cursor }
  }

  /// The positions `segs` land with.
  pure def positions(segs: Set[Seg]): Set[(Part, int)] = segs.map(s => (s.part, s.to))

  /// The sources of `kept` once each partition of `segs` is told the position it lands with.
  pure def told(kept: Part -> int, segs: Set[Seg]): Part -> int = segs.fold(kept, (k, s) => k.set(s.part, s.to))

  /// The attempts live.
  val live: Set[(Pipe, int)] = attempts.keys()

  def attemptOf(k: (Pipe, int)): Attempt = attempts.get(k)

  /// The next state: every variable as given.
  action keepAll(
    d: Pipe -> Dest, k: Part -> int, r: (Pipe, int) -> Attempt, s: Pipe -> int, f: int,
    landedNow: Set[(Part, int)], fenced: bool
  ): bool = all {
    dest' = d,
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
    kept' = zero(PARTS),
    attempts' = Map(),
    started' = PIPES.mapBy(_ => 0),
    faults' = FAULTS,
    landed' = Set(),
    fencedLanded' = false,
  }

  /// An attempt of `pipe` starts, holding no session yet. An attempt started while another of
  /// the pipeline is live meets a fault.
  action start(pipe: Pipe): bool = {
    val load = started.get(pipe) + 1
    val concurrent = attempts.keys().exists(k => k._1 == pipe)
    val attempt = {
      pipe: pipe, epoch: 0, stage: Opening,
      read: zero(partsOf(pipe)), sealed: zero(partsOf(pipe)), committed: zero(partsOf(pipe)),
      landing: Set(),
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

  /// The attempt `k` opens its session, which fences every older one, and plans each partition
  /// from where the destination holds it; a source that cannot read again refuses a read from
  /// before where it stands, and the attempt ends.
  action open(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val planned = partsOf(attempt.pipe).forall(part => rereads(part) or kept.get(part) <= d.cursor.get(part))
    val epoch = d.epoch + 1
    all {
      live.contains(k),
      attempt.stage == Opening,
      keepAll(
        dest.set(attempt.pipe, { ...d, epoch: epoch }),
        kept,
        if (planned) attempts.set(k, { ...attempt, epoch: epoch, stage: Reading, read: d.cursor, sealed: d.cursor, committed: d.cursor })
        else without(k),
        started, faults, landed, fencedLanded,
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

  /// The attempt `k` commits what it sealed at the destination, meeting `outcome`: a session the
  /// destination fenced is refused; otherwise the commit lands, but where it failed, and its
  /// answer is lost where it landed so. No source is told anything yet.
  action land(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val segs = sealedSegs(attempt)
    val fenced = d.epoch != attempt.epoch
    val lands = not(fenced) and outcome != Failed
    val answered = not(fenced) and outcome == Ok
    all {
      live.contains(k),
      attempt.stage == Reading,
      segs != Set(),
      outcome == Ok or faults > 0,
      keepAll(
        if (lands) dest.set(attempt.pipe, applied(d, segs)) else dest,
        kept,
        if (answered) attempts.set(k, { ...attempt, stage: Landed, landing: segs }) else without(k),
        started,
        if (outcome == Ok) faults else faults - 1,
        if (lands) landed.union(positions(segs)) else landed,
        fencedLanded or (lands and fenced),
      ),
    }
  }

  /// The attempt `k` tells the source of every partition its commit landed with that position,
  /// meeting `outcome`, and reads on: a telling that fails may have moved the sources all the
  /// same.
  action tell(k: (Pipe, int), outcome: Outcome): bool = {
    val attempt = attemptOf(k)
    all {
      live.contains(k),
      attempt.stage == Landed,
      outcome == Ok or faults > 0,
      keepAll(
        dest,
        if (outcome == Failed) kept else told(kept, attempt.landing),
        if (outcome == Ok) attempts.set(k, { ...attempt, stage: Reading, committed: attempt.sealed, landing: Set() }) else without(k),
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

  /// A source's position moves only as it is told a committed position, and then to that one: a
  /// position a commit landed with.
  val PositionOnlyInCommitted = PARTS.forall(part =>
    kept.get(part) == 0 or landed.contains((part, kept.get(part))))

  /// Every pipeline holds every row of each of its partitions.
  val converged = PIPES.forall(pipe => partsOf(pipe).forall(part => dest.get(pipe).cursor.get(part) == rowsOf(part)))

  /// Every variable.
  val vars = (dest, kept, attempts, started, faults, landed, fencedLanded)

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

  /// A destination that lands a commit whatever its session's epoch, which it never does.
  action landedFenced(k: (Pipe, int)): bool = {
    val attempt = attemptOf(k)
    val d = dest.get(attempt.pipe)
    val segs = sealedSegs(attempt)
    all {
      live.contains(k),
      attempt.stage == Reading,
      keepAll(
        dest.set(attempt.pipe, applied(d, segs)),
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
      keepAll(dest.set(attempt.pipe, applied(d, segs)), kept, attempts, started, faults, landed, fencedLanded),
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

  /// A telling that fails may move a source all the same, and only to the position its commit
  /// landed with.
  run aTellingLostStillMovesTheSourceOnlyToALandedPositionTest =
    init
      .then(start("p"))
      .then(open(p(1)))
      .then(read(p(1), "u"))
      .then(checkpoint(p(1), "u"))
      .then(land(p(1), Ok))
      .then(tell(p(1), Lost))
      .expect(all {
        not(live.contains(p(1))),
        kept.get("u") == 1,
        Acknowledged,
        PositionOnlyInCommitted,
      })

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
