// Re-planning in a continuous run, as ADR 0031 decides it. A following run reads each partition its
// source names and commits what each sealed at a checkpoint. Each re-plan reads the source's
// partitions again: a running partition the plan no longer names is stopped, its rows up to its
// last checkpoint committed by a later commit, those after read again should a plan name it again;
// an ended partition whose end is not committed waits for a later plan; and every partition the
// plan names that no read holds starts from its cursor, where the destination holds it, a bounded
// one that ended on a checkpoint included, so a table is polled. A read of a bounded partition may
// instead end on rows after its last checkpoint, which its end seals: its commit records the
// partition done, the destination holding those rows and no cursor past them, only the one of its
// last checkpoint before them, so a plan passes it over while its source says it ends, and refuses
// the run, ending its reads, once its source says it never ends, since reading it from that
// checkpoint would hold those rows twice and passing it over would miss every row it gains.
// Partitions appear and disappear between plans, and change from bounded to unbounded and back, a
// read keeping the kind its partition had when it started; a read of an unbounded partition may end
// on its own, the rows after its last checkpoint read again by the next; rows arrive as the run
// reads; the run may crash, and the next plans from what was committed.
//
// quint main: replan replan_rows2
// quint steps: --step simulated --max-steps 60
// quint run: --max-samples 2000
// quint verify: tlc
/// The protocol, for a source whose partitions each hold the rows `ROWS` gives them.
module replan_protocol {
  // ---- Bounds

  /// The partitions a source may name.
  pure val PARTS: Set[str] = Set("p0", "p1", "p2")
  /// The rows each partition holds once every row arrived.
  const ROWS: str -> int
  assume RowsHold = PARTS.forall(p => ROWS.get(p) >= 1)
  /// The source's changes to what it names, and the run's crashes, a behavior meets in all.
  const CHANGES: int

  // ---- State

  /// A read of a partition: whether the partition ends where its rows end, as when the read
  /// started; the offset its next segment starts at; how far it read, sealed and last
  /// checkpointed, which only an end on rows after that checkpoint seals past; whether a plan
  /// stopped it; whether it ended; and whether a commit took all it sealed by its end.
  type Read = {
    part: str,
    bounded: bool,
    base: int,
    read: int,
    sealed: int,
    checkpointed: int,
    stopped: bool,
    ended: bool,
    landed: bool,
  }

  /// What the source names.
  var named: Set[str]
  /// The partitions the source names that end where their rows end.
  var bounded: Set[str]
  /// The rows of each partition that have arrived.
  var arrived: str -> int
  /// The run's reads.
  var reads: Set[Read]
  /// Where the destination holds each partition: every row below it is held.
  var committed: str -> int
  /// The cursor of each partition's last checkpoint the destination holds, where a read of it
  /// resumes: where it holds the partition, but for a done one, whose rows past it are held.
  var cursor: str -> int
  /// How often the destination holds each row of each partition.
  var rows: str -> (int -> int)
  /// The source's changes and the run's crashes left.
  var changes: int
  /// The partitions a commit recorded done: a read finished them on rows after its last
  /// checkpoint.
  var done: Set[str]
  /// Whether the last plan refused the run.
  var refused: bool

  pure def zero(part: str): int -> int = 0.to(ROWS.get(part) - 1).mapBy(_ => 0)

  /// The reads of `part`.
  def readsOf(part: str): Set[Read] = reads.filter(r => r.part == part)

  /// A read of `part` from its cursor.
  def fresh(part: str): Read = {
    part: part,
    bounded: bounded.contains(part),
    base: cursor.get(part),
    read: cursor.get(part),
    sealed: cursor.get(part),
    checkpointed: cursor.get(part),
    stopped: false,
    ended: false,
    landed: false,
  }

  /// Every variable but `reads`, `committed` and `rows`, as it is.
  action keep: bool = all {
    cursor' = cursor,
    named' = named,
    bounded' = bounded,
    arrived' = arrived,
    changes' = changes,
    done' = done,
    refused' = refused,
  }

  // ---- Actions

  action init = all {
    named' = Set("p0", "p1"),
    bounded' = Set("p0"),
    arrived' = PARTS.mapBy(p => ROWS.get(p) - 1),
    reads' = Set(),
    committed' = PARTS.mapBy(_ => 0),
    cursor' = PARTS.mapBy(_ => 0),
    rows' = PARTS.mapBy(p => zero(p)),
    changes' = CHANGES,
    done' = Set(),
    refused' = false,
  }

  /// The run plans again from what the source names: a running read it no longer names stops; an
  /// ended read whose end is committed, and a stopped one with nothing left to commit, give their
  /// places up; each named partition no read then holds starts from where the destination holds
  /// it, unless it is done. A done partition the source says ends is passed over, and one it says
  /// never ends refuses the run, whose reads all end.
  action replan = {
    val held = reads.filter(r => not(r.ended and r.landed) and not(r.stopped and r.sealed <= r.base))
    val settled = held.map(r => if (named.contains(r.part) or r.ended) r else { ...r, stopped: true })
    val unread = named.filter(p => settled.forall(r => r.part != p))
    val refuses = unread.exists(p => done.contains(p) and not(bounded.contains(p)))
    val started = unread.exclude(done).map(p => fresh(p))
    all {
      reads' = if (refuses) Set() else settled.union(started),
      refused' = refuses,
      committed' = committed,
      cursor' = cursor,
      rows' = rows,
      named' = named,
      bounded' = bounded,
      arrived' = arrived,
      changes' = changes,
      done' = done,
    }
  }

  /// The read `r` reads its partition's next row.
  action readRow(r: Read): bool = all {
    reads.contains(r),
    not(r.stopped),
    not(r.ended),
    r.read < arrived.get(r.part),
    reads' = reads.exclude(Set(r)).union(Set({ ...r, read: r.read + 1 })),
    committed' = committed,
    rows' = rows,
    keep,
  }

  /// The read `r` checkpoints where it read: its rows so far are sealed, and a read of a bounded
  /// partition to where its rows end ends, its end sealed with them.
  action checkpoint(r: Read): bool = {
    val ends = r.bounded and r.read == arrived.get(r.part)
    all {
      reads.contains(r),
      not(r.stopped),
      not(r.ended),
      r.sealed < r.read or ends,
      reads' = reads.exclude(Set(r)).union(Set({ ...r, sealed: r.read, checkpointed: r.read, ended: ends })),
      committed' = committed,
      rows' = rows,
      keep,
    }
  }

  /// The read `r` of a bounded partition reaches where its rows end on rows after its last
  /// checkpoint: its end seals them, and the commit that takes it records the partition done.
  action finish(r: Read): bool = all {
    reads.contains(r),
    r.bounded,
    not(r.stopped),
    not(r.ended),
    r.read == arrived.get(r.part),
    r.sealed < r.read,
    reads' = reads.exclude(Set(r)).union(Set({ ...r, sealed: r.read, ended: true })),
    committed' = committed,
    rows' = rows,
    keep,
  }

  /// The read `r` of an unbounded partition ends on its own: the rows it read after its last
  /// checkpoint are left for the next read, which a plan starts once a commit took what it sealed.
  action pause(r: Read): bool = all {
    reads.contains(r),
    not(r.bounded),
    not(r.stopped),
    not(r.ended),
    reads' = reads.exclude(Set(r)).union(Set({ ...r, read: r.sealed, ended: true, landed: r.sealed <= r.base })),
    committed' = committed,
    rows' = rows,
    keep,
  }

  /// The run commits every read's sealed segment, the rows from where its last ended to its seal,
  /// and every sealed end with the cursor of its last checkpoint, recording done each partition
  /// whose read ended on rows past it; a stopped read drains, its rows committed, and gives its
  /// place up.
  action commit = {
    val pending = reads.filter(r => r.sealed > r.base or (r.ended and not(r.landed)))
    all {
      pending != Set(),
      rows' = pending.fold(rows, (rs, r) =>
        rs.set(r.part, 0.to(ROWS.get(r.part) - 1).filter(o => r.base <= o and o < r.sealed).fold(rs.get(r.part), (counts, o) => counts.set(o, counts.get(o) + 1)))),
      committed' = pending.fold(committed, (c, r) => c.set(r.part, r.sealed)),
      cursor' = pending.fold(cursor, (c, r) => c.set(r.part, r.checkpointed)),
      reads' = reads.filter(r => not(r.stopped)).map(r => { ...r, base: r.sealed, landed: r.ended }),
      done' = done.union(pending.filter(r => r.checkpointed < r.sealed).map(r => r.part)),
      named' = named,
      bounded' = bounded,
      arrived' = arrived,
      changes' = changes,
      refused' = refused,
    }
  }

  /// A row of `part` arrives.
  action arrive(part: str): bool = all {
    arrived.get(part) < ROWS.get(part),
    arrived' = arrived.set(part, arrived.get(part) + 1),
    named' = named,
    bounded' = bounded,
    changes' = changes,
    reads' = reads,
    committed' = committed,
    rows' = rows,
    cursor' = cursor,
    done' = done,
    refused' = refused,
  }

  /// The source names `plan` from now on, ending where their rows end those of `ending`.
  action reshape(plan: Set[str], ending: Set[str]): bool = all {
    changes > 0,
    plan != named or ending != bounded,
    named' = plan,
    bounded' = ending,
    changes' = changes - 1,
    arrived' = arrived,
    reads' = reads,
    committed' = committed,
    rows' = rows,
    cursor' = cursor,
    done' = done,
    refused' = refused,
  }

  /// The run crashes: its reads end with it, and the next run plans from what was committed.
  action crash: bool = all {
    changes > 0,
    reads != Set(),
    reads' = Set(),
    changes' = changes - 1,
    named' = named,
    bounded' = bounded,
    arrived' = arrived,
    committed' = committed,
    rows' = rows,
    cursor' = cursor,
    done' = done,
    refused' = refused,
  }

  action step = any {
    replan,
    commit,
    crash,
    all { reads != Set(), nondet r = reads.oneOf() any { readRow(r), checkpoint(r), pause(r), finish(r) } },
    all {
      PARTS.exists(p => arrived.get(p) < ROWS.get(p)),
      nondet p = PARTS.filter(x => arrived.get(x) < ROWS.get(x)).oneOf()
      arrive(p),
    },
    all {
      changes > 0,
      nondet plan = PARTS.powerset().oneOf()
      nondet ending = plan.powerset().oneOf()
      reshape(plan, ending),
    },
  }

  /// The steps `quint run` takes: `step`'s, with the source's changes and crashes seldom.
  action simulated = {
    nondet rare = 1.to(6).oneOf()
    any {
      all { rare <= 2, replan },
      commit,
      all { rare == 1, crash },
      all { reads != Set(), nondet r = reads.oneOf() any { readRow(r), checkpoint(r), pause(r), finish(r) } },
      all {
        PARTS.exists(p => arrived.get(p) < ROWS.get(p)),
        nondet p = PARTS.filter(x => arrived.get(x) < ROWS.get(x)).oneOf()
        arrive(p),
      },
      all {
        rare == 1,
        changes > 0,
        nondet plan = PARTS.powerset().oneOf()
        nondet ending = plan.powerset().oneOf()
        reshape(plan, ending),
      },
    }
  }

  // ---- Invariants

  /// No partition is lost or doubled: no two reads of one partition at once; a read's next
  /// segment starts where the destination holds its partition, so no row is skipped or read twice;
  /// and every row below a partition's position is held once, none past it.
  val NoPartitionLostOrDoubled = PARTS.forall(p => all {
    readsOf(p).size() <= 1,
    readsOf(p).forall(r => r.base == committed.get(p)),
    0.to(ROWS.get(p) - 1).forall(o => rows.get(p).get(o) == (if (o < committed.get(p)) 1 else 0)),
  })

  /// Every variable.
  val vars = (named, bounded, arrived, reads, committed, cursor, rows, changes, done, refused)

  /// Whether a read of `p` a plan stopped holds sealed rows not yet committed.
  def draining(p: str): bool = readsOf(p).exists(r => r.stopped and r.sealed > r.base)

  /// A removed partition drains: a read a plan stopped with sealed rows has them committed, or the
  /// run crashes first and the next reads them again.
  temporal RemovedDrains = weakFair(commit, vars).implies(PARTS.forall(p => draining(p).leadsTo(not(draining(p)))))

  /// Whether a plan is to read `p`: the source names it, and says it ends where no read finished
  /// it.
  def due(p: str): bool = named.contains(p) and not(done.contains(p) and bounded.contains(p))

  /// Whether the reads of `p` follow what the source names: a partition due to be read has a read
  /// running, and any other has none.
  def follows(p: str): bool =
    if (due(p)) readsOf(p).exists(r => not(r.stopped) and not(r.ended))
    else readsOf(p).forall(r => r.stopped or r.ended)

  /// Plans follow the source: a partition due to be read is started, and read again once a read of
  /// it that stopped or ended gave its place up, and any other has no read running, or a plan
  /// refuses the run, as it does where a done partition the source names never ends.
  temporal PlansFollowTheSource =
    (weakFair(replan, vars) and weakFair(commit, vars)).implies(PARTS.forall(p => not(follows(p)).leadsTo(follows(p) or refused)))

  // ---- Tests

  /// The read of `p` reads its next row.
  action readOn(p: str): bool = all { readsOf(p).size() == 1, readRow(readsOf(p).fold(fresh(p), (_, r) => r)) }

  /// The read of `p` checkpoints.
  action checkpointOn(p: str): bool = all { readsOf(p).size() == 1, checkpoint(readsOf(p).fold(fresh(p), (_, r) => r)) }

  /// The read of `p` ends on its own.
  action pauseOn(p: str): bool = all { readsOf(p).size() == 1, pause(readsOf(p).fold(fresh(p), (_, r) => r)) }

  /// The read of `p` reaches where its rows end on rows after its last checkpoint.
  action finishOn(p: str): bool = all { readsOf(p).size() == 1, finish(readsOf(p).fold(fresh(p), (_, r) => r)) }

  /// A plan that starts every partition it names beside whatever read of it there is, which a
  /// plan never does.
  action replanEager = all {
    reads' = reads.union(named.map(p => fresh(p))),
    committed' = committed,
    rows' = rows,
    keep,
  }

  /// A plan that starts every partition it names no read holds, done ones too, from its cursor,
  /// which a plan never does.
  action replanDone = all {
    reads' = reads.union(named.filter(p => readsOf(p) == Set()).map(p => fresh(p))),
    committed' = committed,
    rows' = rows,
    keep,
  }

  /// A plan that starts every partition it names no read holds from its beginning, not from where
  /// the destination holds it, which a plan never does.
  action replanFromStart = all {
    reads' = reads.union(named.filter(p => readsOf(p) == Set()).map(p => { ...fresh(p), base: 0, read: 0, sealed: 0, checkpointed: 0 })),
    committed' = committed,
    rows' = rows,
    keep,
  }
}

/// The protocol with one partition holding two rows and the others one, and two changes: the bound
/// TLC explores whole.
module replan {
  import replan_protocol(ROWS = Map("p0" -> 1, "p1" -> 2, "p2" -> 1), CHANGES = 2).*

  /// Every partition starts with one row still to arrive.
  run everyPartitionAwaitsItsRowTest = init.expect(arrived == PARTS.mapBy(p => ROWS.get(p) - 1))

  /// A read holds a sealed row and a row after it when a plan stops it: the sealed row is
  /// committed, and the next read starts after it.
  run aStoppedReadsUnsealedRowIsReadAgainTest =
    init
      .then(replan)
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(arrive("p1"))
      .then(readOn("p1"))
      .expect(readsOf("p1").exists(r => r.sealed == 1 and r.read == 2))
      .then(reshape(Set("p0"), Set("p0")))
      .then(replan)
      .then(commit)
      .then(reshape(Set("p0", "p1"), Set("p0")))
      .then(replan)
      .expect(all {
        readsOf("p1").exists(r => r.base == 1 and r.read == 1 and not(r.stopped)),
        rows.get("p1") == Map(0 -> 1, 1 -> 0),
      })
}

/// The protocol with each partition holding two rows, and three changes, which only `quint test`
/// and `quint run` explore: TLC's bound holds less.
module replan_rows2 {
  import replan_protocol(ROWS = Map("p0" -> 2, "p1" -> 2, "p2" -> 2), CHANGES = 3).*

  /// A partition the source stops naming is stopped: its sealed row is committed, and its read
  /// drains.
  run aPartitionTheSourceDropsDrainsItsSealedRowsTest =
    init
      .then(replan)
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(reshape(Set("p0"), Set("p0")))
      .then(replan)
      .expect(draining("p1"))
      .then(commit)
      .expect(all {
        not(draining("p1")),
        readsOf("p1") == Set(),
        committed.get("p1") == 1,
        rows.get("p1").get(0) == 1,
        NoPartitionLostOrDoubled,
      })

  /// A partition named again is read from where its drained read was committed.
  run aPartitionNamedAgainResumesWhereItWasCommittedTest =
    init
      .then(replan)
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(reshape(Set("p0"), Set("p0")))
      .then(replan)
      .then(commit)
      .then(reshape(Set("p0", "p1"), Set("p0")))
      .then(replan)
      .expect(readsOf("p1").forall(r => r.base == 1))
      .then(arrive("p1"))
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(commit)
      .expect(committed.get("p1") == 2 and rows.get("p1") == Map(0 -> 1, 1 -> 1) and NoPartitionLostOrDoubled)

  /// A bounded partition read to its end ends; once its end is committed, the next plan reads it
  /// again from there, so rows that arrived since are read.
  run aBoundedPartitionIsPolledOnceItsEndCommitsTest =
    init
      .then(replan)
      .then(readOn("p0"))
      .then(checkpointOn("p0"))
      .expect(readsOf("p0").forall(r => r.ended))
      .then(commit)
      .then(arrive("p0"))
      .then(replan)
      .expect(readsOf("p0").forall(r => r.base == 1 and not(r.ended)))
      .then(readOn("p0"))
      .then(checkpointOn("p0"))
      .then(commit)
      .expect(committed.get("p0") == 2 and NoPartitionLostOrDoubled)

  /// A run that crashed leaves the next to read from what was committed, nothing twice.
  run aCrashLeavesTheNextRunWhatWasCommittedTest =
    init
      .then(replan)
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(crash)
      .then(replan)
      .expect(readsOf("p1").forall(r => r.base == 0))
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(commit)
      .expect(rows.get("p1").get(0) == 1 and NoPartitionLostOrDoubled)

  /// A read keeps the kind its partition had when it started: one started bounded ends where its
  /// rows end though the source now names the partition unbounded, and the next, started
  /// unbounded, does not.
  run aReadKeepsTheKindItStartedWithTest =
    init
      .then(replan)
      .then(reshape(Set("p0", "p1"), Set()))
      .then(readOn("p0"))
      .then(checkpointOn("p0"))
      .expect(readsOf("p0").size() == 1 and readsOf("p0").forall(r => r.ended))
      .then(commit)
      .then(arrive("p0"))
      .then(replan)
      .then(readOn("p0"))
      .then(checkpointOn("p0"))
      .expect(readsOf("p0").size() == 1 and readsOf("p0").forall(r => r.base == 1 and r.sealed == 2 and not(r.ended)))
      .then(commit)
      .expect(committed.get("p0") == 2 and NoPartitionLostOrDoubled)

  /// A read of an unbounded partition that ends on its own leaves the rows after its checkpoint
  /// unsealed: plans wait until a commit takes what it sealed, then read it again from there.
  run aReadThatPausesIsReadAgainFromWhereItWasCommittedTest =
    init
      .then(replan)
      .then(arrive("p1"))
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(readOn("p1"))
      .then(pauseOn("p1"))
      .expect(readsOf("p1").size() == 1 and readsOf("p1").forall(r => r.ended and r.read == 1 and not(r.landed)))
      .then(replan)
      .expect(readsOf("p1").size() == 1 and readsOf("p1").forall(r => r.ended))
      .then(commit)
      .then(replan)
      .expect(readsOf("p1").size() == 1 and readsOf("p1").forall(r => r.base == 1 and not(r.ended)))
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(commit)
      .expect(committed.get("p1") == 2 and rows.get("p1") == Map(0 -> 1, 1 -> 1) and NoPartitionLostOrDoubled)

  /// Reads of unbounded partitions that end on their own with nothing sealed give their places up
  /// at once: with no commit between, the next plan reads p0 again and starts p2 in place of p1.
  run aReadThatPausesWithNothingSealedIsReadAgainAtTheNextPlanTest =
    init
      .then(reshape(Set("p0", "p1"), Set()))
      .then(replan)
      .then(pauseOn("p0"))
      .then(pauseOn("p1"))
      .then(reshape(Set("p0", "p2"), Set()))
      .then(replan)
      .expect(all {
        readsOf("p0").size() == 1,
        readsOf("p0").forall(r => r.base == 0 and not(r.ended)),
        readsOf("p1") == Set(),
        readsOf("p2").size() == 1,
        NoPartitionLostOrDoubled,
      })

  /// A bounded partition a read finished on rows after its last checkpoint is done: the next plan
  /// passes it over, though rows arrived since.
  run aBoundedPartitionFinishedOnRowsIsPassedOverTest =
    init
      .then(replan)
      .then(readOn("p0"))
      .then(finishOn("p0"))
      .expect(readsOf("p0").forall(r => r.ended and r.checkpointed == 0 and r.sealed == 1))
      .then(commit)
      .expect(done == Set("p0") and committed.get("p0") == 1 and cursor.get("p0") == 0)
      .then(arrive("p0"))
      .then(replan)
      .expect(readsOf("p0") == Set() and not(refused) and NoPartitionLostOrDoubled)

  /// A done partition the source then says never ends refuses the plan, which ends every read;
  /// once the source says it ends again, plans pass it over and read the rest.
  run aDonePartitionTheSourceSaysNeverEndsRefusesThePlanTest =
    init
      .then(replan)
      .then(readOn("p0"))
      .then(finishOn("p0"))
      .then(commit)
      .then(reshape(Set("p0", "p1"), Set()))
      .then(replan)
      .expect(refused and reads == Set())
      .then(reshape(Set("p0", "p1"), Set("p0")))
      .then(replan)
      .expect(not(refused) and readsOf("p0") == Set() and readsOf("p1").size() == 1)

  /// A plan that started a done partition the source says never ends would read it from its
  /// cursor, before the rows its read finished on, and hold them twice: the invariant refuses it.
  run aPlanReadingADonePartitionAgainWouldHoldRowsTwiceTest =
    init
      .then(replan)
      .then(readOn("p0"))
      .then(finishOn("p0"))
      .then(commit)
      .then(reshape(Set("p0"), Set()))
      .then(replan)
      .expect(refused and reads == Set())
      .then(replanDone)
      .expect(readsOf("p0").forall(r => r.base == 0) and not(NoPartitionLostOrDoubled))
      .then(readOn("p0"))
      .then(checkpointOn("p0"))
      .then(commit)
      .expect(rows.get("p0").get(0) == 2)

  /// A plan that started a partition beside its stopped read would read it twice at once: the
  /// invariant refuses it.
  run aPlanBesideAStoppedReadWouldDoubleItTest =
    init
      .then(replan)
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(reshape(Set("p0"), Set("p0")))
      .then(replan)
      .then(reshape(Set("p0", "p1"), Set("p0")))
      .then(replanEager)
      .expect(not(NoPartitionLostOrDoubled))

  /// A plan that read a partition from its beginning again would hold its committed rows twice: the
  /// invariant refuses it.
  run aPlanFromTheBeginningWouldHoldRowsTwiceTest =
    init
      .then(replan)
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(commit)
      .then(crash)
      .then(replanFromStart)
      .then(readOn("p1"))
      .then(checkpointOn("p1"))
      .then(commit)
      .expect(not(NoPartitionLostOrDoubled))
}
