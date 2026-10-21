// Re-planning in a continuous run, as ADR 0031 decides it. A following run reads each partition
// its source names and commits what each sealed at a checkpoint. Each re-plan reads the source's
// partitions again: a running partition the plan no longer names is stopped, its rows up to its
// last checkpoint committed by a later commit, those after read again should a plan name it again;
// an ended partition whose end is not committed waits for a later plan; and every partition the
// plan names that no read holds starts from where the destination holds it, a bounded one that
// ended included, so a table is polled. Partitions appear and disappear between plans, and change
// from bounded to unbounded and back, a read keeping the kind its partition had when it started; a
// read of an unbounded partition may end on its own, the rows after its last checkpoint read again
// by the next; rows arrive as the run reads; the run may crash, and the next plans from what was
// committed.
//
// quint steps: --step simulated --max-steps 60
// quint run: --max-samples 2000
// quint verify: tlc
module replan {
  // ---- Bounds

  /// The partitions a source may name.
  pure val PARTS: Set[str] = Set("p0", "p1", "p2")
  /// The rows each partition holds once every row arrived.
  pure val ROWS = 2
  /// The source's changes to what it names, and the run's crashes, a behavior meets in all.
  pure val CHANGES = 3

  // ---- State

  /// A read of a partition: whether the partition ends where its rows end, as when the read
  /// started; the offset its next segment starts at; how far it read and sealed; whether a plan
  /// stopped it; whether it ended; and whether a commit took all it sealed by its end.
  type Read = {
    part: str,
    bounded: bool,
    base: int,
    read: int,
    sealed: int,
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
  /// Where the destination holds each partition.
  var committed: str -> int
  /// How often the destination holds each row of each partition.
  var rows: str -> (int -> int)
  /// The source's changes and the run's crashes left.
  var changes: int

  pure def zero(part: str): int -> int = 0.to(ROWS - 1).mapBy(_ => 0)

  /// The reads of `part`.
  def readsOf(part: str): Set[Read] = reads.filter(r => r.part == part)

  /// A read of `part` from where the destination holds it.
  def fresh(part: str): Read = {
    part: part,
    bounded: bounded.contains(part),
    base: committed.get(part),
    read: committed.get(part),
    sealed: committed.get(part),
    stopped: false,
    ended: false,
    landed: false,
  }

  /// Every variable but `reads`, `committed` and `rows`, as it is.
  action keep: bool = all {
    named' = named,
    bounded' = bounded,
    arrived' = arrived,
    changes' = changes,
  }

  // ---- Actions

  action init = all {
    named' = Set("p0", "p1"),
    bounded' = Set("p0"),
    arrived' = PARTS.mapBy(_ => 1),
    reads' = Set(),
    committed' = PARTS.mapBy(_ => 0),
    rows' = PARTS.mapBy(p => zero(p)),
    changes' = CHANGES,
  }

  /// The run plans again from what the source names: a running read it no longer names stops; an
  /// ended read whose end is committed, and a stopped one with nothing left to commit, give their
  /// places up; each named partition no read then holds starts from where the destination holds
  /// it.
  action replan = {
    val held = reads.filter(r => not(r.ended and r.landed) and not(r.stopped and r.sealed <= r.base))
    val settled = held.map(r => if (named.contains(r.part) or r.ended) r else { ...r, stopped: true })
    val started = named.filter(p => settled.forall(r => r.part != p)).map(p => fresh(p))
    all {
      reads' = settled.union(started),
      committed' = committed,
      rows' = rows,
      keep,
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
      reads' = reads.exclude(Set(r)).union(Set({ ...r, sealed: r.read, ended: ends })),
      committed' = committed,
      rows' = rows,
      keep,
    }
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
  /// and every sealed end; a stopped read drains, its rows committed, and gives its place up.
  action commit = {
    val pending = reads.filter(r => r.sealed > r.base or (r.ended and not(r.landed)))
    all {
      pending != Set(),
      rows' = pending.fold(rows, (rs, r) =>
        rs.set(r.part, 0.to(ROWS - 1).filter(o => r.base <= o and o < r.sealed).fold(rs.get(r.part), (counts, o) => counts.set(o, counts.get(o) + 1)))),
      committed' = pending.fold(committed, (c, r) => c.set(r.part, r.sealed)),
      reads' = reads.filter(r => not(r.stopped)).map(r => { ...r, base: r.sealed, landed: r.ended }),
      keep,
    }
  }

  /// A row of `part` arrives.
  action arrive(part: str): bool = all {
    arrived.get(part) < ROWS,
    arrived' = arrived.set(part, arrived.get(part) + 1),
    named' = named,
    bounded' = bounded,
    changes' = changes,
    reads' = reads,
    committed' = committed,
    rows' = rows,
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
  }

  action step = any {
    replan,
    commit,
    crash,
    all { reads != Set(), nondet r = reads.oneOf() any { readRow(r), checkpoint(r), pause(r) } },
    all {
      PARTS.exists(p => arrived.get(p) < ROWS),
      nondet p = PARTS.filter(x => arrived.get(x) < ROWS).oneOf()
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
      all { reads != Set(), nondet r = reads.oneOf() any { readRow(r), checkpoint(r), pause(r) } },
      all {
        PARTS.exists(p => arrived.get(p) < ROWS),
        nondet p = PARTS.filter(x => arrived.get(x) < ROWS).oneOf()
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

  // ---- Invariants, named as the checks that hold the same

  /// No partition is lost or doubled: no two reads of one partition at once; a read's next
  /// segment starts where the destination holds its partition, so no row is skipped or read twice;
  /// and every row below a partition's position is held once, none past it.
  val NoPartitionLostOrDoubled = PARTS.forall(p => all {
    readsOf(p).size() <= 1,
    readsOf(p).forall(r => r.base == committed.get(p)),
    0.to(ROWS - 1).forall(o => rows.get(p).get(o) == (if (o < committed.get(p)) 1 else 0)),
  })

  /// Every variable.
  val vars = (named, bounded, arrived, reads, committed, rows, changes)

  /// Whether a read of `p` a plan stopped holds sealed rows not yet committed.
  def draining(p: str): bool = readsOf(p).exists(r => r.stopped and r.sealed > r.base)

  /// A removed partition drains: a read a plan stopped with sealed rows has them committed, or the
  /// run crashes first and the next reads them again.
  temporal RemovedDrains = weakFair(commit, vars).implies(PARTS.forall(p => draining(p).leadsTo(not(draining(p)))))

  // ---- Goals: states the simulation reaches, each the start of a test

  /// A partition the source no longer names was stopped with rows sealed, and drained to a commit.
  val removedDrained = committed.keys().exists(p => committed.get(p) > 0 and not(named.contains(p))) and PARTS.forall(p => not(draining(p)))
  /// A partition the source names again is read from where its stopped read was committed.
  val resumed = reads.exists(r => r.base > 0 and not(r.stopped))

  // ---- Tests

  /// The read of `p` reads its next row.
  action readOn(p: str): bool = all { readsOf(p).size() == 1, readRow(readsOf(p).fold(fresh(p), (_, r) => r)) }

  /// The read of `p` checkpoints.
  action checkpointOn(p: str): bool = all { readsOf(p).size() == 1, checkpoint(readsOf(p).fold(fresh(p), (_, r) => r)) }

  /// The read of `p` ends on its own.
  action pauseOn(p: str): bool = all { readsOf(p).size() == 1, pause(readsOf(p).fold(fresh(p), (_, r) => r)) }

  /// A plan that starts every partition it names beside whatever read of it there is, which a
  /// plan never does.
  action replanEager = all {
    reads' = reads.union(named.map(p => fresh(p))),
    committed' = committed,
    rows' = rows,
    keep,
  }

  /// A plan that starts every partition it names no read holds from its beginning, not from where
  /// the destination holds it, which a plan never does.
  action replanFromStart = all {
    reads' = reads.union(named.filter(p => readsOf(p) == Set()).map(p => { ...fresh(p), base: 0, read: 0, sealed: 0 })),
    committed' = committed,
    rows' = rows,
    keep,
  }

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
