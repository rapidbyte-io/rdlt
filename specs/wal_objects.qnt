// A write-ahead log on object storage, as ADR 0051 decides it: a log is opened by creating its
// mark once, with a token; a chunk is published by creating its head where its name is free,
// kept only while the log's mark is there; a removal deletes the mark, then the log's heads.
// Any create may lose its answer and land later. Each operation is one step here: an open's
// listing, a publish's look at the mark and a removal's listing are not apart from it.
module wal_objects {
  /// The loads a run opens logs for.
  pure val LOADS: Set[int] = 1.to(2)
  /// The numbers of the chunks a log publishes.
  pure val NUMBERS: Set[int] = 0.to(1)
  /// The tokens a create marks its object with.
  pure val TOKENS: Set[int] = 1.to(2)

  /// An object's name: a log's open mark, or a chunk's head.
  type Name =
    | Mark(int)
    | HeadChunk({ load: int, number: int })

  /// An object: its name, and the token of the create that made it.
  type Object = { name: Name, token: int }

  pure val NAMES: Set[Name] =
    LOADS.map(l => Mark(l)).union(tuples(LOADS, NUMBERS).map(((l, n)) => HeadChunk({ load: l, number: n })))

  pure val OBJECTS: Set[Object] =
    tuples(NAMES, TOKENS).map(((name, token)) => { name: name, token: token })

  /// The load whose log `name` belongs to.
  pure def loadOf(name: Name): int = match name {
    | Mark(l) => l
    | HeadChunk(h) => h.load
  }

  /// What the store holds.
  var store: Set[Object]
  /// Creates that lost their answer and may still land.
  var late: Set[Object]
  /// Loads whose open was answered: their logs are this run's to publish in.
  var opened: Set[int]
  /// Loads whose open lost its answer before its mark landed: never used again.
  var abandoned: Set[int]
  /// Loads whose log a removal deleted.
  var removed: Set[int]
  /// Chunks whose publish was answered, by load and number.
  var published: Set[(int, int)]

  /// Whether the store holds an object named `name`.
  def holds(name: Name): bool = store.exists(o => o.name == name)

  /// Whether the store holds anything of `l`'s log.
  def holdsAny(l: int): bool = store.exists(o => loadOf(o.name) == l)

  /// The heads of `l`'s log the store holds.
  def heads(l: int): Set[Name] =
    store.map(o => o.name).filter(name => name != Mark(l) and loadOf(name) == l)

  /// Whether `l` may be opened: never opened before, and its log's directory holds nothing.
  def openable(l: int): bool =
    not(opened.union(abandoned).union(removed).contains(l)) and not(holdsAny(l))

  /// A removed log is never open again with chunks: should its mark land again, its log holds
  /// no head and no chunk of it was ever published.
  val NoReopenAfterRemove = removed.forall(l =>
    holds(Mark(l)) implies (heads(l) == Set() and published.forall(c => c._1 != l))
  )

  action init = all {
    store' = Set(),
    late' = Set(),
    opened' = Set(),
    abandoned' = Set(),
    removed' = Set(),
    published' = Set(),
  }

  /// `l`'s open is answered, or loses its answer after its mark landed and reads its token back:
  /// the mark holds `token` and the log is open.
  action openAnswered(l: int, token: int): bool = all {
    openable(l),
    store' = store.union(Set({ name: Mark(l), token: token })),
    opened' = opened.union(Set(l)),
    late' = late,
    abandoned' = abandoned,
    removed' = removed,
    published' = published,
  }

  /// `l`'s open loses its answer and finds no mark of its token: it fails, its load is never
  /// used again, and its one create may still land.
  action openUnanswered(l: int, token: int): bool = all {
    openable(l),
    late' = late.union(Set({ name: Mark(l), token: token })),
    abandoned' = abandoned.union(Set(l)),
    store' = store,
    opened' = opened,
    removed' = removed,
    published' = published,
  }

  /// Chunk `n` of `l`'s log is published: its head is created where its name is free, and kept
  /// only where the log's mark is still there.
  action publishAnswered(l: int, n: int, token: int): bool = {
    val chunk = HeadChunk({ load: l, number: n })
    all {
      opened.contains(l),
      not(holds(chunk)),
      if (holds(Mark(l))) all {
        store' = store.union(Set({ name: chunk, token: token })),
        published' = published.union(Set((l, n))),
      } else all {
        store' = store,
        published' = published,
      },
      late' = late,
      opened' = opened,
      abandoned' = abandoned,
      removed' = removed,
    }
  }

  /// Chunk `n`'s head create loses its answer: the publish fails, and the create may still land.
  action publishUnanswered(l: int, n: int, token: int): bool = {
    val chunk = HeadChunk({ load: l, number: n })
    all {
      opened.contains(l),
      not(holds(chunk)),
      not(late.exists(o => o.name == chunk)),
      late' = late.union(Set({ name: chunk, token: token })),
      store' = store,
      opened' = opened,
      abandoned' = abandoned,
      removed' = removed,
      published' = published,
    }
  }

  /// A create that lost its answer lands, where its name is still free.
  action land(o: Object): bool = all {
    late.contains(o),
    late' = late.exclude(Set(o)),
    store' = if (holds(o.name)) store else store.union(Set(o)),
    opened' = opened,
    abandoned' = abandoned,
    removed' = removed,
    published' = published,
  }

  /// `l`'s log is removed, as its run ends it or a replay fences it: its mark goes first, then
  /// every head its directory lists.
  action remove(l: int): bool = all {
    opened.union(abandoned).contains(l) or holdsAny(l),
    store' = store.filter(o => loadOf(o.name) != l),
    removed' = removed.union(Set(l)),
    late' = late,
    opened' = opened,
    abandoned' = abandoned,
    published' = published,
  }

  action step = {
    nondet l = LOADS.oneOf()
    nondet n = NUMBERS.oneOf()
    nondet token = TOKENS.oneOf()
    nondet o = OBJECTS.oneOf()
    any {
      openAnswered(l, token),
      openUnanswered(l, token),
      publishAnswered(l, n, token),
      publishUnanswered(l, n, token),
      land(o),
      remove(l),
    }
  }

  /// An open tried again after its answer was lost, which the log never does: its first create
  /// may land after the log is removed.
  action openRetried(l: int, lost: int, token: int): bool = all {
    openable(l),
    late' = late.union(Set({ name: Mark(l), token: lost })),
    store' = store.union(Set({ name: Mark(l), token: token })),
    opened' = opened.union(Set(l)),
    abandoned' = abandoned,
    removed' = removed,
    published' = published,
  }

  /// A head whose create lands after its log was removed is a leftover, never an open log.
  run aHeadLandingAfterRemovalIsALeftoverTest =
    init
      .then(openAnswered(1, 1))
      .then(publishUnanswered(1, 0, 2))
      .then(remove(1))
      .then(land({ name: HeadChunk({ load: 1, number: 0 }), token: 2 }))
      .expect(NoReopenAfterRemove and not(holds(Mark(1))) and heads(1).size() == 1)
      .then(remove(1))
      .expect(not(holdsAny(1)))

  /// An open that lost its answer gives its load up: a mark that lands later opens a log no
  /// chunk was published in, and a removal ends it.
  run aMarkLandingLateOpensALogWithNoChunksTest =
    init
      .then(openUnanswered(1, 1))
      .then(land({ name: Mark(1), token: 1 }))
      .expect(holds(Mark(1)) and heads(1) == Set() and not(opened.contains(1)))
      .then(remove(1))
      .expect(NoReopenAfterRemove and not(holdsAny(1)))

  /// A publish that finds its log's mark gone keeps nothing.
  run aPublishAfterRemovalKeepsNothingTest =
    init
      .then(openAnswered(1, 1))
      .then(remove(1))
      .then(publishAnswered(1, 0, 2))
      .expect(not(holdsAny(1)) and published == Set())

  /// An open tried again after its answer was lost would reopen its removed log beside a head
  /// that landed late, naming a chunk the removal deleted: the invariant refuses it.
  run aRetriedOpenWouldReopenARemovedLogTest =
    init
      .then(openRetried(1, 1, 2))
      .then(publishAnswered(1, 0, 1))
      .then(publishUnanswered(1, 1, 2))
      .then(remove(1))
      .then(land({ name: Mark(1), token: 1 }))
      .then(land({ name: HeadChunk({ load: 1, number: 1 }), token: 2 }))
      .expect(not(NoReopenAfterRemove))
}
