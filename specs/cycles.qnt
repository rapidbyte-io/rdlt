// Full reads as cycles across runs whose plans change, as ADR 0004 decides it. A stream's full
// read, its cycle, is recorded by its first commit with where its rows land: the stream's table,
// or a generation hidden from readers until the read completes and swaps it in for the table, as a
// replace stream's does. State holds where the destination holds the stream's partition, which
// says where the rows below it landed only in the place the recorded cycle fills. Each run reads as
// its plan says, and the plan may change between runs: a full read resumes a recorded cycle that
// fills the place it fills, and otherwise starts a new cycle from the beginning, whose first commit
// records it and deletes the old one's position; a read that is not full resumes a cycle that
// fills the table and ends it with its first commit, and is refused where the cycle fills a
// generation. A run may stop at any point, and between runs an operator may reset the stream, with
// its table or not. The table merges, so it holds a row or not: whether a row lands twice is the
// exactly-once specification's concern.
//
// quint steps: --max-steps 40
// quint run: --max-samples 2000
// quint verify: tlc
/// The protocol, for a stream of one partition holding `ROWS` rows.
module cycles_protocol {
  // ---- Bounds

  /// The rows the partition holds, from offset 0.
  const ROWS: int
  assume RowsHold = ROWS >= 1

  // ---- State

  /// A run's read: whether it is full, whether its rows fill a generation, where its next commit
  /// starts, how far it read and sealed, whether its first commit records it as a new cycle, and
  /// whether its first commit ends the cycle state records.
  type Read = {
    full: bool,
    replace: bool,
    base: int,
    read: int,
    sealed: int,
    begins: bool,
    ends: bool,
  }

  /// Whether a run is reading.
  var running: bool
  /// The run's read, while it reads.
  var reading: Read
  /// Where the destination holds the partition: every row below it landed.
  var position: int
  /// Whether state records a cycle.
  var recorded: bool
  /// Whether the recorded cycle's rows fill its generation.
  var replaces: bool
  /// The rows the table holds.
  var table: Set[int]
  /// The rows the recorded cycle's generation holds.
  var generation: Set[int]
  /// The rows the recorded cycle's own reads landed, which no destination keeps: the invariants
  /// read it.
  var landed: Set[int]

  /// Every row of the partition.
  pure val ALL: Set[int] = 0.to(ROWS - 1)

  /// The read of a run that is not reading.
  pure val IDLE: Read = {
    full: false,
    replace: false,
    base: 0,
    read: 0,
    sealed: 0,
    begins: false,
    ends: false,
  }

  /// The rows from `first` to below `end`.
  pure def span(first: int, end: int): Set[int] = ALL.filter(o => first <= o and o < end)

  /// A read of the partition from `at`, as `full`, `replace`, `begins` and `ends` say.
  pure def readFrom(at: int, full: bool, replace: bool, begins: bool, ends: bool): Read = {
    full: full,
    replace: replace,
    base: at,
    read: at,
    sealed: at,
    begins: begins,
    ends: ends,
  }

  /// Every variable but `running` and `reading`, as it is.
  action keep: bool = all {
    position' = position,
    recorded' = recorded,
    replaces' = replaces,
    table' = table,
    generation' = generation,
    landed' = landed,
  }

  // ---- Actions

  action init = all {
    running' = false,
    reading' = IDLE,
    position' = 0,
    recorded' = false,
    replaces' = false,
    table' = Set(),
    generation' = Set(),
    landed' = Set(),
  }

  /// A run begins, reading as its plan says: in full or not, and, read in full, filling a
  /// generation or the table. A full read resumes a recorded cycle that fills the same place, and
  /// otherwise begins a new one from the beginning. A read that is not full resumes a cycle that
  /// fills the table, which its first commit ends, and is refused where the cycle fills a
  /// generation: no run begins.
  action begin(full: bool, replace: bool): bool = all {
    not(running),
    full or not(replace),
    full or not(recorded and replaces),
    reading' =
      if (not(full)) readFrom(position, false, false, false, recorded)
      else if (recorded and replaces == replace) readFrom(position, true, replace, false, false)
      else readFrom(0, true, replace, true, false),
    running' = true,
    keep,
  }

  /// The run reads the partition's next row.
  action readRow: bool = all {
    running,
    reading.read < ROWS,
    reading' = { ...reading, read: reading.read + 1 },
    running' = running,
    keep,
  }

  /// The run checkpoints where it read.
  action checkpoint: bool = all {
    running,
    reading.sealed < reading.read,
    reading' = { ...reading, sealed: reading.read },
    running' = running,
    keep,
  }

  /// The run commits what it sealed into the place it fills: the generation where it replaces,
  /// the table otherwise. A new cycle's first commit records it, its generation holding only its
  /// own rows, and a read that is not full ends the recorded cycle with its first. The commit
  /// that takes a full read's last row completes it, a replace's generation swapping in for the
  /// table, and the run ends.
  action commit = {
    val rows = span(reading.base, reading.sealed)
    val fills = reading.full and reading.replace
    val kept = if (reading.begins) Set() else generation
    val filled = if (fills) kept.union(rows) else kept
    val completes = reading.full and reading.sealed == ROWS
    val ends = completes or reading.ends
    all {
      running,
      reading.sealed > reading.base,
      position' = reading.sealed,
      table' = if (completes and fills) filled else if (fills) table else table.union(rows),
      generation' = if (ends) Set() else filled,
      recorded' = if (ends) false else reading.begins or recorded,
      replaces' = if (ends) false else if (reading.begins) reading.replace else replaces,
      landed' =
        if (ends) Set()
        else if (not(reading.full)) landed
        else if (reading.begins) rows
        else landed.union(rows),
      running' = not(completes),
      reading' =
        if (completes) IDLE
        else { ...reading, base: reading.sealed, begins: false, ends: false },
    }
  }

  /// The run stops: what it read and did not commit is lost.
  action stop: bool = all {
    running,
    running' = false,
    reading' = IDLE,
    keep,
  }

  /// Between runs an operator resets the stream: state forgets where it stands and its cycle,
  /// whose generation stays hidden, and with its tables the table goes too.
  action reset(tables: bool): bool = all {
    not(running),
    position' = 0,
    recorded' = false,
    replaces' = false,
    table' = if (tables) Set() else table,
    generation' = Set(),
    landed' = Set(),
    running' = running,
    reading' = reading,
  }

  action step = any {
    nondet full = Set(true, false).oneOf()
    nondet replace = Set(true, false).oneOf()
    begin(full, replace),
    readRow,
    checkpoint,
    commit,
    stop,
    nondet tables = Set(true, false).oneOf()
    reset(tables),
  }

  // ---- Invariants

  /// The rows below the position are held where state says the stream's rows land: in the
  /// recorded cycle's generation where it replaces, in the table otherwise.
  val PositionsHoldTheirRows =
    span(0, position).subseteq(if (recorded and replaces) generation else table)

  /// A recorded cycle's own reads landed every row below the position: it read from the
  /// beginning, and no other read moved the position past what it read.
  val CyclesReadFromTheBeginning = not(recorded) or landed == span(0, position)

  // ---- Tests

  /// Begins as a rule that records no place with a cycle would: a full read resumes any recorded
  /// cycle, and a read that is not full resumes any position and ends nothing.
  action beginAnyway(full: bool, replace: bool): bool = all {
    not(running),
    full or not(replace),
    reading' =
      if (not(full)) readFrom(position, false, false, false, false)
      else if (recorded) readFrom(position, true, replace, false, false)
      else readFrom(0, true, replace, true, false),
    running' = true,
    keep,
  }

  /// The run reads its next row, checkpoints and commits it.
  run loadRow = readRow.then(checkpoint).then(commit)
}

/// The protocol for a partition of three rows: the bound TLC explores whole.
module cycles {
  import cycles_protocol(ROWS = 3).*

  /// A run left part way: read in full, as `replace` says, it commits its first row and stops.
  run leftPartWay(replace: bool): bool =
    init
      .then(begin(true, replace))
      .then(readRow)
      .then(checkpoint)
      .then(commit)
      .then(stop)

  /// A full read resumes a cycle filling the place it fills, into its generation.
  run aFullReadResumesACycleThatFillsTheSamePlaceTest =
    leftPartWay(true)
      .then(begin(true, true))
      .expect(reading.base == 1 and not(reading.begins))
      .then(readRow)
      .then(readRow)
      .then(checkpoint)
      .then(commit)
      .expect(all { table == ALL, not(recorded), generation == Set(), not(running) })

  /// A full read whose rows fill a generation does not resume a cycle that filled the table: it
  /// begins a new one, and its generation, swapped in, holds every row.
  run aFullReadBeginsANewCycleWhereTheRecordedOneFillsAnotherPlaceTest =
    leftPartWay(false)
      .then(begin(true, true))
      .expect(reading.base == 0 and reading.begins)
      .then(loadRow)
      .expect(all { recorded, replaces, generation == Set(0), table == Set(0), position == 1 })
      .then(readRow)
      .then(readRow)
      .then(checkpoint)
      .then(commit)
      .expect(table == ALL and not(recorded))

  /// A full read filling the table does not resume a cycle that filled a generation: it begins a
  /// new one, and the old generation's rows never reach the table.
  run aFullReadFillingTheTableBeginsANewCycleOverAReplaceTest =
    leftPartWay(true)
      .then(begin(true, false))
      .expect(reading.base == 0 and reading.begins)
      .then(loadRow)
      .expect(all { recorded, not(replaces), generation == Set(), table == Set(0) })

  /// A read that is not full of a stream whose cycle fills a generation is refused.
  run aReadNotFullOfAReplaceCycleIsRefusedTest =
    leftPartWay(true)
      .then(begin(false, false))
      .fail()

  /// A read that is not full resumes a cycle that filled the table and ends it with its first
  /// commit, so the next full read begins from the beginning.
  run aReadNotFullEndsACycleThatFilledTheTableTest =
    leftPartWay(false)
      .then(begin(false, false))
      .expect(reading.base == 1 and reading.ends)
      .then(loadRow)
      .expect(not(recorded) and position == 2 and table == Set(0, 1))
      .then(stop)
      .then(begin(true, false))
      .expect(reading.base == 0 and reading.begins)

  /// A read that is not full and commits nothing leaves the cycle recorded, and the next full
  /// read resumes it.
  run aReadNotFullThatCommitsNothingLeavesTheCycleTest =
    leftPartWay(false)
      .then(begin(false, false))
      .then(stop)
      .then(begin(true, false))
      .expect(reading.base == 1 and not(reading.begins))

  /// A reset ends the cycle, and the next full read begins from the beginning, the table keeping
  /// its rows until the new generation swaps in.
  run aResetEndsTheCycleTest =
    leftPartWay(true)
      .then(reset(false))
      .then(begin(true, true))
      .expect(reading.base == 0 and reading.begins)

  /// A full read resuming a cycle that filled the table, into a generation, would leave the rows
  /// the cycle landed out of the table it swaps in.
  run aFullReadResumingACycleOfAnotherPlaceWouldLoseItsRowsTest =
    leftPartWay(false)
      .then(beginAnyway(true, true))
      .then(loadRow)
      .expect(not(PositionsHoldTheirRows))

  /// A read that is not full resuming a cycle that fills a generation would land the rows after its
  /// position in the table and leave those before it in the generation.
  run aReadNotFullResumingAReplaceCycleWouldStrandItsRowsTest =
    leftPartWay(true)
      .then(beginAnyway(false, false))
      .then(loadRow)
      .expect(not(PositionsHoldTheirRows))

  /// A read that is not full leaving the cycle recorded would have the next full read resume
  /// from where it stopped, past rows the cycle never read.
  run aReadNotFullLeavingTheCycleWouldMakeTheNextFullReadPartialTest =
    leftPartWay(false)
      .then(beginAnyway(false, false))
      .then(loadRow)
      .expect(not(CyclesReadFromTheBeginning))
}
