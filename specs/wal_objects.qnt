// The write-ahead log on object storage that ADR 0051 decides, request by request. A load's
// writer opens its log by creating the log's mark once, publishes each chunk by creating its head
// where the name is free, a chunk past a part with its body uploaded in parts first, and deletes
// the chunks its newest no longer needs. A replaying load lists the open logs, fences one at its
// next chunk, reads what its tail needs, releases it and removes it, mark first. Every request may
// succeed, fail, lose its answer once done, or go unanswered and land later; a create may be
// answered as taken by a racing create. Either load may crash, and an intruder may leave a name
// the store never writes in the log's directory. The requests are the store's, in its order, so a
// trace of this module drives the store request by request.
//
// quint steps: --step simulated --max-steps 80
// quint verify: tlc
// quint run: --max-samples 500
// quint trace: replayed 0x728 replayedLog
// quint trace: fenced 0x150f fencedWriter
// quint trace: latemark 0x67dbfe054e495 lateMarkRemoved
// quint trace: strayed 0x29 strayRefused
// quint trace: removed 0x150f removedAll
// quint trace: leftover 0x19bf leftoverRemoved
// quint trace: random1 0x1 -
// quint trace: random2 0x2 -
// quint trace: random3 0x3 -
// quint trace: random4 0xb6 -
// quint trace: random5 0x12a -
// quint trace: random6 0x139 -
// quint trace: random7 0x70 -
// quint trace: random8 0x185 -
// quint trace: random9 0x121 -
// quint trace: random10 0xa2 -
module wal_objects {
  // ---- Bounds

  /// Attempts a request is given, as the trace driver configures the store.
  pure val ATTEMPTS = 2
  /// Fences a replay tries before it leaves the log to its load, which still publishes.
  pure val TRIES = 2
  /// The writer's chunks: 0 and 1, so a fence lands at 0 to 2 and its release at 1 to 3.
  pure val CHUNKS = 2
  /// The writer's chunks longer than a part: uploaded in parts, with a head naming the body.
  pure val LONG = Set(1)
  /// Times the replaying load starts in all: each start makes its creates' tokens its own.
  pure val STARTS = 3
  /// Faults a behavior meets in all: requests that do not succeed, crashes, the intruder's name.
  pure val FAULTS = 2

  // ---- Who makes requests, and what the store holds

  /// The writing load, the replaying load, and an intruder writing what the store never writes.
  type Who = | W | R | X

  /// What a name in the store names: the log's mark, a chunk's head, a body uploaded in parts
  /// for a chunk by its uploader, or a name the store never writes.
  type Name =
    | Mark
    | ChunkHead(int)
    | Body((int, Who))
    | Stray

  /// What a head is: the chunk itself, or a reference to the body its uploader made.
  type Kind = | Whole | InParts(Who)

  /// An object: its name, who created it and under which token (each of the replaying load's
  /// starts makes its own), the chunks its end names as still needed (a body holds its chunk's),
  /// and its kind.
  type Obj = { name: Name, by: Who, tok: int, live: Set[int], kind: Kind }

  /// The directories a listing reads: the log's, the open logs' marks, and the logs' parent.
  type Dir = | LogDir | MarksDir | LogsDir

  /// What a request asks of the store.
  type Op = | Creates | Gets | Lists | Deletes | Begins | Parts | Completes | Aborts

  /// Where a request goes: an object, or a directory it lists.
  type At = | Object(Name) | Under(Dir)

  /// A request: what it asks, where, and the object a create or a completion makes.
  type Req = { op: Op, at: At, obj: Obj }

  /// What becomes of a request: done and answered; failed with nothing done; done with its answer
  /// lost; unanswered, a put or deletion landing later; or a create answered as taken by a racing
  /// create, nothing done.
  type Outcome = | Ok | Failed | Unknown | Late | Taken

  /// What a request left unanswered does if it lands.
  type Landing = | Lands(Obj) | Unlands(Name)

  /// A put or deletion left unanswered: whose, at which attempt of which round of its create,
  /// and what it does if it lands.
  type Pending = { who: Who, attempt: int, wave: int, landing: Landing }

  // ---- Answers

  /// What a request answered.
  type Got = | Present(Obj) | Absent | Objects(Set[Obj]) | Created | Did

  /// Why a request or an operation failed: every attempt failed, the name was taken, no such
  /// object, or a listing held a name the store never writes.
  type Fault = | Unavailable | NameTaken | Missing | Strays

  /// What an attempt came to: an answer, a failure another attempt may not meet, or a final one.
  type Res = | Answered(Got) | Transient | Final(Fault)

  /// What one of the store's operations answered.
  type Answer =
    | Nil
    | Done
    | Numbers(Set[int])
    | Content(Obj)
    | Listed(bool)
    | Refused(Fault)

  /// The store's operations, as `WalStore` has them: `Listing` is `chunks`.
  type Doing =
    | Idle
    | Opening
    | Staging(int)
    | Appending(int)
    | Publishing(int)
    | Removing(int)
    | RemovingLog
    | Listing
    | Reading(int)
    | Loads
    | Leftovers

  /// How the request in flight is tried: plainly; as a read, which looks at the object once more
  /// within the attempt where it fails; as an upload's completion, which does too; as a create,
  /// which reads its token back where its outcome is unknown; or as that reading back.
  type Mode = | Plain | Readish | Completing | Creating | ReadingBack

  /// Where a load stands in its program.
  type Prog =
    | Off
    | Dead
    | Stopped
    | WRun
    | RLeftovers
    | RLeftover
    | RLoads
    | RTail
    | RReadTail
    | RFenceStage
    | RFencePublish
    | RMissing
    | RAbove
    | RLost
    | RGone
    | RScan
    | RScanOpen
    | RReleaseStage
    | RReleasePublish
    | RReleaseMissing
    | RReleaseAbove
    | RReleaseLost
    | RRemove

  /// A load: its program, the operation it is in, the request that makes next and how it is
  /// tried, what the operation holds so far, what the load keeps between operations (its starts
  /// among them), whether an operation of it ended with no refusal where a request answered a
  /// name the store never writes, the answer of its last operation that ended, and whether its
  /// last request ended one.
  type Proc = {
    prog: Prog,
    doing: Doing,
    at: int,
    req: Req,
    mode: Mode,
    attempt: int,
    wave: int,
    unsure: bool,
    peek: bool,
    missed: bool,
    once: bool,
    orig: Fault,
    staged: Obj,
    listed: Set[Obj],
    found: bool,
    kinds: int -> Kind,
    epoch: int,
    slipped: bool,
    kept: Set[int],
    tries: int,
    tail: int,
    tailBy: Who,
    need: Set[int],
    fence: int,
    reading: Set[(int, Who)],
    sawStray: bool,
    answer: Answer,
    ended: bool,
  }

  // ---- State

  /// What the store holds.
  var store: Set[Obj]
  /// Puts and deletions left unanswered that may still land.
  var pending: Set[Pending]
  /// The writing and the replaying load.
  var procs: Who -> Proc
  /// Faults left to meet.
  var faults: int
  /// Whether a removal of the log deleted its mark and listed it with no name the store never
  /// writes, so went on to delete what it held.
  var removed: bool
  /// The writer's chunks whose publish was answered.
  var wPublished: Set[int]
  /// The fences a replay published and found no chunk above.
  var held: Set[int]
  /// Whether every chunk a replay read came from its head, or from the body its head names.
  var ownTokens: bool
  /// Whether every replay that read a log through read exactly what its tail needs.
  var exact: bool
  /// Whether a replay read a log through.
  var replayed: bool

  // ---- Helpers

  pure val NOBODY: Obj = { name: Stray, by: X, tok: 0, live: Set(), kind: Whole }
  pure val NOREQ: Req = { op: Gets, at: Under(LogDir), obj: NOBODY }

  pure def markOf(p: Who): Obj = { name: Mark, by: p, tok: 0, live: Set(), kind: Whole }

  /// The chunks the writer's chunk `n` may need: itself, or itself and the chunk before; its last
  /// may need none, as a load that closed its log leaves it; past its last, there is no chunk.
  pure def lives(n: int): Set[Set[int]] =
    if (n <= 0) Set(Set(0))
    else if (n >= CHUNKS) Set(Set())
    else if (n == CHUNKS - 1) Set(Set(), Set(n), Set(n - 1, n))
    else Set(Set(n), Set(n - 1, n))

  /// The writer's chunk `n`, needing `live`.
  pure def chunkOf(n: int, live: Set[int]): Obj =
    { name: ChunkHead(n), by: W, tok: 0, live: live, kind: if (LONG.contains(n)) InParts(W) else Whole }

  /// A replay's fence `n` of its start `epoch`, needing `live`.
  pure def fenceOf(n: int, epoch: int, live: Set[int]): Obj =
    { name: ChunkHead(n), by: R, tok: epoch, live: live, kind: Whole }

  pure def numberOf(name: Name): int = match name {
    | ChunkHead(n) => n
    | Body(b) => b._1
    | _ => -1
  }

  pure def isHead(name: Name): bool = match name {
    | ChunkHead(_) => true
    | _ => false
  }

  /// The order a listing gives a log's directory in: by number, a body before its head, the
  /// stray name last.
  pure def rank(name: Name): int = match name {
    | ChunkHead(n) => 2 * n + 1
    | Body(b) => 2 * b._1
    | Stray => 100
    | Mark => -1
  }

  /// The object of `objs` a listing gives first.
  pure def first(objs: Set[Obj]): Obj =
    objs.fold(NOBODY, (best, o) => if (best == NOBODY or rank(o.name) < rank(best.name)) o else best)

  /// The least of `ns`; -1 where there is none.
  pure def least(ns: Set[int]): int =
    ns.fold(-1, (best, n) => if (best == -1 or n < best) n else best)

  /// The greatest of `ns`; -1 where there is none.
  pure def greatest(ns: Set[int]): int = ns.fold(-1, (best, n) => if (n > best) n else best)

  pure def named(s: Set[Obj], name: Name): Set[Obj] = s.filter(o => o.name == name)

  pure def holds(s: Set[Obj], name: Name): bool = s.exists(o => o.name == name)

  pure def nameOf(at: At): Name = match at {
    | Object(name) => name
    | Under(_) => Stray
  }

  pure def dirOf(at: At): Dir = match at {
    | Under(d) => d
    | Object(_) => LogDir
  }

  /// What a listing of `d` holds of the store `s`.
  pure def listing(s: Set[Obj], d: Dir): Set[Obj] =
    if (d == MarksDir) named(s, Mark) else s.filter(o => o.name != Mark)

  /// The object a chunk's bytes are read from, for chunk `n` whose head is of kind `kind`.
  pure def bodyName(n: int, kind: Kind): Name = match kind {
    | InParts(by) => Body((n, by))
    | Whole => ChunkHead(n)
  }

  /// The outcomes each request may meet.
  pure def allowed(op: Op, o: Outcome): bool = match op {
    | Creates => true
    | Deletes => o != Taken
    | Completes => Set(Ok, Failed, Unknown).contains(o)
    | Gets => Set(Ok, Failed, Late).contains(o)
    | Lists => Set(Ok, Failed, Late).contains(o)
    | _ => Set(Ok, Failed).contains(o)
  }

  // ---- What a result holds

  pure def gotOf(r: Res): Got = match r {
    | Answered(g) => g
    | _ => Absent
  }

  pure def isAnswered(r: Res): bool = match r {
    | Answered(_) => true
    | _ => false
  }

  pure def isPresent(r: Res): bool = match gotOf(r) {
    | Present(_) => isAnswered(r)
    | _ => false
  }

  /// The object a result found; none where it found none.
  pure def objOf(r: Res): Obj = match gotOf(r) {
    | Present(o) => o
    | _ => NOBODY
  }

  pure def isListed(r: Res): bool = match gotOf(r) {
    | Objects(_) => isAnswered(r)
    | _ => false
  }

  /// The objects a listing answered.
  pure def objsOf(r: Res): Set[Obj] = match gotOf(r) {
    | Objects(s) => s
    | _ => Set()
  }

  pure def numbersOf(a: Answer): Set[int] = match a {
    | Numbers(ns) => ns
    | _ => Set()
  }

  pure def isNumbers(a: Answer): bool = match a {
    | Numbers(_) => true
    | _ => false
  }

  pure def contentOf(a: Answer): Obj = match a {
    | Content(o) => o
    | _ => NOBODY
  }

  pure def isContent(a: Answer): bool = match a {
    | Content(_) => true
    | _ => false
  }

  pure def isPublishing(d: Doing): bool = match d {
    | Publishing(_) => true
    | _ => false
  }

  pure def readingOf(d: Doing): int = match d {
    | Reading(n) => n
    | _ => -1
  }

  // ---- The store: what a request does, and what it answers

  /// What the store `s` holds once `req` met `o`.
  pure def stored(s: Set[Obj], req: Req, o: Outcome): Set[Obj] = {
    val done = o == Ok or o == Unknown
    match req.op {
      | Creates => if (done and not(holds(s, req.obj.name))) s.union(Set(req.obj)) else s
      | Deletes => if (done) s.filter(x => x.name != nameOf(req.at)) else s
      | Completes => if (done) s.filter(x => x.name != req.obj.name).union(Set(req.obj)) else s
      | _ => s
    }
  }

  /// What `req`, meeting `o` where the store holds `s`, answers.
  pure def answered(s: Set[Obj], req: Req, o: Outcome): Res = match req.op {
    | Creates =>
        if (o == Taken) Final(NameTaken)
        else if (o == Failed or o == Late) Transient
        else if (holds(s, req.obj.name)) Final(NameTaken)
        else if (o == Unknown) Transient
        else Answered(Created)
    | Gets =>
        if (o != Ok) Transient
        else if (holds(s, nameOf(req.at))) Answered(Present(named(s, nameOf(req.at)).fold(NOBODY, (_, o) => o)))
        else Answered(Absent)
    | Lists => if (o != Ok) Transient else Answered(Objects(listing(s, dirOf(req.at))))
    | _ => if (o == Ok) Answered(Did) else Transient
  }

  /// Whether `req` meeting `o` leaves a put or deletion to land later.
  pure def leaves(req: Req, o: Outcome): bool = o == Late and (req.op == Creates or req.op == Deletes)

  pure def landingOf(req: Req): Landing =
    if (req.op == Creates) Lands(req.obj) else Unlands(nameOf(req.at))

  // ---- A request's attempts

  pure def plain(op: Op, name: Name): Req = { op: op, at: Object(name), obj: NOBODY }

  pure def lists(d: Dir): Req = { op: Lists, at: Under(d), obj: NOBODY }

  pure def creates(obj: Obj): Req = { op: Creates, at: Object(obj.name), obj: obj }

  /// `pr` making `req` afresh, tried as `mode`.
  pure def ask(pr: Proc, req: Req, mode: Mode): Proc =
    { ...pr, req: req, mode: mode, attempt: 1, wave: 0, unsure: false, peek: false, missed: false, once: false }

  /// What a request's call does next: another request within it, or its end in a result.
  type Next = | Again(Proc) | Ends(Res)

  /// What `pr`'s call does once its request came to `res`, meeting `o`.
  pure def attempted(pr: Proc, res: Res, o: Outcome): Next = {
    val retry =
      if (pr.attempt < ATTEMPTS) Again({ ...pr, attempt: pr.attempt + 1, peek: false, missed: false })
      else Ends(Final(Unavailable))
    match pr.mode {
      | Plain => if (res == Transient) retry else Ends(res)
      | Readish =>
          // A read that fails looks at the object, which leaves its answer as it was, unless the
          // look goes unanswered too.
          // A read left unanswered ends its attempt: the look is part of the attempt.
          if (not(pr.peek)) {
            if (isPresent(res)) Ends(res)
            else if (o == Late) retry
            else Again({ ...pr, peek: true, missed: isAnswered(res) })
          }
          else if (pr.missed and o != Late) Ends(Final(Missing))
          else retry
      | Completing =>
          // A completion that fails is done where the body is there.
          if (not(pr.peek)) {
            if (isAnswered(res)) Ends(res)
            else Again({ ...pr, peek: true, req: plain(Gets, pr.req.obj.name) })
          }
          else if (isPresent(res)) Ends(Answered(Did))
          else if (pr.attempt < ATTEMPTS)
            Again({ ...pr, attempt: pr.attempt + 1, peek: false, req: { op: Completes, at: Object(pr.req.obj.name), obj: pr.req.obj } })
          else Ends(Final(Unavailable))
      | Creating => {
          val limit = if (pr.once) 1 else ATTEMPTS
          val back = { ...pr, mode: ReadingBack, attempt: 1, req: { op: Gets, at: Object(pr.req.obj.name), obj: pr.req.obj } }
          if (isAnswered(res)) Ends(res)
          else if (res == Transient) {
            if (pr.attempt < limit) Again({ ...pr, attempt: pr.attempt + 1, unsure: true })
            else if (pr.once) Again({ ...back, unsure: true, orig: Unavailable })
            else Ends(Final(Unavailable))
          }
          // A name taken after an attempt whose outcome is unknown may be its own.
          else if (pr.unsure) Again({ ...back, orig: NameTaken })
          else Ends(res)
        }
      | ReadingBack =>
          if (isPresent(res)) {
            if (objOf(res).by == pr.req.obj.by and objOf(res).tok == pr.req.obj.tok) Ends(Answered(Created)) else Ends(Final(pr.orig))
          }
          else if (isAnswered(res)) {
            // Found missing: a create racing it left the name empty, and it creates again.
            if (pr.once) Ends(Final(pr.orig))
            else if (pr.wave + 1 >= ATTEMPTS) Ends(Final(Unavailable))
            else Again({ ...pr, mode: Creating, attempt: 1, wave: pr.wave + 1, req: creates(pr.req.obj) })
          }
          else if (res == Transient) {
            if (pr.attempt < ATTEMPTS) Again({ ...pr, attempt: pr.attempt + 1 }) else Ends(Final(Unavailable))
          }
          else Ends(res)
    }
  }

  // ---- Operations: the request each makes next, and what it answers

  /// A load once its request's call ended: whether its operation ended with it.
  type After = { pr: Proc, ended: bool }

  pure def go(pr: Proc, at: int, req: Req, mode: Mode): After =
    { pr: { ...ask(pr, req, mode), at: at }, ended: false }

  pure def finish(pr: Proc, a: Answer): After =
    { pr: { ...pr, doing: Idle, at: 0, answer: a, req: NOREQ }, ended: true }

  pure def refusal(r: Res): Answer = match r {
    | Final(f) => Refused(f)
    | _ => Refused(Unavailable)
  }

  /// The least head in parts of `pr.listed` whose kind `pr` does not know, which it reads; -1
  /// where none is left.
  pure def unknownLong(pr: Proc): int =
    least(pr.listed.filter(o => isHead(o.name) and o.kind != Whole and not(pr.kinds.keys().contains(numberOf(o.name))))
                   .map(o => numberOf(o.name)))

  /// `kinds` with every head of `objs` that is not in parts noted as whole.
  pure def noteWhole(kinds: int -> Kind, objs: Set[Obj]): int -> Kind =
    objs.filter(o => isHead(o.name) and o.kind == Whole).fold(kinds, (k, o) => k.put(numberOf(o.name), Whole))

  /// `kinds` without chunk `n`.
  pure def forget(kinds: int -> Kind, n: int): int -> Kind =
    kinds.keys().exclude(Set(n)).mapBy(k => kinds.get(k))

  /// A listing of the log's chunks once it holds `pr.listed`: a head to read for its kind, or its
  /// answer.
  pure def listingOn(pr: Proc): After = {
    val n = unknownLong(pr)
    if (n == -1)
      finish({ ...pr, kinds: noteWhole(pr.kinds, pr.listed) },
             Numbers(pr.listed.filter(o => isHead(o.name)).map(o => numberOf(o.name))))
    else go(pr, 1, plain(Gets, ChunkHead(n)), Readish)
  }

  /// A removal's next deletion once it listed `pr.listed`, or its answer.
  pure def removingOn(pr: Proc): After =
    if (pr.listed == Set()) finish(pr, Done)
    else {
      val o = first(pr.listed)
      go({ ...pr, listed: pr.listed.exclude(Set(o)) }, 2, plain(Deletes, o.name), Plain)
    }

  /// What the operation `pr` of the load `p` is in does once its request's call ended in `r`.
  pure def operated(pr: Proc, r: Res, p: Who): After = match pr.doing {
    | Opening =>
        // 0: its log's directory listed; 1: its mark created, once.
        if (pr.at == 0) {
          if (not(isListed(r))) finish(pr, refusal(r))
          else if (objsOf(r) == Set()) {
            val c = go(pr, 1, creates(markOf(p)), Creating)
            { ...c, pr: { ...c.pr, once: true } }
          }
          else finish(pr, Refused(NameTaken))
        }
        else if (isAnswered(r)) finish(pr, Done)
        else finish(pr, refusal(r))
    | Staging(_) =>
        if (isPresent(r)) finish(pr, Done)
        else if (isAnswered(r)) finish(pr, Refused(Missing))
        else finish(pr, refusal(r))
    | Appending(n) =>
        // 0: the upload begun; 1: its first part; 2: the upload given up.
        if (pr.at == 0) {
          if (isAnswered(r)) go(pr, 1, plain(Parts, Body((n, p))), Plain) else finish(pr, refusal(r))
        }
        else if (pr.at == 1) {
          if (isAnswered(r)) finish(pr, Done) else go(pr, 2, plain(Aborts, Body((n, p))), Plain)
        }
        else finish(pr, Refused(Unavailable))
    | Publishing(n) => {
        // 0: the body's last part; 1: its completion; 2: the head created; 3: the mark asked
        // for; 4: the head deleted, the mark gone; 6: the body deleted; 7: the upload given up.
        val inParts = pr.staged.kind != Whole
        val body = bodyName(n, pr.staged.kind)
        val bodyObj = { name: body, by: p, tok: 0, live: pr.staged.live, kind: Whole }
        if (pr.at == 0) {
          if (isAnswered(r)) go(pr, 1, { op: Completes, at: Object(body), obj: bodyObj }, Completing)
          else go(pr, 7, plain(Aborts, body), Plain)
        }
        else if (pr.at == 1) {
          if (isAnswered(r)) go(pr, 2, creates(pr.staged), Creating) else go(pr, 7, plain(Aborts, body), Plain)
        }
        else if (pr.at == 2) {
          if (isAnswered(r)) go(pr, 3, plain(Gets, Mark), Plain)
          else if (r == Final(NameTaken) and inParts) go({ ...pr, orig: NameTaken }, 6, plain(Deletes, body), Plain)
          else finish(pr, refusal(r))
        }
        else if (pr.at == 3) {
          if (isPresent(r)) finish({ ...pr, kinds: pr.kinds.put(n, pr.staged.kind) }, Done)
          else if (isAnswered(r)) go(pr, 4, plain(Deletes, ChunkHead(n)), Plain)
          else finish(pr, refusal(r))
        }
        else if (pr.at == 4) {
          if (not(isAnswered(r))) finish(pr, refusal(r))
          else if (inParts) go({ ...pr, orig: Missing }, 6, plain(Deletes, body), Plain)
          else finish(pr, Refused(Missing))
        }
        else if (pr.at == 6) {
          if (isAnswered(r)) finish(pr, Refused(pr.orig)) else finish(pr, refusal(r))
        }
        // An upload given up: the publish fails whatever the abort answers.
        else finish(pr, Refused(Unavailable))
      }
    | Removing(n) =>
        // 0: the head read for its kind; 1: the head deleted; 2: its body deleted.
        if (pr.at == 0) {
          if (isPresent(r)) go({ ...pr, kinds: pr.kinds.put(n, objOf(r).kind) }, 1, plain(Deletes, ChunkHead(n)), Plain)
          else if (r == Final(Missing)) go(pr, 1, plain(Deletes, ChunkHead(n)), Plain)
          else finish(pr, refusal(r))
        }
        else if (pr.at == 1) {
          if (not(isAnswered(r))) finish(pr, refusal(r))
          else {
            val kind = if (pr.kinds.keys().contains(n)) pr.kinds.get(n) else Whole
            val forgot = { ...pr, kinds: forget(pr.kinds, n) }
            if (kind == Whole) finish(forgot, Done) else go(forgot, 2, plain(Deletes, bodyName(n, kind)), Plain)
          }
        }
        else if (isAnswered(r)) finish(pr, Done)
        else finish(pr, refusal(r))
    | RemovingLog =>
        // 0: the mark deleted; 1: the log listed; 2: each object deleted.
        if (pr.at == 0) {
          if (isAnswered(r)) go(pr, 1, lists(LogDir), Plain) else finish(pr, refusal(r))
        }
        else if (pr.at == 1) {
          if (not(isListed(r))) finish(pr, refusal(r))
          else if (holds(objsOf(r), Stray)) finish(pr, Refused(Strays))
          else removingOn({ ...pr, listed: objsOf(r), kinds: Map() })
        }
        else if (isAnswered(r)) removingOn(pr)
        else finish(pr, refusal(r))
    | Listing =>
        // 0: the log listed; 1: a head in parts read for its kind.
        if (pr.at == 0) {
          if (not(isListed(r))) finish(pr, refusal(r))
          else if (holds(objsOf(r), Stray)) finish(pr, Refused(Strays))
          else listingOn({ ...pr, listed: objsOf(r) })
        }
        else if (isPresent(r)) listingOn({ ...pr, kinds: pr.kinds.put(numberOf(objOf(r).name), objOf(r).kind) })
        else finish(pr, refusal(r))
    | Reading(n) =>
        // 0: the head read for its kind, where it is not known; 1: the chunk's bytes read.
        if (pr.at == 0) {
          if (isPresent(r)) go({ ...pr, kinds: pr.kinds.put(n, objOf(r).kind) }, 1, plain(Gets, bodyName(n, objOf(r).kind)), Readish)
          else finish(pr, refusal(r))
        }
        else if (isPresent(r)) finish(pr, Content(objOf(r)))
        else finish(pr, refusal(r))
    | Loads =>
        if (isListed(r)) finish(pr, Listed(holds(objsOf(r), Mark))) else finish(pr, refusal(r))
    | Leftovers =>
        // 0: the logs' directories listed; 1: the open logs' marks listed.
        if (pr.at == 0) {
          if (isListed(r)) go({ ...pr, found: objsOf(r) != Set() }, 1, lists(MarksDir), Plain) else finish(pr, refusal(r))
        }
        else if (isListed(r)) finish(pr, Listed(pr.found and not(holds(objsOf(r), Mark))))
        else finish(pr, refusal(r))
    | Idle => finish(pr, Nil)
  }

  /// `pr`, of the load `p`, beginning the operation `d` with its first request.
  pure def begin(pr: Proc, d: Doing, p: Who): After = {
    val fresh = { ...pr, doing: d, at: 0, sawStray: false }
    match d {
      | Opening => go(fresh, 0, lists(LogDir), Plain)
      | Staging(_) => go(fresh, 0, plain(Gets, Mark), Plain)
      | Appending(n) => go(fresh, 0, plain(Begins, Body((n, p))), Plain)
      | Publishing(n) =>
          if (pr.staged.kind != Whole) go(fresh, 0, plain(Parts, Body((n, p))), Plain)
          else go(fresh, 2, creates(pr.staged), Creating)
      | Removing(n) =>
          if (pr.kinds.keys().contains(n)) go(fresh, 1, plain(Deletes, ChunkHead(n)), Plain)
          else go(fresh, 0, plain(Gets, ChunkHead(n)), Readish)
      | RemovingLog => go(fresh, 0, plain(Deletes, Mark), Plain)
      | Listing => go(fresh, 0, lists(LogDir), Plain)
      | Reading(n) =>
          if (pr.kinds.keys().contains(n)) go(fresh, 1, plain(Gets, bodyName(n, pr.kinds.get(n))), Readish)
          else go(fresh, 0, plain(Gets, ChunkHead(n)), Readish)
      | Loads => go(fresh, 0, lists(MarksDir), Plain)
      | Leftovers => go(fresh, 0, lists(LogsDir), Plain)
      | Idle => finish(fresh, Nil)
    }
  }

  // ---- The writer's program

  /// The writer once its chunk `n` is published or an unneeded chunk deleted: the next chunk below
  /// `n` its newest no longer needs, deleted; else its next chunk, needing `live`; else its end.
  pure def onward(q: Proc, n: int, live: Set[int]): After = {
    val unneeded = q.kept.filter(k => k < n and not(q.staged.live.contains(k)))
    if (unneeded != Set()) begin(q, Removing(least(unneeded)), W)
    else if (n + 1 < CHUNKS) begin({ ...q, staged: chunkOf(n + 1, live) }, Staging(n + 1), W)
    else { pr: { ...q, prog: Stopped }, ended: false }
  }

  /// The writer's next operation once its operation `done` answered `a`, its next chunk needing
  /// `live` where it stages one.
  pure def writes(pr: Proc, done: Doing, a: Answer, live: Set[int]): After =
    if (a != Done) { pr: { ...pr, prog: Stopped }, ended: false }
    else match done {
      | Opening => begin({ ...pr, staged: chunkOf(0, live) }, Staging(0), W)
      | Staging(n) => if (LONG.contains(n)) begin(pr, Appending(n), W) else begin(pr, Publishing(n), W)
      | Appending(n) => begin(pr, Publishing(n), W)
      | Publishing(n) => onward({ ...pr, kept: pr.kept.union(Set(n)) }, n, live)
      | Removing(k) => onward({ ...pr, kept: pr.kept.exclude(Set(k)) }, numberOf(pr.staged.name), live)
      | _ => { pr: { ...pr, prog: Stopped }, ended: false }
    }

  // ---- The replay's program

  /// The replay's attempt ended: the next may begin.
  pure def over(pr: Proc): After = { pr: { ...pr, prog: Off, doing: Idle, req: NOREQ }, ended: false }

  pure def goTo(pr: Proc, prog: Prog, d: Doing): After = begin({ ...pr, prog: prog }, d, R)

  /// The replay's next try at fencing, once a fence lost its number to a chunk of the load.
  pure def lost(pr: Proc): After =
    if (pr.tries + 1 >= TRIES) over(pr) else goTo({ ...pr, tries: pr.tries + 1 }, RTail, Listing)

  /// The chunks the replay still reads once it fenced the log: those its tail needs that it has
  /// not read.
  pure def toRead(pr: Proc): Set[int] = pr.need.filter(k => not(pr.reading.exists(r => r._1 == k)))

  /// The replay's next read of what its tail needs; once it read all, its look at whether the
  /// log is still open.
  pure def scanOn(pr: Proc): After =
    if (toRead(pr) == Set()) goTo(pr, RScanOpen, Loads) else goTo(pr, RScan, Reading(least(toRead(pr))))

  /// The replay's next operation once its operation answered `a`.
  pure def replaying(pr: Proc, a: Answer): After = {
    // A take that failed, or found the log gone, looks whether the log is still open; the
    // attempt ends either way.
    val gone = goTo(pr, RGone, Loads)
    match pr.prog {
      | RLeftovers =>
          if (a == Listed(true)) goTo(pr, RLeftover, RemovingLog)
          else if (a == Listed(false)) goTo(pr, RLoads, Loads)
          else over(pr)
      | RLeftover => if (a == Done) goTo(pr, RLoads, Loads) else over(pr)
      | RLoads => if (a == Listed(true)) goTo({ ...pr, tries: 0 }, RTail, Listing) else over(pr)
      | RTail =>
          if (not(isNumbers(a))) gone
          else if (numbersOf(a) == Set())
            goTo({ ...pr, tail: -1, need: Set(), fence: 0, staged: fenceOf(0, pr.epoch, Set()) }, RFenceStage, Staging(0))
          else goTo({ ...pr, tail: greatest(numbersOf(a)) }, RReadTail, Reading(greatest(numbersOf(a))))
      | RReadTail =>
          if (not(isContent(a))) gone
          else {
            val o = contentOf(a)
            goTo({ ...pr, tailBy: o.by, need: o.live, fence: pr.tail + 1, staged: fenceOf(pr.tail + 1, pr.epoch, o.live) },
               RFenceStage, Staging(pr.tail + 1))
          }
      | RFenceStage => if (a == Done) goTo(pr, RFencePublish, Publishing(pr.fence)) else gone
      | RFencePublish =>
          if (a == Done) goTo(pr, RAbove, Listing)
          else if (a == Refused(NameTaken)) lost(pr)
          else if (a == Refused(Missing)) goTo(pr, RMissing, Loads)
          else gone
      | RMissing => if (a == Listed(true)) lost(pr) else gone
      | RAbove =>
          if (not(isNumbers(a))) gone
          else if (numbersOf(a).exists(k => k > pr.fence)) goTo(pr, RLost, Removing(pr.fence))
          else if (pr.tail >= 0 and pr.need != Set())
            scanOn({ ...pr, reading: if (pr.need.contains(pr.tail)) Set((pr.tail, pr.tailBy)) else Set() })
          else goTo(pr, RRemove, RemovingLog)
      | RLost => if (a == Done) lost(pr) else gone
      | RScan =>
          // A chunk the tail needs that is missing makes the log unreadable, where it is still
          // open.
          if (isContent(a)) scanOn({ ...pr, reading: pr.reading.union(Set((numberOf(contentOf(a).name), contentOf(a).by))) })
          else gone
      | RScanOpen =>
          if (a == Listed(true)) goTo({ ...pr, staged: fenceOf(pr.fence + 1, pr.epoch, Set()) }, RReleaseStage, Staging(pr.fence + 1))
          else over(pr)
      | RReleaseStage => if (a == Done) goTo(pr, RReleasePublish, Publishing(pr.fence + 1)) else over(pr)
      | RReleasePublish =>
          if (a == Done) goTo(pr, RReleaseAbove, Listing)
          else if (a == Refused(Missing)) goTo(pr, RReleaseMissing, Loads)
          else over(pr)
      | RReleaseAbove =>
          if (not(isNumbers(a))) over(pr)
          else if (numbersOf(a).exists(k => k > pr.fence + 1)) goTo(pr, RReleaseLost, Removing(pr.fence + 1))
          else goTo(pr, RRemove, RemovingLog)
      | _ => over(pr)
    }
  }

  // ---- Actions

  pure val IDLE: Proc = {
    prog: Off, doing: Idle, at: 0, req: NOREQ, mode: Plain, attempt: 1, wave: 0, unsure: false,
    peek: false, missed: false, once: false, orig: Unavailable, staged: NOBODY, listed: Set(),
    found: false, kinds: Map(), epoch: 0, slipped: false, kept: Set(), tries: 0, tail: -1, tailBy: X, need: Set(),
    fence: -1, reading: Set(), sawStray: false, answer: Nil, ended: false,
  }

  /// Who made the head of chunk `n` in `s`; the intruder where there is none.
  pure def byOf(s: Set[Obj], n: int): Who = named(s, ChunkHead(n)).fold(X, (_, o) => o.by)

  /// Whether `from`, read as chunk `n`'s bytes while the store held `heads` of it, is the head
  /// itself or the body the head names by its token.
  pure def readOwn(n: int, heads: Set[Obj], from: Obj): bool = heads.forall(h => from.name == bodyName(n, h.kind))

  /// The number of the fence the replay's listing looks above: its take's or its release's.
  pure def fenced(pr: Proc): int = if (pr.prog == RReleaseAbove) pr.fence + 1 else pr.fence

  action init = all {
    store' = Set(),
    pending' = Set(),
    procs' = Map(W -> begin({ ...IDLE, prog: WRun }, Opening, W).pr, R -> IDLE),
    faults' = FAULTS,
    removed' = false,
    wPublished' = Set(),
    held' = Set(),
    ownTokens' = true,
    exact' = true,
    replayed' = false,
  }

  /// The load `p` makes its next request, which meets `o`; `live` is what the writer's next
  /// chunk needs, where it stages one.
  action request(p: Who, o: Outcome, live: Set[int]): bool = requesting(p, o, live, true)

  /// The request of `request`, its listing of the log's directory heeded or not: a load that does
  /// not heed it goes on as if the name the store never writes were not there, which no load does.
  action requesting(p: Who, o: Outcome, live: Set[int], heed: bool): bool = {
    val pr = procs.get(p)
    val req = pr.req
    val seen = answered(store, req, o)
    val strayAnswered = req.op == Lists and dirOf(req.at) == LogDir and holds(objsOf(seen), Stray)
    val res =
      if (heed or not(strayAnswered)) seen
      else Answered(Objects(objsOf(seen).filter(x => x.name != Stray)))
    val after0: After = match attempted(pr, res, o) {
      | Again(q) => { pr: q, ended: false }
      | Ends(r) => operated(pr, r, p)
    }
    // A request that answered the name marks its operation, whatever the operation goes on to do.
    val sawStray = pr.sawStray or strayAnswered
    val after: After = { pr: { ...after0.pr, sawStray: sawStray }, ended: after0.ended }
    val a = after.pr.answer
    val slipped = after.ended and sawStray and not(Set(Refused(Strays), Refused(NameTaken)).contains(a))
    val moved: Proc =
      if (not(after.ended)) after.pr
      else if (p == W) writes(after.pr, pr.doing, a, live).pr
      else replaying(after.pr, a).pr
    val published = p == W and after.ended and a == Done and isPublishing(pr.doing)
    val above = p == R and after.ended and (pr.prog == RAbove or pr.prog == RReleaseAbove) and isNumbers(a)
    val readChunk = p == R and after.ended and readingOf(pr.doing) >= 0 and isContent(a)
    val scanned = p == R and after.ended and pr.prog == RScanOpen and a == Listed(true)
    all {
      pr.doing != Idle,
      allowed(req.op, o),
      o == Ok or faults > 0,
      if (p == W) lives(numberOf(pr.staged.name) + 1).contains(live) else live == Set(),
      store' = stored(store, req, o),
      pending' =
        if (leaves(req, o)) pending.union(Set({ who: p, attempt: pr.attempt, wave: pr.wave, landing: landingOf(req) }))
        else pending,
      procs' = procs.set(p, { ...moved, slipped: moved.slipped or slipped, ended: after.ended, answer: if (after.ended) a else Nil }),
      faults' = if (o == Ok) faults else faults - 1,
      removed' = (removed or (pr.doing == RemovingLog and pr.at == 1 and (a == Done or (not(after.ended) and after.pr.at == 2)))),
      wPublished' = if (published) wPublished.union(Set(numberOf(pr.staged.name))) else wPublished,
      held' = if (above and numbersOf(a).forall(k => k <= fenced(pr))) held.union(Set(fenced(pr))) else held,
      ownTokens' = (ownTokens and (not(readChunk) or readOwn(readingOf(pr.doing), named(store, ChunkHead(readingOf(pr.doing))), contentOf(a)))),
      exact' = (exact and (not(scanned) or pr.reading == pr.need.map(k => (k, byOf(store, k))))),
      replayed' = (replayed or scanned),
    }
  }

  /// A put or deletion left unanswered lands: a put only where its name is still free.
  action land(l: Pending): bool = all {
    pending.contains(l),
    pending' = pending.exclude(Set(l)),
    store' = match l.landing {
      | Lands(obj) => if (holds(store, obj.name)) store else store.union(Set(obj))
      | Unlands(name) => store.filter(o => o.name != name)
    },
    procs' = procs,
    faults' = faults,
    removed' = removed,
    wPublished' = wPublished,
    held' = held,
    ownTokens' = ownTokens,
    exact' = exact,
    replayed' = replayed,
  }

  /// The load `p` crashes: what it staged is lost, and its requests in flight may still land; the
  /// writer is gone for good, and the replay's next attempt may begin.
  action crash(p: Who): bool = all {
    faults > 0,
    not(Set(Off, Dead, Stopped).contains(procs.get(p).prog)),
    procs' = procs.set(p, { ...IDLE, prog: if (p == W) Dead else Off, epoch: procs.get(p).epoch, slipped: procs.get(p).slipped }),
    faults' = faults - 1,
    store' = store,
    pending' = pending,
    removed' = removed,
    wPublished' = wPublished,
    held' = held,
    ownTokens' = ownTokens,
    exact' = exact,
    replayed' = replayed,
  }

  /// The replay's next attempt begins in the same process, which knows the kinds of the heads it
  /// read before: it lists what removals left first.
  action start: bool = all {
    procs.get(R).prog == Off,
    procs.get(R).epoch < STARTS,
    procs' = procs.set(R, goTo({ ...IDLE, kinds: procs.get(R).kinds, epoch: procs.get(R).epoch + 1, slipped: procs.get(R).slipped }, RLeftovers, Leftovers).pr),
    store' = store,
    pending' = pending,
    faults' = faults,
    removed' = removed,
    wPublished' = wPublished,
    held' = held,
    ownTokens' = ownTokens,
    exact' = exact,
    replayed' = replayed,
  }

  /// The intruder leaves a name the store never writes in the log's directory.
  action intrude: bool = all {
    faults > 0,
    not(holds(store, Stray)),
    store' = store.union(Set(NOBODY)),
    faults' = faults - 1,
    pending' = pending,
    procs' = procs,
    removed' = removed,
    wPublished' = wPublished,
    held' = held,
    ownTokens' = ownTokens,
    exact' = exact,
    replayed' = replayed,
  }

  pure val OUTCOMES: Set[Outcome] = Set(Ok, Failed, Unknown, Late, Taken)

  /// The loads with a request to make.
  val active: Set[Who] = Set(W, R).filter(p => procs.get(p).doing != Idle)

  /// The loads that may crash: those running.
  val crashable: Set[Who] = Set(W, R).filter(p => not(Set(Off, Dead, Stopped).contains(procs.get(p).prog)))

  /// What the request `p` makes next may meet: anything its kind of request meets while faults
  /// are left, success alone after.
  def outcomesFor(p: Who): Set[Outcome] =
    if (faults > 0) OUTCOMES.filter(o => allowed(procs.get(p).req.op, o)) else Set(Ok)

  /// What the next chunk `p` stages may need: the writer's next chunk's sets; nothing for the
  /// replay.
  def livesFor(p: Who): Set[Set[int]] =
    if (p == W) lives(numberOf(procs.get(W).staged.name) + 1) else Set(Set())

  action step = any {
    all {
      active != Set(),
      nondet who = active.oneOf()
      nondet outcome = outcomesFor(who).oneOf()
      nondet live = livesFor(who).oneOf()
      request(who, outcome, live),
    },
    all { pending != Set(), nondet landing = pending.oneOf() land(landing) },
    all { crashable != Set(), faults > 0, nondet whom = crashable.oneOf() crash(whom) },
    start,
    intrude,
  }

  /// The steps `quint run` takes and the driver's traces come from: `step`'s, each request mostly
  /// met without fault, and faults, landings, crashes and the intruder seldom, so a run of eighty
  /// steps reaches a replay of the log.
  action simulated = {
    nondet rare = 1.to(8).oneOf()
    any {
      all {
        active != Set(),
        nondet who = active.oneOf()
        nondet outcome = (if (rare == 1) outcomesFor(who) else Set(Ok)).oneOf()
        nondet live = livesFor(who).oneOf()
        request(who, outcome, live),
      },
      all { rare == 1, pending != Set(), nondet landing = pending.oneOf() land(landing) },
      all { rare == 1, crashable != Set(), faults > 0, nondet whom = crashable.oneOf() crash(whom) },
      all { rare <= 2 or active == Set(), start },
      all { rare == 1, intrude },
    }
  }

  // ---- Goals: states a trace the driver replays ends in, each reached by some run

  /// A replay read a log through.
  val replayedLog = replayed
  /// The writer found the name of its next chunk taken by a fence a replay holds.
  val fencedWriter = held != Set() and procs.get(W).answer == Refused(NameTaken)
  /// A replay's fence found a chunk above it, and the replay deletes it.
  val lostAbove = procs.get(R).prog == RLost
  /// A log opened only by a mark that landed after its open gave up was removed.
  val lateMarkRemoved = removed and procs.get(W).answer == Refused(Unavailable) and procs.get(W).prog == Stopped
  /// A replay refused a listing that held the intruder's name.
  val strayRefused = procs.get(R).answer == Refused(Strays)
  /// A replay read a log through, released it and removed all of it.
  val removedAll = replayed and store == Set()
  /// A replay removes what a removal left: a log's objects with no mark beside them.
  val leftoverRemoved = procs.get(R).prog == RLeftover and procs.get(R).doing == RemovingLog and procs.get(R).at == 2

  // ---- Invariants, named as the simulation's checks where one checks the same

  /// A removed log is never open again with its load's chunks: should its mark land again, the
  /// writer published nothing and no head of its is there.
  val NoReopenAfterRemove =
    removed implies (holds(store, Mark) implies
      (wPublished == Set() and not(store.exists(o => isHead(o.name) and o.by == W))))

  /// No two loads hold one log: every chunk the writer was told it published lies below every
  /// fence a replay published and found nothing above.
  val OneHolder = held.forall(f => wPublished.forall(n => n < f))

  /// A replay reads a chunk's bytes from its head, or from the body its head names by its token.
  val OwnTokenReplay = ownTokens

  /// An operation of which a request listed the log's directory with a name the store never
  /// writes in it ends refused, whatever it did between.
  val StraysRefused = Set(W, R).forall(p => not(procs.get(p).slipped))

  /// A replay that read a crashed load's log through read exactly the chunks its tail needs, each
  /// from the load that wrote it; one that could not is refused with a typed error and replays
  /// nothing.
  val CrashExactOrRefused = exact

  // ---- Tests

  /// What the writer's next chunk needs in a test: itself, or nothing past its last chunk.
  val nextLive: Set[int] = {
    val n = numberOf(procs.get(W).staged.name) + 1
    if (n >= CHUNKS) Set() else if (n <= 0) Set(0) else Set(n)
  }

  action writerDoes(o: Outcome): bool = request(W, o, nextLive)

  action replayDoes(o: Outcome): bool = request(R, o, Set())

  pure def namesOf(s: Set[Obj]): Set[Name] = s.map(o => o.name)

  /// An open tried again after its answer was lost, which the log never does: its first create
  /// may land after the log is removed.
  action openRetried: bool = {
    val pr = procs.get(W)
    all {
      pr.doing == Opening,
      pr.at == 1,
      pending' = pending.union(Set({ who: W, attempt: 1, wave: 0, landing: Lands(markOf(W)) })),
      store' = store.union(Set(markOf(W))),
      procs' = procs.set(W, writes(finish(pr, Done).pr, Opening, Done, Set(0)).pr),
      faults' = faults,
      removed' = removed,
      wPublished' = wPublished,
      held' = held,
      ownTokens' = ownTokens,
      exact' = exact,
      replayed' = replayed,
    }
  }

  /// A fence that takes the log without looking for a chunk above it, which a replay never does.
  action aboveIgnored: bool = {
    val pr = procs.get(R)
    all {
      pr.prog == RAbove,
      held' = held.union(Set(pr.fence)),
      procs' = procs.set(R, goTo(pr, RRemove, RemovingLog).pr),
      store' = store,
      pending' = pending,
      faults' = faults,
      removed' = removed,
      wPublished' = wPublished,
      ownTokens' = ownTokens,
      exact' = exact,
      replayed' = replayed,
    }
  }

  /// A replay that stops reading before it read all its tail needs, which a replay never does.
  action scanSkipped: bool = {
    val pr = procs.get(R)
    all {
      pr.prog == RScan,
      procs' = procs.set(R, goTo(pr, RScanOpen, Loads).pr),
      store' = store,
      pending' = pending,
      faults' = faults,
      removed' = removed,
      wPublished' = wPublished,
      held' = held,
      ownTokens' = ownTokens,
      exact' = exact,
      replayed' = replayed,
    }
  }

  /// A replay whose note of a head's kind is stale: it believes the head of chunk 1 is the chunk
  /// itself, which a replay never does.
  action kindsStale: bool = {
    val pr = procs.get(R)
    all {
      pr.prog == RTail,
      procs' = procs.set(R, { ...pr, kinds: pr.kinds.put(1, Whole) }),
      store' = store,
      pending' = pending,
      faults' = faults,
      removed' = removed,
      wPublished' = wPublished,
      held' = held,
      ownTokens' = ownTokens,
      exact' = exact,
      replayed' = replayed,
    }
  }

  /// A third fault in a test whose scenario takes more than the bound allows, which a behavior
  /// never has.
  action faultGranted: bool = all {
    faults' = faults + 1,
    store' = store,
    pending' = pending,
    procs' = procs,
    removed' = removed,
    wPublished' = wPublished,
    held' = held,
    ownTokens' = ownTokens,
    exact' = exact,
    replayed' = replayed,
  }

  /// The writer opens its log and publishes its two chunks, the second in parts, then deletes the
  /// first, which the second no longer needs.
  run aWriterPublishesItsChunksAndDeletesWhatItNoLongerNeedsTest =
    init
      .then(13.reps(_ => writerDoes(Ok)))
      .expect(all {
        wPublished == Set(0, 1),
        namesOf(store) == Set(Mark, Body((1, W)), ChunkHead(1)),
        procs.get(W).prog == Stopped,
      })

  /// A replay of a log the writer left reads its tail, fences above it, reads the chunk the tail
  /// still needs, releases the log and removes all of it.
  run aReplayReadsWhatTheTailNeedsAndRemovesTheLogTest =
    init
      .then(4.reps(_ => writerDoes(Ok)))
      .then(request(W, Ok, Set(0, 1)))
      .then(7.reps(_ => writerDoes(Ok)))
      .expect(procs.get(W).prog == Stopped and namesOf(store) == Set(Mark, ChunkHead(0), Body((1, W)), ChunkHead(1)))
      .then(start)
      .then(23.reps(_ => replayDoes(Ok)))
      .expect(all {
        store == Set(),
        held == Set(2, 3),
        replayed,
        procs.get(R).prog == Off,
        CrashExactOrRefused,
        OwnTokenReplay,
      })

  /// A replay that fences the log at the writer's next chunk takes the name first: the writer
  /// finds it taken, deletes the body it uploaded and stops.
  run aWriterFindsItsNextChunkTakenByAFenceAndStopsTest =
    init
      .then(5.reps(_ => writerDoes(Ok)))
      .then(start)
      .then(9.reps(_ => replayDoes(Ok)))
      .expect(held == Set(1) and procs.get(R).prog == RScanOpen)
      .then(7.reps(_ => writerDoes(Ok)))
      .expect(all {
        procs.get(W).answer == Refused(NameTaken),
        procs.get(W).prog == Stopped,
        not(holds(store, Body((1, W)))),
        wPublished == Set(0),
        fencedWriter,
        OneHolder,
      })

  /// A fence published at a number the writer freed, below its newest chunk, is found below that
  /// chunk and deleted, and the replay tries again above it.
  run aFenceBelowTheWritersNewestChunkIsDeletedTest =
    init
      .then(2.reps(_ => writerDoes(Ok)))
      .then(start)
      .then(4.reps(_ => replayDoes(Ok)))
      .expect(procs.get(R).prog == RFenceStage and procs.get(R).fence == 0)
      .then(11.reps(_ => writerDoes(Ok)))
      .expect(wPublished == Set(0, 1) and not(holds(store, ChunkHead(0))))
      .then(5.reps(_ => replayDoes(Ok)))
      .expect(lostAbove)
      .then(replayDoes(Ok))
      .expect(all {
        procs.get(R).prog == RTail,
        procs.get(R).tries == 1,
        not(holds(store, ChunkHead(0))),
        held == Set(),
        OneHolder,
      })

  /// A writer whose mark's create went unanswered finds no mark of its own and gives its load up;
  /// the mark landing later opens a log no chunk was published in, which a replay fences and
  /// removes.
  run aMarkLandingLateOpensALogAReplayRemovesTest =
    init
      .then(writerDoes(Ok))
      .then(writerDoes(Late))
      .then(writerDoes(Ok))
      .expect(procs.get(W).answer == Refused(Unavailable) and procs.get(W).prog == Stopped)
      .then(land({ who: W, attempt: 1, wave: 0, landing: Lands(markOf(W)) }))
      .then(start)
      .then(11.reps(_ => replayDoes(Ok)))
      .expect(all { lateMarkRemoved, store == Set(), procs.get(R).prog == Off, NoReopenAfterRemove })

  /// A replay that finds what an intruder left in a log's directory, which the store never
  /// writes, refuses it rather than read or remove it.
  run aStrayInALogsDirectoryIsRefusedTest =
    init
      .then(intrude)
      .then(start)
      .then(4.reps(_ => replayDoes(Ok)))
      .expect(all { strayRefused, not(removed), holds(store, Stray), StraysRefused })

  /// An open tried again after its answer was lost would reopen its removed log: the invariant
  /// refuses it.
  run aRetriedOpenWouldReopenARemovedLogTest =
    init
      .then(writerDoes(Ok))
      .then(openRetried)
      .then(3.reps(_ => writerDoes(Ok)))
      .expect(wPublished == Set(0))
      .then(crash(W))
      .then(start)
      .then(19.reps(_ => replayDoes(Ok)))
      .expect(removed and store == Set() and NoReopenAfterRemove)
      .then(land({ who: W, attempt: 1, wave: 0, landing: Lands(markOf(W)) }))
      .expect(not(NoReopenAfterRemove))

  /// A head's create whose answer was lost is tried again, finds its name taken, and reads its
  /// token back: the head is its own, and the chunk published.
  run aCreateWhoseAnswerWasLostReadsItsOwnTokenTest =
    init
      .then(3.reps(_ => writerDoes(Ok)))
      .then(writerDoes(Unknown))
      .then(3.reps(_ => writerDoes(Ok)))
      .expect(wPublished == Set(0) and faults == FAULTS - 1 and procs.get(W).doing == Staging(1))

  /// A fence that took the log without looking above it would hold it below the writer's newest
  /// chunk: the invariant refuses it.
  run aFenceIgnoringChunksAboveItWouldShareTheLogTest =
    init
      .then(2.reps(_ => writerDoes(Ok)))
      .then(start)
      .then(4.reps(_ => replayDoes(Ok)))
      .then(11.reps(_ => writerDoes(Ok)))
      .then(3.reps(_ => replayDoes(Ok)))
      .expect(procs.get(R).prog == RAbove)
      .then(aboveIgnored)
      .expect(not(OneHolder))

  /// A replay that stopped reading before it read all its tail needs would replay less than the
  /// log holds: the invariant refuses it.
  run aReplaySkippingANeededChunkWouldNotBeExactTest =
    init
      .then(4.reps(_ => writerDoes(Ok)))
      .then(request(W, Ok, Set(0, 1)))
      .then(7.reps(_ => writerDoes(Ok)))
      .then(start)
      .then(10.reps(_ => replayDoes(Ok)))
      .expect(procs.get(R).prog == RScan)
      .then(scanSkipped)
      .then(replayDoes(Ok))
      .expect(not(CrashExactOrRefused))

  /// A replay that goes on past a listing that held the intruder's name, as if it were not there,
  /// ends its operation unrefused: the invariant refuses it.
  run aReplayIgnoringAStrayWouldNotRefuseItTest =
    init
      .then(intrude)
      .then(start)
      .then(3.reps(_ => replayDoes(Ok)))
      .expect(StraysRefused)
      .then(requesting(R, Ok, Set(), false))
      .expect(not(StraysRefused))

  /// A replay that read a head as the chunk itself where it names a body would read the wrong
  /// bytes: the invariant refuses it.
  run aReplayWithAStaleKindWouldReadAHeadAsItsChunkTest =
    init
      .then(13.reps(_ => writerDoes(Ok)))
      .then(start)
      .then(3.reps(_ => replayDoes(Ok)))
      .expect(procs.get(R).prog == RTail)
      .then(kindsStale)
      .then(2.reps(_ => replayDoes(Ok)))
      .expect(not(OwnTokenReplay))

  /// A read left unanswered ends its attempt without a look at the object: the next request is
  /// the read of the next attempt.
  run aReadLeftUnansweredIsTriedAgainWithoutALookTest =
    init
      .then(13.reps(_ => writerDoes(Ok)))
      .then(start)
      .then(3.reps(_ => replayDoes(Ok)))
      .then(replayDoes(Ok))
      .then(replayDoes(Late))
      .expect(procs.get(R).req.op == Gets and procs.get(R).attempt == 2 and not(procs.get(R).peek))

  /// A fence of an earlier start of the replay that landed late is not the replay's own head: the
  /// create of a later start reads a token that is not its own back, finds its name taken, and
  /// fences again.
  run aFenceOfAnEarlierStartIsNotTheReplaysOwnTest =
    init
      .then(2.reps(_ => writerDoes(Ok)))
      .then(start)
      .then(5.reps(_ => replayDoes(Ok)))
      .then(replayDoes(Late))
      .then(replayDoes(Failed))
      .then(replayDoes(Ok))
      .expect(procs.get(R).prog == Off and pending.size() == 1)
      .then(start)
      .then(5.reps(_ => replayDoes(Ok)))
      .then(faultGranted)
      .then(replayDoes(Failed))
      .then(land({ who: R, attempt: 1, wave: 0, landing: Lands(fenceOf(0, 1, Set())) }))
      .then(2.reps(_ => replayDoes(Ok)))
      .expect(procs.get(R).prog == RTail and procs.get(R).tries == 1 and procs.get(R).epoch == 2)
}
