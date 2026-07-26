# Feature Specification: Audit Remediation — Silent Losses Closed, the Record Made True

**Feature Branch**: `020-audit-remediation`

**Created**: 2026-07-26

**Status**: Draft

**Input**: User description: "use /speckit-specify to implement NEXT_STEPS.md (lets skip CI as it is a billing issue I cant fix now)"

## Source Document

The authoritative evidence base is `NEXT_STEPS.md` at the repo root, produced
2026-07-26 against commit `634222e` on `main` (feature 019 complete, all four
bars PASS). It is the product of eleven parallel analysis lenses over the whole
workspace and the entire `specs/` corpus: 175 raw findings, deduplicated and
tagged with impact and effort, with every claimed defect then put through an
adversarial verification pass that tried to refute it.

**That verification pass is the reason this feature can be planned at all.**
Forty-seven defect claims were checked; **29 survived and 18 were refuted**.
The refutations are not noise — several were "mechanics accurate, consequence
unreachable", which is precisely the failure mode that turns a remediation
feature into churn. They are recorded in `NEXT_STEPS.md` Appendix A and adopted
here as binding non-goals.

This specification defines the outcomes and how completion is judged.
`NEXT_STEPS.md` holds the item-level detail, the `file:line` anchors, the
reproduction reasoning for each defect, and the refutation reasoning for each
rejected claim — the same division of labour `PERF_ANALYSIS.md` had with feature
019. Requirements here name *behaviour that must change*; the anchors that
locate it live in the audit.

Three properties of the source document carry into this feature as obligations:

- **Its refutations are binding.** A refuted claim MUST NOT be re-raised as
  work without new evidence that defeats the recorded refutation. Re-litigating
  them is the specific waste this feature exists to avoid.
- **Its severities are honest, not uniform.** The audit separates "silently
  wrong data today" from "latent behind a capability no in-tree connector
  declares". The increment order below follows that separation; a low-severity
  item is not promoted because it is cheap.
- **Its performance items are questions, not instructions.** Every performance
  entry in the audit is phrased measure-then-take. Feature 019 removed
  allocations twice on impeccable counting arguments and measured **worse both
  times** (recorded as D-13 and D-21). That precedent is adopted here as a
  standing null hypothesis.

### Standing of record

The state this feature starts from, and must not regress:

| property | recorded standing |
|---|---|
| Benchmark matrix vs reference implementation | 13.2× / 1.7× / 95.0× / 63.6× / 2.6× — no losses, no parities |
| Enforcement bars | 4 of 4 PASS against the justifying session |
| Workspace gate (local) | green: 675+ tests, crash sweep 23/23, 12 golden pins, lint, doc-tests |
| Continuous integration | **not executing** — every job fails before its first step |
| Confirmed open defects | 29 (2 high, 11 medium, 16 low) |
| Named recorded deferrals with fired triggers | 4 (D17, D18, D19, lowering parity) |
| Mutation record | generated 2026-07-20 at `f58570b` — predates features 006–019 |
| License text shipped | none |

### Decisions adopted at specification time

These were open at audit time. They are settled here and are not re-opened
during planning.

- **E1 — Continuous integration repair is OUT OF SCOPE.** The audit
  root-caused the total CI failure to an organisation billing state — every job
  fails 3–5 seconds after start with zero steps recorded, and the surviving
  check-run annotation reads *"The job was not started because recent account
  payments have failed or your spending limit needs to be increased."* The owner
  cannot resolve that during this feature. **Consequence, recorded rather than
  worked around**: the local gate remains the gate of record, exactly as it was
  for 019. Any item whose verification requires a hosted runner lands as a
  reviewed change with its verification explicitly marked unperformed — it is
  never claimed green, and it never blocks an increment from merging.
- **E2 — Publishing is not part of this feature.** The recorded 0.2 → 0.3
  window's remaining work is *readiness*: the tree must be publishable and its
  metadata truthful. The act of publishing is the owner's, outside this
  feature, and no requirement here is satisfied by anything appearing on a
  package registry.
- **E3 — Defect fixes are behaviour changes; nothing else is.** Where a fix
  changes observable behaviour, that change is confined to the defect named in
  the audit. Persisted data formats, emitted row-identity bytes, and golden
  statement pins stay byte-identical unless a requirement here explicitly
  versions them. Two fixes are known to be schema-affecting (integer widening
  and decimal refusal); both are called out in their stories rather than
  discovered at review.
- **E4 — Greenfield, as always.** Where this feature replaces an
  implementation — a duplicated channel core, a hand-maintained mirror — the
  superseded copy is DELETED in the same change. No shim, alias, dual path, or
  flag keeps it reachable.
- **E5 — Red before green.** Every defect fix lands with a regression pin
  captured against the pre-fix build, so the pin is demonstrated to fail on the
  defect rather than merely to pass afterwards. This is the 019 US6 pattern
  (identity corpus captured from the pre-change build) generalised to the whole
  feature.
- **E6 — One disposition per audit item, none silent.** `NEXT_STEPS.md`
  enumerates roughly 120 items across eight sections. Every one ends this
  feature in exactly one terminal state — fixed, rejected with a recorded
  reason, or deferred with a named re-trigger. A section this feature declines
  wholesale is declined explicitly, not by omission.

## User Scenarios & Testing *(mandatory)*

Each story is an independently mergeable increment: it can be developed,
verified, and merged on its own with the full local gate green, and it delivers
a closed defect class or a corrected record by itself. Order is
value-per-risk — the zero-risk record correction first, then the classes where
data is silently wrong today, then hardening, then the deferrals and the
measurement queue.

### User Story 1 - The repository stops misinforming its own readers (Priority: P1)

A maintainer opens the project instructions and is told the last feature is
"planned, not yet implemented" — while its results are merged, measured, and
enforcing four bars. They read that the keep-in-sync cell is a loss and an
optimisation target; it is a 2.6× win. They read a completion record whose
header says the work is in progress, whose middle says a session was never run,
and whose end records that exact session passing. A downstream consumer looks
for the license the project declares in every crate manifest and finds no
license text at all.

Nothing in this story changes engine behaviour. It changes what the project
says about itself, so that every later increment — and every future feature —
is planned against facts.

**Why this priority**: it is the cheapest work in the feature and the only work
that makes all the other work correct. A stale instruction file actively
misdirects the next planning session, and the missing license is a distribution
defect that predates every open bug. Zero code risk, immediate effect.

**Independent Test**: a reader picks any status claim in the project
instructions, the 019 completion record, the performance analysis, or the
benchmark documentation, and checks it against the artifacts; every claim holds.
The repository ships the text of the license it declares.

**Acceptance Scenarios**:

1. **Given** the project instruction file, **When** a maintainer reads the
   status of the most recent completed feature, **Then** it states the feature
   is complete, cites its recorded results, and does not present superseded
   figures from an earlier feature as current.
2. **Given** the 019 completion record, **When** a reader follows it end to
   end, **Then** no statement in it contradicts another, its header status
   matches the state of its task list, and every contract row has a terminal
   disposition.
3. **Given** a requirement that shipping code deliberately does not satisfy
   because measurement inverted its premise, **When** a reader looks it up,
   **Then** they find the measured inversion recorded in place, with the
   condition under which it would be revisited.
4. **Given** the performance analysis document, **When** a reader plans against
   it, **Then** a banner tells them it has been executed, points at the
   dispositions, and names the claims its own execution disproved.
5. **Given** the published documentation, **When** an operator asks how to get
   more aggregate throughput, **Then** they find the measured concurrent-pipeline
   scaling and the deliberate design trade behind it.
6. **Given** a distributor or package consumer, **When** they look for the
   license the manifests declare, **Then** the license text is present in the
   repository.
7. **Given** any project document that describes a bar, an artifact format, a
   build verb, or a configuration behaviour, **When** it is compared against the
   corresponding artifact or code, **Then** they agree.

---

### User Story 2 - Values reach the destination or are counted, never silently dropped (Priority: P1)

A pipeline ingests semi-structured records. One column carries identifiers at
the top of the unsigned 64-bit range. The engine types the column as a signed
integer, then writes NULL for every value that does not fit — no error, no
discard count, no warning. The run reports success and the data is gone. The
same shape of silence sits behind declared type hints: a hinted column that
receives an object loses its pin entirely, a decimal that exceeds its declared
precision is stored out of range and fails much later with a message about the
destination, and every value that simply does not parse under a hint becomes
NULL with nothing counted anywhere.

The crate's own rule is "counted, never silent", and its sibling passthrough
path already refuses the unsigned case loudly for exactly this reason. This
story makes the rule true on the inference path.

**Why this priority**: this is the highest-severity class in the audit — data
loss with a success report, reachable through ordinary configuration, on the
engine's most-used ingestion path. It is also the class where the fix is
provable: values are either represented, refused, or counted.

**Independent Test**: a corpus containing full-range unsigned integers,
hint-violating objects, over-precision decimals and unparseable hinted values
is ingested; every value is either present at the destination, or reflected in
the run's discard accounting, or refused with a typed error. No value
disappears silently. Every emitted row-identity byte is unchanged against a
corpus captured from the pre-change build.

**Acceptance Scenarios**:

1. **Given** a column whose observed integers exceed the range of the type
   inference would choose, **When** the batch is built, **Then** the values
   arrive at the destination intact in a representation that can hold them —
   not as NULL.
2. **Given** a column with a declared type hint, **When** a record carries an
   object or array in that column, **Then** the hint still governs the column's
   type; the column does not silently widen or abort the run as though the hint
   were absent.
3. **Given** a declared decimal precision, **When** a value needs more digits
   than the precision allows, **Then** the value is refused by the same rule
   that already refuses values needing more scale — never stored beyond its
   declared precision.
4. **Given** a hinted column receiving values that cannot be represented under
   that hint, **When** the load completes, **Then** those values are counted in
   the run's accounting, or — if counting them is rejected at plan time — the
   behaviour is documented on the hint's own public surface and pinned by test.
5. **Given** an embedding application that supplies a type hint the engine
   cannot honour, **When** the pipeline runs, **Then** it receives a typed
   configuration error naming the column — not a panic from inside the
   shredder.
6. **Given** any of the above changes, **When** the identity corpus captured
   from the pre-change build is replayed, **Then** every emitted row identity is
   byte-identical.

---

### User Story 3 - A file destination's "replace" replaces, and a resumed read resumes (Priority: P1)

An incremental pipeline reads a growing set of column-oriented files. One input
is rewritten larger between runs — its earlier content changed, its size grew.
The size tripwire does not fire because the size differs; the identity tripwire
does not fire because it only covers same-size rewrites; and no content hash is
recorded for this unit kind at all. The next run resumes at a recorded unit
index into a file that no longer has the same content there, and loads the
wrong rows without a word.

On the write side, a user switches output format, or removes partitioning, and
runs a full-refresh load. The destination only recognises as its own the files
matching the configuration it has *now*, so the previous load's files survive
the "replace" and the table becomes a silent mixture of two loads. And when a
partition value happens to equal the table's own name, the key that identifies
the destination's own objects is split at the wrong place: the truncation
targets an object that does not exist, the commit fails, and the real object is
never removed.

**Why this priority**: two of the three are silently wrong data on the engine's
second-most-used connector family, and the third turns a legitimate
configuration into a fatal commit. The blast radius is a user's warehouse table.

**Independent Test**: a grown-and-rewritten input is refused or correctly
re-read rather than silently resumed into; a destination reconfigured across
format and partitioning still contains exactly one load's rows after a
full-refresh; a partition value colliding with the table name completes
truncation and commit.

**Acceptance Scenarios**:

1. **Given** an input file that grew and whose already-consumed prefix changed,
   **When** the next run plans it, **Then** the change is detected and surfaced
   — the run never resumes from a recorded position that the current content
   does not justify.
2. **Given** an input that legitimately grew by appending, **When** the next run
   plans it, **Then** the resume proceeds — the integrity check does not reject
   honest appends.
3. **Given** a destination that previously wrote files in another format or
   under partition directories, **When** a full-refresh load runs after the
   configuration changed, **Then** the destination contains only the current
   load's rows.
4. **Given** a partition value equal to the table name, **When** a full-refresh
   load runs, **Then** truncation identifies the destination's own objects
   correctly and the commit succeeds.
5. **Given** a two-pass text format whose input changes between the passes,
   **When** the second pass encounters a value the first pass's inference cannot
   explain, **Then** every inferred type fails with the same typed error —
   including boolean, which currently coerces silently.
6. **Given** a deterministic storage failure that cannot heal, **When** it
   occurs, **Then** it is not retried as though it were transient.
7. **Given** a long-lived pipeline over a rotating set of inputs, **When** it
   has run many times, **Then** its recorded position state does not grow
   without bound — or the growth is a documented, decided retention rule rather
   than an accident.

---

### User Story 4 - A stalled server cannot hang the pipeline (Priority: P2)

A pipeline reads a paged web API. The server accepts the connection and then
stops responding mid-body. There is no request timeout anywhere in the client,
so the read blocks forever: no typed error, no retry budget engaged, no
progress, no failure. An operator sees a pipeline that is simply never finished.

Nearby, a pipeline configured to page through a POST endpoint whose request
body is not a keyed document sends the byte-identical request for every page. The
duplicate-request guard does not notice, because the page parameters it hashes
do change — they are just never used. The run ingests up to ten thousand copies
of the first page and then fails with a message about a page limit.

**Why this priority**: an unbounded hang is the worst failure mode a scheduled
pipeline can have, because it consumes the schedule slot and produces no signal.
The pagination case is bounded but produces a large volume of duplicate data
before a misleading error.

**Independent Test**: a deliberately stalling endpoint produces a typed,
timely failure that engages the retry budget; a pagination configuration that
cannot advance is refused before the first request rather than discovered after
ten thousand.

**Acceptance Scenarios**:

1. **Given** a server that accepts a connection and never responds, **When** a
   stream reads from it, **Then** the request fails with a typed error within a
   bounded time, and the engine's retry handling applies.
2. **Given** a token endpoint that stalls, **When** credentials are refreshed,
   **Then** the same bound applies.
3. **Given** a client that cannot be constructed in the current environment,
   **When** the pipeline starts, **Then** it reports a typed configuration
   failure rather than panicking.
4. **Given** a pagination configuration whose page parameters cannot reach the
   request, **When** the configuration is validated, **Then** it is rejected
   with a typed error naming the conflict — before any request is sent.
5. **Given** a server that expresses its pacing as a point in time rather than a
   number of seconds, **When** it rate-limits a request, **Then** the pacing is
   honoured, still bounded by the configured cap.
6. **Given** several child streams whose credentials expire together, **When**
   they refresh concurrently, **Then** a freshly obtained credential is not
   discarded by another stream's stale failure.
7. **Given** a parent record whose field value contains characters that are
   structural in a request target, **When** it is interpolated into a child
   stream's target, **Then** the value is encoded so it cannot restructure the
   request.

---

### User Story 5 - A declared schema contract means what it says (Priority: P2)

A user pins a stream's schema policy to freeze: no drift, ever, and a typed
error before any row is written if drift appears. Within a run, that holds. But
the registry that detects drift is built empty at the start of every run, so the
first batch of run N+1 is a table creation — and table creation is always
allowed to proceed. A column that widened, or appeared, between two runs sails
through the frozen contract as part of "creating" a table that already exists.
Meanwhile per-stream schema state is persisted on every commit, apparently for
exactly this cross-run detection, and nothing anywhere reads it.

Inside a single run there is a matching asymmetry: under freeze, a new scalar
field aborts the run, but a new list-of-objects field silently creates and loads
an entire new child table.

**Why this priority**: it is a promise the project makes in its design document
without qualification, and the engine does not keep it. It is placed after the
silent-data-loss stories because no data is currently wrong — the contract is
weaker than advertised, which is serious but not corrupting. It carries the
feature's only genuine design question, and that question needs the room this
position gives it.

**Independent Test**: a frozen stream is run twice with drift introduced
between the runs; the outcome matches whatever the project promises about
freeze after this story — and the promise, the code, and the persisted state
all agree. Within a run, a new nested collection is treated by the same rule as
a new column.

**Acceptance Scenarios**:

1. **Given** a stream frozen by policy, **When** its schema drifts between two
   runs, **Then** the engine's behaviour matches its documented contract — and
   the contract does not promise more than the engine delivers.
2. **Given** per-stream schema state persisted at every commit, **When** this
   story completes, **Then** that state is either consumed by the mechanism it
   exists for, or documented as diagnostic-only — it is not left written and
   never read.
3. **Given** a frozen stream, **When** a record introduces a new nested
   collection mid-run, **Then** the policy applies to that new table exactly as
   it applies to a new column.
4. **Given** a stream whose first batch of a run legitimately observes fewer
   columns than the previous run wrote, **When** drift is evaluated, **Then**
   the absence is not reported as drift.

---

### User Story 6 - Nested types work against a real catalog, and are proven to (Priority: P2)

A destination advertises that it accepts structured and list-valued columns.
Its schema mapping builds them. No test — anywhere, at any tier — has ever
created one against a live catalog. Reconciliation compares a live table's
column types against a freshly built wanted schema *including the identifiers
the catalog assigns to nested fields*, and the two numbering conventions differ.
The second load of an unchanged stream carrying a structured column can
therefore fail as though the user had made contradictory changes.

**Why this priority**: an advertised capability with no live coverage is
precisely the gap the project's "verified connectors" claim exists to forbid.
The defect is medium rather than high only because nothing pins it either way
today — which is itself the finding.

*Amended at plan time.* Phase 0 upgraded this from the audit's "plausible" to
**confirmed and guaranteed**: the identity comparison includes the nested field
identifiers, and for any catalog that normalizes them on create, the second
load of an unchanged stream fails for every schema in which a structured or
list column is followed by another column. The acceptance scenarios below are
unchanged; only the certainty is.

**Independent Test**: a stream with a structured column and a list-valued
column is loaded into a live catalog, read back, and then loaded again
unchanged; the second load settles without reporting drift.

**Acceptance Scenarios**:

1. **Given** a table whose schema the catalog has normalised, **When** an
   unchanged stream is ensured a second time, **Then** reconciliation reports no
   drift.
2. **Given** a genuinely contradictory change to a nested column, **When** it is
   ensured, **Then** it is still refused with a typed error.
3. **Given** the advertised structured and list-valued capabilities, **When** the
   test suite runs against a live catalog, **Then** those capabilities are
   exercised end to end, including read-back.
4. **Given** a container-backed test fixture, **When** it starts, **Then** it
   pulls a pinned image version rather than a moving tag.
5. **Given** a change from nullable to required, or the reverse, **When**
   reconciliation runs, **Then** the mismatch is surfaced there rather than
   later as an unrelated alignment failure.

---

### User Story 7 - The engine's remaining sharp edges are filed down (Priority: P2)

Sixteen smaller defects share a shape: each one is either latent behind a
capability no shipped connector declares, reachable only from an embedding
application, or a mis-signal rather than a mis-computation. A wrapped
out-of-range time value that becomes a plausible wrong time. A decimal encoded
at the schema's scale rather than the data's. A transient file lock that aborts
a run at one entry point and retries at another. Diagnostic spans attributed to
the wrong stream under concurrency. Recovery-log residue that accumulates
without bound when a pipeline dies before its first checkpoint. Internal
invariant failures that tell the operator to fix their configuration. Failures
logged nowhere.

None of these is worth its own increment; together they are the difference
between an engine that is correct and one that is trustworthy under stress.

**Why this priority**: high aggregate value, low individual risk, and every one
is a small self-contained change with a cheap pin. It sits after the classes
that are wrong today.

**Independent Test**: each item has a pin that fails on the pre-fix build. The
group merges as one increment with the full gate green.

**Acceptance Scenarios**:

1. **Given** a value outside the range its wire form can represent, **When** it
   is encoded for a destination, **Then** the write fails with a typed error
   rather than transmitting a different, plausible value.
2. **Given** encoded values whose scale is carried by the data, **When** they
   are written, **Then** the scale used is the data's own.
3. **Given** an environmental failure that one entry point of a connector
   treats as transient, **When** the same failure occurs at that connector's
   other entry points, **Then** it is classified the same way.
4. **Given** several streams running concurrently, **When** diagnostics are
   emitted, **Then** each is attributed to the stream that produced it.
5. **Given** a pipeline that repeatedly fails before its first checkpoint,
   **When** it is retried many times, **Then** recovery-log residue does not
   accumulate without bound.
6. **Given** a failure that can only be an internal defect, **When** it
   surfaces, **Then** its classification and the process exit code direct the
   operator to report a bug rather than to edit configuration.
7. **Given** a destination that declares it cannot accept a type, **When** that
   type appears nested inside one it does accept, **Then** it is either
   converted as the shallow case is, or the capability combination is refused —
   never passed through unconverted.
8. **Given** a failure on a path whose result is discarded, **When** it occurs,
   **Then** it is at least visible in diagnostics rather than lost entirely.
9. **Given** an identifier-normalisation bound, **When** normalisation runs at
   the edge of that bound, **Then** the stated bound holds.

---

### User Story 8 - The gate becomes as strong as the project claims (Priority: P2)

The project's method is a gate: tests, sweeps, mutation testing, golden pins.
Its mutation record was generated before nine features landed — before the
recovery-log format was rewritten, before the value encoder was rewritten,
before a workspace-wide refactor renamed most of the files it names. It
describes code that no longer exists, and the tests written to close its
survivors have never been shown to kill their current targets. A test named for
an invariant asserts something else. A comment claims a test covers an input it
does not touch. Test containers carry no identifying label, and twice a leaked
set of them filled the host disk and turned the gate red. A crash sweep passed
twenty-three of twenty-three while two real exactly-once defects were live,
because no crash point can produce the one state that matters — the destination
committed and the client never learned.

**Why this priority**: every later claim in the project rests on this gate. It
is placed after the defect classes because a stronger gate is worth most once
the known defects are out of it, and because several of the pins this story
writes are the pins the earlier stories need anyway.

**Independent Test**: the mutation record is regenerated against the current
tree and every survivor has a terminal disposition; each named pin fails on a
deliberately broken build; a single command reclaims every container the test
suite starts.

**Acceptance Scenarios**:

1. **Given** the current tree, **When** mutation testing runs, **Then** the
   record reflects code that exists, and every survivor is either killed by a
   new pin or recorded as equivalent with a reason.
2. **Given** an input whose only consumer is an internal budget, **When** that
   input is broken, **Then** a test fails — and the comment claiming existing
   coverage is corrected to state what is actually true.
3. **Given** a test named for an invariant, **When** it runs, **Then** it
   asserts that invariant.
4. **Given** a guard that only matters when two capabilities disagree, **When**
   the disagreeing combination is exercised, **Then** a test covers it.
5. **Given** an aborted test run that skipped cleanup, **When** the operator
   runs one reclaim command, **Then** every container and volume the suite
   created is removed, identified by label rather than by guesswork.
6. **Given** the state where a destination committed and the client did not
   learn, **When** the crash sweep runs, **Then** that state is either swept or
   recorded as a deliberate boundary with the coverage that stands in for it.
7. **Given** a container-backed test that fails intermittently, **When** it
   does, **Then** the occurrence is recorded as data rather than absorbed by a
   re-run convention.

---

### User Story 9 - The tree is ready to be published (Priority: P3)

The project's next publish is a recorded semver window, and the queue of work
blocking it is empty of design decisions — what remains is mechanics. Package
descriptions that would become registry headlines are wrong: one says the
configuration format is the one the tool does not parse; another describes a
connector as a source when it has been a source and a destination for two
features. No crate sets the fields that give a registry page a body. Nothing
ever builds the documentation, so a broken link surfaces after publication. The
public surface of the semver-sacred crates can ship undocumented.
Whole-workspace builds mask per-crate breakage that a publish would expose.

**Why this priority**: it gates a release, not correctness, and the owner
controls when the release happens. Nothing here is urgent; everything here is
cheap and must be true before the window is used.

**Independent Test**: a packaging dry run for each publishable crate succeeds
and lists complete, accurate metadata; a documentation build is clean; each
crate builds in the feature configurations a consumer can select.

**Acceptance Scenarios**:

1. **Given** each publishable crate, **When** its packaging is verified,
   **Then** the description matches what the crate actually does and the fields
   that render a registry page are present.
2. **Given** the documentation build, **When** it runs, **Then** it completes
   with no warnings.
3. **Given** the crates whose interfaces are semver-sacred, **When** their
   public items are checked, **Then** each is documented.
4. **Given** a consumer selecting a narrowed feature set, **When** that
   configuration is built, **Then** it compiles.
5. **Given** the compatibility gate, **When** it runs, **Then** it covers every
   crate this project publishes.
6. **Given** an item that can only be verified by a hosted runner, **When** this
   story completes, **Then** the change is landed and reviewed, and its
   unperformed verification is recorded as such.

---

### User Story 10 - Every recorded deferral is taken or re-recorded (Priority: P3)

Feature 017 deferred four items with named re-trigger conditions. Every one of
those triggers has since fired, and none of the four was taken or re-recorded —
they simply stopped being mentioned. Two implementations of the same
backpressure accounting still ship side by side. A hand-maintained parity
between two lowering paths, deferred on the bet that it would not drift, has
drifted. A configuration mirror whose own comment describes how it will silently
lose a field is still a hand-maintained mirror, next to a sibling that shows the
fix.

A deferral with a fired trigger and no disposition is indistinguishable from a
forgotten defect. This story closes the class.

**Why this priority**: it is structural debt with no user-visible symptom
today, and its value is mostly in what it prevents. Taking it after the defect
work means the refactors move code that is already correct.

**Independent Test**: for each named deferral, the repository contains either
the taken change or a fresh record with a new trigger; no invariant is
hand-maintained in two places without a machine-checked parity.

**Acceptance Scenarios**:

1. **Given** a correctness invariant implemented twice, **When** this story
   completes, **Then** one implementation remains and the other is deleted.
2. **Given** two code paths required to stay in agreement, **When** they are
   checked, **Then** either they share one implementation or a test fails when
   they disagree.
3. **Given** a configuration surface mirrored by hand, **When** a field is added
   to the underlying configuration, **Then** either it is automatically
   reachable, or a test fails.
4. **Given** each remaining named deferral, **When** the close-out is read,
   **Then** it is taken, rejected with a reason, or re-recorded with a new
   trigger — never silent.
5. **Given** the manifests, **When** their dependencies are compared against
   actual usage, **Then** unused declarations are gone and the documents
   describing them agree.

---

### User Story 11 - The performance queue is answered, not assumed (Priority: P3)

The audit leaves a queue of performance questions, several of them owed from
earlier features: a plan capture for the single largest server-side number in
the matrix, never taken; a blocked-time attribution the previous analysis named
as its own top gap, never run; a value-encoder fast path whose prize the previous
close-out sized at a fifth of the encoder, declined on dependency grounds; an
allocator comparison explicitly deferred until two other changes landed, which
both landed; and a redundant download of every already-consumed remote object on
every run.

This story runs the measurements. It takes only what measurement says to take.
The project has two recorded cases where removing allocations on an airtight
counting argument measured worse, and that is the expected outcome here until
proven otherwise.

**Why this priority**: the matrix currently has no losses and no parities, so
every item here is headroom rather than a fix. It is last because measurement
capacity is the scarce resource and it should be spent after the correctness
work stops changing the code being measured.

**Independent Test**: each queued question ends with a recorded number and a
decision — taken with before/after evidence, or rejected with the measurement
that rejected it. No performance change ships without a measured win.

**Acceptance Scenarios**:

1. **Given** the largest server-side cost in the matrix, **When** this story
   completes, **Then** a captured execution plan for it exists in the record,
   and any change proposed against it is justified by that plan rather than by
   intuition.
2. **Given** a candidate optimisation, **When** it is measured and does not
   improve the target, **Then** it is not shipped, and the measurement is
   recorded so it is not attempted a third time.
3. **Given** a candidate optimisation that does improve the target, **When** it
   ships, **Then** the affected byte-identity and golden pins are unchanged and
   the before/after measurement is recorded.
4. **Given** an incremental pipeline over remote objects, **When** it runs
   against inputs it has already fully consumed, **Then** it does not transfer
   them again.
5. **Given** each remaining recorded performance deferral, **When** the close-out
   is read, **Then** it carries a measured disposition rather than an open
   trigger.
6. **Given** the enforcement bars, **When** this story completes, **Then** all
   of them still pass.

### Edge Cases

- What happens when a defect fix changes an inferred column's type for existing
  users — how is the schema-affecting change surfaced rather than discovered?
- What happens when a regression pin cannot be captured against the pre-fix
  build because the defect is only reachable through an embedding application
  the test suite does not have?
- How does the feature handle an audit item that closer inspection shows to be
  wrong — the fix is written, and the defect turns out not to exist?
- What happens when a fix for a latent capability combination has no in-tree
  connector that can exercise it? Is a synthetic destination in the test kit
  acceptable evidence, and is that evidence recorded as synthetic?
- How is an item verified whose only verification surface is a hosted runner
  that cannot execute?
- What happens when the regenerated mutation record produces a far larger
  survivor list than the stale one, more than this feature can triage?
- What happens when the integrity check added for rewritten inputs rejects a
  legitimate append pattern in the field?
- How does the retention rule for accumulated position state behave for an input
  that disappears and later returns?
- What happens when a container reclaim command runs while another developer's
  test suite is using labelled containers on the same host?
- What happens when a measurement in the performance queue cannot be taken
  because the machine cannot be made quiet, or the required kernel setting is
  not writable?
- How does an increment behave if the local gate is red on the merge base for a
  reason unrelated to the increment?

## Requirements *(mandatory)*

### Functional Requirements

**Cross-cutting: how any change in this feature is allowed to land**

- **FR-001**: Every defect fix MUST land with a regression pin demonstrated to
  fail against the pre-fix build. A pin that has only been observed to pass
  after the fix MUST NOT be accepted as evidence.
- **FR-002**: Behaviour changes MUST be confined to the defects named in the
  source document. Any behaviour change discovered to be necessary beyond them
  MUST be recorded as a deviation with its reasoning, not absorbed silently.
- **FR-003**: Persisted data formats, emitted row-identity values, and golden
  statement pins MUST remain byte-identical unless a requirement in this
  specification explicitly versions them; where a fix forces a change, it MUST
  ship with a version bump and migration note.
- **FR-004**: Where this feature replaces an implementation, the superseded
  implementation MUST be deleted in the same change. Compatibility shims,
  aliases, dual paths, and flags that keep a superseded path reachable MUST NOT
  be introduced.
- **FR-005**: No change in this feature may require `unsafe` code; a candidate
  that cannot be expressed safely MUST be rejected regardless of its measured
  gain.
- **FR-006**: Failures introduced or reclassified by this feature MUST use
  typed constructors and structured signals. Tests MUST NOT assert behaviour by
  substring-matching a rendered error message, and user-facing strings MUST NOT
  contain internal citation identifiers.
- **FR-007**: Comments this feature adds or touches MUST state the live rule or
  invariant and stand alone without reference to specification documents, task
  identifiers, or review findings. Comments the source document identifies as
  false MUST be corrected in the same increment that touches their subject.
- **FR-008**: Each increment MUST leave the full local gate green when merged,
  and MUST be independently revertible without breaking a later increment that
  has not yet landed.
- **FR-009**: The defect claims recorded as refuted in the source document's
  Appendix A MUST NOT be implemented as work. Re-raising one requires new
  evidence that defeats the recorded refutation, recorded alongside it.
- **FR-010**: An item whose verification requires the unavailable hosted-runner
  environment MUST land as a reviewed change with its unperformed verification
  recorded explicitly. It MUST NOT be reported as verified, and MUST NOT block
  its increment.
- **FR-011**: Every item enumerated in the source document MUST reach exactly
  one terminal disposition in the close-out — fixed, rejected with a recorded
  reason, or deferred with a named re-trigger. The close-out MUST contain zero
  uncited dispositions.
- **FR-012**: Measured test coverage MUST be at least 80%, established
  baseline-first.

**The record and the license**

- **FR-013**: The repository MUST ship the text of the license its manifests
  declare.
- **FR-014**: The project instruction file MUST state the true status of every
  completed feature, cite the recorded results that supersede earlier figures,
  and MUST NOT present superseded measurements as current.
- **FR-015**: The most recent feature's completion record MUST be internally
  consistent: its status MUST match the state of its task list, no statement in
  it may contradict another, and every contract clause row MUST carry a terminal
  disposition.
- **FR-016**: A requirement that shipping code deliberately does not satisfy
  MUST carry, in place, the measurement that inverted its premise and the
  condition under which it would be revisited.
- **FR-017**: A working document whose programme has been executed MUST say so
  where a reader will see it, point at the dispositions, and name the claims its
  own execution disproved.
- **FR-018**: The measured aggregate-throughput characteristic and the design
  trade behind it MUST be documented where an operator planning capacity will
  find it.
- **FR-019**: Statements in project documents that are contradicted by code,
  configuration, or artifacts MUST be corrected — including described
  configuration behaviours, artifact format versions, enforcement-bar status,
  and build-verb scope.
- **FR-020**: A configuration knob that is accepted but ignored MUST either be
  honoured, rejected, or documented as ignored; it MUST NOT be silently
  accepted while a document implies it has effect.

**Value fidelity on the ingestion path**

- **FR-021**: An integer value outside the range of the type inference would
  otherwise select MUST be delivered in a representation that holds it, or
  refused, or counted — never silently replaced with a null.
- **FR-022**: A declared type hint MUST govern its column's type for every
  observed value shape, including values that are not scalars.
- **FR-023**: A decimal value that does not fit its declared precision MUST be
  refused by the same rule that refuses values not fitting the declared scale.
- **FR-024**: Values that cannot be represented under a declared hint MUST be
  reflected in the run's accounting; if plan-time analysis rejects counting
  them, the behaviour MUST be documented on the hint's public surface and pinned
  by test.
- **FR-025**: A type hint the engine cannot honour MUST be refused with a typed
  configuration error naming the column, at pipeline construction; no hint value
  may reach a code path that panics.
- **FR-026**: The changes in this group MUST leave every emitted row-identity
  value byte-identical, verified against a corpus captured from the pre-change
  build.
- **FR-027**: Where a fix changes the type a column is inferred to have, the
  change MUST be recorded as schema-affecting in the close-out, with the
  before-and-after types named.

**Declared schema contracts**

- **FR-028**: The engine's schema-policy behaviour and its documented contract
  MUST agree. Either the enforcement extends to the boundary the contract
  claims, or the contract is narrowed to the boundary the engine enforces; a
  documented promise stronger than the enforcement MUST NOT remain.
- **FR-029**: Per-stream schema state persisted at commit MUST be consumed by
  the mechanism it exists for, or documented as diagnostic-only. It MUST NOT
  remain written and never read.
- **FR-030**: A schema policy MUST apply to a table newly created mid-run by the
  same rule it applies to a column newly added mid-run.
- **FR-031**: Drift evaluation MUST NOT report drift when a run legitimately
  observes a subset of the columns a previous run wrote.

**File-family correctness**

- **FR-032**: Resume MUST NOT proceed from a recorded position that the input's
  current content does not justify, for every input unit kind — including kinds
  that record no content hash today.
- **FR-033**: The integrity check of FR-032 MUST NOT reject an input that grew
  by legitimate appending.
- **FR-034**: A full-refresh load MUST remove everything the destination
  previously wrote for that target, regardless of the output format or
  partitioning configuration in effect when those files were written.
- **FR-035**: Identification of a destination's own stored objects MUST be
  derived exactly from the known location, not by searching for a pattern that
  data values can also match.
- **FR-036**: Where a format is read in two passes, an input that changes
  between passes MUST fail with a typed error for every inferred type, with no
  type silently coercing.
- **FR-037**: Storage failures that cannot heal MUST be classified as
  non-retryable in the single classification rulebook.
- **FR-038**: Accumulated per-input position state and per-target commit
  records MUST be bounded by a decided, documented rule, or their unbounded
  growth MUST be documented as accepted with its cost stated.
- **FR-039**: Temporary resources acquired while planning or staging MUST be
  released when those steps fail.

**Network connector robustness**

- **FR-040**: Every outbound request, including credential acquisition, MUST be
  bounded by a timeout. The configuration surface for it MUST be additive with
  a default.
- **FR-041**: Failure to construct a network client MUST surface as a typed
  configuration error, never a panic.
- **FR-042**: A pagination configuration whose page parameters cannot reach the
  request MUST be rejected at configuration validation, before any request is
  sent.
- **FR-043**: Server-supplied pacing MUST be honoured in every standard form the
  protocol allows, still bounded by the configured cap.
- **FR-044**: Concurrent credential refresh MUST NOT discard a credential that
  another request has just obtained.
- **FR-045**: Values interpolated into a request target MUST be encoded so that
  data cannot alter the target's structure.
- **FR-046**: Headers that can carry credentials MUST be guarded against being
  set from a source that would bypass the credential handling.

**Catalog destination**

- **FR-047**: Schema drift reconciliation MUST compare structure — names and
  types — and MUST NOT treat catalog-assigned nested identifiers as a
  difference.
- **FR-048**: Genuinely contradictory schema changes MUST still be refused with
  a typed error after FR-047.
- **FR-049**: Every capability a destination advertises MUST be exercised
  end-to-end against a live catalog, including nested types and read-back.
- **FR-050**: Container images used by tests MUST be pinned to a verified
  version rather than a moving tag.
- **FR-051**: Nullability drift MUST be surfaced by reconciliation rather than
  later as an unrelated alignment failure.

**Engine hardening and error taxonomy**

- **FR-052**: A value outside the range its wire form can represent MUST be
  refused with a typed error rather than transmitted as a different value.
- **FR-053**: Encoded values MUST derive their scale from the data carrying
  them.
- **FR-054**: A given environmental failure MUST be classified consistently
  across all entry points of the connector that raises it.
- **FR-055**: Diagnostic context MUST be attributed to the work that produced
  it under concurrency.
- **FR-056**: Recovery-log residue MUST NOT accumulate without bound across
  repeated runs that fail before their first checkpoint.
- **FR-057**: A failure that can only indicate an internal defect MUST be
  classified as internal and MUST map to a process exit code distinct from the
  configuration code; the documented exit-code taxonomy MUST cover the fallback
  case.
- **FR-058**: A type a destination declares it cannot accept MUST NOT reach it
  unconverted because it was nested inside a type the destination does accept;
  either the conversion recurses or the capability combination is refused.
- **FR-059**: Discarded failures on paths whose results are dropped MUST be
  visible in diagnostics.
- **FR-060**: Identifier normalisation MUST honour the bound it states, at every
  value of that bound its interface allows.

**The verification gate**

- **FR-061**: The mutation-testing record MUST be regenerated against the
  current tree, and every survivor MUST reach a terminal disposition — killed by
  a new pin, or recorded as equivalent with a reason.
- **FR-062**: Each pin named in the source document MUST exist and MUST be
  demonstrated to fail when its target invariant is broken.
- **FR-063**: A test named for an invariant MUST assert that invariant, or be
  renamed to what it does assert.
- **FR-064**: Containers and volumes created by the test suite and the benchmark
  harness MUST be identifiable by a stable label, and a single documented
  command MUST reclaim them.
- **FR-065**: The crash sweep MUST either cover the state in which a destination
  committed and the client did not learn, or record that boundary explicitly
  with the coverage that stands in for it.
- **FR-066**: Intermittent container-backed test failures MUST be recorded as
  data rather than absorbed by a re-run convention.

**Publish readiness**

- **FR-067**: Each publishable crate's declared description MUST match what the
  crate does, and the metadata fields that render a package page MUST be
  present.
- **FR-068**: The documentation build MUST complete without warnings, and the
  public items of the crates whose interfaces are frozen MUST be documented.
- **FR-069**: Each publishable crate MUST build in the feature configurations a
  consumer can select, verified per crate rather than only as part of the whole
  workspace.
- **FR-070**: The interface-compatibility gate MUST cover every crate this
  project publishes.

**Recorded deferrals**

- **FR-071**: A correctness invariant implemented in two places MUST be reduced
  to one implementation, or its agreement MUST be enforced by a test that fails
  when the two disagree.
- **FR-072**: A configuration surface mirrored by hand MUST either be derived so
  that new fields are automatically reachable, or guarded by a test that fails
  when a field becomes unreachable.
- **FR-073**: Every named deferral whose recorded trigger has fired MUST be
  taken, rejected with a reason, or re-recorded with a new trigger.
- **FR-074**: Dependencies a crate does not use MUST be removed, and documents
  describing a crate's dependencies MUST agree with its manifest.

**Performance, measurement-gated**

- **FR-075**: Every performance item in the source document's queue MUST end
  this feature with a recorded measurement outcome. An item MUST NOT be closed
  by assertion, and MUST NOT be closed by omission.
- **FR-076**: No performance change may ship without a measured improvement on
  the target it names, taken through the harness on a machine that passed the
  quiet guard.
- **FR-077**: The owed execution-plan capture for the largest server-side cost
  in the matrix MUST be taken and recorded before any change is proposed against
  that path.
- **FR-078**: The blocked-time attribution the previous analysis named as its
  own top gap MUST be taken, or its unavailability recorded with the reason,
  before further serial-path optimisation is proposed.
- **FR-079**: A performance change that ships MUST leave its byte-identity
  oracles and golden pins unchanged, and MUST record its before/after
  measurement.
- **FR-080**: A candidate that measures no better MUST be recorded as a negative
  result with its numbers, so it is not attempted again.
- **FR-081**: An incremental pipeline MUST NOT re-transfer remote inputs it has
  already fully consumed and whose identity is unchanged.
- **FR-082**: All enforcement bars MUST still pass at the end of this feature.

### Key Entities

- **Audit item**: one enumerated entry in the source document, carrying an
  impact, an effort, an anchor, and — for defect claims — a verification
  verdict. The unit this feature disposes of.
- **Verification verdict**: the recorded outcome of the adversarial pass over a
  defect claim; a refutation is as binding as a confirmation.
- **Regression pin**: a test captured against the pre-fix build, whose failure
  there is the evidence that it tests the defect.
- **Terminal disposition**: the single end state of an audit item — fixed,
  rejected with a reason, or deferred with a named re-trigger.
- **Recorded deferral**: an item postponed with a named condition under which it
  returns. Its trigger firing without a disposition is itself a defect in the
  record.
- **Schema policy contract**: the promise made to a user about what schema
  change is allowed, and the boundary within which that promise holds.
- **Resume position**: a recorded point in an input plus the integrity evidence
  that the input still justifies resuming there.
- **Destination ownership**: the set of stored objects a destination may remove
  on a full refresh; must not depend on configuration that has since changed.
- **Mutation record**: the survivor list from mutation testing, valid only
  against the tree that produced it.
- **Container label**: the stable marker that makes test residue identifiable
  and reclaimable.
- **Measurement outcome**: a number plus a decision; the only admissible way to
  close a performance item.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All 29 verification-confirmed defects reach a terminal
  disposition, and every one that was fixed has a pin that fails on the pre-fix
  build.
- **SC-002**: Zero of the 18 refuted claims appear as implemented work; each
  appears exactly once, as a recorded non-goal.
- **SC-003**: A reader can pick any status, measurement, format, or behaviour
  claim in the project's own documents and find it consistent with the
  artifacts — with no known exceptions outstanding at close-out.
- **SC-004**: The repository satisfies the distribution terms of the license it
  declares.
- **SC-005**: No ingestion path silently substitutes a null or a different value
  for a value it cannot represent: every such value is delivered, refused, or
  counted, demonstrated over a corpus that exercises each case.
- **SC-006**: A destination reconfigured across output format and partitioning
  contains exactly one load's rows after a full refresh, and a rewritten input
  is never resumed into on stale evidence.
- **SC-007**: A stalled server produces a typed failure within a bounded time
  in every network path, including credential acquisition — no configuration
  can produce an unbounded wait.
- **SC-008**: The engine's schema-policy behaviour and its documented contract
  agree, and no persisted field remains written and never read.
- **SC-009**: Every capability the catalog destination advertises is exercised
  end-to-end against a live catalog, and an unchanged nested-type stream can be
  loaded twice without reporting drift.
- **SC-010**: The mutation record is regenerated against the current tree and
  every survivor has a terminal disposition; each pin named in the audit is
  demonstrated to kill its target.
- **SC-011**: A single documented command reclaims every container and volume
  the test suite and benchmark harness create, verified after a deliberately
  aborted run.
- **SC-012**: Every publishable crate passes a packaging verification with
  accurate, complete metadata, builds in each consumer-selectable feature
  configuration, and produces a warning-free documentation build.
- **SC-013**: No correctness invariant remains hand-maintained in two places
  without either one implementation or a test that fails when they disagree.
- **SC-014**: Every named deferral whose trigger has fired carries a
  disposition; the count of fired-but-undisposed deferrals is zero.
- **SC-015**: Every performance item in the queue carries a recorded number and
  a decision; the count closed by assertion or omission is zero.
- **SC-016**: The benchmark matrix has no cell worse than its standing of
  record, and all enforcement bars pass.
- **SC-017**: The close-out disposes every item in the source document with zero
  uncited dispositions, and measured coverage is at least 80%.
- **SC-018**: Every increment merged with the local gate green, each one
  independently revertible.

## Non-Goals

Recorded so they are not re-litigated.

**Continuous integration repair (E1).** The cause is an organisation billing
state the owner cannot resolve during this feature. Restoring the runner, and
diagnosing whatever residual workflow breakage surfaces once jobs actually
execute, is later work. This feature neither fixes it nor pretends the local
gate is CI.

**Publishing (E2).** Readiness is in scope; the publish is not.

**The 18 refuted defect claims** (source document Appendix A). Each was checked
against the code and refuted with recorded reasoning. Named here so the
refutation is the answer rather than a second investigation:

- Error exits bypassing a cleanup tail — every error on those paths is
  non-retryable, so the claimed overlap is unreachable.
- Stream failure observed only after the drain completes — committed progress is
  preserved and resume is correct; it is a failure-latency design choice that
  wants a stated rationale, not a fix.
- A missing directory synchronisation in the recovery log — the log is
  explicitly not the source of truth.
- Nullability hardcoded in one lowering arm — real drift, addressed as a parity
  invariant rather than as a defect; unreachable for every shipped destination.
- Trailing discard counters never committed (raised twice) — nothing in the
  workspace consumes those counters.
- Column-projection discard semantics differing between two paths — the
  contracted design.
- A defensive clamp that should be a typed error — dead code; every route
  refuses the input earlier.
- A zero-row full-refresh divergence between two publish paths — unreachable in
  both.
- Pagination stop-condition ordering — reordering changes behaviour on no input.
- Credentials followed to a cross-origin next-page URL — the party that controls
  that URL already holds the credential.
- A documentation line contradicting relative-URL resolution — the behaviour is
  documented and pinned elsewhere; the one stale line is a documentation fix.
- A catalog property escape hatch overriding credentials — documented, recorded
  behaviour.
- Vended-credential expiry misclassified as fatal — traced: the fatal path is
  reached only by catalog authentication, and storage expiry stays transient.
- Staged part-name collisions — require a character that identifier
  normalisation cannot produce.
- Report read failures swallowed by the benchmark runner — an upstream failure
  exits non-zero and is rejected before the read.
- A manifest open error treated as absence — the same error fails typed moments
  later in the same run.
- A swallowed timestamp error disarming a tripwire — designed, pinned behaviour;
  the error cannot occur on the supported platform.

**Optimisations the previous feature's evidence already killed.** The fourteen
hypotheses recorded as negative results in `PERF_ANALYSIS.md` §5, plus the two
allocation removals that measured worse in execution, plus intra-pipeline
parallelism for the merge path — measured to be bounded below the target it
would need to clear.

**New connectors, new destinations, and new pipeline capability.** The engine
stays small; breadth belongs above it. Nothing in this feature adds a connector.

**Re-opening settled contract decisions.** Row identity remains frozen. The
statement shapes guarded by golden pins remain unless a measured win requires a
change and re-pins it visibly.

## Assumptions

- `NEXT_STEPS.md` and its Appendix A are the evidence base, in the role
  `PERF_ANALYSIS.md` held for feature 019. Item-level anchors are not restated
  here; where the audit and this specification differ, the audit's facts win and
  this specification is amended.
- The two candidate resolutions for the schema-policy contract — extending
  enforcement across run boundaries by seeding drift detection from persisted
  state, or narrowing the documented contract to the boundary the engine
  enforces — are both admissible, and the choice is left to planning. The audit
  records why the naive form of the first is wrong: a run whose first batch
  legitimately observes a subset of columns would be reported as drift, so
  extending enforcement requires real design rather than a comparison.
  FR-028 and FR-031 constrain the outcome, not the shape.
- The preferred resolution for values that cannot be represented under a
  declared hint is to count them, because the crate's stated discipline is
  "counted, never silent". Documenting the behaviour instead is the recorded
  fallback if plan-time analysis shows counting cannot be threaded out of the
  build path cleanly.
- The local gate (lint, tests, sweep, instruments) is the gate of record for
  this feature, as it was for 019, with cold-start measurement on its own verb.
  Two consecutive clean runs remain the standard for claiming green, because
  container-backed legs are known to flake.
- Container-backed tests continue to skip rather than fail when the runtime is
  absent, and images are verified at feature start.
- The performance targets in this feature are not percentages. Its performance
  requirement is that every queued question has a recorded answer; a queue that
  ends with several recorded negatives is a successful outcome, not a failed one.
- Measurement capacity is scarce and operator-gated: sessions need a quiet
  machine, and one item needs a kernel setting that may not be writable. Where a
  measurement cannot be taken, recording why is the disposition.
- The second recorded benchmark session that the bars file queues — tightening
  two bars and deciding the deliberately unbarred cell — is operator work. It is
  in scope if the owner runs it and recorded as deferred with its trigger if
  not.
- A synthetic destination in the test kit is acceptable evidence for capability
  combinations no shipped connector declares, provided the evidence is recorded
  as synthetic and the shipped connectors' declarations are unchanged.
- The regenerated mutation record may produce more survivors than this feature
  can triage. If so, triage is prioritised by the subsystems this feature
  changed, and the remainder is recorded as a named deferral with its trigger
  rather than left as an untriaged list.
- No new runtime dependency is assumed. Any that planning finds necessary must
  be justified against the small-core principle with registry facts — versions
  that resolve against the workspace pins, the feature path that reaches it, and
  what it costs the dependency tree.
