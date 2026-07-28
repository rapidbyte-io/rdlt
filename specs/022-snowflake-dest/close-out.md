# Close-out: Snowflake Destination Connector (022)

**Status**: US1–US4 DELIVERED; US5 measurement in progress. Every claim here cites the evidence that produced
it; no disposition is silent. Verifications this machine cannot perform are
recorded UNPERFORMED with the reason, never as green.

## Contract matrix (SD1–SD8)

| clause | status | evidence |
|---|---|---|
| SD1 — one library, one boundary | **MET** | every library type stops in `dest/client.rs`; what crosses out is an `Executor`, SPI errors and strings. Replacing the library is a change to one file. |
| SD2 — full unattended auth vocabulary, every secret guarded | ON TRACK | T001 A3 (PAT rides the password channel, oauth rejects it), A4 (invalid secret → `kind = Auth`, secret provably absent from the rendered error) |
| SD3 — the atomic unit is pure DML, DDL strictly outside | ON TRACK | T001 A2: `CURRENT_SESSION()` identical across `query()` calls; BEGIN/INSERT/ROLLBACK across three separate calls yields count 0; the same sequence with a CREATE in the middle yields count 1 — DDL auto-commit proven a second time, on a second transport |
| SD4 — merge without enforced constraints | **MET** | `SnowflakeDialect` on the shared seam: `MERGE INTO` for upsert (no `ON CONFLICT` exists), `QUALIFY ROW_NUMBER()` for survivor selection (no `DISTINCT ON`). All five strategies proven live; key uniqueness ASSERTED after each merge because nothing in the database enforces it. |
| SD5 — identifier policy is total | ON TRACK | T001 A2 false alarm: the qual database has no `PUBLIC` schema, so session-context reliance failed and fully-qualified three-part names worked — D2's decision validated by accident |
| SD6 — ingestion verified on every shipped path | ON TRACK | T001 A5: parquet written by the workspace's OWN arrow writer (lowercase columns, embedded NULL) loaded into a quoted-upper table via `MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE`, 3/3 rows, `rows_loaded` present for SD6's verification |
| SD7 — crash discipline + statement economy | **MET** | three crash points × three actions × two write modes × both ingestion paths, each crashed twice and required to converge duplicate-free, with an armed-fire pin that fails if a crash site goes dead. Economy: unchanged schema → zero statements; one added column → exactly one `ALTER`; cost independent of column count (200-column table costs what a 3-column one does); 13 statements per load measured server-side. |
| SD8 — house verification standard | **MET** | shared conformance suite passes live; differential oracle vs postgres on whole rows; option-validation parity in the shared core's own words; secret hygiene across Debug/errors/constructors; skips announced rather than silent. |

## Story matrix

| story | status | evidence |
|---|---|---|
| US1 — exactly-once loads, one document | **DELIVERED** | Append/Replace land exact totals live; a replay publishes nothing; awkward values survive both ingestion paths; crash sweep converges. `destination: snowflake:` carries the whole vocabulary from YAML. |
| US2 — full merge parity | **DELIVERED** | five strategies live; differential oracle agrees with postgres row-for-row across four strategy × hard-delete combinations; refusals carry the shared core's wording, and acceptances match too. |
| US3 — frugal with round trips | **DELIVERED** | economy pinned unit-side and measured server-side; one optimization built, measured and DECLINED with its numbers (D-22). |
| US4 — verified like the other connectors | **DELIVERED** | conformance live; auth matrix wired for all four methods (two proven live, two UNPERFORMED by owner decision); a real secret leak found and fixed (D-21). |
| US5 — recorded performance standing | **DELIVERED** | the knee measured (D-23), and the constant it produced changed UNIT as a result (D-28): bytes, not rows. |

## Task ledger

| task | disposition | note |
|---|---|---|
| T001 environment gate | **DONE** | six probes; two plan corrections (A1 reqwest cost, A7 fakesnow); research.md addenda A1–A8 |
| T002 close-out skeleton | **DONE** | this file |
| T003 ensure extraction | **DONE (narrowed)** | D12 + D13 — `sqlcore::ensure` plans decisions, both destinations lower them; 18 ensure pins written first and green after; no DdlDialect trait; golden suites byte-identical; 816/816 |
| T004 session extraction | **DONE (narrowed)** | D12 + D14 — `protocol::unit` carries six pure items, `ReplayDisposition` foremost; both destinations wired; behaviour unchanged, 821/821 |
| T005–T016 | **DONE** | crate, config, boundary, ddl, session, both ingestion paths, Destination impl, pipeline-spec wiring, live smoke |
| T017 crash sweep | **DONE** | full matrix, live |
| T019–T024 | **DONE** | dialect, option parity, duplicate-key advice, live strategy matrix, differential oracle |
| T026–T028 | **DONE** | economy instrumentation, pins, live server-side count |
| T030–T033 | **DONE** | conformance + auth matrix; gating posture; fakesnow already dispositioned (C-02); secret hygiene |
| T035 batch knee | **DONE (instrument corrected)** | see D-23 |
| T038–T040 | **DONE** | README, quickstart (corrected and compiled), parity matrix |
| T018 / T025 / T029 / T034 / T037 story gates | **DONE** | full local gate green at each increment; every commit ran it |

## Deviations and corrections

### C-01 (T001) — the recorded reqwest cost was wrong, and smaller

Research D1 stated the adopted crate brings "a second reqwest major
wherever the `iceberg` feature is off". The lock says the workspace gains
**no reqwest at all**: 0.13.4 was already present via
opendal ← iceberg-storage-opendal ← the iceberg destination. Total lock
impact is +18 crates, all the RustCrypto stack the encrypted-PKCS#8 key
path needs. The narrower true statement — a `snowflake`-on / `iceberg`-off
build gets the 0.13 line from snowflake — replaces it in D1 and in the
plan's dependency line.

### C-02 (T001) — fakesnow rejected on the envelope, not the semantics

The plan carried fakesnow as a possible hermetic leg pending a fidelity
probe. Probed: fakesnow hardcodes `queryResultFormat: "arrow"` on every
success response, and the adopted crate is JSON-only **by design** — it
rejects other formats and ships a unit test asserting exactly
`"unsupported result format: arrow"`. Neither side is configurable from
here. Its SQL semantics were fine (DDL, DML, BEGIN/ROLLBACK, and
`MERGE … QUALIFY` all executed), so the rejection is narrow and the
re-trigger is precise: fakesnow honouring a JSON result format, or the
crate gaining arrow support. No hermetic protocol leg is adopted; the mock
executor seam covers protocol-shaped tests.

### C-03 (T003/T004) — both extractions narrowed, with the reason and the trigger

The tasks committed to TAKING both fired sqlcore triggers. A four-way
structural survey (`extraction-plan.md`) found that a shared async execute
skeleton is not writable: the duckdb session commits inside a synchronous
closure holding a `MutexGuard<Connection>` while postgres is async over an
owned client, and reconciling them needs a dependency the shared core's
contract forbids or a redesign of duckdb's concurrency. The narrowing was
pre-authorized by the task text ("extract only the shared shapes and
re-record the remainder with a named trigger — never a silent partial") and
is exercised here rather than forced. What IS extracted is chosen for value,
not volume: `ReplayDisposition` alone converts an inverted, comment-only
invariant into a typed decision the third destination inherits by
construction. Full reasoning and the ordered edit plan: D12 +
`extraction-plan.md`. Trigger for the remainder is recorded in D12.

### C-04 (T003/T004 survey) — a Snowflake exactly-once trap caught before it was written

`TRUNCATE TABLE` is DDL on Snowflake and auto-commits the open transaction —
the exact hazard the pure-DML unit exists to prevent — and the Replace clear
runs inside the unit. `SnowflakeDialect::clear_table` must therefore emit
`DELETE FROM`, as the duckdb dialect already does. Found by the extraction
survey, not by a failing test, and recorded before the implementation could
inherit the bug.

### C-05 (T003 step 1) — the podman shim fix un-skipped a test that had been green by absence

Repairing the host-exec shim (the session bridge had assumed distrobox
inside a toolbox) made the iceberg Polaris fixture reachable for the first
time in this session — and one nested-types cell immediately FAILED with
`unknown resume cursor Cursor(Number(1))`.

**Not a regression, and proven so**: the identical failure reproduces on the
pre-change tree in a scratch worktree, so the postgres refactor is innocent.
The cell built a fresh source per attempt carrying only the NEWEST batch, so
the second load could not resolve the cursor its own first load had
committed. The harness refuses that rather than silently restarting from
zero — correctly, per the recorded rule that test sources must honour resume
cursors.

**The file's own doc comment warns about exactly this hazard** ("a
container-gated test skips when no runtime is present, and a skipping test
is green") and then fell into it: the defect was invisible for as long as
the fixture could not start. Fixed by giving the stream every batch it has
ever produced, which is what a real source has and what makes the cursor
resolvable. Iceberg suite now 56/56 with **0 skipped**; workspace 800/800.

Recorded here rather than in 016's close-out because it was found by this
feature's environment work; the fix is a test correction, not a behaviour
change.

### D13 (T003) — how the extraction was made safe, and what it actually shared

No test pinned ensure DDL text before this feature, so the extraction could
not have been verified after the fact. The order was therefore forced and is
worth recording as method: **hoist rendering out of execution WITHIN each
destination first, pin the rendered statement vectors container-free, and
only then move the decision logic across a crate boundary.** Red-before-green
applied to a refactor.

The 18 pins were each demonstrated able to FAIL before being trusted —
`CACHE 32` → `64`, reordering a widen before its `ADD COLUMN`, removing
duckdb's legacy index drop, and adding `NOT NULL` to duckdb's validity column
(i.e. "harmonizing" it with postgres, which DuckDB rejects outright). Two of
them caught defects in themselves while being written: a wrong assumption
that the DEFAULT merge strategy is upsert (it is delete-insert, whose index
is supporting, not arbiter), and a filter matching the scd2 INDEX because it
names a validity column.

**What the extraction actually shared**, after the survey ruled out sharing
SQL: leg selection, the within-session widen predicate, scd2 validity
ordering, index planning, and `TableFacts::of` — both destinations derived
`has_identity` and `is_child` with identical code.

**The `stages` rule is the item that justifies the task.** Ensure and commit
must agree on whether a table round-trips through a stage: a table the commit
plan publishes from a stage but ensure never created one for fails at write
time, and the reverse builds a stage that is written, truncated and never
read. That rule was derived independently at each site and free to drift; it
is now one function consulted twice.

One real defect was introduced and caught en route: the new duckdb module
landed between `#[cfg(feature = "failpoints")]` and `FAIL_POINTS`, silently
moving the guard onto the wrong item. Reattached, and named here because a
mis-scoped cfg is invisible in a green build.

### D14 (T004) — the invariant that was only ever prose

The extraction shipped as six pure items in `sqlcore::protocol::unit`, and
one of them is the reason the task was worth doing at all.

**`ReplayDisposition`.** What a redelivered unit owes is INVERTED between
publish paths: a direct-to-target destination must roll back (its
redelivered rows are already in the target, inside the open transaction, so
committing lands them twice), while a staged destination must run the
planner's truncate program and commit (its redelivered rows sit in stages
that reached no reader). Before this change that rule existed ONLY as two
long comments in two executors, stating opposite things, with nothing
binding them and nothing a third destination could inherit. It is now one
function with the reasoning attached to each variant, and both executors
consult it instead of restating it.

Also shared: the receipt-existence and load-committed probes (identical
apart from placeholder dialect, which is now a closure), the
staged-emptiness probe (identical text everywhere), `roots_of`, and
`load_mismatch`.

**One item was written and then deliberately rebuilt.** `roots_of` first
walked the parent links itself — and sqlcore already had `plan::root_of`
doing exactly that walk. Two implementations of one traversal, free to
disagree about depth bounds and cycle handling, is precisely the drift this
module exists to prevent, so it was rewritten to call the existing function.
Worth recording because the mistake is easy to make while extracting: the
duplication you are removing can be re-created by the removal.

**`load_mismatch` is landed but NOT wired to duckdb**, whose `open`
discards the load id it would need. Adopting it there is a behaviour
ADDITION, not a refactor, and gets its own decision rather than riding in on
this one.

**No shared execute skeleton exists, by design** (D12): the transaction
driving, placeholder binding, error mappers, crash points and failpoint
registries stay in each destination, because they are structure rather than
logic and the two structures cannot be unified without a redesign.

**Housekeeping taken while in the file**: `protocol.rs` + `protocol/` was
the only split-file module in the workspace — every other multi-file module
uses `mod.rs`, and the new submodule had created the inconsistency. Moved to
`protocol/mod.rs`.

### D-20 (T019) — three seams that were only ever one dialect

Snowflake is the first destination that cannot ride the shared trait's
defaults, and it found three places where "shared" meant "Postgres, spelled
neutrally". Each is recorded because each would have been a silent trap for
the NEXT destination too:

**The upsert action arrived as the literal text `DO UPDATE SET …`.** A
dialect without `ON CONFLICT` could only consume that by taking the string
apart — pattern-matching one dialect's SQL to produce another's, with nothing
to catch the day the wording changed. Now `UpsertAction`, and the incoming
row's name comes from the dialect rather than being assumed to be `EXCLUDED`.

**`column_list` baked in the double-quote rule.** A destination folding
identifiers to upper case gets a column list disagreeing with every other
statement it emits, surfacing as a missing-column error far from the cause.

**The hard-delete predicate spelled a boolean flag `IS TRUE`.** It reads as
standard SQL and compiles NOWHERE on Snowflake — and the service reports the
syntax error at the enclosing subquery, pointing away from its cause. Found
by a live merge failing, bisected against the account across six candidate
shapes, and now a dialect method whose Snowflake form is NULL-safe.

pg/duckdb golden SQL stayed byte-identical throughout all three.

### D-21 (T033) — an inline private key rendered in full through Debug

The secret-hygiene matrix found a real leak on its first run. `PemSource`
derived `Debug`, so a private key supplied INLINE rather than as a path
printed verbatim, while every secret beside it in the same struct rendered as
`***`. `Debug` is what a panic, a log line and an error report all reach for.

Fixed in the SPI, where the type lives: a path renders as itself (it is not a
credential, and hiding it makes "cannot read the key" unactionable), inline
material as a placeholder. Pinned three ways including nested inside a derived
`Debug`, which is how the guarantee would quietly be lost.

**One expectation of the test was wrong and was corrected rather than
enforced**: serialization deliberately keeps the values, because serde IS the
document. A config that redacted on the way out could be parsed once and never
written back, and the placeholder would then load as the credential. That is
precisely why `Debug` — not serde — is the guarded channel.

### D-22 (T028) — an optimization measured, and declined at one statement

`open` issues three idempotent `IF NOT EXISTS` statements per load. Replacing
them with a single catalog read plus conditional creates was BUILT and
MEASURED: 13 → 12 statements on a repeat load. One statement, against a cost
that grows with neither the data nor the table count.

Declined. The standing rule is that a counting-argument optimization is guilty
until measured; this one was measured and stayed guilty. The numbers are
recorded at the measurement site in place of the change.

### D-23 (T035) — the first knee instrument measured the wrong thing

The batch-knee sweep was first written to vary the batch size the ENGINE
delivers, with a checkpoint on every batch. Throughput rose monotonically
from 25 to 628 rows/s across 100 → 10,000 rows per batch and located no knee
at all — because what it actually measured was **commit frequency**, which on
a SaaS destination dominates everything else. Each batch carried its own
commit unit of roughly seven statements.

That is a real and useful number (fewer commits win, monotonically, on this
service) but it is not the rows-per-statement knee, and reporting it as one
would have set a shipped constant from a measurement of something else.

The instrument was rebuilt to time exactly what the constant controls: the
same rows, split into statements different ways, executed against a real
table with no engine and no commit in the loop. A measurement seam
(`insert_statements_chunked`) makes the chunk size varyable by the instrument
WITHOUT becoming configuration — shipping a knob in order to tune it would
leave every user holding a decision that belongs to whoever measured it.

### D-24 (T030) — two auth legs UNPERFORMED by owner decision

Password and OAuth are implemented, unit-tested, and their live legs are
WRITTEN — they skip with a printed reason on an account that has not
provisioned them. The owner decided not to provision a password test user or
an OAuth security integration for this feature.

Recorded rather than left absent, because a leg that was never written and a
leg that could not run look identical from a green suite. Key-pair and PAT
are proven live, and the bad-credential SHAPE is asserted for every method
including the two whose happy paths are unperformed.

### D-25 (T031) — a green suite that could not be told from a skipped one

With credentials absent the crate's suite reported 104/104 passing in two
seconds, and nothing distinguished that from a run where every live leg
executed. The gate's own doc comment names this hazard; the gate itself had
it.

Skips are now announced once per test binary with the reason. The count still
reads as passes — that is how the harness works — but a reader can now tell
"green because it ran" from "green because it did not".

### D-26 (environment) — the toolbox lost its C toolchain twice mid-feature

The development container was recreated twice during this feature, each time
losing gcc/make/cmake and failing every build script with `linker cc not
found`. Reinstalled both times. Recorded because two long live measurements
were killed by it and their partial results discarded rather than reported.

Separately, podman exhausted its 2048-lock table with 1,799 orphaned
anonymous test volumes; only the 64-hex anonymous ones were removed, leaving
the named project volumes untouched, since a blanket prune would have
destroyed unrelated data.

### D-27 (T041) — semver: the two crates that matter are clean; sqlcore is not, and it costs nothing

`cargo semver-checks --baseline-rev main`:

- **rdlt-core**: no semver update required.
- **rdlt-connector**: no semver update required — the new `objects` module is
  additive behind a feature, and `PemSource`'s hand-written `Debug` replaces a
  derived one without changing the API surface.
- **rdlt-connector-sqlcore**: **requires a major.**
  `MergeDialect::upsert_stmt` went from six positional parameters to one
  struct, which is a breaking change for any external implementor of the
  trait.

The requirement was that this feature must not force the 0.2→0.3 window. It
does not, and the reason is worth stating plainly rather than leaving as an
inference: **nothing in this workspace has been published.** All four crates
are absent from crates.io — the first semver-checks invocation failed for
exactly that reason before being re-run against a git baseline. There is no
released 0.2 for anyone to depend on, so there is no compatibility to
preserve and no window to open.

The alternative — keeping the old six-parameter method alongside the new one
— was rejected on the standing greenfield rule: two ways to spell one
decision is how a seam drifts, and it is precisely the shape this feature
spent three deviations removing (D-20).

Recorded so that whoever runs the publish feature knows the sqlcore seam
changed shape in this window and does not read "no update required" for two
crates as covering all four.

### D-28 (T035) — the measurement changed the constant's UNIT, not just its value

The corrected instrument, 20,000 rows × 12 columns against the qual account,
timing the same rows packed into statements different ways:

```text
  100 rows/stmt → 200 statements,  20 KB each,  101 rows/s
  250 rows/stmt →  80 statements,  50 KB each,  215 rows/s
  500 rows/stmt →  40 statements, 101 KB each,  405 rows/s
 1000 rows/stmt →  20 statements, 203 KB each,  426 rows/s
 2000 rows/stmt →  10 statements, 405 KB each,  659 rows/s
 5000 rows/stmt →   4 statements, 1.0 MB each,  612 rows/s
10000 rows/stmt →   2 statements, 2.0 MB each,  801 rows/s
20000 rows/stmt →   1 statement,  4.0 MB,      1064 rows/s
```

Throughput rises 6.5× from 100 to 2,000 rows and then goes flat and noisy —
5,000 measured SLOWER than 2,000. The knee is around **400 KB**.

The finding that mattered was not the number but the UNIT. A row count is the
obvious constant and the wrong one: this shape reaches 400 KB at 2,000 rows,
while a hundred-column table reaches it at a few hundred — so a row count
tuned here becomes a multi-megabyte statement there. The shipped constant is
now a BYTE budget, and rows are packed until it is reached, so no table shape
needs its own tuning.

Snowflake documents a 1 MB limit for a single statement. The sweep's 4 MB
statement was nonetheless accepted through the driver, which means the
effective cap is higher than the documented one — a reason to stay under the
documented one rather than to rely on the observed one. A row-count ceiling
remains as a backstop for pathologically narrow rows, where the byte budget
alone would put millions in one statement and make a single failure expensive
to retry.

A single row over budget still ships rather than being refused: splitting one
row is impossible, and refusing it here would turn a service limit into a
client-side dead end with no way forward.

### D-29 (T013, found in review) — the scope wipe could delete a live load's parts

`clear_scope` originally deleted EVERY object under the pipeline prefix when
a session opened. The justification recorded at the time was that the engine
does not run two loads of one pipeline concurrently — which is an assumption
about the CALLER, not something this crate enforces or can check.

Where it breaks: two sessions of one pipeline overlapping at all — a retry
starting while the previous attempt is still staging, or two runs launched
close together. The second session's open deletes the first's live parts, and
the first's `COPY INTO … FILES = (…)` then fails on a file that existed when
the statement was built. The failure surfaces on the INNOCENT session, as a
missing-file error naming a key nobody can explain.

Fixed by making part keys load-scoped and reclamation conditional: a session
removes its OWN leftovers unconditionally, and anything else only once it is
older than an hour. Orphans are unreachable either way — no receipt names them
— so collecting them is housekeeping, and doing housekeeping eagerly is what
turned it into a bug.

Recorded because the original reasoning was written down and looked sound:
"the engine does not allow this" is a claim about a component that was free to
change, defended by nothing.

## Unperformed verifications

| what | reason |
|---|---|
| hermetic protocol leg | no fidelity-compatible emulator exists today (C-02) |
| PrivateLink-specific host-override behaviour | no PrivateLink environment; the SEAM itself is proven (T001 A6 — a real login completed through `custom_base_url`) |
| CI-only checks | the recorded external blocker stands; never claimed green |
| password auth, live | no password user provisioned on the qual account; owner decision (D-24). Implemented, unit-tested, live leg written and skipping with reason. |
| OAuth auth, live | no security integration on the qual account; owner decision (D-24). Same posture as password. |
| internal-stage `PUT` | unreachable through the SQL API and the adopted driver; deferred with a named upstream trigger (parity.md) |
| GCS / Azure external stages | no such bucket available; the Snowflake side is storage-agnostic, the client writer and config are S3-shaped |
| duplicate-merge-key diagnosis, live provocation | the survivor subquery makes a duplicate source key unreachable through the normal path — the mapping is unit-tested and the CODE is confirmed live by a raw provocation |

## Gate of record

The crash sweep is the evidence US1's exactly-once claim rests on, so it is
recorded with what it actually covered rather than as a checkmark.

| | |
|---|---|
| crash sweep | **2/2 PASS**, 4,308 s — every registered point × 3 actions × Append/Replace × both ingestion paths; each cell crashes, crashes AGAIN during recovery, then converges to exact totals |
| armed-fire pin | every crash site fired on the path that can reach it; a site gone dead fails here rather than passing silently |
| workspace | **966/966**, 2 skipped — both skips are `#[ignore]`d measurement instruments (`batch_knee`, `ingestion_session`), not gated cells that quietly stopped running |
| perf gate | 6 benches, 0 regressed, all within tolerance |
| cold start | 26.3 ms median (bar <= 40 ms) |
| lint / docs | clean |

Two earlier sweep runs were DISCARDED rather than recorded, and why matters:
both had a second sweep running concurrently against the same account, which
is the very collision D-29 describes — two sessions of one pipeline sharing a
part key and a wipe. The first of those runs is what surfaced D-29 at all; its
failure was real evidence of a real defect, but it was not a valid gate
result, and the run that IS recorded here was uninterrupted with exactly one
instance.

### The toolchain the gate ran on

The recorded gate runs on **1.96.0**, the version `rust-toolchain.toml` pins
and the version the perf baselines were recorded with.

Getting there took finding that `RUSTUP_TOOLCHAIN=1.97.1` was set in the
environment, which silently overrides the file — every build and test earlier
in this feature ran on 1.97.1 without the pin applying, and 1.96.0 was not
installed at all. The perf gate is what caught it: the benchmarks themselves
passed with zero regressions, and `compare-iai.sh` still refused, because
instruction counts recorded under one compiler say nothing about a build from
another.

The baselines were NOT re-recorded, though the gate offers that as the way
through. Re-recording would move the project's perf reference onto an
unpinned toolchain — a decision, not a cleanup step. The pinned toolchain was
installed and the override dropped for the run instead.

One near-miss worth recording: an interrupted run reported exit code 0 because
the command had been piped to `tail`, which exits 0 whatever happens upstream.
It was caught by reading the output for a summary line that was not there. A
gate that can report success without producing a result is not a gate, so the
recorded run is unpiped and its exit code is the runner's own.

## Recorded ingestion session (T036, re-scoped — see D-30)

Qual account, eu-central-1; stage bucket eu-west-2 (cross-region); bench-shaped
rows of twelve mixed-width columns; one load per cell into a fresh scratch
schema. UNBARRED — a SaaS cell carries network variance and cannot gate a
build under the benchmark governance.

### Where the two paths cross — they do not

| rows | INSERT s | COPY s | faster |
|---|---|---|---|
| 100 | 11.84 | 7.59 | COPY |
| 250 | 13.54 | 13.24 | COPY |
| 500 | 14.91 | 8.42 | COPY |
| 1,000 | 7.38 | 7.83 | INSERT |
| 2,500 | 8.49 | 10.74 | INSERT |
| 5,000 | 15.30 | 8.41 | COPY |
| 10,000 | 21.83 | 7.40 | COPY |

**There is no crossover to encode.** Below roughly 2,500 rows every cell costs
7–15 s whatever the path and whatever the size — that is per-load fixed cost
(connect, ensure, unit, commit), and the winner alternates. The separation only
becomes real at 5,000 rows and above.

This was measured expecting the opposite. The hypothesis was that one PUT plus
one COPY must cost more than a single INSERT for small loads, so a row
threshold would be needed. The data says the fixed cost of a LOAD dwarfs the
difference between the two mechanisms at that size, and a threshold would be a
constant fitted to noise. **The shipped rule — a configured bucket means the
bulk path, always — stands, now because it was measured rather than because it
was simple.**

### Scaling

| rows | path | wall | rows/s |
|---|---|---|---|
| 250,000 | INSERT | 429.26 s | 582 |
| 250,000 | COPY | 114.10 s | 2,191 |
| 1,000,000 | COPY | 515.15 s | 1,941 |

INSERT holds its rate against the batch-knee sweep's 628 rows/s at 20k — no
degradation with scale, which is what the byte-budget packing had to deliver.

COPY at 1M runs 11% slower per row than at 250k (4× the rows, 4.51× the wall).
That is NOT claimed as linear and NOT claimed as degradation: one run of each,
on a cross-region SaaS cell, cannot separate a mild multi-part effect from
variance. It is recorded as an open question with the numbers that raise it.

COPY is 3.3–3.8× faster than INSERT at scale. Both paths landed exact totals.

### D-30 — T036 re-scoped, and why

As written the task called for 1M rows through BOTH paths. The INSERT leg was
dropped: at a measured 582 rows/s it is ~28 minutes of warehouse time to
confirm what 250k already establishes, and the batch-knee sweep plus the 250k
run already show the rate holds. Warehouse time was spent instead on the small
end, where the shipped default actually lives — and that is where the
measurement overturned the expectation rather than confirming it.

The whole session cost 1,236 s of warehouse time.
