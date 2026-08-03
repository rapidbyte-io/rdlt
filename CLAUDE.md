<!-- SPECKIT START -->
The root driver documents those earlier features executed — REFACTORING.md,
PERF_ANALYSIS.md, NEXT_STEPS.md, BENCH_REFINMENT.md — were DELETED once
executed; the features' own specs/ directories carry what they concluded.
Read them from git history if a reference below sends you looking.

NOTE: root REFACTORING.md is a LIVE driver document (house-style refactor,
14 crates, waves 1-9 + a Wave 0) — NOT yet executed. Its Part 3 carries the
order, the six recorded owner decisions, and the two-feature split: gate
integrity first (this feature), house style second.

For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/032-oracle/plan.md` (feature: ORACLE SOURCE — a NEW connector,
not a second generation, on branch `032-oracle` off main @ 662ccaf4.
Built greenfield on the connector sdk, then REBUILT ONCE MID-FEATURE
on the owner's call after review measured the driver, not the
connector, as the problem. THE DRIVER DECISION IS THE FEATURE'S SPINE
(plan.md T001/T003/T005): it started on `oracle-rs` (pure Rust, no
Instant Client) which was VENDORED and patched four times, and review
round 3 then showed most SEVERE defects were the driver's own —
NVARCHAR2 silently destroyed, BINARY_DOUBLE mojibake, signed scale
lost, a ~297-page cursor ceiling (measured twice: it never closes a
server cursor) and an O(n^2) rescan, with 200k extrapolated at 3-7
MINUTES. The alternative was PROBED LIVE and every one of those
defects was simply absent, so D1 was reversed: the connector now
rides `oracle` 0.6.3 (kubo/rust-oracle, ODPI-C, SYNCHRONOUS behind a
dedicated thread) and the vendored fork plus ROWID keyset paging, SDU
page sizing, connection recycling and the type_hints mechanism were
DELETED rather than fixed — net -25,000 lines. THE TRANSPORT CHANGED
WITH IT: oracle pushes ARROW, not NDJSON, joining postgres; the house
rule is JSON for natively-JSON sources (rest, jsonl/csv) and Arrow
for typed/columnar ones, and oracle was the outlier. Arrow is why
decimals stay Decimal128 (exact at 38 digits, refusing rather than
truncating), binary stays Binary (NDJSON hex-DOUBLED it), and NaN/Inf
survive. THE PRICE, measured not assumed: ODPI-C compiles from
vendored C source so the BUILD needs no Oracle client (verified — the
e2e suite passes with none present, and there is no link-time
libclntsh), but the CONNECTION dlopens one at RUNTIME; Instant Client
is a 70 MB unauthenticated OTN download, fetchable by CI, NOT
vendorable, and needs libaio. Live cells skip-not-fail without it.
FIVE REVIEW ROUNDS, ~35 defects fixed and pinned; the recurring
lesson is that a fix is not finished until attacked — round 2's D1
was round 1's own fix, and the stale-statement infinite loop appeared
THREE times (cursored, then cursorless). THE BENCHMARK IS THREE-WAY
and recorded: rdlt 832.6 ms vs dlt 3.42 s (4.1x) vs Airbyte 45.45 s
(54.6x) at 200k x 12 — but read the Airbyte ratio with 018's caveat,
it is JOB WALL CLOCK including orchestration and sits in Airbyte's
recorded ~45-60 s floor regardless of dataset size. rdlt/dlt
reproduced across FOUR runs at 4.0-4.2x. GATE: 1094/1096, count
exactly as predicted (1046 + 50), with ONE failure —
postgres's memory_bound — PROVEN pre-existing on main four ways and
left for postgres to own, NOT patched from inside this feature.
STANDING: no README on the crate.)
Previous feature 031 for reference:
`specs/031-duckdb-v2/plan.md` (feature: DUCKDB SECOND GENERATION —
COMPLETE and SWAPPED IN on branch `031-duckdb-v2` (off main @
961d0b77; NOT merged): the duckdb destination rewritten
TRUE-greenfield on the connector sdk as `rdlt-connector-duckdb-v2`,
then — owner decision — generation 1 DELETED and the crate renamed
`rdlt-connector-duckdb` (facade DestSpec::Duckdb arm ported to
destination::Config + Shell::new; consumers across FIVE crates ported
to config-keyed READ-ONLY testhook oracles; Makefile collapsed to one
binary(crash_sweep) line). Every second-generation rewrite is now
LIVE under its real name. The committed CONTRACT INVENTORY corrects
the 017-era record: duckdb classification is MESSAGE-PREFIX keyed
("IO Error" transient, "Constraint Error" the duplicate-key
diagnosis) — the structured channel is degenerate, probe-pinned. A
config DOCUMENT is born in v2 (gen 1 was builder-only; schema
attached). FOUR REVIEW ROUNDS (terminus round 4 under /code-review),
the big catches all fixed+pinned with guards red-proven: the suite
MEASURED the two-instance WAL-truncation hazard (a second read-write
open beside a live one truncates its WAL = silent commit loss) → a
process-global canonicalized-path registry refuses the second
in-process open, the claim outliving SESSIONS (round 2's catch) and
the whole check-open-claim sequence under ONE lock (round 3's TOCTOU
catch); THE REORDER TRACE (round 3): a same-set different-order
re-ensure passed the drop guard, emitted no DDL, and overwrote
`previous`, so the positional write guard blessed value-swapping
batches — the ensure guard is now SEQUENCE-PREFIX (drops AND reorders
refused); commit probes hoisted INSIDE the one transaction; uniform
commit-path classification; the positional append exact-arity;
sqlcore's validate_merge gained the merge-key-existence check (shared
with postgres). STANDING OWNER RECORDS: the full shared-table
collision gate needs the ENGINE's rename knowledge (S1 remainder);
within-run widen (S3); IO-prefix transience incl. deterministic paths
(S5); no README on the renamed crate. Gates twice clean at every
stage, first attempt each, counts predicted and verified: 1092 →
1097 (review rounds) → 1046 post-swap (= 1097 − gen 1's 51). plan.md
carries D1-D7, the four review rounds, the gate blocks, and the
swap-in record.)
Previous feature 030 for reference:
`specs/030-file-v2/plan.md` (feature: FILE FAMILY SECOND GENERATION —
COMPLETE, SWAPPED IN, and MERGED to main @ 892951bd (fast-forward from
8a10d0fe): the file family rewritten TRUE-greenfield on the connector
sdk, BOTH halves (source + destination — the postgres shape), then —
owner decision — generation 1 DELETED and the crate renamed
`rdlt-connector-file` (facade pipeline_spec ported to the sdk Shells;
the engine's crash_sweep re-pointed; the swap's ONE gate catch: gen 1
carried the workspace's only e2e-named binaries, so TARGET=e2e came up
EMPTY and failed per the 024 discipline — tests/e2e.rs restores the
cross-connector jsonl→duckdb cell under the load-bearing name).
Post-swap gates twice clean 1024/1024. The committed CONTRACT INVENTORY
(specs/030-file-v2/contract-inventory.md) is authoritative: persisted
cursor v1 wire keys done/size/eol (+ additive mtime_ms/etag/tail_hash/
row_groups_hash), the TAIL-HASH resume rulebook, the 4-phase commit
(replay dedup from the _rdlt_commits receipt log → receipt-guarded
once-per-load Replace truncation → deterministic publish → state then
receipt LAST), the ownership-precise truncation shape rule, all 11
crash points, every frozen message spelling, and the 14-item
suspicious docket. FIVE REVIEW ROUNDS (terminus round 5). The big
catches, all fixed+pinned: the gen-1 STAGED-NAME AMBIGUITY —
{load}-{table}-{slug}-{index} with `-` legal in tables AND slugs let
(events, us-east) and (events-us, east) share ONE staging file =
silent cross-table corruption; amended to table-as-path-segment
(staging is transient, reclaimed wholesale — not a persisted format);
a partition VALUE of `..` escaped the table directory via path
resolution (now __dot__/__dotdot__ sentinels); duplicate CSV headers
silently dropped a column (typed refusal); tail-hash poisoning on
unverified resumes (a healthy append refused as rewritten, advice
that duplicates); wildcards reached dot-prefixed names through `**`
(uncommitted staged parts readable by a data glob); the parquet
reader silently ignored a wrong-kind resume check; no future-version
gate on the cursor; the per-commit part-index defect (caught LIVE by
the S3 crash sweep: 6 rows where 4 loaded). An anti-transcription
verdict forced a full re-derivation of the read/location layers
mid-review (memory: rewrite means no copying). Weld proofs carried
verbatim from gen 1 caught 4 paraphrased planner spellings. Both sdk
conformance kits certify the Shells. Suite: 107 offline + the
failpoints crash_sweep binary (11 points × 3 actions, S3 arm against
RUSTFS skip-not-fail) wired into TARGET=sweep beside gen 1. GATES
TWICE CLEAN at the PREDICTED 1131/1131 (= 1024 + 107; semver clean, 0
regressed, cold 23.4/23.8 ms). STANDING OWNER RECORDS: S4 path_safe
collisions merge partitions (frozen layout); S6 two concurrent
sessions of one pipeline destroy each other's staging; S9
type_hints/validate accepted-and-ignored off their formats. plan.md
carries D1-D7, the five review rounds, and the gate of record.)
Previous feature 029 for reference:
`specs/029-iceberg-v2/plan.md` (feature: ICEBERG SECOND GENERATION —
COMPLETE and SWAPPED IN on branch `029-iceberg-v2` (stacked on 028's
branch; NOT merged): the destination rewritten TRUE-greenfield on the
connector sdk as `rdlt-connector-iceberg-v2`, then — owner decision —
generation 1 DELETED and the crate renamed `rdlt-connector-iceberg`
(facade pipeline_spec ported to destination::Config + Shell::new; the
Makefile sweep filter edited to binary(crash_sweep); the spark
deep-tier line retired with gen 1). The committed CONTRACT INVENTORY
(specs/029-iceberg-v2/contract-inventory.md) is authoritative and
corrected three 016-era claims: Replace shipped TYPED-UNSUPPORTED,
state lives in `_rdlt_state` marker-table properties under
`rdlt.state.{scope}` in a SEPARATE post-data property commit, and the
identity scope is ident_hash(pipeline, 12). Fresh design D1-D7 under
the one-dependency rule (sdk alone — NO sqlcore; the library trio
behind ONE boundary module); D7: existing_receipt deliberately None
(receipts are per-table snapshot properties; publish converges from
partial state via the per-attempt already_committed history scan).
FOUR REVIEW ROUNDS (terminus round 4), NINE fixes pinned — the big
ones: two streams resolving to ONE physical table was SILENT
CORRUPTION (colliding file paths + replay-discard; refused at both the
config and ensure gates); the status parser missed the library
token-endpoint's `code:` context key so a wrong client secret
classified TRANSIENT-forever in BOTH generations (measured live,
verified in library source; the tail-scan truncated too, its pin
mutation-proven); the `tables` map keys on the ENGINE'S NORMALIZED
root-table name (doc corrected, pinned live); three vacuity closures
(drift-refusal enforcement site, writer retirement offline against
memory FileIO, backoff shape). STANDING OWNER RECORDS: N2 — without a
WAL workdir a mid-publish transient restart mints a new load id and
snapshot convergence cannot see the prior attempt (rows can append
twice; inherent to the catalog's non-atomic multi-table publish); N3
token-TTL fatal; props-override Secret bypass; 12-hex scope risk.
Fixture: Polaris pinned BY DIGEST + RUSTFS by version tag, plain
podman host-network, PID-derived ports; the sdk conformance kit
CERTIFIES the shell live (new over gen 1). Gates twice clean at every
stage, counts predicted and verified: 1076 → 1081 (review) → 1024
post-swap (= 1081 − gen 1's 57). THE SESSION LESSON the gates
surfaced: 1,988 leaked anonymous podman volumes at the 2,048-lock
ceiling were the flake amplifier — `podman volume prune` is the fix
and `make reclaim` does NOT sweep volumes. plan.md carries D1-D7,
four review rounds, the coverage map, the swap-in record, and three
gate-of-record blocks.)
Previous feature 028 for reference:
`specs/028-snowflake-v2/plan.md` (feature: SNOWFLAKE SECOND GENERATION —
COMPLETE and SWAPPED IN on branch `028-snowflake-v2` (off main @
8cc1c22e, NOT merged): the destination rewritten TRUE-greenfield on the
connector sdk as `rdlt-connector-snowflake-v2`, then — owner decision —
generation 1 DELETED and the crate renamed `rdlt-connector-snowflake`
(facade pipeline_spec ported to destination::Config + Shell::new, which
validates the hand-parsed document). Fresh design D1-D6: born on the
sdk under the one-dependency rule (sdk + the recorded sqlcore
exception), unit.rs owns the transaction AND the DML-only executor
discipline, catalog.rs the read-before-write image, client.rs the ONE
library boundary (4-method Executor), typed ConfigError with options
validated at parse; every frozen surface byte-identical (messages,
classification, crash points sf.stage.write/upload + sf.unit.publish +
sf.receipt.visible, SQL text, PUT/COPY staging semantics, protocol
dispositions). FIVE REVIEW ROUNDS (terminus round 5), EIGHT defects
fixed+pinned — five inherited from generation 1 that no gate ever saw:
the engine ensures MID-UNIT on schema evolution (gen 1 panicked in
debug / DDL auto-committed the partial unit in release; now the unit is
deliberately ended by rollback first, with rolled-back Replace clears
OWED and re-executed by open_unit on every unit-opening path); the COPY
column list followed the FIRST write only (a mid-unit added column
landed NULL silently; now the latest write's superset); the Replace
clear guard was memory-only against sqlcore's DirectToTarget contract
(now the third bookkeeping table `_rdlt_cleared`, written in the
clear's own transaction and seeded at connect); the shared SELECT
EXISTS full-feed probe answers BOOLEAN and was unreadable through the
numeric scalar arms (measured live — merge_scope/scd2-absent:retire
publishes failed in BOTH generations; Boolean arm + live retire cell);
a just-created merge stage table never joined the catalog image so
same-session evolution skipped its ADD COLUMN (leg folds in;
record_created EXTENDS). Plus the cross-table owed-clears and phase-2
gate-bypass fixes, and a gate-caught coexistence collision (both
generations' conformance cells shared /tmp staging derived from the
kit's fixed identity — serialised while coexisting, scaffolding died at
swap). Gates twice clean at every stage, counts predicted and verified:
1106 → 1122 (review rounds) → 1011 post-swap (= 1122 − gen 1's 111; 0
skips now — the two #[ignore]d instrument skips died with gen 1's
suites). Cross-type Widen stays a recorded limitation (loud service
refusal, gen-1 parity). plan.md carries D1-D6, five review rounds, the
swap-in record, and three gate-of-record blocks.)
Previous feature 027 for reference:
`specs/027-sdk-trio/plan.md` (feature: THE SDK TRIO — the connector-facing
foundation as three deliberate layers, ALL SECOND-GENERATION and SWAPPED
IN on branch `027-sdk-trio` (off main @ cb130ee8, NOT merged): the SPI
`rdlt-connector` rewritten greenfield (check() probes, error context()
single-frame, non_exhaustive capabilities with with_* builders,
OpenContext, modules parquet/store, ByteSender/ByteReceiver; workspace
0.2.0→0.3.0 — the 014 window LANDED, semver gate passes); the NEW
`rdlt-connector-sdk` connector-builder FRAMEWORK (D10 — inversion of
control: config::Document parse-then-validate that renders NO text,
SourceConnector + Feed with ControlFlow cancellation, DestinationConnector
+ Backend with the session choreography owning write-before-ensure
refusal and existing_receipt→replay→publish; proof discipline = an
in-memory example connector passes the SAME testkit kits shipping
connectors answer to; shells carry the generic from-text family so the
config triple lives ONCE); `rdlt-testkit` rewritten (scanner
DO-NOT-SIMPLIFY design intact, conformance docs claim EXACTLY the
asserted clauses S1/S2/S4 + D1-D6/D8, TWO inherited gen-1 defects fixed
parity-safe and pinned: the S4 check skipped after an S1/S2 failure, and
open() failures all mislabelled D4). WAVE 4: rest AND postgres fully
adopted (owner scope — duckdb/file/iceberg/snowflake await the owner's
rewrite); the pg destination's commit split into the choreography hooks
with 14/14 crash sweeps unchanged as the exactly-once proof; pg-dest's
non-validating from_* asymmetry CLOSED. WAVE 5: the ONE-DEPENDENCY rule
(connectors depend on the sdk alone, SPI via its `spi` re-export, sdk
forwards failpoints/schema/object-store; ENFORCED by the sdk's
test_dependency_rule with sqlcore + optional-fixtures-testkit as the
recorded exceptions) and `docs/connector-authoring.md`. Every stage
gated TWICE CLEAN with counts predicted and verified; environment
flakes recorded, never re-rolled. plan.md carries D1-D10, the wave
records, swap-ins, review rounds, and gate-of-record blocks.)
Previous feature 026 for reference:
`specs/026-rest-v2/plan.md` (feature: REST SECOND GENERATION — COMPLETE,
SWAPPED IN, and MERGED, the second application of the 025 playbook.
Written greenfield as `rdlt-connector-rest-v2` (no code copied from
generation 1), gated clean while both generations coexisted, taken
through a THREE-ROUND adversarial review loop, then — owner decision —
generation 1 was DELETED and this crate renamed to
`rdlt-connector-rest`. Same complete functionality; the Rust API is
renamed by 025's seven naming rules with the ledger in the plan
(`source::Rest`/`source::Config`, `http::Client`/`http::Credentials`,
`paginate::{Paginator, Context, Decision, Error}`; NO crate-root
re-exports — module paths are canonical; the JSONPath-subset selector
HOISTED to `source::select`, removing gen 1's config→read layering
inversion). FROZEN and verified: the whole YAML document vocabulary
including the legacy tagged-auth form and flat cursor aliases, error
classification, crash-point IDs `rest.request`/`rest.decode`/
`rest.checkpoint`, cursor semantics, wire behavior (merge precedence,
POST-body page params, percent-encoding + dot-segment escape, the
fingerprint loop guard, the 64KiB action window, Link parsing). THREE
defects inherited from generation 1 were found by review and fixed
parity-safe, each pinned: sequential placeholder substitution let a
parent value shaped like another `{token}` have that field's value
injected (now ONE left-to-right template pass); an OAuth2 token-endpoint
429 was run-aborting fatal (now RateLimited carrying Retry-After); child
fan-out context double-framed the classification (context now wraps the
INNER cause). Every generation-1 test ported byte-identical and passed
as written. plan.md carries STATUS, the SWAP-IN record, decisions D1-D6,
frozen surfaces, and the three REVIEW ROUNDS.)
Previous feature 025 for reference:
`specs/025-postgres-v2/plan.md` (feature: POSTGRES SECOND GENERATION —
COMPLETE, SWAPPED IN, and MERGED. Written greenfield as
`rdlt-connector-postgres-v2` (no code copied from generation 1), gated
twice clean while both generations coexisted, adversarially reviewed over
six recorded rounds, then — owner decision — generation 1 was DELETED and
this crate renamed to `rdlt-connector-postgres`; commit `79211241` is the
last tree carrying both. Same complete functionality; the Rust API is
renamed by the plan's seven naming rules with the full old→new ledger in
Appendix C (`source::Postgres`/`source::Config`,
`destination::Postgres::new(..).schema(..)`, `tls::Policy`,
`fixtures::PostgresContainer`; crate feature `dest` is now `destination`;
test binaries `source_crash_sweep`/`destination_crash_sweep`). FROZEN and
verified byte-identical (Appendix B): the YAML vocabulary (`conn`,
`dataset` — serde renames, NOT field names), persisted cursor + CDC state
JSON, COPY BINARY + pgoutput v1 wire, golden SQL, crash-point IDs (the
scanner selfcheck still finds 11 directly-armed of 14 declared, same three
indirect). PERF: iai parity BEATEN (decode −2.1%, encode −4.8%) and
wall-clock A/B no regression; `benches/iai_pg.rs` deliberately keeps the
generation-1 benchmark IDs `pg_copy_decode_10k`/`pg_copy_encode_10k` so
the recorded perf baselines keep binding — do not rename them. The A/B
harness `tests/perf_ab.rs` died with generation 1 (it needed both crates);
its figures are recorded in plan.md STATUS. Three v1 defects quietly fixed
and pinned: post-COMMIT ROLLBACK under `pg.tx.acked`, an inter-stream CDC
panic race, `--5` numeric literal misparse. Recorded and deliberately NOT
changed: the row-key `|`-join collision (persisted-state encoding, owner
schedules any fix) and CDC snapshot single-instant best-effort under
mid-run errors. plan.md carries STATUS, the SWAP-IN record, REVIEW ROUNDS,
decisions D1-D6, and the naming rules.)
Previous feature 024 for reference:
`specs/024-gate-integrity/plan.md` (feature: TEST-GATE INTEGRITY — make the
gate incapable of passing silently. COMPLETE 49/49 on branch
`024-gate-integrity` off main @ 34ccd379, since merged to main (in
d92cec06's history). All five stories
delivered; GI1-GI8 all MET; gate TWICE CLEAN (961/961 both runs, 2 named
instrument skips, six sweep suites, semver clean, 6 benches 0 regressed, cold
start 23.5/23.9 ms); coverage 87.25% reproducing 023's recorded 87.22%. Contract contracts/gate-integrity.md.
WHAT THE GATE NOW GUARANTEES that it did not: an empty test selection FAILS
(nine `--no-tests=pass` flags deleted — the runner already defaults to fail, so
the flags protected nothing and hid renames); every one of 107 test binaries is
invoked or exempt BY NAME; ten crash-point registries across six crates verify
against their own SOURCES rather than themselves; a file compiled by no gate
command is now type-checked; a resource probe could be DEMANDED
(`RDLT_TESTKIT_REQUIRE_CONTAINERS` / `_REQUIRE_SNOWFLAKE`; the four gate
env knobs were later REMOVED by 027 D9 — sane default only, count
discipline is the net); `make semver` exists at all.
THE FIGURE THAT SUMS IT UP: `make test TARGET=prop` went 0.000s/0 tests ->
38.026s/1 test. Its selector was `test(shred_property)` — a test-NAME filter —
while `shred_property` is the BINARY and its test is `shred_invariants_hold`.
The 4,096-case property run had been green while executing nothing. A
zero-second pass is the signature of this whole defect class.
DO NOT "SIMPLIFY" THE REGISTRY SCANNER (rdlt-testkit crash.rs). Three designs
were overturned by measurement and each wrong one FAILS OPEN: (1) set equality
breaks because THREE postgres points are armed indirectly (macro takes a
variable, literal sits at the constructor) and reporting them missing invites
SHRINKING the registry; (2) counting occurrences assumed the declaration lives
in the scanned tree — six connectors satisfy that by coincidence, the ENGINE
does not (ENGINE_POINTS is in its test file), so declaration blocks are located
by SHAPE and excluded; (3) one assertion per registry fails where a crate has
three over one tree (file, postgres) — it is one per CRATE against the union.
Two arming spellings are recognised (`crash_point!`, `crash_at`); a third needs
adding to ARMING_PATTERNS, and the vacuity guard is what makes a missing
spelling fail rather than agree.
THE ICEBERG NEXTEST FILTER STAYS NEGATIVE — re-spelling it positively was
investigated and REJECTED (a positive list of ten fails the other way when an
eleventh live binary is added). Membership is asserted by a test instead.
US2's guarantee is deliberately WEAKER than the rest (close-out D-4): the
reachability enumeration is derived from the filesystem but nothing FAILS on an
unreachable binary — that needs the gate to model its own target graph.
GATE WORKFLOW, learned the hard way THREE times (close-out D-7): `make check`
spawns sub-makes that RE-READ the Makefile, so editing anything during a run
measures a mixture — make all edits, then run one untouched gate. And do not
wait on a PID from `pgrep -f 'make check'`: the pattern matches the waiting
shell. Wait on a completion marker in the log.
close-out.md is authoritative for every disposition.)
Previous feature 023 for reference:
`specs/023-snowflake-put/plan.md` (feature: Snowflake internal-stage
ingestion as the SINGLE path — COMPLETE 51/54 on branch `023-snowflake-put`,
NOT merged and NOT pushed. Gate TWICE CLEAN on the pinned 1.96.0 toolchain:
948/948 (2 skips, both #[ignore]d instruments), six in-gate sweep suites,
6 benches 0 regressed, cold start 23.8/23.3 ms vs a 40 ms bar; Snowflake's
own sweep separately 2/2 at 27 cells; coverage 87.22% (floor 80); semver no
update required. US1/US2/US4/US5 delivered; US3 PARTIAL by owner decision
(password + OAuth stay UNPERFORMED, the same call 022 made — legs written,
skip announced, credential entries turn them green with zero code change).
TWO MISSES RECORDED AND DELIBERATELY NOT AMENDED — SC-012's wall-clock half
and the one sentence of SP7 repeating it; rewriting either is the owner's
call. `specs/023-snowflake-put/close-out.md` is authoritative for every
disposition.
It makes the service's own recommended mechanism the ONLY one and DELETED
both 022 workarounds: batched INSERT and the external S3 stage, with their
config, credentials, encoders, constants, suites and four dependencies. Contract contracts/snowflake-put.md SP1-SP8,
which AMENDS 022's SD1 and SD6 explicitly.
THE DEPENDENCY IS THE HARD PART, not the code. The upload comes from a FORK
(rapidbyte-io/snowflake-connector-rs, feat/put-file-upload) pinned by REV
with the `version` key DELIBERATELY OMITTED — verified locally: a dep
carrying BOTH git AND version PUBLISHES SILENTLY with the git source
stripped, shipping a crate that resolves upstream (no PUT), compiles, and
fails at runtime with 391911; git-without-version makes packaging REFUSE,
which is the safe form. Blast radius: rdlt, rdlt-cli,
rdlt-connector-snowflake unpublishable until upstreamed or the fork is
published under its own name (a `package =` rename needs ZERO source
changes). This VIOLATES the constitution's "dependencies resolvable at plan
time with registry facts" — recorded in plan.md Complexity Tracking with
its exits, not waved through.
FOUR SERVICE FACTS MEASURED LIVE with the fork before design froze, each to
be pinned: (1) PUT does NOT commit an open transaction (rolled-back count 0,
txid IDENTICAL across the PUT, COMMIT-instead kept the row) so staging stays
INSIDE the unit; (2) CREATE STAGE does not commit but DROP STAGE DOES (3/3
each) so teardown stays outside; (3) snappy arrow parquet passes through
untouched, no .gz, +12 bytes of encryption padding only; (4) a multi-file
PUT returns Ok with a MIXED rowset — Err ONLY when every row failed — so
EVERY row's `status` must be inspected or data is lost silently.
NAMING TRAP: FILES=() must use PUT's reported `target` (basename + any
compression suffix), relative to the FROM prefix. LIST's `name` column is
NOT usable (doubles the prefix -> 091016) and is lowercased. Column matching
stays CASE_INSENSITIVE — 022 pinned it deliberately (lowercase arrow names
vs upper catalog); a research pass proposed reversing it and would have
broken every load.
TWO DEFECTS THE ACCOUNT CAUGHT that no reading would have (close-out D-32,
D-33): MATCH_BY_COLUMN_NAME sets an absent target column to NULL rather than
its DEFAULT, which nulled the stage table's arrival column and made every
merge survivor ARBITRARY — so columns are now projected EXPLICITLY. Then the
projection itself: `$1:"COL"` into a staged file is CASE-SENSITIVE, so the
symmetrical-looking upper-case form found nothing and every column arrived
NULL. Target list = catalog's case; projection = the FILE's case. Do not
"restore" the symmetrical version.
MEASURED, and it NARROWED the plan's premise: three identical 250k runs span
34.6%. INSERT at 582 rows/s is far outside that band so its supersession is
REAL (1,885 = 3.24x); the external bucket's 2,191/1,941 fall INSIDE it, so
that supersession is NOT ESTABLISHED EITHER WAY — the bucket's removal rests
on simplification and deleting user credentials, NOT throughput. Never claim
a speed win over the bucket. This also RESOLVED 022's open 11% question: it
was variance (spread is 3x the gap, and 023's figures move the opposite way
across the same size step).
SC-012 MISSED BY HALF and recorded, not amended: sweep cells fell 30 -> 27
(Merge newly covered at the publish) but wall clock roughly DOUBLES, because
the sweep's loads are 40 rows and an upload costs far more round trips than
a statement at that size. The criterion conflated matrix size with time.
NOTE the Makefile's sweep target has NO snowflake line and never has — this
sweep is run BY HAND (022 did it twice at 4,308 s), so `make check` cost is
unaffected.
Named stage required: @~ has NO scoping (visible across schemas/databases),
@%TABLE can only load its OWN table (001023).
All six research open questions are now TERMINAL (close-out): part bound
answered in research A2; LIST exposes `last_modified` so remote reclaim
ships at parity with the deleted path, comparing in SQL on the SERVICE's
clock (a local clock has no defined relation to the stamping one, and
hand-parsing a date to decide what to DELETE is an expensive bug) with a
deliberately generous 24 h window; the local-write and upload moments earn a
point EACH because they leave different debris in different places reclaimed
by different code; the sweep DOES gain Merge, at the publish only; the
upstream issue was never filed; and the distribution check reads `cargo
metadata` so implicit members cannot slip through.
Previous feature 022 for reference:
`specs/022-snowflake-dest/plan.md` (Snowflake destination — COMPLETE 43/43,
merged @ 1ef4860b. Gate TWICE CLEAN on the PINNED 1.96.0 toolchain: 966/966
(2 skips, both #[ignore]d instruments), 13/13 crash sweeps, 6 benches 0
regressed, cold start 26.3/25.9 ms vs a 40 ms bar. SD1-SD8 MET, zero uncited
dispositions. Three service facts pinned: DDL auto-commits; nothing enforces
uniqueness (PKs informational, so merge correctness rests on the SQL and
every merge test reads back); CURRENT_TIMESTAMP is per-STATEMENT not
per-transaction (2,938 ms drift) so each unit captures one instant into a
session variable — safe because SET does NOT commit. Identifiers
quoted-UPPERCASE. MERGE INTO + QUALIFY ROW_NUMBER() replace ON CONFLICT and
DISTINCT ON. `IS TRUE` COMPILES NOWHERE on Snowflake — flag_set/flag_unset
are dialect methods. Seams widened in sqlcore: UpsertAction replaces a
postgres-syntax string, column_list_with takes the quoting. A REAL LEAK was
found and fixed: PemSource's derived Debug printed inline private keys
(D-21). Measured and DECLINED: an open() round-trip saving of 1 statement
(D-22). No INSERT/COPY crossover exists — below ~2,500 rows path choice is
noise (D-30).
BEWARE: `RUSTUP_TOOLCHAIN=1.97.1` in the shell environment SILENTLY
overrides rust-toolchain.toml — run gates with `env -u RUSTUP_TOOLCHAIN`.
The perf gate is the ONLY thing that catches it, by refusing a comparison
whose benches showed zero regressions; never re-record baselines to clear
that refusal.
Previous feature 020 for reference:
`specs/020-audit-remediation/plan.md` (feature: audit remediation —
COMPLETE on branch `020-audit-remediation`, NOT merged and NOT pushed.
Executed NEXT_STEPS.md (audited 2026-07-26 @ 634222e: 11 lenses, 175
findings, 47 defect claims adversarially verified — 29 CONFIRMED, 18
REFUTED). All 11 stories delivered; close-out CLOSED: contract matrix
AR1-AR8 all MET; all 157 ledger items carry a terminal disposition
(130 fixed, 22 deferred with named triggers, 5 rejected with the
measurement that rejected them, zero uncited); the 18 refutations
verified absent from implemented work. Gate of record: `make check`
TWICE CLEAN on a rebooted machine — 791/791 0 skipped with containers,
cold-start 25.6 ms (bar <=40), perf gate within tolerance; coverage
85.64% lines (floor 80). The 18 refutations in Appendix A REMAIN
BINDING NON-GOALS. THE 0.2->0.3 WINDOW DID NOT OPEN: US5's design was
attacked before implementation (research R0 / close-out D-10) and
scoped down to within-run enforcement + inheritance — no StateDoc
bump, no persisted-format change, no semver break; the standing
publish-time bump is still owed but nothing in 020 forces it. CI
REPAIR REMAINS OUT OF SCOPE (E1, org billing); every CI-only
verification is recorded UNPERFORMED, never green. TWO OTHER
verifications are UNPERFORMED and say so: T097's Polaris live image
probe (no container runtime at that increment) and T176's netem (no
`tc`, and the container shares the HOST netns — a qdisc on lo would
degrade the real machine; D-40's substitute measurement proved more
useful). THE DURABLE PERF FACT: pg-to-pg-dedup-1m is ~71% SERVER-side
(80.3% of wall is one INSERT..ON CONFLICT node; 4,013,669 WAL records
= 556 bytes of WAL per row against a ~121-byte source row, one index
on the table), so client-CPU wins buy headroom, not wall — read any
future perf claim on this shape against that denominator. Eight US11
measurements, four TAKEN (COPY encoder fast path -1.98% process
instructions D-35; stage sequence CACHE 32 -3.3% of the merge cell
D-37; partition ArrayFormatter hoisted out of the row loop -2.72%
D-41; S3 skip-fetch for finished etag-matched objects D-44) and four
DECLINED WITH NUMBERS (allocator 3.3% ceiling D-34; WAL residual 8.5%
but 019's D2 binds D-36; file-dest buffering constant not O(dataset)
D-38; canonical-JSON allocation 6.19% ceiling D-39). D17 taken (one
byte-budget channel; the engine's copy DELETED, AR6 verified); D18
buffering half closed on a heap profile, blocking half still open;
D19 rejected, premise changed. Contract: contracts/audit-remediation.md
AR1-AR8. Deviations, negatives and every disposition: close-out.md).
Previous feature 019 for reference:
`specs/019-performance-improvements/plan.md` (feature: performance
improvements — COMPLETE, merged @ 634222e, executing PERF_ANALYSIS.md
as nine increments. RECORDED 3-WAY SESSION 2026-07-25 on the merged
tree, all four bars PASS: pg-to-pg-1m 778.8 ms 13.2x vs dlt (bar >=4x),
pg-to-s3parquet-1m 999.4 ms 1.7x (deliberately UNBARRED — one session
on a newly-comparable cell is not a basis for a bar), s3jsonl-to-pg-200k
665.2 ms 95.0x (>=40x), s3jsonl-to-s3parquet-200k 914.1 ms 63.6x
(>=45x), pg-to-pg-dedup-1m 4.82 s 2.6x (>=2x, new bar). NO LOSSES AND NO
PARITIES REMAIN. The honest misses, recorded not buried: US2 wall
-14.3% vs the >=15% floor and RSS -7.5% vs >=8% (attributed to the
parquet destination's whole-part buffering, which US7 never
re-measured); US6 cell-CPU -4.9% vs >=10%; T047's context-switch target
4.0x vs 10x. US9 was RE-SCOPED ON EVIDENCE: T089-T095 NOT built —
single-pipeline throughput reached 1.19M rows/s (3.3x the rate the
3.5x target was derived from), 8 concurrent pipelines scale 8.43x, and
the story's lever addressed only 22.2% of the merge cell, Amdahl-bounded
at 1.29x against SC-005's required 1.5x. So the 0.2->0.3 semver window
STAYED CLOSED in 019 (feature 020 US5 reopens it). Persisted-format
bumps: WAL v1->2 (arrow IPC file segments, parquet DELETED from
rdlt-engine, exact-match refusal both ways) and bench artifact v2->3.
[profile.release] fat LTO + cgu1 (-13.2% CPU, binary -16%); [profile.dist]
strip only, NO panic=abort, NO allocator crate. COPY encoder rewritten
on ToSql::to_sql over a borrowed ColumnView (-40.3% instructions);
full-refresh publishes COPY straight into the target in one unit tx.
Snappy is now the default parquet compression. TWO ALLOCATION REMOVALS
MEASURED WORSE (D-13, D-21) — treat any counting-argument optimization
as guilty until measured. Contract: contracts/performance-improvements.md
PI1-PI8. Outcomes and every deviation: close-out.md).
Previous feature 018 for reference:
`specs/018-bench-refinement/plan.md` (feature: benchmark refinement —
COMPLETE. The benchmark is ONE e2e five-cell THREE-WAY matrix
(rdlt/dlt/Airbyte, same seeded sources, per-product destination
databases/prefixes, every arm rowcount-verified, timing boundaries in
Caveats). Constitution v1.1.0 (Principle VIII cells/bars, recorded-
session-floor requirement) amended BEFORE the vocabulary deletion
(631d9bd < 212edf5); 25 cells / 10 fixtures / all v1 artifacts / 8
bars DELETED at 212edf5, archive commit 40841ab cited everywhere
(Milestones, artifact-v1 rejection error). Artifact format_version 2
(class gone, extra{} + forced added). Cold-start on the instruments
track (benches/check-cold-start.sh <=40ms). Competitors: dlt
honest-fastest (connectorx headline, pyarrow context) + Airbyte as
driver kind (flat variants discovery, benches/competitors/airbyte/
setup.py+driver.py over abctl kind on rootless podman; pods reach host
fixtures at 169.254.1.2; ingress-nginx MUST stay scaled to 0 and node
pids-limit raised — spike/01; API via supervised port-forward :8600).
Recorded sessions 2026-07-25: 2-way then 3-way 15/15 arms — rdlt
1.95/1.62/1.15/0.99/14.60 s; vs dlt 5.3x / 1.0x / 55.3x / 60.1x / 0.9x;
Airbyte ~45-60 s job wall, floor-dominated (Caveats). **THESE 018
FIGURES ARE SUPERSEDED BY 019 — see the 019 block above for the current
standing.** The "0.9x LOSS on the dedup cell" that 018 recorded was
never real: 019 US1 found the cell delivered 3M rows against dlt's 1M
(the source discovers every table when `tables` is absent), and the
corrected cell is a 2.6x WIN carrying its own bar. Do not plan against
the numbers in this paragraph. bars.toml at 018 time: 3 bars vs
dlt (4x/40x/45x) below recorded floors, policy entries; parity+loss
cells, RSS, Airbyte ratios, Iceberg cell all deliberately unbarred/
not-taken (policy log). Close-out + deviations:
specs/018-bench-refinement/close-out.md. Contract:
contracts/bench-refinement.md BR1-BR8).
Previous feature 017 for reference:
`specs/017-workspace-refactoring/plan.md` (feature: workspace refactoring
program executing REFACTORING.md end-to-end — fix 12 latent defects
B1-B12 with red-before/green-after regression pins, then cross-cutting
refactors R1-R13 + delivery-surface items D1-D15 as ~12 independently
mergeable increments in value-per-risk order (Part 4 + Part 5 folded
in). Constitution v1.0.0 ratified (.specify/memory/constitution.md) —
this feature enforces Principles V (typed taxonomy, no citation IDs in
user-facing strings; substring-matching rendered errors FORBIDDEN) and
VI (self-contained comments). Key decisions (research.md): B5 duckdb
classification via structured code/extended_code (probe-pinned); B6
iceberg via status context value (probe + designed fallback);
DestError::RateLimited is ADDITIVE (#[non_exhaustive] verified); one
Secret in rdlt-connector::secret behind new SPI `schema` feature; R2
commit protocol = pure sqlcore planner commit_script->Vec<Step>,
destinations execute (golden pins prove SQL-identical); R6 shared
apply_delta/apply_batch used by Loader + two-pass WAL replay (B10);
R7 one file Location abstraction w/ read+write halves + one
keys_of_table ownership helper (closes B2/B9); D1-D5 testkit
containers module (runtime_available() superset probe, PgFixture
Option-returning skip-not-fail) + fixtures module (batch_of/
schema_for/meta_for); breaking renames = deprecated aliases or NAMED
deferrals to the recorded 0.2->0.3 window — window NOT opened here.
Behavior changes CONFINED to defect fixes + classification
corrections; persisted formats/golden pins byte-identical (WR1);
close-out matrix zero uncited dispositions (WR7); full gate green at
EVERY increment merge (WR8). Contract:
contracts/workspace-refactoring.md WR1-WR8).
Previous feature 016 for reference:
`specs/016-iceberg-dest/plan.md` (feature: provider-agnostic Iceberg
REST-catalog DESTINATION — new THIN crate rdlt-connector-iceberg
(facade rdlt::connector::iceberg, CLI destination: iceberg:) wrapping
Apache iceberg-rust at ONE boundary (errors.rs + commit.rs — library
types never cross the public surface; duckdb-rs wrapping precedent).
SURVEY RESOLVED AT PLAN TIME with registry facts: iceberg 0.10.0 +
iceberg-catalog-rest 0.10.0 + iceberg-storage-opendal 0.10.0
(opendal-s3) — arrow ^58/parquet ^58 match the workspace pin (single
arrow 58 tree proven by live cargo-tree probe — workspace pins 58.3);
toolchain pinned 1.96.0 (rust-toolchain.toml + workspace rust-version).
NOT taken: iceberg-catalog-glue (aws-sdk smithy tree; Glue/SigV4 is
PHASE-2, recorded); rdlt-connector-rest NOT a dep; file-crate
location/ NOT extracted (config VOCABULARY shared — family S3
spelling + Secret — plumbing not). Exactly-once = snapshot-native D3:
commit identity (rdlt.pipeline/load-id/commit-seq) in snapshot
SUMMARY properties, replay detected from snapshot history, StateDoc
in table property rdlt.state updated in the same atomic commit;
bounded conflict retry (4 attempts, refresh->rebuild->commit,
exhaustion typed naming table+competing snapshot). Closed type
mapping (Json->string documented; field IDs library-assigned only);
additive drift = UpdateSchema add-nullable-column. Write modes:
Append (fast-append) + Replace (overwrite once-per-load, durable
guard from snapshot history); T001 PROBES overwrite support in 0.10 —
fallback DESIGNED: v1 narrows to Append with Replace
typed-unsupported, recorded never silent (ID5). Auth v1:
oauth2_client_credentials + bearer (Secret-wrapped, grep-proof);
credential VENDING default (X-Iceberg-Access-Delegation, session
tokens; expiry = transient), family-S3 storage override explicit.
Tests: Polaris container + 015 RUSTFS container canonical leg
(testcontainers skip-not-fail, images/env VERIFIED at T001 like 015);
UC OSS candidate bearer leg gate-verified; pyiceberg read-back venv in
the standard gate (competitors-harness pattern), Spark read-back DEEP
tier only. Crash points ice.files.write/ice.commit/
ice.receipt.visible swept live x3 actions with duplicate-free
snapshot-history pins. Bench: iceberg-polaris-200k SCOREBOARD (never
gated). Verification: matrix zero uncited, parity vs dlt iceberg w/
deferrals named, >=80% coverage baseline-first, README, quickstart.
Contract: contracts/iceberg-dest.md ID1-ID8).
Previous feature 015 for reference:
`specs/015-file-completeness/plan.md` (file family unified —
rdlt-connector-parquet absorbed into rdlt-connector-file
(src/{source,dest}/ + shared location/ + formats/; ParquetDir frozen
alias); Location = Local | S3 via object_store; one cursor rulebook
incl. TAIL-HASH resume integrity; CSV record format w/ JOIN lattice;
gzip/zstd whole-file units; dest parquet+jsonl both kinds w/
partition_by + ownership-precise Replace truncation; RUSTFS container
cells; contract file-family.md FF1-FF8).
Previous feature 014 for reference:
`specs/014-rest-completeness/plan.md` (REST source completeness —
client/ (OAuth2 single-flight, Secret, bounded Retry-After), read/
(Paginator trait + 7 families, TYPED response-action matching,
parent-child fan-out), additive config incl. tagged-YAML compat;
contract rest-source.md RS1-RS8; 014 recorded the one-time semver
MAJOR — 0.2→0.3 at next publish, config enums #[non_exhaustive]).
Previous feature 013 for reference:
`specs/013-duckdb-completeness/plan.md` (rdlt-connector-sqlcore shared
merge core behind golden-SQL pins; duckdb full dlt parity; contract
shared-merge-core.md SM1-SM8).
Previous feature 012 for reference: `specs/012-bench-harness/plan.md`
(crates/rdlt-bench declarative TOML cells, bars.toml enforced by
rdlt-bench gate, generated RESULTS.md tables; new cells are scoreboard
unless 004 governance grants a bar; contract bench-harness.md
BH1-BH8; 015 added the generic Container fixture kind).
Features 005-011 (`specs/0{05,06,07,08,09,10,11}-*/`) are the merged
base being composed; 004's benchmark governance and 003's hardening
nets remain in force. The established architecture is feature 001:
`specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as
amended by features 002 and 006) remain authoritative; the approved
technical design is `2026-07-18-rdlt-engine-design.md` at the repo root.
Run tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
