# 032 — ORACLE SOURCE (`rdlt-connector-oracle`)

Owner goal: "design and plan and rewrite rdlt-connector-oracle (we
might use: github.com/stiang/oracle-rs; test against oracle running
in a container — testcontainers) (greenfield/clean layout/from
scratch clean implementation) — similarly current to postgres/rest."

Branch `032-oracle` off main @ 662ccaf4 (001-031 merged: all six
second generations live on the sdk). UNLIKE 025-031 there is NO
generation 1 — this is a NEW connector. "Similarly current to
postgres/rest" = the same discipline: born on the sdk, config
Document, typed errors, conformance-certified, container cells
skip-not-fail, review rounds to terminus, gates twice clean with
predicted counts. The contract-inventory phase is replaced by the
committed RESEARCH RECORD (`specs/032-oracle/research.md`, R1-R3 —
registry facts, fixture facts, semantics facts; every claim sourced).

## Decisions

> **D1 IS SUPERSEDED BY T005.** The driver choice below — `oracle-rs`,
> pure Rust, no Instant Client — was reversed on measurement after
> review round 3 showed most severe defects were the driver's own.
> The connector now rides `oracle` 0.6.3 (ODPI-C) and pushes ARROW
> rather than NDJSON. Everything else in D1 stands.

**D1 — SOURCE only, born on the sdk.** `SourceConnector` + `Feed`
(the rest shape; a destination is a future feature — the dominant
Oracle ELT direction is out). Dependencies: the sdk (SPI via `spi`)
+ `oracle-rs` (THE recorded driver exception, like duckdb's own
driver) — pure Rust, MIT/Apache-2.0, rustls-only, tokio-native,
structured `OracleError { code: u32 }`, zero C deps (verified by
dry-run resolve; the ODPI-C/OCI alternatives need Oracle's
proprietary Instant Client at runtime on every machine — a
disqualifier for the embeddable engine, the toolbox/CI, and dist).
sdk `test_dependency_rule` gains
`("rdlt-connector-oracle", &["rdlt-connector-sdk"])`.

**D2 — T001 IS A LIVE PROBE GATE, before any design freezes.** The
driver's release carries a KNOWN silent-truncation defect past its
100-row prefetch (issue #8; fix unmerged; maintenance stalled since
2026-03) — data loss on exactly the source workload. T001 runs three
probes against the live container: (a) a >100-row SELECT with
rowcount assertion; (b) the cancellation-desync shape (#11 — a
dropped in-flight future); (c) NUMBER fidelity through the String
mapping at 38 digits. DESIGNED FALLBACKS, chosen by probe outcome:
truncation confirmed → the read path paginates connector-side
(`ORDER BY <key> OFFSET :n ROWS FETCH NEXT :k ROWS ONLY` per batch —
sidesteps fetch_more entirely) OR the fork is pinned BY REV with the
`version` key deliberately omitted (the 023 packaging-refusal rule:
git-without-version REFUSES to publish, which is the safe form);
cancellation-desync confirmed → the connector treats ANY cancelled
call as connection-poisoning and drops-and-reconnects (pinned).

**D3 — Config Document.** `source::Config` (sdk `config::Document`,
schema attached): `host`, `port` (default 1521), `service` (e.g.
FREEPDB1), `user`, `password` (Secret), TLS posture per the driver's
rustls support, and `streams` (name, table or query, cursor column,
primary_key, type_hints) modeled on the postgres source vocabulary.
Typed `ConfigError` with the sdk from-text framings
(`invalid oracle source YAML/JSON/config: {0}`).

**D4 — Type rulebook (R3.1, deliberate row by row).** Oracle `DATE`
CARRIES TIME → `TimestampNaive`, never `Date`; `TIMESTAMP` →
TimestampNaive; `WITH TIME ZONE` → TimestampTz; `WITH LOCAL TIME
ZONE` → TimestampTz WITH the session pinned
`ALTER SESSION SET TIME_ZONE='UTC'` at connect (otherwise values
depend on client env — pinned); `NUMBER(p,s)` → `Decimal{p,s}` (both
cap at 38) riding the driver's String mapping; BARE `NUMBER`
(floating scale) → Utf8 canonical render with the one-warn-per-column
`rdlt::lossy` discipline (the pg documented-lossy precedent);
BINARY_FLOAT/DOUBLE → Float64; VARCHAR2/NVARCHAR2/CHAR/CLOB/NCLOB →
Utf8; BLOB/RAW → Binary; BOOLEAN (23ai) → Bool; JSON (21c+) → Json;
ROWID → Utf8. Anything else is a typed refusal naming the column.

**D5 — Identifiers: the 022 quoted-UPPERCASE rule.** Unquoted Oracle
identifiers fold UPPERCASE; the connector uppercases bare names and
always emits quoted.

**D6 — Incremental = the watermark cursor rulebook** (the postgres
spine): per-stream cursor column (numeric/timestamp), watermark never
lowered, persisted cursor as a v1 wire format with literal-shape
pins. Consistent snapshots via `SET TRANSACTION READ ONLY` (one
consistent SCN per read). ORA_ROWSCN is EXCLUDED from v1 and
recorded: block-level unless the table was created with
ROWDEPENDENCIES, and a conservative upper bound even then — a
correctness trap dressed as a feature. AS-OF-SCN reads: not in v1;
the ORA-01555 nuance (transient for fresh statements, PERMANENT for
a pinned snapshot) is recorded for whoever adds them.

**D7 — Classification, crash points, certification.** ORA codes
arrive STRUCTURED — the rulebook keys on `code`, never message text
(R3.4 seed: 12170/12541/03113/03114/01033/00018 transient;
01017/00942/00904 fatal; 01555 transient-for-fresh-statements).
Crash points `ora.query` (top of each stream read) and
`ora.checkpoint` (before cursor emission), swept by the crate's own
failpoints binary; registry + scanner census row + ungated twin per
the house rules. The sdk conformance kit certifies the Shell against
the LIVE container. Fixture (R2):
`docker.io/gvenzl/oracle-free:23.26.2-slim-faststart` (digest in
research.md) via the GenericImage/RUSTFS pattern — NOT the
testcontainers-modules oracle module (floating tag, no skip/reclaim
hooks); wait for `DATABASE IS READY TO USE!` (120 s budget) THEN poll
`SELECT 1 FROM DUAL` as APP_USER against `//host:port/FREEPDB1` (the
log precedes PDB listener registration — the gap yields ORA-12514);
skip-not-fail; ONE test binary shares ONE container (the slowest
fixture the gate will have — startup ~75 s).

## T001 PROBE RECORD (2026-08-02, live against
gvenzl/oracle-free:23.26.2-slim-faststart on podman)

- PROBE A (truncation): CONFIRMED AND WORSE. 500 rows inserted; the
  first batch returns 100 with `has_more_rows = false` (a lie);
  calling `fetch_more` anyway returns 0 ROWS (the cursor is dead —
  the tail is unrecoverable through the driver's continuation), and
  the misused cursor then KILLED THE CONNECTION (reset by peer on the
  next statement). Upstream 0.1.7 cannot stream past its 100-row
  prefetch, and the public query path HARDCODES prefetch 100 (the
  QueryOptions.prefetch_rows knob is not plumbed).
- PROBE B (NUMBER fidelity): 38 significant digits round-trip EXACTLY
  through the String mapping (raw and TO_CHAR identical).
- PROBE C (cancellation): a dropped in-flight future did NOT desync
  this shape (post-cancel query OK) — milder than issue #11's report,
  but the poisoning class is real: an ERRORED statement left the
  connection `ConnectionNotReady` (probed), and a name-collision
  CREATE surfaced as code 0 with a misleading LOB message (the real
  ORA code lost by the driver).

THE DECISION (D2's fallback taken): v1 builds on UPSTREAM 0.1.7 —
registry-resolvable today, constitution-clean — with the read path
designed around the defect: every stream read runs inside
`SET TRANSACTION READ ONLY` (one consistent SCN) and pages by
ROWID-KEYSET (`WHERE ROWID > :last ORDER BY ROWID FETCH FIRST 100
ROWS ONLY`-class pagination, page size = the 100-row prefetch so
every page arrives COMPLETE in its first batch; `fetch_more` and
`has_more_rows` are NEVER consulted). Cost: ~1 round-trip per 100
rows (localhost fixture ~1-3 ms/query — acceptable for v1; the
recorded motivation for the fork upgrade on WAN workloads). The
CLIENT BOUNDARY treats EVERY driver/protocol error as
connection-poisoning: drop, reconnect, classify by the structured ORA
code (transient family retries; the reconnect itself is the recovery)
— never reuse a connection that has seen an error. THE FORK OPTION
stays recorded, not taken: a rev-pinned fork (023's packaging rules —
git WITHOUT version so publishing REFUSES) restoring true streaming +
plumbed prefetch is the owner's upgrade path; upstream is stalled
(2026-03) so the fork means owning the rev.

**D8 — Performance (owner requirement, 2026-08-02).** The 100-row
page ceiling is a THROUGHPUT defect, not just a nuisance. Two levers,
staged: (1) v1 pipelines the keyset reads (the next page's query is
in flight while the current page renders to the channel) so the
round-trip cost overlaps; (2) the PATCH PATH is promoted from
"recorded option" to ACTIVE INVESTIGATION — the driver's internal
`ExecuteOptions::for_query(prefetch_rows)` exists and query hardcodes
100 at one site; if plumbing a real prefetch (and/or fixing the fetch
continuation) is a small patch, the workspace carries a patched
driver ([patch.crates-io] path or rev-pinned fork per 023's
packaging-refusal rules) and page size becomes a config knob. A
100k-row timing cell records the measured throughput either way.

**D9 — LOBs, flawless at any size (owner requirement).** The type
rulebook must not freeze until a LIVE LOB probe answers: what does
the driver return for CLOB/BLOB at 1 KB / 1 MB / 64 MB (inline value
vs locator vs error)? The design ladder: inline values pass through
the D4 mappings; if large LOBs arrive as locators or fail, the read
path falls back to SQL-side CHUNKING (DBMS_LOB.SUBSTR loops stitched
connector-side — always works over the wire, any size, bounded
memory); refusal is NOT an acceptable end state for CLOB/BLOB.
Correctness pins at each probed size band; a large-LOB cell in the
suite.

**D10 — The version matrix (owner requirement: an older-Oracle use
case exists).** Fixture legs: 23 Free (primary, R2) AND 21c XE
(`gvenzl/oracle-xe:21-slim` — containerizable, inside the driver's
claimed envelope), both skip-not-fail. 19c/12c: NO free container
image exists, and the driver has KNOWN breakage there (issues
#13/#9) — recorded as a standing limitation with the patch path (D8)
as the remedy; the owner's target version should be probed the moment
media/connectivity for it exists.

## T002 PROBE + PATCH-DIFFICULTY RECORD (2026-08-02)

LOBs, probed live at 1 KB / 1 MB / 8 MB (CLOB and BLOB): the driver
returns a LOCATOR at EVERY size — never inline content — so a plain
SELECT alone cannot deliver LOB data at all. The locator carries
`size` and `chunk_size`, and the driver's SEPARATE LOB-read path
(`read_lob`/`read_clob`/`read_blob`/`read_lob_chunked`) is correctly
implemented (right sequence numbers, the 23ai token, large-SDU
header, MULTI-PACKET receive). `DBMS_LOB.SUBSTR` chunking also works
as a pure-SQL fallback (4000 chars / 2000 bytes per call).

SOURCE READING OF THE DRIVER (the patch question) found the decisive
fact — A STANDING CORRECTNESS HOLE BEYOND ROW COUNTS: the query path
reads a SINGLE TNS packet (`receive()`, one ~8 KB SDU) while the LOB
path uses the multi-packet `receive_response()`. Any result page
whose WIRE size exceeds one packet is cut — loudly if the cut lands
mid-row, SILENTLY if it lands on a message boundary. At the hardcoded
100-row prefetch that is ~80 bytes/row before the hole opens, so
KEYSET PAGING ALONE DOES NOT MAKE WIDE-ROW TABLES SAFE. Also: the
`has_more_rows` flag is hardcoded `false` (connection.rs:2743) — the
truth is in the TTC end-of-batch flags the query path discards — and
`FetchMessage` is malformed for modern servers (hardcoded sequence 0,
missing the ub8 token required at ttc_field_version >= 18, small-SDU
header under a negotiated large SDU, no Marker handling) — which is
exactly the probe-A signature (0 rows, then a dead connection).

**D8/D9 REVISED — THE DECISION: PATCH AND VENDOR.** The workspace
carries a rev-pinned fork of the driver (023's packaging rules: git
WITHOUT version, so `cargo package` REFUSES rather than silently
publishing a crate that resolves upstream). The patch is ~100-150 LOC
in two files: plumb `prefetch_rows` (the wire field is ub4 — 10k is
legal), switch the query path's two `receive()` calls to
`receive_response()`, return an honest `has_more_rows`, and repair
`FetchMessage` (sequence, token, header, Marker). This closes the
silent-truncation hole AND the throughput ceiling on the 23 leg. The
21c leg keeps KEYSET PAGING (its capabilities force
`supports_end_of_response = false`, so multi-packet query termination
would need a resumable parser — structural, out of v1) with a
conservative page size and a recorded wide-row caveat. LOBs need NO
patch: the connector reads locators via `read_lob_chunked` (~1 MB
chunks; the driver's whole-buffer rescan is quadratic-ish on huge
LOBs, so chunking is the correctness-and-cost answer), with the
`DBMS_LOB.SUBSTR` path kept as the documented fallback.

## T003 — THE VENDORED PATCH, BUILT AND PROBED (2026-08-02)

The fork lives at `vendor/oracle-rs` (published 0.1.7 source, MIT OR
Apache-2.0 preserved; consumed via `[patch.crates-io]`). FOUR patches
attempted, THREE KEPT, ONE REVERTED ON EVIDENCE — plus a FOURTH kept
patch added later, recorded under "Keepalive" below:

KEPT — and probed working against 23 Free:
1. `FetchMessage` framing repaired: real sequence number (was
   hardcoded 0), the ub8 token required at ttc_field_version >= 18
   (Oracle 23ai), and the body now rides the connection's own
   multi-packet sender instead of hand-writing a small-SDU header
   under a negotiated large SDU.
2. Honest `has_more_rows`: the query parser reports continuation from
   the server's ORA-01403 terminator instead of the hardcoded
   `false` that made every truncated batch look complete.
3. `set_prefetch_rows` — the ub4 wire field is now a real knob
   (default stays 100).
   MEASURED: a 500-row SELECT returns ALL 500 IN ONE ROUND TRIP with
   `has_more = false` — the T001 truncation defect and the
   round-trip-per-100-rows ceiling are both gone for rows that fit
   the SDU.

REVERTED — the honest negative: switching the query path to a
multi-packet read. `receive_response` hangs on queries (it waits for
an end-of-response flag row replies never set, and its scan cannot
walk a row stream — probed: even `SELECT 1 FROM DUAL` hung). A
query-aware accumulator terminating on the ERROR marker ALSO fails:
the marker byte occurs inside binary row data, so a wide-row stream
cuts mid-message (probed: `BufferUnderflow` on 2000×3.9 KB rows).
Terminating a multi-packet row stream correctly requires the RESUMABLE
PARSER the patch report called structural — deliberately out of this
feature.

**THE STANDING LIMIT, now precise and LOUD:** one query reply must fit
one SDU packet (8 KB default, negotiable). Narrow rows stream freely
at any prefetch; a page of wide rows that exceeds the SDU fails with
`BufferUnderflow` — an ERROR, never silent truncation, which is the
property that matters. THE CONNECTOR'S OBLIGATION (design, not hope):
size every page from the DESCRIBED column widths so a page can never
exceed the SDU — wide tables get small pages, narrow tables get large
ones — and expose the SDU as config for operators who raise it
server-side. Wide-row streaming at high throughput is the recorded
motivation for the resumable-parser work, whoever takes it.

### Keepalive — the fourth kept patch (added at review round 3)

The driver opens its socket with `set_nodelay(true)` and nothing
else. A query whose server-side thinking time exceeds a firewall's or
NAT's idle timeout therefore has its connection reaped SILENTLY, and
the read waits out its ENTIRE statement budget (600 s by default)
against a socket nothing will ever answer. This is precisely why the
Oracle JDBC driver exposes `oracle.net.keepAlive`, which the owner's
own production config sets.

`Config::keepalive: Option<Duration>` now drives `set_tcp_keepalive`
at the connect site; the vendored default stays `None` (upstream
behaviour), while the CONNECTOR defaults it ON at 30 s — well below
the 300 s idle timeout common to firewalls. `socket2` is already in
the workspace graph via tokio, so this adds no new dependency.

Noted while reading that site: `connect_with_config` uses a bare
`TcpStream::connect` and never consults `Config::connect_timeout`
(that field belongs to the `transport::tcp` path, which this
constructor does not use). The connector enforces its own connect
budget instead — review round 2, D5.

### The JDBC parameter vocabulary

The owner's production JDBC string maps onto `tuning` one-for-one,
with one deliberate absence:

| JDBC | document |
|---|---|
| `defaultRowPrefetch` | `page_rows` (only ever LOWERS the derived page) |
| `oracle.jdbc.defaultLobPrefetchSize` | `lob_chunk_bytes` |
| `oracle.jdbc.ReadTimeout` | `read_timeout_ms` |
| `oracle.net.CONNECT_TIMEOUT` | `connect_timeout_ms` |
| `oracle.net.keepAlive` | `keepalive_secs` (`0` disables) |
| `useFetchSizeWithLongColumn` | **none, and none is needed** |

The last exists because the JDBC driver could not stream LONG columns
alongside a fetch size. LONG is deprecated by Oracle in favour of
CLOB, and this connector reads LOBs through locators, which are not
coupled to the page size at all. An unknown `tuning` key is REFUSED
rather than ignored, so a config carrying it fails loudly instead of
appearing to take effect — pinned.

## T004 — THE ROUND-3 MEASUREMENTS THAT BLOCK THE DESIGN (2026-08-03)

Round 3's driver lens claimed the read path leaks a server cursor per
page. PROBED LIVE, twice, independently (this session and the
benchmark harness), and CONFIRMED — with a second defect the probe
exposed on its own:

**M1 — a read dies at ~297 pages, whatever the page size.** 5,000 rows
at `page_rows: 5` delivered 1,475 rows (295 pages) and then Oracle
closed the connection. The benchmark harness measured the same wall at
297 pages on a different shape, INDEPENDENT of `page_rows` (7 and 14
both die there) and of table size, and PROVED the cause by raising the
server's `open_cursors` to 20,000 — at which the identical 20k read
completes (1,429 pages). Stock Oracle allows 300. Every page is a
distinct SQL text (the watermark and ROWID are interpolated literals),
the driver's statement cache holds a server cursor per distinct text,
and NOTHING ever closes one: `FunctionCode::CloseCursors` has no
sender in the driver, `close_cursor` only flips a local bool, and
`evict_lru` drops entries silently.

**M2 — constant SQL does NOT fix it, and binds are broken.** Probed
directly: 600 executions of one BYTE-IDENTICAL query died at 299, so
the statement cache does not reuse the server cursor. The obvious
alternative — bind the watermark and ROWID as parameters — fails at
the FIRST call with `connection closed unexpectedly`. Neither of the
cheap fixes exists.

**M3 — the read is super-linear in TABLE SIZE.** With the cursor limit
worked around: 20k rows 2.87 s, 40k 10.5 s, 80k 31.8 s. At an
identical 2,858 pages a 20k table costs 2.17 ms/page and a 40k table
3.68 ms/page — per-page cost tracks the TABLE's size, not the page
count, i.e. each page rescans. `WHERE ROWID > :last ORDER BY ROWID`
re-walks the blocks below the watermark every time. Extrapolated, 200k
rows is ~3-7 minutes; the plan predicted 8-31 s. NOT measured at 200k,
and no figure is claimed for it.

**THE COMMON ROOT.** M1 and M3 are the same defect wearing two hats:
the driver cannot STREAM a result set. Continuation past the prefetch
is broken (T001), the multi-packet read that would fix it was
attempted and reverted on evidence (T003), so the read path re-queries
per page — which forces both a fresh cursor per page and a rescan per
page. Every remedy short of fixing the driver's row streaming is a
workaround for that one thing.

**Two more defects the benchmark fixture found live:**
- `BINARY_DOUBLE`/`BINARY_FLOAT` are SILENTLY CORRUPTED. The driver's
  `parse_column_value` has no arm for either, so the `_ =>` catch-all
  returns `String::from_utf8_lossy(&bytes)` — raw wire bytes as
  mojibake — and the connector's `Float64` declaration then fails to
  parse and KEEPS the garbage. It surfaced only because Postgres
  refused the embedded NUL. `row.rs` decodes both correctly, so the
  fix is two arms in the driver.
- Named time-zone regions are refused on read
  (`FROM_TZ(..., 'UTC')` fails the whole stream).

## T005 — THE DRIVER DECISION REOPENED, AND PROBED (2026-08-03)

Round 3 established that the majority of the SEVERE defects are not
in the connector but in `oracle-rs` itself, and that most of this
crate's machinery (ROWID keyset paging, SDU page-size derivation,
connection recycling, four vendored patches) exists only to route
around them. So D1's rejection of the OCI route was reopened and the
alternative PROBED LIVE, T001-style, before any decision.

**The candidate:** `oracle` 0.6.3 (github.com/kubo/rust-oracle),
UPL-1.0/Apache-2.0, actively maintained, wrapping ODPI-C. It is
SYNCHRONOUS — no async feature exists — so it would sit behind
`spawn_blocking`. ODPI-C compiles from vendored source (needs only a
C toolchain, no client at BUILD time) but dlopens Oracle Client
libraries at RUNTIME.

**The runtime cost, measured rather than assumed.** Instant Client
Basic Lite is a 70 MB download from Oracle, no authentication, under
the OTN licence — so it can be fetched by CI but NOT vendored into
this repo. On this Fedora toolbox it additionally needed `libaio`,
which was not installed. That is the real price of this route: every
developer machine and CI runner needs a proprietary shared library
that we may not redistribute.

**THE PROBE, against the same 23ai container.** Every defect that
defeats `oracle-rs` is simply absent:

| what | `oracle-rs` (measured) | `oracle` 0.6.3 (measured) |
|---|---|---|
| NVARCHAR2 `N'zażółć'` | `"\0z\0a\u{1}|…"` destroyed | `"zażółć"` |
| BINARY_DOUBLE | mojibake (no decode arm) | `2.25`; Infinity as `inf` |
| FLOAT(126) describe | `Decimal{126,129}` (sign lost) | `Float(126)` |
| NUMBER(10,-2) describe | `Decimal{10,254}` | `Number(10, -2)`, value exact |
| CLOB of 3,999 by plain INSERT | UNREADABLE (O7) | reads whole |
| rows through ONE cursor | impossible (`fetch_more` hangs) | **50,000 in 22.7 ms** |
| consecutive statements | died at 299 | **2,000/2,000** |
| bound parameters | fail on the FIRST call | work |
| column nullability | discarded (patched in) | present |

The 22.7 ms is DRIVER-LEVEL fetch only, not a connector-path
comparison — the honest point is not the ratio but that there is no
per-page rescan and no cursor ceiling at all, which is what O1 and O2
are.

**THE CONSEQUENCE.** Adopting it DELETES rather than fixes: the
vendored fork and its four patches, ROWID keyset paging, SDU-derived
page sizing, connection recycling, the has_more_rows end-of-stream
rule, and O1-O7 in their entirety. The connector gets substantially
smaller.

**AND THE TRANSPORT CHANGES WITH IT (owner call, 2026-08-03).** The
SPI carries both `RawJson` and `Arrow`, and the house rule is already
implemented everywhere but here — postgres pushes Arrow (4 sites),
rest pushes JSON (natively JSON), file pushes JSON for jsonl/csv and
Arrow for parquet. Oracle is a TYPED source pushing NDJSON, which
means rendering typed values to JSON text so the engine's shredder
can parse them back into Arrow builders. That round trip is why BLOBs
are hex-encoded (double the bytes), why decimals cross as strings and
land as TEXT, why NaN/Infinity are forced to null, and why the whole
`type_hints` mechanism had to be built at all — with Arrow the schema
travels with the batch. Oracle moves to Arrow.

## THE REWRITE ON `oracle` 0.6.3 + ARROW (2026-08-03)

Executed on the owner's call after T005. What it DELETED is the point:

- `vendor/oracle-rs` and all FOUR patches, plus the
  `[patch.crates-io]` stanza. The workspace no longer forks a driver.
- ROWID keyset paging, session-data-unit page sizing, the
  `has_more_rows` end-of-stream rule, and the 250-page connection
  recycler — every one of them a workaround for a driver that could
  not stream.
- The `type_hints` mechanism, in both the config and the connector.
  Arrow carries its own schema, so a second copy could only
  disagree with it. The derivation survives as an EARLY REFUSAL: a
  table holding an unmappable type fails at discovery, naming the
  column.
- The LOB byte ceiling, with its knob (the driver materializes LOBs
  itself; an inert knob is worse than none).

What replaced them: `client.rs` owns the SYNCHRONOUS connection on a
dedicated thread and hands out futures; `schema.rs` maps Oracle types
straight to Arrow; `batch.rs` builds arrays BY DECLARED TYPE;
`read.rs` runs ONE query per stream and streams it.

**WHAT ARROW FIXED THAT NDJSON COULD NOT CARRY.** Exact decimals are
`Decimal128` via a scaled `i128` — all 38 of Oracle's digits, and a
value that exceeds its declared scale is REFUSED rather than
truncated. Binary is `Binary`, not hex text at double the size. NaN
and Infinity are held natively instead of null-ified for want of a
JSON literal. And the round trip itself is gone: the connector no
longer renders typed values to text for the engine to parse back into
these very builders.

**THE CURSOR RULEBOOK SURVIVED**, because it is about correctness
across RUNS rather than within one: watermark ordering with ROWID as
the tie-break, and `c > w OR (c = w AND ROWID > tie)` on resume.

### The new driver has one of its own, and a pin caught it

`TIMESTAMP WITH TIME ZONE` was arriving with its OFFSET DISCARDED.
`03:04:05.678 +02:00` should be the instant `01:04:05.678Z`; the
driver's `DateTime<Utc>` conversion keeps the wall-clock fields and
relabels them UTC, so it arrived as `03:04:05.678Z` — every zoned
value silently shifted by its own zone, in the direction that makes
the data look plausible.

Measured, not reasoned: the live cell asserted the instant and
failed by exactly two hours. The connector now reads
`oracle::sql_type::Timestamp` and applies `tz_hour_offset` /
`tz_minute_offset` itself, on BOTH the stored value and the
watermark so the two cannot disagree, with a unit pin covering
positive and negative offsets so a sign error cannot pass.

The lesson is the one this whole feature keeps teaching: a mature
driver is not a correct one, and the only thing that ever caught
these was asserting the VALUE rather than that the read succeeded.

### Four defects the rewrite itself introduced, all caught by pins

1. **The empty watermark, AGAIN.** The batch builder was never told
   which column carried the cursor, so every checkpoint persisted
   `""` — round 2's D1 in new clothes. Caught by the existing resume
   cells.
2. **The watermark rendered by asking for a String first**, which for
   a DATE returns Oracle's NLS spelling (`02-JAN-26`); the resume
   literal then rejected it with ORA-01861 on the very next run. Now
   rendered strictly by the DECLARED Arrow type.
3. **THE STALE STATEMENT.** The driver's cursor cannot outlive the
   closure that owns the connection, so each batch re-queries — and
   the statement was built ONCE, from the position the read started
   at. Every batch therefore re-ran the same SQL and returned rows
   1-25 forever: an infinite loop AND duplicate data, which presented
   as the crash sweep running past twenty minutes with no output.
   Found by tracing the emitted SQL, not by reading it: the trace
   showed the `WHERE` clause never appearing. The statement is now
   rebuilt from a position that advances with every batch, and the
   cursorless path gained the `ROWID >` predicate it was missing for
   the same reason. Sweep: hung indefinitely → 11.5 s.
4. **A container leak of my own making.** "Share one fixture through
   a `static OnceCell`" is wrong twice over: a static is never
   dropped at process exit, so testcontainers' cleanup never ran and
   one run left EIGHTEEN databases behind — and it bought nothing,
   because nextest gives each test its own process. Reverted. The
   real bound is a `oracle-live` test-group (max-threads 3) in
   .config/nextest.toml, added after unbounded parallelism was
   measured starting SIXTEEN Oracle databases at once.

### The operational price, stated plainly

ODPI-C compiles from vendored C source, so the BUILD needs only a C
toolchain — verified: `cargo check` passes with no Oracle client
present. The CONNECTION dlopens Oracle Client at runtime. Instant
Client Basic Lite is a 70 MB unauthenticated download under the OTN
licence: CI can fetch it, we may NOT vendor it, and on Fedora it also
needs `libaio`. The live cells skip-not-fail without it, naming the
remedy.

## STANDING OWNER RECORDS (round 3)

> **O1, O2, O4, O5 and O7 ARE DISSOLVED** by the move to `oracle`
> 0.6.3 — they were properties of the pure-Rust driver, not of
> Oracle. O3 (connections never closed) is also gone: the driver
> sends a real logoff. What remains of this list is O6.

Recorded against the PREVIOUS driver, kept for the record:

**O1 — the read is SUPER-LINEAR in table size.** Measured: 20k rows
2.87 s, 40k 10.5 s, 80k 31.8 s; per-page cost tracks the TABLE's size,
not the page count, because `WHERE ROWID > :last ORDER BY ROWID`
re-walks the blocks below the watermark on every page. The cursor
ceiling is fixed (connections are recycled at 250 pages), so a large
read now COMPLETES — it is simply slow. The remedy is either paging
by an INDEXED primary key instead of ROWID, or the driver work in O2.
The 200k benchmark cell is defined and deliberately unrecorded until
this moves.

**O2 — the driver cannot stream a result set, and that is the root of
everything above.** Continuation past the prefetch (`fetch_more`)
HANGS — re-probed at round 3 with all four patches in place, so it is
not fixed by them. `receive_response` waits for an end-of-response
flag that row replies never set, and a marker-based terminator cuts
wide rows mid-message. Correct termination needs a RESUMABLE PARSER.
Until then: one reply must fit one packet, every page is a fresh
query, and O1 follows.

**O7 — a CLOB just under 4 KB, written by a PLAIN INSERT, is
UNREADABLE.** Found by measurement at round 4 while pinning something
else, and reproduced from three directions:

- 3,000 characters reads back whole; **3,999 fails**
  (`buffer underflow: need 114 bytes but only 96 available`, raised
  inside the LOB LOCATOR read, not the row parse).
- It is NOT about size in general: the 2 MiB CLOB written with
  `DBMS_LOB.WRITEAPPEND` reads back whole, and always has.
- It is NOT the read parameters: identical error at
  `lob_chunk_bytes` of 4,000, 8,000 and 1 MiB, and at every page size.

The distinguishing factor is how the value is STORED — around
Oracle's inline/out-of-line threshold (`ENABLE STORAGE IN ROW`, just
under 4,000 bytes), which a plain `INSERT` of that length straddles
and `DBMS_LOB.WRITEAPPEND` does not. The existing 2 MiB cell never
caught it because every LOB it writes goes through `DBMS_LOB`.

CONSEQUENCE: LOB support is NOT the "flawless" the feature goal asks
for. It fails LOUDLY and fatally (not silently, and not retried — the
round-3 classification work covers it), but an ordinary estate whose
CLOBs were inserted normally will hit this. Diagnosing the driver's
locator parse is the fix and is not attempted here.

**O3 — no connection is ever closed.** `Connection::close()` sends a
proper logoff; nothing calls it, and `Drop` only sets a flag. A
many-stream pipeline that retries leaves sessions for the server to
reap by dead-connection detection.

**O4 — a server negotiating an SDU BELOW 8192 is undetectable.** The
gate caps the request at 8192, but `inner.sdu_size` from the ACCEPT
has no accessor, so an estate whose `sqlnet.ora` lowers the default
would derive pages for packets larger than it will send.

**O5 — sub-microsecond precision is truncated.** The driver decodes
fractional seconds as `nanos / 1000`, so `TIMESTAMP(9)` loses three
digits, and a resume at that boundary can re-deliver the rows inside
the truncated microsecond.

**O6 — TIMESTAMP WITH TIME ZONE watermarks are compared LEXICALLY.**
Correct while every value shares one offset (and SQL-side comparison
is always correct); with MIXED offsets the persisted watermark can
stall, which duplicates rows on the next run rather than losing them.

## REVIEW ROUNDS

The connector was built, then attacked. Each round is recorded with
what it found, because the pattern of what a round finds is the useful
part — not the count.

### Round 1 — eleven findings, all fixed

Five reviewers over the fresh code. The class that dominated: the
connector trusted values it had not checked. A cursor value that was
NULL, a ROWID that was not shaped like a ROWID, a watermark compared
as a `String` (so `"99" > "150"` and a 150-row read checkpointed 99),
numerics arriving as text and rendering as JSON strings. Each fixed
and pinned.

### Round 3 — five lenses, ~35 verified defects, two measured blockers

The heaviest round by far, and the first where reviewers MEASURED
rather than reasoned: three findings were reproduced live against the
container before being reported. Recorded in full at T004 above; the
classes were —

**Silent corruption.** NVARCHAR2/NCHAR were destroyed on every read
(the handshake advertises UTF-16, the decoder assumed UTF-8, and
`from_utf8_lossy` cannot fail); BINARY_FLOAT/BINARY_DOUBLE had no
decode arm at all and returned mojibake; `precision`/`scale` lost
their sign, so `FLOAT(126)` described as a decimal whose scale
exceeded its precision.

**Silent loss.** A short page was taken as end-of-stream without
consulting `has_more_rows` — reproducing one layer up the exact
defect that flag was patched into existence to report. The page
budget counted CHARACTERS where the server counts bytes and ignored
the ROWID column every page projects. A nullable cursor column
refused only at the row, which (NULLs sorting last) fired on the
final page of the first run and then never again, so those rows were
absent for the life of the pipeline. Two columns differing only in
case collapsed into one JSON key.

**Reachability.** `HR.EMPLOYEES` was quoted as ONE identifier, so no
table outside the login schema — the ordinary estate shape — could be
read; nor could a case-sensitive name. A bare `NUMBER`, which is how
Oracle spells a sequence-backed surrogate key, was rejected as a
cursor with a message claiming it was not numeric.

**Classification.** A wrong password was TRANSIENT and retried five
times per run; Oracle's default profile locks an account after ten
failed logins, so two runs would lock the user for every consumer of
it. Deterministic failures — unreadable values, over-SDU replies, a
pre-12.1 server — were all retried to exhaustion instead of failing
by name.

**Vacuity.** `ora.query` was armed above the connection, so all three
sweep cells aborted before a row moved and the assertion held
regardless; and the sweep discarded the crashed run's cursor, so
"a crash costs no rows and a resume repeats none" was satisfied by a
plain uncrashed read. Both are the 024 class.

**A guarantee that did not exist.** The crate's front-page doc
advertised consistent-SCN snapshot reads; nothing opened a
transaction, and the one that would was removed in T002 because it
poisons this driver.

**And the type declarations reached nothing at all** — every column's
exact type was derived from the describe and thrown away, so
`NUMBER(12,2)` landed as TEXT in the destination.

### Round 2 — eight defects, and the worst was round 1's own fix

D1/D2 are the round's lesson. Round 1 found that a NULL cursor value
silently SKIPPED its row; the fix kept the row — and
`unwrap_or_default()` then persisted an EMPTY watermark, which
`checked_watermark_literal` refuses on the next run. The cursor
poisoned itself. Both behaviours were wrong for the same reason: a
watermark has no representation for NULL, so neither keeping nor
dropping the row is honest. The read now REFUSES and names the three
remedies. **A fix is not a fix until it has been attacked too.**

The rest:

- **D3** the page was sized from the REQUESTED `sdu_bytes`, but the
  server negotiates the real value and the driver exposes no accessor
  for what was agreed — a larger request would size pages for packets
  that never arrive. Capped at the accepted default (512..=8192).
- **D4** `read_config`/`is_json` in the facade were cfg-gated without
  `oracle`, so `--features oracle` alone did not compile.
- **D5** `connect_timeout` never reached the driver's config path (it
  belongs to a transport the config constructor does not use) — a
  black-holed host hung forever. Enforced at the boundary instead.
- **D6** the statement budget covered queries only; LOB reads (many
  round trips) and the session pin could stall unbounded.
- **D7** the vendored `set_prefetch_rows` doc still claimed the
  multi-packet reader that was attempted and REVERTED.
- **D8** a persisted tie steered a stream whose cursor had been
  removed, starting every full read at that ROWID and skipping
  everything below it — and the full-read path never checkpoints, so
  nothing corrected it.

## STATUS

- Branch created; research committed (R1-R3); this plan written;
  T001 PROBED LIVE and the driver strategy DECIDED (above); D8-D10
  added on owner requirements (performance, LOBs, version matrix) —
  T002 probes LOB behavior + the patch difficulty + 21c XE before the
  read path freezes.
- NEXT: build in the established rhythm
  (config → client boundary (drop-and-reconnect posture) → type
  rulebook → cursor rulebook → connector/Shell) → fresh suite (kit
  LIVE, cursor wire pins, classification pins on structured codes,
  the sweep) → review rounds to terminus → gates twice clean
  (baseline 1046; counts predicted and verified; hygiene by test
  image/label only — never the dev toolbox).
