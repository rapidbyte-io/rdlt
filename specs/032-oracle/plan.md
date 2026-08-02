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

## STATUS

- Branch created; research committed (R1-R3); this plan written.
- NEXT: T001 probe gate (D2) against the live container → the driver
  strategy decision recorded → build in the established rhythm
  (config → client boundary (drop-and-reconnect posture) → type
  rulebook → cursor rulebook → connector/Shell) → fresh suite (kit
  LIVE, cursor wire pins, classification pins on structured codes,
  the sweep) → review rounds to terminus → gates twice clean
  (baseline 1046; counts predicted and verified; hygiene by test
  image/label only — never the dev toolbox).
