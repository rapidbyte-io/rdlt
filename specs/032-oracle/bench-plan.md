# 032 — BENCH CELL PLAN: `oracle-to-pg-200k`

Research + design for one new e2e matrix cell (Oracle source → Postgres
destination) in the house harness. **Nothing here is implemented and no
number here is measured** — every figure below is a PREDICTION, marked as
such, and §9 is the list of things that must be verified live before any
artifact is committed.

Governance posture up front: **SCOREBOARD, NO BAR** (benches/GOVERNANCE.md,
constitution v1.1.0, 018 BR8). The cell is measured and reported; `bars.toml`
gains nothing. §8 says why that is not merely procedural here.

---

## 1. What the house harness requires (read, not assumed)

Facts taken from the code, so the cell below fits without argument:

| Fact | Where |
|---|---|
| Cells are `*.toml` under `benches/cells/`; every `*.toml` in that dir loads, ids must be unique, unknown keys are a load error | `crates/rdlt-bench/src/cells.rs` `load_cells` |
| A cell with a `pipeline` MUST declare `[cell.verify]`, and the delivered table set must MATCH the declared set exactly — a surplus table fails the cell | `cells.rs::Cell::check`, `runner.rs::verify_outcome` |
| Verify reads the rdlt CLI's own `RunReport`, not the destination | `runner.rs::report_table_rows` |
| `fixtures[0]` is PRIMARY: it supplies `{{conn}}` / `{{data}}` / `{{port}}`; `conn` is optional (`if let Some(conn) = primary.conn()`) | `runner.rs:461`, `cells.rs::Cell::primary_fixture` |
| Every listed fixture is reset before every warmup and every counted run | `runner.rs`, `fixtures.rs::Started::reset` |
| `reset_sql` is legal ONLY on `postgres_container` (load-time refusal otherwise) | `fixtures.rs::FixtureDef::validate` |
| Generic `container` kind = image + `port:container_port` + `run_args` (pre-image) + `container_args` (post-image); readiness = **TCP connect on the host port**; teardown = `rm -f` | `fixtures.rs::bring_up_container`, `start_container` |
| `generate_sh` runs AFTER the container is up, in the fixture's data dir; `hash = [files]` blake3s them as the recorded dataset identity | `fixtures.rs::start` |
| Containers are started `--label rdlt-test=1` (the `make reclaim` convention) | `fixtures.rs::start_container` |
| Competitor variants live in `benches/competitors/*/variants.toml`, ONE flat id namespace; `self_timed_container` (dlt) or `driver` (Airbyte) | `competitors.rs` |
| A failed image build, failed `prerequisite_sh`, or non-zero driver exit ⇒ `CompetitorSide::Missing{reason}` — loud, never an error, never silent | `competitors.rs:434-600` |
| Artifact `format_version` is **3** (not 2 — the task brief's "v2" is stale; 019 bumped it) | `artifact.rs:17` |
| Default `warmups = 1`, `runs = 5` | `cells.rs` |
| Cell ids are FROZEN once recorded; new ids spell `<source>-to-<dest>-<size>` | `benches/README.md` |

Consequence worth stating: **adding `benches/cells/oracle.toml` puts the cell
into the default `TARGET=e2e make bench` run** (the runner globs the whole
cells dir; only `--filter` narrows it). The recorded matrix goes from five
cells to six, and `RESULTS.md`'s generated matrix section picks it up as soon
as an artifact exists. That is intended, but it is a change to the recorded
matrix and should be called out at the feature's close-out.

---

## 2. Blockers (read these before planning any session)

**B1 — HARD, in-repo: the release CLI cannot run an Oracle pipeline today.**
`crates/rdlt/Cargo.toml` has no `oracle` feature, `crates/rdlt/src/pipeline_spec.rs`
has no `SourceSpec::Oracle` arm, and `crates/rdlt-cli/Cargo.toml`'s feature
list is `["rest", "duckdb", "postgres", "parquet", "iceberg", "snowflake"]`.
The bench runs `target/release/rdlt run <spec>`; without facade + CLI wiring
the cell cannot execute at all. Required before the cell is even runnable:

1. `rdlt` feature `oracle = ["dep:rdlt-connector-oracle"]` + the optional dep.
2. `pipeline_spec.rs`: `#[cfg(feature = "oracle")] Oracle(OracleSourceSpec)`
   following the postgres precedent (untagged inline-document *or*
   `{config: path}`), dispatching to
   `crate::connector::oracle::source::Shell::new(config)`.
   `source::Config` is `#[non_exhaustive]` + `deny_unknown_fields` and
   `Secret` deserializes from a plain string (`secret.rs:108`), so the inline
   form needs no new types.
3. `rdlt-cli` feature list gains `"oracle"`.
4. Confirm the release build picks up `[patch.crates-io] oracle-rs = { path =
   "vendor/oracle-rs" }` (root `Cargo.toml:195`) — the three protocol fixes
   (T003) are what make paging work at all. A bench run against unpatched
   0.1.7 would silently measure the truncation defect.

**B2 — competitor, recorded not fatal: dlt's ConnectorX backend needs Oracle
Instant Client.** ConnectorX does list Oracle
(https://sfu-db.github.io/connector-x/databases/oracle.html), but its Oracle
source is the `oracle` Rust crate over **ODPI-C**, which dlopen's `libclntsh`
from Oracle Instant Client at runtime
(https://oracle.github.io/odpi/doc/installation.html). Instant Client is not
pip-installable and carries Oracle's OTN license. So the pg cells' headline
configuration (`backend="connectorx"`) does **not** transfer. dlt's arm here
runs pip-only thin mode; see §6. This exclusion must be written into the cell
`note` and RESULTS.md Caveats, because "we did not run dlt's fastest backend"
is exactly the kind of quiet handicap the governance exists to prevent. If the
owner wants it closed, the route is adding Instant Client to the competitor
image (accepting the OTN download) and adding a third arm — an owner decision,
not a default.

**B3 — competitor, likely `Missing{reason}`: Airbyte's free Oracle source is
alpha/community and untested on 23ai.**
`airbyte/source-oracle`, definition id `b39a7370-74c3-45a6-ac3a-380d48520a83`,
current tag **0.5.8**, `releaseStage: alpha`, `supportLevel: community`,
license **ELv2**, `registryOverrides.oss.enabled: true` — so it IS in the
default OSS catalog and needs **no** `create_custom` registration
(metadata.yaml on airbytehq/airbyte master; docs.airbyte.com/integrations/sources/oracle).
ELv2 restricts reselling as a competing hosted service; it does **not** require
a key or payment for self-hosted internal use. **Do not confuse it with**
`source-oracle-enterprise`
(docs.airbyte.com/integrations/enterprise-connectors/source-oracle-enterprise)
— a separate, paid, license-key-gated connector (LogMiner CDC), out of scope.

The real risk is quality, not licensing: the docs state "tested with Oracle
11g, 12c, 18c, 19c, and 21c" — **23ai / `gvenzl/oracle-free` is not in that
list**, and the docs page carries a "Low" sync-success health badge. Nothing
documents a 23ai failure either; it is simply unverified. Plan for both
outcomes: setup.py's per-cell try/except already records a failing connector's
reason into `state.json`, and driver.py turns that into `Missing{reason}`.
An Airbyte arm that cannot read 23ai is a documented absence, not a reason to
switch the fixture (switching would break same-conditions — see §4).

**B4 — design, and it may cost rdlt the cell: the SDU ceiling.** The connector's
read path pages by ROWID keyset and sizes each page from the DESCRIBED column
widths so one reply fits ONE 8 KB SDU packet
(`source/read.rs`, `source/schema.rs::rows_per_page`,
`DEFAULT_SDU_BYTES = 8192`, `PAGE_BUDGET = 0.55`); the multi-packet query read
was attempted and **reverted on evidence** (plan.md T003), leaving "one query
reply must fit one SDU" as the standing limit. Competitors do not share it:
Airbyte's source-oracle is JDBC thin, and python-oracledb thin does multi-packet
reads with a tunable `arraysize`. **This cell can legitimately be a LOSS for
rdlt.** That is fine and it is precisely why it is scoreboard-only (018: "cells
at parity or behind carry no bar — the matrix reports them as they are"). The
value of the cell is that it puts a number on the ceiling the resumable-parser
work would move. Anyone reading the row must read it as *"rdlt v1's
SDU-bounded paging vs competitors' streaming reads"*, not as an engine
comparison — that sentence belongs in the cell `note`.

---

## 3. Sizing: why N = 200,000

Two independent constraints, and the binding one is **not** Oracle.

**(a) rdlt's rows-per-round-trip is derivable in advance.** `rows_per_page`
sums each column's DECLARED width + 2 bytes, and divides `8192 × 0.55 = 4505`.
For the proposed `EVENTS` mirror (§5) the declared widths Oracle reports are:
NUMBER → 22 regardless of precision (×5 = 110), BINARY_DOUBLE → 8,
VARCHAR2(64|16|36|32) → 148, TIMESTAMP WITH TIME ZONE → 13, DATE → 7; total
286, plus 2×12 overhead = 310, plus the ROWID column the reader adds (~18+2)
≈ **330 bytes/row → ≈ 13 rows per round trip** (PREDICTED; §9 verifies it).

So N rows cost ≈ N/13 sequential round trips. At a plausible localhost
Oracle statement round trip of 0.5–2 ms:

| N | pages | PREDICTED rdlt wall | ×6 (1 warmup + 5 runs) |
|---|---|---|---|
| 200k | ~15,400 | 8–31 s | 1–3 min |
| 1M | ~77,000 | 40–155 s | 4–15 min |

At 1M the rdlt arm alone can eat a quarter-hour before dlt's arm starts, on a
matrix that already asks for a quiet machine and a whole recorded session.
200k keeps the cell inside the band the existing 200k cells occupy.

**(b) Oracle Free's caps are not the constraint.** Free is capped at 2 CPU
threads / 2 GB RAM / 12 GB user data. 200k × 12 columns is ~35–45 MB — three
orders of magnitude inside the data cap — and a single
`INSERT … SELECT … FROM DUAL CONNECT BY LEVEL <= 200000` seeds it in seconds
(the connector's own suite already uses that shape:
`tests/cases/test_live.rs:112`, `tests/crash_sweep.rs:61`). 1M would also fit;
it is the round-trip count, not Oracle, that rules it out.

**Decision: N = 200,000.** It matches the existing `s3jsonl-*-200k` cells'
size band, keeps a run in the seconds, and leaves an obvious upgrade path:
when the resumable parser lands and pages stop being SDU-bounded, a
**new** cell id (`oracle-to-pg-1m`) records that — ids are frozen, so the
200k row stays comparable across time rather than being silently redefined.

Keep `warmups = 1`, `runs = 5` (house default) **unless** the measured wall
exceeds ~60 s/run, in which case drop to `runs = 3` and record the reason in
the cell comment, the way the Airbyte variant already does.

---

## 4. The fixture

**Reuse the connector suite's exact image and tag** — one Oracle version for
the gate and the bench:
`docker.io/gvenzl/oracle-free:23.26.2-slim-faststart`
(`tests/cases/common.rs::IMAGE_TAG`; research.md R2 records the multi-arch
digest `sha256:0489e0c1f20b2ca632075653c66f284234689ccff62c9a39809d9a5b3e7c1642`
should the house prefer Polaris-style digest pinning). `slim-faststart`: the
datafiles ship pre-expanded, which is the right trade when every run is a
first boot. Pull ~1.33 GB compressed — the same order as RUSTFS/Polaris.

**Kind: the generic `container` kind — no harness change needed.** This is
the RUSTFS pattern exactly (container + `generate_sh` calling a script under
`benches/fixtures/`), and it avoids adding a `FixtureKind::OracleContainer`
plus a bring-up function to `fixtures.rs`.

The one wrinkle it creates and how it is handled: generic-container readiness
is `wait_tcp(port)`, and Oracle's listener accepts TCP long before the PDB is
open — a connect in that gap yields ORA-12514. So `wait_tcp` passing means
nothing here, and **the real readiness probe lives at the top of
`seed_oracle.sh`**, mirroring what the connector's own fixture does
(`common.rs::await_service`: poll a real query, not the log line):

```
# benches/fixtures/seed_oracle.sh  (sketch — not implemented)
#   $1 container name  $2 host port  $3 app user  $4 password  $5 rows  $6 identity-out
# 1. wait for "DATABASE IS READY TO USE!" in `podman logs`, 300 s budget
# 2. THEN poll `SELECT 1 FROM DUAL` as APP_USER against //127.0.0.1:$2/FREEPDB1
#    via `podman exec -i <name> sqlplus -S ...` until it answers (the log line
#    says the CDB opened, not that FREEPDB1 registered with the listener)
# 3. CREATE TABLE EVENTS + one CONNECT BY insert of $5 rows + COMMIT
# 4. print the identity block (count + order-independent hash) to $6
```

Registry entry:

```toml
# benches/fixtures/fixtures.toml  (addition)

# One Oracle Database 23ai Free (gvenzl/oracle-free, pinned to the SAME tag the
# connector's live suite uses). Seeded once per session with EVENTS 200k×12 —
# the Oracle mirror of the pg `events` shape, so the two source cells differ in
# the SOURCE, not the data. Read-only: no reset seam (the pg fixture's
# reset_dest_schemas covers every destination this cell writes).
#
# Readiness is NOT the harness's TCP probe: Oracle's listener accepts long
# before FREEPDB1 registers (ORA-12514 in the gap), so seed_oracle.sh does the
# real wait — log line, THEN a live `SELECT 1 FROM DUAL` — before it seeds.
[[fixture]]
id = "oracle"
kind = "container"
image = "docker.io/gvenzl/oracle-free:23.26.2-slim-faststart"
port = 15210
container_port = 1521
run_args = [
  "-e", "ORACLE_PASSWORD=rdlt-bench-sys",
  "-e", "APP_USER=RDLT",
  "-e", "APP_USER_PASSWORD=rdlt-bench",
]
generate_sh = "sh {{benches}}/fixtures/seed_oracle.sh rdlt-bench-oracle 15210 RDLT rdlt-bench 200000 {{data}}/oracle-identity.txt"
hash = ["{{data}}/oracle-identity.txt"]
```

Notes on that entry:

- **Port 15210** — free, and it reads as 1521 in the house's five-digit style
  (5439 pg, 19110 rustfs).
- **No `conn`** — the rdlt source document takes discrete `host`/`port`/
  `service` fields, and the dlt arm needs a SQLAlchemy URL. Two consumers, two
  formats; overloading one `{{conn}}` string for both would be worse than
  writing each where it is used. `runner.rs` handles an absent `conn` already.
- **No `reset_sql`** — it is refused on non-postgres kinds at load time
  (`fixtures.rs::validate`), and correctly so: the Oracle side is read-only.
- **Container name is deterministic** (`rdlt-bench-oracle`), which is what lets
  `seed_oracle.sh` `podman exec` into it — the same assumption the Airbyte
  driver already makes about `rdlt-bench-pg`.
- **Startup is the slowest thing in the matrix** — 15–40 s typical for
  slim-faststart, budget 120–300 s on a loaded machine. Fixtures are shared
  across cells within one invocation, so it is paid once per session.
- **Volume hygiene, learned from 029:** fixture teardown is `rm -f` without
  `-v`. If this image declares a `VOLUME` for `/opt/oracle/oradata`, every run
  leaks an anonymous volume, and 1,988 of those at the 2,048-lock ceiling is
  exactly what wrecked the 029 session. §9 makes checking this a gate item;
  `podman volume prune` before a session either way, and note that
  `make reclaim` does **not** sweep volumes.

---

## 5. The seeded dataset

Mirror `benches/fixtures/seed_pg.sql`'s `events` column-for-column so the two
source cells differ in the source, not the payload. Deterministic (no
`DBMS_RANDOM`, no `SYSDATE`), so every session seeds a byte-identical table.

```sql
CREATE TABLE EVENTS (
  ID         NUMBER(19)               NOT NULL PRIMARY KEY,
  SMALL      NUMBER(10)               NOT NULL,
  BIG        NUMBER(19)               NOT NULL,
  RATIO      BINARY_DOUBLE            NOT NULL,
  AMOUNT     NUMBER(12,4)             NOT NULL,
  NAME       VARCHAR2(64)             NOT NULL,
  CODE       VARCHAR2(16)             NOT NULL,
  ACTIVE     NUMBER(1)                NOT NULL,
  CREATED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
  BIRTHDAY   DATE                     NOT NULL,
  TOKEN      VARCHAR2(36)             NOT NULL,
  NOTE       VARCHAR2(32)
);

INSERT INTO EVENTS
SELECT LEVEL,
       MOD(LEVEL, 100000),
       LEVEL * 2654435761,
       LEVEL / 3,
       MOD(LEVEL, 99999999) / 10000,
       'user-' || LEVEL,
       'C-' || LPAD(TO_CHAR(MOD(LEVEL, 65536)), 6, '0'),
       CASE WHEN MOD(LEVEL, 3) = 0 THEN 1 ELSE 0 END,
       FROM_TZ(TIMESTAMP '2026-01-01 00:00:00' + NUMTODSINTERVAL(MOD(LEVEL, 86400), 'SECOND'), 'UTC'),
       DATE '1970-01-01' + MOD(LEVEL, 20000),
       LOWER(RAWTOHEX(STANDARD_HASH('token-' || LEVEL, 'MD5'))),
       CASE WHEN MOD(LEVEL, 10) = 0 THEN NULL ELSE 'note-' || MOD(LEVEL, 1000) END
FROM DUAL CONNECT BY LEVEL <= 200000;
COMMIT;

-- Recorded dataset identity (order-independent, printed to the hash file):
SELECT COUNT(*), SUM(ORA_HASH(ID || '|' || NAME || '|' || AMOUNT || '|' || NVL(NOTE,'~'))) FROM EVENTS;
```

Type-mapping notes, tied to plan.md D4:

- Column names are kept identical to pg's `events`; none of them is an Oracle
  reserved word, and the connector emits quoted-UPPERCASE anyway (D5). VERIFY
  the `CREATE TABLE` parses unquoted at seed time (§9) — the seed script is
  the one place a reserved-word collision would bite.
- `ACTIVE NUMBER(1)` rather than 23ai `BOOLEAN`: keeps the table readable by
  the 21c XE leg (plan D10) and by Airbyte's connector, which predates 23ai
  BOOLEAN. Costs the cell nothing — it is a source-read benchmark.
- **No LOB column.** LOBs are read through a separate per-value locator path
  (D9 / T002) whose cost is a different measurement entirely; mixing it into a
  throughput cell would make the number uninterpretable. A LOB cell is a
  separate, later cell if the owner wants one.
- `token` is `VARCHAR2(36)` holding hex, not a UUID type (Oracle has none).

---

## 6. The cell

```toml
# benches/cells/oracle.toml   (new file — the cells dir globs, so this joins
# the default `TARGET=e2e make bench` run)

# ---------------------------------------------------------------------------
# oracle-to-pg-200k — Oracle 23ai Free → Postgres, 200k rows, full replace.
# ---------------------------------------------------------------------------
[[cell]]
id = "oracle-to-pg-200k"
# oracle is PRIMARY (supplies {{data}}); the postgres destination is addressed
# by database name at its fixed port, as every cross-store cell does. The pg
# fixture's reset recreates dest_rdlt/dest_dlt/dest_airbyte before every run.
fixtures = ["oracle", "pg"]
pipeline = "cells/pipelines/oracle-to-pg.yaml"
note = "Oracle 23ai Free → Postgres, 200k×12 rows, full replace — the legacy-relational extract. rdlt's v1 Oracle read is SDU-bounded (one query reply must fit one 8 KB packet, so ~13 rows per round trip on this shape); the competitor arms stream multi-packet. Read this row as v1's paging ceiling, not as an engine comparison. dlt runs python-oracledb THIN mode: its connectorx backend needs Oracle Instant Client (ODPI-C), which is not pip-installable, so it is deliberately not run — recorded, not hidden."
[cell.verify]
events = 200_000

# dlt: sql_database over SQLAlchemy's oracle+oracledb dialect, thin mode.
# The backend is an A/B decided by measurement before recording (pyarrow vs
# the sqlalchemy default) — whichever is faster is dlt's honest fastest here.
[[cell.competitor]]
variant = "dlt"
args = [
  "pipeline_oracle_pg.py",
  "oracle+oracledb://RDLT:rdlt-bench@127.0.0.1:15210/?service_name=FREEPDB1",
  "postgresql://postgres:postgres@127.0.0.1:5439/dest_dlt",
  "pyarrow",
  "EVENTS",
  "RDLT",
]
network = "host"

# Airbyte: driver.py drives the pre-created connection (setup.py). Full-refresh
# overwrite. airbyte/source-oracle is alpha/community and documented as tested
# only through 21c — an arm that cannot read 23ai records Missing{reason}.
[[cell.competitor]]
variant = "airbyte"
args = ["oracle-to-pg-200k", "200000"]
```

`[cell.workload]` is free-form and recorded verbatim into the artifact — use
it to make the number interpretable rather than mysterious:

```toml
[cell.workload]
rows = 200_000
columns = 12
oracle_image = "gvenzl/oracle-free:23.26.2-slim-faststart"
sdu_bytes = 8192
rows_per_page = 13          # MEASURED, filled after §9.3 — not guessed
read_strategy = "rowid-keyset, one page per round trip, SET TRANSACTION READ ONLY"
```

### The rdlt arm's pipeline

```yaml
# benches/cells/pipelines/oracle-to-pg.yaml
# 032 cell oracle-to-pg-200k: Oracle 23ai Free (RDLT.EVENTS, 200k×12) →
# postgres destination (dest_rdlt), full replace. Fixtures [oracle, pg]:
# oracle is primary; the postgres destination is addressed by its database name
# on the pg fixture's server (fixed port 5439). The Oracle source document is
# INLINE (the postgres-source precedent) — discrete host/port/service fields,
# so there is no {{conn}} to substitute.
pipeline: bench-oracle-to-pg
workdir: {{workdir}}/.rdlt
write_mode: replace
source:
  oracle:
    host: 127.0.0.1
    port: 15210
    service: FREEPDB1
    user: RDLT
    password: rdlt-bench
    streams:
      - name: events
        table: EVENTS
destination:
  postgres:
    conn: "host=127.0.0.1 port=5439 user=postgres password=postgres dbname=dest_rdlt"
    dataset: bench
```

`verify` declares exactly `events = 200_000`; the stream name (`events`) is
what the engine reports as the table, and a surplus table fails the cell.

### The dlt arm

**It can run.** python-oracledb runs **Thin mode by default — no Instant
Client, no Oracle client libraries** (python-oracledb.readthedocs.io
installation guide; oracle.github.io/python-oracledb). SQLAlchemy 2.x ships
the Oracle dialect in core with `oracle+oracledb://` selecting that DBAPI, and
thin is the default there too (docs.sqlalchemy.org/en/20/dialects/oracle.html).
dlt's `sql_database` is SQLAlchemy-based and dlt publishes an Oracle→Postgres
how-to page. `cx_Oracle`, by contrast, *does* require Instant Client
(cx-oracle.readthedocs.io) and is legacy — not used.

Versions checked on PyPI: `dlt` 1.29.1 (2026-07-24), `oracledb` 4.0.2
(2026-07-14), `SQLAlchemy` 2.0.51 (2026-06-15). dlt's `pyproject.toml` extras
are `sql_database = ["sqlalchemy>=1.4"]` and `postgres = ["psycopg2-binary"]`
— **there is no `dlt[oracle]` extra**; the driver is installed separately.

Dockerfile change (`benches/competitors/dlt/Dockerfile`) — one added dep and
one added script, **dlt's own version pin unchanged at 1.29.0** so the
`pin = "dlt 1.29.0"` string in `variants.toml` stays true:

```dockerfile
RUN pip install --no-cache-dir "setuptools<81" \
    "dlt[filesystem,postgres,sql_database]==1.29.0" \
    pyarrow connectorx pandas s3fs \
    oracledb==4.0.2
...
COPY ... pipeline_oracle_pg.py ./
```

The image content changes, which strictly speaking changes the environment the
existing five cells' dlt arms run in. Adding a wheel that no existing script
imports should not move their numbers; the conservative call, and the one the
version policy implies, is to **re-run the full matrix in the same session**
rather than splice one new row into an older recording.

`benches/competitors/dlt/pipeline_oracle_pg.py` (shape, following
`pipeline_pg_pg.py` exactly — same argv style, same last-line JSON contract):

```python
"""Baseline: Oracle table -> Postgres via pinned dlt's sql_database source
(032 cell oracle-to-pg-200k). Thin-mode python-oracledb through SQLAlchemy's
oracle+oracledb dialect: NO Oracle Instant Client. connectorx is deliberately
not offered here — its Oracle source is ODPI-C and needs Instant Client.

Usage: pipeline_oracle_pg.py <sqlalchemy_oracle_url> <dest_pg_url> <backend>
                             [table] [schema]
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb, backend, table.
"""
import json, resource, sys, time
import dlt
from dlt.sources.sql_database import sql_database

src, dest = sys.argv[1], sys.argv[2]
backend = sys.argv[3] if len(sys.argv) > 3 else "pyarrow"
table   = sys.argv[4] if len(sys.argv) > 4 else "EVENTS"
schema  = sys.argv[5] if len(sys.argv) > 5 else "RDLT"

source = sql_database(credentials=src, schema=schema,
                      table_names=[table], backend=backend)

started = time.monotonic()
pipe = dlt.pipeline(pipeline_name=f"bench_oracle_pg_{backend}",
                    destination=dlt.destinations.postgres(dest),
                    dataset_name="bench", dev_mode=False)
pipe.run(source, write_disposition="replace")
elapsed = time.monotonic() - started

with pipe.sql_client() as client:
    rows = client.execute_sql("SELECT count(*) FROM bench.events")[0][0]
print(json.dumps({"rows": rows, "seconds": elapsed,
                  "rows_per_s": rows / elapsed,
                  "peak_rss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                  "backend": backend, "table": table}))
```

Two Oracle-specific things to settle live (§9), not by reading:

- **Identifier case.** Oracle folds unquoted names UPPERCASE; SQLAlchemy's
  oracle dialect normalizes UPPERCASE-only names to lowercase on the Python
  side and re-uppercases outbound, so `table_names=["EVENTS"]` vs `["events"]`
  and the landed dest table name (`bench.events` vs `bench."EVENTS"`) are both
  empirical. The `SELECT count(*)` above assumes `bench.events`.
- **NUMBER typing.** dlt used to assume every Oracle `NUMBER` is a
  float/decimal, which broke large integers under the pyarrow backend
  (dlt issue #3133, fixed by PR #3144, 2025-09-24 — inside 1.29.x). Our `BIG`
  column is `LEVEL * 2654435761`, up to ~5.3e14 — comfortably inside float64's
  exact-integer range, so it should not trip even an old path, but confirm the
  landed values match rather than assuming.

### The Airbyte arm

`airbyte/source-oracle` 0.5.8 is in the default OSS catalog, so **no
`create_custom` registration is needed** — the existing `setup.py` flow works
as-is (it creates sources via `POST /api/public/v1/sources` with a
`sourceType` discriminator in `configuration`, then discovers, then creates the
connection). Wiring, all inside `benches/competitors/airbyte/setup.py`:

```python
ORACLE_PORT = os.environ.get("AB_ORACLE_PORT", "15210")
ORACLE_USER = os.environ.get("AB_ORACLE_USER", "RDLT")
ORACLE_PASS = os.environ.get("AB_ORACLE_PASS", "rdlt-bench")

def oracle_source_config():
    return {
        "sourceType": "oracle",                 # VERIFY the exact string live
        "host": POD_HOST, "port": int(ORACLE_PORT),
        "connection_data": {"connection_type": "service_name",
                            "service_name": "FREEPDB1"},
        "username": ORACLE_USER, "password": ORACLE_PASS,
        "schemas": [ORACLE_USER],               # case-sensitive; upper-cased user
        "encryption": {"encryption_method": "unencrypted"},
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
    }
```

Spec facts verified from the connector's `spec.json` on airbytehq/airbyte
master + the docs page: `connection_data` is a `oneOf` of
`{"connection_type":"service_name","service_name":…}` /
`{"connection_type":"sid","sid":…}`; `encryption` is a `oneOf` of
`unencrypted` / `client_nne` (`encryption_algorithm` AES256|RC4_56|3DES168) /
`encrypted_verify_certificate` (needs `ssl_certificate`); `schemas` is an array
(minItems 1) that "defaults to upper-cased username"; `jdbc_url_params` is an
optional string; default port 1521. `tunnel_method` is documented on the docs
page but did not render in the fetched spec extraction — its shape above is
the standard JDBC-base form and is INFERRED, so drop the key if setup rejects it.

Cell entry in `CELLS`:

```python
{
    "id": "oracle-to-pg-200k",
    "source": ("oracle", None), "destination": ("pg", DEST_DB),
    "stream": "EVENTS",                      # VERIFY against discover output
    "sync_mode": "full_refresh_overwrite",
    "verify": pg_verify("EVENTS", 200_000),  # VERIFY the landed dest table name
},
```

plus an `oracle` branch in `build_source`. `driver.py` needs **no change** —
it is cell-generic (state.json → trigger → poll → verify → summary JSON).
`variants.toml` and `prerequisite_sh` also unchanged.

One setup-path change: `benches/bench-setup.sh`'s Airbyte leg brings up
throwaway `rdlt-bench-pg` + `rdlt-bench-rustfs` so `discover` has schemas to
read. It must also bring up a throwaway Oracle on 15210 with the `EVENTS`
table (schema only — **do not seed 200k rows for discover**; it reads
metadata) and add 15210 to its in-use-port pre-check. Budget ~40–90 s for the
boot in that script's timeouts.

Pods reach host fixtures at `169.254.1.2` (pasta) — `POD_HOST` already carries
that; the Oracle listener must be reachable there on 15210, which is the same
published-port mechanism the pg fixture already relies on.

If any of this fails — connector not in the deployed catalog, discover failing
against 23ai, sync erroring — setup.py records the reason and the arm reports
`Missing{reason}`. That is the correct outcome, not a fallback to a different
Oracle version: switching the Airbyte arm to a 21c XE container would give it
a *different source server* and break the same-conditions rule the whole matrix
rests on.

---

## 7. Files touched (nothing here is written yet)

| File | Change |
|---|---|
| `crates/rdlt/Cargo.toml` | `oracle` feature + optional dep (B1) |
| `crates/rdlt/src/pipeline_spec.rs` | `SourceSpec::Oracle` arm + dispatch (B1) |
| `crates/rdlt-cli/Cargo.toml` | add `"oracle"` to the rdlt feature list (B1) |
| `benches/fixtures/fixtures.toml` | new `oracle` fixture (§4) |
| `benches/fixtures/seed_oracle.sh` | NEW — readiness probe + seed + identity (§4/§5) |
| `benches/cells/oracle.toml` | NEW — the cell (§6) |
| `benches/cells/pipelines/oracle-to-pg.yaml` | NEW — the rdlt arm (§6) |
| `benches/competitors/dlt/Dockerfile` | `oracledb==4.0.2` + COPY the new script |
| `benches/competitors/dlt/pipeline_oracle_pg.py` | NEW — the dlt arm (§6) |
| `benches/competitors/airbyte/setup.py` | oracle source config + CELLS entry + `build_source` branch |
| `benches/bench-setup.sh` | throwaway Oracle for the Airbyte discover leg |
| `benches/README.md` | the matrix is six cells; the connectorx-exclusion caveat |
| `benches/RESULTS.md` | Caveats entry (SDU ceiling, connectorx exclusion, Airbyte alpha) |
| `benches/bars.toml` | **UNCHANGED** — no bar (§8) |

---

## 8. Governance posture

**Scoreboard. No bar. Not now, and not after one session.**

- Constitution v1.1.0 and 018 BR8: bars are measurement-first — a new cell is
  scoreboard until a recorded three-way session exists, and then at most one
  bar per cell, set below its cited session floor, with a RESULTS.md policy-log
  entry. `bars.toml` is currently four bars over five cells and its header
  states the rule explicitly.
- Beyond the rule, the substance: B4 means this cell may well be a **loss**.
  018's own precedent handles that — `pg-to-s3parquet-1m` carries no bar
  because "one session on a newly-comparable cell is not a basis for a bar",
  and the withdrawn "0.9× dedup loss" is the standing warning against reading
  one number as a verdict. A cell whose rdlt arm is architecturally capped
  should never carry a ratio bar; it should carry a `note` that explains the
  cap and a number that the parser work can be measured against.
- `cross_validate` only requires a bar to name an existing cell, so nothing
  structural forces the issue. Leave it alone.
- **Competitor arms record `Missing{reason}` — never silently skipped.** Both
  competitor paths already do this (`competitors.rs:434-600`); the plan above
  keeps that honest by making the Airbyte failure mode a recorded absence
  rather than a substituted fixture.
- **Version policy**: bumping a competitor pin means re-measuring every cell
  before quoting a multiple. Adding `oracledb` to the shared image does not
  bump dlt's pin, but it does change the image — re-run the whole matrix in
  one session rather than splicing a row.

---

## 9. VERIFY LIVE before any number is recorded

Nothing below is measured. Each item is a gate on committing an artifact.

**Wiring**
1. B1 landed: `cargo build --release -p rdlt-cli` with `oracle`, and
   `target/release/rdlt run benches/cells/pipelines/oracle-to-pg.yaml` runs
   end to end by hand before the harness ever touches it.
2. The release binary is built against `vendor/oracle-rs` (the patched driver),
   not published 0.1.7 — check `cargo tree -p oracle-rs` / the lock. An
   unpatched build would measure the truncation defect as if it were throughput.

**Fixture**
3. The fixture boots under `cargo run -p rdlt-bench -- run --filter
   'oracle-to-pg-200k'`, and `seed_oracle.sh`'s readiness probe genuinely
   closes the ORA-12514 window (kill it once mid-boot and confirm the failure
   is a clean message, not a hung run).
4. The `CREATE TABLE` parses unquoted — no column name collides with an Oracle
   keyword at seed time.
5. The seed identity hash is **identical across two sessions** (that is the
   whole point of the `hash` field).
6. **Does the image declare a `VOLUME`?** `podman image inspect` it. Teardown
   is `rm -f` without `-v`, so if it does, each run leaks an anonymous volume
   — the 029 flake amplifier. Run `podman volume prune` before the session
   regardless; `make reclaim` does not sweep volumes.
7. Startup + seed cost measured once, and the harness's port-15210 assumption
   holds under rootless podman.

**rdlt arm**
8. Measured `rows_per_page` for this exact table (log it or derive it from the
   round-trip count) — the §3 prediction of ~13 is arithmetic, not a
   measurement, and the whole sizing argument rests on it.
9. Per-run wall is in the 10–60 s band. If it is not, re-choose N **before**
   recording anything, not after.
10. `verify` passes with the delivered set exactly `{events: 200000}` — no
    surplus table.
11. Two clean sessions, quiet guard un-forced (`forced: false` in the artifact).

**dlt arm**
12. The image builds with `oracledb==4.0.2` and the five existing cells still
    pass unchanged in the same session.
13. Thin mode actually connects — no `libclntsh` anywhere in the container.
14. **Backend A/B**: `pyarrow` vs the `sqlalchemy` default, same session,
    interleaved. Whichever wins is what the `dlt` arm runs, and the loser's
    number goes in the session record so the choice is evidenced.
15. Identifier case: which of `EVENTS`/`events` reflection accepts, and what
    the landed dest table is actually called (the script's `SELECT count(*)
    FROM bench.events` depends on it).
16. Landed values match the source for `BIG` and `AMOUNT` (the NUMBER-typing
    issue, dlt #3133).
17. Rowcount 200,000 in `dest_dlt.bench.*`.

**Airbyte arm**
18. `airbyte/source-oracle` is present in the *deployed* abctl version's
    catalog, and the exact `sourceType` string the public API expects.
19. `discover` succeeds **against 23ai** — the documented tested range stops
    at 21c. This is the single most likely `Missing{reason}`.
20. The discovered stream name and namespace (`EVENTS` vs `RDLT.EVENTS`) and
    the resulting destination table name, so `pg_verify` checks the right one.
21. Pods reach `169.254.1.2:15210`.
22. `full_refresh_overwrite` completes and `rowsSynced == 200000`.
23. If any of 18–22 fails: the arm records `Missing{reason}` and the matrix
    runs two-way. Do **not** substitute a 21c fixture for this arm.

**Session**
24. Two clean sessions, then RESULTS.md regenerated via `TARGET=report make
    bench`; Caveats gain the SDU-ceiling sentence, the connectorx-exclusion
    sentence, and the Airbyte-alpha sentence.
25. `bars.toml` untouched; `TARGET=gate make bench` still passes on the other
    four bars.

---

## 10. Sources

**dlt / Oracle**: docs.sqlalchemy.org/en/20/dialects/oracle.html;
python-oracledb.readthedocs.io installation (thin mode is the default, no
client libraries); cx-oracle.readthedocs.io installation (Instant Client
required); dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database;
dlthub.com Oracle→Postgres how-to; dlt `pyproject.toml` on dlt-hub/dlt devel
(extras); PyPI dlt 1.29.1 / oracledb 4.0.2 / SQLAlchemy 2.0.51;
sfu-db.github.io/connector-x/databases/oracle.html + oracle.github.io/odpi
installation (ConnectorX→ODPI-C→Instant Client); dlt issue #3133 / PR #3144
(NUMBER typing).

**Airbyte**: `airbyte-integrations/connectors/source-oracle/metadata.yaml` and
`spec.json` on airbytehq/airbyte master; docs.airbyte.com/integrations/sources/oracle;
docs.airbyte.com/integrations/enterprise-connectors/source-oracle-enterprise;
docs.airbyte.com/platform/operator-guides/using-custom-connectors;
reference.airbyte.com/reference/createsource; hub.docker.com/r/airbyte/source-oracle/tags;
airbyte issue #68608 (public-API custom-definition hang on OSS 1.8.5 — not
needed here, since source-oracle ships in the default catalog).

**In-repo**: `crates/rdlt-bench/src/{cells,fixtures,competitors,runner,artifact}.rs`;
`benches/{GOVERNANCE.md,README.md,bars.toml,bench-setup.sh}`;
`benches/cells/e2e.toml`; `benches/fixtures/{fixtures.toml,seed_pg.sql}`;
`benches/competitors/{dlt,airbyte}/*`; `specs/032-oracle/{plan.md,research.md}`;
`crates/rdlt-connector-oracle/src/source/{config,read,schema}.rs`;
`crates/rdlt-connector-oracle/tests/cases/common.rs`; `specs/018-bench-refinement/plan.md`.
