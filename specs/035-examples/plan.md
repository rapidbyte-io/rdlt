# 035 — THE EXAMPLES SUITE

Owner goal: every connector visible and testable from `examples/` —
FULL configuration per connector (basics active, the rest commented
with real values), compose files for everything containerisable, so
"how do I use rdlt fully" is answered by reading and running, not by
reading source.

## D1 — One example per (source, destination) pair that TEACHES

Not a matrix of all pairs (20 pipelines nobody reads); a set where
every connector appears at least once on its natural side, and each
example is the canonical place to see ONE connector's full vocabulary:

| example | source (full vocab shown) | destination (full vocab shown) | container |
|---|---|---|---|
| pokemon-to-jsonl | **rest** | file/jsonl | none |
| oracle-to-jsonl | **oracle** | file/jsonl | oracle-free |
| postgres-to-parquet | **postgres** | **file/parquet** | postgres |
| jsonl-to-postgres | **file (source)** | **postgres** | postgres |
| csv-to-duckdb | file/csv (join lattice) | **duckdb** | none |
| postgres-to-iceberg | postgres (basics) | **iceberg** | postgres + polaris + rustfs |
| jsonl-to-snowflake | file/jsonl (basics) | **snowflake** | none (SaaS) |

Bold marks where a connector's FULL vocabulary lives; a connector
appearing twice shows basics the second time with a pointer.

## D2 — Compose, pinned to the images the gates already trust

`compose.yaml` per example directory (the modern canonical name; both
`docker compose` and `podman-compose` read it). Images are EXACTLY
the pins the test fixtures run:

- postgres: `docker.io/library/postgres:16-alpine`
- oracle: `docker.io/gvenzl/oracle-free:23.26.2-slim-faststart`
- polaris: `docker.io/apache/polaris@sha256:5b574ce5…` (by DIGEST —
  upstream publishes no stable tag; this digest is what every
  recorded gate ran against)
- rustfs: `docker.io/rustfs/rustfs:1.0.0-beta.11`

Seed data ships as `initdb` SQL mounted into the containers, so
`compose up` alone produces a queryable source. Polaris needs a
bootstrap (catalog + grants); the crate's stdlib-only
`polaris_bootstrap.py` is adapted into the example and run as a
one-shot compose service, with the dual-endpoint design it already
has (the catalog stores the host-reachable S3 endpoint; the bootstrap
reaches RUSTFS over the compose network).

Fixed ports, chosen high to avoid collisions with dev services, and
stated in each README.

## D3 — "Full configuration" means the CONFIG STRUCTS, not prose

Every field of every connector's config appears in its designated
example: active where the example needs it, commented with a REAL
plausible value and a one-line meaning everywhere else. The source of
truth is the serde structs (wire spellings, enum variants, defaults);
compiled from the code, not from memory. Refusal rules worth knowing
(service XOR sid, zero thresholds, budget-below-target) appear as
comments where the field is shown.

Pipeline-LEVEL vocabulary (write_mode variants, batch_policy,
commit_policy, `config:` path vs inline) is spread deliberately:
each concept is ACTIVE in at least one example and cross-referenced
in the README's coverage table.

## D4 — Examples are load-bearing: parsed by the gate, run before commit

- A new test (`crates/rdlt/tests/examples.rs`) parses EVERY
  `examples/*/pipeline.yaml` through the real Spec parser — comment
  rot in an active block fails the gate. It also asserts the example
  set and the README's coverage table name the same pipelines.
- Every example is EXECUTED before landing, via its own compose file
  where one exists, with the row counts recorded in the README (the
  030-established discipline). Snowflake runs against the owner's
  credential entries with placeholders shipped; oracle needs Instant
  Client at runtime (fetched for verification, not vendored).
- Commented blocks cannot be parse-tested as written; the examples
  test therefore also UNCOMMENT-parses each pipeline's
  `# yaml-option:`-marked lines to hold the commented vocabulary to
  the same standard. (Marker spelling decided at build time.)

## D5 — Verification tooling on this machine

No docker; `podman-compose` 1.6.0 (installed to ~/.local/bin) drives
the compose files verbatim — probed end to end with a postgres
service before this plan was written. The shipped files remain plain
compose: users with docker run `docker compose up -d` unchanged.

## Build order

1. Vocabulary inventory from the config structs (in flight).
2. postgres-to-parquet + jsonl-to-postgres (+ shared-shape compose,
   seed SQL) — the postgres pair proves the compose discipline.
3. csv-to-duckdb (no container — quick win, csv join lattice).
4. oracle-to-jsonl gains compose + seed; verified live with Instant
   Client.
5. postgres-to-iceberg (compose trio + bootstrap).
6. jsonl-to-snowflake (full vocab, live-verified, placeholders ship).
7. pokemon-to-jsonl extended to the full REST vocabulary.
8. The examples test + README rewrite (matrix, coverage table,
   compose quickstart).
9. Gate.

Each stage's example is run before the next begins; no example lands
unexecuted except where the README states exactly what was not run
and why.

---

## Build record (2026-08-03)

All seven examples built and EXECUTED as written; six verified on
this machine end to end, snowflake verified against the owner's
account via a credential-substituted copy that never touched the tree
(placeholders ship). Verified results are in `examples/README.md`'s
matrix; each also proves idempotence with a second run.

### The enforcement test earned its keep immediately

`crates/rdlt/tests/examples.rs` holds three properties: every
pipeline PARSES AND BUILDS through the real Spec gate (the duckdb
shell opens its database at build, so builds run under a temp working
directory); the reference map and the example directories agree; and
each connector's reference example mentions EVERY property of its
config schema. The coverage check found real gaps on its first run —
oracle's `tuning` was missing `batch_rows` and `statement_cache`, the
SQL-destination examples were missing the scd2 family, iceberg's
parquet/parts blocks were partial. All closed.

### What execution taught that reading would not have

- **The old "switch to merge" advice was never executable**: the file
  destination REFUSES `write_mode: merge` (files cannot update a row
  in place). The oracle example now demonstrates the pairing that IS
  real for a file destination — `append` + cursor (250 rows, then 0)
  — and the merge demo lives on SQL destinations.
- **Replace never applies a cursor** (engine rule: a mirror rebuilt
  from only-the-new-rows would lose every old one). The
  postgres-to-parquet example first shipped an ACTIVE cursor under
  replace that filtered nothing; it is commented with the rule stated.
- **Append without a cursor duplicates on re-run** — correct
  semantics, wrong example hygiene. The iceberg example gained
  cursors on both streams; run 2 reads 0.
- **gvenzl init scripts run BEFORE the APP_USER machinery**, so the
  oracle seed creates its own user.
- **Seeds must spread cursor columns**: rows stamped by one
  transaction share one timestamp, and an inclusive watermark re-reads
  them all — a fixture corner no example should manufacture.

### Standing observations (not fixed here)

- The duckdb destination creates its database FILE but not the
  file's parent directory — a first-run paper cut.
- The pipeline spec exposes ONE global `write_mode`, while the
  engine's builder API supports per-stream modes. YAML users cannot
  mix modes in one pipeline.
- `podman compose` (the built-in delegator) needs a provider;
  `podman-compose` 1.6.0 drives all four compose files verbatim.

### Verification tooling

Independent readbacks, never rdlt's own report: psql counts
(postgres), python-duckdb (duckdb: 60 rows, sum 23660.51), the
Polaris REST catalog (iceberg: snapshot summaries + partition spec),
Snowflake's SQL API over a PAT (40 rows), PokéAPI's own `count`
(1,351), sqlplus in-container (oracle seed).

## Gate of record

`make check` TWICE CLEAN, `TMPDIR` off the tmpfs:

| | run 1 | run 2 |
|---|---|---|
| suite | 1146/1146, 0 skipped | 1146/1146, 0 skipped |
| semver | no update required | no update required |
| benches | 6, 0 regressed | 6, 0 regressed |
| cold start | 24.8 ms | 25.1 ms (bar ≤ 40) |

1146 = 1143 + the 3 examples tests. Run 2's first attempt failed on
ONE cell: the snowflake live conformance took an HTTP 503 FROM THE
SERVICE — a service-side transient in a cell no 035 change touches
(only the example yaml is new); it passed isolated and on the re-run.
Recorded, not hidden.
