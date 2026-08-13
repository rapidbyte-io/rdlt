-- Same-conditions postgres fixture for the 018 e2e matrix (research D-07).
-- Piped through `psql -U postgres` on the default database, so it may CREATE
-- DATABASE and `\connect` between them.
--
-- One server hosts:
--   src          — the seeded source datasets (read by every pg-source arm).
--   dest_rdlt    — empty; rdlt's destination.
--   dest_dlt     — empty; dlt's destination.
--   dest_airbyte — empty; Airbyte's destination (P3).
--
-- Two source tables in `src`:
--   events    — 1M × 12 typed columns (the standard matrix source; also the
--               dedup cell's LOAD 1).
--   events_v2 — the same 1M ids with 50% of rows carrying changed non-key
--               values (the dedup cell's LOAD 2 — full re-delivery, dedup by
--               id, research D-08).
--
-- Deterministic: no now()/random(), so every run seeds byte-identical tables.
-- The identity block at the end (row counts + content hashes) is captured as
-- the fixture's recorded dataset identity.

CREATE DATABASE src;
CREATE DATABASE dest_rdlt;
CREATE DATABASE dest_dlt;
CREATE DATABASE dest_airbyte;

\connect src

CREATE TABLE events (
    id         int8 PRIMARY KEY,
    small      int4          NOT NULL,
    big        int8          NOT NULL,
    ratio      float8        NOT NULL,
    amount     numeric(12,4) NOT NULL,
    name       text          NOT NULL,
    code       varchar(16)   NOT NULL,
    active     bool          NOT NULL,
    created_at timestamptz   NOT NULL,
    birthday   date          NOT NULL,
    token      uuid          NOT NULL,
    note       text                    -- nullable: 1 in 10 NULL
);
INSERT INTO events
SELECT i,
       (i % 100000)::int4,
       (i * 2654435761)::int8,
       i::float8 / 3.0,
       ((i % 99999999)::numeric) / 10000,
       'user-' || i,
       'C-' || lpad((i % 65536)::text, 6, '0'),
       (i % 3 = 0),
       TIMESTAMPTZ '2026-01-01 00:00:00Z' + (i % 86400) * INTERVAL '1 second',
       DATE '1970-01-01' + (i % 20000),
       md5('token-' || i)::uuid,
       CASE WHEN i % 10 = 0 THEN NULL ELSE 'note-' || (i % 1000) END
FROM generate_series(1, 1000000) i;

-- events_v2: identical shape and ids; rows with an even id carry changed
-- non-key values (name/amount/note), so a full re-delivery deduped by id
-- rewrites 50% of the table and leaves 50% untouched.
CREATE TABLE events_v2 (LIKE events INCLUDING ALL);
INSERT INTO events_v2
SELECT id,
       small,
       big,
       ratio,
       CASE WHEN id % 2 = 0 THEN amount + 1 ELSE amount END,
       CASE WHEN id % 2 = 0 THEN 'user-v2-' || id ELSE name END,
       code,
       active,
       created_at,
       birthday,
       token,
       CASE WHEN id % 2 = 0 THEN 'note-v2-' || (id % 1000) ELSE note END
FROM events;

ANALYZE events;
ANALYZE events_v2;

-- Identity block (recorded in the evidence artifact):
SELECT 'events'    AS dataset, count(*) AS rows,
       md5(string_agg(md5(t::text), '' ORDER BY id)) AS content_md5
FROM events t
UNION ALL
SELECT 'events_v2', count(*),
       md5(string_agg(md5(t::text), '' ORDER BY id))
FROM events_v2 t;
