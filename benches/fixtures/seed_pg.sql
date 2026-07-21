-- Feature 005 T024: deterministic benchmark datasets (research R7).
-- pg_wide: 1M rows x 12 typed columns (the gated pg->duckdb / pg->pg cells).
-- pg_jsonb: 200k rows carrying the harness's nested document shape in jsonb
--           (scoreboard context: the Json escape-hatch path end-to-end).
-- Identity = row counts + the content hashes printed at the end; every run
-- of this script produces byte-identical tables (no now()/random()).

DROP TABLE IF EXISTS pg_wide, pg_jsonb CASCADE;

CREATE TABLE pg_wide (
    id         int8 PRIMARY KEY,
    small      int4         NOT NULL,
    big        int8         NOT NULL,
    ratio      float8       NOT NULL,
    amount     numeric(12,4) NOT NULL,
    name       text         NOT NULL,
    code       varchar(16)  NOT NULL,
    active     bool         NOT NULL,
    created_at timestamptz  NOT NULL,
    birthday   date         NOT NULL,
    token      uuid         NOT NULL,
    note       text                  -- nullable: 1 in 10 NULL
);
INSERT INTO pg_wide
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

CREATE TABLE pg_jsonb (
    id  int8 PRIMARY KEY,
    doc jsonb NOT NULL
);
INSERT INTO pg_jsonb
SELECT i,
       jsonb_build_object(
           'id', i,
           'name', 'user-' || i,
           'score', i * 0.5,
           'profile', jsonb_build_object('city', 'NYC', 'zip', 10001 + i % 100),
           'tags', jsonb_build_array(
               jsonb_build_object('label', 'a'),
               jsonb_build_object('label', 'b')
           )
       )
FROM generate_series(1, 200000) i;

ANALYZE pg_wide;
ANALYZE pg_jsonb;

-- Identity block (record in the evidence artifact):
SELECT 'pg_wide'  AS dataset, count(*) AS rows,
       md5(string_agg(md5(t::text), '' ORDER BY id)) AS content_md5
FROM pg_wide t
UNION ALL
SELECT 'pg_jsonb', count(*),
       md5(string_agg(md5(t::text), '' ORDER BY id))
FROM pg_jsonb t;
