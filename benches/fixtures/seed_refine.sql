-- Feature 010 FR-011 refinement cells (carried verbatim from
-- run-merge-refinements.sh): 10M-row scoped target (100 days x 100k rows,
-- indexes mirroring what ensure_table auto-provisions for merge_key tables)
-- and a 2x-duplicated 1M-row stage. Deterministic.
DROP TABLE IF EXISTS target, stage_scope, stage_dupes CASCADE;
CREATE TABLE target (id BIGINT, day BIGINT, v TEXT);
INSERT INTO target SELECT i, i % 100, 'row-'||i FROM generate_series(1, 10000000) i;
CREATE INDEX rdlt_ix_target_day ON target (day);
CREATE INDEX rdlt_ix_target_id ON target (id);
CREATE TABLE stage_scope (id BIGINT, day BIGINT);
INSERT INTO stage_scope SELECT i, 7 FROM generate_series(7, 10000000, 100) i; -- day 7: 100k rows
CREATE TABLE stage_dupes (id BIGINT, seq BIGINT, arrival BIGSERIAL);
INSERT INTO stage_dupes (id, seq)
SELECT i % 1000000, i % 2 + 1 FROM generate_series(1, 2000000) i;             -- 1M ids x 2
ANALYZE;
SELECT 'target=' || count(*) FROM target;
