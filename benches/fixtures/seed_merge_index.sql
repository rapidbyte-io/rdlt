-- Feature 008 FR-009 merge-index cells (carried verbatim from
-- run-merge-index.sh): 10M-row target; 5k-key incremental delta and 5M-key
-- half-table delta. Deterministic — no now()/random().
DROP TABLE IF EXISTS target, stage_small, stage_half CASCADE;
CREATE TABLE target (id BIGINT, v TEXT);
INSERT INTO target SELECT i, 'row-'||i FROM generate_series(1, 10000000) i;
CREATE TABLE stage_small (id BIGINT);
INSERT INTO stage_small SELECT i FROM generate_series(1, 10000000, 2000) i; -- 5k keys
CREATE TABLE stage_half (id BIGINT);
INSERT INTO stage_half SELECT i FROM generate_series(1, 10000000, 2) i;     -- 5M keys
ANALYZE;
SELECT 'target=' || count(*) FROM target;
