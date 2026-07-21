SELECT count(*) FROM (SELECT DISTINCT ON (id) * FROM stage_dupes
                      ORDER BY id, seq DESC NULLS LAST, arrival DESC) d;
