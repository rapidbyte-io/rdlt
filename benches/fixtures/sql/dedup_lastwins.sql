SELECT count(*) FROM (SELECT DISTINCT ON (id) * FROM stage_dupes
                      ORDER BY id, arrival DESC) d;
