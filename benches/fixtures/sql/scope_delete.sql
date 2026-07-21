DELETE FROM target WHERE (day) IN (SELECT day FROM stage_scope WHERE day IS NOT NULL);
