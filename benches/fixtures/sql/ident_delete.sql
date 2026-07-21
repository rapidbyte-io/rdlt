DELETE FROM target WHERE (id) IN (SELECT id FROM stage_scope);
