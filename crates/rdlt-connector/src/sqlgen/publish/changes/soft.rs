//! The rows a commit computes where deletes and truncates mark rows deleted, which stay.
//!
//! A key's inserts and updates set its columns, the last not flagging a column setting it. A
//! delete, or a truncate sequenced past the key's row, marks the row, where there is one then: it
//! takes the deletion's sequence, and its deletion time unless the row was deleted already.

use super::super::super::{Sql, SqlDialect, SqlPlanner};
use super::{Changed, MARKED, MERGED};
use crate::error::{ConnectorError, Result};

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statement computing into staging, where deletes and truncates mark rows deleted in the
    /// column `at`, each changed key's row.
    pub(super) fn soft<'a>(&'a self, changed: &Changed<'_>, at: &str) -> Result<Sql<'a, D>> {
        let at = self.quote(at);
        let Some(at_ordinal) = changed.columns.iter().position(|column| *column == at) else {
            return Err(ConnectorError::data(format!(
                "table {} has no column {at} to mark deleted rows in",
                changed.target
            )));
        };
        let Changed {
            op,
            seq,
            q,
            target,
            keys,
            ..
        } = changed;
        let keys = keys.join(", ");
        let mut sql = self.computing(changed);
        sql.push(&format!(
            ", _rdlt_truncates AS (SELECT {seq}, {at} FROM _rdlt_admitted WHERE {op} = 3), \
             _rdlt_upserts AS (SELECT * FROM _rdlt_admitted WHERE {op} IN (0, 1)), \
             _rdlt_first AS (SELECT {keys}, MIN({seq}) AS {q} FROM _rdlt_upserts GROUP BY {keys}), \
             _rdlt_last AS (SELECT {keys}, MAX({seq}) AS {q} FROM _rdlt_upserts GROUP BY {keys}), \
             _rdlt_assigned AS (SELECT {keys}, MAX({seq}) AS {q} FROM _rdlt_upserts _rdlt_v WHERE \
             NOT {flag} GROUP BY {keys}), \
             _rdlt_keys AS (SELECT {keys} FROM _rdlt_admitted WHERE {op} IN (0, 1, 2) UNION SELECT \
             {kept_keys} FROM {target} _rdlt_p WHERE EXISTS (SELECT 1 FROM _rdlt_truncates) AND \
             _rdlt_p.{seq} < (SELECT MAX(_rdlt_t.{seq}) FROM _rdlt_truncates _rdlt_t)), \
             _rdlt_deletions AS (SELECT {marked_keys}, _rdlt_a.{seq}, _rdlt_a.{at} FROM _rdlt_keys \
             _rdlt_k JOIN _rdlt_admitted _rdlt_a ON {on_ak} AND _rdlt_a.{op} = 2 WHERE EXISTS \
             (SELECT 1 FROM {target} _rdlt_p WHERE {on_pk}) OR EXISTS (SELECT 1 FROM _rdlt_first \
             _rdlt_f WHERE {on_fk} AND _rdlt_f.{q} < _rdlt_a.{seq}) UNION ALL SELECT \
             {marked_keys}, _rdlt_t.{seq}, _rdlt_t.{at} FROM _rdlt_keys _rdlt_k CROSS JOIN \
             _rdlt_truncates _rdlt_t WHERE EXISTS (SELECT 1 FROM {target} _rdlt_p WHERE {on_pk} \
             AND _rdlt_p.{seq} < _rdlt_t.{seq}) OR EXISTS (SELECT 1 FROM _rdlt_first _rdlt_f \
             WHERE {on_fk} AND _rdlt_f.{q} < _rdlt_t.{seq})), \
             _rdlt_seqs AS (SELECT {keys}, MAX({seq}) AS {q} FROM (SELECT {keys}, {seq} FROM \
             _rdlt_upserts UNION ALL SELECT {keys}, {seq} FROM _rdlt_deletions) _rdlt_e GROUP BY \
             {keys}) SELECT ",
            flag = changed.flagged("_rdlt_v", at_ordinal),
            kept_keys = changed.keyed("_rdlt_p"),
            marked_keys = changed.keyed("_rdlt_k"),
            on_ak = changed.on("_rdlt_a", "_rdlt_k"),
            on_pk = changed.on("_rdlt_p", "_rdlt_k"),
            on_fk = changed.on("_rdlt_f", "_rdlt_k"),
        ));
        let staged = changed.staged_by(&mut sql);
        sql.push(&format!(
            "{staged}, {}, CASE WHEN _rdlt_l.{q} IS NULL THEN {MARKED} ELSE {MERGED} END FROM \
             _rdlt_seqs _rdlt_x LEFT JOIN _rdlt_last _rdlt_l ON {} LEFT JOIN _rdlt_upserts _rdlt_u \
             ON {} AND _rdlt_u.{seq} = _rdlt_l.{q} LEFT JOIN _rdlt_assigned _rdlt_g ON {} LEFT \
             JOIN {target} _rdlt_p ON {}",
            marked(changed, &at),
            changed.on("_rdlt_l", "_rdlt_x"),
            changed.on("_rdlt_u", "_rdlt_x"),
            changed.on("_rdlt_g", "_rdlt_x"),
            changed.on("_rdlt_p", "_rdlt_x"),
        ));
        Ok(sql)
    }
}

/// The columns of each changed key's row, `_rdlt_x`: its row `_rdlt_p` where no insert or update
/// applied, otherwise as its last upsert `_rdlt_u` sets each, or where that flags one unchanged,
/// as its upserts before or its row left it; its deletion time as [`deleted_at`] says.
fn marked(changed: &Changed<'_>, at: &str) -> String {
    let columns: Vec<String> = changed
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| {
            if *column == changed.seq {
                return format!("_rdlt_x.{}", changed.q);
            }
            if changed.is_key_or_seq(column) {
                return format!("_rdlt_x.{column}");
            }
            if column == at {
                return deleted_at(changed, at);
            }
            if changed.unchanged.is_none() {
                return format!(
                    "CASE WHEN _rdlt_l.{q} IS NULL THEN _rdlt_p.{column} ELSE _rdlt_u.{column} END",
                    q = changed.q
                );
            }
            format!(
                "CASE WHEN _rdlt_l.{q} IS NULL THEN _rdlt_p.{column} WHEN {flag} THEN {chained} \
                 ELSE _rdlt_u.{column} END",
                q = changed.q,
                flag = changed.flagged("_rdlt_u", ordinal),
                chained = changed.chained(column, ordinal, "_rdlt_x", &changed.target),
            )
        })
        .collect();
    columns.join(", ")
}

/// The deletion time of each changed key's row, `_rdlt_x`: as its last upsert setting it,
/// `_rdlt_g`, sets it, or else as its row holds it; where that is null, the time of the first
/// deletion after them that has one.
fn deleted_at(changed: &Changed<'_>, at: &str) -> String {
    let (seq, q) = (&changed.seq, &changed.q);
    format!(
        "COALESCE(CASE WHEN _rdlt_g.{q} IS NULL THEN _rdlt_p.{at} ELSE (SELECT _rdlt_v.{at} FROM \
         _rdlt_upserts _rdlt_v WHERE {on_v} AND _rdlt_v.{seq} = _rdlt_g.{q}) END, (SELECT \
         MIN(_rdlt_e.{at}) FROM _rdlt_deletions _rdlt_e WHERE {on_e} AND _rdlt_e.{seq} = (SELECT \
         MIN(_rdlt_f.{seq}) FROM _rdlt_deletions _rdlt_f WHERE {on_f} AND _rdlt_f.{at} IS NOT NULL \
         AND (_rdlt_g.{q} IS NULL OR _rdlt_f.{seq} > _rdlt_g.{q}))))",
        on_v = changed.on("_rdlt_v", "_rdlt_x"),
        on_e = changed.on("_rdlt_e", "_rdlt_x"),
        on_f = changed.on("_rdlt_f", "_rdlt_x"),
    )
}
