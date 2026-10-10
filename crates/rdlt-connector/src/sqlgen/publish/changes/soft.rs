//! The rows a commit computes where deletes and truncates mark rows deleted, which stay.
//!
//! A key's inserts and updates set its columns, the last not flagging a column setting it. A
//! delete, or a truncate sequenced past the key's row, marks the row, where there is one then: it
//! takes the deletion's sequence, and its deletion time unless the row was deleted already.
//!
//! The table is read whole only where the commit truncates: `_rdlt_latest` holds the last
//! truncate's sequence, or no row, and what reads the table for the rows before it joins from it.

use super::super::super::{Sql, SqlDialect, SqlPlanner};
use super::{Changed, MARKED, MERGED, Pass};
use crate::error::{ConnectorError, Result};

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statement computing into staging, where deletes and truncates mark rows deleted in the
    /// column `at`, each changed key's row.
    pub(super) fn soft<'a>(&'a self, changed: &Changed<'_>, at: &str) -> Result<Sql<'a, D>> {
        let at = self.quote(at);
        let Some(at_ordinal) = changed.columns.iter().position(|column| *column == at) else {
            return Err(ConnectorError::internal(format!(
                "table {} was checked to hold {at}, which marks its deleted rows",
                changed.target
            )));
        };
        let (seq, q, target) = (&changed.seq, &changed.q, &changed.target);
        let mut sql = self.computing(changed);
        sql.push(&marking(changed, &at, at_ordinal));
        let staged = changed.of.staged_by(&mut sql, "NULL");
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
        if changed.flags() {
            sql.push(&format!(
                " LEFT JOIN _rdlt_set _rdlt_s ON {} LEFT JOIN _rdlt_vals _rdlt_n ON {}",
                changed.on("_rdlt_s", "_rdlt_x"),
                changed.on("_rdlt_n", "_rdlt_x"),
            ));
        }
        Ok(sql)
    }
}

/// The common table expressions of the keys a commit marks or merges where deletes are soft:
/// its truncates and upserts, each key's first and last upsert and the last setting its deletion
/// time, the keys it touches, the deletions that mark each, and each key's sequence then.
fn marking(changed: &Changed<'_>, at: &str, at_ordinal: usize) -> String {
    let Changed {
        op,
        seq,
        q,
        target,
        keys,
        ..
    } = changed;
    let keys = keys.join(", ");
    format!(
        ", _rdlt_truncates AS (SELECT {seq}, {at} FROM _rdlt_admitted WHERE {op} = 3), \
         _rdlt_upserts AS (SELECT * FROM _rdlt_admitted WHERE {op} IN (0, 1)), \
         _rdlt_first AS (SELECT {keys}, MIN({seq}) AS {q} FROM _rdlt_upserts GROUP BY {keys}), \
         _rdlt_last AS (SELECT {keys}, MAX({seq}) AS {q} FROM _rdlt_upserts GROUP BY {keys}), \
         _rdlt_assigned AS (SELECT {keys}, MAX({seq}) AS {q} FROM _rdlt_upserts _rdlt_v WHERE \
         NOT {flag} GROUP BY {keys}), {settings}\
         _rdlt_latest AS (SELECT MAX({seq}) AS {q} FROM _rdlt_truncates HAVING MAX({seq}) IS \
         NOT NULL), \
         _rdlt_keys AS (SELECT {keys} FROM _rdlt_admitted WHERE {op} IN (0, 1, 2) UNION SELECT \
         {kept_keys} FROM _rdlt_latest _rdlt_c CROSS JOIN {target} _rdlt_p WHERE \
         _rdlt_p.{seq} < _rdlt_c.{q}), \
         {truncated}, \
         _rdlt_deletions AS (SELECT {marked_keys}, _rdlt_a.{seq}, _rdlt_a.{at}, 1 AS {kind} FROM _rdlt_keys \
         _rdlt_k JOIN _rdlt_admitted _rdlt_a ON {on_ak} AND _rdlt_a.{op} = 2 WHERE EXISTS \
         (SELECT 1 FROM {target} _rdlt_p WHERE {on_pk}) OR EXISTS (SELECT 1 FROM _rdlt_first \
         _rdlt_f WHERE {on_fk} AND _rdlt_f.{q} < _rdlt_a.{seq}) UNION ALL SELECT \
         {timed_keys}, _rdlt_t.{seq}, _rdlt_t.{at}, 0 FROM _rdlt_timed _rdlt_n JOIN \
         _rdlt_truncates _rdlt_t ON _rdlt_t.{seq} = _rdlt_n.{next} WHERE _rdlt_n.{kind} = 1), \
         _rdlt_seqs AS (SELECT {keys}, MAX({seq}) AS {q} FROM (SELECT {keys}, {seq} FROM \
         _rdlt_upserts UNION ALL SELECT {keys}, {seq} FROM _rdlt_deletions UNION ALL SELECT \
         {bound_keys}, _rdlt_c.{q} FROM _rdlt_latest _rdlt_c CROSS JOIN _rdlt_bounds _rdlt_b \
         WHERE _rdlt_b.{low} < _rdlt_c.{q}) _rdlt_e GROUP BY {keys}) SELECT ",
        flag = changed.flagged("_rdlt_v", at_ordinal),
        settings = if changed.flags() {
            format!("{}, ", changed.settings())
        } else {
            String::new()
        },
        kept_keys = changed.keyed("_rdlt_p"),
        marked_keys = changed.keyed("_rdlt_k"),
        timed_keys = changed.keyed("_rdlt_n"),
        bound_keys = changed.keyed("_rdlt_b"),
        truncated = truncated(changed, at),
        on_ak = changed.on("_rdlt_a", "_rdlt_k"),
        on_pk = changed.on("_rdlt_p", "_rdlt_k"),
        on_fk = changed.on("_rdlt_f", "_rdlt_k"),
        low = changed.pass.low,
        kind = changed.pass.kind,
        next = changed.pass.next,
    )
}

/// The common table expressions that bring each key together with the truncates that mark it, in
/// one ordered pass instead of a pair for every key and truncate.
///
/// `_rdlt_bounds` holds, of each key, the least sequence it has a row at, the table's or its
/// first upsert's, which a truncate must pass to mark it, and the sequence past which a truncate
/// may give it its deletion time: that, or its last upsert setting the time, whichever is later.
/// `_rdlt_timed` orders those with the truncates that say when they deleted, and finds for each
/// key the first of them past it. Every truncate past a key marks it, so the last gives it its
/// sequence, and only the first that says when can give it its deletion time.
fn truncated(changed: &Changed<'_>, at: &str) -> String {
    let Changed {
        seq,
        q,
        target,
        pass,
        ..
    } = changed;
    let Pass {
        low,
        past,
        kind,
        next,
    } = pass;
    let nulls: Vec<&str> = changed.keys.iter().map(|_| "NULL").collect();
    // A key the table holds no row of and no upsert makes a row of has no row to mark.
    let least = format!(
        "CASE WHEN _rdlt_p.{seq} IS NULL THEN _rdlt_f.{q} WHEN _rdlt_f.{q} IS NULL OR \
         _rdlt_p.{seq} < _rdlt_f.{q} THEN _rdlt_p.{seq} ELSE _rdlt_f.{q} END"
    );
    format!(
        "_rdlt_lows AS (SELECT {keys}, {least} AS {low}, _rdlt_g.{q} AS {q} FROM _rdlt_keys \
         _rdlt_k LEFT JOIN {target} _rdlt_p ON {on_pk} LEFT JOIN _rdlt_first _rdlt_f ON {on_fk} \
         LEFT JOIN _rdlt_assigned _rdlt_g ON {on_gk}), \
         _rdlt_bounds AS (SELECT {plain_keys}, {low}, CASE WHEN {q} IS NULL OR {q} < {low} THEN \
         {low} ELSE {q} END AS {past} FROM _rdlt_lows WHERE {low} IS NOT NULL), \
         _rdlt_line AS (SELECT {plain_keys}, {past} AS {q}, 1 AS {kind} FROM _rdlt_bounds UNION \
         ALL SELECT {nulls}, {seq}, 0 FROM _rdlt_truncates WHERE {at} IS NOT NULL), \
         _rdlt_timed AS (SELECT {plain_keys}, {kind}, MIN(CASE WHEN {kind} = 0 THEN {q} END) OVER \
         (ORDER BY {q} DESC, {kind} DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS \
         {next} FROM _rdlt_line)",
        keys = changed.keyed("_rdlt_k"),
        plain_keys = changed.keys.join(", "),
        nulls = nulls.join(", "),
        on_pk = changed.on("_rdlt_p", "_rdlt_k"),
        on_fk = changed.on("_rdlt_f", "_rdlt_k"),
        on_gk = changed.on("_rdlt_g", "_rdlt_k"),
    )
}

/// The columns of each changed key's row, `_rdlt_x`: its row `_rdlt_p` where no insert or update
/// applied, otherwise as its last upsert `_rdlt_u` sets each, or for a column rows may flag
/// unchanged, as the last upsert not flagging it set it, `_rdlt_n`'s, or else as its row left it;
/// its deletion time as [`deleted_at`] says.
fn marked(changed: &Changed<'_>, at: &str) -> String {
    let columns: Vec<String> = changed
        .columns
        .iter()
        .map(|column| {
            if *column == changed.seq {
                return format!("_rdlt_x.{}", changed.q);
            }
            if column == at {
                return deleted_at(changed, at);
            }
            if !changed.flaggable(column) {
                return format!(
                    "CASE WHEN _rdlt_l.{q} IS NULL THEN _rdlt_p.{column} ELSE _rdlt_u.{column} END",
                    q = changed.q
                );
            }
            // No upsert of the key sets the column: its row keeps what it holds.
            format!(
                "CASE WHEN _rdlt_s.{column} IS NULL THEN _rdlt_p.{column} ELSE _rdlt_n.{column} END"
            )
        })
        .collect();
    columns.join(", ")
}

/// The deletion time of each changed key's row, `_rdlt_x`: as its last upsert setting it,
/// `_rdlt_g`, sets it, or else as its row holds it; where that is null, the time of the first
/// deletion after them that has one, a truncate's before a delete's of its own sequence, since
/// the truncate applies first.
fn deleted_at(changed: &Changed<'_>, at: &str) -> String {
    let (seq, q, kind) = (&changed.seq, &changed.q, &changed.pass.kind);
    // The sequence of the first deletion past the last upsert setting the time that says when.
    let first = format!(
        "(SELECT MIN(_rdlt_f.{seq}) FROM _rdlt_deletions _rdlt_f WHERE {on_f} AND _rdlt_f.{at} \
         IS NOT NULL AND (_rdlt_g.{q} IS NULL OR _rdlt_f.{seq} > _rdlt_g.{q}))",
        on_f = changed.on("_rdlt_f", "_rdlt_x"),
    );
    format!(
        "COALESCE(CASE WHEN _rdlt_g.{q} IS NULL THEN _rdlt_p.{at} ELSE (SELECT _rdlt_v.{at} FROM \
         _rdlt_upserts _rdlt_v WHERE {on_v} AND _rdlt_v.{seq} = _rdlt_g.{q}) END, (SELECT \
         MIN(_rdlt_e.{at}) FROM _rdlt_deletions _rdlt_e WHERE {on_e} AND _rdlt_e.{seq} = {first} \
         AND _rdlt_e.{kind} = (SELECT MIN(_rdlt_h.{kind}) FROM _rdlt_deletions _rdlt_h WHERE \
         {on_h} AND _rdlt_h.{seq} = {first} AND _rdlt_h.{at} IS NOT NULL)))",
        on_v = changed.on("_rdlt_v", "_rdlt_x"),
        on_e = changed.on("_rdlt_e", "_rdlt_x"),
        on_h = changed.on("_rdlt_h", "_rdlt_x"),
    )
}
