//! The rows a commit computes where deletes and truncates remove rows outright.
//!
//! A truncate removes every row sequenced before it, those the commit applied too, so of each key
//! only the rows at or past the commit's last truncate apply. After the key's last delete among
//! them, its inserts and updates make its row, starting from nothing; without a delete, from the
//! row the table holds, unless a truncate removed it. A key whose last change is a delete is
//! buried.

use super::super::super::{Sql, SqlDialect, SqlPlanner};
use super::{BOUND, BURIED, Changed, MERGED};

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statement computing into staging, where deletes and truncates remove rows, each changed
    /// key's row, each key removed, and the bound the commit's last truncate raised.
    pub(super) fn hard<'a>(&'a self, changed: &Changed<'_>) -> Sql<'a, D> {
        let Changed {
            op, seq, q, keys, ..
        } = changed;
        let keys = keys.join(", ");
        let mut sql = self.computing(changed);
        sql.push(&format!(
            ", _rdlt_cut AS (SELECT MAX({seq}) AS {q} FROM _rdlt_admitted WHERE {op} = 3), \
             _rdlt_live AS (SELECT _rdlt_a.* FROM _rdlt_admitted _rdlt_a WHERE _rdlt_a.{op} <> 3 \
             AND NOT EXISTS (SELECT 1 FROM _rdlt_cut _rdlt_c WHERE _rdlt_a.{seq} < _rdlt_c.{q})), \
             _rdlt_deleted AS (SELECT {keys}, MAX({seq}) AS {q} FROM _rdlt_live WHERE {op} = 2 \
             GROUP BY {keys}), \
             _rdlt_upserts AS (SELECT _rdlt_l.* FROM _rdlt_live _rdlt_l WHERE _rdlt_l.{op} IN (0, 1) \
             AND NOT EXISTS (SELECT 1 FROM _rdlt_deleted _rdlt_d WHERE {on_dl} AND \
             _rdlt_l.{seq} < _rdlt_d.{q})), \
             _rdlt_last AS (SELECT {keys}, MAX({seq}) AS {q} FROM _rdlt_upserts GROUP BY {keys})\
             {flags} SELECT ",
            on_dl = changed.on("_rdlt_d", "_rdlt_l"),
            flags = kept(changed),
        ));
        let staged = changed.of.staged_by(&mut sql, "NULL");
        sql.push(&format!(
            "{staged}, {}, {MERGED} FROM _rdlt_last _rdlt_m JOIN _rdlt_upserts _rdlt_u ON {} AND \
             _rdlt_u.{seq} = _rdlt_m.{q}{} UNION ALL SELECT ",
            merged(changed),
            changed.on("_rdlt_u", "_rdlt_m"),
            if changed.flags() {
                format!(
                    " LEFT JOIN _rdlt_set _rdlt_s ON {} LEFT JOIN _rdlt_vals _rdlt_n ON {} LEFT \
                     JOIN _rdlt_kept _rdlt_k ON {}",
                    changed.on("_rdlt_s", "_rdlt_m"),
                    changed.on("_rdlt_n", "_rdlt_m"),
                    changed.on("_rdlt_k", "_rdlt_m"),
                )
            } else {
                String::new()
            },
        ));
        let staged = changed.of.staged_by(&mut sql, "NULL");
        sql.push(&format!(
            "{staged}, {}, {BURIED} FROM _rdlt_deleted _rdlt_d WHERE NOT EXISTS (SELECT 1 FROM \
             _rdlt_upserts _rdlt_u WHERE {}) UNION ALL SELECT ",
            removed(changed, Some("_rdlt_d")),
            changed.on("_rdlt_u", "_rdlt_d"),
        ));
        let staged = changed.of.staged_by(&mut sql, "NULL");
        sql.push(&format!(
            "{staged}, {}, {BOUND} FROM _rdlt_cut _rdlt_c WHERE _rdlt_c.{q} IS NOT NULL",
            removed(changed, None),
        ));
        sql
    }
}

/// Where rows may flag columns unchanged, the common table expressions a flagged column is
/// read from: what the key's upserts set, and `_rdlt_kept`, the row the table holds of each
/// changed key unless the commit deleted its key or truncated it, found by its key and never by
/// reading the table whole.
fn kept(changed: &Changed<'_>) -> String {
    if !changed.flags() {
        return String::new();
    }
    format!(
        ", {}, _rdlt_kept AS (SELECT {} FROM _rdlt_last _rdlt_m CROSS JOIN {} _rdlt_p WHERE {} \
         AND NOT EXISTS (SELECT 1 FROM _rdlt_deleted _rdlt_d WHERE {}) AND NOT EXISTS (SELECT 1 \
         FROM _rdlt_cut _rdlt_c WHERE _rdlt_p.{} < _rdlt_c.{}))",
        changed.settings(),
        changed.names("_rdlt_p"),
        changed.target,
        changed.on("_rdlt_p", "_rdlt_m"),
        changed.on("_rdlt_d", "_rdlt_p"),
        changed.seq,
        changed.q,
    )
}

/// The columns of each changed key's row, `_rdlt_m`, whose last upsert is `_rdlt_u`: each as it
/// sets it, or for a column rows may flag unchanged, as the last upsert not flagging it set it,
/// `_rdlt_n`'s, or else as the row the table holds, `_rdlt_k`, left it.
fn merged(changed: &Changed<'_>) -> String {
    let columns: Vec<String> = changed
        .columns
        .iter()
        .map(|column| {
            // Staging refuses a flag on a key or the sequence, so theirs are set by every row.
            if !changed.flaggable(column) {
                return format!("_rdlt_u.{column}");
            }
            format!(
                "CASE WHEN _rdlt_s.{column} IS NULL THEN _rdlt_k.{column} ELSE _rdlt_n.{column} END"
            )
        })
        .collect();
    columns.join(", ")
}

/// The columns of a removal: the key of `removed`, a key's computed sequence, or for the bound,
/// no key and the truncate's sequence, `_rdlt_c`'s; every other column null.
fn removed(changed: &Changed<'_>, removed: Option<&str>) -> String {
    let columns: Vec<String> = changed
        .columns
        .iter()
        .map(|column| match removed {
            _ if *column == changed.seq => {
                format!("{}.{}", removed.unwrap_or("_rdlt_c"), changed.q)
            }
            Some(alias) if changed.keys.contains(column) => format!("{alias}.{column}"),
            _ => "NULL".to_owned(),
        })
        .collect();
    columns.join(", ")
}
