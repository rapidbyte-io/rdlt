//! The statement computing a history table's commit: each key's events in order, which of them
//! change its versions, and the versions and closings those make.
//!
//! A key's events are its current version, then by sequence its admitted upserts and deletes,
//! and a delete for each truncate sequenced past its current version or one of its upserts.
//! Whether an event acts follows from the key's last version or upsert before it and whether a
//! delete came since: after a delete nothing is current, or where deletes are soft, a deleted
//! version of the last one's data; after an upsert, a version of its data, whether it opened one
//! or equalled the current. An upsert acts unless that version is current, not deleted and of
//! its hash; a delete acts where that version is current and, where deletes are soft, not
//! deleted. Each acting event closes the version the acting event before it opened, so a version
//! lasts until the next acting event of its key begins.
use super::super::super::tables::STAGING_COLUMNS;
use super::super::super::{Sql, SqlDialect, SqlPlanner};
use super::{BOUND, BURIED, CLOSED, OPENED, Versioned};

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statement computing into staging the versions the commit opens, the closings of the
    /// current versions, and where deletes are hard, the keys it buried and the bound it raised.
    pub(super) fn chained<'a>(&'a self, versioned: &Versioned<'_>) -> Sql<'a, D> {
        let mut sql = self.sql();
        let staging_columns: Vec<String> = STAGING_COLUMNS.iter().map(|c| self.quote(c)).collect();
        sql.push(&format!(
            "INSERT INTO {} ({}, {}) WITH ",
            versioned.staging,
            staging_columns.join(", "),
            versioned.names()
        ));
        self.events(&mut sql, versioned);
        sql.push(&chain(versioned));
        outcomes(&mut sql, versioned);
        sql
    }

    /// Writes the common table expressions of each key's events into `sql`: `_rdlt_admitted`,
    /// the commit's rows that apply, `_rdlt_truncates` among them for a change stream, and
    /// `_rdlt_events`, with each event's kind.
    fn events(&self, sql: &mut Sql<'_, D>, versioned: &Versioned<'_>) {
        let (names, keys) = (versioned.names(), versioned.keys.join(", "));
        let kind = &versioned.aliases.kind;
        let Some((changed, op)) = versioned.changed.as_ref().zip(versioned.op.as_ref()) else {
            let of = versioned.of;
            sql.push(&format!(
                "_rdlt_admitted AS (SELECT {names} FROM {} WHERE ",
                versioned.staging
            ));
            self.rows_of(sql, of.staged, of.pipeline, of.epoch, of.segments);
            sql.push(&format!(
                "), _rdlt_keys AS (SELECT DISTINCT {keys} FROM _rdlt_admitted), {}, \
                 _rdlt_events AS (SELECT {names}, 0 AS {kind} FROM _rdlt_anchors UNION ALL SELECT \
                 {names}, 1 FROM _rdlt_admitted)",
                anchors(versioned)
            ));
            return;
        };
        self.admitting(sql, changed);
        sql.push(&truncating(versioned, op));
    }
}

/// The common table expressions of a change stream's events: its truncates, the keys the commit
/// touches, their current versions, and each key's events, a truncate among them where it is the
/// first past a version or an upsert of the key.
fn truncating(versioned: &Versioned<'_>, op: &str) -> String {
    let Versioned {
        target,
        seq,
        is_current,
        aliases,
        ..
    } = versioned;
    let (names, keys) = (versioned.names(), versioned.keys.join(", "));
    let kind = &aliases.kind;
    // A truncate's delete of each key takes its sequence, validity and deletion time.
    let truncated: Vec<&str> = [seq, &versioned.valid_from]
        .into_iter()
        .chain(&versioned.at)
        .map(String::as_str)
        .collect();
    let kept_keys: Vec<String> = versioned
        .keys
        .iter()
        .map(|key| format!("_rdlt_p.{key}"))
        .collect();
    // Each anchor and upsert of a key meets the truncates in one ordered pass, which finds the
    // first truncate past it: the only one that can act on what it left. The table is read
    // whole only where the commit truncates: `_rdlt_latest` then holds a row to join from.
    let (mark, first, q) = (&aliases.gone, &aliases.next, &aliases.q);
    let nulls: Vec<&str> = versioned.keys.iter().map(|_| "NULL").collect();
    format!(
        ", _rdlt_truncates AS (SELECT {} FROM _rdlt_admitted WHERE {op} = 3), \
         _rdlt_latest AS (SELECT MAX({seq}) AS {q} FROM _rdlt_truncates HAVING MAX({seq}) IS \
         NOT NULL), \
         _rdlt_keys AS (SELECT {keys} FROM _rdlt_admitted WHERE {op} <> 3 UNION SELECT {} FROM \
         _rdlt_latest _rdlt_c CROSS JOIN {target} _rdlt_p WHERE _rdlt_p.{is_current} AND \
         _rdlt_p.{seq} < _rdlt_c.{q}), {}, \
         _rdlt_marks AS (SELECT {keys}, {seq} AS {q}, 1 AS {mark} FROM _rdlt_anchors UNION ALL \
         SELECT {keys}, {seq}, 1 FROM _rdlt_admitted WHERE {op} IN (0, 1) UNION ALL SELECT \
         {nulls}, {seq}, 0 FROM _rdlt_truncates), \
         _rdlt_after AS (SELECT {keys}, {mark}, MIN(CASE WHEN {mark} = 0 THEN {q} END) OVER \
         (ORDER BY {q} DESC, {mark} DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS \
         {first} FROM _rdlt_marks), \
         _rdlt_events AS (SELECT {names}, 0 AS {kind} FROM _rdlt_anchors UNION ALL SELECT \
         {names}, CASE WHEN {op} = 2 THEN 2 ELSE 1 END FROM _rdlt_admitted WHERE {op} <> 3 \
         UNION ALL SELECT {}, 3 FROM (SELECT DISTINCT {keys}, {first} FROM _rdlt_after WHERE \
         {mark} = 1 AND {first} IS NOT NULL) _rdlt_k JOIN _rdlt_truncates _rdlt_t ON \
         _rdlt_t.{seq} = _rdlt_k.{first})",
        truncated.join(", "),
        kept_keys.join(", "),
        anchors(versioned),
        versioned.projected(|column| {
            if truncated.contains(&column) {
                format!("_rdlt_t.{column}")
            } else if versioned.keys.iter().any(|key| key == column) {
                format!("_rdlt_k.{column}")
            } else {
                "NULL".to_owned()
            }
        }),
        nulls = nulls.join(", "),
    )
}

/// Writes the rows the commit computes into `sql`: each version an upsert or a soft delete
/// opens, each current version the table held as the commit closes it, and where deletes are
/// hard, each key's last delete no upsert follows and the commit's last truncate.
fn outcomes<D: SqlDialect>(sql: &mut Sql<'_, D>, versioned: &Versioned<'_>) {
    let Versioned {
        seq,
        valid_from,
        aliases,
        ..
    } = versioned;
    let (kind, next, pos, q) = (&aliases.kind, &aliases.next, &aliases.pos, &aliases.q);
    let code = format!("CASE WHEN _rdlt_a.{kind} = 0 THEN {CLOSED} ELSE {OPENED} END");
    let staged = versioned.staged_by(sql, &code);
    sql.push(&format!(
        " SELECT {staged}, {} FROM _rdlt_acting _rdlt_a WHERE _rdlt_a.{kind} = 1 OR \
         (_rdlt_a.{kind} = 0 AND _rdlt_a.{next} IS NOT NULL)",
        versioned.version(|column| format!("_rdlt_a.{column}"))
    ));
    if let Some(at) = &versioned.at {
        let staged = versioned.staged_by(sql, &OPENED.to_string());
        let deleting = [seq, valid_from, at];
        sql.push(&format!(
            " UNION ALL SELECT {staged}, {} FROM _rdlt_acting _rdlt_a JOIN _rdlt_acting \
             _rdlt_o ON {} AND _rdlt_o.{pos} = _rdlt_a.{} WHERE _rdlt_a.{kind} IN (2, 3)",
            versioned.version(|column| {
                let taken = deleting.iter().any(|name| *name == column);
                format!("{}.{column}", if taken { "_rdlt_a" } else { "_rdlt_o" })
            }),
            versioned.on("_rdlt_o", "_rdlt_a"),
            aliases.from,
        ));
    }
    let Some(op) = versioned.op.as_ref().filter(|_| versioned.hard) else {
        return;
    };
    let keys = versioned.keys.join(", ");
    let staged = versioned.staged_by(sql, &BURIED.to_string());
    sql.push(&format!(
        " UNION ALL SELECT {staged}, {} FROM (SELECT {keys}, MAX({seq}) AS {q} FROM \
         _rdlt_admitted _rdlt_x WHERE _rdlt_x.{op} = 2 AND NOT EXISTS (SELECT 1 FROM \
         _rdlt_admitted _rdlt_u WHERE {} AND _rdlt_u.{op} IN (0, 1) AND _rdlt_u.{seq} > \
         _rdlt_x.{seq}) AND NOT _rdlt_x.{seq} < COALESCE((SELECT MAX(_rdlt_t.{seq}) FROM \
         _rdlt_truncates _rdlt_t), _rdlt_x.{seq}) GROUP BY {keys}) _rdlt_d",
        versioned.removal(Some("_rdlt_d")),
        versioned.on("_rdlt_u", "_rdlt_x"),
    ));
    let staged = versioned.staged_by(sql, &BOUND.to_string());
    sql.push(&format!(
        " UNION ALL SELECT {staged}, {} FROM (SELECT MAX({seq}) AS {q} FROM _rdlt_truncates) \
         _rdlt_d WHERE _rdlt_d.{q} IS NOT NULL",
        versioned.removal(None),
    ));
}

/// The common table expression of each key's current version the table holds, `_rdlt_anchors`,
/// found by its key: the cross join reads the keys first, where SQLite would otherwise read the
/// table whole.
fn anchors(versioned: &Versioned<'_>) -> String {
    format!(
        "_rdlt_anchors AS (SELECT {} FROM _rdlt_keys _rdlt_k CROSS JOIN {} _rdlt_p WHERE {} AND \
         _rdlt_p.{})",
        versioned.projected(|column| format!("_rdlt_p.{column}")),
        versioned.target,
        versioned.on("_rdlt_p", "_rdlt_k"),
        versioned.is_current,
    )
}

/// The common table expressions deciding each event: `_rdlt_ordered` numbers a key's events,
/// its current version first; `_rdlt_prior` finds the last version or upsert and the last delete
/// before each; `_rdlt_decided` says whether it acts, and `_rdlt_acting`, of those that do, when
/// the next begins and which version or upsert a soft delete keeps.
fn chain(versioned: &Versioned<'_>) -> String {
    let Versioned {
        seq,
        valid_from,
        row_hash,
        aliases,
        ..
    } = versioned;
    let keys = versioned.keys.join(", ");
    let (kind, pos, last, gone) = (&aliases.kind, &aliases.pos, &aliases.last, &aliases.gone);
    let before = format!(
        "PARTITION BY {keys} ORDER BY {pos} ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING"
    );
    // A soft delete's version is deleted, and so may be the current version the table holds.
    let deleted = versioned
        .at
        .as_ref()
        .map_or(String::new(), |at| format!(" OR _rdlt_l.{at} IS NOT NULL"));
    format!(
        ", _rdlt_ordered AS (SELECT _rdlt_e.*, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY \
         CASE WHEN {kind} = 0 THEN 0 ELSE 1 END, {seq}, {kind}) AS {pos} FROM _rdlt_events \
         _rdlt_e), \
         _rdlt_prior AS (SELECT _rdlt_o.*, MAX(CASE WHEN {kind} IN (0, 1) THEN {pos} END) OVER \
         ({before}) AS {last}, MAX(CASE WHEN {kind} IN (2, 3) THEN {pos} END) OVER ({before}) AS \
         {gone} FROM _rdlt_ordered _rdlt_o), \
         _rdlt_decided AS (SELECT _rdlt_w.*, CASE WHEN _rdlt_w.{kind} = 0 THEN 1 WHEN \
         _rdlt_l.{pos} IS NULL OR COALESCE(_rdlt_w.{gone}, 0) > _rdlt_w.{last}{deleted} THEN \
         CASE WHEN _rdlt_w.{kind} = 1 THEN 1 ELSE 0 END WHEN _rdlt_w.{kind} = 1 AND \
         _rdlt_w.{row_hash} = _rdlt_l.{row_hash} THEN 0 ELSE 1 END AS {acts} FROM _rdlt_prior \
         _rdlt_w LEFT JOIN _rdlt_prior _rdlt_l ON {on_lw} AND _rdlt_l.{pos} = _rdlt_w.{last}), \
         _rdlt_acting AS (SELECT _rdlt_d.*, LEAD({valid_from}) OVER (PARTITION BY {keys} ORDER \
         BY {pos}) AS {next}, MAX(CASE WHEN {kind} IN (0, 1) THEN {pos} END) OVER ({before}) AS \
         {from} FROM _rdlt_decided _rdlt_d WHERE {acts} = 1)",
        acts = aliases.acts,
        next = aliases.next,
        from = aliases.from,
        on_lw = versioned.on("_rdlt_l", "_rdlt_w"),
    )
}

impl Versioned<'_> {
    /// Each of the table's columns as `value` gives it.
    fn projected(&self, value: impl Fn(&str) -> String) -> String {
        let values: Vec<String> = self.columns.iter().map(|column| value(column)).collect();
        values.join(", ")
    }

    /// The columns of a version the commit computes: each as `value` gives it but its validity,
    /// which ends where the next acting event of its key, `_rdlt_a`'s, begins.
    fn version(&self, value: impl Fn(&str) -> String) -> String {
        let next = &self.aliases.next;
        self.projected(|column| {
            if *column == self.valid_to {
                format!("_rdlt_a.{next}")
            } else if *column == self.is_current {
                format!("(_rdlt_a.{next} IS NULL)")
            } else {
                value(column)
            }
        })
    }

    /// The columns of a removal: the key of `removed` with its computed sequence, or for the
    /// bound, no key and the sequence `_rdlt_d` computed; every other column null.
    fn removal(&self, removed: Option<&str>) -> String {
        self.projected(|column| match removed {
            _ if *column == self.seq => format!("_rdlt_d.{}", self.aliases.q),
            Some(alias) if self.keys.iter().any(|key| key == column) => {
                format!("{alias}.{column}")
            }
            _ => "NULL".to_owned(),
        })
    }
}
