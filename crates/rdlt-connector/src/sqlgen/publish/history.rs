//! Publishing a history table's staged rows: each key keeps every version, a change closing the
//! current one and opening its own, as [`HistoryColumns`] says.
//!
//! One statement reads the table, its tombstones and the commit's staged rows, and computes into
//! staging each version the commit opens, closed again or current, the closing of each key's
//! current version, and for a change stream with hard deletes each key a delete buried and the
//! bound a truncate raised. The statements after it only move those rows, so none reads what
//! another changed. A history table never fills a generation, so the computed rows are told from
//! the staged ones by their generation column, which holds their code: a plain history table's
//! staging has no op column to hold it.

mod chain;
#[cfg(test)]
mod tests;

use super::super::tables::STAGING_COLUMNS;
use super::super::{Column, Sql, SqlDialect, SqlPlanner, Statement};
use super::changes::Changed;
use super::keyed::{Of, unused};
use crate::destination::{Deletion, HistoryColumns, MergeKey};
use crate::error::{ConnectorError, Result};

/// A version the commit opens, with its final `valid_to` and `is_current`.
const OPENED: i8 = 1;
/// A key's current version as the commit closes it.
const CLOSED: i8 = 2;
/// A key a hard delete removed: its tombstone.
const BURIED: i8 = 3;
/// The bound a hard truncate raised, naming no key.
const BOUND: i8 = 4;

/// The names one history table's commit shares across its statements, quoted.
struct Versioned<'a> {
    of: &'a Of<'a>,
    /// For a change stream's table, the names its guard reads.
    changed: Option<Changed<'a>>,
    /// The table's columns, in order.
    columns: Vec<String>,
    target: String,
    staging: String,
    tombstones: String,
    keys: Vec<String>,
    seq: String,
    op: Option<String>,
    /// The column deleted versions hold their deletion time in, where deletes are soft.
    at: Option<String>,
    /// Whether the table is a change stream's whose deletes remove rows.
    hard: bool,
    valid_from: String,
    valid_to: String,
    is_current: String,
    row_hash: String,
    aliases: Aliases,
}

/// What the chain computes of each of a key's events, under names no column of the table has.
struct Aliases {
    /// Whether the event is the key's current version, an upsert, a delete or a truncate.
    kind: String,
    /// The event's position among its key's.
    pos: String,
    /// The position of the key's last version or upsert before the event.
    last: String,
    /// The position of the key's last delete or truncate before the event.
    gone: String,
    /// Whether the event changes the key's versions.
    acts: String,
    /// The latest a key's acting events up to the event begin.
    run: String,
    /// The latest instant a key's versions the table holds hold.
    floor: String,
    /// When the event begins: when it says, or the latest instant its key held before it, if
    /// that is later.
    began: String,
    /// When the key's next event that acts begins.
    next: String,
    /// The position of the version a soft delete keeps the data of.
    from: String,
    /// A computed sequence.
    q: String,
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statements publishing the history table's rows `of` names into `target`, whose
    /// columns are `columns`, by `key` and its `history` columns, then removing what the commit
    /// computed from staging; the staged rows themselves are removed after.
    pub(super) fn versioned(
        &self,
        target: &str,
        (key, history): (&MergeKey, &HistoryColumns),
        columns: &[Column],
        of: &Of<'_>,
    ) -> Result<Vec<Statement>> {
        if of.staged.generation.is_some() {
            return Err(ConnectorError::internal(
                "a history table publishes into its table, never a generation",
            ));
        }
        if key
            .changes
            .as_ref()
            .is_some_and(|changes| changes.unchanged.is_some())
        {
            return Err(ConnectorError::internal(
                "a history table's rows are whole: its hashes need every column",
            ));
        }
        let versioned = Versioned::new(self, target, (key, history), columns, of);
        let codes = [OPENED, CLOSED, BURIED, BOUND];
        let mut plan = vec![
            self.chained(&versioned).finish(),
            self.closing(&versioned),
            self.moved_versions(&versioned, &versioned.target, &versioned.names(), &[OPENED]),
        ];
        if versioned.hard {
            let names = format!("{}, {}", versioned.keys.join(", "), versioned.seq);
            plan.extend([
                self.reopened(&versioned),
                self.unbound(&versioned),
                self.moved_versions(&versioned, &versioned.tombstones, &names, &[BURIED, BOUND]),
            ]);
        }
        let mut forget = self.sql();
        forget.push(&format!("DELETE FROM {} WHERE ", versioned.staging));
        self.computed(&mut forget, &versioned, &codes);
        plan.push(forget.finish());
        Ok(plan)
    }

    /// The statement closing each key's current version the commit closes: found by its first key
    /// column among the closings', through the key's index, then matched on every key column.
    ///
    /// It runs before the opened versions move in, so the only current version of a key is the
    /// one the table held.
    fn closing(&self, versioned: &Versioned<'_>) -> Statement {
        let Versioned {
            target,
            staging,
            valid_to,
            is_current,
            ..
        } = versioned;
        let first = &versioned.keys[0];
        let on = versioned.on("_rdlt_s", target);
        let mut sql = self.sql();
        let closed = |sql: &mut Sql<'_, D>, column: &str| {
            sql.push(&format!(
                "(SELECT _rdlt_s.{column} FROM {staging} _rdlt_s WHERE "
            ));
            self.computed(sql, versioned, &[CLOSED]);
            sql.push(&format!(" AND {on})"));
        };
        sql.push(&format!("UPDATE {target} SET {valid_to} = "));
        closed(&mut sql, valid_to);
        sql.push(&format!(", {is_current} = "));
        closed(&mut sql, is_current);
        sql.push(&format!(
            " WHERE {target}.{is_current} AND {target}.{first} IN (SELECT _rdlt_s.{first} FROM \
             {staging} _rdlt_s WHERE "
        ));
        self.computed(&mut sql, versioned, &[CLOSED]);
        sql.push(&format!(
            ") AND EXISTS (SELECT 1 FROM {staging} _rdlt_s WHERE "
        ));
        self.computed(&mut sql, versioned, &[CLOSED]);
        sql.push(&format!(" AND {on})"));
        sql.finish()
    }

    /// The statement deleting the tombstones of each key the commit opened a version of or
    /// buried: an opened version is sequenced past its key's tombstone, and a buried key takes
    /// another.
    fn reopened(&self, versioned: &Versioned<'_>) -> Statement {
        let Versioned {
            staging,
            tombstones,
            ..
        } = versioned;
        let first = &versioned.keys[0];
        let mut sql = self.sql();
        sql.push(&format!(
            "DELETE FROM {tombstones} WHERE {tombstones}.{first} IN (SELECT _rdlt_s.{first} FROM \
             {staging} _rdlt_s WHERE "
        ));
        self.computed(&mut sql, versioned, &[OPENED, BURIED]);
        sql.push(&format!(
            ") AND EXISTS (SELECT 1 FROM {staging} _rdlt_s WHERE "
        ));
        self.computed(&mut sql, versioned, &[OPENED, BURIED]);
        sql.push(&format!(" AND {})", versioned.on("_rdlt_s", tombstones)));
        sql.finish()
    }

    /// The statement deleting the tombstones sequenced before the bound the commit raised, and the
    /// old bound, where it raised one.
    ///
    /// Without a new bound the tombstones are not read: those it deletes are found by their key,
    /// or its absence, through the key's index.
    fn unbound(&self, versioned: &Versioned<'_>) -> Statement {
        let Versioned {
            staging,
            tombstones,
            seq,
            ..
        } = versioned;
        let first = &versioned.keys[0];
        let mut sql = self.sql();
        sql.push(&format!(
            "DELETE FROM {tombstones} WHERE ({tombstones}.{first} IS NULL OR {tombstones}.{first} \
             IN (SELECT _rdlt_t.{first} FROM {staging} _rdlt_s CROSS JOIN {tombstones} _rdlt_t \
             WHERE "
        ));
        self.computed(&mut sql, versioned, &[BOUND]);
        sql.push(&format!(
            " AND _rdlt_t.{seq} < _rdlt_s.{seq})) AND EXISTS (SELECT 1 FROM {staging} _rdlt_s \
             WHERE "
        ));
        self.computed(&mut sql, versioned, &[BOUND]);
        sql.push(&format!(
            " AND ({tombstones}.{first} IS NULL OR {tombstones}.{seq} < _rdlt_s.{seq}))"
        ));
        sql.finish()
    }

    /// The statement inserting into `into` the `names` of the rows the commit computed as `codes`.
    fn moved_versions(
        &self,
        versioned: &Versioned<'_>,
        into: &str,
        names: &str,
        codes: &[i8],
    ) -> Statement {
        let mut sql = self.sql();
        sql.push(&format!(
            "INSERT INTO {into} ({names}) SELECT {names} FROM {} WHERE ",
            versioned.staging
        ));
        self.computed(&mut sql, versioned, codes);
        sql.finish()
    }

    /// A condition on staging rows: those the commit computed as `codes`.
    fn computed(&self, sql: &mut Sql<'_, D>, versioned: &Versioned<'_>, codes: &[i8]) {
        let of = versioned.of;
        self.staged_by(
            sql,
            of.pipeline,
            of.epoch,
            of.segments,
            [STAGING_COLUMNS[0], STAGING_COLUMNS[1], STAGING_COLUMNS[2]],
        );
        let codes: Vec<String> = codes.iter().map(ToString::to_string).collect();
        sql.push(&format!(
            " AND {} IN ({})",
            self.quote(STAGING_COLUMNS[3]),
            codes.join(", ")
        ));
    }
}

impl<'a> Versioned<'a> {
    /// The names `of`'s commit into the history table `target`, whose columns are `columns`,
    /// shares, quoted by `planner`.
    fn new<D: SqlDialect>(
        planner: &SqlPlanner<D>,
        target: &str,
        (key, history): (&MergeKey, &HistoryColumns),
        columns: &[Column],
        of: &'a Of<'a>,
    ) -> Self {
        let quote = |name: &str| planner.quote(name);
        let alias = |name: &str| quote(&unused(name, columns));
        let changes = key.changes.as_ref();
        let at = changes.and_then(|changes| match &changes.deletion {
            Deletion::Soft { at } => Some(quote(at)),
            Deletion::Hard => None,
        });
        Self {
            of,
            changed: changes
                .map(|changes| Changed::new(planner, target, (key, changes), columns, of)),
            columns: columns.iter().map(|column| quote(&column.name)).collect(),
            target: quote(target),
            staging: quote(&planner.staging_table(&of.staged.name)),
            tombstones: quote(&planner.tombstone_table(&of.staged.name)),
            keys: key.columns.iter().map(|column| quote(column)).collect(),
            seq: quote(&key.seq),
            op: changes.map(|changes| quote(&changes.op)),
            hard: changes.is_some() && at.is_none(),
            at,
            valid_from: quote(&history.valid_from),
            valid_to: quote(&history.valid_to),
            is_current: quote(&history.is_current),
            row_hash: quote(&history.row_hash),
            aliases: Aliases {
                kind: alias("_rdlt_kind"),
                pos: alias("_rdlt_pos"),
                last: alias("_rdlt_last"),
                gone: alias("_rdlt_gone"),
                acts: alias("_rdlt_acts"),
                run: alias("_rdlt_run"),
                floor: alias("_rdlt_floor"),
                began: alias("_rdlt_began"),
                next: alias("_rdlt_next"),
                from: alias("_rdlt_from"),
                q: alias("_rdlt_q"),
            },
        }
    }

    /// The table's columns.
    fn names(&self) -> String {
        self.columns.join(", ")
    }

    /// The condition that the rows `left` and `right` name hold the same key.
    fn on(&self, left: &str, right: &str) -> String {
        let pairs: Vec<String> = self
            .keys
            .iter()
            .map(|key| format!("{left}.{key} = {right}.{key}"))
            .collect();
        pairs.join(" AND ")
    }
}
