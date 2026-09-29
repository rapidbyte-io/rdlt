//! Merging a change stream's staged rows into its table, as the reference merge does: each row
//! applies in sequence order, only past the row its key holds and the table's tombstones, as an
//! insert, update, delete or truncate.
//!
//! One statement reads the table, its tombstones and the commit's staged rows, and computes into
//! staging each changed key's row, each key a hard delete removed, and the bound a hard truncate
//! raised; the statements after it only move those rows, so none reads what another changed.

mod hard;
mod soft;
#[cfg(test)]
mod tests;

use super::super::tables::STAGING_COLUMNS;
use super::super::{Column, Sql, SqlDialect, SqlPlanner, SqlValue, Statement, integer};
use super::keyed::{Of, unused};
use crate::destination::{ChangeColumns, Deletion, MergeKey};
use crate::error::{ConnectorError, Result};

/// A key's row once the commit's changes apply, with an applied insert or update among them.
const MERGED: i8 = 4;
/// A key's row once the commit's changes apply, only soft deletes and truncates among them.
const MARKED: i8 = 5;
/// A key a hard delete removed: its tombstone.
const BURIED: i8 = 6;
/// The bound a hard truncate raised, naming no key.
const BOUND: i8 = 7;

/// The names one change table's commit shares across its statements, quoted.
pub(super) struct Changed<'a> {
    of: &'a Of<'a>,
    /// The table's columns, in order.
    columns: Vec<String>,
    target: String,
    staging: String,
    tombstones: String,
    keys: Vec<String>,
    seq: String,
    op: String,
    unchanged: Option<String>,
    /// The alias of a computed sequence, and of a staged row's rank, which no column is named.
    q: String,
    rank: String,
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statements merging the change stream's rows `of` names into `target`, whose columns
    /// are `columns`, by `key`, then removing what the commit computed from staging; the staged
    /// rows themselves are removed after.
    pub(super) fn changed(
        &self,
        target: &str,
        key: &MergeKey,
        changes: &ChangeColumns,
        columns: &[Column],
        of: &Of<'_>,
    ) -> Result<Vec<Statement>> {
        if of.staged.generation.is_some() {
            return Err(ConnectorError::internal(
                "a change stream merges into its table, never a generation",
            ));
        }
        let quote = |name: &str| self.quote(name);
        let merging = Changed {
            of,
            columns: columns.iter().map(|column| quote(&column.name)).collect(),
            target: quote(target),
            staging: quote(&self.staging_table(&of.staged.name)),
            tombstones: quote(&self.tombstone_table(&of.staged.name)),
            keys: key.columns.iter().map(|column| quote(column)).collect(),
            seq: quote(&key.seq),
            op: quote(&changes.op),
            unchanged: changes.unchanged.as_deref().map(quote),
            q: quote(&unused("_rdlt_q", columns)),
            rank: quote(&unused("_rdlt_rank", columns)),
        };
        let computed = match &changes.deletion {
            Deletion::Hard => self.hard(&merging),
            Deletion::Soft { at } => self.soft(&merging, at)?,
        };
        Ok(vec![
            computed.finish(),
            self.keyed(&merging, &merging.target, &[MERGED, MARKED, BURIED]),
            self.bounded(&merging, &merging.target, false),
            self.moved(
                &merging,
                &merging.target,
                &merging.names(""),
                &[MERGED, MARKED],
            ),
            self.keyed(&merging, &merging.tombstones, &[MERGED, BURIED]),
            self.bounded(&merging, &merging.tombstones, true),
            self.moved(
                &merging,
                &merging.tombstones,
                &merging.tombstone_names(),
                &[BURIED, BOUND],
            ),
        ])
    }

    /// The start of the statement computing rows into staging: the insert, then the common table
    /// expressions every mode reads: the commit's staged rows once each, the tombstones' bound,
    /// and the rows past the bound, their key's tombstone and its row.
    fn computing<'a>(&'a self, changed: &Changed<'_>) -> Sql<'a, D> {
        let Changed {
            staging,
            op,
            seq,
            rank,
            q,
            tombstones,
            target,
            ..
        } = changed;
        let mut sql = self.sql();
        let staged = changed.staged_names();
        let staging_columns: Vec<String> = STAGING_COLUMNS.iter().map(|c| self.quote(c)).collect();
        sql.push(&format!(
            "INSERT INTO {staging} ({}, {}, {op}) WITH _rdlt_staged AS (SELECT {staged} FROM \
             (SELECT {staged}, ROW_NUMBER() OVER (PARTITION BY {}, {seq} ORDER BY {op}) AS {rank} \
             FROM {staging} WHERE ",
            staging_columns.join(", "),
            changed.names(""),
            changed.keys.join(", "),
        ));
        self.rows_of(
            &mut sql,
            changed.of.staged,
            changed.of.pipeline,
            changed.of.epoch,
            changed.of.segments,
        );
        let first = &changed.keys[0];
        sql.push(&format!(
            " AND {op} IN (0, 1, 2, 3)) _rdlt_ranked WHERE {rank} = 1), \
             _rdlt_bound AS (SELECT MAX({seq}) AS {q} FROM {tombstones} WHERE {first} IS NULL), \
             _rdlt_admitted AS (SELECT _rdlt_s.* FROM _rdlt_staged _rdlt_s WHERE NOT EXISTS \
             (SELECT 1 FROM _rdlt_bound _rdlt_b WHERE _rdlt_s.{seq} < _rdlt_b.{q}) AND \
             (_rdlt_s.{op} = 3 OR (NOT EXISTS (SELECT 1 FROM {tombstones} _rdlt_t WHERE {} AND \
             _rdlt_t.{seq} >= _rdlt_s.{seq}) AND NOT EXISTS (SELECT 1 FROM {target} _rdlt_p WHERE \
             {} AND _rdlt_p.{seq} >= _rdlt_s.{seq}))))",
            changed.on("_rdlt_t", "_rdlt_s"),
            changed.on("_rdlt_p", "_rdlt_s"),
        ));
        sql
    }

    /// The statement deleting from `table` the rows of each key the commit computed a row coded
    /// `codes` for: found by their first key column among the computed rows', through the key's
    /// index, then matched on every key column.
    fn keyed(&self, changed: &Changed<'_>, table: &str, codes: &[i8]) -> Statement {
        let op = &changed.op;
        let codes: Vec<String> = codes.iter().map(ToString::to_string).collect();
        let codes = codes.join(", ");
        let first = &changed.keys[0];
        let mut sql = self.sql();
        sql.push(&format!(
            "DELETE FROM {table} WHERE {table}.{first} IN (SELECT _rdlt_s.{first} FROM {} _rdlt_s \
             WHERE ",
            changed.staging
        ));
        self.computed_rows(&mut sql, changed);
        sql.push(&format!(
            " AND _rdlt_s.{op} IN ({codes})) AND EXISTS (SELECT 1 FROM {} _rdlt_s WHERE ",
            changed.staging
        ));
        self.computed_rows(&mut sql, changed);
        sql.push(&format!(
            " AND _rdlt_s.{op} IN ({codes}) AND {})",
            changed.on("_rdlt_s", table)
        ));
        sql.finish()
    }

    /// The statement deleting from `table`, where the commit raised the bound, its rows sequenced
    /// before it, and with `bound_too`, the rows naming no key: the old bound, never above a new
    /// one.
    ///
    /// Without a new bound, the table is not read.
    fn bounded(&self, changed: &Changed<'_>, table: &str, bound_too: bool) -> Statement {
        let (op, seq) = (&changed.op, &changed.seq);
        let mut sql = self.sql();
        sql.push(&format!(
            "DELETE FROM {table} WHERE EXISTS (SELECT 1 FROM {} _rdlt_s WHERE ",
            changed.staging
        ));
        self.computed_rows(&mut sql, changed);
        sql.push(&format!(
            " AND _rdlt_s.{op} = {BOUND}) AND ({table}.{seq} < (SELECT MAX(_rdlt_s.{seq}) FROM {} \
             _rdlt_s WHERE ",
            changed.staging
        ));
        self.computed_rows(&mut sql, changed);
        let old = if bound_too {
            format!(" OR {table}.{} IS NULL", changed.keys[0])
        } else {
            String::new()
        };
        sql.push(&format!(" AND _rdlt_s.{op} = {BOUND}){old})"));
        sql.finish()
    }

    /// The statement inserting into `into` the `names` of the rows the commit computed as `codes`.
    fn moved(&self, changed: &Changed<'_>, into: &str, names: &str, codes: &[i8]) -> Statement {
        let codes: Vec<String> = codes.iter().map(ToString::to_string).collect();
        let mut sql = self.sql();
        sql.push(&format!(
            "INSERT INTO {into} ({names}) SELECT {names} FROM {} WHERE ",
            changed.staging
        ));
        self.computed_rows(&mut sql, changed);
        sql.push(&format!(" AND {} IN ({})", changed.op, codes.join(", ")));
        sql.finish()
    }

    /// A condition on staging rows: those of the commit's segments, staged or computed.
    fn computed_rows(&self, sql: &mut Sql<'_, D>, changed: &Changed<'_>) {
        let of = changed.of;
        self.rows_of(sql, of.staged, of.pipeline, of.epoch, of.segments);
    }
}

impl Changed<'_> {
    /// The values of the staging columns of a computed row: who staged the commit's rows, in one
    /// of its segments, bound to `sql`.
    fn staged_by<D: SqlDialect>(&self, sql: &mut Sql<'_, D>) -> String {
        let of = self.of;
        let segment = of
            .segments
            .ranges()
            .first()
            .map_or(0, |range| range.first.0);
        let values = [
            SqlValue::Text(of.pipeline.to_string()),
            integer(of.epoch.0),
            integer(segment),
        ]
        .map(|value| sql.bind(value));
        format!("{}, NULL", values.join(", "))
    }

    /// The table's columns, each qualified by `alias` where there is one.
    fn names(&self, alias: &str) -> String {
        let prefix = if alias.is_empty() {
            String::new()
        } else {
            format!("{alias}.")
        };
        let names: Vec<String> = self
            .columns
            .iter()
            .map(|column| format!("{prefix}{column}"))
            .collect();
        names.join(", ")
    }

    /// The columns a staged row has: the table's, its op and its unchanged flags.
    fn staged_names(&self) -> String {
        let mut names = format!("{}, {}", self.names(""), self.op);
        if let Some(unchanged) = &self.unchanged {
            names = format!("{names}, {unchanged}");
        }
        names
    }

    /// The columns of a tombstone: the key's and the sequence.
    fn tombstone_names(&self) -> String {
        format!("{}, {}", self.keys.join(", "), self.seq)
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

    /// The key's columns, each qualified by `alias`.
    fn keyed(&self, alias: &str) -> String {
        let keys: Vec<String> = self
            .keys
            .iter()
            .map(|key| format!("{alias}.{key}"))
            .collect();
        keys.join(", ")
    }

    /// The condition that the row `alias` flags the column at `ordinal` unchanged.
    fn flagged(&self, alias: &str, ordinal: usize) -> String {
        match &self.unchanged {
            Some(unchanged) => format!("COALESCE({alias}.{unchanged}, '') LIKE '%,{ordinal},%'"),
            None => "1 = 0".to_owned(),
        }
    }

    /// Whether the table's column `column`, quoted, is one of its key's or its sequence.
    fn is_key_or_seq(&self, column: &str) -> bool {
        *column == self.seq || self.keys.iter().any(|key| key == column)
    }

    /// The value of the column `column`, quoted, at `ordinal`, of the key `outer` names, as the
    /// last of the key's upserts not flagging it unchanged sets it, or else as the row `kept`
    /// holds it.
    fn chained(&self, column: &str, ordinal: usize, outer: &str, kept: &str) -> String {
        self.chained_past(column, ordinal, outer, (kept, ""))
    }

    /// As [`Changed::chained`], the row `kept` holds counting only where `condition`, on it as
    /// `_rdlt_k`, holds.
    fn chained_past(
        &self,
        column: &str,
        ordinal: usize,
        outer: &str,
        (kept, condition): (&str, &str),
    ) -> String {
        let seq = &self.seq;
        format!(
            "CASE WHEN EXISTS (SELECT 1 FROM _rdlt_upserts _rdlt_v WHERE {on_v} AND NOT {flag_v}) \
             THEN (SELECT _rdlt_v.{column} FROM _rdlt_upserts _rdlt_v WHERE {on_v} AND \
             _rdlt_v.{seq} = (SELECT MAX(_rdlt_w.{seq}) FROM _rdlt_upserts _rdlt_w WHERE {on_w} \
             AND NOT {flag_w})) ELSE (SELECT _rdlt_k.{column} FROM {kept} _rdlt_k WHERE {on_k}\
             {condition}) END",
            on_v = self.on("_rdlt_v", outer),
            flag_v = self.flagged("_rdlt_v", ordinal),
            on_w = self.on("_rdlt_w", outer),
            flag_w = self.flagged("_rdlt_w", ordinal),
            on_k = self.on("_rdlt_k", outer),
        )
    }
}
