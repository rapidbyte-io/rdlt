//! Writing a row whose key a row may already hold, in the style the dialect writes it.

#[cfg(test)]
mod tests;

use super::{SqlDialect, SqlPlanner, SqlValue, Statement};

/// How a dialect writes a row whose key a row may already hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Upserts {
    /// `INSERT … ON CONFLICT (key) DO NOTHING`, or `DO UPDATE`, as SQLite and PostgreSQL write it:
    /// one statement, whatever other transactions write.
    OnConflict,
    /// Standard SQL: the row holding the key updated, then the row inserted where none holds it.
    ///
    /// A transaction writing the key at the same time as another may fail on the key's
    /// constraint, which the destination reports as any write that failed.
    Guarded,
}

/// A row to write: its key's columns and values, then its other columns and values.
pub(super) struct Row<'a> {
    pub(super) table: &'a str,
    pub(super) key: &'a [(&'a str, SqlValue)],
    pub(super) values: &'a [(&'a str, SqlValue)],
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statements writing `row` unless a row holds its key; where `replace`, one that does
    /// takes its values.
    pub(super) fn upsert(&self, row: &Row<'_>, replace: bool) -> Vec<Statement> {
        match self.dialect.upserts() {
            Upserts::OnConflict => vec![self.on_conflict(row, replace)],
            Upserts::Guarded => self.guarded(row, replace),
        }
    }

    fn on_conflict(&self, row: &Row<'_>, replace: bool) -> Statement {
        let mut sql = self.sql();
        let columns: Vec<&str> = row
            .key
            .iter()
            .chain(row.values)
            .map(|(name, _)| *name)
            .collect();
        let values: Vec<String> = row
            .key
            .iter()
            .chain(row.values)
            .map(|(_, value)| sql.bind(value.clone()))
            .collect();
        let key: Vec<&str> = row.key.iter().map(|(name, _)| *name).collect();
        let action = if replace && !row.values.is_empty() {
            let set: Vec<String> = row
                .values
                .iter()
                .map(|(name, _)| format!("{name} = excluded.{name}"))
                .collect();
            format!("DO UPDATE SET {}", set.join(", "))
        } else {
            "DO NOTHING".to_owned()
        };
        sql.push(&format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {action}",
            row.table,
            columns.join(", "),
            values.join(", "),
            key.join(", ")
        ));
        sql.finish()
    }

    fn guarded(&self, row: &Row<'_>, replace: bool) -> Vec<Statement> {
        let mut plan = Vec::new();
        let matches = |sql: &mut super::Sql<'_, D>| {
            let conditions: Vec<String> = row
                .key
                .iter()
                .map(|(name, value)| format!("{name} = {}", sql.bind(value.clone())))
                .collect();
            conditions.join(" AND ")
        };
        if replace && !row.values.is_empty() {
            let mut update = self.sql();
            let set: Vec<String> = row
                .values
                .iter()
                .map(|(name, value)| format!("{name} = {}", update.bind(value.clone())))
                .collect();
            let key = matches(&mut update);
            update.push(&format!(
                "UPDATE {} SET {} WHERE {key}",
                row.table,
                set.join(", ")
            ));
            plan.push(update.finish());
        }
        let mut insert = self.sql();
        let columns: Vec<&str> = row
            .key
            .iter()
            .chain(row.values)
            .map(|(name, _)| *name)
            .collect();
        let values: Vec<String> = row
            .key
            .iter()
            .chain(row.values)
            .map(|(_, value)| insert.bind(value.clone()))
            .collect();
        let key = matches(&mut insert);
        let from = self
            .dialect
            .values_table()
            .map_or_else(String::new, |values| format!(" FROM {values}"));
        insert.push(&format!(
            "INSERT INTO {table} ({}) SELECT {}{from} WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE \
             {key})",
            columns.join(", "),
            values.join(", "),
            table = row.table,
        ));
        plan.push(insert.finish());
        plan
    }
}
