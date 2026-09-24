//! Destination tables, the staging table beside each, generation tables, and changing their
//! columns.

use super::{Column, SqlDialect, SqlPlanner, Statement};
use crate::destination::{TableChange, TableRef};
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::GenerationId;
use crate::types::{Field, LogicalType};

/// The columns a staging table has before the table's own: who staged each row, at which epoch,
/// in which segment, and for which generation (null for the table itself).
pub const STAGING_COLUMNS: [&str; 4] = [
    "_rdlt_pipeline",
    "_rdlt_epoch",
    "_rdlt_segment",
    "_rdlt_generation",
];

/// The staging table of the table `name`.
pub fn staging_table(name: &str) -> String {
    format!("_rdlt_staging__{name}")
}

/// The table holding `generation` of the table `name` until it is swapped in.
pub fn generation_table(name: &str, generation: GenerationId) -> String {
    format!("_rdlt_generation_{generation}__{name}")
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// The table rows for `table` are published into: its generation's table, or itself.
    pub fn target(&self, table: &TableRef) -> String {
        match table.generation {
            Some(generation) => generation_table(&table.name, generation),
            None => table.name.to_string(),
        }
    }

    /// The statements creating the generation table `table` writes with the columns of its base
    /// table, `base`, where the generation was never created; nothing when the base is missing.
    pub fn generation(&self, table: &TableRef, base: &[Column]) -> Vec<Statement> {
        if table.generation.is_none() || base.is_empty() {
            return Vec::new();
        }
        let columns = base
            .iter()
            .map(|column| format!("{} {}", self.quote(&column.name), column.declared));
        let mut plan = vec![self.create(&self.target(table), columns)];
        plan.extend(self.register(table));
        plan
    }

    /// The statements applying `change`, given the columns its target and staging tables have
    /// now, empty where a table is missing.
    ///
    /// A column the change declares at a type the existing column does not hold is a `Data` error
    /// coded `schema_conflict`, and so is a widen the dialect cannot apply; nothing is planned
    /// then. A change the tables already reflect plans nothing.
    pub fn change(
        &self,
        change: &TableChange,
        target: &[Column],
        staging: &[Column],
    ) -> Result<Vec<Statement>> {
        let table = change.table();
        let names = [self.target(table), staging_table(&table.name)];
        match change {
            TableChange::Create { schema, .. } => {
                let fields: Vec<&Field> = schema.fields().iter().collect();
                self.fields(table, &names, [target, staging], &fields, true)
            }
            TableChange::AddColumn { field, .. } => {
                if target.is_empty() {
                    return Err(ConnectorError::data(format!(
                        "table {} does not exist",
                        names[0]
                    )));
                }
                self.fields(table, &names, [target, staging], &[field], false)
            }
            TableChange::Widen { column, to, .. } => {
                if !target
                    .iter()
                    .any(|existing| existing.name == column.as_ref())
                {
                    return Err(ConnectorError::data(format!(
                        "table {} has no column {column}",
                        names[0]
                    )));
                }
                let declared = self.declared(to)?;
                let mut plan = Vec::new();
                for (name, columns) in names.iter().zip([target, staging]) {
                    let Some(existing) = columns.iter().find(|existing| existing.name == **column)
                    else {
                        continue;
                    };
                    if self.dialect.holds(&existing.declared, to) {
                        continue;
                    }
                    let Some(sql) = self.dialect.widen(name, column, &declared) else {
                        return Err(conflict(name, existing, to));
                    };
                    plan.push(Statement {
                        sql,
                        params: Vec::new(),
                    });
                }
                Ok(plan)
            }
        }
    }

    /// Creates the tables of `names` that are missing with `fields`, and adds the fields an
    /// existing one lacks; `create` also creates the key index of a new merge table.
    fn fields(
        &self,
        table: &TableRef,
        [target_name, staging_name]: &[String; 2],
        [target, staging]: [&[Column]; 2],
        fields: &[&Field],
        create: bool,
    ) -> Result<Vec<Statement>> {
        let declared = fields
            .iter()
            .map(|field| Ok((field.name(), self.declared(field.logical_type())?)))
            .collect::<Result<Vec<_>>>()?;
        for (name, columns) in [(target_name, target), (staging_name, staging)] {
            for field in fields {
                if let Some(existing) = columns.iter().find(|column| column.name == field.name())
                    && !self.dialect.holds(&existing.declared, field.logical_type())
                {
                    return Err(conflict(name, existing, field.logical_type()));
                }
            }
        }
        let mut plan = Vec::new();
        if target.is_empty() {
            plan.extend(self.create_target(table, target_name, fields, &declared, create));
        } else {
            plan.extend(self.add_missing(target_name, target, &declared));
        }
        if staging.is_empty() {
            plan.push(self.create_staging(staging_name, target, &declared));
        } else {
            plan.extend(self.add_missing(staging_name, staging, &declared));
        }
        Ok(plan)
    }

    /// Creates the table `name` with `fields`, declared as `declared`, and the key index of a
    /// merge table when `create` asks for it.
    fn create_target(
        &self,
        table: &TableRef,
        name: &str,
        fields: &[&Field],
        declared: &[(&str, String)],
        create: bool,
    ) -> Vec<Statement> {
        let columns = fields
            .iter()
            .zip(declared)
            .map(|(field, (column, declared))| {
                let null = if field.is_nullable() { "" } else { " NOT NULL" };
                format!("{} {declared}{null}", self.quote(column))
            });
        let mut plan = vec![self.create(name, columns)];
        if let Some(key) = table.merge.as_ref().filter(|_| create) {
            let columns: Vec<String> = key.columns.iter().map(|c| self.quote(c)).collect();
            plan.push(Statement {
                sql: format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})",
                    self.quote(&format!("_rdlt_key__{name}")),
                    self.quote(name),
                    columns.join(", ")
                ),
                params: Vec::new(),
            });
        }
        plan
    }

    /// Creates the staging table `name` with the staging columns, the columns `target` has and
    /// `declared`, all nullable.
    fn create_staging(
        &self,
        name: &str,
        target: &[Column],
        declared: &[(&str, String)],
    ) -> Statement {
        let mut columns: Vec<(&str, String)> = target
            .iter()
            .map(|column| (column.name.as_str(), column.declared.clone()))
            .filter(|(column, _)| !declared.iter().any(|(field, _)| field == column))
            .collect();
        columns.extend(
            declared
                .iter()
                .map(|(column, declared)| (*column, declared.clone())),
        );
        let [pipeline, epoch, segment, generation] = STAGING_COLUMNS.map(|c| self.quote(c));
        let staging = [
            format!("{pipeline} {} NOT NULL", self.text),
            format!("{epoch} {} NOT NULL", self.integer),
            format!("{segment} {} NOT NULL", self.integer),
            format!("{generation} {}", self.integer),
        ];
        let data = columns
            .iter()
            .map(|(column, declared)| format!("{} {declared}", self.quote(column)));
        self.create(name, staging.into_iter().chain(data))
    }

    fn create(&self, name: &str, columns: impl Iterator<Item = String>) -> Statement {
        Statement {
            sql: format!(
                "CREATE TABLE IF NOT EXISTS {} ({})",
                self.quote(name),
                columns.collect::<Vec<_>>().join(", ")
            ),
            params: Vec::new(),
        }
    }

    /// Adds each of `declared` that `columns` lacks to the table `name`, as nullable.
    fn add_missing(
        &self,
        name: &str,
        columns: &[Column],
        declared: &[(&str, String)],
    ) -> Vec<Statement> {
        declared
            .iter()
            .filter(|(field, _)| !columns.iter().any(|column| column.name == *field))
            .map(|(field, declared)| Statement {
                sql: format!(
                    "ALTER TABLE {} ADD COLUMN {} {declared}",
                    self.quote(name),
                    self.quote(field)
                ),
                params: Vec::new(),
            })
            .collect()
    }

    /// The type a column of `logical` is declared with; the engine lowers every column to a type
    /// the destination stores, so any other is refused.
    fn declared(&self, logical: &LogicalType) -> Result<String, ConnectorError> {
        self.dialect.column_type(logical).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!("the destination does not store {logical:?}"),
            )
        })
    }
}

/// The error for `existing`, a column of the table `table`, not holding `logical`.
fn conflict(table: &str, existing: &Column, logical: &LogicalType) -> ConnectorError {
    ConnectorError::data(format!(
        "table {table} already has column {} as {}, which does not hold {logical:?}",
        existing.name, existing.declared
    ))
    .with_code("schema_conflict")
}
