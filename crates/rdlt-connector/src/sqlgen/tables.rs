//! Destination tables, the staging table beside each, generation tables, and changing their
//! columns.

use sha2::{Digest as _, Sha256};

use super::{Column, Owned, SqlDialect, SqlPlanner, Statement};
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

/// The prefix of every table `sqlgen` keeps: the catalog, staging and generation tables.
pub const TABLE_PREFIX: &str = "_rdlt_";

/// The prefix of a derived name cut to the dialect's identifiers, which no other name the
/// planner makes begins with.
const FITTED_PREFIX: &str = "_rdlt_fit_";

impl<D: SqlDialect> SqlPlanner<D> {
    /// The staging table of the table `name`.
    pub fn staging_table(&self, name: &str) -> String {
        self.fitted(format!("_rdlt_staging__{name}"))
    }

    /// The table holding `generation` of the table `name` until it is swapped in.
    pub fn generation_table(&self, name: &str, generation: GenerationId) -> String {
        self.fitted(format!("_rdlt_generation_{generation}__{name}"))
    }

    /// `derived`, a name derived from a table's, within the dialect's longest identifier: one too
    /// long becomes the reserved prefix of cut names and the SHA-256 of the whole, in base 32.
    ///
    /// No name the planner derives uncut and no catalog table begins with that prefix, so a cut
    /// name meets only another cut name, and only where two names share their SHA-256.
    pub(super) fn fitted(&self, derived: String) -> String {
        match self.dialect.max_identifier() {
            Some(max) if derived.len() > max => {
                format!(
                    "{FITTED_PREFIX}{}",
                    base32(&Sha256::digest(derived.as_bytes()))
                )
            }
            _ => derived,
        }
    }

    /// Refuses `table` where a table of `owned`, the tables pipelines own, or of `generations`,
    /// every generation table with its base, would share one of the tables or indexes derived
    /// from its name: its staging, tombstones, root index and key indexes, and for a generation
    /// its generation table with that table's indexes.
    ///
    /// Two such tables would publish each other's rows, so the clash is a `Config` error, coded
    /// `table_name_clash`, which renaming either table resolves.
    pub fn distinct(
        &self,
        table: &TableRef,
        owned: &[String],
        generations: &[(String, String)],
    ) -> Result<()> {
        let name = &*table.name;
        let indexed = |data: String| {
            [
                self.key_index_name(&data),
                self.root_index_name(&data),
                data,
            ]
        };
        let derived = |table: &str| {
            let (staging, tombstones) = (self.staging_table(table), self.tombstone_table(table));
            let mut names = vec![self.key_index_name(table), self.root_index_name(table)];
            names.extend(
                [indexed(staging), indexed(tombstones)]
                    .into_iter()
                    .flatten(),
            );
            names
        };
        let mut own = derived(name);
        if table.generation.is_some() {
            own.extend(indexed(self.target(table)));
        }
        let tables = owned
            .iter()
            .filter(|other| other.as_str() != name)
            .map(|other| (other, derived(other)));
        let filling = generations
            .iter()
            .filter(|(_, base)| base != name)
            .map(|(generation, base)| (base, indexed(generation.clone()).to_vec()));
        let clash = tables
            .chain(filling)
            .find(|(_, names)| names.iter().any(|derived| own.contains(derived)));
        match clash {
            Some((other, _)) => Err(ConnectorError::config(format!(
                "tables {name} and {other} would share the tables derived from their names, which \
                 the destination cuts to its longest identifier"
            ))
            .with_code("table_name_clash")),
            None => Ok(()),
        }
    }

    /// The table rows for `table` are published into: its generation's table, or itself.
    pub fn target(&self, table: &TableRef) -> String {
        match table.generation {
            Some(generation) => self.generation_table(&table.name, generation),
            None => table.name.to_string(),
        }
    }

    /// The statements creating the generation table `table`, which `owned` names, writes with
    /// the columns of its base table, `base`, where the generation was never created; nothing
    /// when the base is missing.
    pub fn generation(
        &self,
        owned: &Owned,
        table: &TableRef,
        base: &[Column],
    ) -> Result<Vec<Statement>> {
        owned.is(&table.name)?;
        if table.generation.is_none() || base.is_empty() {
            return Ok(Vec::new());
        }
        let name = self.target(table);
        let columns = base
            .iter()
            .map(|column| {
                let declared = self.rendered(&table.name, column)?;
                Ok(format!("{} {declared}", self.quote(&column.name)))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut plan = vec![self.create(&name, columns.into_iter())];
        plan.extend(self.register(owned, table)?);
        Ok(plan)
    }

    /// The statements applying `change` to the table `owned` names, given the columns its target,
    /// staging and tombstones tables have now, empty where a table is missing.
    ///
    /// A widen applies to each of them that has the column: a change stream's tombstones hold
    /// its key.
    ///
    /// A column the change declares at a type the existing column does not hold is a `Data` error
    /// coded `schema_conflict`, and so is a widen the dialect cannot apply; nothing is planned
    /// then. A change the tables already reflect plans nothing.
    pub fn change(
        &self,
        owned: &Owned,
        change: &TableChange,
        [target, staging, tombstones]: [&[Column]; 3],
    ) -> Result<Vec<Statement>> {
        let table = change.table();
        owned.is(&table.name)?;
        let names = [
            self.target(table),
            self.staging_table(&table.name),
            self.tombstone_table(&table.name),
        ];
        match change {
            TableChange::Create { schema, .. } => {
                let fields: Vec<&Field> = schema.fields().iter().collect();
                self.fields([&names[0], &names[1]], [target, staging], &fields)
            }
            TableChange::AddColumn { field, .. } => {
                if target.is_empty() {
                    return Err(ConnectorError::data(format!(
                        "table {} does not exist",
                        names[0]
                    )));
                }
                self.fields([&names[0], &names[1]], [target, staging], &[field])
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
                // A change stream's tombstones hold the key, so they widen with it.
                for (name, columns) in names.iter().zip([target, staging, tombstones]) {
                    let Some(existing) = columns.iter().find(|existing| existing.name == **column)
                    else {
                        continue;
                    };
                    if self.dialect.holds(&existing.declared, to) {
                        continue;
                    }
                    let (table, widened) = (self.quote(name), self.quote(column));
                    let Some(sql) = self.dialect.widen(&table, &widened, &declared) else {
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
    /// existing one lacks.
    fn fields(
        &self,
        [target_name, staging_name]: [&String; 2],
        [target, staging]: [&[Column]; 2],
        fields: &[&Field],
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
            plan.push(self.create_target(target_name, fields, &declared));
        } else {
            plan.extend(self.add_missing(target_name, target, &declared));
        }
        if staging.is_empty() {
            plan.push(self.create_staging([target_name, staging_name], target, &declared)?);
        } else {
            plan.extend(self.add_missing(staging_name, staging, &declared));
        }
        Ok(plan)
    }

    /// Creates the table `name` with `fields`, declared as `declared`.
    fn create_target(
        &self,
        name: &str,
        fields: &[&Field],
        declared: &[(&str, String)],
    ) -> Statement {
        let columns = fields
            .iter()
            .zip(declared)
            .map(|(field, (column, declared))| {
                let null = if field.is_nullable() { "" } else { " NOT NULL" };
                format!("{} {declared}{null}", self.quote(column))
            });
        self.create(name, columns)
    }

    /// Creates the staging table `name` of the table `target_name` with the staging columns, the
    /// columns `target` has and `declared`, all nullable.
    fn create_staging(
        &self,
        [target_name, name]: [&String; 2],
        target: &[Column],
        declared: &[(&str, String)],
    ) -> Result<Statement> {
        let mut columns: Vec<(&str, String)> = Vec::new();
        for column in target {
            if !declared.iter().any(|(field, _)| *field == column.name) {
                columns.push((&column.name, self.rendered(target_name, column)?));
            }
        }
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
        Ok(self.create(name, staging.into_iter().chain(data)))
    }

    /// The type `column` of the table `table` is declared with, as the dialect renders it.
    ///
    /// What the database reports is never written into a statement: a type the dialect does not
    /// declare columns with is a `Data` error coded `schema_conflict`.
    pub(super) fn rendered(&self, table: &str, column: &Column) -> Result<String, ConnectorError> {
        self.dialect.declares(&column.declared).ok_or_else(|| {
            ConnectorError::data(format!(
                "table {table} has column {} of a type the destination does not declare",
                column.name
            ))
            .with_code("schema_conflict")
        })
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

/// `bytes` in base 32, lower case, without padding: every character one any database keeps in an
/// identifier, alike in every case.
fn base32(bytes: &[u8]) -> String {
    let digit = |bits: u32| match u8::try_from(bits & 31).unwrap_or_default() {
        letter @ 0..26 => char::from(b'a' + letter),
        number => char::from(b'2' + (number - 26)),
    };
    let mut text = String::with_capacity(bytes.len() * 8 / 5 + 1);
    let (mut held, mut bits) = (0_u32, 0_u32);
    for byte in bytes {
        held = (held << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            text.push(digit(held >> bits));
        }
    }
    if bits > 0 {
        text.push(digit(held << (5 - bits)));
    }
    text
}
