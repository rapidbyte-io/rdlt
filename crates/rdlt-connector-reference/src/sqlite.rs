//! A transactional destination that writes to a SQLite database file, through `sqlgen`.

mod database;
mod location;
mod session;
#[cfg(test)]
mod tests;
mod values;

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{STAGING_COLUMNS, SqlPlanner, Sqlite, TABLE_PREFIX};
use rdlt_connector::{
    DeleteModes, IdentifierCase, IdentifierChars, IdentifierRules, SchemaChanges, TypeKind,
    WriteModes,
};
use schemars::JsonSchema;
use serde::Deserialize;

use database::Database;
pub use session::{SqliteSession, SqliteWriter};

/// Configuration of [`SqliteDestination`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteDestinationConfig {
    /// The database file, created when missing, for its user alone.
    ///
    /// Its directory is its user's alone to write, and a path starting with `file:` is refused.
    pub path: PathBuf,
}

/// Loads into a SQLite database: each commit is one transaction that publishes the staged rows,
/// writes the state and records the receipt, fenced by the pipeline's epoch.
///
/// Every table has a staging table beside it; a replace generation fills its own table until the
/// commit that finishes it renames it over the table. Column types are SQLite's storage classes,
/// so the engine stores decimals, times, UUIDs and JSON as text.
///
/// Two floats do not load: a float that is no number, which SQLite stores as a null, and negative
/// zero, which a `REAL` column reads back as zero. A batch holding either is refused where it
/// is flushed, as a data error coded `float_unstorable`, and nothing of it is staged; a stream
/// whose floats may be either needs them made nulls or text before this destination.
#[derive(Debug)]
pub struct SqliteDestination {
    path: PathBuf,
    planner: Arc<SqlPlanner<Sqlite>>,
}

#[destination(id = "io.rapidbyte.sqlite")]
impl DestinationConnector for SqliteDestination {
    type Config = SqliteDestinationConfig;
    type Session = SqliteSession;

    fn capabilities(&self) -> Capabilities {
        capabilities()
    }

    async fn connect(config: SqliteDestinationConfig, _context: &ConnectContext) -> Result<Self> {
        // A path SQLite would read as a URI is refused before anything is opened.
        location::named(&config.path)?;
        Ok(Self {
            path: config.path,
            planner: Arc::new(SqlPlanner::try_new(Sqlite)?),
        })
    }

    async fn check(&self) -> Result<()> {
        let database = Database::open(self.path.clone()).await?;
        let planner = Arc::clone(&self.planner);
        database
            .transaction(move |transaction| {
                session::owners::catalog(transaction, &planner)?;
                database::run_all(transaction, &planner.bootstrap())
            })
            .await
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<SqliteSession>> {
        let database = Database::open(self.path.clone()).await?;
        SqliteSession::open(
            database,
            Arc::clone(&self.planner),
            context.pipeline.clone(),
        )
        .await
    }
}

/// What the SQLite destination stores: SQLite's storage classes, widened in place within the
/// integer and float families; tables never take `sqlgen`'s prefix, SQLite's or its table-valued
/// pragmas', and columns never take the staging columns' names.
fn capabilities() -> Capabilities {
    use TypeKind as K;
    let integers = [K::Int8, K::Int16, K::Int32, K::Int64];
    let mut widenings = BTreeSet::from([(K::Float32, K::Float64)]);
    for (index, from) in integers.iter().enumerate() {
        widenings.extend(integers[index + 1..].iter().map(|to| (*from, *to)));
    }
    let mut capabilities = Capabilities::minimal();
    capabilities.write_modes = WriteModes {
        append: true,
        replace: true,
        merge: true,
        history: true,
    };
    capabilities.types = BTreeSet::from([
        K::Bool,
        K::Int8,
        K::Int16,
        K::Int32,
        K::Int64,
        K::Float32,
        K::Float64,
        K::Utf8,
        K::Binary,
    ]);
    capabilities.schema_changes = SchemaChanges {
        add_column: true,
        widenings,
    };
    capabilities.delete_modes = DeleteModes {
        hard: true,
        soft: true,
    };
    capabilities.partial_updates = true;
    capabilities.merge_changes = true;
    capabilities.drop_tables = true;
    capabilities.identifiers = IdentifierRules {
        case: IdentifierCase::Lower,
        max_len: NonZeroU16::new(128).expect("128 is non-zero"),
        chars: IdentifierChars::Any,
        reserved: STAGING_COLUMNS
            .iter()
            .map(|&name| name.to_owned())
            .collect(),
        reserved_table_prefixes: [TABLE_PREFIX, "sqlite_", "pragma_"]
            .map(str::to_owned)
            .into(),
    };
    capabilities
}

#[cfg(feature = "certify")]
impl ReadBack for SqliteDestination {
    async fn published(&self, table: &TableRef, rows: PublishedRows) -> Result<()> {
        let (path, name) = (self.path.clone(), table.name.clone());
        crate::blocking::blocking(move || {
            let Some((connection, name)) = readable(&path, &name)? else {
                return Ok(());
            };
            values::read_table_each(&connection, &Sqlite, &name, &mut |batch| {
                rows.blocking_send(batch)
            })
        })
        .await
    }
}

/// A connection that only reads the database at `path` and the name it holds `table` under,
/// where a pipeline owns the table; none where the table or the database is missing.
///
/// Reading changes nothing and creates no database. A name the destination keeps, as the
/// catalog's, is a `Config` error coded `table_name_reserved`, and a table no pipeline owns one
/// coded `table_unowned`.
fn readable(path: &std::path::Path, table: &str) -> Result<Option<(rusqlite::Connection, String)>> {
    let planner = SqlPlanner::try_new(Sqlite)?;
    planner.named(table)?;
    let Some(connection) = database::reading(path)? else {
        return Ok(None);
    };
    let name = session::owners::published(&connection, &planner, table)?;
    Ok(name.map(|name| (connection, name)))
}

/// Every row of `table` in the database at `path`, in batches of bounded size; none where the
/// table or the database is missing.
///
/// Columns read back as their storage class: integers as `Int64`, floats as `Float64`. Only a
/// table a pipeline owns is read, as [`readable`] says.
pub fn published(path: impl Into<PathBuf>, table: &str) -> Result<Vec<RecordBatch>> {
    match readable(&path.into(), table)? {
        Some((connection, name)) => values::read_table(&connection, &Sqlite, &name),
        None => Ok(Vec::new()),
    }
}
