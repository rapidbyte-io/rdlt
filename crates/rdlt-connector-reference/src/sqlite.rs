//! A transactional destination that writes to a SQLite database file, through `sqlgen`.

mod database;
mod session;
mod values;

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{CATALOG_TABLES, SqlPlanner, Sqlite};
use rdlt_connector::{
    IdentifierCase, IdentifierChars, IdentifierRules, SchemaChanges, TypeKind, WriteModes,
};
use schemars::JsonSchema;
use serde::Deserialize;

use database::Database;
pub use session::{SqliteSession, SqliteWriter};

/// Configuration of [`SqliteDestination`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteDestinationConfig {
    /// The database file, created when missing.
    pub path: PathBuf,
}

/// Loads into a SQLite database: each commit is one transaction that publishes the staged rows,
/// writes the state and records the receipt, fenced by the pipeline's epoch.
///
/// Every table has a staging table beside it; a replace generation fills its own table until the
/// commit that finishes it renames it over the table. Column types are SQLite's storage classes,
/// so the engine stores decimals, times, UUIDs and JSON as text.
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
        Ok(Self {
            path: config.path,
            planner: Arc::new(SqlPlanner::try_new(Sqlite)?),
        })
    }

    async fn check(&self) -> Result<()> {
        let database = Database::open(self.path.clone()).await?;
        let planner = Arc::clone(&self.planner);
        database
            .transaction(move |transaction| database::run_all(transaction, &planner.bootstrap()))
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
/// integer and float families.
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
        history: false,
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
    capabilities.identifiers = IdentifierRules {
        case: IdentifierCase::Lower,
        max_len: NonZeroU16::new(128).expect("128 is non-zero"),
        chars: IdentifierChars::Any,
        reserved: CATALOG_TABLES.iter().map(|&name| name.to_owned()).collect(),
    };
    capabilities
}

/// Every row of `table` in the database at `path`, as one batch; none when the table is missing.
///
/// Columns read back as their storage class: integers as `Int64`, floats as `Float64`.
pub fn published(path: impl Into<PathBuf>, table: &str) -> Result<Vec<RecordBatch>> {
    let connection = database::connect(&path.into())?;
    values::read_table(&connection, &Sqlite, table)
}
