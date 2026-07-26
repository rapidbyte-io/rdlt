//! Shared unit-test fixtures for the commit-retry family. The bounded-retry
//! loop is exercised from three modules (append in `commit`, property write in
//! `state`, schema evolution in `ensure`); they share ONE mock catalog whose
//! `update_table` conflicts a configured number of times before succeeding — a
//! live competitor cannot be timed into the load→commit window reliably.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Struct,
    TableMetadata, Type,
};
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};

pub(crate) fn test_table() -> Table {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("schema");
    table_with_schema(schema)
}

/// A live table carrying `schema`, built the way a catalog builds one.
///
/// `from_table_creation` RE-ASSIGNS field ids, which is exactly what a REST
/// catalog does on create — so a table built here differs from a freshly mapped
/// schema in precisely the way a real one does. That makes id-sensitivity
/// reproducible without a container, which matters: a container-gated test
/// SKIPS when no runtime is present, and a skipping test is green, so it can
/// never be the evidence that a fix was needed.
pub(crate) fn table_with_schema(schema: Schema) -> Table {
    let creation = TableCreation::builder()
        .name("events".to_string())
        .location("memory://wh/ns/events".to_string())
        .schema(schema)
        .build();
    let metadata: TableMetadata =
        iceberg::spec::TableMetadataBuilder::from_table_creation(creation)
            .expect("metadata builder")
            .build()
            .expect("metadata")
            .metadata;
    Table::builder()
        .metadata(metadata)
        .identifier(TableIdent::new(
            NamespaceIdent::new("ns".into()),
            "events".into(),
        ))
        .file_io(iceberg::io::FileIO::new_with_memory())
        .metadata_location("memory://wh/ns/events/metadata/v0.json")
        .runtime(iceberg::Runtime::current())
        .build()
        .expect("table")
}

pub(crate) fn data_file() -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("memory://wh/ns/events/data/f.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(1)
        .file_size_in_bytes(1)
        .build()
        .expect("data file")
}

fn conflict_error() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::CatalogCommitConflicts,
        "injected CAS conflict",
    )
}

/// A catalog whose `update_table` conflicts a configured number of times
/// before succeeding — the deterministic harness for the bounded-retry loop.
#[derive(Debug)]
pub(crate) struct ConflictCatalog {
    table: Mutex<Table>,
    conflicts_remaining: AtomicU32,
    pub commits: AtomicU32,
}

impl ConflictCatalog {
    pub(crate) fn failing(conflicts: u32) -> Arc<Self> {
        Arc::new(Self {
            table: Mutex::new(test_table()),
            conflicts_remaining: AtomicU32::new(conflicts),
            commits: AtomicU32::new(0),
        })
    }
}

#[async_trait]
impl Catalog for ConflictCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        unimplemented!()
    }
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        unimplemented!()
    }
    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        unimplemented!()
    }
    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        unimplemented!()
    }
    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        unimplemented!()
    }
    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> iceberg::Result<Table> {
        unimplemented!()
    }
    async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<Table> {
        Ok(self.table.lock().expect("table lock").clone())
    }
    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
        unimplemented!()
    }
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        unimplemented!()
    }
    async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        let remaining = self.conflicts_remaining.load(Ordering::SeqCst);
        if remaining > 0 {
            self.conflicts_remaining
                .store(remaining - 1, Ordering::SeqCst);
            return Err(conflict_error());
        }
        Ok(self.table.lock().expect("table lock").clone())
    }
}
