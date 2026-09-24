use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Capabilities, ColumnKey, ColumnPath, CommitMeta, ConnectorError,
    DestinationSession, DestinationWriter, Field, GenerationId, IdentifierCase, LogicalType,
    Receipt, Result, SchemaChanges, SchemaVersion, StateChange, StateEntry, StreamName,
    TableChange, TablePath, TableRef, TableSchema,
};

use super::{SharedSession, Tables};
use crate::error::ErrorKind;
use crate::naming::Naming;
use crate::plan::StreamPlan;
use crate::policy::SchemaSettings;
use crate::table::{MetaNames, Model, Resolver, Settings};

type Changes = Arc<Mutex<Vec<TableChange>>>;

/// Records the schema changes applied to it, yielding once for each so concurrent callers
/// interleave.
struct Recorder(Changes);

impl DestinationSession for Recorder {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.0.lock().push(change.clone());
        Box::pin(async {
            tokio::task::yield_now().await;
            Ok(())
        })
    }

    fn writer<'a>(
        &'a mut self,
        _table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async { Err(ConnectorError::internal("no writers here")) })
    }

    fn commit<'a>(&'a mut self, _meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async { Err(ConnectorError::internal("no commits here")) })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn table(generation: Option<GenerationId>) -> TableRef {
    TableRef {
        path: TablePath::new(["orders"]).unwrap(),
        name: "orders".into(),
        version: SchemaVersion(0),
        generation,
        merge: None,
    }
}

fn resolver() -> Resolver {
    let mut capabilities = Capabilities::minimal();
    capabilities.schema_changes = SchemaChanges::all();
    Resolver {
        stream: StreamName::new("orders").unwrap(),
        settings: Settings {
            pipeline: SchemaSettings::default(),
            stream: StreamPlan::new(StreamName::new("orders").unwrap()),
            key: Vec::new(),
        },
        naming: Naming::new(capabilities.identifiers.clone()),
        capabilities: Arc::new(capabilities),
        meta: MetaNames {
            load_id: "_rdlt_load_id".into(),
            loaded_at: "_rdlt_loaded_at".into(),
            seq: None,
        },
    }
}

fn schema(fields: &[(&str, LogicalType)]) -> TableSchema {
    TableSchema::new(
        fields
            .iter()
            .map(|(name, logical)| Field::new(*name, logical.clone(), true))
            .collect(),
    )
    .unwrap()
}

/// Tables over a recording session, with one table added from `model`.
fn tables(generation: Option<GenerationId>, model: Model) -> (Tables, Changes) {
    let changes = Changes::default();
    let session = SharedSession::new(Box::new(Recorder(Arc::clone(&changes))));
    let mut tables = Tables::new(session);
    tables.add(resolver(), &table(generation), model);
    (tables, changes)
}

#[tokio::test]
async fn a_change_is_applied_once_and_later_batches_fit_without_one() {
    let (tables, changes) = tables(None, Model::default());
    let first = schema(&[("id", LogicalType::Int64)]);
    let (view, _) = tables.fit(0, &first).await.unwrap();
    assert_eq!(view.model.version, 1);
    assert!(matches!(changes.lock()[..], [TableChange::Create { .. }]));
    tables.fit(0, &first).await.unwrap();
    let wider = schema(&[("id", LogicalType::Int64), ("note", LogicalType::Utf8)]);
    let (view, routes) = tables.fit(0, &wider).await.unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(view.table.version, SchemaVersion(2));
    let applied = changes.lock();
    assert_eq!(applied.len(), 2, "{applied:?}");
    assert!(matches!(&applied[1], TableChange::AddColumn { field, .. } if field.name() == "note"));
    assert_eq!(tables.view(0).model.version, 2);
}

#[tokio::test]
async fn concurrent_changes_to_one_table_each_apply_on_top_of_the_other() {
    let (tables, changes) = tables(None, Model::default());
    tables
        .fit(0, &schema(&[("id", LogicalType::Int64)]))
        .await
        .unwrap();
    let first = schema(&[("id", LogicalType::Int64), ("a", LogicalType::Utf8)]);
    let second = schema(&[("id", LogicalType::Int64), ("b", LogicalType::Int32)]);
    let (one, two) = tokio::join!(tables.fit(0, &first), tables.fit(0, &second));
    let (one, two) = (one.unwrap().0, two.unwrap().0);
    assert_eq!(
        (one.model.version.min(two.model.version), tables.view(0).model.version),
        (2, 3)
    );
    let view = tables.view(0);
    let names: Vec<&str> = view.model.columns.iter().map(Field::name).collect();
    assert!(names.contains(&"a") && names.contains(&"b"), "{names:?}");
    let applied = changes.lock();
    assert_eq!(applied.len(), 3, "the create and one added column each: {applied:?}");
}

#[tokio::test]
async fn widenings_that_keep_the_stored_type_change_nothing_in_the_destination() {
    let (tables, changes) = tables(None, Model::default());
    tables
        .fit(0, &schema(&[("n", LogicalType::Int32)]))
        .await
        .unwrap();
    tables
        .fit(0, &schema(&[("n", LogicalType::Int64)]))
        .await
        .unwrap();
    assert!(matches!(
        &changes.lock()[1],
        TableChange::Widen {
            from: LogicalType::Int32,
            to: LogicalType::Int64,
            ..
        }
    ));
}

#[tokio::test]
async fn a_replace_generation_is_created_with_the_tables_columns_once_the_table_exists() {
    let (fresh, changes) = tables(Some(GenerationId(4)), Model::default());
    fresh.create_generation(0).await.unwrap();
    assert!(changes.lock().is_empty(), "no table yet");
    let (created, changes) = tables(Some(GenerationId(4)), Model::default());
    created
        .fit(0, &schema(&[("id", LogicalType::Int64)]))
        .await
        .unwrap();
    created.create_generation(0).await.unwrap();
    let applied = changes.lock().clone();
    assert_eq!(applied.len(), 2);
    let TableChange::Create {
        table,
        schema: columns,
    } = &applied[1]
    else {
        panic!("{applied:?}")
    };
    assert_eq!(table.generation, Some(GenerationId(4)));
    assert_eq!(columns.fields().len(), 3, "id and two metadata columns");
    let (plain, changes) = tables(None, Model::default());
    plain
        .fit(0, &schema(&[("id", LogicalType::Int64)]))
        .await
        .unwrap();
    plain.create_generation(0).await.unwrap();
    assert_eq!(changes.lock().len(), 1, "no generation to create");
}

#[tokio::test]
async fn the_delta_holds_tables_changed_since_state_recorded_them() {
    let (tables, _) = tables(None, Model::default());
    assert!(tables.delta().changes.is_empty());
    tables
        .fit(0, &schema(&[("id", LogicalType::Int64)]))
        .await
        .unwrap();
    let delta = tables.delta();
    assert_eq!(delta.versions, [(0, 1)]);
    let entries: Vec<StateEntry> = delta
        .changes
        .iter()
        .map(|change| match change {
            StateChange::Put(record) => StateEntry::from_record(record).unwrap(),
            StateChange::Delete(key) => panic!("{key}"),
        })
        .collect();
    assert!(matches!(
        &entries[0],
        StateEntry::Schema {
            version: SchemaVersion(1),
            ..
        }
    ));
    assert!(
        matches!(&entries[1], StateEntry::Names { physical, .. } if physical.as_ref() == "orders")
    );
    tables.recorded(&delta.versions);
    assert!(tables.delta().changes.is_empty());
    tables.recorded(&[(0, 0)]);
    assert!(
        tables.delta().changes.is_empty(),
        "an older version never lowers the record"
    );
}

#[tokio::test]
async fn a_closed_session_refuses_further_calls() {
    let (tables, _) = tables(None, Model::default());
    assert!(format!("{tables:?}").starts_with("Tables"));
    assert!(format!("{:?}", tables.session()).starts_with("SharedSession"));
    tables.session().close().await.unwrap();
    tables.session().close().await.unwrap();
    let error = tables
        .fit(0, &schema(&[("id", LogicalType::Int64)]))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Internal);
}

type Columns = Arc<Mutex<BTreeMap<String, LogicalType>>>;

/// Keeps one table's columns and refuses a change that declares one at another type, as the
/// destination contract says; a crashed attempt's columns stay behind.
struct Physical(Columns);

impl Physical {
    fn change(&self, change: &TableChange) -> Result<()> {
        let mut columns = self.0.lock();
        let declared: Vec<(String, LogicalType, Option<LogicalType>)> = match change {
            TableChange::Create { schema, .. } => schema
                .fields()
                .iter()
                .map(|field| (field.name().to_owned(), field.logical_type().clone(), None))
                .collect(),
            TableChange::AddColumn { field, .. } => {
                vec![(field.name().to_owned(), field.logical_type().clone(), None)]
            }
            TableChange::Widen {
                column, from, to, ..
            } => vec![(column.to_string(), to.clone(), Some(from.clone()))],
        };
        for (name, to, from) in declared {
            match columns.get(&name) {
                Some(held) if *held != to && Some(held) != from.as_ref() => {
                    return Err(ConnectorError::data(format!("{name} is {held:?}"))
                        .with_code("schema_conflict"));
                }
                _ => {
                    columns.insert(name, to);
                }
            }
        }
        Ok(())
    }
}

impl DestinationSession for Physical {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        let result = self.change(change);
        Box::pin(async { result })
    }

    fn writer<'a>(
        &'a mut self,
        _table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async { Err(ConnectorError::internal("no writers here")) })
    }

    fn commit<'a>(&'a mut self, _meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async { Err(ConnectorError::internal("no commits here")) })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// One attempt's tables over the destination columns `columns`, folding identifiers to lower
/// case; nothing is committed, so each attempt starts from no table.
fn attempt(columns: &Columns) -> Tables {
    let mut resolver = resolver();
    let mut capabilities = (*resolver.capabilities).clone();
    capabilities.identifiers.case = IdentifierCase::Lower;
    resolver.naming = Naming::new(capabilities.identifiers.clone());
    resolver.capabilities = Arc::new(capabilities);
    let mut tables = Tables::new(SharedSession::new(Box::new(Physical(Arc::clone(columns)))));
    tables.add(resolver, &table(None), Model::default());
    tables
}

fn source(name: &str) -> ColumnKey {
    ColumnKey::Source(ColumnPath::from(name))
}

#[tokio::test]
async fn an_attempt_names_columns_around_those_a_crashed_attempt_left_behind() {
    let columns = Columns::default();
    let upper = schema(&[("X", LogicalType::Int64)]);
    let lower = schema(&[("x", LogicalType::Utf8)]);
    let crashed = attempt(&columns);
    crashed.fit(0, &upper).await.unwrap();
    let (before, _) = crashed.fit(0, &lower).await.unwrap();
    let retried = attempt(&columns);
    retried.fit(0, &lower).await.unwrap();
    let (after, _) = retried.fit(0, &upper).await.unwrap();
    let physical = columns.lock().clone();
    for (key, logical) in [
        (source("X"), LogicalType::Int64),
        (source("x"), LogicalType::Utf8),
    ] {
        let name = after.model.names.get(&key).unwrap();
        assert_eq!(physical.get(name), Some(&logical), "{name} in {physical:?}");
        assert_ne!(before.model.names.get(&key), None);
    }
    let names: Vec<&str> = after.model.names.iter().map(|(_, name)| name).collect();
    assert_eq!(physical.len(), names.len() + 2, "no column besides these and metadata");
}

/// Refuses every change with `code`, counting the calls.
struct Refusing {
    code: Option<&'static str>,
    calls: Arc<Mutex<usize>>,
}

impl DestinationSession for Refusing {
    fn apply_schema<'a>(&'a mut self, _change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        *self.calls.lock() += 1;
        let error = ConnectorError::data("refused");
        let error = match self.code {
            Some(code) => error.with_code(code),
            None => error,
        };
        Box::pin(async { Err(error) })
    }

    fn writer<'a>(
        &'a mut self,
        _table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async { Err(ConnectorError::internal("no writers here")) })
    }

    fn commit<'a>(&'a mut self, _meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async { Err(ConnectorError::internal("no commits here")) })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn only_a_first_conflict_is_named_around() {
    for (code, calls) in [(None, 1), (Some("schema_conflict"), 2)] {
        let counted = Arc::new(Mutex::new(0));
        let session = Refusing {
            code,
            calls: Arc::clone(&counted),
        };
        let mut tables = Tables::new(SharedSession::new(Box::new(session)));
        tables.add(resolver(), &table(None), Model::default());
        let error = tables
            .fit(0, &schema(&[("id", LogicalType::Int64)]))
            .await
            .unwrap_err();
        assert_eq!((error.kind(), error.code()), (ErrorKind::Destination, code));
        assert_eq!(*counted.lock(), calls, "{code:?}");
        assert_eq!(tables.view(0).model.version, 0, "a refused change changes nothing");
    }
}
