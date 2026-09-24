use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorError, DestinationSession, DestinationWriter,
    Field, GenerationId, LogicalType, Receipt, Result, SchemaChanges, SchemaVersion, StateChange,
    StateEntry, StreamName, TableChange, TablePath, TableRef, TableSchema,
};

use super::{SharedSession, Tables};
use crate::error::ErrorKind;
use crate::naming::Naming;
use crate::plan::StreamPlan;
use crate::policy::SchemaSettings;
use crate::table::{MetaNames, Model, Resolver, Settings};

type Changes = Arc<Mutex<Vec<TableChange>>>;

/// Records the schema changes applied to it.
struct Recorder(Changes);

impl DestinationSession for Recorder {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.0.lock().push(change.clone());
        Box::pin(async { Ok(()) })
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
