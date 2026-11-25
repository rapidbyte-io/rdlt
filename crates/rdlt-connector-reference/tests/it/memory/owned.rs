//! A memory session touches only tables its pipeline owns.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use rdlt_connector::{
    ChildTable, CommitKey, CommitMeta, ConnectContext, ConnectorError, ConnectorErrorKind,
    Destination, DroppedTable, GenerationId, MergeKey, OpenedSession, RootKey, SchemaVersion,
    SegmentId, TableChange, TablePath, TableRef, TableSchema, destination_factory,
};
use rdlt_connector_reference::{MemoryDestination, published};
use serde_json::json;

use crate::fixtures::open_as;

pub(super) async fn store(name: &str) -> Box<dyn Destination> {
    destination_factory::<MemoryDestination>()
        .connect(json!({ "store": name }), ConnectContext::new())
        .await
        .expect("the memory destination connects")
}

/// The table `name` at the path `path`, merging by `key` where one is given.
pub(super) fn table(name: &str, path: &str, key: Option<&str>) -> TableRef {
    TableRef {
        path: TablePath::new([path]).expect("a valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: key.map(|key| MergeKey {
            columns: vec![key.into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
    }
}

/// Rows of `ids`, each with its id as 16 bytes in `key` and `seq`.
fn rows(ids: &[i64]) -> RecordBatch {
    let bytes = || {
        BinaryArray::from_iter_values(ids.iter().map(|id| {
            let mut bytes = [0_u8; 16];
            bytes[8..].copy_from_slice(&id.to_be_bytes());
            bytes
        }))
    };
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
        ("key", Arc::new(bytes()) as _),
        ("seq", Arc::new(bytes()) as _),
    ])
    .expect("a valid batch")
}

pub(super) async fn stage(
    session: &mut OpenedSession,
    table: &TableRef,
    segment: u64,
    ids: &[i64],
) {
    let create = TableChange::Create {
        table: table.clone(),
        schema: TableSchema::from_arrow(&rows(ids).schema()).expect("a schema"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = session
        .session
        .writer(table)
        .await
        .expect("a table writer opens");
    writer
        .write(SegmentId(segment), rows(ids))
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// Commit `seq` of `session`, of `segments`, expecting the session's commit before it landed, or
/// for its first the key the session found landed.
pub(super) fn meta(session: &OpenedSession, seq: u64, segments: &[u64]) -> CommitMeta {
    let epoch = session.epoch;
    let found = session.landed.as_ref().map(|landed| landed.key);
    CommitMeta {
        key: CommitKey { epoch, seq },
        after: if seq == 1 {
            found
        } else {
            Some(CommitKey {
                epoch,
                seq: seq - 1,
            })
        },
        segments: segments.iter().copied().map(SegmentId).collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    }
}

pub(super) fn ids(store: &str, table: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = published(store, table)
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name("id").expect("an id column");
            column
                .as_primitive::<Int64Type>()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect();
    ids.sort_unstable();
    ids
}

pub(super) fn refusal<T: std::fmt::Debug>(
    outcome: Result<T, ConnectorError>,
) -> (ConnectorErrorKind, Option<String>) {
    let error = outcome.expect_err("the call is refused");
    (error.kind(), error.code().map(str::to_owned))
}

#[tokio::test]
async fn a_listed_child_table_of_another_pipeline_is_refused() {
    let destination = store("owned-children").await;
    let items = table("items", "items", Some("key"));
    let mut victim = open_as(destination.as_ref(), "victim").await;
    stage(&mut victim, &items, 1, &[1, 2]).await;
    victim
        .session
        .commit(&meta(&victim, 1, &[1]))
        .await
        .expect("the victim commits");
    let mut intruder = open_as(destination.as_ref(), "intruder").await;
    stage(&mut intruder, &table("r", "r", Some("id")), 1, &[2]).await;
    let mut listing = meta(&intruder, 1, &[1]);
    listing.child_tables = vec![ChildTable {
        table: "items".into(),
        merge: MergeKey {
            root: Some(RootKey {
                table: "r".into(),
                id: "key".into(),
                seq: "seq".into(),
            }),
            ..items.merge.clone().expect("a merge key")
        },
    }];
    assert_eq!(
        refusal(intruder.session.commit(&listing).await),
        (ConnectorErrorKind::Config, Some("table_owned".to_owned()))
    );
    assert_eq!(ids("owned-children", "items"), [1, 2]);
    assert_eq!(ids("owned-children", "r"), Vec::<i64>::new());
}

/// Pipeline `a` replaces its table while pipeline `b` creates, and where `dropping` then drops,
/// a table of the same path under another name; returns what `a`'s table holds once its
/// generation is finished.
async fn replaced_beside_a_namesake(store_name: &str, dropping: bool) -> Vec<i64> {
    let destination = store(store_name).await;
    let orders = table("orders", "orders", None);
    let mut a = open_as(destination.as_ref(), "a").await;
    stage(&mut a, &orders, 1, &[1]).await;
    a.session
        .commit(&meta(&a, 1, &[1]))
        .await
        .expect("the first load commits");
    let filling = TableRef {
        generation: Some(GenerationId(1)),
        ..orders.clone()
    };
    stage(&mut a, &filling, 2, &[2]).await;
    a.session
        .commit(&meta(&a, 2, &[2]))
        .await
        .expect("the generation's rows commit");
    let namesake = table("orders_abc234", "orders", None);
    let mut b = open_as(destination.as_ref(), "b").await;
    stage(&mut b, &namesake, 1, &[99]).await;
    b.session
        .commit(&meta(&b, 1, &[1]))
        .await
        .expect("the namesake commits");
    if dropping {
        let mut drop = meta(&b, 2, &[]);
        drop.drop_tables = vec![DroppedTable {
            path: namesake.path.clone(),
            name: "orders_abc234".into(),
        }];
        b.session.commit(&drop).await.expect("the drop lands");
    }
    let mut finish = meta(&a, 3, &[]);
    finish.finish_generations = vec![(orders.path.clone(), GenerationId(1))];
    a.session
        .commit(&finish)
        .await
        .expect("the finishing commit lands");
    ids(store_name, "orders")
}

#[tokio::test]
async fn a_path_another_pipeline_registered_swaps_the_pipeline_s_own_table() {
    assert_eq!(replaced_beside_a_namesake("owned-swap", false).await, [2]);
    assert_eq!(
        replaced_beside_a_namesake("owned-swap-drop", true).await,
        [2]
    );
}

#[tokio::test]
async fn a_writer_of_a_table_dropped_since_stages_nothing() {
    let destination = store("owned-dropped").await;
    let orders = table("orders", "orders", None);
    let mut session = open_as(destination.as_ref(), "a").await;
    let create = TableChange::Create {
        table: orders.clone(),
        schema: TableSchema::from_arrow(&rows(&[1]).schema()).expect("a schema"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = session
        .session
        .writer(&orders)
        .await
        .expect("a table writer opens");
    let mut drop = meta(&session, 1, &[]);
    drop.drop_tables = vec![DroppedTable {
        path: orders.path.clone(),
        name: "orders".into(),
    }];
    session.session.commit(&drop).await.expect("the drop lands");
    writer
        .write(SegmentId(1), rows(&[1]))
        .await
        .expect("the write buffers");
    assert_eq!(
        refusal(writer.flush().await),
        (ConnectorErrorKind::Config, Some("table_unowned".to_owned()))
    );
    assert!(rdlt_connector_reference::tables("owned-dropped").is_empty());
}

#[tokio::test]
async fn a_generation_finished_for_a_table_dropped_since_creates_nothing() {
    let destination = store("owned-finished").await;
    let orders = table("orders", "orders", None);
    let mut session = open_as(destination.as_ref(), "a").await;
    stage(&mut session, &orders, 1, &[1]).await;
    session
        .session
        .commit(&meta(&session, 1, &[1]))
        .await
        .expect("the load commits");
    let mut drop = meta(&session, 2, &[]);
    drop.drop_tables = vec![DroppedTable {
        path: orders.path.clone(),
        name: "orders".into(),
    }];
    session.session.commit(&drop).await.expect("the drop lands");
    let mut finish = meta(&session, 3, &[]);
    finish.finish_generations = vec![(orders.path.clone(), GenerationId(1))];
    session
        .session
        .commit(&finish)
        .await
        .expect("the commit lands");
    assert!(rdlt_connector_reference::tables("owned-finished").is_empty());
    // A table another pipeline creates since is not what the path named either.
    let mut other = open_as(destination.as_ref(), "b").await;
    stage(&mut other, &orders, 1, &[5]).await;
    other
        .session
        .commit(&meta(&other, 1, &[1]))
        .await
        .expect("the other pipeline's load commits");
    let again = meta(&session, 4, &[]);
    let again = CommitMeta {
        finish_generations: vec![(orders.path.clone(), GenerationId(1))],
        ..again
    };
    // The session of `a` was not fenced by `b`'s: it still commits, and swaps nothing.
    session
        .session
        .commit(&again)
        .await
        .expect("the commit lands");
    assert_eq!(ids("owned-finished", "orders"), [5]);
}

#[tokio::test]
async fn a_writer_or_a_change_of_a_table_never_created_claims_nothing() {
    use rdlt_connector::{Field, Fields, LogicalType};
    let destination = store("owned-never-created").await;
    let mut session = open_as(destination.as_ref(), "p").await;
    let ghost = table("ghost", "ghost", None);
    let unowned = (ConnectorErrorKind::Config, Some("table_unowned".to_owned()));
    let generation = TableRef {
        generation: Some(GenerationId(4)),
        ..ghost.clone()
    };
    for written in [
        ghost.clone(),
        table("ghost", "ghost", Some("id")),
        generation,
    ] {
        let writer = session.session.writer(&written).await.map(drop);
        assert_eq!(refusal(writer), unowned, "{written:?}");
    }
    let added = TableChange::AddColumn {
        table: ghost.clone(),
        field: Field::new("more", LogicalType::Utf8, true),
    };
    let widened = TableChange::Widen {
        table: ghost.clone(),
        column: "id".into(),
        from: LogicalType::Int32,
        to: LogicalType::Int64,
    };
    for change in [added, widened] {
        let changed = session.session.apply_schema(&change).await;
        assert_eq!(refusal(changed), unowned, "{change:?}");
    }
    // A creation refused, of a type out of canonical form, claims nothing either.
    let unsorted = Fields::new(vec![
        Field::new("b", LogicalType::Int64, true),
        Field::new("a", LogicalType::Int64, true),
    ])
    .expect("valid fields");
    let column = Field::new("meta", LogicalType::Struct(unsorted), true);
    let refused = TableChange::Create {
        table: ghost.clone(),
        schema: TableSchema::new(vec![column]).expect("a schema"),
    };
    let created = session.session.apply_schema(&refused).await;
    assert_eq!(refusal(created).0, ConnectorErrorKind::Data);
    assert!(rdlt_connector_reference::tables("owned-never-created").is_empty());
    // Any pipeline may create the table, and load it.
    let mut other = open_as(destination.as_ref(), "q").await;
    stage(&mut other, &ghost, 1, &[7]).await;
    other
        .session
        .commit(&meta(&other, 1, &[1]))
        .await
        .expect("the load lands");
    assert_eq!(ids("owned-never-created", "ghost"), [7]);
}

#[tokio::test]
async fn a_batch_of_a_column_the_table_lacks_or_does_not_hold_stages_nothing() {
    let destination = store("owned-columns").await;
    let mut session = open_as(destination.as_ref(), "p").await;
    let orders = table("orders", "orders", None);
    stage(&mut session, &orders, 1, &[1]).await;
    let mut extra = rows(&[2]).columns().to_vec();
    extra.push(Arc::new(Int64Array::from(vec![2])));
    let mut fields: Vec<_> = rows(&[2]).schema().fields().iter().cloned().collect();
    fields.push(Arc::new(arrow_schema::Field::new(
        "extra",
        arrow_schema::DataType::Int64,
        true,
    )));
    let lacked =
        RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), extra).expect("a batch");
    let texts = Arc::new(arrow_array::StringArray::from(vec!["abc"])) as ArrayRef;
    let retyped = RecordBatch::try_from_iter([("id", texts)]).expect("a batch");
    for (batch, code) in [(lacked, "column_missing"), (retyped, "type_unconvertible")] {
        let mut writer = session
            .session
            .writer(&orders)
            .await
            .expect("a table writer opens");
        writer
            .write(SegmentId(2), batch)
            .await
            .expect("the write buffers");
        let refused = (ConnectorErrorKind::Data, Some(code.to_owned()));
        assert_eq!(refusal(writer.flush().await), refused);
    }
    let staged = rdlt_connector_reference::staged("owned-columns", "orders");
    assert_eq!(staged, 1, "the first row alone is staged");
}
