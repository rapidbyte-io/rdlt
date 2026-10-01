//! History tables merge into their table: a replace generation of one is refused.

use std::time::UNIX_EPOCH;

use rdlt_connector::{
    ConnectContext, ConnectorErrorKind, Destination, DestinationConnector, GenerationId,
    HistoryColumns, LoadId, MergeKey, OpenContext, PipelineId, SchemaVersion, TablePath, TableRef,
    destination_factory,
};
use rdlt_connector_reference::{FilesDestination, MemoryDestination};
use serde_json::json;

fn table(generation: Option<GenerationId>) -> TableRef {
    TableRef {
        path: TablePath::new(["history"]).expect("valid table path"),
        name: "history".into(),
        version: SchemaVersion(1),
        generation,
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: Some(HistoryColumns {
                valid_from: "valid_from".into(),
                valid_to: "valid_to".into(),
                is_current: "is_current".into(),
                row_hash: "row_hash".into(),
            }),
        }),
    }
}

/// Whether `C` opens a writer of a history table, and of a generation of it.
async fn writers<C: DestinationConnector>(
    config: serde_json::Value,
) -> (bool, Option<ConnectorErrorKind>) {
    let destination: Box<dyn Destination> = destination_factory::<C>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("history").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let merged = opened.session.writer(&table(None)).await.is_ok();
    let generation = opened.session.writer(&table(Some(GenerationId(1)))).await;
    (merged, generation.err().map(|error| error.kind()))
}

#[tokio::test]
async fn a_history_table_takes_no_generation() {
    let refused = (true, Some(ConnectorErrorKind::Internal));
    let memory = writers::<MemoryDestination>(json!({ "store": "history_generation" })).await;
    assert_eq!(memory, refused, "memory");
    let root = crate::fixtures::tempdir().expect("a temporary directory");
    let config = json!({ "root": root.path(), "format": "jsonl" });
    assert_eq!(writers::<FilesDestination>(config).await, refused, "files");
}
