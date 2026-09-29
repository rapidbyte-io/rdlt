use rdlt_connector::{
    Field, LogicalType, ReadMode, SchemaVersion, Sequences, StreamName, TableSchema, TableState,
};

use super::to_record;
use crate::error::ErrorKind;
use crate::plan::{StreamPlan, WriteMode};

fn plan(read: ReadMode, write: WriteMode) -> StreamPlan {
    StreamPlan::new(StreamName::new("orders").unwrap())
        .read(read)
        .write(write)
}

/// A table state records: created, with the sequences `sequences` names.
fn table(sequences: Option<Sequences>) -> TableState {
    TableState {
        schema: Some((
            SchemaVersion(1),
            TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap(),
        )),
        physical: Some("orders".into()),
        names: rdlt_connector::NameMap::default(),
        sequences,
        exact: std::collections::BTreeSet::new(),
    }
}

#[test]
fn a_change_merge_records_its_source_s_sequences_on_a_new_table_or_its_own() {
    let changes = plan(ReadMode::Cdc, WriteMode::Merge);
    assert_eq!(to_record(&changes, None).unwrap(), Some(Sequences::Source));
    let unnamed = TableState::default();
    assert_eq!(
        to_record(&changes, Some(&unnamed)).unwrap(),
        Some(Sequences::Source)
    );
    let own = table(Some(Sequences::Source));
    assert_eq!(to_record(&changes, Some(&own)).unwrap(), None);
}

#[test]
fn a_change_merge_into_rows_the_engine_sequenced_is_refused() {
    let changes = plan(ReadMode::Cdc, WriteMode::Merge);
    // Rows the engine merged, and rows of a table created before state recorded its sequences.
    for held in [Some(Sequences::Engine), None] {
        let error = to_record(&changes, Some(&table(held))).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config, "{held:?}");
        assert_eq!(error.code(), Some("table_sequences_mismatch"), "{held:?}");
    }
}

#[test]
fn every_other_stream_records_the_engine_s_sequences_over_whatever_the_table_held() {
    let others = [
        plan(ReadMode::Full, WriteMode::Merge),
        plan(ReadMode::Incremental, WriteMode::Append),
        plan(ReadMode::Full, WriteMode::Replace),
        plan(ReadMode::Cdc, WriteMode::Append),
    ];
    for other in &others {
        assert_eq!(to_record(other, None).unwrap(), Some(Sequences::Engine));
        let source = table(Some(Sequences::Source));
        assert_eq!(
            to_record(other, Some(&source)).unwrap(),
            Some(Sequences::Engine)
        );
        assert_eq!(
            to_record(other, Some(&table(None))).unwrap(),
            Some(Sequences::Engine)
        );
        let engine = table(Some(Sequences::Engine));
        assert_eq!(to_record(other, Some(&engine)).unwrap(), None);
    }
}
