use rdlt_connector::{
    Field, LogicalType, ReadMode, SchemaVersion, Sequences, StreamName, TableSchema, TableState,
};

use super::{Keying, to_record};
use crate::error::ErrorKind;
use crate::plan::{StreamPlan, WriteMode};

fn plan(read: ReadMode, write: WriteMode) -> StreamPlan {
    StreamPlan::new(StreamName::new("orders").unwrap())
        .read(read)
        .write(write)
}

/// A table state records: created, with the sequences `sequences` names.
fn table(sequences: Option<Sequences>) -> TableState {
    kept(sequences, false)
}

/// A table state records: created, with the sequences `sequences` names, keeping history or not.
fn kept(sequences: Option<Sequences>, history: bool) -> TableState {
    TableState {
        schema: Some((
            SchemaVersion(1),
            TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap(),
        )),
        physical: Some("orders".into()),
        names: rdlt_connector::NameMap::default(),
        sequences,
        history,
        key: Vec::new(),
        change_time: None,
        exact: std::collections::BTreeSet::new(),
    }
}

/// What state must record of `plan`'s table, recorded as `table`, before a load that merges by
/// no key: who makes its sequences and whether it keeps history.
fn recorded(
    plan: &StreamPlan,
    table: Option<&TableState>,
) -> Result<Option<(Sequences, bool)>, crate::error::Error> {
    let keying = to_record(plan, table, &[], None)?;
    Ok(keying.map(|keying: Keying| (keying.sequences, keying.history)))
}

#[test]
fn a_change_merge_records_its_source_s_sequences_on_a_new_table_or_its_own() {
    let changes = plan(ReadMode::Cdc, WriteMode::Merge);
    assert_eq!(
        recorded(&changes, None).unwrap(),
        Some((Sequences::Source, false))
    );
    let unnamed = TableState::default();
    assert_eq!(
        recorded(&changes, Some(&unnamed)).unwrap(),
        Some((Sequences::Source, false))
    );
    let own = table(Some(Sequences::Source));
    assert_eq!(recorded(&changes, Some(&own)).unwrap(), None);
}

#[test]
fn a_change_merge_into_rows_the_engine_sequenced_is_refused() {
    let changes = plan(ReadMode::Cdc, WriteMode::Merge);
    // Rows the engine merged, and rows of a table created before state recorded its sequences.
    for held in [Some(Sequences::Engine), None] {
        let error = recorded(&changes, Some(&table(held))).unwrap_err();
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
        assert_eq!(
            recorded(other, None).unwrap(),
            Some((Sequences::Engine, false))
        );
        let source = table(Some(Sequences::Source));
        assert_eq!(
            recorded(other, Some(&source)).unwrap(),
            Some((Sequences::Engine, false))
        );
        assert_eq!(
            recorded(other, Some(&table(None))).unwrap(),
            Some((Sequences::Engine, false))
        );
        let engine = table(Some(Sequences::Engine));
        assert_eq!(recorded(other, Some(&engine)).unwrap(), None);
    }
}

#[test]
fn a_history_stream_records_that_its_table_keeps_history() {
    let full = plan(ReadMode::Full, WriteMode::History);
    assert_eq!(
        recorded(&full, None).unwrap(),
        Some((Sequences::Engine, true))
    );
    let changes = plan(ReadMode::Cdc, WriteMode::History);
    assert_eq!(
        recorded(&changes, Some(&TableState::default())).unwrap(),
        Some((Sequences::Source, true))
    );
    let own = kept(Some(Sequences::Source), true);
    assert_eq!(recorded(&changes, Some(&own)).unwrap(), None);
}

#[test]
fn a_history_table_takes_only_history_streams_and_a_history_stream_only_its_own() {
    let history = plan(ReadMode::Incremental, WriteMode::History);
    // A table created without history, whatever state recorded of its sequences.
    for held in [Some(Sequences::Engine), None] {
        let error = recorded(&history, Some(&table(held))).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config, "{held:?}");
        assert_eq!(error.code(), Some("table_history_mismatch"), "{held:?}");
    }
    let versions = kept(Some(Sequences::Engine), true);
    for other in [
        plan(ReadMode::Incremental, WriteMode::Merge),
        plan(ReadMode::Full, WriteMode::Append),
        plan(ReadMode::Full, WriteMode::Replace),
    ] {
        let error = recorded(&other, Some(&versions)).unwrap_err();
        assert_eq!(error.code(), Some("table_history_mismatch"));
    }
}

fn columns(names: &[&str]) -> Vec<rdlt_connector::ColumnPath> {
    names
        .iter()
        .map(|name| rdlt_connector::ColumnPath::from(*name))
        .collect()
}

/// A created table of the engine's sequences, merged by `key`, keeping history from `change_time`
/// where one is given.
fn keyed(key: &[&str], change_time: Option<&str>) -> TableState {
    TableState {
        key: columns(key),
        change_time: change_time.map(rdlt_connector::ColumnPath::from),
        ..kept(Some(Sequences::Engine), change_time.is_some())
    }
}

#[test]
fn a_merge_records_its_key_and_keeps_it_while_loads_merge_nothing() {
    let merge = plan(ReadMode::Incremental, WriteMode::Merge);
    let id = columns(&["id"]);
    let first = to_record(&merge, Some(&keyed(&[], None)), &id, None).unwrap();
    assert_eq!(first.map(|keying| keying.key), Some(id.clone()));
    assert_eq!(
        to_record(&merge, Some(&keyed(&["id"], None)), &id, None).unwrap(),
        None
    );
    // A load that merges nothing records nothing new, and the key stays for the next merge.
    let append = plan(ReadMode::Incremental, WriteMode::Append);
    assert_eq!(
        to_record(&append, Some(&keyed(&["id"], None)), &[], None).unwrap(),
        None
    );
}

#[test]
fn a_merge_by_another_key_is_refused_once_the_table_is_created() {
    let merge = plan(ReadMode::Incremental, WriteMode::Merge);
    let tenant = columns(&["tenant"]);
    for other in [columns(&["tenant"]), columns(&["id", "tenant"])] {
        let error = to_record(&merge, Some(&keyed(&["id"], None)), &other, None).unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some("table_key_mismatch"))
        );
    }
    // A table not yet created holds no rows of any key.
    let uncreated = TableState {
        schema: None,
        ..keyed(&["id"], None)
    };
    let keying = to_record(&merge, Some(&uncreated), &tenant, None).unwrap();
    assert_eq!(keying.map(|keying| keying.key), Some(tenant));
}

#[test]
fn a_history_stream_is_held_to_the_change_time_its_table_began_with() {
    let history = plan(ReadMode::Incremental, WriteMode::History);
    let (id, at) = (columns(&["id"]), rdlt_connector::ColumnPath::from("at"));
    let table = keyed(&["id"], Some("at"));
    assert_eq!(
        to_record(&history, Some(&table), &id, Some(&at)).unwrap(),
        None
    );
    let created = rdlt_connector::ColumnPath::from("created");
    for other in [Some(&created), None] {
        let error = to_record(&history, Some(&table), &id, other).unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some("table_change_time_mismatch")),
            "{other:?}"
        );
    }
    let uncreated = TableState {
        schema: None,
        ..table
    };
    let keying = to_record(&history, Some(&uncreated), &id, Some(&created)).unwrap();
    assert_eq!(keying.and_then(|keying| keying.change_time), Some(created));
    // A stream keeping no history records no change time, whatever its catalog names.
    let merge = plan(ReadMode::Incremental, WriteMode::Merge);
    let keying = to_record(&merge, None, &id, Some(&at)).unwrap().unwrap();
    assert_eq!(keying.change_time, None);
}
