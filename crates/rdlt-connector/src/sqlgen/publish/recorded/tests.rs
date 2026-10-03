use super::{encode_merge_key, merge_key};
use crate::destination::{ChangeColumns, Deletion, HistoryColumns, MergeKey, RootKey};
use crate::error::ConnectorErrorKind;

fn roots() -> [Option<RootKey>; 2] {
    [
        None,
        Some(RootKey {
            table: "roots".into(),
            id: "id".into(),
            seq: "root_seq".into(),
        }),
    ]
}

fn changes() -> [Option<ChangeColumns>; 4] {
    let change = |unchanged: Option<&str>, deletion| ChangeColumns {
        op: "op".into(),
        unchanged: unchanged.map(Into::into),
        deletion,
    };
    [
        None,
        Some(change(None, Deletion::Hard)),
        Some(change(Some("unchanged"), Deletion::Hard)),
        Some(change(None, Deletion::Soft { at: "at".into() })),
    ]
}

fn histories() -> [Option<HistoryColumns>; 2] {
    [
        None,
        Some(HistoryColumns {
            valid_from: "from".into(),
            valid_to: "to".into(),
            is_current: "current".into(),
            row_hash: "hash".into(),
        }),
    ]
}

#[test]
fn every_merge_key_reads_back_as_it_was_recorded() {
    for columns in [vec!["id".into()], vec!["a".into(), "b".into()]] {
        for root in roots() {
            for changes in changes() {
                for history in histories() {
                    let key = MergeKey {
                        columns: columns.clone(),
                        seq: "seq".into(),
                        root: root.clone(),
                        changes: changes.clone(),
                        history: history.clone(),
                    };
                    let recorded = encode_merge_key(&key);
                    assert_eq!(merge_key(&recorded, "seq").ok(), Some(key), "{recorded}");
                }
            }
        }
    }
}

#[test]
fn a_merge_key_recorded_in_another_form_is_refused() {
    let current = r#"{"format":1,"columns":["id"],"root":null,"changes":null,"history":null}"#;
    assert!(merge_key(current, "seq").is_ok());
    for other in [
        // As an earlier build recorded them.
        r#"["id"]"#,
        r#"{"columns":["rid"],"root":{"table":"roots","id":"id","seq":"seq"}}"#,
        // Of another format, of none, or with a field this build does not know.
        r#"{"format":2,"columns":["id"],"root":null,"changes":null,"history":null}"#,
        r#"{"columns":["id"],"root":null,"changes":null,"history":null}"#,
        r#"{"format":1,"columns":["id"],"root":null,"changes":null,"history":null,"x":1}"#,
        r#"{"format":1,"columns":["id"],"root":{"table":"r","id":"i","seq":"s","x":1},"changes":null,"history":null}"#,
    ] {
        let error = merge_key(other, "seq").unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Internal, "{other}");
    }
}
