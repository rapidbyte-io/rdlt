//! What the merge refuses, and the code each refusal carries.

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use rdlt_connector::{ChangeOp, Deletion, HistoryColumns};

use super::super::admitted;
use super::super::refused::{code, failed};
use super::{Row, apply, empty, key, merge, row, rows, soft, stored_schema, written};

/// The code `outcome` was refused under.
fn refusal<T: std::fmt::Debug>(outcome: Result<T, ArrowError>) -> Option<&'static str> {
    code(&outcome.expect_err("the rows are refused"))
}

/// What merging `incoming` into an empty table of [`stored_schema`] is refused under.
fn merged(incoming: &RecordBatch, deletion: Deletion) -> Option<&'static str> {
    refusal(merge(
        &stored_schema(),
        &[],
        &[],
        std::slice::from_ref(incoming),
        &key(deletion),
    ))
}

/// `batch` with its column `name` replaced by `values`.
fn with(batch: &RecordBatch, name: &str, values: &ArrayRef) -> RecordBatch {
    let schema = batch.schema();
    let columns: Vec<(String, ArrayRef)> = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            let column = if field.name() == name {
                Arc::clone(values)
            } else {
                Arc::clone(column)
            };
            (field.name().clone(), column)
        })
        .collect();
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

#[test]
fn a_flag_on_a_key_the_sequence_or_no_stored_column_is_refused_under_its_code() {
    // The written batch's fields are id, value, seq, at, op, unchanged.
    let flagged = |bit: u8| {
        written(&[Row {
            unchanged: Some(vec![1 << bit]),
            ..row(1, "a", 1)
        }])
    };
    for (bit, code) in [
        (0, "flag_on_key"),
        (2, "flag_on_key"),
        (4, "flag_on_missing_column"),
        (5, "flag_on_missing_column"),
    ] {
        assert_eq!(
            merged(&flagged(bit), Deletion::Hard),
            Some(code),
            "bit {bit}"
        );
        let checked = admitted(&flagged(bit), Some(&stored_schema()), &key(Deletion::Hard));
        assert_eq!(refusal(checked), Some(code), "bit {bit}");
    }
    // A field the batch has and the table does not store is no column to keep.
    let narrow = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("seq", DataType::Binary, false),
    ]));
    let checked = admitted(&flagged(1), Some(&narrow), &key(Deletion::Hard));
    assert_eq!(refusal(checked), Some("flag_on_missing_column"));
    // Where the table's columns are not known, a stored field's flag is taken.
    admitted(&flagged(1), None, &key(Deletion::Hard)).expect("a flag on the value");
    admitted(&flagged(3), Some(&stored_schema()), &key(Deletion::Hard)).expect("on the time");
    // Bits past the batch's fields name nothing.
    let merged = apply(
        &empty(),
        &[&[Row {
            unchanged: Some(vec![0, 0b1000]),
            ..row(1, "a", 1)
        }]],
        Deletion::Hard,
    );
    assert_eq!(rows(&merged), [(1, Some("a".into()), 1, None)]);
}

#[test]
fn a_row_without_a_sequence_or_an_op_a_change_stream_has_is_refused_under_its_code() {
    let batch = written(&[row(1, "a", 1)]);
    let unsequenced = with(
        &batch,
        "seq",
        &(Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as ArrayRef),
    );
    assert_eq!(
        merged(&unsequenced, Deletion::Hard),
        Some("sequence_missing")
    );
    for op in [4_i8, -1, 100] {
        let odd = with(
            &batch,
            "op",
            &(Arc::new(Int8Array::from(vec![op])) as ArrayRef),
        );
        assert_eq!(merged(&odd, Deletion::Hard), Some("op_invalid"), "{op}");
    }
    let none = with(
        &batch,
        "op",
        &(Arc::new(Int8Array::from(vec![None])) as ArrayRef),
    );
    assert_eq!(merged(&none, Deletion::Hard), Some("op_invalid"));
    let text = with(
        &batch,
        "op",
        &(Arc::new(StringArray::from(vec!["update"])) as ArrayRef),
    );
    assert_eq!(merged(&text, Deletion::Hard), Some("op_invalid"));
    let opless = batch.project(&[0, 1, 2, 3, 5]).expect("a projection");
    assert_eq!(merged(&opless, Deletion::Hard), Some("op_invalid"));
    let flags = with(
        &batch,
        "unchanged",
        &(Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    );
    assert_eq!(merged(&flags, Deletion::Hard), Some("flags_invalid"));
}

#[test]
fn rows_without_their_sequence_or_their_key_s_column_are_refused_under_their_code() {
    let batch = written(&[row(1, "a", 1)]);
    let unsequenced = with(
        &batch,
        "seq",
        &(Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as ArrayRef),
    );
    // A plain merge table's rows need their sequence too, and a batch its sequence column.
    let plain = rdlt_connector::MergeKey {
        changes: None,
        ..key(Deletion::Hard)
    };
    let stored = unsequenced.project(&[0, 1, 2, 3]).expect("a projection");
    let refused = merge(&stored_schema(), &[], &[], &[stored], &plain);
    assert_eq!(refusal(refused), Some("sequence_missing"));
    let keyless = batch.project(&[0, 1, 3]).expect("a projection");
    let refused = merge(&stored_schema(), &[], &[], &[keyless], &plain);
    assert_eq!(refusal(refused), Some("merge_key_invalid"));
    // Rows that lack their key's column are no rows of a null key.
    let unkeyed = batch.project(&[1, 2, 3]).expect("a projection");
    let refused = merge(
        &stored_schema(),
        &[],
        &[],
        std::slice::from_ref(&unkeyed),
        &plain,
    );
    assert_eq!(refusal(refused), Some("merge_key_invalid"));
    let changes = batch.project(&[1, 2, 3, 4, 5]).expect("a projection");
    assert_eq!(merged(&changes, Deletion::Hard), Some("merge_key_invalid"));
    assert_eq!(
        refusal(admitted(&unkeyed, None, &plain)),
        Some("merge_key_invalid")
    );
}

#[test]
fn a_delete_or_a_truncate_of_a_soft_history_table_says_when_or_is_refused() {
    let history = rdlt_connector::MergeKey {
        history: Some(HistoryColumns {
            valid_from: "from".into(),
            valid_to: "to".into(),
            is_current: "current".into(),
            row_hash: "hash".into(),
        }),
        ..key(soft())
    };
    for op in [ChangeOp::Delete, ChangeOp::Truncate] {
        let untimed = written(&[Row {
            op,
            at: None,
            ..row(1, "a", 2)
        }]);
        let checked = admitted(&untimed, None, &history);
        assert_eq!(refusal(checked), Some("deletion_untimed"), "{op:?}");
        let timeless = untimed.project(&[0, 1, 2, 4, 5]).expect("a projection");
        let checked = admitted(&timeless, None, &history);
        assert_eq!(refusal(checked), Some("deletion_untimed"), "{op:?}");
        let timed = written(&[Row {
            op,
            at: Some(7),
            ..row(1, "a", 2)
        }]);
        admitted(&timed, None, &history).expect("a deletion that says when");
        // A table that keeps no history takes a deletion that says no time, and so does a
        // history table whose deletes remove.
        admitted(&untimed, None, &key(soft())).expect("no history");
        let hard = rdlt_connector::MergeKey {
            history: history.history.clone(),
            ..key(Deletion::Hard)
        };
        admitted(&untimed, None, &hard).expect("hard deletes");
    }
    // An upsert says no deletion time.
    admitted(&written(&[row(1, "a", 1)]), None, &history).expect("an upsert");
}

#[test]
fn a_refusal_is_a_data_error_under_its_code_and_any_other_failure_the_merge_s_own() {
    let batch = written(&[Row {
        unchanged: Some(vec![1]),
        ..row(1, "a", 1)
    }]);
    let refused = admitted(&batch, None, &key(Deletion::Hard)).expect_err("a flag on the key");
    let error = failed("merging rows", &refused);
    assert_eq!(error.kind(), rdlt_connector::ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("flag_on_key"));
    let own = ArrowError::ComputeError("an index out of bounds".to_owned());
    assert_eq!(code(&own), None);
    let error = failed("merging rows", &own);
    assert_eq!(error.kind(), rdlt_connector::ConnectorErrorKind::Internal);
    assert_eq!(error.code(), None);
}
