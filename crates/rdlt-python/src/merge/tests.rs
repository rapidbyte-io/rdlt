use std::sync::Arc;

use std::num::NonZeroUsize;

use arrow_array::{ArrayRef, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_pyarrow::{FromPyArrow as _, ToPyArrow as _};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use pyo3::types::{PyAnyMethods as _, PyDict, PyDictMethods as _};
use pyo3::{Bound, PyAny, Python};
use rdlt_connector::merge::{
    admitted, failed, folded, holds, merge_children_sparse, merge_sparse, read_back,
    refuse_history_generation, tombstone_schema, written_schema,
};
use rdlt_connector::{
    ChangeColumns, Deletion, Field, GenerationId, HistoryColumns, LogicalType, MergeKey, RootKey,
    SchemaVersion, TablePath, TableRef, TableSchema, TimeUnit,
};

use crate::embedded;

fn rows(ids: &[i64], seq: i64, value: &str) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
        (
            "seq",
            Arc::new(Int64Array::from(vec![seq; ids.len()])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from(vec![value; ids.len()])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn key() -> MergeKey {
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    }
}

/// `code`, run with `globals`, which it fills: Python as an author writes it.
fn run<'py>(py: Python<'py>, code: &str, globals: &Bound<'py, PyDict>) {
    py.run(&std::ffi::CString::new(code).unwrap(), Some(globals), None)
        .unwrap();
}

/// `batches` as a Python list of pyarrow batches.
fn exported<'py>(py: Python<'py>, batches: &[RecordBatch]) -> Bound<'py, PyAny> {
    let exported: Vec<_> = batches
        .iter()
        .map(|batch| batch.to_pyarrow(py).unwrap())
        .collect();
    pyo3::types::PyList::new(py, exported).unwrap().into_any()
}

/// The merge's globals: the schema, the key as the session is told it, and `published` and
/// `incoming` as pyarrow batches.
fn merging<'py>(
    py: Python<'py>,
    published: &[RecordBatch],
    incoming: &[RecordBatch],
) -> Bound<'py, PyDict> {
    let globals = PyDict::new(py);
    let schema = rows(&[], 0, "").schema();
    globals
        .set_item("schema", schema.as_ref().to_pyarrow(py).unwrap())
        .unwrap();
    globals
        .set_item("key", serde_json::to_string(&key()).unwrap())
        .unwrap();
    globals
        .set_item("published", exported(py, published))
        .unwrap();
    globals
        .set_item("incoming", exported(py, incoming))
        .unwrap();
    globals
}

#[test]
fn python_s_merge_answers_the_rows_rust_s_merge_does() {
    let published = vec![rows(&[1, 2, 3], 1, "old"), rows(&[4, 5], 1, "old")];
    let incoming = vec![rows(&[2, 6], 2, "new"), rows(&[2], 3, "newest")];
    let schema = rows(&[], 0, "").schema();
    let expected = merge_sparse(&schema, &published, &[], &incoming, &key()).unwrap();
    let answered: Vec<RecordBatch> = embedded::with(|py| {
        let globals = merging(py, &published, &incoming);
        run(
            py,
            "import json\n\
             from rdlt.connector import merge\n\
             merged = merge.merge_sparse(schema, published, [], incoming, json.loads(key))\n",
            &globals,
        );
        let merged = globals.get_item("merged").unwrap().unwrap();
        let rows = merged.getattr("rows").unwrap();
        rows.try_iter()
            .unwrap()
            .map(|batch| RecordBatch::from_pyarrow_bound(&batch.unwrap()).unwrap())
            .collect()
    });
    assert_eq!(answered, expected.rows);
}

#[test]
fn a_refusal_reaches_python_as_rust_s_kind_code_and_message() {
    let published = vec![rows(&[1], 1, "old")];
    let unsequenced = rows(&[1], 2, "new").project(&[0, 2]).unwrap();
    let schema = rows(&[], 0, "").schema();
    let refused = merge_sparse(
        &schema,
        &published,
        &[],
        std::slice::from_ref(&unsequenced),
        &key(),
    )
    .unwrap_err();
    let expected = failed("merge_sparse", refused);
    let (kind, code, message): (String, Option<String>, String) = embedded::with(|py| {
        let globals = merging(py, &published, std::slice::from_ref(&unsequenced));
        run(
            py,
            "import json\n\
             from rdlt.connector import merge\n\
             try:\n    merge.merge_sparse(schema, published, [], incoming, json.loads(key))\n\
             except Exception as error:\n    refused = (type(error).__name__, error.code, str(error))\n",
            &globals,
        );
        globals
            .get_item("refused")
            .unwrap()
            .expect("the merge is refused")
            .extract()
            .unwrap()
    });
    assert_eq!(
        (kind.as_str(), code.as_deref(), message),
        ("InvalidData", expected.code(), expected.to_string())
    );
}

#[test]
fn a_table_schema_s_arrow_in_python_is_rust_s() {
    let table = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
        Field::new("doc", LogicalType::Json, true),
        Field::new("token", LogicalType::Uuid, true),
        Field::new(
            "at",
            LogicalType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "tags",
            LogicalType::List(Box::new(Field::new("item", LogicalType::Utf8, true))),
            true,
        ),
    ])
    .unwrap();
    let answered: Schema = embedded::with(|py| {
        let globals = PyDict::new(py);
        globals
            .set_item("table", serde_json::to_string(&table).unwrap())
            .unwrap();
        run(
            py,
            "import json\n\
             from rdlt.connector import to_arrow\n\
             schema = to_arrow(json.loads(table))\n",
            &globals,
        );
        let schema = globals.get_item("schema").unwrap().unwrap();
        Schema::from_pyarrow_bound(&schema).unwrap()
    });
    assert_eq!(answered, table.to_arrow());
}

/// The batches of `answered`, a list the kit gave back.
fn batches_of(answered: &Bound<'_, PyAny>) -> Vec<RecordBatch> {
    answered
        .try_iter()
        .unwrap()
        .map(|batch| RecordBatch::from_pyarrow_bound(&batch.unwrap()).unwrap())
        .collect()
}

/// What `call` raises, run with `globals`: its type's name, its code and its message.
fn refusal(
    py: Python<'_>,
    call: &str,
    globals: &Bound<'_, PyDict>,
) -> (String, Option<String>, String) {
    let code = format!(
        "import json\nfrom rdlt.connector import merge, to_arrow\ntry:\n    {call}\nexcept Exception as error:\n    refused = (type(error).__name__, getattr(error, 'code', None), str(error))\n"
    );
    run(py, &code, globals);
    globals
        .get_item("refused")
        .unwrap()
        .expect("the call is refused")
        .extract()
        .unwrap()
}

/// What Python's `expression` answers, run with `globals`.
fn answer<'py>(
    py: Python<'py>,
    expression: &str,
    globals: &Bound<'py, PyDict>,
) -> Bound<'py, PyAny> {
    let code =
        format!("import json\nfrom rdlt.connector import merge, to_arrow\nanswer = {expression}\n");
    run(py, &code, globals);
    globals.get_item("answer").unwrap().unwrap()
}

#[test]
fn a_child_table_s_merge_in_python_answers_rust_s() {
    let root = RootKey {
        table: "orders".into(),
        id: "id".into(),
        seq: "seq".into(),
    };
    let child = MergeKey {
        root: Some(root.clone()),
        ..key()
    };
    let published = vec![rows(&[1, 1, 2], 1, "old")];
    let incoming = vec![rows(&[1, 1], 2, "x"), rows(&[1], 3, "y")];
    let roots = vec![
        rows(&[1, 1], 2, "").project(&[0, 1]).unwrap(),
        rows(&[1], 3, "").project(&[0, 1]).unwrap(),
    ];
    let schema = rows(&[], 0, "").schema();
    let expected =
        merge_children_sparse(&schema, &published, &incoming, &child, &root, &roots).unwrap();
    let answered = embedded::with(|py| {
        let globals = merging(py, &published, &incoming);
        globals
            .set_item("child", serde_json::to_string(&child).unwrap())
            .unwrap();
        globals
            .set_item("root", serde_json::to_string(&root).unwrap())
            .unwrap();
        globals.set_item("roots", exported(py, &roots)).unwrap();
        let children = answer(
            py,
            "merge.merge_children_sparse(schema, published, incoming, json.loads(child), json.loads(root), roots)",
            &globals,
        );
        batches_of(&children)
    });
    assert_eq!(answered, expected);
}

#[test]
fn a_read_back_in_python_answers_rust_s_every_column_in_each_batch() {
    let sparse = vec![
        rows(&[1], 1, "a").project(&[0, 1]).unwrap(),
        rows(&[2], 1, "b"),
    ];
    let schema = rows(&[], 0, "").schema();
    let expected = read_back(&schema, &sparse).unwrap();
    let answered = embedded::with(|py| {
        let globals = merging(py, &sparse, &[]);
        batches_of(&answer(py, "merge.read_back(schema, published)", &globals))
    });
    assert_eq!(answered, expected);
}

#[test]
fn a_stream_of_batches_merges_as_its_batches_do() {
    let published = vec![rows(&[1, 2], 1, "old"), rows(&[3], 1, "old")];
    let incoming = vec![rows(&[2], 2, "new")];
    let schema = rows(&[], 0, "").schema();
    let expected = merge_sparse(&schema, &published, &[], &incoming, &key()).unwrap();
    let answered = embedded::with(|py| {
        let globals = merging(py, &published, &incoming);
        let merged = answer(
            py,
            "merge.merge_sparse(schema, [__import__('pyarrow').Table.from_batches(published)], [], incoming, json.loads(key))",
            &globals,
        );
        batches_of(&merged.getattr("rows").unwrap())
    });
    assert_eq!(answered, expected.rows);
}

#[test]
fn the_schemas_python_is_answered_are_rust_s() {
    let schema = rows(&[], 0, "").schema();
    let changes = ChangeColumns {
        op: "_op".into(),
        unchanged: Some("_unchanged".into()),
        deletion: Deletion::Hard,
    };
    let written = written_schema(&schema, &changes);
    let buried = tombstone_schema(&schema, &key()).unwrap();
    let (answered_written, answered_buried) = embedded::with(|py| {
        let globals = merging(py, &[], &[]);
        globals
            .set_item("changes", serde_json::to_string(&changes).unwrap())
            .unwrap();
        let read = |expression: &str| {
            Schema::from_pyarrow_bound(&answer(py, expression, &globals)).unwrap()
        };
        (
            read("merge.written_schema(schema, json.loads(changes))"),
            read("merge.tombstone_schema(schema, json.loads(key))"),
        )
    });
    assert_eq!(
        (Arc::new(answered_written), Arc::new(answered_buried)),
        (written, buried)
    );
}

#[test]
fn a_fold_in_python_answers_rust_s_and_no_shapes_are_refused() {
    let schema = rows(&[], 0, "").schema();
    let held = vec![
        rows(&[1, 2, 3], 1, "a"),
        rows(&[4], 1, "b").project(&[0, 1]).unwrap(),
        rows(&[5], 1, "c"),
    ];
    let expected = folded(&schema, held.clone(), NonZeroUsize::new(2).unwrap(), 100).unwrap();
    let (answered, refused) = embedded::with(|py| {
        let globals = merging(py, &held, &[]);
        let answered = batches_of(&answer(
            py,
            "merge.folded(schema, published, 2, 100)",
            &globals,
        ));
        (
            answered,
            refusal(py, "merge.folded(schema, published, 0, 100)", &globals),
        )
    });
    assert_eq!(answered, expected);
    assert_eq!((refused.0.as_str(), refused.1), ("ValueError", None));
    assert!(refused.2.contains("shapes"), "{}", refused.2);
}

#[test]
fn what_holds_and_admitted_refuse_reaches_python_as_rust_s_refusal() {
    let narrow = Arc::new(Schema::new(vec![ArrowField::new(
        "n",
        DataType::Int8,
        true,
    )]));
    let wide =
        RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![1, 300])) as ArrayRef)])
            .unwrap();
    let kept =
        RecordBatch::try_from_iter([("n", Arc::new(Int8Array::from(vec![1, 2])) as ArrayRef)])
            .unwrap();
    let unheld = failed("holds", holds([&wide], &narrow).unwrap_err());
    let unsequenced = rows(&[1], 1, "a").project(&[0, 2]).unwrap();
    let schema = rows(&[], 0, "").schema();
    let unadmitted = failed(
        "admitted",
        admitted(&unsequenced, Some(&schema), &key()).unwrap_err(),
    );
    let (refused_holds, refused_admitted) = embedded::with(|py| {
        let globals = merging(
            py,
            &[wide.clone(), unsequenced.clone()],
            std::slice::from_ref(&kept),
        );
        globals
            .set_item("narrow", narrow.as_ref().to_pyarrow(py).unwrap())
            .unwrap();
        globals
            .set_item("sequenced", rows(&[1], 1, "a").to_pyarrow(py).unwrap())
            .unwrap();
        answer(py, "merge.holds(incoming, narrow)", &globals);
        answer(
            py,
            "merge.admitted(sequenced, None, json.loads(key))",
            &globals,
        );
        (
            refusal(py, "merge.holds(published[:1], narrow)", &globals),
            refusal(
                py,
                "merge.admitted(published[1], schema, json.loads(key))",
                &globals,
            ),
        )
    });
    for (refused, expected) in [(refused_holds, unheld), (refused_admitted, unadmitted)] {
        assert_eq!(
            (refused.0.as_str(), refused.1.as_deref(), refused.2),
            ("InvalidData", expected.code(), expected.to_string())
        );
    }
}

#[test]
fn a_history_table_s_generation_is_refused_in_python_as_rust_s_internal_error() {
    let history = MergeKey {
        history: Some(HistoryColumns {
            valid_from: "valid_from".into(),
            valid_to: "valid_to".into(),
            is_current: "is_current".into(),
            row_hash: "row_hash".into(),
        }),
        ..key()
    };
    let generation = TableRef {
        path: TablePath::new(["prices"]).unwrap(),
        name: "prices".into(),
        version: SchemaVersion(1),
        generation: Some(GenerationId(3)),
        merge: Some(history),
    };
    let expected = refuse_history_generation(&generation).unwrap_err();
    let table = TableRef {
        generation: None,
        ..generation.clone()
    };
    let refused = embedded::with(|py| {
        let globals = PyDict::new(py);
        globals
            .set_item("generation", serde_json::to_string(&generation).unwrap())
            .unwrap();
        globals
            .set_item("table", serde_json::to_string(&table).unwrap())
            .unwrap();
        answer(
            py,
            "merge.refuse_history_generation(json.loads(table))",
            &globals,
        );
        refusal(
            py,
            "merge.refuse_history_generation(json.loads(generation))",
            &globals,
        )
    });
    assert_eq!(
        (refused.0.as_str(), refused.1, refused.2),
        ("ConnectorError", None, expected.to_string())
    );
}

#[test]
fn a_table_schema_rdlt_cannot_read_is_refused_in_python_as_a_value_error() {
    let (kind, code, message) = embedded::with(|py| {
        let globals = PyDict::new(py);
        refusal(
            py,
            "to_arrow({'fields': [[{'name': 'n', 'type': 'int99'}]]})",
            &globals,
        )
    });
    assert_eq!((kind.as_str(), code), ("ValueError", None));
    assert!(
        message.starts_with("no table schema rdlt can read: "),
        "{message}"
    );
}
