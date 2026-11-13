//! What a Python stream says of itself, and the catalog a Python source discovers.

use arrow_schema::{DataType, Field as ArrowField, Schema, TimeUnit as ArrowTimeUnit};
use pyo3::types::{PyAnyMethods as _, PyDict};
use pyo3::{Py, PyAny, Python};
use rdlt_connector::{
    Catalog, ColumnPath, ConnectContext, ConnectorErrorKind, Field, LogicalType, ReadMode,
    StreamName, TableSchema, TimeUnit,
};

use super::super::NativeSourceFactory;
use super::class_of;
use crate::embedded;
use crate::runtime;

/// The `connect` the Python source `code` defines.
fn connect(py: Python<'_>, code: &str) -> Py<PyAny> {
    let globals = PyDict::new(py);
    let code = format!("import json, types\n{code}");
    py.run(&std::ffi::CString::new(code).unwrap(), Some(&globals), None)
        .unwrap();
    globals.get_item("connect").unwrap().unbind()
}

/// A Python stream `name`, whose spec is the Python dict `spec`, reading one row.
fn stream(name: &str, spec: &str) -> String {
    format!(
        "\
def read_{name}(partition, cursor, context):
    yield {{'id': 1}}
stream_{name} = types.SimpleNamespace(read=read_{name}, plan=None, spec=json.dumps({{
    'spec': {spec}, 'planned': False, 'stated': False, 'committed': False,
    'acknowledged': False, 'cursor_version': 1}}))
"
    )
}

/// Python's words for a spec of stream `name`, which says it can be read again where
/// `replayable` is `True`.
fn spec(name: &str, replayable: &str) -> String {
    format!(
        "{{'name': '{name}', 'schema': None, 'primary_key': None, 'read_modes': ['full'], \
         'checkpointing': 'natural', 'replayable': {replayable}, 'change_time': None}}"
    )
}

/// The catalog the source `code` defines discovers.
fn discovered(py: Python<'_>, code: &str) -> rdlt_connector::Result<Catalog> {
    let factory = NativeSourceFactory::new(
        "io.example.py",
        "1.0.0".to_owned(),
        "{}",
        false,
        connect(py, code),
    )
    .unwrap()
    .factory();
    let runtime = runtime::get(py).unwrap();
    py.detach(|| {
        runtime.block_on(async {
            let source = factory
                .connect(serde_json::json!({}), ConnectContext::new())
                .await?;
            source.discover().await
        })
    })
}

#[test]
fn a_stream_says_its_namespace_schema_read_modes_and_change_time_as_its_catalog_does() {
    embedded::with(|py| {
        let schema = TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new(
                "changed_at",
                LogicalType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ])
        .unwrap();
        let declared = format!(
            "{{'name': 'rows', 'namespace': 'sales', 'schema': json.loads(r'''{}'''), \
             'primary_key': ['id'], 'read_modes': ['cdc'], 'checkpointing': 'natural', 'replayable': True, \
             'change_time': 'changed_at'}}",
            serde_json::to_string(&schema).unwrap()
        );
        let code = format!(
            "{}\nconnected = types.SimpleNamespace(check=lambda: None, streams=[stream_rows], \
             discovers=False)\nconnect = lambda config, host: connected",
            stream("rows", &declared)
        );
        let catalog = discovered(py, &code).expect("the source discovers its streams' specs");
        let found = catalog
            .get(&StreamName::with_namespace("sales", "rows").unwrap())
            .expect("rows");
        assert_eq!(found.schema(), Some(&schema));
        assert_eq!(found.read_modes(), [ReadMode::Cdc]);
        assert_eq!(found.change_time(), Some(&ColumnPath::from("changed_at")));
        assert_eq!(found.primary_key(), Some(&[ColumnPath::from("id")][..]));
    });
}

#[test]
fn a_source_that_discovers_answers_its_own_catalog_checked_as_rust_checks_one() {
    embedded::with(|py| {
        // Its streams say they can be read again; the catalog it discovers says they cannot.
        let code = |answered: &[&str]| {
            let specs: Vec<String> = answered.iter().map(|name| spec(name, "False")).collect();
            format!(
                "{}{}\nconnected = types.SimpleNamespace(check=lambda: None, \
                 streams=[stream_a, stream_b], discovers=True, \
                 discover=lambda: json.dumps([{}]))\nconnect = lambda config, host: connected",
                stream("a", &spec("a", "True")),
                stream("b", &spec("b", "True")),
                specs.join(", ")
            )
        };
        let catalog = discovered(py, &code(&["a", "b"])).expect("its own catalog");
        let found: Vec<(String, bool)> = catalog
            .iter()
            .map(|spec| (spec.name().to_string(), spec.is_replayable()))
            .collect();
        assert_eq!(found, [("a".to_owned(), false), ("b".to_owned(), false)]);
        let refused = discovered(py, &code(&["a", "b", "a"])).expect_err("a stream named twice");
        assert_eq!(refused.kind(), ConnectorErrorKind::Internal);
        assert_eq!(refused.code(), None);
    });
}

#[test]
fn an_arrow_schema_is_the_table_schema_rust_makes_of_it_and_one_rust_cannot_take_is_refused() {
    embedded::with(|py| {
        let made: String = py
            .eval(
                c"__import__('rdlt._native', fromlist=['table_schema']).table_schema(__import__('pyarrow').schema([('id', __import__('pyarrow').int64()), ('at', __import__('pyarrow').timestamp('us', tz='UTC'))]).__arrow_c_schema__())",
                None,
                None,
            )
            .unwrap()
            .extract()
            .unwrap();
        let arrow = Schema::new(vec![
            ArrowField::new("id", DataType::Int64, true),
            ArrowField::new(
                "at",
                DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let schema: TableSchema = serde_json::from_str(&made).unwrap();
        assert_eq!(schema, TableSchema::from_arrow(&arrow).unwrap());
        let refused = py
            .eval(
                c"__import__('rdlt._native', fromlist=['table_schema']).table_schema(__import__('pyarrow').schema([('span', __import__('pyarrow').month_day_nano_interval())]).__arrow_c_schema__())",
                None,
                None,
            )
            .expect_err("an interval has no logical type");
        assert_eq!(class_of(py, &refused), "ValueError");
    });
}
