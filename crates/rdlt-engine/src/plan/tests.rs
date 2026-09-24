use rdlt_connector::{ColumnPath, LogicalType, PipelineId, ReadMode, StreamName};

use super::{PipelinePlan, StreamPlan, WriteMode};
use crate::error::ErrorKind;
use crate::policy::{Nested, SchemaPolicy, SchemaSettings};

fn stream(name: &str) -> StreamPlan {
    StreamPlan::new(StreamName::new(name).unwrap())
}

fn pipeline() -> PipelineId {
    PipelineId::parse("orders").unwrap()
}

#[test]
fn a_stream_defaults_to_a_full_read_appended() {
    let plan = stream("a");
    assert_eq!(plan.name(), &StreamName::new("a").unwrap());
    assert_eq!(plan.read_mode(), ReadMode::Full);
    assert_eq!(plan.write_mode(), WriteMode::Append);
}

#[test]
fn supported_mode_combinations_are_accepted() {
    let streams = [
        stream("a"),
        stream("b").write(WriteMode::Replace),
        stream("c").read(ReadMode::Incremental),
        stream("d").write(WriteMode::Merge),
        stream("e")
            .read(ReadMode::Incremental)
            .write(WriteMode::Merge)
            .key(["id"]),
    ];
    let plan = PipelinePlan::new(pipeline(), streams.clone()).unwrap();
    assert_eq!(plan.pipeline(), &pipeline());
    assert_eq!(plan.streams(), streams);
}

#[test]
fn unsupported_plans_are_configuration_errors() {
    let cases = [
        (vec![], "plan_empty"),
        (vec![stream("a"), stream("a")], "plan_duplicate_stream"),
        (
            vec![
                stream("a")
                    .read(ReadMode::Incremental)
                    .write(WriteMode::Replace),
            ],
            "plan_mode_invalid",
        ),
        (
            vec![stream("a").read(ReadMode::Cdc)],
            "plan_mode_unsupported",
        ),
        (vec![stream("a").key(["id"])], "plan_key_unused"),
        (
            vec![
                stream("a")
                    .write(WriteMode::Merge)
                    .key(Vec::<ColumnPath>::new()),
            ],
            "plan_key_empty",
        ),
        (
            vec![stream("a").column(nested(), SchemaSettings::new())],
            "plan_column_nested",
        ),
        (
            vec![stream("a").hint(nested(), LogicalType::Int64)],
            "plan_column_nested",
        ),
        (
            vec![stream("a").write(WriteMode::Merge).key([nested()])],
            "plan_column_nested",
        ),
        (
            vec![stream("a").hint("n", LogicalType::Null)],
            "plan_hint_invalid",
        ),
    ];
    for (streams, code) in cases {
        let error = PipelinePlan::new(pipeline(), streams).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config);
        assert_eq!(error.code(), Some(code));
    }
    let duplicate = PipelinePlan::new(pipeline(), [stream("a"), stream("a")]).unwrap_err();
    assert_eq!(duplicate.stream(), Some(&StreamName::new("a").unwrap()));
}

#[test]
fn streams_that_would_share_a_table_are_refused() {
    let dotted = StreamPlan::new(StreamName::new("public.orders").unwrap());
    let namespaced = StreamPlan::new(StreamName::with_namespace("public", "orders").unwrap());
    let error = PipelinePlan::new(pipeline(), [dotted, namespaced]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.code(), Some("plan_table_collision"));
}

fn nested() -> ColumnPath {
    ColumnPath::new(["a", "b"]).unwrap()
}

#[test]
fn plans_carry_keys_hints_and_schema_settings() {
    let settings = SchemaSettings::new().policy(SchemaPolicy::Freeze);
    let column = SchemaSettings::new().nested(Nested::Json);
    let plan = stream("a")
        .write(WriteMode::Merge)
        .key(["id"])
        .schema(settings)
        .column("payload", column)
        .hint("amount", LogicalType::Int64);
    assert_eq!(plan.merge_key(), Some(&[ColumnPath::from("id")][..]));
    assert_eq!(plan.schema_settings(), &settings);
    assert_eq!(
        plan.column_settings(&ColumnPath::from("payload")),
        Some(&column)
    );
    assert_eq!(plan.column_settings(&ColumnPath::from("id")), None);
    assert_eq!(
        plan.hinted(&ColumnPath::from("amount")),
        Some(&LogicalType::Int64)
    );
    assert_eq!(stream("b").merge_key(), None);
    let pipeline = PipelinePlan::new(pipeline(), [plan])
        .unwrap()
        .schema(settings);
    assert_eq!(pipeline.schema_settings(), &settings);
}
