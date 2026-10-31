use rdlt_connector::{
    ColumnPath, Exclusion, LogicalType, OP_COLUMN, PipelineId, ReadMode, SEQ_COLUMN, StreamName,
    UNCHANGED_COLUMN,
};

use super::{DeleteMode, OnTruncate, PipelinePlan, StreamPlan, Until, WriteMode};
use crate::error::{ErrorKind, codes};
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
        stream("f").write(WriteMode::History),
        stream("g")
            .read(ReadMode::Incremental)
            .write(WriteMode::History)
            .key(["id"]),
    ];
    let plan = PipelinePlan::new(pipeline(), streams.clone()).unwrap();
    assert_eq!(plan.pipeline(), &pipeline());
    assert_eq!(plan.streams(), streams);
}

#[test]
fn unsupported_plans_are_configuration_errors() {
    let cases = [
        (vec![], codes::PLAN_EMPTY),
        (vec![stream("a"), stream("a")], codes::PLAN_DUPLICATE_STREAM),
        (
            vec![
                stream("a")
                    .read(ReadMode::Incremental)
                    .write(WriteMode::Replace),
            ],
            codes::PLAN_MODE_INVALID,
        ),
        (vec![stream("a").key(["id"])], codes::PLAN_KEY_UNUSED),
        (
            vec![
                stream("a")
                    .write(WriteMode::Merge)
                    .key(Vec::<ColumnPath>::new()),
            ],
            codes::PLAN_KEY_EMPTY,
        ),
        (
            vec![stream("a").column(nested(), SchemaSettings::new())],
            codes::PLAN_COLUMN_NESTED,
        ),
        (
            vec![stream("a").hint(nested(), LogicalType::Int64)],
            codes::PLAN_COLUMN_NESTED,
        ),
        (
            vec![stream("a").write(WriteMode::Merge).key([nested()])],
            codes::PLAN_COLUMN_NESTED,
        ),
        (
            vec![stream("a").hint("n", LogicalType::Null)],
            codes::PLAN_HINT_INVALID,
        ),
        (
            vec![stream("a").column("n", SchemaSettings::new().nested(Nested::normalize()))],
            codes::PLAN_NESTED_NORMALIZE_COLUMN,
        ),
        (
            vec![stream("a").exclude([nested()])],
            codes::PLAN_COLUMN_NESTED,
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
    assert_eq!(error.code(), Some(codes::PLAN_TABLE_COLLISION));
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

#[test]
fn normalizing_goes_eight_levels_deep_unless_told_otherwise() {
    assert_eq!(Nested::normalize(), Nested::Normalize { max_depth: 8 });
}

#[test]
fn change_streams_append_as_a_log_or_merge_with_their_deletes_and_truncates() {
    let plan = PipelinePlan::new(
        PipelineId::parse("changes").unwrap(),
        [
            stream("log").read(ReadMode::Cdc),
            stream("merged")
                .read(ReadMode::Cdc)
                .write(WriteMode::Merge)
                .deletes(DeleteMode::Soft)
                .on_truncate(OnTruncate::Ignore),
            stream("defaults")
                .read(ReadMode::Cdc)
                .write(WriteMode::Merge),
            stream("history")
                .read(ReadMode::Cdc)
                .write(WriteMode::History)
                .key(["id"])
                .deletes(DeleteMode::Soft),
        ],
    )
    .expect("change streams plan");
    let [log, merged, defaults, history] = plan.streams() else {
        panic!("four streams");
    };
    assert!(!log.merges_changes());
    assert!(merged.merges_changes());
    // A change stream kept as history applies its deletes and truncates as a merged one does.
    assert!(history.merges_changes());
    assert_eq!(history.delete_mode(), DeleteMode::Soft);
    assert!(history.keeps_history() && !merged.keeps_history());
    assert_eq!(
        (merged.delete_mode(), merged.truncate_mode()),
        (DeleteMode::Soft, OnTruncate::Ignore)
    );
    assert_eq!(
        (defaults.delete_mode(), defaults.truncate_mode()),
        (DeleteMode::Hard, OnTruncate::Apply)
    );
}

#[test]
fn change_streams_replace_nothing_and_only_merged_ones_take_delete_modes() {
    let cases = [
        (
            stream("a").read(ReadMode::Cdc).write(WriteMode::Replace),
            codes::PLAN_MODE_INVALID,
        ),
        (
            stream("a").read(ReadMode::Cdc).deletes(DeleteMode::Soft),
            codes::PLAN_DELETES_UNUSED,
        ),
        (
            stream("a")
                .read(ReadMode::Incremental)
                .write(WriteMode::Merge)
                .on_truncate(OnTruncate::Ignore),
            codes::PLAN_DELETES_UNUSED,
        ),
        (
            stream("a")
                .read(ReadMode::Incremental)
                .write(WriteMode::History)
                .deletes(DeleteMode::Soft),
            codes::PLAN_DELETES_UNUSED,
        ),
    ];
    for (stream, code) in cases {
        let error = PipelinePlan::new(PipelineId::parse("p").unwrap(), [stream]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config);
        assert_eq!(error.code(), Some(code));
    }
}

#[test]
fn a_run_commits_as_a_stream_where_it_follows_its_source_or_reads_changes() {
    let plan = |read: ReadMode, until: Until| {
        let write = if read == ReadMode::Cdc {
            WriteMode::Merge
        } else {
            WriteMode::Append
        };
        PipelinePlan::new(pipeline(), [stream("orders").read(read).write(write)])
            .unwrap()
            .with_until(until)
    };
    let cases = [
        (ReadMode::Incremental, Until::Exhausted, false),
        (ReadMode::Full, Until::Exhausted, false),
        (ReadMode::Cdc, Until::Exhausted, true),
        (ReadMode::Incremental, Until::Forever, true),
        (
            ReadMode::Full,
            Until::For(std::time::Duration::from_secs(1)),
            true,
        ),
    ];
    for (read, until, streams) in cases {
        assert_eq!(
            plan(read, until).commits_as_stream(),
            streams,
            "{read:?} {until:?}"
        );
    }
}

#[test]
fn a_column_the_stream_needs_or_sets_cannot_be_excluded() {
    let merged = || stream("a").write(WriteMode::Merge).key(["id", "tag"]);
    let cases = [
        stream("a").exclude([OP_COLUMN]),
        stream("a").exclude([SEQ_COLUMN]),
        stream("a")
            .read(ReadMode::Cdc)
            .exclude(["n", UNCHANGED_COLUMN]),
        merged().exclude(["tag"]),
        stream("a")
            .column("n", SchemaSettings::new().policy(SchemaPolicy::Freeze))
            .exclude(["n"]),
        stream("a").hint("n", LogicalType::Int64).exclude(["n"]),
    ];
    for plan in cases {
        let error = PipelinePlan::new(pipeline(), [plan.clone()]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config, "{plan:?}");
        assert_eq!(error.code(), Some(codes::PLAN_EXCLUDE_INVALID), "{plan:?}");
        assert_eq!(error.stream(), Some(&StreamName::new("a").unwrap()));
    }
    // A column named like one the stream needs is excluded where it needs none of them.
    let others = [
        merged().exclude(["n"]),
        stream("a")
            .column("n", SchemaSettings::new())
            .exclude(["m"]),
        stream("a").hint("n", LogicalType::Int64).exclude(["m"]),
        stream("a").exclude(["_rdlt_loaded_at"]),
    ];
    for plan in others {
        PipelinePlan::new(pipeline(), [plan]).unwrap();
    }
}

#[test]
fn an_exclusion_is_refused_where_a_read_s_start_could_not_carry_its_names() {
    // Each name takes 17 bytes, its 15 and its field's key and length: 262,140 bytes together,
    // and the last name's 4 or 5 more make the limit exactly, or pass it.
    let names = |last: &str| {
        let names = (0..15_420).map(|index| format!("column_{index:08}"));
        let names: Vec<String> = names.chain([last.to_owned()]).collect();
        names
    };
    let carried = names("xy");
    let carried = stream("a").exclude(carried.iter().map(String::as_str));
    PipelinePlan::new(pipeline(), [carried]).unwrap();
    let beyond = names("xyz");
    let beyond = stream("a").exclude(beyond.iter().map(String::as_str));
    let error = PipelinePlan::new(pipeline(), [beyond]).unwrap_err();
    assert_eq!(error.code(), Some(codes::PLAN_EXCLUDE_INVALID));
}

#[test]
fn a_stream_carries_the_columns_it_excludes_once_each() {
    let plan = stream("a").exclude(["b"]).exclude(["c", "b"]);
    let excluded: Vec<&ColumnPath> = plan.excluded().collect();
    assert_eq!(excluded, [&ColumnPath::from("b"), &ColumnPath::from("c")]);
    assert_eq!(plan.exclusion(), Exclusion::new(["b", "c"]));
    assert!(stream("a").exclusion().is_empty());
    let plan = PipelinePlan::new(pipeline(), [plan.clone()]).unwrap();
    assert_eq!(plan.streams()[0].exclusion(), Exclusion::new(["c", "b"]));
}
