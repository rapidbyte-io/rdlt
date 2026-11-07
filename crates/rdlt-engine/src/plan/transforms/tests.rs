use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use rdlt_connector::{ColumnPath, OP_COLUMN, PipelineId, ReadMode, Secret, StreamName};

use crate::error::{Code, Error, ErrorKind, codes};
use crate::limits::{LITERAL_BYTES, NAMED_COLUMNS, TRANSFORMS_PER_STREAM};
use crate::plan::{DeleteMode, PipelinePlan, StreamPlan, WriteMode};
use crate::transform::{Literal, Mask, Op, Transform, TransformError};

fn stream(name: &str) -> StreamPlan {
    StreamPlan::new(StreamName::new(name).unwrap())
}

fn planned(stream: StreamPlan) -> Result<PipelinePlan, Error> {
    PipelinePlan::new(PipelineId::parse("p").unwrap(), [stream])
}

fn path(dotted: &str) -> ColumnPath {
    ColumnPath::new(dotted.split('.')).unwrap()
}

/// Asserts `plan` is refused as a configuration error with `code`.
fn refused_with(plan: StreamPlan, code: Code) {
    let shown = format!("{plan:?}");
    let refused = planned(plan).unwrap_err();
    assert_eq!(
        (refused.kind(), refused.code()),
        (ErrorKind::Config, Some(code)),
        "{shown}"
    );
    assert_eq!(
        refused.stream().map(ToString::to_string),
        Some("s".to_owned())
    );
}

/// A filter keeping every row, written as an embedder's.
struct KeepAll;

impl Transform for KeepAll {
    fn name(&self) -> &'static str {
        "keep_all"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        true
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }
}

fn filter() -> Arc<dyn Transform> {
    Arc::new(KeepAll)
}

fn key(text: &str) -> Mask {
    Mask::hmac_sha256(Secret::new(text.to_owned()))
}

/// `count` names, each `prefix` and its number.
fn named(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|n| format!("{prefix}{n}")).collect()
}

/// `count` top-level columns, each named `prefix` and its number.
fn columns(prefix: &str, count: usize) -> Vec<ColumnPath> {
    let names = named(prefix, count);
    names
        .iter()
        .map(|name| ColumnPath::from(name.as_str()))
        .collect()
}

#[test]
fn includes_and_transforms_a_plan_can_carry_are_accepted() {
    let cdc = |name: &str, write: WriteMode| stream(name).read(ReadMode::Cdc).write(write);
    let streams = [
        stream("a").include_only(["id", "name"]),
        stream("b")
            .transform(Op::rename([("a", "b")]))
            .transform(Op::constant([("c", Literal::Integer(1))])),
        stream("c").transform(filter()),
        stream("d").write(WriteMode::Replace).transform(filter()),
        cdc("e", WriteMode::Merge).transform(filter()),
        cdc("f", WriteMode::Merge)
            .deletes(DeleteMode::Soft)
            .transform(filter()),
        cdc("g", WriteMode::History).transform(filter()),
        cdc("h", WriteMode::Append).transform(filter()),
        stream("i").transform(Op::mask([path("s.inner"), path("t")], key("k"))),
        stream("j").transform(Op::mask([path("s.a"), path("s.b")], Mask::Null)),
        stream("k")
            .transform(Op::mask(["a"], Mask::Null))
            .transform(Op::mask(["a"], Mask::redact("x"))),
        stream("l").include_only(columns("c", NAMED_COLUMNS)),
    ];
    for plan in streams {
        let name = plan.name().clone();
        assert!(planned(plan).is_ok(), "{name}");
    }
}

#[test]
fn includes_a_plan_cannot_carry_are_refused() {
    let cases = [
        (
            stream("s").include_only(["a"]).exclude(["b"]),
            codes::PLAN_INCLUDE_INVALID,
        ),
        (
            stream("s").include_only(Vec::<ColumnPath>::new()),
            codes::PLAN_INCLUDE_INVALID,
        ),
        (
            stream("s").include_only(columns("c", NAMED_COLUMNS + 1)),
            codes::PLAN_INCLUDE_INVALID,
        ),
        (
            stream("s").include_only([path("a.b")]),
            codes::PLAN_COLUMN_NESTED,
        ),
    ];
    for (plan, code) in cases {
        refused_with(plan, code);
    }
}

#[test]
fn ops_naming_a_key_a_change_column_or_an_engine_name_are_refused() {
    let merged = || stream("s").write(WriteMode::Merge).key(["id"]);
    let plans = [
        merged().transform(Op::rename([("id", "key")])),
        merged().transform(Op::rename([("key", "id")])),
        merged().transform(Op::mask(["id"], Mask::Null)),
        merged().transform(Op::constant([("id", Literal::Integer(1))])),
        stream("s").transform(Op::rename([(OP_COLUMN, "op")])),
        stream("s").transform(Op::mask([OP_COLUMN], Mask::Null)),
        stream("s").transform(Op::rename([("a", "_rdlt_a")])),
        stream("s").transform(Op::constant([("_rdlt_c", Literal::Integer(1))])),
        stream("s").transform(Op::rename([("", "a")])),
        stream("s").transform(Op::constant([("", Literal::Integer(1))])),
    ];
    for plan in plans {
        refused_with(plan, codes::TRANSFORM_INVALID);
    }
}

#[test]
fn an_op_reading_a_name_with_the_engine_s_prefix_is_refused() {
    let plans = [
        stream("s").transform(Op::mask(["_rdlt_x"], Mask::Null)),
        stream("s").transform(Op::mask([path("_rdlt_x.inner")], Mask::Null)),
        stream("s").transform(Op::rename([("_rdlt_y", "y")])),
    ];
    for plan in plans {
        refused_with(plan, codes::TRANSFORM_INVALID);
    }
}

#[test]
fn ops_naming_one_column_twice_are_refused() {
    let plans = [
        stream("s")
            .transform(Op::rename([("a", "x")]))
            .transform(Op::constant([("x", Literal::Integer(1))])),
        stream("s").transform(Op::rename([("a", "b"), ("c", "b")])),
        stream("s").transform(Op::rename([("a", "b"), ("a", "c")])),
        stream("s").transform(Op::rename([("a", "b"), ("b", "c")])),
        stream("s").transform(Op::mask([path("s"), path("s.inner")], Mask::Null)),
        stream("s").transform(Op::mask(
            [path("s.inner.deep"), path("t"), path("s.inner")],
            Mask::Null,
        )),
    ];
    for plan in plans {
        refused_with(plan, codes::TRANSFORM_INVALID);
    }
}

#[test]
fn a_mask_naming_no_column_or_keyed_by_nothing_is_refused() {
    let plans = [
        stream("s").transform(Op::mask(Vec::<ColumnPath>::new(), Mask::Null)),
        stream("s").transform(Op::mask(["s"], key(""))),
    ];
    for plan in plans {
        refused_with(plan, codes::TRANSFORM_INVALID);
    }
}

#[test]
fn a_filter_whose_rows_would_stay_in_their_table_is_refused() {
    let plans = [
        stream("s").write(WriteMode::Merge).transform(filter()),
        stream("s")
            .read(ReadMode::Incremental)
            .write(WriteMode::Merge)
            .transform(filter()),
        stream("s").write(WriteMode::History).transform(filter()),
        stream("s")
            .read(ReadMode::Cdc)
            .write(WriteMode::Merge)
            .deletes(DeleteMode::Ignore)
            .transform(filter()),
        stream("s")
            .read(ReadMode::Cdc)
            .write(WriteMode::History)
            .deletes(DeleteMode::Ignore)
            .transform(filter()),
    ];
    for plan in plans {
        refused_with(plan, codes::TRANSFORM_INVALID);
    }
}

#[test]
fn a_stream_listing_more_transforms_than_the_limit_is_refused() {
    let many = (0..=TRANSFORMS_PER_STREAM).fold(stream("s"), |plan, n| {
        plan.transform(Op::constant([(format!("c{n}"), Literal::Integer(1))]))
    });
    refused_with(many, codes::TRANSFORM_INVALID);
}

#[test]
fn a_stream_lists_as_many_transforms_as_the_limit() {
    let most = (0..TRANSFORMS_PER_STREAM).fold(stream("s"), |plan, n| {
        plan.transform(Op::constant([(format!("c{n}"), Literal::Integer(1))]))
    });
    assert!(planned(most).is_ok());
}

#[test]
fn a_mask_naming_one_path_twice_is_refused() {
    for paths in [["s", "s"], ["s.inner", "s.inner"]] {
        let mask = Op::mask(paths.map(path), Mask::Null);
        refused_with(stream("s").transform(mask), codes::TRANSFORM_INVALID);
    }
}

#[test]
fn a_rename_giving_a_column_its_own_name_is_refused() {
    for name in ["id", "a"] {
        let rename = Op::rename([(name, name)]);
        refused_with(stream("s").transform(rename), codes::TRANSFORM_INVALID);
    }
}

#[test]
fn an_op_naming_more_columns_than_the_limit_is_refused_and_one_naming_as_many_is_not() {
    let ops = |count: usize| {
        let pairs = named("a", count).into_iter().zip(named("b", count));
        let constants = named("c", count)
            .into_iter()
            .map(|name| (name, Literal::Integer(1)));
        [
            Op::rename(pairs),
            Op::constant(constants),
            Op::mask(columns("m", count), Mask::Null),
        ]
    };
    for op in ops(NAMED_COLUMNS) {
        assert!(planned(stream("s").transform(op)).is_ok());
    }
    for op in ops(NAMED_COLUMNS + 1) {
        refused_with(stream("s").transform(op), codes::TRANSFORM_INVALID);
    }
}

#[test]
fn a_constant_or_redaction_longer_than_the_limit_is_refused_and_one_as_long_is_not() {
    let ops = |bytes: usize| {
        let text = "x".repeat(bytes);
        [
            Op::constant([("c", Literal::Text(text.clone()))]),
            Op::mask(["s"], Mask::redact(text)),
        ]
    };
    for op in ops(LITERAL_BYTES) {
        assert!(planned(stream("s").transform(op)).is_ok());
    }
    for op in ops(LITERAL_BYTES + 1) {
        refused_with(stream("s").transform(op), codes::TRANSFORM_INVALID);
    }
}

#[test]
fn plans_compare_ops_by_value_and_code_by_identity() {
    let code = filter();
    let with = |code: &Arc<dyn Transform>| {
        stream("s")
            .include_only(["a"])
            .transform(Op::rename([("a", "b")]))
            .transform(Arc::clone(code))
    };
    assert_eq!(with(&code), with(&code));
    assert_ne!(with(&code), with(&filter()));
    assert_eq!(with(&code).transforms().len(), 2);
    assert_eq!(with(&code).included().map(BTreeSet::len), Some(1));
    assert_ne!(with(&code), stream("s").transform(Arc::clone(&code)));
}

#[test]
fn a_stream_without_include_or_transforms_lists_neither() {
    let plain = stream("s");
    assert!(plain.included().is_none());
    assert!(plain.transforms().is_empty());
}
