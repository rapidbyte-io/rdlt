use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int32Type;
use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef, BooleanArray, DictionaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
    StringViewArray, StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use rdlt_connector::testing::{Outcome, Report};
use rdlt_connector::{ColumnPath, Secret};
use rdlt_engine::{Bound, Literal, Mask, Op, Transform, TransformError};

use super::{TransformInputs, certify_transform};

/// Two batches of one schema: an id, a name, a note and a struct holding a secret, of `rows`
/// rows each.
fn inputs(rows: i64) -> TransformInputs {
    let batch = |first: i64| {
        let ids = Int64Array::from_iter_values(first..first + rows);
        let names =
            StringArray::from_iter_values((first..first + rows).map(|id| format!("näme-{id}")));
        let secret: ArrayRef = Arc::new(StringArray::from_iter_values(
            (first..first + rows).map(|id| format!("s{id}")),
        ));
        let nested = StructArray::new(
            Fields::from(vec![Field::new("secret", DataType::Utf8, false)]),
            vec![secret],
            None,
        );
        RecordBatch::try_from_iter([
            ("id", Arc::new(ids) as ArrayRef),
            ("name", Arc::new(names.clone()) as ArrayRef),
            ("note", Arc::new(names) as ArrayRef),
            ("s", Arc::new(nested) as ArrayRef),
        ])
        .unwrap()
    };
    TransformInputs {
        batches: vec![batch(0), batch(rows)],
        keys: vec!["id".to_owned()],
        change_time: None,
    }
}

fn outcome<'a>(report: &'a Report, id: &str) -> &'a Outcome {
    &report
        .results
        .iter()
        .find(|result| result.clause.id == id)
        .unwrap()
        .outcome
}

/// How a clause fared: `passed`, `failed`, `inapplicable` or `unobserved`.
fn kind(report: &Report, id: &str) -> &'static str {
    match outcome(report, id) {
        Outcome::Passed => "passed",
        Outcome::Failed(_) => "failed",
        Outcome::Inapplicable(_) => "inapplicable",
        Outcome::Unobserved(_) => "unobserved",
    }
}

/// The reason a clause gave, or empty where it passed.
fn reason(report: &Report, id: &str) -> String {
    match outcome(report, id) {
        Outcome::Passed => String::new(),
        Outcome::Failed(reason) | Outcome::Inapplicable(reason) | Outcome::Unobserved(reason) => {
            reason.to_string()
        }
    }
}

/// Two batches of the same columns as [`inputs`], holding nulls, text views, dictionaries and
/// slices of larger batches, of `rows` rows each.
fn layered(rows: i64) -> TransformInputs {
    let batch = |first: i64| {
        let ids = first - 1..first + rows + 2;
        let names: Vec<Option<String>> = ids
            .clone()
            .map(|id| (id % 4 != 3).then(|| format!("näme-{id}")))
            .collect();
        let notes: Vec<Option<&str>> = ids
            .clone()
            .map(|id| {
                (id % 4 != 3).then(|| ["a", "b", "c"][usize::try_from(id.rem_euclid(3)).unwrap()])
            })
            .collect();
        let secret: ArrayRef = Arc::new(StringArray::from_iter_values(
            ids.clone().map(|id| format!("s{id}")),
        ));
        let nested = StructArray::new(
            Fields::from(vec![Field::new("secret", DataType::Utf8, false)]),
            vec![secret],
            None,
        );
        let notes: DictionaryArray<Int32Type> = notes.into_iter().collect();
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from_iter_values(ids)) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringViewArray::from_iter(names)) as ArrayRef,
            ),
            ("note", Arc::new(notes) as ArrayRef),
            ("s", Arc::new(nested) as ArrayRef),
        ])
        .unwrap()
        .slice(1, usize::try_from(rows).unwrap())
    };
    TransformInputs {
        batches: vec![batch(0), batch(rows)],
        keys: vec!["id".to_owned()],
        change_time: None,
    }
}

#[test]
fn every_op_meets_every_t_clause() {
    let key = || Mask::hmac_sha256(Secret::new("k".to_owned()));
    let ops = [
        Op::rename([("note", "remark")]),
        Op::constant([
            ("t", Literal::Text("x".to_owned())),
            ("i", Literal::Integer(3)),
            ("b", Literal::Boolean(true)),
        ]),
        Op::mask(
            [
                ColumnPath::from("name"),
                ColumnPath::new(["s", "secret"]).unwrap(),
            ],
            key(),
        ),
        Op::mask(["note"], Mask::redact("***")),
        Op::mask(["note"], key()),
        Op::mask(["s"], Mask::Null),
    ];
    for rows in [0, 1, 5, 1000] {
        for op in &ops {
            for given in [inputs(rows), layered(rows)] {
                let report = certify_transform(op, &given);
                assert_eq!(report.failures().count(), 0, "{report}");
                if rows >= 5 {
                    report.assert_passed();
                }
            }
        }
    }
}

/// A transform breaking one clause on purpose.
enum Bad {
    /// Numbers rows across batches: state carried from one to the next.
    Stateful(AtomicI64),
    /// Adds one to every id.
    Rekeys,
    /// Drops its last row.
    Drops,
    /// Returns a batch unlike the schema it declares.
    Lies,
    /// Declares every output column reads only itself, yet copies `name` into `note`.
    Overreads,
    /// Allocates beyond the bound it declares.
    Overruns,
    /// Answers each call with another value.
    Wavers(AtomicI64),
    /// Reverses its rows.
    Reverses,
    /// Keeps only the first row of each batch.
    KeepsFirst,
    /// Answers one value too few for the rows it filters.
    ShortKeep,
    /// Moves its first row last.
    Rotates,
    /// Keeps every other row of a batch, and says it reads every column.
    EveryOther,
    /// Reads each name's id from its place in the buffer, as if no batch were a slice.
    Slices,
    /// Returns a column fewer when its batch does not begin at id 0.
    Narrows,
    /// Returns a batch carrying metadata its schema does not declare.
    Tags,
    /// Declares a lineage of the wrong length.
    ShortLineage,
    /// Declares a lineage naming the column one past its input's last.
    FarLineage,
    /// Allocates a fixed amount a call, beyond a bound that grows with the rows.
    Fixed,
    /// Adds one to every id of a batch of two or three rows.
    Small,
    /// Returns schema metadata its declaration lacks on batches of fewer than three rows.
    TagsSmall,
    /// Declares a nullable column and returns it as one that is not.
    Nullable,
    /// Declares a column with field metadata and returns it with none.
    Labelled,
}

impl Transform for Bad {
    fn name(&self) -> &'static str {
        match self {
            Self::Stateful(_) => "stateful",
            Self::Rekeys => "rekeys",
            Self::Drops => "drops",
            Self::Lies => "lies",
            Self::Overreads => "overreads",
            Self::Overruns => "overruns",
            Self::Wavers(_) => "wavers",
            Self::Reverses => "reverses",
            Self::KeepsFirst => "keeps_first",
            Self::ShortKeep => "short_keep",
            Self::Rotates => "rotates",
            Self::EveryOther => "every_other",
            Self::Slices => "slices",
            Self::Narrows => "narrows",
            Self::Tags => "tags",
            Self::TagsSmall => "tags_small",
            Self::ShortLineage => "short_lineage",
            Self::FarLineage => "far_lineage",
            Self::Fixed => "fixed",
            Self::Small => "small",
            Self::Nullable => "nullable",
            Self::Labelled => "labelled",
        }
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        let mut fields: Vec<_> = input.fields().iter().cloned().collect();
        if matches!(
            self,
            Self::Stateful(_) | Self::Wavers(_) | Self::Overruns | Self::Narrows
        ) {
            fields.push(Arc::new(Field::new("n", DataType::Int64, false)));
        }
        match self {
            Self::Nullable => fields.push(Arc::new(Field::new("n", DataType::Int64, true))),
            Self::Labelled => fields.push(Arc::new(
                Field::new("n", DataType::Int64, false)
                    .with_metadata(HashMap::from([("k".to_owned(), "v".to_owned())])),
            )),
            _ => {}
        }
        if matches!(self, Self::Fixed) {
            let pad = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
            fields.push(Arc::new(Field::new("pad", pad, false)));
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    fn filters(&self) -> bool {
        matches!(self, Self::KeepsFirst | Self::ShortKeep | Self::EveryOther)
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        let rows = batch.num_rows();
        Ok(match self {
            Self::KeepsFirst => (0..rows).map(|row| Some(row == 0)).collect(),
            Self::ShortKeep => (1..rows).map(|_| Some(true)).collect(),
            Self::EveryOther => (0..rows).map(|row| Some(row % 2 == 0)).collect(),
            _ => (0..rows).map(|_| Some(true)).collect(),
        })
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        let rows = batch.num_rows();
        let schema = self.output(batch.schema_ref())?;
        let with = |n: Int64Array| {
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(n));
            RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
        };
        let numbered = |counter: &AtomicI64| {
            let first = counter.fetch_add(i64::try_from(rows).unwrap(), Ordering::Relaxed);
            Int64Array::from_iter_values(first..first + i64::try_from(rows).unwrap())
        };
        Ok(match self {
            Self::Stateful(counter) => with(numbered(counter)),
            Self::Wavers(counter) => with(Int64Array::from_value(
                counter.fetch_add(1, Ordering::Relaxed),
                rows,
            )),
            Self::Overruns => with(Int64Array::from_value(0, rows)),
            Self::Rekeys => {
                let ids = batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .unary::<_, Int64Type>(|id| id + 1);
                let mut columns = batch.columns().to_vec();
                columns[0] = Arc::new(ids);
                RecordBatch::try_new(schema, columns).unwrap()
            }
            other => other.damaged(batch, &schema),
        })
    }

    fn bound(&self, _input: &Schema) -> Option<Bound> {
        match self {
            Self::Overruns => Some(Bound::new(0)),
            Self::Fixed => Some(Bound::new(0).with_rows(100)),
            _ => None,
        }
    }

    fn lineage(&self, input: &Schema) -> Option<Vec<Vec<usize>>> {
        let identity = || (0..input.fields().len()).map(|column| vec![column]);
        match self {
            Self::Overreads => Some(identity().collect()),
            Self::Narrows => Some(identity().chain([vec![0]]).collect()),
            Self::ShortLineage => Some(Vec::new()),
            Self::FarLineage => Some(
                identity()
                    .map(|mut reads| {
                        reads.push(input.fields().len());
                        reads
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    fn keep_reads(&self, input: &Schema) -> Option<Vec<usize>> {
        matches!(self, Self::EveryOther).then(|| (0..input.fields().len()).collect())
    }
}

/// `batch` under `schema` with metadata added.
fn tagged(batch: &RecordBatch, schema: &SchemaRef) -> RecordBatch {
    let tagged = schema
        .as_ref()
        .clone()
        .with_metadata(HashMap::from([("tag".to_owned(), "x".to_owned())]));
    RecordBatch::try_new(Arc::new(tagged), batch.columns().to_vec()).unwrap()
}

impl Bad {
    /// `batch` damaged in the way `self` says; `schema` is the output schema it declares.
    fn damaged(&self, batch: RecordBatch, schema: &SchemaRef) -> RecordBatch {
        let rows = batch.num_rows();
        match self {
            Self::Small if (2..=3).contains(&rows) => {
                let ids = batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .unary::<_, Int64Type>(|id| id + 1);
                let mut columns = batch.columns().to_vec();
                columns[0] = Arc::new(ids);
                RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
            }
            Self::Rotates => {
                let rows = u64::try_from(rows).unwrap();
                let indices = UInt64Array::from_iter_values((1..rows).chain(0..1.min(rows)));
                arrow_select::take::take_record_batch(&batch, &indices).unwrap()
            }
            Self::Slices => {
                let names = batch.column(1).as_string::<i32>();
                let data = names.value_data();
                let ids = (0..rows).map(|row| i64::from(data[row * 7 + 6]) - i64::from(b'0'));
                let mut columns = batch.columns().to_vec();
                columns[0] = Arc::new(Int64Array::from_iter_values(ids));
                RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
            }
            Self::Narrows => {
                let first = batch.column(0).as_primitive::<Int64Type>().value(0);
                let mut columns = batch.columns().to_vec();
                let fields = if first == 0 {
                    columns.push(Arc::new(Int64Array::from_value(0, rows)));
                    schema.fields().clone()
                } else {
                    Fields::from(schema.fields()[..columns.len()].to_vec())
                };
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
            }
            Self::Fixed => {
                let values = StringArray::from_iter_values(["x".repeat(2000)]);
                let pad = DictionaryArray::<Int32Type>::new(
                    Int32Array::from_value(0, rows),
                    Arc::new(values),
                );
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(pad));
                RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
            }
            Self::Drops => batch.slice(0, rows.saturating_sub(1)),
            Self::Lies => batch.project(&[0]).unwrap(),
            Self::Overreads => {
                let mut columns = batch.columns().to_vec();
                columns[2] = Arc::clone(&columns[1]);
                RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
            }
            Self::Reverses => {
                let indices =
                    UInt64Array::from_iter_values((0..u64::try_from(rows).unwrap()).rev());
                arrow_select::take::take_record_batch(&batch, &indices).unwrap()
            }
            other => other.relabelled(batch, schema),
        }
    }

    /// `batch` returned under a schema other than the schema it declares.
    fn relabelled(&self, batch: RecordBatch, schema: &SchemaRef) -> RecordBatch {
        let rows = batch.num_rows();
        match self {
            Self::Tags => tagged(&batch, schema),
            Self::TagsSmall if rows < 3 => tagged(&batch, schema),
            Self::Nullable | Self::Labelled => {
                let mut fields = batch.schema().fields().to_vec();
                fields.push(Arc::new(Field::new("n", DataType::Int64, false)));
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(Int64Array::from_value(0, rows)));
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
            }
            _ => batch,
        }
    }
}

#[test]
fn a_transform_breaking_a_clause_fails_it() {
    let cases = [
        (Bad::Stateful(AtomicI64::new(0)), "T-ROWWISE"),
        (Bad::Rekeys, "T-KEYS"),
        (Bad::Drops, "T-ROWS"),
        (Bad::Lies, "T-SCHEMA"),
        (Bad::Overruns, "T-BOUND"),
        (Bad::Wavers(AtomicI64::new(0)), "T-REPLAY"),
        (Bad::Reverses, "T-ROWS"),
        (Bad::Rotates, "T-ROWS"),
        (Bad::KeepsFirst, "T-ROWWISE"),
        (Bad::ShortKeep, "T-ROWS"),
        (Bad::EveryOther, "T-ROWS"),
        (Bad::Slices, "T-ROWWISE"),
        (Bad::Small, "T-ROWWISE"),
        (Bad::Narrows, "T-ROWWISE"),
        (Bad::Narrows, "T-LINEAGE"),
        (Bad::Tags, "T-SCHEMA"),
        (Bad::Nullable, "T-SCHEMA"),
        (Bad::Labelled, "T-SCHEMA"),
        (Bad::Overreads, "T-LINEAGE"),
        (Bad::ShortLineage, "T-LINEAGE"),
        (Bad::FarLineage, "T-LINEAGE"),
    ];
    for (bad, id) in cases {
        let report = certify_transform(&bad, &inputs(5));
        assert_eq!(kind(&report, id), "failed", "{id}: {report}");
    }
}

#[test]
fn a_bound_is_held_on_the_parts_the_engine_cuts() {
    let report = certify_transform(&Bad::Fixed, &inputs(1000));
    assert_eq!(kind(&report, "T-BOUND"), "failed", "{report}");
    assert!(reason(&report, "T-BOUND").contains("first row"), "{report}");
    let report = certify_transform(&Bad::Overruns, &inputs(5));
    assert!(
        reason(&report, "T-BOUND").contains("whole batch"),
        "{report}"
    );
}

#[test]
fn a_clause_that_saw_nothing_is_unobserved_not_passed() {
    let empty = inputs(0);
    let cases = [
        (Bad::Wavers(AtomicI64::new(0)), "T-REPLAY"),
        (Bad::Stateful(AtomicI64::new(0)), "T-ROWWISE"),
        (Bad::Rekeys, "T-KEYS"),
    ];
    for (bad, id) in cases {
        let report = certify_transform(&bad, &empty);
        assert_eq!(kind(&report, id), "unobserved", "{id}: {report}");
    }
    let mut misnamed = inputs(5);
    misnamed.keys = vec!["Id".to_owned()];
    let report = certify_transform(&Bad::Rekeys, &misnamed);
    assert_eq!(kind(&report, "T-KEYS"), "unobserved", "{report}");
    assert!(reason(&report, "T-KEYS").contains("Id"), "{report}");
}

#[test]
fn the_change_time_passes_as_it_arrives() {
    let mut timed = inputs(5);
    timed.keys.clear();
    timed.change_time = Some("id".to_owned());
    let report = certify_transform(&Bad::Rekeys, &timed);
    assert_eq!(kind(&report, "T-KEYS"), "failed", "{report}");
}

#[test]
fn inputs_a_transform_refuses_leave_the_clauses_their_declarations_make_inapplicable() {
    struct Refuses;
    impl Transform for Refuses {
        fn name(&self) -> &'static str {
            "refuses"
        }
        fn output(&self, _input: &Schema) -> Result<SchemaRef, TransformError> {
            Err(TransformError::new("no"))
        }
        fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
            Ok(batch)
        }
    }
    let mut keyless = inputs(3);
    keyless.keys.clear();
    let report = certify_transform(&Refuses, &keyless);
    assert_eq!(kind(&report, "T-KEYS"), "inapplicable", "{report}");
    assert_eq!(kind(&report, "T-READS"), "inapplicable", "{report}");
    assert_eq!(kind(&report, "T-REPLAY"), "unobserved", "{report}");
    let report = certify_transform(&Refuses, &inputs(3));
    assert_eq!(kind(&report, "T-KEYS"), "unobserved", "{report}");
}

struct Unbounded;
impl Transform for Unbounded {
    fn name(&self) -> &'static str {
        "unbounded"
    }
    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }
}

#[test]
fn clauses_a_transform_or_its_inputs_give_nothing_to_check_are_not_failed() {
    let mut keyless = inputs(3);
    keyless.keys.clear();
    keyless.batches.truncate(1);
    let report = certify_transform(&Op::rename([("note", "remark")]), &keyless);
    assert_eq!(kind(&report, "T-KEYS"), "inapplicable", "{report}");
    let report = certify_transform(&Unbounded, &inputs(3));
    assert_eq!(kind(&report, "T-BOUND"), "inapplicable", "{report}");
    assert_eq!(kind(&report, "T-LINEAGE"), "inapplicable", "{report}");
    assert_eq!(kind(&report, "T-READS"), "inapplicable", "{report}");
}

/// Keeps the rows whose name sorts after `näme-2`, and says it reads the columns `declares`.
struct Status {
    declares: Vec<usize>,
    filters: bool,
    bound: Option<Bound>,
}

fn status(declares: Vec<usize>) -> Status {
    Status {
        declares,
        filters: true,
        bound: None,
    }
}

impl Transform for Status {
    fn name(&self) -> &'static str {
        "status"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        self.filters
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        let names = batch.column(1).as_string::<i32>();
        Ok(names
            .iter()
            .map(|name| name.map(|name| name > "näme-2"))
            .collect())
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }

    fn bound(&self, _input: &Schema) -> Option<Bound> {
        self.bound.clone()
    }

    fn keep_reads(&self, _input: &Schema) -> Option<Vec<usize>> {
        Some(self.declares.clone())
    }
}

#[test]
fn a_filter_is_held_to_the_columns_it_declares_it_reads() {
    let cases = [
        (vec![1], "passed"),
        (vec![0], "failed"),
        (vec![1, 9], "failed"),
    ];
    for (declares, expected) in cases {
        let report = certify_transform(&status(declares), &inputs(5));
        assert_eq!(kind(&report, "T-READS"), expected, "{report}");
    }
    let report = certify_transform(&Unbounded, &inputs(5));
    assert_eq!(kind(&report, "T-READS"), "inapplicable", "{report}");
    let silent = Status {
        filters: false,
        ..status(vec![])
    };
    let report = certify_transform(&silent, &inputs(5));
    assert_eq!(kind(&report, "T-READS"), "inapplicable", "{report}");
}

#[test]
fn a_filter_meets_the_clauses_and_is_bounded_by_what_it_allocates() {
    let report = certify_transform(&status(vec![1]), &inputs(5));
    assert_eq!(kind(&report, "T-ROWS"), "passed", "{report}");
    let tight = Status {
        bound: Some(Bound::new(0)),
        ..status(vec![1])
    };
    let report = certify_transform(&tight, &inputs(5));
    assert_eq!(kind(&report, "T-BOUND"), "failed", "{report}");
    let roomy = Status {
        bound: Some(Bound::new(100_000)),
        ..status(vec![1])
    };
    let report = certify_transform(&roomy, &inputs(5));
    assert_eq!(kind(&report, "T-BOUND"), "passed", "{report}");
}

#[test]
fn a_check_that_moved_nothing_is_unobserved_not_passed() {
    let rename = Op::rename([("note", "remark")]);
    let mut single = inputs(1);
    single.batches.truncate(1);
    let report = certify_transform(&rename, &single);
    assert_eq!(kind(&report, "T-LINEAGE"), "unobserved", "{report}");
    assert_eq!(kind(&report, "T-ROWS"), "unobserved", "{report}");
    assert_eq!(kind(&report, "T-ROWWISE"), "unobserved", "{report}");
    let report = certify_transform(&rename, &inputs(1));
    assert_eq!(kind(&report, "T-ROWWISE"), "passed", "{report}");
    let report = certify_transform(&status(vec![1]), &single);
    assert_eq!(kind(&report, "T-READS"), "unobserved", "{report}");
    let report = certify_transform(&rename, &inputs(2));
    assert_eq!(kind(&report, "T-LINEAGE"), "passed", "{report}");
    assert_eq!(kind(&report, "T-ROWWISE"), "passed", "{report}");
    assert_eq!(kind(&report, "T-ROWS"), "unobserved", "{report}");
    let report = certify_transform(&rename, &inputs(3));
    assert_eq!(kind(&report, "T-ROWS"), "passed", "{report}");
}

#[test]
fn a_column_that_never_differs_when_moved_is_named_and_leaves_the_clause_unobserved() {
    let mut constant = inputs(5);
    for batch in &mut constant.batches {
        let mut columns = batch.columns().to_vec();
        columns[2] = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            "same", 5,
        )));
        *batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
    }
    let report = certify_transform(&Op::constant([("i", Literal::Integer(3))]), &constant);
    assert_eq!(kind(&report, "T-LINEAGE"), "unobserved", "{report}");
    assert!(reason(&report, "T-LINEAGE").contains("note"), "{report}");
    let report = certify_transform(&status(vec![1]), &constant);
    assert_eq!(kind(&report, "T-READS"), "unobserved", "{report}");
    assert!(reason(&report, "T-READS").contains("note"), "{report}");
}

#[test]
fn a_named_column_no_batch_shows_is_unobserved_beside_one_that_is_shown() {
    let mut timed = inputs(5);
    timed.change_time = Some("Time".to_owned());
    let report = certify_transform(&Op::rename([("note", "remark")]), &timed);
    assert_eq!(kind(&report, "T-KEYS"), "unobserved", "{report}");
    assert!(reason(&report, "T-KEYS").contains("Time"), "{report}");
}

#[test]
fn batches_without_rows_leave_what_only_they_could_show_unobserved() {
    let rename = Op::rename([("note", "remark")]);
    let whole = inputs(5).batches.remove(0);
    let bare = whole.slice(0, 0);
    let without_id = whole.project(&[1, 2, 3]).unwrap();
    let keyed = TransformInputs {
        batches: vec![bare.clone(), without_id],
        keys: vec!["id".to_owned()],
        change_time: None,
    };
    let report = certify_transform(&rename, &keyed);
    assert_eq!(kind(&report, "T-KEYS"), "unobserved", "{report}");
    let lone = whole.project(&[0, 1, 2]).unwrap().slice(0, 1);
    let paired = TransformInputs {
        batches: vec![bare.clone(), bare, lone],
        ..TransformInputs::default()
    };
    let report = certify_transform(&rename, &paired);
    assert_eq!(kind(&report, "T-ROWWISE"), "unobserved", "{report}");
}

#[test]
fn schema_metadata_that_varies_with_the_rows_fails_rowwise() {
    let report = certify_transform(&Bad::TagsSmall, &inputs(5));
    assert_eq!(kind(&report, "T-ROWWISE"), "failed", "{report}");
}
