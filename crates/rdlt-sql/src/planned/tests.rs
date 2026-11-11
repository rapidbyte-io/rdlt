use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date64Array, Decimal256Array,
    DictionaryArray, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray,
    RecordBatch, RunArray, StringArray, StringViewArray,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::col;
use datafusion_expr::logical_plan::builder::{LogicalPlanBuilder, LogicalTableSource};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use rdlt_engine::{Bound, NAMED_COLUMNS, Transform, allocated_beyond, codes};

use super::compact::compacted;
use super::{logical, within};
use crate::functions::{ARRAY, TEXT, TEXT_ARRAY};
use crate::{SQL_CONDITION_DEPTH, SQL_DEPTH, SQL_NESTED_CONDITIONS, Sql, SqlErrorKind, depth};

fn people() -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "email",
            Arc::new(StringArray::from(vec!["A@X", "b@y", "C@z"])) as ArrayRef,
        ),
        (
            "status",
            Arc::new(StringArray::from(vec![Some("live"), None, Some("test")])) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// Six people, of whom a slice at an offset is taken.
fn more_people() -> RecordBatch {
    let ids: Vec<i64> = (1..=6).collect();
    let emails = ["A@X", "b@Y", "ÇA@z", "d@W", "E@v", "ß@u"];
    let statuses = [
        Some("live"),
        None,
        Some("test"),
        Some("live"),
        None,
        Some("gone"),
    ];
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
        (
            "email",
            Arc::new(StringArray::from(emails.to_vec())) as ArrayRef,
        ),
        (
            "status",
            Arc::new(StringArray::from(statuses.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn a_query_is_planned_once_for_each_schema_and_projects_a_bare_column_without_a_copy() {
    let sql = Sql::parse("SELECT id, lower(email) AS email, id * 100 AS cents FROM input").unwrap();
    let batch = people();
    let output = sql.output(batch.schema_ref()).unwrap();
    let names: Vec<&str> = output.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["id", "email", "cents"]);
    let applied = sql.apply(batch.clone()).unwrap();
    assert!(Arc::ptr_eq(applied.column(0), batch.column(0)));
    let emails: Vec<&str> = applied
        .column(1)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert_eq!(emails, ["a@x", "b@y", "c@z"]);
    assert_eq!(
        applied
            .column(2)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec(),
        [100, 200, 300]
    );
    assert_eq!(applied.schema_ref().fields(), output.fields());
    let bound = sql.bound(batch.schema_ref()).unwrap();
    assert!(allocated_beyond(&applied, &batch) <= bound.of(&batch));
}

#[test]
fn a_batch_of_another_schema_is_planned_for_its_own() {
    let sql = Sql::parse("SELECT id, upper(status) AS status FROM input").unwrap();
    let batch = people();
    let reordered = batch.project(&[2, 0]).unwrap();
    for _ in 0..2 {
        let applied = sql.apply(batch.clone()).unwrap();
        assert_eq!(applied.column(0).as_ref(), batch.column(0).as_ref());
        let applied = sql.apply(reordered.clone()).unwrap();
        assert_eq!(applied.column(0).as_ref(), batch.column(0).as_ref());
        let statuses: Vec<Option<&str>> = applied.column(1).as_string::<i32>().iter().collect();
        assert_eq!(statuses, [Some("LIVE"), None, Some("TEST")]);
    }
    assert_eq!(
        sql.lineage(reordered.schema_ref()).unwrap(),
        [vec![1], vec![0]]
    );
}

#[test]
fn a_query_says_which_input_columns_each_output_column_reads() {
    let sql = Sql::parse(
        "SELECT id, lower(email) AS email, id * 100 AS cents, 'x' AS tag, \
         concat(email, status) AS both FROM input",
    )
    .unwrap();
    let lineage = sql.lineage(people().schema_ref()).unwrap();
    assert_eq!(lineage, vec![vec![0], vec![1], vec![0], vec![], vec![1, 2]]);
}

#[test]
fn a_substring_is_planned_as_the_listed_function() {
    let sql = Sql::parse(
        "SELECT substr(email, 2, 2) AS a, substring(email FROM 1 FOR 2) AS b FROM input",
    )
    .unwrap();
    let applied = sql.apply(people()).unwrap();
    let column = |index: usize| -> Vec<&str> {
        applied
            .column(index)
            .as_string::<i32>()
            .iter()
            .flatten()
            .collect()
    };
    assert_eq!(
        (column(0), column(1)),
        (vec!["@X", "@y", "@z"], vec!["A@", "b@", "C@"])
    );
}

#[test]
fn a_filter_says_it_reads_the_columns_of_its_predicate_alone() {
    let cases = [
        ("SELECT * FROM input WHERE id > 1", vec![0]),
        (
            "SELECT email FROM input WHERE status <> 'x' AND id > 1",
            vec![0, 2],
        ),
        ("SELECT email FROM input", vec![]),
    ];
    for (text, reads) in cases {
        let sql = Sql::parse(text).unwrap();
        assert_eq!(sql.keep_reads(people().schema_ref()), Some(reads), "{text}");
    }
}

#[test]
fn a_filter_answers_null_where_its_predicate_is_null() {
    let sql = Sql::parse("SELECT * FROM input WHERE status <> 'test'").unwrap();
    let keep = sql.keep(&people()).unwrap();
    assert_eq!(
        keep.iter().collect::<Vec<_>>(),
        [Some(true), None, Some(false)]
    );
}

#[test]
fn a_slice_at_an_offset_gives_the_rows_the_whole_batch_gives() {
    let sql = Sql::parse(
        "SELECT id, upper(email) AS email, coalesce(status, 'none') AS status, \
         CASE WHEN id > 2 THEN email ELSE status END AS pick FROM input \
         WHERE status <> 'test'",
    )
    .unwrap();
    let batch = more_people();
    let whole = sql.apply(batch.clone()).unwrap();
    let kept = sql.keep(&batch).unwrap();
    for (offset, rows) in [(1, 3), (2, 4), (5, 1), (0, 6)] {
        let part = batch.slice(offset, rows);
        assert_eq!(sql.apply(part.clone()).unwrap(), whole.slice(offset, rows));
        assert_eq!(sql.keep(&part).unwrap(), kept.slice(offset, rows));
    }
}

#[test]
fn a_query_naming_a_column_the_batch_lacks_is_refused_naming_it() {
    let sql = Sql::parse("SELECT id, amount AS cents FROM input").unwrap();
    let refused = sql.output(people().schema_ref()).unwrap_err();
    assert_eq!(refused.column(), Some("amount"));
    assert!(sql.bound(people().schema_ref()).is_none());
}

#[test]
fn text_made_from_an_encoded_column_handed_in_directly_is_refused_naming_its_column() {
    // A dictionary's or a run's text, decoded, can take far more than its bytes; the engine's
    // stage decodes such columns before a query sees them.
    let notes: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();
    let batch = RecordBatch::try_from_iter([("note", Arc::new(notes) as ArrayRef)]).unwrap();
    for made in [
        "CAST(note AS VARCHAR)",
        "lower(note)",
        "note || 'x'",
        "note = 'a'",
        "CASE WHEN note IS NULL THEN 'b' ELSE note END",
    ] {
        let sql = Sql::parse(&format!("SELECT note, {made} AS made FROM input")).unwrap();
        let refused = sql.output(batch.schema_ref()).unwrap_err();
        assert_eq!(refused.column(), Some("made"), "{made}");
    }
    let runs = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![2, 3]),
        &StringArray::from(vec!["a", "b"]),
    )
    .unwrap();
    let ran = RecordBatch::try_from_iter([("note", Arc::new(runs) as ArrayRef)]).unwrap();
    let sql = Sql::parse("SELECT note, upper(note) AS made FROM input").unwrap();
    assert_eq!(
        sql.output(ran.schema_ref()).unwrap_err().column(),
        Some("made")
    );
    let passed = Sql::parse("SELECT note, note IS NULL AS missing FROM input").unwrap();
    let applied = passed.apply(batch.clone()).unwrap();
    assert!(Arc::ptr_eq(applied.column(0), batch.column(0)));
}

#[test]
fn a_column_of_nulls_alone_is_read_as_any_type() {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("status", Arc::new(NullArray::new(2)) as ArrayRef),
    ])
    .unwrap();
    let sql =
        Sql::parse("SELECT id, lower(status) AS status FROM input WHERE status <> 'test'").unwrap();
    let applied = sql.apply(batch.clone()).unwrap();
    assert_eq!(applied.column(1).null_count(), 2);
    assert_eq!(sql.keep(&batch).unwrap().null_count(), 2);
    let bound = sql.bound(batch.schema_ref()).unwrap();
    assert!(allocated_beyond(&applied, &batch) <= bound.of(&batch));
}

#[test]
fn a_number_is_bounded_by_the_width_of_its_type() {
    let wide = Decimal256Array::from(vec![i256::from(-5), i256::from(7), i256::from(-9)])
        .with_precision_and_scale(60, 2)
        .unwrap();
    let batch = RecordBatch::try_from_iter([("d", Arc::new(wide) as ArrayRef)]).unwrap();
    let sql = Sql::parse("SELECT abs(d) AS a, -d AS n, d + d AS s FROM input").unwrap();
    let bound = sql.bound(batch.schema_ref()).unwrap();
    for rows in [1, 3] {
        let part = batch.slice(0, rows);
        let applied = sql.apply(part.clone()).unwrap();
        assert_eq!(applied.column(0).len(), rows);
        assert!(allocated_beyond(&applied, &part) <= bound.of(&part));
    }
}

#[test]
fn a_value_cast_to_text_stays_within_its_bound_however_long_its_text() {
    // A negative scale prints a zero for each of its places beside the digits.
    let nines = i256::from_string(&"9".repeat(76)).unwrap();
    let values = (0..2_000).map(|row| {
        if row % 2 == 0 {
            nines
        } else {
            nines.wrapping_neg()
        }
    });
    let decimals = Decimal256Array::from_iter_values(values)
        .with_precision_and_scale(76, -76)
        .unwrap();
    let days = (0..2_000).map(|row| {
        if row % 2 == 0 {
            7_000_000_000_000_000
        } else {
            -1
        }
    });
    let batch = RecordBatch::try_from_iter([
        ("d", Arc::new(decimals) as ArrayRef),
        (
            "day",
            Arc::new(Date64Array::from_iter_values(days)) as ArrayRef,
        ),
    ])
    .unwrap();
    let sql = Sql::parse("SELECT CAST(d AS VARCHAR) AS d, CAST(day AS VARCHAR) AS day FROM input")
        .unwrap();
    let bound = sql.bound(batch.schema_ref()).unwrap();
    for rows in [1, 2_000] {
        let part = batch.slice(0, rows);
        let applied = sql.apply(part.clone()).unwrap();
        let allocated = allocated_beyond(&applied, &part);
        assert!(
            allocated <= bound.of(&part),
            "{allocated} > {}",
            bound.of(&part)
        );
        let longest = applied
            .column(0)
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::len)
            .max();
        assert!(longest > Some(150), "{longest:?}");
    }
}

#[test]
fn a_failure_names_its_column_and_function_and_never_a_value() {
    let sql = Sql::parse("SELECT CAST(email AS BIGINT) AS n FROM input").unwrap();
    let failed = sql.apply(people()).unwrap_err();
    assert_eq!(failed.column(), Some("n"));
    for value in ["A@X", "b@y", "C@z"] {
        assert!(!failed.to_string().contains(value), "{failed}");
    }
    assert_eq!(
        failed.to_string(),
        "column n: evaluating the expression failed"
    );
    let wrapped =
        RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef)])
            .unwrap();
    let sql = Sql::parse("SELECT abs(n) AS a FROM input").unwrap();
    let failed = sql.apply(wrapped).unwrap_err();
    assert_eq!(failed.to_string(), "column a: evaluating abs failed");
}

#[test]
fn a_bound_is_what_the_types_it_outputs_take() {
    let bound = |column: DataType, query: &str| {
        let schema = Schema::new(vec![Field::new("c", column, true)]);
        Sql::parse(query).unwrap().bound(&schema)
    };
    // An output and its builder's slack, twice what the output takes.
    let twice = |bound: Bound| bound.times(2);
    // A negation takes a row of its type: the value and its validity.
    let negated = bound(DataType::Int64, "SELECT -c AS x FROM input");
    assert_eq!(negated, Some(twice(Bound::new(ARRAY).with_rows(9))));
    // A cast to text takes the most text a value of its type has, beside a row of text.
    let text = |longest: u64| Bound::new(TEXT_ARRAY).with_rows(TEXT + longest);
    let cast = "SELECT CAST(c AS VARCHAR) AS x FROM input";
    for (column, longest) in [
        (DataType::Boolean, 5),
        (DataType::Int32, 20),
        (DataType::Float32, 32),
        (DataType::Date64, 32),
        (DataType::Null, 0),
        (DataType::Decimal128(10, -3), 16),
        (DataType::Decimal128(10, 4), 17),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            48,
        ),
        (DataType::Time32(arrow_schema::TimeUnit::Second), 48),
        (DataType::Time64(arrow_schema::TimeUnit::Nanosecond), 48),
    ] {
        let shown = format!("{column}");
        assert_eq!(bound(column, cast), Some(twice(text(longest))), "{shown}");
    }
}

#[test]
fn text_cut_from_a_larger_buffer_is_copied_and_text_held_whole_is_not() {
    let long = ["a value longer than a view holds inline"; 4];
    let arrays: [ArrayRef; 6] = [
        Arc::new(StringViewArray::from_iter_values(long)),
        Arc::new(BinaryViewArray::from_iter_values(long.map(str::as_bytes))),
        Arc::new(StringArray::from_iter_values(long)),
        Arc::new(LargeStringArray::from_iter_values(long)),
        Arc::new(BinaryArray::from_iter_values(long.map(str::as_bytes))),
        Arc::new(LargeBinaryArray::from_iter_values(long.map(str::as_bytes))),
    ];
    for whole in arrays {
        assert!(
            compacted(&whole).unwrap().is_none(),
            "{}",
            whole.data_type()
        );
        let cut = whole.slice(1, 1);
        let copied = compacted(&cut).unwrap().unwrap();
        assert_eq!(copied.len(), 1);
        assert!(copied.get_array_memory_size() < cut.get_array_memory_size());
    }
}

#[test]
fn a_query_is_a_step_of_a_stream_plan() {
    let sql = Sql::parse("SELECT * FROM input WHERE id > 1").unwrap();
    let step = rdlt_engine::TransformStep::from(sql);
    assert!(step.as_transform().filters());
    assert_eq!(step.as_transform().name(), "sql");
}

#[test]
fn a_query_says_which_input_column_each_output_column_passes_unchanged() {
    let sql = Sql::parse(
        "SELECT id, upper(email) AS email, id AS again, status AS state, 'x' AS tag FROM input",
    )
    .unwrap();
    let passes = sql.passes(people().schema_ref()).unwrap();
    assert_eq!(passes, [Some(0), None, Some(0), Some(2), None]);
}

#[test]
fn an_output_column_under_the_engines_prefix_is_refused_naming_it() {
    let batch =
        RecordBatch::try_from_iter([("_rdlt_op", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let sql = Sql::parse("SELECT * FROM input").unwrap();
    let refused = sql.output(batch.schema_ref()).unwrap_err();
    assert_eq!(refused.column(), Some("_rdlt_op"));
}

/// How the expressions of `text`, a statement, nest, as the limits measure them.
fn nesting_of(text: &str) -> depth::Nesting {
    let parsed = Parser::new(&GenericDialect {})
        .with_recursion_limit(1 << 12)
        .try_with_sql(text)
        .and_then(|mut parser| parser.parse_statements())
        .unwrap();
    let [statement] = parsed.as_slice() else {
        panic!("{text} is one statement");
    };
    depth::nesting(statement)
}

/// How deep the expressions of `text` nest, as the limit measures them.
fn depth_of(text: &str) -> usize {
    nesting_of(text).depth
}

/// A query of `form`, as deep as the limit admits.
fn deepest(form: fn(usize) -> String) -> usize {
    let admitted = (1..4096).take_while(|times| depth_of(&form(*times)) <= SQL_DEPTH);
    admitted.last().expect("one level fits the limit")
}

/// Batches of a number `n`, a text `t` and a truth `b`.
fn mixed() -> RecordBatch {
    RecordBatch::try_from_iter([
        ("n", Arc::new(Int64Array::from(vec![1, -2, 3])) as ArrayRef),
        (
            "t",
            Arc::new(StringArray::from(vec!["aΐ", "b", "c"])) as ArrayRef,
        ),
        (
            "b",
            Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn a_query_as_deep_as_the_limit_admits_runs_on_a_small_stack_and_one_deeper_is_refused() {
    let forms: [fn(usize) -> String; 6] = [
        |times| {
            format!(
                "SELECT {}n{} AS x FROM input",
                "(".repeat(times),
                ")".repeat(times)
            )
        },
        |times| format!("SELECT {}n AS x FROM input", "n + ".repeat(times)),
        |times| {
            let (open, close) = ("CASE WHEN n > 0 THEN ", " ELSE n END");
            format!(
                "SELECT {}n{} AS x FROM input",
                open.repeat(times),
                close.repeat(times)
            )
        },
        |times| {
            format!(
                "SELECT {}t{} AS x FROM input",
                "lower(".repeat(times),
                ")".repeat(times)
            )
        },
        |times| {
            format!(
                "SELECT n FROM input WHERE {}b",
                "b IS DISTINCT FROM ".repeat(times)
            )
        },
        |times| format!("SELECT {}b AS x FROM input", "NOT ".repeat(times)),
    ];
    for form in forms {
        let times = deepest(form);
        let (at, over) = (form(times), form(times + 1));
        assert_eq!(depth_of(&at), SQL_DEPTH);
        let ran = std::thread::Builder::new()
            .name("a small stack".to_owned())
            .stack_size(2 << 20)
            .spawn(move || {
                let sql = Sql::parse(&at).map_err(|error| error.to_string())?;
                let batch = mixed();
                sql.output(batch.schema_ref())
                    .map_err(|error| error.to_string())?;
                sql.keep(&batch).map_err(|error| error.to_string())?;
                sql.apply(batch).map_err(|error| error.to_string())?;
                Ok::<_, String>(())
            })
            .unwrap()
            .join();
        let shown: String = form(times).chars().take(60).collect();
        assert_eq!(ran.unwrap(), Ok(()), "{shown} at {times}");
        let refused = Sql::parse(&over).unwrap_err();
        assert_eq!(
            refused.kind(),
            SqlErrorKind::Syntax,
            "{shown} at {}",
            times + 1
        );
    }
}

#[test]
fn a_case_within_as_many_conditions_as_the_limit_admits_runs_and_one_within_more_is_refused() {
    // The planner reads the nullability of a `CASE` in a condition again for each operator above
    // it there, so a nest of them far within the depth limit plans in time exponential in it.
    let forms: [fn(usize) -> String; 2] = [
        |levels| {
            format!(
                "SELECT {}b{} AS x FROM input",
                "CASE WHEN ".repeat(levels),
                " THEN NULL ELSE NULL END".repeat(levels)
            )
        },
        |levels| {
            format!(
                "SELECT {}n{} AS x FROM input",
                "coalesce(".repeat(levels),
                ", 1)".repeat(levels)
            )
        },
    ];
    for form in forms {
        let at = form(SQL_NESTED_CONDITIONS + 1);
        let shown: String = at.chars().take(60).collect();
        let ran = std::thread::Builder::new()
            .name("a small stack".to_owned())
            .stack_size(2 << 20)
            .spawn(move || {
                let sql = Sql::parse(&at).map_err(|error| error.to_string())?;
                let batch = mixed();
                sql.keep(&batch).map_err(|error| error.to_string())?;
                let applied = sql.apply(batch).map_err(|error| error.to_string())?;
                Ok::<_, String>(applied.num_rows())
            })
            .unwrap()
            .join();
        assert_eq!(ran.unwrap(), Ok(3), "{shown}");
        for levels in [SQL_NESTED_CONDITIONS + 2, 18] {
            let refused = Sql::parse(&form(levels)).unwrap_err();
            assert_eq!(
                (refused.kind(), refused.code()),
                (SqlErrorKind::Syntax, codes::TRANSFORM_INVALID),
                "{shown} at {levels}"
            );
        }
    }
}

#[test]
fn a_condition_as_deep_as_the_limit_admits_runs_and_one_deeper_is_refused() {
    let realistic = "SELECT CASE WHEN lower(btrim(t)) = 'x' AND coalesce(n, 0) > 3 THEN 1 ELSE 0 END \
                     AS x FROM input";
    let forms: [fn(usize) -> String; 2] = [
        |deep| {
            let (open, close) = ("abs(".repeat(deep - 1), ")".repeat(deep - 1));
            format!("SELECT CASE WHEN {open}n{close} = 1 THEN n ELSE 1 END AS x FROM input")
        },
        |deep| {
            let (open, close) = ("abs(".repeat(deep), ")".repeat(deep));
            format!("SELECT coalesce({open}n{close}, 1) AS x FROM input")
        },
    ];
    let mut admitted = vec![realistic.to_owned()];
    for form in forms {
        let (at, over) = (form(SQL_CONDITION_DEPTH), form(SQL_CONDITION_DEPTH + 1));
        assert_eq!(nesting_of(&at).condition_depth, SQL_CONDITION_DEPTH, "{at}");
        let refused = Sql::parse(&over).unwrap_err();
        assert_eq!(
            (refused.kind(), refused.code()),
            (SqlErrorKind::Syntax, codes::TRANSFORM_INVALID),
            "{over}"
        );
        admitted.push(at);
    }
    for text in admitted {
        let sql = Sql::parse(&text).unwrap();
        let batch = mixed();
        sql.keep(&batch).unwrap();
        assert_eq!(sql.apply(batch).unwrap().num_rows(), 3, "{text}");
    }
}

#[test]
fn a_long_but_flat_select_list_plans_and_runs_on_a_small_stack() {
    let columns: Vec<String> = (0..2000).map(|at| format!("n + 1 AS c{at}")).collect();
    let text = format!("SELECT n, {} FROM input", columns.join(", "));
    let ran = std::thread::Builder::new()
        .name("a small stack".to_owned())
        .stack_size(2 << 20)
        .spawn(move || {
            let sql = Sql::parse(&text).map_err(|error| error.to_string())?;
            let applied = sql.apply(mixed()).map_err(|error| error.to_string())?;
            Ok::<_, String>(applied.num_columns())
        })
        .unwrap()
        .join();
    assert_eq!(ran.unwrap(), Ok(2001));
}

#[test]
fn a_concatenation_of_empty_literals_runs_as_written() {
    // `concat`'s own rewrite folds empty literals away, leaving a call it refuses to run.
    for text in [
        "SELECT id, concat('', '') AS y FROM input",
        "SELECT id, concat('') AS y FROM input",
        "SELECT id, concat('', '') AS y FROM input WHERE concat('') = ''",
    ] {
        let sql = Sql::parse(text).unwrap();
        let out = sql
            .apply(people())
            .unwrap_or_else(|error| panic!("{text}: {error}"));
        let y = out.column_by_name("y").unwrap().as_string::<i32>();
        assert_eq!(y.iter().collect::<Vec<_>>(), [Some(""); 3], "{text}");
    }
}

/// A cast to each listed type, under each name it goes by, of a value it holds.
const CASTS: &[&str] = &[
    "CAST(n AS BOOLEAN)",
    "CAST(n AS BOOL)",
    "CAST(n AS TINYINT)",
    "CAST(n AS SMALLINT)",
    "CAST(n AS INT2)",
    "CAST(n AS INT)",
    "CAST(n AS INTEGER)",
    "CAST(n AS INT4)",
    "CAST(n AS BIGINT)",
    "CAST(n AS INT8)",
    "CAST(abs(n) AS TINYINT UNSIGNED)",
    "CAST(abs(n) AS SMALLINT UNSIGNED)",
    "CAST(abs(n) AS INT UNSIGNED)",
    "CAST(abs(n) AS INTEGER UNSIGNED)",
    "CAST(abs(n) AS BIGINT UNSIGNED)",
    "CAST(n AS REAL)",
    "CAST(n AS FLOAT)",
    "CAST(n AS FLOAT4)",
    "CAST(n AS DOUBLE)",
    "CAST(n AS DOUBLE PRECISION)",
    "CAST(n AS FLOAT8)",
    "CAST(n AS DECIMAL)",
    "CAST(n AS DECIMAL(10, 2))",
    "CAST(n AS NUMERIC(38, 4))",
    "CAST(n AS CHAR)",
    "CAST(n AS VARCHAR)",
    "CAST(n AS TEXT)",
    "CAST(n AS STRING)",
    "CAST('2020-01-02' AS DATE)",
    "CAST('12:34:56' AS TIME)",
    "CAST('12:34:56' AS TIME WITHOUT TIME ZONE)",
    "CAST('2020-01-02 03:04:05' AS TIMESTAMP)",
    "CAST('2020-01-02 03:04:05' AS TIMESTAMP(0))",
    "CAST('2020-01-02 03:04:05' AS TIMESTAMP(3))",
    "CAST('2020-01-02 03:04:05' AS TIMESTAMP(6))",
    "CAST('2020-01-02 03:04:05' AS TIMESTAMP(9))",
    "CAST('2020-01-02 03:04:05' AS TIMESTAMP WITH TIME ZONE)",
    "CAST('2020-01-02 03:04:05' AS TIMESTAMP(3) WITHOUT TIME ZONE)",
    // Spelled otherwise, and planned as the listed types they are.
    "CAST(n AS CHARACTER)",
    "CAST(n AS CHARACTER(3))",
    "CAST(n AS CHARACTER VARYING(10))",
    "CAST(n AS CHAR VARYING(10))",
    "CAST(n AS NVARCHAR(10))",
    "CAST(n AS CHARACTER LARGE OBJECT)",
    "CAST(n AS CHAR LARGE OBJECT)",
    "CAST(n AS CLOB)",
    "CAST(n AS TINYTEXT)",
    "CAST(n AS MEDIUMTEXT)",
    "CAST(n AS LONGTEXT)",
    "CAST(n AS DEC)",
    "CAST(n AS DEC(10, 2))",
    "CAST('2020-01-02 03:04:05' AS DATETIME)",
    "CAST('2020-01-02 03:04:05' AS DATETIME(6))",
    "CAST('2020-01-02 03:04:05' AS TIMESTAMP_NTZ)",
    "CAST('2020-01-02 03:04:05' AS DATETIME64(3))",
    "CAST('2020-01-02' AS DATE32)",
    "CAST(n AS INT16)",
    "CAST(n AS INT32)",
    "CAST(n AS INT64)",
    "CAST(abs(n) AS UINT8)",
    "CAST(abs(n) AS UINT16)",
    "CAST(abs(n) AS UINT32)",
    "CAST(abs(n) AS UINT64)",
    "CAST(abs(n) AS UTINYINT)",
    "CAST(abs(n) AS USMALLINT)",
    "CAST(abs(n) AS UBIGINT)",
    "CAST(n AS FLOAT32)",
    "CAST(n AS FLOAT64)",
    "CAST(n AS SIGNED)",
    "CAST(n AS SIGNED INTEGER)",
    "CAST(abs(n) AS UNSIGNED)",
    "CAST(abs(n) AS UNSIGNED INTEGER)",
];

#[test]
fn a_cast_to_each_listed_type_parses_plans_runs_and_drops_on_a_small_stack() {
    for cast in CASTS {
        let text = format!("SELECT {cast} AS x FROM input WHERE CAST(n AS BIGINT) IS NOT NULL");
        let ran = std::thread::Builder::new()
            .name("a small stack".to_owned())
            .stack_size(2 << 20)
            .spawn(move || {
                let sql = Sql::parse(&text).map_err(|error| error.to_string())?;
                let batch = mixed();
                sql.output(batch.schema_ref())
                    .map_err(|error| error.to_string())?;
                sql.keep(&batch).map_err(|error| error.to_string())?;
                let applied = sql.apply(batch).map_err(|error| error.to_string())?;
                drop(sql);
                Ok::<_, String>(applied.num_rows())
            })
            .unwrap()
            .join();
        assert_eq!(ran.unwrap(), Ok(3), "{cast}");
    }
}

#[test]
fn text_made_anew_is_bounded_by_a_view_builders_first_block_its_rows_and_its_bytes() {
    // Twice, for builders that double: the first block a builder of views takes, 16 KiB, and an
    // array's rounding, 256 bytes, beside 17 bytes a row and three bytes of output a byte of
    // `t`; and once, the copy of `t` held in a larger buffer, an array's rounding and its bytes.
    let sql = Sql::parse("SELECT upper(t) AS x FROM input").unwrap();
    let schema = Schema::new(vec![Field::new("t", DataType::Utf8, true)]);
    let expected = Bound::new(2 * ((16 << 10) + 256) + 256)
        .with_rows(2 * 17)
        .with_column(0, 2 * 3 + 1);
    assert_eq!(sql.bound(&schema), Some(expected));
}

#[test]
fn a_query_outputs_as_many_columns_as_the_engine_names_and_no_more() {
    assert!(within(NAMED_COLUMNS).is_ok());
    let refused = within(NAMED_COLUMNS + 1).unwrap_err();
    assert!(refused.to_string().contains("columns"), "{refused}");
}

#[test]
fn an_identifier_is_matched_as_written() {
    let schema = Schema::new(vec![Field::new("Name", DataType::Utf8, true)]);
    let sql = Sql::parse("SELECT Name, lower(Name) AS quiet FROM input").unwrap();
    let output = sql.output(&schema).unwrap();
    assert_eq!(output.field(0).name(), "Name");
    let lowered = Sql::parse("SELECT name FROM input").unwrap();
    assert!(lowered.output(&schema).is_err());
}

#[test]
fn a_plan_reading_other_than_the_whole_of_input_is_refused() {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let source = Arc::new(LogicalTableSource::new(Arc::clone(&schema)));
    let scanned = |table: &str, projection: Option<Vec<usize>>| {
        let scan = LogicalPlanBuilder::scan(table, Arc::clone(&source) as _, projection).unwrap();
        scan.project(vec![col("n")]).unwrap().build().unwrap()
    };
    assert!(logical(&scanned("input", None)).is_ok());
    assert!(logical(&scanned("input", Some(vec![0]))).is_err());
    assert!(logical(&scanned("other", None)).is_err());
}

#[cfg(feature = "bench")]
#[test]
fn an_accepted_plan_shows_only_a_projection_of_input_and_listed_functions() {
    let text = "SELECT id, lower(email) AS email FROM input AS i WHERE status <> 'test'";
    let sql = Sql::parse(text).unwrap();
    let facts = crate::bench::facts(&sql, people().schema_ref()).unwrap();
    assert_eq!(
        facts.nodes,
        ["Projection", "Filter", "SubqueryAlias", "TableScan"]
    );
    assert_eq!(facts.functions, ["lower"]);
}
