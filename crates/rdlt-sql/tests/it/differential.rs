//! rdlt-sql's per-batch evaluation against DataFusion's own executor over a memory table: the
//! values and the types of what each gives, and that a column rdlt-sql says holds no null holds
//! none, for the listed expressions and for drawn queries, on each batch whole and cut as the
//! stage cuts it.

use arrow_array::{Array as _, RecordBatch};
use arrow_schema::{DataType, Fields};
use arrow_select::filter::{filter_record_batch, prep_null_mask_filter};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{SessionConfig, SessionContext};
use proptest::prelude::*;
use rdlt_engine::Transform;
use rdlt_sql::{Sql, SqlErrorKind};

use crate::composed;
use crate::queries::{EXPRESSIONS, PREDICATES, batches, query};

/// What DataFusion says of a `concat` its constant folding emptied: the plan as written runs, and
/// a SQL transform, which folds nothing, runs it.
const EMPTIED_CONCAT: &str = "concat does not support zero arguments";

/// `batches`' rows as a table prints them, or how many there are where there are none.
fn shown(batches: &[RecordBatch]) -> String {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if rows == 0 {
        return "no rows".to_owned();
    }
    pretty_format_batches(batches)
        .expect("the batches print")
        .to_string()
}

/// A session over `batch` as the table `input`, configured as SQL transforms are planned.
///
/// An optimizer rule of its own that fails is skipped rather than failing the query: SQL
/// transforms run none of them, and `simplify_expressions`, folding `concat` of empty literals
/// into a call of none, refuses queries the plan as written runs.
fn session(batch: &RecordBatch) -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .set_bool("datafusion.optimizer.skip_failed_rules", true)
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false)
        .set_bool("datafusion.sql_parser.map_string_types_to_utf8view", false)
        .set_str("datafusion.execution.time_zone", "UTC");
    let context = SessionContext::new_with_config(config);
    context
        .register_batch("input", batch.clone())
        .expect("the batch registers");
    context
}

/// Each column's type, and whether it may hold nulls.
type Typed = Vec<(DataType, bool)>;

fn typed(fields: &Fields) -> Typed {
    let fields = fields.iter();
    fields
        .map(|field| (field.data_type().clone(), field.is_nullable()))
        .collect()
}

/// What `text` gives over `batch` and its columns' types, from DataFusion's executor, or why it
/// refused it.
async fn executed(context: &SessionContext, text: &str) -> Result<(String, Typed), String> {
    let frame = context.sql(text).await.map_err(|error| error.to_string())?;
    let physical = frame.create_physical_plan().await;
    let physical = physical.map_err(|error| error.to_string())?;
    let types = typed(physical.schema().fields());
    let task = context.task_ctx();
    let batches = datafusion::physical_plan::collect(physical, task).await;
    let batches = batches.map_err(|error| error.to_string())?;
    Ok((shown(&batches), types))
}

/// What rdlt-sql gives: its rows as [`shown`] prints them, each column's type and whether its
/// schema allows nulls, and whether each column holds one.
#[derive(Debug)]
struct Evaluated {
    shown: String,
    types: Typed,
    nulls: Vec<bool>,
}

/// Why `ours` differs from what DataFusion's executor gives: other rows, other column types, or
/// a column rdlt-sql says holds no null that holds one.
///
/// Either planner may prove a column holds no null where the other does not, so neither's
/// nullability binds the other's: rdlt-sql's need only be true of what it gives.
fn differs(ours: &Evaluated, theirs: &(String, Typed)) -> Option<String> {
    let (shown, types) = theirs;
    if ours.shown != *shown {
        return Some(format!("rows: ours\n{}\nDataFusion's\n{shown}", ours.shown));
    }
    let ours_types = ours.types.iter().map(|(data_type, _)| data_type);
    if !ours_types.eq(types.iter().map(|(data_type, _)| data_type)) {
        return Some(format!(
            "types: ours {:?}, DataFusion's {types:?}",
            ours.types
        ));
    }
    let said = ours.types.iter().map(|(_, nullable)| *nullable);
    let unsound = said
        .zip(&ours.nulls)
        .position(|(nullable, held)| !nullable && *held);
    unsound.map(|column| format!("column {column} is said to hold no null and holds one"))
}

/// `batch` whole, in halves, in quarters, and as its first row and the rest: parts as the stage
/// cuts a batch, slices at offsets among them.
fn cuts(batch: &RecordBatch) -> Vec<Vec<RecordBatch>> {
    let rows = batch.num_rows();
    let parted = |parts: usize| -> Vec<RecordBatch> {
        let starts: Vec<usize> = (0..=parts).map(|part| part * rows / parts).collect();
        let pairs = starts.windows(2);
        pairs
            .map(|pair| batch.slice(pair[0], pair[1] - pair[0]))
            .collect()
    };
    let mut cuts = vec![parted(1), parted(2), parted(4)];
    if rows > 1 {
        cuts.push(vec![batch.slice(0, 1), batch.slice(1, rows - 1)]);
    }
    cuts
}

/// What `text` gives over `parts`, evaluated a part at a time as the stage does, the rows its
/// filter drops taken out only where it drops one, or why it refused it.
fn evaluated(parts: &[RecordBatch], text: &str) -> Result<Evaluated, String> {
    let sql = Sql::parse(text).map_err(|error| error.to_string())?;
    let output = sql
        .output(parts[0].schema_ref())
        .map_err(|error| error.to_string())?;
    let mut outputs = Vec::with_capacity(parts.len());
    for part in parts {
        let keep = sql.keep(part).map_err(|error| error.to_string())?;
        let kept = if keep.true_count() == keep.len() {
            part.clone()
        } else {
            let keep = match keep.null_count() {
                0 => keep,
                _ => prep_null_mask_filter(&keep),
            };
            filter_record_batch(part, &keep).expect("the answer is a row's each")
        };
        outputs.push(sql.apply(kept).map_err(|error| error.to_string())?);
    }
    let columns = 0..output.fields().len();
    let nulls = columns.map(|column| {
        let mut columns = outputs.iter().map(|output| output.column(column));
        columns.any(|column| column.null_count() > 0)
    });
    Ok(Evaluated {
        shown: shown(&outputs),
        types: typed(output.fields()),
        nulls: nulls.collect(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_batch_evaluation_gives_what_datafusions_executor_gives() {
    for seed in 0..u64::from(rdlt_testkit::cases(12)) {
        for batch in rdlt_testkit::draw::draw(&batches(), seed) {
            let context = session(&batch);
            for (index, expression) in EXPRESSIONS.iter().enumerate() {
                let predicate = PREDICATES.get(index % (PREDICATES.len() + 1)).copied();
                let text = query(expression, predicate);
                let expected = executed(&context, &text).await;
                let expected = expected.unwrap_or_else(|error| panic!("{text}: {error}"));
                for parts in cuts(&batch) {
                    let ours = evaluated(&parts, &text);
                    let ours = ours.unwrap_or_else(|error| panic!("{text}: {error}"));
                    let cut = parts.len();
                    if let Some(difference) = differs(&ours, &expected) {
                        panic!("seed {seed}: {text} in {cut} parts: {difference}");
                    }
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_concat_datafusion_folds_to_no_arguments_gives_what_the_query_folded_by_hand_gives() {
    let text = "SELECT n, replace(concat('', CASE WHEN TRUE THEN '' ELSE t END), 'a', 'xy') AS y, \
                strpos(coalesce(t, right(t, 1)), 'a') AS z FROM input";
    let folded = "SELECT n, '' AS y, strpos(coalesce(t, right(t, 1)), 'a') AS z FROM input";
    for seed in 0..u64::from(rdlt_testkit::cases(12)) {
        let batch = rdlt_testkit::draw::draw(&composed::batch(), seed);
        let context = session(&batch);
        let refused = executed(&context, text).await;
        let refused = refused.expect_err("DataFusion folds the call empty");
        assert!(refused.contains(EMPTIED_CONCAT), "{refused}");
        let (expected, _) = executed(&context, folded)
            .await
            .expect("the query as folded runs");
        for parts in cuts(&batch) {
            let ours = evaluated(&parts, text).expect("the query as written runs");
            assert_eq!(ours.shown, expected, "seed {seed}, {} parts", parts.len());
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(96)))]

    #[test]
    fn a_drawn_query_gives_what_datafusions_executor_gives(
        text in composed::query(),
        batch in composed::batch(),
    ) {
        // A drawn query is one query far within the depth limit, so its one refusal of this kind
        // is of a `CASE` lying within more conditions than the limit, which DataFusion plans.
        let refused = Sql::parse(&text).err().map(|refused| refused.kind());
        prop_assume!(refused != Some(SqlErrorKind::Syntax));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime starts");
        let context = session(&batch);
        let expected = runtime.block_on(executed(&context, &text));
        prop_assume!(!matches!(&expected, Err(error) if error.contains(EMPTIED_CONCAT)));
        for parts in cuts(&batch) {
            let cut = parts.len();
            match (evaluated(&parts, &text), &expected) {
                (Ok(ours), Ok(expected)) => {
                    let difference = differs(&ours, expected);
                    prop_assert!(difference.is_none(), "{} in {} parts: {:?}", text, cut, difference);
                }
                (ours, expected) => prop_assert!(
                    false,
                    "{text} in {cut} parts: ours {ours:?}, DataFusion's {expected:?}"
                ),
            }
        }
    }
}
