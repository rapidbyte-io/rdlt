//! rdlt-sql's per-batch evaluation against DataFusion's own executor over a memory table: the
//! values, the types and the nullability of what each gives, for the listed expressions and for
//! drawn queries, on each batch whole and cut as the stage cuts it.

use arrow_array::{Array as _, RecordBatch};
use arrow_schema::{DataType, Fields};
use arrow_select::filter::{filter_record_batch, prep_null_mask_filter};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{SessionConfig, SessionContext};
use proptest::prelude::*;
use rdlt_engine::Transform;
use rdlt_sql::Sql;

use crate::composed;
use crate::queries::{EXPRESSIONS, PREDICATES, batches, query};

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

/// `ours` with each column's nulls allowed only where DataFusion's executor allows them too.
///
/// Its optimizer folds constants, so it may know a column holds no null where the plan as
/// written, which rdlt-sql runs, does not: a column rdlt-sql says holds none, DataFusion's must
/// not allow either, and every other difference stays.
fn narrowed(ours: (String, Typed), theirs: &(String, Typed)) -> (String, Typed) {
    let (shown, types) = ours;
    let allowed = theirs.1.iter().map(|(_, nullable)| *nullable);
    let types = types.into_iter().zip(allowed);
    let types = types.map(|((data_type, nullable), allowed)| (data_type, nullable && allowed));
    (shown, types.collect())
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
/// filter drops taken out only where it drops one, and its columns' types, or why it refused it.
fn evaluated(parts: &[RecordBatch], text: &str) -> Result<(String, Typed), String> {
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
    Ok((shown(&outputs), typed(output.fields())))
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
                    let ours = narrowed(ours, &expected);
                    assert_eq!(ours, expected, "seed {seed}: {text} in {cut} parts");
                }
            }
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime starts");
        let context = session(&batch);
        let expected = runtime.block_on(executed(&context, &text));
        for parts in cuts(&batch) {
            let cut = parts.len();
            match (evaluated(&parts, &text), &expected) {
                (Ok(ours), Ok(expected)) => {
                    let ours = narrowed(ours, expected);
                    prop_assert_eq!(&ours, expected, "{} in {} parts", text, cut);
                }
                (ours, expected) => prop_assert!(
                    false,
                    "{text} in {cut} parts: ours {ours:?}, DataFusion's {expected:?}"
                ),
            }
        }
    }
}
