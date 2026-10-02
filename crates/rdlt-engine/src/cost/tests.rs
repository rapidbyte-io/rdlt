//! The invariant tying the cost model to the engine: no consumer of a batch materializes more
//! than the model says the batch expands to.

mod lowering;

use std::sync::Arc;

use arrow_array::builder::BinaryViewBuilder;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Int16Array, Int32Array, Int64Array,
    LargeListViewArray, ListArray, ListViewArray, NullArray, RecordBatch, RecordBatchOptions,
    RunArray, StructArray, new_empty_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field as ArrowField, Fields};
use proptest::prelude::*;
use rdlt_connector::cost::{Allocations, Rendering};
use rdlt_connector::{Admission, Field, LogicalType, Push, SourceEvent, TypeKind};
use rdlt_testkit::drawn::{Drawn, KINDS, Scalar, array, field, values};

use super::{Admitted, Charging};
use crate::budget::MemoryBudget;
use crate::env::Env;
use crate::normalize::as_list;
use crate::table::convert::{convert, decoded, json, normalize, text};

/// Counts what this crate's tests allocate, for those that hold a peak to a charge: each runs in
/// a process of its own.
#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;

/// What `column` expands to, as the engine charges it for a destination storing `native` kinds.
fn expanded(column: &ArrayRef, native: &[TypeKind]) -> u64 {
    Rendering::new(native.iter().copied()).expanded_array(
        column.as_ref(),
        0..column.len(),
        u64::MAX,
    )
}

/// The bytes a consumer added making `produced` of `column`: what `produced`'s rows take, or
/// the allocations it does not share with `column` where those are fewer, as a result that keeps
/// its input's buffers added none of them.
fn materialized(column: &ArrayRef, produced: &ArrayRef) -> u64 {
    let rows = produced.to_data().get_slice_memory_size().unwrap();
    let added = Allocations::of_array(column.as_ref()).add_array(produced.as_ref());
    u64::try_from(rows).unwrap().min(added)
}

/// The nodes of `data_type`: an empty array still holds one offset a node.
fn nodes(data_type: &DataType) -> u64 {
    let below: u64 = match data_type {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => nodes(item.data_type()),
        DataType::Struct(fields) => fields.iter().map(|field| nodes(field.data_type())).sum(),
        DataType::Dictionary(_, values) => nodes(values),
        DataType::RunEndEncoded(_, values) => nodes(values.data_type()),
        _ => 0,
    };
    below + 1
}

/// Asserts that `produced`, which a consumer made of `column`, takes no more than `cost`.
fn within(what: &str, column: &ArrayRef, produced: &ArrayRef, cost: u64) {
    let slack = 8 * nodes(produced.data_type());
    let bytes = materialized(column, produced);
    assert!(
        bytes <= cost.saturating_add(slack),
        "{what} of {} made {bytes} bytes of a column costed {cost}",
        column.data_type()
    );
}

/// Runs `column`, a column of `field`, through every consumer the engine has, and asserts none
/// materializes more than the column expands to for a destination storing `native` kinds.
fn consumed(field: &ArrowField, column: &ArrayRef, native: &[TypeKind]) {
    let logical = Field::from_arrow(field).expect("a logical type");
    let logical = logical.logical_type();
    let cost = expanded(column, native);
    // Arrow decodes no run-end encoding of nested values: such a column fails where it is lowered.
    let Ok(decoded) = decoded(column) else {
        return;
    };
    within("decoding", column, &decoded, cost);
    // A value its plain type cannot hold fails its conversion, which then makes nothing.
    if let Ok(plain) = normalize(&decoded, logical) {
        within("normalizing", column, &plain, cost);
    }
    let nested = matches!(
        logical,
        LogicalType::Struct(_) | LogicalType::List(_) | LogicalType::Json
    );
    if let Ok(converted) = convert(column, logical, logical) {
        within("converting", column, &converted, cost);
        if nested || !native.contains(&logical.kind()) {
            let text = text(&converted, logical).expect("every value has a text");
            within("rendering as text", column, &text, cost);
        }
    }
    if nested {
        let json = json(&decoded, logical).expect("every value has JSON text");
        within("rendering as JSON", column, &json, cost);
    }
    if matches!(logical, LogicalType::List(_)) {
        let list: ArrayRef = Arc::new(as_list(&decoded).expect("a list type"));
        within("listing", column, &list, cost);
    }
}

fn batch((columns, rows): &Drawn) -> RecordBatch {
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .enumerate()
        .map(|(column, (_, shape))| {
            let values: Vec<&Scalar> = rows.iter().map(|row| &row[column]).collect();
            array(shape, &values)
        })
        .collect();
    let fields: Vec<_> = columns
        .iter()
        .zip(&arrays)
        .map(|((name, shape), array)| field(name, shape, array, true))
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(
        Arc::new(arrow_schema::Schema::new(fields)),
        arrays,
        &options,
    )
    .expect("the drawn batch is valid")
}

fn kinds() -> impl Strategy<Value = Vec<TypeKind>> {
    proptest::sample::subsequence(KINDS.to_vec(), 0..=KINDS.len())
}

/// `column` behind keys of every dictionary key type: each row keys its own value, and every
/// third key is null.
fn keyed(column: &ArrayRef) -> Vec<ArrayRef> {
    let rows = column.len();
    macro_rules! keyed {
        ($($key:ty),*) => {
            vec![$(Arc::new(
                DictionaryArray::<$key>::try_new(
                    (0..rows)
                        .map(|row| (row % 3 != 2).then(|| row.try_into().expect("a few rows")))
                        .collect(),
                    Arc::clone(column),
                )
                .expect("keys within the values"),
            ) as ArrayRef),*]
        };
    }
    keyed!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type
    )
}

/// `column` as runs of two rows, their ends in every run-end type: each value twice.
fn runs(column: &ArrayRef) -> Vec<ArrayRef> {
    let ends = || (1..=column.len()).map(|run| 2 * run);
    let short = Int16Array::from_iter_values(ends().map(|end| end.try_into().expect("few rows")));
    let int = Int32Array::from_iter_values(ends().map(|end| end.try_into().expect("few rows")));
    let long = Int64Array::from_iter_values(ends().map(|end| end.try_into().expect("few rows")));
    vec![
        Arc::new(RunArray::<Int16Type>::try_new(&short, column).expect("ordered run ends")),
        Arc::new(RunArray::<Int32Type>::try_new(&int, column).expect("ordered run ends")),
        Arc::new(RunArray::<Int64Type>::try_new(&long, column).expect("ordered run ends")),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(512)))]

    /// Every type in every encoding the test kit draws, through every consumer: as drawn, as
    /// a slice of its rows, behind keys of every type and as runs of every run-end type.
    #[test]
    fn no_consumer_materializes_more_than_a_batch_expands_to(
        drawn in values::drawn(),
        native in kinds(),
    ) {
        let batch = batch(&drawn);
        let schema = batch.schema();
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            consumed(field, column, &native);
            if column.len() > 1 {
                consumed(field, &column.slice(1, column.len() - 1), &native);
            }
            if column.is_empty() {
                continue;
            }
            for encoded in keyed(column).into_iter().chain(runs(column)) {
                let field = field.as_ref().clone().with_data_type(encoded.data_type().clone());
                consumed(&field, &encoded, &native);
                consumed(&field, &encoded.slice(encoded.len() / 2, 1), &native);
            }
        }
    }
}

fn column(name: &str, array: ArrayRef) -> (ArrowField, ArrayRef) {
    (
        ArrowField::new(name, array.data_type().clone(), true),
        array,
    )
}

#[test]
fn a_list_view_expands_to_the_items_every_row_names() {
    const ROWS: usize = 500;
    let item = Arc::new(ArrowField::new("item", DataType::Int64, true));
    let items: ArrayRef = Arc::new(Int64Array::from_iter_values(
        0..i64::try_from(ROWS).unwrap(),
    ));
    let small: ArrayRef = Arc::new(
        ListViewArray::try_new(
            Arc::clone(&item),
            ScalarBuffer::from(vec![0_i32; ROWS]),
            ScalarBuffer::from(vec![i32::try_from(ROWS).unwrap(); ROWS]),
            Arc::clone(&items),
            None,
        )
        .unwrap(),
    );
    let large: ArrayRef = Arc::new(
        LargeListViewArray::try_new(
            item,
            ScalarBuffer::from(vec![0_i64; ROWS]),
            ScalarBuffer::from(vec![i64::try_from(ROWS).unwrap(); ROWS]),
            items,
            None,
        )
        .unwrap(),
    );
    for views in [small, large] {
        let (field, views) = column("views", views);
        consumed(&field, &views, &KINDS);
        assert!(expanded(&views, &KINDS) >= u64::try_from(ROWS * ROWS * 8).unwrap());
    }
}

#[test]
fn a_view_expands_to_the_bytes_it_names() {
    const ROWS: usize = 500;
    const BYTES: usize = 10_000;
    let mut views = BinaryViewBuilder::new();
    let block = views.append_block(vec![7_u8; BYTES].into());
    for _ in 0..ROWS {
        views
            .try_append_view(block, 0, u32::try_from(BYTES).unwrap())
            .unwrap();
    }
    let (field, views) = column("views", Arc::new(views.finish()));
    consumed(&field, &views, &KINDS);
    assert!(expanded(&views, &KINDS) >= u64::try_from(ROWS * BYTES).unwrap());
}

#[test]
fn a_struct_expands_to_its_field_names_every_row() {
    const ROWS: usize = 1_000;
    const NAME: usize = 1_000;
    let child = Arc::new(ArrowField::new("k".repeat(NAME), DataType::Boolean, true));
    let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true; ROWS]));
    let (field, rows) = column("rows", Arc::new(StructArray::from(vec![(child, flags)])));
    consumed(&field, &rows, &KINDS);
    assert!(expanded(&rows, &KINDS) >= u64::try_from(ROWS * NAME).unwrap());
}

/// A dictionary of no values whose keys are all null, keyed by each key type.
fn null_keys(values: &DataType, rows: usize) -> Vec<ArrayRef> {
    let values = new_empty_array(values);
    macro_rules! keyed {
        ($($key:ty),*) => {
            vec![$(Arc::new(
                DictionaryArray::<$key>::try_new(
                    std::iter::repeat_n(None, rows).collect(),
                    Arc::clone(&values),
                )
                .unwrap(),
            ) as ArrayRef),*]
        };
    }
    keyed!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type
    )
}

#[test]
fn a_null_key_expands_to_its_values_slot() {
    const ROWS: usize = 100;
    const WIDTH: i32 = 10_000;
    let decimals: Fields = (0..200)
        .map(|index| ArrowField::new(format!("d{index}"), DataType::Decimal256(76, 0), true))
        .collect();
    let item = Arc::new(ArrowField::new("item", DataType::Int64, true));
    let wide = [
        (DataType::FixedSizeBinary(WIDTH), ROWS * WIDTH as usize),
        (DataType::Struct(decimals), ROWS * 200 * 32),
        (DataType::FixedSizeList(item, 1_000), ROWS * 1_000 * 8),
    ];
    for (values, bytes) in wide {
        for keys in null_keys(&values, ROWS) {
            let (field, keys) = column("keys", keys);
            consumed(&field, &keys, &KINDS);
            assert!(
                expanded(&keys, &KINDS) >= u64::try_from(bytes).unwrap(),
                "{}",
                keys.data_type()
            );
        }
    }
}

/// One list of `items` nulls.
fn nulls(items: usize) -> ListArray {
    ListArray::new(
        Arc::new(ArrowField::new("item", DataType::Null, true)),
        OffsetBuffer::from_lengths([items]),
        Arc::new(NullArray::new(items)),
        None,
    )
}

#[test]
fn an_encoding_multiplies_what_its_value_expands_to() {
    const ROWS: usize = 200;
    const ITEMS: usize = 5_000;
    let keyed: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; ROWS]),
            Arc::new(nulls(ITEMS)),
        )
        .unwrap(),
    );
    let short: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![i16::try_from(ROWS).unwrap()]),
            &nulls(ITEMS),
        )
        .unwrap(),
    );
    let run: ArrayRef = Arc::new(
        RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![i32::try_from(ROWS).unwrap()]),
            &nulls(ITEMS),
        )
        .unwrap(),
    );
    let long: ArrayRef = Arc::new(
        RunArray::<Int64Type>::try_new(
            &Int64Array::from(vec![i64::try_from(ROWS).unwrap()]),
            &nulls(ITEMS),
        )
        .unwrap(),
    );
    for encoded in [keyed, short, run, long] {
        let (field, encoded) = column("lists", encoded);
        consumed(&field, &encoded, &KINDS);
        // Each row renders every null of its list.
        assert!(expanded(&encoded, &KINDS) >= u64::try_from(ROWS * ITEMS * 4).unwrap());
    }
}

#[test]
fn only_what_rows_name_is_materialized() {
    const HELD: usize = 100_000;
    // One row naming one item of a long child whose items need widening.
    let items: ArrayRef = Arc::new(arrow_array::UInt32Array::from(vec![7; HELD]));
    let one: ArrayRef = Arc::new(ListArray::new(
        Arc::new(ArrowField::new("item", DataType::UInt32, true)),
        OffsetBuffer::new(vec![5_i32, 6].into()),
        Arc::clone(&items),
        None,
    ));
    // One key naming one of many values that need casting.
    let words: ArrayRef = Arc::new(arrow_array::StringViewArray::from_iter_values(
        (0..HELD).map(|index| format!("a value too long to inline {index}")),
    ));
    let key: ArrayRef =
        Arc::new(DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![3]), words).unwrap());
    // One key naming one row of a run spanning many.
    let wide: ArrayRef = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_iter([[9_u8; 100]].into_iter()).unwrap(),
    );
    let run = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![i32::try_from(HELD).unwrap()]),
        &wide,
    )
    .unwrap();
    let keyed_run: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            arrow_array::Int8Array::from(vec![Some(1), None]),
            Arc::new(run),
        )
        .unwrap(),
    );
    // The middle rows of a list, cut as the engine cuts a batch.
    let lists: ArrayRef = Arc::new(ListArray::new(
        Arc::new(ArrowField::new("item", DataType::UInt32, true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(10, HELD / 10)),
        items,
        None,
    ));
    let middle = lists.slice(100, 2);
    for (named, limit) in [(one, 200), (key, 400), (keyed_run, 800), (middle, 1_000)] {
        let (field, named) = column("named", named);
        consumed(&field, &named, &KINDS);
        assert!(expanded(&named, &KINDS) <= limit, "{}", named.data_type());
    }
}

/// A budget of `capacity` bytes and an admission charging it for a destination storing text.
fn charging(capacity: u64) -> (MemoryBudget, Charging) {
    let budget = MemoryBudget::new(capacity);
    let admission = Charging::new(budget.clone(), Arc::new(Rendering::text()));
    (budget, admission)
}

#[tokio::test]
async fn a_push_is_admitted_for_the_larger_of_what_it_holds_and_what_it_becomes() {
    let (budget, admission) = charging(1 << 30);
    // A slice of three rows keeps its whole buffer alive.
    let whole = Int64Array::from(vec![7; 100_000]);
    let slice = RecordBatch::try_from_iter([("n", Arc::new(whole.slice(0, 3)) as ArrayRef)]);
    let held = Push::Arrow(slice.unwrap());
    let permit = admission.admit(&SourceEvent::Push(held)).await.unwrap();
    assert!(budget.reserved() >= 800_000, "{}", budget.reserved());
    let admitted = Admitted::of(permit.unwrap()).unwrap();
    assert_eq!(admitted.bytes, budget.reserved());
    drop(admitted);
    assert_eq!(budget.reserved(), 0);
    // A dictionary of one long value named by every row becomes far more than it holds.
    let long = "x".repeat(1_000);
    let keyed = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0; 10_000]),
        Arc::new(arrow_array::StringArray::from(vec![long.as_str()])),
    );
    let expanding = RecordBatch::try_from_iter([("s", Arc::new(keyed.unwrap()) as ArrayRef)]);
    let change = Push::Changes(expanding.unwrap());
    let permit = admission.admit(&SourceEvent::Push(change)).await.unwrap();
    assert!(budget.reserved() >= 10_000_000, "{}", budget.reserved());
    drop(permit);
    let json = Push::Json(bytes::Bytes::from_static(b"[{}]"));
    let permit = admission.admit(&SourceEvent::Push(json)).await.unwrap();
    assert_eq!(budget.reserved(), 4);
    drop(permit);
}

#[tokio::test]
async fn a_checkpoint_is_admitted_for_its_cursor_and_signals_for_nothing() {
    let (budget, admission) = charging(100);
    let cursor = rdlt_connector::Cursor::new(1, &[7; 40]).unwrap();
    let checkpoint = SourceEvent::Checkpoint {
        cursor,
        answers: None,
    };
    let held = admission.admit(&checkpoint).await.unwrap();
    assert_eq!(budget.reserved(), 40);
    // Only a commit releases a cursor, so it keeps no push of the whole budget out.
    let json = Push::Json(bytes::Bytes::from(vec![b' '; 100]));
    let pushed = admission.admit(&SourceEvent::Push(json)).await.unwrap();
    assert_eq!(budget.reserved(), 140);
    drop((held, pushed));
    for event in [
        SourceEvent::Replan,
        SourceEvent::Behind { records: 3 },
        SourceEvent::Log {
            level: rdlt_connector::LogLevel::Info,
            message: "line".to_owned(),
        },
        SourceEvent::Metric {
            name: "rows".to_owned(),
            value: 1.0,
        },
    ] {
        assert!(admission.admit(&event).await.unwrap().is_none());
    }
    assert_eq!(budget.reserved(), 0);
    // What a read keeps beside its events is charged at once, apart from what is in flight.
    let kept = admission.charge(500);
    assert_eq!(budget.reserved(), 500);
    let pushed = admission
        .admit(&SourceEvent::Push(Push::Json(bytes::Bytes::from_static(
            b"[]",
        ))))
        .await
        .unwrap();
    assert_eq!(budget.reserved(), 502);
    drop((kept, pushed));
    assert_eq!(budget.reserved(), 0);
    assert!(Admitted::of(Box::new(7_u8)).is_none());
}

#[tokio::test]
async fn a_push_expanding_beyond_the_budget_holds_the_budget_and_no_more() {
    let (budget, admission) = charging(1_000);
    // A million rows of a wide value: far more than the budget, measured in one step.
    let wide = arrow_array::new_null_array(&DataType::FixedSizeBinary(64), 1_000_000);
    let batch = RecordBatch::try_from_iter([("w", wide)]).unwrap();
    let permit = admission
        .admit(&SourceEvent::Push(Push::Arrow(batch)))
        .await
        .unwrap();
    assert_eq!(budget.reserved(), 1_000);
    // What it says it holds is what the budget reserved, so nothing later counts as paid for.
    assert_eq!(Admitted::of(permit.unwrap()).unwrap().bytes, 1_000);
}

#[tokio::test(start_paused = true)]
async fn a_checkpoint_behind_what_reads_keep_fails_at_the_deadline_and_holds_no_one_up() {
    const WAIT: std::time::Duration = std::time::Duration::from_secs(3600);
    let pool = crate::compute::RayonPool::new(std::num::NonZeroUsize::MIN).unwrap();
    let env = Arc::new(crate::env::SystemEnv::new(pool));
    let budget = MemoryBudget::new(256 << 20).within(Arc::clone(&env) as Arc<dyn Env>, WAIT);
    let admission = Charging::new(budget.clone(), Arc::new(Rendering::text()));
    // Four reads each keep a dictionary of a quarter of the budget, as a decoder's is charged.
    let kept: Vec<_> = (0..4).map(|_| admission.charge(64 << 20)).collect();
    assert_eq!(budget.reserved(), 256 << 20);
    // A checkpoint finds no room, and nothing in flight or waiting for a commit could make any.
    let cursor = rdlt_connector::Cursor::new(1, &[7; 40]).unwrap();
    let checkpoint = SourceEvent::Checkpoint {
        cursor,
        answers: None,
    };
    let started = env.instant();
    let refused = admission.admit(&checkpoint).await.unwrap_err();
    assert_eq!(env.instant().duration_since(started), WAIT);
    assert_eq!(refused.code(), Some("memory_budget_wait_exceeded"));
    assert!(refused.is_retryable());
    let said = refused.to_string();
    assert!(said.contains("268435456 are kept by reads"), "{said}");
    // The read's failure is the budget's, whichever stream met it.
    let failed = crate::error::Error::connector(crate::error::Side::Source, "reading", refused);
    assert_eq!(failed.kind(), crate::ErrorKind::Memory);
    assert!(failed.is_retryable());
    // The checkpoint left the queue: a push is admitted at once, as before it.
    let json = SourceEvent::Push(Push::Json(bytes::Bytes::from_static(b"[{}]")));
    let pushed = tokio::select! {
        biased;
        pushed = admission.admit(&json) => pushed,
        () = env.sleep(std::time::Duration::from_secs(1)) => panic!("the push waits"),
    };
    assert!(pushed.unwrap().is_some());
    drop(kept);
    assert_eq!(budget.reserved(), 0);
}
