use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, ArrayRef, Decimal128Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use rdlt_connector::cost::Stored;
use rdlt_connector::{DecimalType, LoadId, LogicalType, TimeUnit};

use super::{Case, Kept, LoweringCase, Made, Storage, events, stamp, with_column};
use crate::cost::CHANGE_ROW;
use crate::table::Stamp;
use crate::table::aligned;

const ROWS: u32 = 64;

fn prepared(case: LoweringCase) -> (Case, RecordBatch) {
    let built = Case::new(case, ROWS);
    let prepared = built.prepare().unwrap();
    assert_eq!(
        prepared.batch.schema(),
        built.plan().view().schema,
        "{case:?}"
    );
    assert_eq!(prepared.discarded_values, built.kept().discarded_values);
    (built, prepared.batch)
}

#[test]
fn each_case_keeps_the_rows_and_discards_the_values_it_says() {
    let all = ROWS as usize;
    for (case, rows, discarded_values) in [
        (LoweringCase::AppendNative, all, 0),
        (LoweringCase::AppendText, all, 0),
        (LoweringCase::MergeUnique, all, 0),
        (LoweringCase::MergeDuplicates, all / 2, 0),
        (LoweringCase::History, all, 0),
        (LoweringCase::Changes, all, 0),
        (LoweringCase::SplitJson, all, 0),
        (LoweringCase::OneBadValue, all, 1),
        (LoweringCase::OneDroppedRow, all - 1, 0),
        (LoweringCase::TemporalWidening, all, 0),
    ] {
        let (built, batch) = prepared(case);
        let kept = Kept {
            rows,
            discarded_values,
        };
        assert_eq!(built.kept(), kept, "{case:?}");
        assert_eq!(batch.num_rows(), rows, "{case:?}");
        assert_eq!(built.rows(), all, "{case:?}");
    }
}

#[test]
fn every_case_is_named_once() {
    let names: Vec<&str> = LoweringCase::ALL
        .into_iter()
        .map(LoweringCase::name)
        .collect();
    assert_eq!(
        names,
        [
            "append_native",
            "append_text",
            "merge_unique",
            "merge_duplicates",
            "history",
            "changes",
            "split_json",
            "one_bad_value",
            "one_dropped_row",
            "temporal_widening",
        ]
    );
}

#[test]
fn the_text_case_stores_every_column_as_text_and_the_native_one_as_it_arrives() {
    let (text, batch) = prepared(LoweringCase::AppendText);
    let uuids = batch.column_by_name("uuid").unwrap().as_string::<i32>();
    assert_eq!(uuids.value(0), "00000000-0000-0000-0000-000000000000");
    assert_eq!(uuids.value(1), "00000000-0000-0000-9e37-79b97f4a7c15");
    let view = text.plan().view();
    assert!(
        view.lowered
            .iter()
            .all(|lowered| *lowered == LogicalType::Utf8)
    );
    let uuid = view
        .model
        .columns
        .iter()
        .find(|column| column.name() == "uuid");
    assert_eq!(uuid.unwrap().logical_type(), &LogicalType::Uuid);
    let (native, _) = prepared(LoweringCase::AppendNative);
    let view = native.plan().view();
    let types: Vec<&LogicalType> = view
        .model
        .columns
        .iter()
        .map(rdlt_connector::Field::logical_type)
        .collect();
    assert_eq!(view.lowered.iter().collect::<Vec<_>>(), types);
}

#[test]
fn a_merge_keeps_the_last_row_of_each_key() {
    let (_, batch) = prepared(LoweringCase::MergeDuplicates);
    let column = |name: &str| {
        let values = batch.column_by_name(name).unwrap();
        values.as_primitive::<Int64Type>().values().to_vec()
    };
    let mut ids = column("id");
    // Key k is held by rows k and k + 32, whose values win.
    for (id, a) in ids.iter().zip(column("a")) {
        assert_eq!(a, (id + 32) * 7);
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..32).collect::<Vec<_>>());
}

#[test]
fn a_history_table_hashes_its_rows_and_a_change_stream_carries_its_flags() {
    let (history, batch) = prepared(LoweringCase::History);
    assert!(history.plan().view().meta.history.is_some());
    assert_eq!(history.plan().view().model.columns.len(), 50);
    let hashes = batch.column_by_name("_rdlt_row_hash").unwrap();
    assert_eq!(hashes.null_count(), 0);
    let (changes, batch) = prepared(LoweringCase::Changes);
    assert_eq!(changes.plan().view().model.columns.len(), 200);
    // A pushed bitmap covers the change batch's own fields, its change columns among them.
    let pushed = changes.batch();
    let bitmaps = pushed
        .column_by_name("_rdlt_unchanged")
        .unwrap()
        .as_binary::<i32>();
    let fields = pushed.num_columns().div_ceil(8);
    assert!(bitmaps.iter().all(|bitmap| bitmap.unwrap().len() == fields));
    let flags = batch.column_by_name("_rdlt_unchanged").unwrap();
    let flags = flags.as_binary::<i32>();
    // Row 0 flags column 1 and row 1 column 2, one bit a row.
    assert_eq!((flags.value(0)[0], flags.value(1)[0]), (0b10, 0b100));
    let bits = |bitmap: &[u8]| bitmap.iter().map(|byte| byte.count_ones()).sum::<u32>();
    assert!(flags.iter().all(|bitmap| bits(bitmap.unwrap()) == 1));
}

#[test]
fn a_split_column_reads_its_integers_and_keeps_the_rest_as_json() {
    let (split, batch) = prepared(LoweringCase::SplitJson);
    let fields = split.plan().incoming().schema.fields();
    let index = fields.iter().position(|field| field.name() == "a").unwrap();
    let read = Stored {
        column: LogicalType::Int64,
        text: false,
        read: true,
    };
    assert_eq!(split.plan().stored()[index], Some(read));
    let own = batch
        .column_by_name("a")
        .unwrap()
        .as_primitive::<Int64Type>();
    assert_eq!((own.value(1), own.null_count()), (1, 7));
    let rest = batch.column_by_name("a__json").unwrap().as_string::<i32>();
    assert_eq!(
        (rest.value(0), rest.value(10)),
        (r#"{"cents":0}"#, r#""10""#)
    );
}

#[test]
fn instants_widen_into_their_column_and_one_it_cannot_hold_is_discarded() {
    let micros = LogicalType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let (widened, batch) = prepared(LoweringCase::TemporalWidening);
    let columns = &widened.plan().view().model.columns;
    let at = columns.iter().find(|column| column.name() == "at").unwrap();
    assert_eq!(at.logical_type(), &micros);
    let at = batch.column_by_name("at").unwrap();
    let at = at.as_primitive::<TimestampMicrosecondType>();
    assert_eq!((at.value(3), at.null_count()), (3_000_000, 7));
    assert!(at.is_null(10));
    let (_, batch) = prepared(LoweringCase::OneBadValue);
    let at = batch.column_by_name("at").unwrap();
    assert_eq!(at.null_count(), 1);
    assert!(at.is_null(32));
}

#[test]
fn a_row_holding_an_instant_its_column_cannot_hold_is_dropped_and_its_integers_widen() {
    let (dropped, batch) = prepared(LoweringCase::OneDroppedRow);
    let columns = &dropped.plan().view().model.columns;
    let n = columns.iter().find(|column| column.name() == "n").unwrap();
    assert_eq!(n.logical_type(), &LogicalType::Int64);
    // Row 32 holds the instant no nanosecond holds.
    let n = batch.column_by_name("n").unwrap();
    let kept: Vec<i64> = (0..64).filter(|row| *row != 32).collect();
    assert_eq!(n.as_primitive::<Int64Type>().values().to_vec(), kept);
    assert_eq!(batch.column_by_name("at").unwrap().null_count(), 0);
}

#[test]
fn a_change_batch_is_charged_its_change_columns_and_what_splitting_a_row_holds() {
    let changes = Case::new(LoweringCase::Changes, ROWS);
    let stored = aligned(changes.batch(), &changes.plan().stored());
    let row = changes.plan().row_bytes() + CHANGE_ROW;
    let mut measure = changes
        .rendering
        .lowering(changes.batch(), stored, row, u64::MAX);
    assert_eq!(changes.charge(), measure.expanded(0..64));
    let append = Case::new(LoweringCase::AppendNative, ROWS);
    let (stored, row) = (append.plan().stored(), append.plan().row_bytes());
    let mut measure = append
        .rendering
        .lowering(append.batch(), stored, row, u64::MAX);
    assert_eq!(append.charge(), measure.expanded(0..64));
}

#[test]
fn a_case_stored_as_text_lowers_every_column_to_text() {
    for case in LoweringCase::ALL {
        let built = Case::stored(case, ROWS, Storage::Text);
        let view = built.plan().view();
        let text = view
            .lowered
            .iter()
            .all(|lowered| *lowered == LogicalType::Utf8);
        assert!(text, "{case:?}");
        let prepared = built.prepare().unwrap();
        assert_eq!(prepared.batch.num_rows(), built.kept().rows, "{case:?}");
    }
}

#[test]
#[should_panic(expected = "a case holds two rows at least")]
fn a_case_of_one_row_is_a_bug() {
    Case::new(LoweringCase::MergeDuplicates, 1);
}

#[test]
fn a_change_flags_only_data_columns_and_never_its_key() {
    let changes = Case::new(LoweringCase::Changes, 400);
    let pushed = changes.batch();
    let bitmaps = pushed
        .column_by_name("_rdlt_unchanged")
        .unwrap()
        .as_binary::<i32>();
    let mut flagged = std::collections::BTreeSet::new();
    for bitmap in bitmaps.iter().map(Option::unwrap) {
        for bit in 0..bitmap.len() * 8 {
            if bitmap[bit / 8] & (1 << (bit % 8)) != 0 {
                flagged.insert(bit);
            }
        }
    }
    // Every data column but the key, column 0, and none of the change columns past them.
    assert_eq!(flagged, (1..200).collect());
}

/// Whether `prepared` holds each column of `pushed` as the very array it was given.
fn handed_over(pushed: &RecordBatch, prepared: &RecordBatch) -> bool {
    let schema = pushed.schema();
    schema
        .fields()
        .iter()
        .zip(pushed.columns())
        .all(|(field, column)| {
            let lowered = prepared.column_by_name(field.name());
            lowered.is_some_and(|lowered| lowered.to_data().ptr_eq(&column.to_data()))
        })
}

/// `batch` with its column `name` of strings held as views.
fn viewed(batch: &RecordBatch, name: &str) -> RecordBatch {
    let index = batch.schema().index_of(name).unwrap();
    let mut columns = batch.columns().to_vec();
    columns[index] = arrow_cast::cast(&columns[index], &DataType::Utf8View).unwrap();
    let mut fields: Vec<ArrowField> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields[index] = fields[index].clone().with_data_type(DataType::Utf8View);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// Every case, and appends whose lowering reads or writes values in other ways: of a table
/// holding a column the batch lacks, of strings held as views, and of decimals.
fn cases() -> Vec<(&'static str, Case)> {
    let native = |made| Case::planned(Storage::Native.capabilities(), made);
    let mixed = events(0, ROWS, 10);
    let wide: ArrayRef = Arc::new(Int64Array::from_iter_values(0..i64::from(ROWS)));
    let lacking = with_column(&mixed, "extra", LogicalType::Int64, wide);
    let cents = Decimal128Array::from_iter_values(0..i128::from(ROWS));
    let cents: ArrayRef = Arc::new(cents.with_precision_and_scale(10, 2).unwrap());
    let decimal = LogicalType::Decimal(DecimalType::new(10, 2).unwrap());
    let decimals = with_column(&mixed, "price", decimal, cents);
    let cases = LoweringCase::ALL.map(|case| (case.name(), Case::new(case, ROWS)));
    cases
        .into_iter()
        .chain([
            ("lacking", native(Made::into_table(lacking, mixed.clone()))),
            ("views", native(Made::append(viewed(&mixed, "name")))),
            ("decimals", native(Made::append(decimals))),
        ])
        .collect()
}

#[test]
fn a_plan_copies_nothing_only_where_it_hands_every_column_over_as_it_arrives() {
    for (name, case) in cases() {
        let (plan, pushed) = (case.plan(), case.batch());
        // Strings held as views are of another type than their column stores.
        let hands_over = matches!(name, "append_native" | "views");
        assert_eq!(plan.hands_over(), hands_over, "{name}");
        // A load's first piece builds the load's constant columns, a byte a row.
        assert!(!plan.copies_nothing(pushed, &stamp()), "{name}");
        case.prepare().unwrap();
        let copies_nothing = plan.copies_nothing(pushed, &stamp());
        assert_eq!(copies_nothing, name == "append_native", "{name}");
        if copies_nothing {
            // Every column is the array it arrived as, beside the load's two constant ones.
            let prepared = case.prepare().unwrap().batch;
            assert!(handed_over(pushed, &prepared), "{name}");
            assert_eq!(prepared.num_columns(), pushed.num_columns() + 2, "{name}");
        }
    }
}

#[test]
fn a_plan_copies_nothing_for_rows_of_the_load_its_constants_were_built_for() {
    let case = Case::new(LoweringCase::AppendNative, ROWS);
    case.prepare().unwrap();
    let (plan, pushed) = (case.plan(), case.batch());
    assert!(plan.copies_nothing(&pushed.slice(1, 10), &stamp()));
    assert!(!plan.copies_nothing(&events(0, ROWS + 1, 10), &stamp()));
    let next = Stamp {
        load_id: LoadId::from_parts(UNIX_EPOCH, 2),
        ..stamp()
    };
    assert!(!plan.copies_nothing(pushed, &next));
}
