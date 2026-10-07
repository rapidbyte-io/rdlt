use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatch};
use rdlt_connector::cost::Stored;
use rdlt_connector::{LogicalType, TimeUnit};

use super::{Case, Kept, LoweringCase, Storage};
use crate::cost::CHANGE_ROW;
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
            "temporal_widening",
        ]
    );
}

#[test]
fn the_text_case_stores_every_column_as_text_and_the_native_one_as_it_arrives() {
    let (text, _) = prepared(LoweringCase::AppendText);
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
