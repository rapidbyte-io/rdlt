use std::sync::Arc;

use arrow_array::builder::{ListViewBuilder, StringViewBuilder};
use arrow_array::types::{Int8Type, Int32Type, Int64Type, UInt16Type};
use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, Int64Array, NullArray, RecordBatch, RunArray,
    StringArray,
};
use bytes::Bytes;

use super::{Budget, spend};
use crate::cost::Rendering;
use crate::cursor::Cursor;
use crate::sink::Push;
use crate::testing::limits::{HELD_BYTES, HELD_EVENT_BYTES, HELD_ROWS};

fn left(budget: &Budget) -> (usize, usize) {
    use std::sync::atomic::Ordering::Relaxed;
    (budget.bytes.load(Relaxed), budget.rows.load(Relaxed))
}

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable([("column", column, true)]).unwrap()
}

#[test]
fn what_is_left_is_spent_to_nothing_and_never_below() {
    let left = std::sync::atomic::AtomicUsize::new(10);
    assert!(spend(&left, 0));
    assert!(spend(&left, 4));
    assert!(spend(&left, 6));
    assert!(spend(&left, 0));
    assert!(!spend(&left, 1));
    let left = std::sync::atomic::AtomicUsize::new(10);
    assert!(!spend(&left, 11));
    // What was asked beyond what was left leaves nothing for what follows.
    assert!(!spend(&left, 1));
    assert!(!spend(&left, usize::MAX));
}

/// Columns in every encoding: plain, views, list views, dictionaries, runs, and nulls.
fn encodings() -> Vec<ArrayRef> {
    let texts: ArrayRef = Arc::new(StringArray::from(vec!["ann", "a longer name"]));
    let views = {
        let mut views = StringViewBuilder::new();
        views.append_value("a string longer than a view holds inline");
        views.append_value("short");
        Arc::new(views.finish()) as ArrayRef
    };
    let list_views = {
        let mut lists = ListViewBuilder::new(arrow_array::builder::Int64Builder::new());
        lists.append_value([Some(1), Some(2)]);
        lists.append_value([Some(3)]);
        Arc::new(lists.finish()) as ArrayRef
    };
    let dictionary = |keys: Vec<i8>| -> ArrayRef {
        Arc::new(DictionaryArray::<Int8Type>::try_new(keys.into(), Arc::clone(&texts)).unwrap())
    };
    let wide_keys: ArrayRef = Arc::new(
        DictionaryArray::<UInt16Type>::try_new(vec![1_u16; 100].into(), Arc::clone(&texts))
            .unwrap(),
    );
    let run = |ends: Vec<i32>| -> ArrayRef {
        let ends = Int32Array::from(ends);
        Arc::new(RunArray::<Int32Type>::try_new(&ends, &texts).unwrap())
    };
    let long_run: ArrayRef = {
        let ends = Int64Array::from(vec![1000_i64, 2000]);
        Arc::new(RunArray::<Int64Type>::try_new(&ends, &texts).unwrap())
    };
    vec![
        Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
        Arc::clone(&texts),
        views,
        list_views,
        dictionary(vec![0, 1, 1, 1]),
        wide_keys,
        run(vec![2, 5]),
        long_run,
        Arc::new(NullArray::new(1000)),
    ]
}

#[tokio::test]
async fn each_push_is_charged_its_bytes_its_rows_and_what_holds_it_whatever_its_encoding() {
    for column in encodings() {
        let kind = column.data_type().clone();
        let rows = column.len();
        let arrow = Push::Arrow(batch(Arc::clone(&column)));
        let changes = Push::Changes(batch(column));
        for push in [arrow, changes] {
            let budget = Budget::new();
            budget.push(&push).await.unwrap();
            let cost = Rendering::native().charge(&push, u64::MAX);
            let bytes = usize::try_from(cost).unwrap() + HELD_EVENT_BYTES;
            assert_eq!(
                left(&budget),
                (HELD_BYTES - bytes, HELD_ROWS - rows),
                "{kind}"
            );
        }
    }
    let budget = Budget::new();
    budget
        .push(&Push::Json(Bytes::from_static(b"[{\"a\":1}]")))
        .await
        .unwrap();
    // A JSON push is charged a row for each record its text holds.
    assert_eq!(
        left(&budget),
        (HELD_BYTES - 9 - HELD_EVENT_BYTES, HELD_ROWS - 1)
    );
    budget
        .cursor(&Cursor::new(1, Bytes::from_static(b"abc")).unwrap())
        .unwrap();
    assert_eq!(
        left(&budget),
        (HELD_BYTES - 12 - 2 * HELD_EVENT_BYTES, HELD_ROWS - 1)
    );
}

#[tokio::test]
async fn a_clause_holds_up_to_its_rows_and_its_bytes_and_nothing_once_beyond_either() {
    let nulls = |rows: usize| Push::Arrow(batch(Arc::new(NullArray::new(rows))));
    let budget = Budget::new();
    budget.push(&nulls(HELD_ROWS - 1)).await.unwrap();
    budget.push(&nulls(1)).await.unwrap();
    budget.push(&nulls(0)).await.unwrap();
    assert!(budget.push(&nulls(1)).await.is_err());
    // Events of no bytes are held as many as what holds them leaves room for.
    let budget = Budget::new();
    let empty = Push::Json(Bytes::new());
    for _ in 0..HELD_BYTES / HELD_EVENT_BYTES {
        budget.push(&empty).await.unwrap();
    }
    let beyond = budget.push(&empty).await.unwrap_err();
    assert!(beyond.unobserved, "a source that sends more broke nothing");
    // Beyond its bytes, a clause holds no cursor either, nor rows it had room for.
    let cursor = Cursor::new(1, Bytes::new()).unwrap();
    assert!(budget.cursor(&cursor).is_err());
    assert!(budget.push(&nulls(1)).await.is_err());
    let budget = Budget::new();
    let half = Push::Json(Bytes::from(vec![b' '; HELD_BYTES / 2 - HELD_EVENT_BYTES]));
    budget.push(&half).await.unwrap();
    budget.push(&half).await.unwrap();
    assert_eq!(left(&budget).0, 0);
    assert!(budget.cursor(&cursor).is_err());
}
