//! What weighing a batch costs its sender, counted in what it looks at.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, Int64Array, ListArray, ListViewArray, RecordBatch,
    RunArray, StringViewArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};

use super::{cost, cut, least};
use crate::codec::tests::samples::batch_of;
use crate::error::WireError;
use crate::limits::Limits;

/// One list of `items` texts too long for a view to hold.
fn texts(items: usize) -> ArrayRef {
    let views = (0..items).map(|at| format!("a text of over twelve bytes, {at}"));
    let views = StringViewArray::from_iter_values(views);
    let field = Arc::new(Field::new("item", DataType::Utf8View, true));
    let offsets = OffsetBuffer::from_lengths([items]);
    Arc::new(ListArray::new(field, offsets, Arc::new(views), None))
}

/// How many pieces `batch` is cut into within `limits`, and how many columns, rows, runs and
/// keys weighing them looked at.
fn weighing(batch: &RecordBatch, limits: Limits) -> (usize, u64) {
    let mut sender = crate::codec::Encoder::default();
    sender.schema(&batch.schema()).unwrap();
    let mut cutting = crate::codec::Cut::new(batch.clone(), limits);
    let mut pieces = 0;
    while sender.piece(&mut cutting).unwrap().is_some() {
        pieces += 1;
    }
    (pieces, cutting.weigher.visits())
}

#[test]
fn plain_columns_are_weighed_by_arithmetic_however_many_rows_they_hold() {
    // A million rows of eight integers: a frame as it is, and 977 at a peer's least limits.
    let column: ArrayRef = Arc::new(Int64Array::from(vec![1; 1_000_000]));
    let columns = (0..8).map(|at| (format!("c{at}"), Arc::clone(&column)));
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let whole = cost(&batch, Limits::default());
    assert_eq!(whole.pieces, 1);
    // Each stretch is twice the last: twenty-one of them, of eight columns.
    assert!(whole.visits <= 8 * 24, "{whole:?}");
    let pieces = cost(&batch, least());
    assert_eq!(pieces.pieces, 977);
    assert!(pieces.visits <= 977 * 8 * 16, "{pieces:?}");
}

#[test]
fn a_run_of_many_rows_is_weighed_once() {
    let values = texts(30_000);
    let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![30_000]), &values).unwrap();
    let batch = batch_of(Arc::new(runs));
    let (pieces, visits) = weighing(&batch, Limits::default());
    assert_eq!(pieces, 1);
    // The run's value once, with the first row in it, and each stretch.
    assert!(visits <= 30_000 + 100, "{visits}");
}

#[test]
fn a_row_beyond_the_values_of_a_frame_is_weighed_no_further_than_them() {
    // One row naming three million texts, for a receiver of a million values a frame.
    let texts = StringViewArray::from_iter_values((0..3_000_000).map(|_| "t"));
    let field = Arc::new(Field::new("item", DataType::Utf8View, true));
    let (offsets, sizes) = (vec![0], vec![3_000_000]);
    let lists = ListViewArray::new(field, offsets.into(), sizes.into(), Arc::new(texts), None);
    let batch = batch_of(Arc::new(lists));
    let limits = least();
    let mut sender = crate::codec::Encoder::default();
    sender.schema(&batch.schema()).unwrap();
    let mut cutting = crate::codec::Cut::new(batch.clone(), limits);
    let refused = sender.piece(&mut cutting).unwrap_err();
    assert!(matches!(refused, WireError::Refused(_)), "{refused}");
    assert!(refused.to_string().contains("batch values"), "{refused}");
    let visits = cutting.weigher.visits();
    assert!(visits <= limits.batch_values + 16, "{visits}");
    assert_eq!(cutting.probe.compactions, 0);
    assert!(cut(&batch, &Limits::default()).is_ok());
}

#[test]
fn a_row_beyond_a_frame_is_refused_from_its_weight_before_it_is_narrowed_or_encoded() {
    // Five mebibytes in one row, for a receiver of four a frame: as bytes, as a view, and as
    // the items one list view names of a child of twice as many.
    let limits = least();
    let blob = vec![7_u8; 5 << 20];
    let text = "t".repeat(5 << 20);
    let items = Arc::new(arrow_array::Int8Array::from(vec![7; 10 << 20]));
    let field = Arc::new(Field::new("item", DataType::Int8, true));
    let lists = ListViewArray::new(field, vec![9].into(), vec![5 << 20].into(), items, None);
    let rows: [ArrayRef; 3] = [
        Arc::new(arrow_array::BinaryArray::from_iter_values([blob])),
        Arc::new(StringViewArray::from_iter_values([text])),
        Arc::new(lists),
    ];
    for row in rows {
        let batch = batch_of(row);
        let mut sender = crate::codec::Encoder::default();
        sender.schema(&batch.schema()).unwrap();
        let mut cutting = crate::codec::Cut::new(batch.clone(), limits);
        let refused = sender.piece(&mut cutting).unwrap_err();
        let WireError::Refused(refusal) = &refused else {
            panic!("{}: {refused}", batch.schema());
        };
        // The list view's row is also beyond a frame's values, which are weighed first.
        let named = ["frame bytes", "view bytes", "batch values"];
        assert!(named.contains(&refusal.field), "{refusal:?}");
        assert!(refusal.actual >= 5 << 20, "{refusal:?}");
        assert_eq!(
            (cutting.probe.compactions, sender.encodes),
            (0, 0),
            "{}",
            batch.schema()
        );
    }
}

/// Kibibytes: the most memory this process has held, where the system tells.
fn peak() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

/// Kibibytes: how far `run` raised the most memory this process has held.
fn raised<T>(run: impl FnOnce() -> T) -> (T, u64) {
    let before = peak();
    let out = run();
    let grown = peak()
        .zip(before)
        .map_or(0, |(after, before)| after - before);
    (out, grown)
}

/// A dictionary column of one row whose key is `key`, over `values`.
fn one_key(key: i32, values: ArrayRef) -> RecordBatch {
    let keyed = DictionaryArray::try_new(Int32Array::from(vec![key]), values).unwrap();
    batch_of(Arc::new(keyed))
}

#[test]
fn a_key_is_weighed_without_holding_anything_for_the_values_before_it() {
    // One row naming the last of thirty-two million nulls, which hold no bytes.
    let values: ArrayRef = Arc::new(arrow_array::NullArray::new(32_000_000));
    let batch = one_key(31_999_999, values);
    let (weight, grown) = raised(|| {
        let mut weigher = crate::codec::weigh::Weigher::new(&batch);
        weigher.begin();
        weigher.weigh(0)
    });
    assert_eq!((weight.values, weight.frame_bytes()), (1, 5));
    assert!(grown < 16 << 10, "weighing one key held {grown} KiB");
}

#[test]
fn keys_are_weighed_by_arithmetic_whatever_the_value_they_name_holds() {
    // Two thousand keys naming one list of twenty thousand dictionary keys.
    let flags: ArrayRef = Arc::new(arrow_array::Int8Array::from(vec![1, 2]));
    let inner = arrow_array::Int8Array::from(vec![0; 20_000]);
    let inner: ArrayRef = Arc::new(DictionaryArray::try_new(inner, flags).unwrap());
    let field = Arc::new(Field::new("item", inner.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths([20_000]);
    let values: ArrayRef = Arc::new(ListArray::new(field, offsets, inner, None));
    let keyed = DictionaryArray::try_new(Int32Array::from(vec![0; 2_000]), values).unwrap();
    let batch = batch_of(Arc::new(keyed));
    let mut weigher = crate::codec::weigh::Weigher::new(&batch);
    weigher.begin();
    let weight = weigher.weigh_rows(0..2_000);
    assert_eq!((weight.values, weight.frame_bits), (2_000, 2_000 * 33));
    assert!(weigher.visits() <= 4, "{}", weigher.visits());
}

/// Checks a one-row batch of the dictionary `values` is refused by a sender at a peer's least
/// limits, naming `field`, before anything is rebuilt or encoded and holding little beside it.
fn refused_from_the_weight_of_its_dictionary(values: ArrayRef, field: &str) {
    let batch = one_key(0, values);
    let mut sender = crate::codec::Encoder::default();
    sender.schema(&batch.schema()).unwrap();
    let mut cutting = crate::codec::Cut::new(batch.clone(), least());
    let (refused, grown) = raised(|| sender.piece(&mut cutting).unwrap_err());
    let WireError::Refused(refusal) = &refused else {
        panic!("{}: {refused}", batch.schema());
    };
    assert_eq!(refusal.field, field, "{refusal:?}");
    assert_eq!((cutting.probe.compactions, sender.encodes), (0, 0));
    assert!(grown < 16 << 10, "refusing the batch held {grown} KiB");
    assert!(cutting.is_done());
    assert_eq!(sender.piece(&mut cutting).unwrap(), None);
}

#[test]
fn a_dictionary_beyond_a_frame_is_refused_from_its_weight_before_it_is_rebuilt_or_encoded() {
    // Six hundred views of one text of a mebibyte: a mebibyte held, six hundred named.
    let text = "t".repeat(1 << 20);
    let one = StringViewArray::from_iter_values([text.as_str()]);
    let (views, buffers, _) = one.into_parts();
    let shared = vec![views[0]; 600];
    let views: ArrayRef = Arc::new(StringViewArray::new(shared.into(), buffers, None));
    refused_from_the_weight_of_its_dictionary(views, "view bytes");
    // Three hundred list views each naming the same mebibyte of items.
    let items = Arc::new(arrow_array::Int8Array::from(vec![7; 1 << 20]));
    let field = Arc::new(Field::new("item", DataType::Int8, true));
    let (offsets, sizes) = (vec![0_i64; 300], vec![1_i64 << 20; 300]);
    let lists =
        arrow_array::LargeListViewArray::new(field, offsets.into(), sizes.into(), items, None);
    refused_from_the_weight_of_its_dictionary(Arc::new(lists), "batch values");
    // Two hundred thousand texts no key but one names: eight megabytes as they are.
    let texts = (0..200_000).map(|at| format!("a value no key names, number {at:08}"));
    let texts = arrow_array::StringArray::from_iter_values(texts);
    refused_from_the_weight_of_its_dictionary(Arc::new(texts), "frame bytes");
}

#[test]
fn rows_that_all_fit_are_weighed_in_stretches_each_twice_the_last() {
    // Ten rows of one column: stretches of one, one, two and four rows, and the two left.
    let batch = batch_of(Arc::new(Int64Array::from(vec![1; 10])));
    let whole = cost(&batch, Limits::default());
    assert_eq!(
        (whole.pieces, whole.probe.weighed, whole.visits),
        (1, 10, 5)
    );
}
