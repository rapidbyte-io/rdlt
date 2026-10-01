use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use arrow_array::builder::{
    FixedSizeListBuilder, LargeListBuilder, LargeListViewBuilder, ListBuilder, ListViewBuilder,
    MapBuilder, StringBuilder, TimestampSecondBuilder,
};
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, DictionaryArray, DurationSecondArray, Int64Array,
    NullArray, PrimitiveArray, RecordBatch, RunArray, StringArray, StructArray, Time32SecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UnionArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{Field, UnionFields};

use super::{RenderError, Rendering};
use crate::testing::limits::YIELD_ROWS;

/// The output of `future`, and how many times it yielded before it.
fn polled<F: Future>(future: F) -> (F::Output, usize) {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    let mut yields = 0;
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return (output, yields),
            Poll::Pending => yields += 1,
        }
    }
}

fn rendered(batch: &RecordBatch, limit: usize) -> Result<Vec<String>, RenderError> {
    polled(Rendering::new(limit).rows(batch, |_| true)).0
}

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable([("at", column, true)]).unwrap()
}

/// A second no calendar holds once its zone's offset is added.
const EDGE: i64 = 8_210_266_876_799;

fn edge() -> ArrayRef {
    Arc::new(TimestampSecondArray::from(vec![EDGE]).with_timezone("+14:00"))
}

fn dictionary<K: ArrowDictionaryKeyType>(values: ArrayRef) -> ArrayRef
where
    K::Native: From<bool>,
{
    let keys = PrimitiveArray::<K>::from_iter_values([K::Native::from(false)]);
    Arc::new(DictionaryArray::<K>::try_new(keys, values).unwrap())
}

fn run<R: RunEndIndexType>(values: &dyn Array) -> ArrayRef
where
    R::Native: From<bool>,
{
    let ends = PrimitiveArray::<R>::from_iter_values([R::Native::from(true)]);
    Arc::new(RunArray::<R>::try_new(&ends, values).unwrap())
}

fn structure(value: ArrayRef) -> ArrayRef {
    let field = Arc::new(Field::new("at", value.data_type().clone(), true));
    Arc::new(StructArray::from(vec![(field, value)]))
}

fn instants() -> TimestampSecondBuilder {
    TimestampSecondBuilder::new().with_timezone("+14:00")
}

fn list() -> ArrayRef {
    let mut list = ListBuilder::new(instants());
    list.append_value([Some(EDGE)]);
    Arc::new(list.finish())
}

/// Columns holding instants, dates, times and spans no calendar or clock holds, in every
/// encoding and nesting.
fn temporal() -> Vec<ArrayRef> {
    let large_list = {
        let mut list = LargeListBuilder::new(instants());
        list.append_value([Some(EDGE)]);
        Arc::new(list.finish()) as ArrayRef
    };
    let fixed_list = {
        let mut list = FixedSizeListBuilder::new(instants(), 1);
        list.values().append_value(EDGE);
        list.append(true);
        Arc::new(list.finish()) as ArrayRef
    };
    let map = {
        let mut map = MapBuilder::new(None, StringBuilder::new(), instants());
        map.keys().append_value("k");
        map.values().append_value(EDGE);
        map.append(true).unwrap();
        Arc::new(map.finish()) as ArrayRef
    };
    vec![
        edge(),
        Arc::new(TimestampSecondArray::from(vec![i64::MAX])),
        Arc::new(TimestampMillisecondArray::from(vec![i64::MIN]).with_timezone("-12:00")),
        Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX]).with_timezone("UTC")),
        Arc::new(TimestampNanosecondArray::from(vec![i64::MIN]).with_timezone("Europe/Warsaw")),
        Arc::new(Date32Array::from(vec![i32::MAX])),
        Arc::new(Date64Array::from(vec![i64::MIN])),
        Arc::new(Time32SecondArray::from(vec![i32::MAX])),
        Arc::new(Time64NanosecondArray::from(vec![i64::MIN])),
        Arc::new(DurationSecondArray::from(vec![i64::MIN])),
        dictionary::<Int8Type>(edge()),
        dictionary::<Int16Type>(edge()),
        dictionary::<Int32Type>(edge()),
        dictionary::<Int64Type>(edge()),
        dictionary::<UInt8Type>(edge()),
        dictionary::<UInt16Type>(edge()),
        dictionary::<UInt32Type>(edge()),
        dictionary::<UInt64Type>(edge()),
        run::<Int16Type>(edge().as_ref()),
        run::<Int32Type>(edge().as_ref()),
        run::<Int64Type>(edge().as_ref()),
        list(),
        large_list,
        fixed_list,
        structure(edge()),
        map,
        structure(list()),
        structure(dictionary::<Int32Type>(edge())),
        dictionary::<Int32Type>(structure(edge())),
        run::<Int32Type>(structure(list()).as_ref()),
    ]
}

#[test]
fn what_no_calendar_holds_renders_as_the_integer_it_is_in_every_encoding() {
    for column in temporal() {
        let kind = column.data_type().clone();
        // What encodes or nests the edge renders it; a column of its own renders its value.
        let integer = if kind.is_nested() || !kind.is_temporal() {
            EDGE.to_string()
        } else {
            let integers = arrow_cast::cast(&column, &integer_of(&kind)).unwrap();
            arrow_cast::display::array_value_to_string(&integers, 0).unwrap()
        };
        let rows = rendered(&batch(column), usize::MAX).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(rows.len(), 1, "{kind}");
        assert!(rows[0].contains(&integer), "{kind}: {}", rows[0]);
        // The column says what its integers count.
        assert!(
            rows[0].starts_with(&format!("at<{kind}>=")),
            "{kind}: {}",
            rows[0]
        );
    }
}

fn integer_of(kind: &arrow_schema::DataType) -> arrow_schema::DataType {
    use arrow_schema::DataType;
    match kind {
        DataType::Date32 | DataType::Time32(_) => DataType::Int32,
        _ => DataType::Int64,
    }
}

#[test]
fn instants_where_no_cast_reaches_do_not_render_and_do_not_panic() {
    let union = {
        let field = Field::new("at", edge().data_type().clone(), true);
        let fields = UnionFields::try_new([0], [field]).unwrap();
        let ids = ScalarBuffer::from(vec![0_i8]);
        Arc::new(UnionArray::try_new(fields, ids, None, vec![edge()]).unwrap()) as ArrayRef
    };
    let list_view = {
        let mut list = ListViewBuilder::new(instants());
        list.append_value([Some(EDGE)]);
        Arc::new(list.finish()) as ArrayRef
    };
    let large_list_view = {
        let mut list = LargeListViewBuilder::new(instants());
        list.append_value([Some(EDGE)]);
        Arc::new(list.finish()) as ArrayRef
    };
    for column in [union.clone(), structure(union), list_view, large_list_view] {
        let kind = column.data_type().clone();
        let rendered = rendered(&batch(column), usize::MAX);
        let unrendered = matches!(
            &rendered,
            Err(RenderError::Unrendered { column, .. }) if column == "at"
        );
        // A list view renders where its cast is one Arrow has.
        let integers = rendered
            .as_ref()
            .is_ok_and(|rows| rows[0].contains(&EDGE.to_string()));
        assert!(unrendered || integers, "{kind}: {rendered:?}");
    }
}

#[test]
fn rows_render_their_kept_columns_by_name_whatever_their_order() {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![Some("ann"), Some("ola")]));
    let nothing: ArrayRef = Arc::new(NullArray::new(2));
    let columns = [
        ("name", Arc::clone(&names), true),
        ("_meta", Arc::clone(&names), true),
        ("id", Arc::clone(&ids), true),
        ("empty", nothing, true),
    ];
    let batch = RecordBatch::try_from_iter_with_nullable(columns).unwrap();
    let keep = |name: &str| !name.starts_with('_');
    let (rows, _) = polled(Rendering::new(usize::MAX).rows(&batch, keep));
    assert_eq!(
        rows.unwrap(),
        [
            "empty=null, id=1, name=ann",
            "empty=null, id=null, name=ola"
        ]
    );
    let reordered = [("id", ids, true), ("name", names, true)];
    let reordered = RecordBatch::try_from_iter_with_nullable(reordered).unwrap();
    assert_eq!(
        rendered(&reordered, usize::MAX).unwrap(),
        ["id=1, name=ann", "id=null, name=ola"]
    );
    // Rows of no column kept are rows all the same.
    let (rows, _) = polled(Rendering::new(0).rows(&batch, |_| false));
    assert_eq!(rows.unwrap(), ["", ""]);
}

#[test]
fn rendering_stops_at_its_limit_and_no_text_is_held_beyond_it() {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    // Two rows of `id=10` and `id=20`.
    assert_eq!(rendered(&batch, 10).unwrap().len(), 2);
    for limit in [0, 1, 4, 5, 9] {
        assert_eq!(
            rendered(&batch, limit),
            Err(RenderError::Beyond(limit)),
            "{limit}"
        );
    }
    // The limit is of all a rendering renders, batch after batch.
    let mut rendering = Rendering::new(15);
    assert!(polled(rendering.rows(&batch, |_| true)).0.is_ok());
    let again = polled(rendering.rows(&batch, |_| true)).0;
    assert_eq!(again, Err(RenderError::Beyond(15)));
    // A row of a million items of nothing is cut where the limit is, not rendered whole.
    let mut items = ListBuilder::new(arrow_array::builder::NullBuilder::new());
    items.values().append_nulls(1 << 20);
    items.append(true);
    let wide = RecordBatch::try_from_iter([("items", Arc::new(items.finish()) as ArrayRef)]);
    assert_eq!(
        rendered(&wide.unwrap(), 4096),
        Err(RenderError::Beyond(4096))
    );
}

#[test]
fn text_rendered_elsewhere_is_charged_against_the_same_limit() {
    let mut rendering = Rendering::new(8);
    assert_eq!(rendering.charge("abcd"), Ok(()));
    assert_eq!(rendering.charge(""), Ok(()));
    assert_eq!(rendering.charge("abcd"), Ok(()));
    assert_eq!(rendering.charge(""), Ok(()));
    assert_eq!(rendering.charge("a"), Err(RenderError::Beyond(8)));
    let mut rendering = Rendering::new(8);
    assert_eq!(rendering.charge("abcdefghi"), Err(RenderError::Beyond(8)));
    // Once beyond, nothing more fits.
    assert_eq!(rendering.charge("a"), Err(RenderError::Beyond(8)));
}

#[test]
fn rendering_yields_between_pieces_so_a_bound_can_end_it() {
    for (rows, yields) in [
        (0, 0),
        (1, 0),
        (YIELD_ROWS, 0),
        (YIELD_ROWS + 1, 1),
        (2 * YIELD_ROWS, 1),
        (3 * YIELD_ROWS + 1, 3),
    ] {
        let batch = batch(Arc::new(NullArray::new(rows)));
        let mut rendering = Rendering::new(usize::MAX);
        let (rendered, yielded) = polled(rendering.rows(&batch, |_| true));
        assert_eq!(rendered.unwrap().len(), rows);
        assert_eq!(yielded, yields, "{rows} rows");
    }
}
