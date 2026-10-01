//! What crosses is what was weighed, and equals the rows: every part of every nested layout,
//! cut whole and in pieces.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, Int64Array, ListArray, ListViewArray,
    RecordBatch, StringArray, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionFields};

use super::least;
use crate::codec::compact::{compacted, plain};
use crate::codec::tests::nested;
use crate::codec::tests::odd::rendered;
use crate::codec::tests::samples::batch_of;
use crate::codec::weigh::{Weigher, Weight};
use crate::codec::{Cut, Decoder, Encoder, IpcFrame, Shape};
use crate::limits::Limits;

fn item(data_type: &DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type.clone(), true))
}

/// What a cut of `batch` within `limits` delivers: its rows rendered, what the receiver's walk
/// counts in each batch frame and the frame's bytes, and how many times a piece was halved.
struct Crossed {
    rows: Vec<String>,
    frames: Vec<(Shape, u64)>,
    halvings: usize,
    /// How many dictionary frames went.
    dictionaries: usize,
}

/// Sends the frames `encoder` cuts `batch` into to `decoder`.
fn deliver(
    encoder: &mut Encoder,
    decoder: &mut Decoder,
    batch: &RecordBatch,
    limits: Limits,
) -> Result<Crossed, String> {
    let mut cut = Cut::new(batch.clone(), limits);
    let mut crossed = Crossed {
        rows: Vec::new(),
        frames: Vec::new(),
        halvings: 0,
        dictionaries: 0,
    };
    let piece = |encoder: &mut Encoder, cut: &mut Cut| -> Result<Option<Vec<IpcFrame>>, String> {
        let cut = catch_unwind(AssertUnwindSafe(|| encoder.piece(cut)));
        let cut = cut.map_err(|_| "the cut panicked".to_owned())?;
        cut.map_err(|error| format!("the sender refused: {error}"))
    };
    while let Some(frames) = piece(encoder, &mut cut)? {
        for frame in &frames {
            let (piece, shape) = decoder
                .shaped(frame)
                .map_err(|error| format!("the receiver refused: {error}"))?;
            let Some(piece) = piece else {
                crossed.dictionaries += 1;
                continue;
            };
            if piece.schema() != batch.schema() {
                return Err("the schema changed".to_owned());
            }
            let bytes = u64::try_from(frame.header.len() + frame.body.len()).unwrap();
            crossed.frames.push((shape, bytes));
            crossed.rows.extend(rendered(&piece));
        }
    }
    crossed.halvings = cut.probe.halvings;
    Ok(crossed)
}

/// What a cut of `batch` within `limits` delivers to a receiver of the same limits.
fn crossed(batch: &RecordBatch, limits: Limits) -> Result<Crossed, String> {
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(limits);
    let schema = encoder.schema(&batch.schema()).map_err(|e| e.to_string())?;
    decoder.schema(&schema).map_err(|e| e.to_string())?;
    deliver(&mut encoder, &mut decoder, batch, limits)
}

/// What `batch`'s rows weigh as one piece, and its frame's overhead.
fn weighed(batch: &RecordBatch) -> (Weight, u64) {
    let mut weigher = Weigher::new(batch);
    weigher.begin();
    let mut weight = Weight::default();
    for row in 0..batch.num_rows() {
        weight += weigher.weigh(row);
    }
    (weight, weigher.overhead())
}

/// Checks `part` cut whole crosses as its rows, in one frame its receiver counts as its
/// sender weighed it.
fn crosses_as_weighed(part: &RecordBatch) -> Result<(), String> {
    let whole = crossed(part, Limits::default())?;
    if whole.rows != rendered(part) {
        return Err(format!("rows changed: {:?}", whole.rows));
    }
    let [(shape, bytes)] = whole.frames[..] else {
        return Err(format!("{} frames", whole.frames.len()));
    };
    if whole.halvings != 0 {
        return Err(format!("halved {} times", whole.halvings));
    }
    if part.num_rows() == 0 {
        return Ok(());
    }
    let (weight, overhead) = weighed(part);
    let exact = (weight.values, weight.view_bytes) == (shape.values, shape.view_bytes);
    let over = weight.values >= shape.values && weight.view_bytes >= shape.view_bytes;
    // A rebuilt column drops what a null list spans under a list view or a dense union.
    if !exact && (plain(part.column(0).data_type()) || !over) {
        return Err(format!("weighed {weight:?}, walked {shape:?}"));
    }
    let most = weight.frame_bytes() + overhead;
    if bytes > most {
        return Err(format!("a frame of {bytes} bytes, weighed at most {most}"));
    }
    Ok(())
}

/// Checks every part of `column` crosses as weighed, and `column` cut into pieces of at most
/// one, two and three rows crosses as its rows.
fn checked(name: &str, column: &ArrayRef) -> Vec<String> {
    let batch = batch_of(Arc::clone(column));
    if Encoder::default().schema(&batch.schema()).is_err() {
        // No schema message describes a dictionary of dictionaries.
        return Vec::new();
    }
    let mut problems = Vec::new();
    for start in 0..=batch.num_rows() {
        for length in 0..=batch.num_rows() - start {
            if let Err(problem) = crosses_as_weighed(&batch.slice(start, length)) {
                problems.push(format!("{name} {start}+{length}: {problem}"));
            }
        }
    }
    let inner = batch.slice(1, batch.num_rows().saturating_sub(2));
    for (source, most) in [(&batch, 1), (&batch, 2), (&inner, 2), (&inner, 3)] {
        let limits = Limits {
            batch_rows: most,
            ..Limits::default()
        };
        match crossed(source, limits) {
            Ok(pieces) if pieces.rows == rendered(source) && pieces.halvings == 0 => {}
            Ok(pieces) => problems.push(format!(
                "{name} in pieces of {most}: rows changed or {} halvings",
                pieces.halvings
            )),
            Err(problem) => problems.push(format!("{name} in pieces of {most}: {problem}")),
        }
    }
    problems
}

#[test]
fn every_part_of_every_nested_layout_crosses_as_its_rows_and_as_it_was_weighed() {
    let mut problems = Vec::new();
    let columns = nested::columns();
    for (name, column) in &columns {
        problems.extend(checked(name, column));
    }
    let shown = &problems[..problems.len().min(12)];
    assert!(
        problems.is_empty(),
        "{} problems in {} columns, the first: {shown:#?}",
        problems.len(),
        columns.len()
    );
}

#[test]
fn a_column_of_a_plain_layout_goes_as_it_is_and_every_other_is_rebuilt() {
    let (mut as_it_is, mut rebuilt) = (0, 0);
    for (name, column) in nested::columns() {
        let part = batch_of(column.slice(1, column.len() - 1));
        let narrowed = compacted(&part).unwrap();
        if plain(column.data_type()) {
            let same = narrowed
                .column(0)
                .to_data()
                .ptr_eq(&part.column(0).to_data());
            assert!(same, "{name}");
            as_it_is += 1;
        } else {
            rebuilt += 1;
        }
    }
    assert!(as_it_is > 100 && rebuilt > 100, "{as_it_is} and {rebuilt}");
    // Fixed-width values, bytes, and lists, structs and dictionaries of them, and nothing else.
    let int = DataType::Int32;
    let listed = |data_type: &DataType| DataType::List(item(data_type));
    let keyed = |values: DataType| DataType::Dictionary(Box::new(DataType::Int8), Box::new(values));
    for goes in [
        DataType::Null,
        DataType::Boolean,
        DataType::Decimal256(40, 2),
        DataType::FixedSizeBinary(3),
        DataType::LargeBinary,
        DataType::LargeList(item(&listed(&DataType::Utf8))),
        DataType::FixedSizeList(item(&int), 2),
        DataType::Struct(vec![Field::new("a", keyed(DataType::Utf8), true)].into()),
    ] {
        assert!(plain(&goes), "{goes}");
    }
    let union = UnionFields::try_new(vec![0], vec![Field::new("a", int.clone(), true)]).unwrap();
    for rebuilt in [
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::ListView(item(&int)),
        DataType::LargeListView(item(&int)),
        DataType::Union(union.clone(), arrow_schema::UnionMode::Dense),
        DataType::Union(union, arrow_schema::UnionMode::Sparse),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", int.clone(), true)),
        ),
        listed(&DataType::Utf8View),
        keyed(listed(&DataType::Utf8View)),
        DataType::Struct(vec![Field::new("a", DataType::BinaryView, true)].into()),
    ] {
        assert!(!plain(&rebuilt), "{rebuilt}");
    }
}

/// `rows` union values, of two children in turn.
fn unions(rows: usize, dense: bool) -> ArrayRef {
    let fields = UnionFields::try_new(
        vec![0, 1],
        vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int64, true),
        ],
    )
    .unwrap();
    let narrow = |row: usize| i32::try_from(row).unwrap();
    let ids: ScalarBuffer<i8> = (0..rows).map(|row| i8::from(row % 2 == 1)).collect();
    let (each, offsets) = if dense {
        let offsets: ScalarBuffer<i32> = (0..rows).map(|row| narrow(row / 2)).collect();
        (rows.div_ceil(2), Some(offsets))
    } else {
        (rows, None)
    };
    let a = Int32Array::from_iter_values((0..each).map(narrow));
    let b = Int64Array::from_iter_values((0..each).map(|v| 1_000_000 + i64::from(narrow(v))));
    let children: Vec<ArrayRef> = vec![Arc::new(a), Arc::new(b)];
    Arc::new(UnionArray::try_new(fields, ids, offsets, children).unwrap())
}

#[test]
fn lists_of_unions_sliced_from_longer_ones_cross_as_their_rows_whole_and_in_pieces() {
    for dense in [true, false] {
        let rows = 3_000;
        let union = unions(3 * rows, dense);
        let offsets = OffsetBuffer::from_lengths(vec![3; rows]);
        let lists = ListArray::new(item(union.data_type()), offsets, union, None);
        let batch = batch_of(Arc::new(lists.slice(1, rows - 1)));
        for most in [1 << 20, 1_024] {
            let limits = Limits {
                batch_rows: most,
                ..Limits::default()
            };
            let pieces = crossed(&batch, limits).unwrap();
            assert_eq!(pieces.rows, rendered(&batch), "dense {dense} at {most}");
        }
    }
}

#[test]
fn a_dictionary_whose_values_are_a_part_of_lists_of_unions_crosses_as_its_rows() {
    let union = unions(30, true);
    let offsets = OffsetBuffer::from_lengths(vec![3; 10]);
    let lists: ArrayRef = Arc::new(ListArray::new(
        item(union.data_type()),
        offsets,
        union,
        None,
    ));
    let keys = Int8Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
    let keyed = DictionaryArray::try_new(keys, lists.slice(1, 8)).unwrap();
    let batch = batch_of(Arc::new(keyed));
    let limits = Limits {
        batch_rows: 2,
        ..Limits::default()
    };
    let pieces = crossed(&batch, limits).unwrap();
    assert_eq!(pieces.rows, rendered(&batch));
    // The values are rebuilt once and sent once, ahead of the first of four pieces.
    assert_eq!((pieces.frames.len(), pieces.dictionaries), (4, 1));
}

#[test]
fn a_row_that_fits_its_frame_is_sent_whatever_else_its_list_views_child_holds() {
    // One row naming all but three items of its child: within a frame's values as its rows
    // name them, and beyond them were the child sent whole.
    let limits = least();
    let lists = |named: Vec<i32>, child: usize| {
        let offsets = vec![0; named.len()];
        let items = Arc::new(Int8Array::from(vec![7; child]));
        let field = item(&DataType::Int8);
        batch_of(Arc::new(ListViewArray::new(
            field,
            offsets.into(),
            named.into(),
            items,
            None,
        )))
    };
    let batch = lists(vec![524_287], 524_290);
    assert_eq!(weighed(&batch).0.values, 1_048_575);
    let whole = crossed(&batch, limits).unwrap();
    assert_eq!((whole.frames.len(), whole.halvings), (1, 0));
    assert_eq!(whole.frames[0].0.values, 1_048_575);
    assert_eq!(whole.rows, rendered(&batch));
    // Two rows naming the same half of it go in one frame, encoded once.
    let whole = crossed(&lists(vec![262_143, 262_143], 524_290), limits).unwrap();
    assert_eq!((whole.frames.len(), whole.halvings), (1, 0));
}

#[test]
fn a_batch_of_no_rows_sends_no_dictionary_that_the_next_batch_must_replace() {
    let tags = (0..1000).map(|tag| format!("tag number {tag}"));
    let tags = Arc::new(StringArray::from_iter_values(tags));
    let keys = Int32Array::from_iter_values(0..1000);
    let batch = batch_of(Arc::new(DictionaryArray::try_new(keys, tags).unwrap()));
    let limits = Limits::default();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(limits);
    let schema = encoder.schema(&batch.schema()).unwrap();
    decoder.schema(&schema).unwrap();
    let mut dictionaries = Vec::new();
    for part in [
        batch.clone(),
        batch.slice(3, 0),
        batch.clone(),
        batch.clone(),
    ] {
        let sent = deliver(&mut encoder, &mut decoder, &part, limits).unwrap();
        assert_eq!(sent.rows, rendered(&part));
        dictionaries.push(sent.dictionaries);
    }
    assert_eq!(dictionaries, [1, 0, 0, 0]);
}
