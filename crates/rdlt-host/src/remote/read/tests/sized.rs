//! A read's frame is charged, before it is decoded, no less than the push it decodes to, so the
//! push is admitted within that charge.

use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::cost::{batch_arrays, json_charge, push_charge};
use rdlt_connector::wire::v1;
use rdlt_connector::{Push, SourceEvent};
use rdlt_testkit::drawn::values::drawn;
use rdlt_testkit::drawn::{Drawn, Scalar, array, field};
use rdlt_wire::bounded::{Bounded, Bounds, Charge, Charging, Held, Whole};
use rdlt_wire::limits::{Class, count};
use rdlt_wire::plane::{Incoming, Outgoing};
use rdlt_wire::prost::Message as _;
use rdlt_wire::{Encoder, Limits};

use super::{batch_frames, json_frame, keyed, reader, schema_frame};
use crate::remote::read::{Read, Reader};

/// Records what each message is charged, and holds nothing for it.
struct Recorded(Arc<Mutex<Vec<usize>>>);

impl Charge for Recorded {
    fn charge(&self, _: Class, bytes: usize) -> Charging {
        self.0.lock().unwrap().push(bytes);
        Box::pin(async { Ok(Box::new(()) as Held) })
    }
}

/// A frame as the read took it: what it was charged, the allocation it arrived in, what the
/// read's decoder kept as it was charged, and what it became.
struct Taken {
    charged: u64,
    allocation: u64,
    kept: u64,
    read: Read,
}

/// `frames` sent as a connector sends them and received as the host receives them, each charged
/// as `reader` sizes it once the frames before it are decoded, then decoded by it.
fn taken(reader: &mut Reader, frames: Vec<v1::ReadFrame>) -> Vec<Taken> {
    let limits = Limits::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async move {
        let body = tonic::body::Body::new(Outgoing::request(
            tokio_stream::iter(frames),
            limits.largest(),
        ));
        let bounds = Bounds::of(
            &limits,
            Class::of_answer("Read"),
            rdlt_wire::scan::response("Read"),
        );
        let asked = Arc::new(Mutex::new(Vec::new()));
        let charge = Arc::new(Recorded(Arc::clone(&asked))) as Arc<dyn Charge>;
        let bounded = Bounded::new(body, bounds, None).charged(Some(charge));
        let mut incoming = Incoming::<v1::ReadFrame>::request(bounded);
        let mut taken = Vec::new();
        loop {
            let arrived = incoming.arrived_sized(|whole| reader.size(whole)).await;
            let Some(received) = arrived.expect("the frame arrives") else {
                return taken;
            };
            let charged = asked.lock().unwrap().pop().expect("the frame is charged");
            let (allocation, kept) = (count(received.allocation.len()), reader.kept());
            let read = reader
                .event(received.message, received.allocation)
                .expect("an admitted frame");
            taken.push(Taken {
                charged: count(charged),
                allocation,
                kept,
                read,
            });
        }
    })
}

/// Checks that each push `frames` carry is charged no more than its frame was.
fn within(taken: &[Taken]) -> usize {
    let mut pushes = 0;
    for frame in taken {
        if let Read::Event(SourceEvent::Push(push)) = &frame.read {
            let charge = push_charge(push);
            assert!(
                charge <= frame.charged,
                "a push charged {charge} arrived in a frame charged {}",
                frame.charged
            );
            pushes += 1;
        }
    }
    pushes
}

/// The frames of a read of `batches`, one schema, each batch's dictionaries before it.
fn read_of(batches: &[RecordBatch]) -> Vec<v1::ReadFrame> {
    let mut encoder = Encoder::default();
    let mut frames = vec![schema_frame(&mut encoder, &batches[0], 1)];
    for batch in batches {
        frames.extend(batch_frames(&mut encoder, batch, 1));
    }
    frames
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
    let schema = Arc::new(arrow_schema::Schema::new(fields));
    RecordBatch::try_new_with_options(schema, arrays, &options).expect("a drawn batch is valid")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(64)))]

    #[test]
    fn a_frame_of_any_batch_is_charged_no_less_than_the_push_it_decodes_to(drawn in drawn()) {
        let batch = batch(&drawn);
        let taken = taken(&mut reader(), read_of(&[batch]));
        prop_assert!(within(&taken) >= 1, "the batch is pushed");
    }
}

#[test]
fn a_frame_of_dictionaries_and_of_a_thousand_columns_is_charged_its_push_and_no_more() {
    let columns = (0..1_000).map(|index| {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        (format!("c{index}"), ids)
    });
    let wide = RecordBatch::try_from_iter(columns).expect("a valid batch");
    for batches in [vec![wide], vec![keyed("x"), keyed("y"), keyed("z")]] {
        let taken = taken(&mut reader(), read_of(&batches));
        assert_eq!(within(&taken), batches.len());
        let arrays = batch_arrays(batches[0].schema_ref().fields());
        for frame in &taken {
            if let Read::Event(_) = frame.read {
                assert_eq!(
                    frame.charged,
                    frame.allocation + frame.kept + arrays,
                    "a batch's frame is charged its message, what its decoder keeps and its arrays"
                );
            }
        }
    }
}

#[test]
fn a_json_frame_is_charged_what_its_push_is() {
    for length in [1, 300, 16_384, 2_097_152] {
        let text = Bytes::from(vec![b' '; length]);
        let taken = taken(&mut reader(), vec![json_frame(text.clone())]);
        assert_eq!(within(&taken), 1);
        if length >= 300 {
            assert_eq!(taken[0].charged, json_charge(count(length)), "{length}");
        }
    }
}

#[test]
fn a_json_frame_that_sets_its_text_twice_is_sized_by_the_first_and_its_push_is_charged_more() {
    let frame = |text: Bytes| json_frame(text).encode_to_vec();
    // Protocol buffers' decoder merges the frame's two settings, its text the last.
    let message = Bytes::from(
        [
            frame(Bytes::from_static(b" ")),
            frame(vec![b' '; 1 << 20].into()),
        ]
        .concat(),
    );
    let form = rdlt_wire::scan::response("Read").expect("a read frame's form");
    let decoded = rdlt_wire::scan::decoded(form, &message, usize::MAX).expect("a frame");
    let whole = Whole {
        message: &message,
        allocation: message.len(),
        decoded: decoded.max(message.len()),
    };
    let mut reader = reader();
    let charged = count(reader.size(&whole));
    let frame = v1::ReadFrame::decode(message.clone()).expect("a read frame");
    let Ok(Read::Event(SourceEvent::Push(push))) = reader.event(frame, message.clone()) else {
        panic!("the frame is a push");
    };
    let Push::Json(text) = &push else {
        panic!("the push is JSON");
    };
    assert_eq!(text.len(), 1 << 20);
    assert!(
        push_charge(&push) > charged,
        "the push is charged {} beyond its frame's {charged}, so it is refused",
        push_charge(&push)
    );
}
