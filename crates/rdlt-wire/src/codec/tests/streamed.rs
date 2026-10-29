//! A batch's frames as a stream carries them: sent from the batch's own buffers, read into an
//! allocation of their own, and decoded where they arrived.

use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use bytes::Buf as _;
use proptest::prelude::*;
use rdlt_testkit::drawn::values;

use super::batch;
use crate::codec::measure::measured;
use crate::codec::pieces::Body as _;
use crate::codec::{Cut, Decoder, Encoded, Encoder, IpcFrame};
use crate::limits::Limits;
use crate::stream::{Call, End, FrameReader, Kind, Outgoing, Rows};

/// What `future` comes to, which it does at once: it reads only bytes held in memory.
fn ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("a read of bytes in memory waits"),
    }
}

/// `frames` sent as batch frames from their pieces, and read back as a host reads a read's.
fn streamed(frames: Vec<Encoded>, limits: &Limits) -> Vec<IpcFrame> {
    let mut sent = Vec::new();
    for frame in frames {
        let kind = Kind::Batch {
            rows: Rows::Arrow,
            dictionary: frame.is_dictionary(),
            tag: 1,
        };
        let mut frame = Outgoing::new(kind, frame.into_pieces()).expect("a frame within a head");
        while frame.has_remaining() {
            sent.extend_from_slice(frame.chunk());
            let taken = frame.chunk().len();
            frame.advance(taken);
        }
    }
    let bounds = Call::Read.bounds(End::Connector, limits);
    let mut reader = FrameReader::new(&sent[..], bounds);
    let mut received = Vec::new();
    while let Some(frame) = ready(reader.next()).expect("a frame its sender sent") {
        received.push(IpcFrame::framed(frame.body).expect("an encapsulated message"));
    }
    received
}

/// The rows `received` decode to, in order, by a receiver of `batch`'s schema within `limits`.
fn decoded(batch: &RecordBatch, received: &[IpcFrame], limits: Limits) -> Vec<RecordBatch> {
    let mut decoder = Decoder::new(limits);
    let schema = Encoder::default().schema(&batch.schema()).unwrap();
    decoder.schema(&schema).unwrap();
    let pieces = received.iter().filter_map(|frame| {
        decoder
            .frame(frame)
            .expect("the receiver takes what its sender cut")
    });
    pieces.collect()
}

/// Whether two refusals of a frame refuse it alike: by the same limit, or for the same problem.
fn alike(one: &crate::WireError, other: &crate::WireError) -> bool {
    use crate::WireError::{Malformed, Refused};
    match (one, other) {
        (Refused(one), Refused(other)) => one == other,
        (Malformed { problem: one, .. }, Malformed { problem: other, .. }) => one == other,
        _ => false,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// Every frame a cut makes of any batch, sent from the batch's own buffers, is read off the
    /// stream into an allocation of its own and decoded there to the batch's rows, in order.
    #[test]
    fn every_frame_a_cut_makes_round_trips_through_a_stream(
        drawn in values::drawn(),
        frame_bytes in 4_096_u64..262_144,
    ) {
        let batch = batch(&drawn);
        let limits = Limits { frame_bytes, ..Limits::default() };
        let mut encoder = Encoder::default();
        encoder.schema(&batch.schema()).unwrap();
        let mut cut = Cut::new(batch.clone(), limits);
        let mut received = Vec::new();
        for _ in 0..=batch.num_rows() {
            match encoder.piece(&mut cut) {
                Ok(Some(frames)) => received.extend(streamed(frames, &limits)),
                Ok(None) => break,
                // A row beyond a frame is refused before it is sent.
                Err(crate::WireError::Refused(_)) => return Ok(()),
                Err(error) => panic!("the sender failed: {error}"),
            }
        }
        prop_assert!(cut.is_done());
        let pieces = decoded(&batch, &received, limits);
        let mut start = 0;
        for piece in &pieces {
            prop_assert_eq!(piece, &batch.slice(start, piece.num_rows()));
            start += piece.num_rows();
        }
        prop_assert_eq!(start, batch.num_rows());
    }

    /// The sender's walk of a frame over the pieces it sends it from finds what the receiver's
    /// walk finds over the frame gathered whole: the same buffers, values and view bytes.
    #[test]
    fn the_walk_agrees_over_a_body_and_its_pieces(drawn in values::drawn()) {
        let batch = batch(&drawn);
        let limits = Limits::default();
        let mut encoder = Encoder::default();
        encoder.schema(&batch.schema()).unwrap();
        let (dictionaries, frame) = encoder.encoded(&batch).unwrap();
        let columns = encoder.columns.as_ref();
        for frame in dictionaries.iter().chain(std::iter::once(&frame)) {
            let gathered = frame.gathered();
            let over_pieces = measured(columns, &limits, frame.header(), frame.body());
            let whole = measured(columns, &limits, &gathered.header, &gathered.body[..]);
            prop_assert_eq!(frame.body().len(), gathered.body.len());
            match (over_pieces, whole) {
                (Ok(pieces), Ok(whole)) => {
                    let spans = |walked: &crate::codec::shape::Walked| -> Vec<(usize, usize)> {
                        walked.spans.iter().map(|span| (span.offset, span.length)).collect()
                    };
                    prop_assert_eq!(spans(&pieces.walked), spans(&whole.walked));
                    prop_assert_eq!(pieces.walked.values, whole.walked.values);
                    prop_assert_eq!(pieces.walked.view_bytes, whole.walked.view_bytes);
                    prop_assert_eq!(pieces.dictionary, whole.dictionary);
                }
                (Err(pieces), Err(whole)) => prop_assert!(alike(&pieces, &whole), "{pieces} {whole}"),
                (pieces, whole) => prop_assert!(
                    false,
                    "the walks disagree: {:?} over pieces, {:?} whole",
                    pieces.err(),
                    whole.err()
                ),
            }
        }
    }
}
