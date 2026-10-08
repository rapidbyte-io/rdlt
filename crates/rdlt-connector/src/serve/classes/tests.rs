use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::Frame;
use rdlt_wire::Limits;
use rdlt_wire::limits::{Class, HANDSHAKE_BYTES, MIN_FRAME_BYTES};
use rdlt_wire::tonic::{Status, body::Body};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::super::Served;
use super::super::service::Service;
use super::{classed, window};

#[test]
fn each_call_s_request_has_the_class_of_what_it_carries() {
    let expected = [
        ("Handshake", Class::Handshake),
        ("Configure", Class::Config),
        ("Check", Class::Control),
        ("Discover", Class::Control),
        ("Plan", Class::State),
        ("Read", Class::Cursor),
        ("Committed", Class::State),
        ("Open", Class::Control),
        ("ApplySchema", Class::Schema),
        ("Write", Class::Data),
        ("Commit", Class::State),
        ("Close", Class::Control),
        ("Heartbeat", Class::Control),
        ("ReadPublished", Class::Control),
        ("ReadAcknowledged", Class::Control),
        ("Unknown", Class::Control),
    ];
    for (name, expected) in expected {
        assert_eq!(Class::of_request(name), expected, "{name}");
    }
}

/// A body of the frames sent to it, which ends once its sender is dropped.
struct Fed(mpsc::UnboundedReceiver<Frame<Bytes>>);

impl http_body::Body for Fed {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        self.0.poll_recv(context).map(|frame| frame.map(Ok))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_still_arriving_on_a_connection_hold_its_window_and_the_rest_wait() {
    // Limits whose largest message is a frame of the least size, so a handshake fills a frame.
    let limits = Limits {
        frame_bytes: MIN_FRAME_BYTES,
        catalog_bytes: 1 << 20,
        state_bytes: 1 << 20,
        config_bytes: 1 << 20,
        cursor_bytes: 1 << 20,
        schema_bytes: 1 << 20,
        ..Limits::default()
    };
    let stopping = CancellationToken::new();
    let service = Service::new(Arc::new(Served::new()), limits, None, 1, stopping);
    let window = window(&limits);
    let mut classed = classed(service, &limits, window.clone());
    // Five handshakes, each of a declared length at its class's bound, each only begun.
    let length = u32::try_from(HANDSHAKE_BYTES).unwrap() - 5;
    let message = 5 + usize::try_from(length).unwrap();
    let mut feeds = Vec::new();
    let mut calls = tokio::task::JoinSet::new();
    for _ in 0..5 {
        let (feed, frames) = mpsc::unbounded_channel();
        let mut begun = vec![0];
        begun.extend(length.to_be_bytes());
        begun.push(0x0a);
        feed.send(Frame::data(Bytes::from(begun))).unwrap();
        feeds.push(feed);
        let request = http::Request::post("/rdlt.connector.v1.Connector/Handshake")
            .header("content-type", "application/grpc")
            .body(Body::new(Fed(frames)))
            .unwrap();
        calls.spawn(tower::Service::call(&mut classed, request));
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // As many as the window holds take room, and the fifth what is left of it as it waits,
    // refused by nothing.
    assert_eq!(limits.largest() * 4 / message, 4);
    assert_eq!(window.taken(), limits.largest() * 4);
    assert!(calls.try_join_next().is_none(), "no call is answered yet");
    drop(feeds);
    while let Some(answered) = calls.join_next().await {
        answered.unwrap().unwrap();
    }
    assert_eq!(window.taken(), 0, "every call's room is given back");
}
