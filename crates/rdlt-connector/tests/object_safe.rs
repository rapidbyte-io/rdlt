//! Compile-time proof that the SPI traits stay object-safe — a future process/WASM
//! host holds connectors as trait objects (contract: connector-spi.md preamble).

use rdlt_connector::{Destination, LoadSession, Source};

#[allow(dead_code)]
fn source_is_object_safe(_: &dyn Source) {}

#[allow(dead_code)]
fn destination_is_object_safe(_: &dyn Destination) {}

#[allow(dead_code)]
fn load_session_is_object_safe(_: &mut dyn LoadSession) {}

#[test]
fn traits_are_object_safe() {
    // The assertions above are the test; they fail to compile if object safety breaks.
}

#[tokio::test]
async fn records_channel_round_trip_and_backpressure() {
    use bytes::Bytes;
    use rdlt_connector::{PushPayload, records_channel};

    let (mut out, mut input) = records_channel(1024);
    out.raw_json(Bytes::from_static(b"{\"a\":1}\n"))
        .await
        .unwrap();
    let push = input.recv().await.expect("one message");
    match push.payload {
        PushPayload::RawJson(bytes) => assert_eq!(&bytes[..], b"{\"a\":1}\n"),
        other => panic!("unexpected payload: {other:?}"),
    }

    // Backpressure: a second oversized push must block until the first is dropped.
    let (mut out, mut input) = records_channel(16);
    out.raw_json(Bytes::from_static(&[b'x'; 16])).await.unwrap();
    let first = input.recv().await.expect("first message");
    let mut blocked = Box::pin(out.raw_json(Bytes::from_static(&[b'y'; 16])));
    tokio::select! {
        _ = &mut blocked => panic!("push proceeded past an exhausted byte budget"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
    }
    drop(first); // releases the budget
    blocked.await.unwrap();
    assert!(input.recv().await.is_some());
}

#[tokio::test]
async fn closing_the_channel_reads_as_channel_closed() {
    use bytes::Bytes;
    use rdlt_connector::records_channel;

    let (mut out, mut input) = records_channel(8);
    // Fill the budget, then close while the next push is blocked on it.
    out.raw_json(Bytes::from_static(&[b'x'; 8])).await.unwrap();
    let _held = input.recv().await;
    let mut blocked = Box::pin(out.raw_json(Bytes::from_static(&[b'y'; 8])));
    tokio::select! {
        _ = &mut blocked => panic!("should be blocked"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
    }
    input.close();
    blocked
        .await
        .expect_err("closure must surface as ChannelClosed");
}
