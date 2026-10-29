use super::Transport;
use crate::limits::{MAX_CALLS, STREAM_WINDOW};

#[test]
fn the_connection_window_holds_every_calls_stream_window_and_the_heartbeats() {
    let transport = Transport::default();
    let calls = u64::from(MAX_CALLS) + 1;
    assert!(u64::from(transport.connection_window) >= calls * u64::from(transport.stream_window));
    assert!(transport.connection_window < 1 << 31);
}

#[test]
fn a_stream_window_holds_four_transport_frames() {
    let transport = Transport::default();
    assert!(transport.stream_window >= 4 * transport.max_frame);
    assert_eq!(u64::from(transport.stream_window), STREAM_WINDOW);
    assert!((16_384..=16_777_215).contains(&transport.max_frame));
}
