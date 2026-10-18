use std::time::Duration;

use rdlt_connector::{ConnectorErrorKind, Role};
use tokio::io::{AsyncReadExt as _, DuplexStream};

use super::{Connection, DEADLINE_EXCEEDED, Deadlines, Options, client};

/// Options whose connect deadline is 20 ms.
fn connecting() -> Options {
    Options {
        deadlines: Deadlines {
            connect: Duration::from_millis(20),
            ..Deadlines::default()
        },
        ..Options::default()
    }
}

/// Reads `peer` to its end, which comes once nothing holds the host's end.
async fn ended(peer: &mut DuplexStream) {
    let mut said = Vec::new();
    let mut bounded = peer.take(1 << 20);
    let reading = bounded.read_to_end(&mut said);
    tokio::time::timeout(Duration::from_millis(100), reading)
        .await
        .expect("the host's end closes with its failed handshake")
        .expect("the host's end reads to its end");
}

#[tokio::test(start_paused = true)]
async fn a_handshake_its_peer_never_answers_fails_by_the_connect_deadline_and_leaves_no_connection()
{
    // Room for one byte: HTTP/2's preface is never written whole. Room for 64 KiB: the
    // connection is set up and only the handshake's answer never comes.
    for room in [1, 1 << 16] {
        let (host, mut peer) = tokio::io::duplex(room);
        let handshaking = Connection::handshake(host, Role::Source, connecting());
        let error = tokio::time::timeout(Duration::from_millis(200), handshaking)
            .await
            .expect("the connect deadline ends the handshake")
            .expect_err("the peer answers nothing");
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Transient, Some(DEADLINE_EXCEEDED)),
            "room {room}"
        );
        ended(&mut peer).await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_client_whose_peer_never_reads_fails_by_the_connect_deadline() {
    let (host, mut peer) = tokio::io::duplex(1);
    let error = tokio::time::timeout(Duration::from_millis(200), client(host, connecting()))
        .await
        .expect("the connect deadline ends the setup")
        .expect_err("the peer reads nothing");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Transient, Some(DEADLINE_EXCEEDED))
    );
    ended(&mut peer).await;
}
