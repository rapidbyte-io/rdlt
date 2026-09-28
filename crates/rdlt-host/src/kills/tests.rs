use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::Kills;

#[test]
fn each_kill_is_counted() {
    let kills = Kills::new();
    assert_eq!(kills.count(), 0);
    kills.kill();
    kills.clone().kill();
    assert_eq!(kills.count(), 2, "clones share the count");
}

#[tokio::test]
async fn a_kill_cuts_the_streams_severed_before_it_and_none_after() {
    let kills = Kills::new();
    let (mut before_peer, before) = tokio::io::duplex(64);
    let mut before = kills.sever(before);
    kills.kill();
    let (mut after_peer, after) = tokio::io::duplex(64);
    let mut after = kills.sever(after);
    assert_eq!(kills.count(), 1);
    let cut = before.write_all(b"x").await.expect_err("the stream is cut");
    assert_eq!(cut.kind(), std::io::ErrorKind::ConnectionAborted);
    drop(before_peer.write_all(b"y").await);
    after
        .write_all(b"z")
        .await
        .expect("a stream severed after the kill lives");
    let mut read = [0; 1];
    after_peer
        .read_exact(&mut read)
        .await
        .expect("the peer reads");
    assert_eq!(&read, b"z");
}
