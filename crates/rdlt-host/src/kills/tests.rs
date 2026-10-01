use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use rdlt_connector::BoxFuture;

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

/// A stream whose reads and writes fail, as a connection reset does.
struct Reset;

impl tokio::io::AsyncRead for Reset {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()))
    }
}

impl tokio::io::AsyncWrite for Reset {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn a_kill_lands_once_when_the_connection_it_awaits_ends_after_it() {
    let kills = Kills::new();
    let (peer, stream) = tokio::io::duplex(64);
    let mut watched = kills.watch(stream, kills.next());
    let mut peer = peer;
    // Bytes, and a read into no room, are no end.
    peer.write_all(b"ab").await.expect("the peer writes");
    let mut read = [0; 1];
    watched.read_exact(&mut read).await.expect("it reads");
    assert_eq!(watched.read(&mut []).await.expect("no room"), 0);
    kills.kill();
    watched.read_exact(&mut read).await.expect("it reads on");
    watched.write_all(b"c").await.expect("it writes");
    watched.flush().await.expect("it flushes");
    assert_eq!(kills.landed(), 0, "the connection lives");
    drop(peer);
    assert_eq!(watched.read(&mut read).await.expect("the end"), 0);
    assert_eq!(kills.landed(), 1);
    assert_eq!(watched.read(&mut read).await.expect("the end, again"), 0);
    watched.write_all(b"d").await.expect_err("the peer is gone");
    assert_eq!(kills.landed(), 1, "a connection ends once");
}

#[tokio::test]
async fn a_connection_that_ended_before_its_kill_never_lands_it() {
    let kills = Kills::new();
    let (peer, stream) = tokio::io::duplex(64);
    let mut watched = kills.watch(stream, kills.next());
    drop(peer);
    let mut read = [0; 1];
    assert_eq!(watched.read(&mut read).await.expect("the end"), 0);
    kills.kill();
    assert_eq!(watched.read(&mut read).await.expect("the end, again"), 0);
    watched.write_all(b"x").await.expect_err("the peer is gone");
    assert_eq!((kills.count(), kills.landed()), (1, 0));
    // One awaiting a later kill is not landed by an earlier one.
    let (peer, stream) = tokio::io::duplex(64);
    let mut later = kills.watch(stream, kills.next());
    drop(peer);
    assert_eq!(later.read(&mut read).await.expect("the end"), 0);
    assert_eq!(kills.landed(), 0);
}

#[tokio::test]
async fn a_kill_lands_when_a_read_a_write_or_a_flush_fails_after_it() {
    type Fails = fn(&mut Box<dyn crate::network::Stream>) -> BoxFuture<'_, bool>;
    let fails: [Fails; 4] = [
        |stream| Box::pin(async move { stream.read(&mut [0; 1]).await.is_err() }),
        |stream| Box::pin(async move { stream.write(b"x").await.is_err() }),
        |stream| Box::pin(async move { stream.flush().await.is_err() }),
        |stream| {
            let slices = [std::io::IoSlice::new(b"x")];
            Box::pin(async move { stream.write_vectored(&slices).await.is_err() })
        },
    ];
    for failing in fails {
        let kills = Kills::new();
        let mut alive = kills.watch(Reset, kills.next());
        let mut killed = kills.watch(Reset, kills.next());
        // A connection that fails before any kill ended by itself.
        assert!(failing(&mut alive).await);
        assert_eq!(kills.landed(), 0);
        kills.kill();
        assert!(failing(&mut killed).await);
        assert_eq!(kills.landed(), 1);
        assert!(failing(&mut killed).await && failing(&mut alive).await);
        assert_eq!(kills.landed(), 1);
        // Shutting down a connection is no end a kill made.
        let mut closing = kills.watch(Reset, kills.next());
        kills.kill();
        closing.shutdown().await.expect("it shuts down");
        assert_eq!(kills.landed(), 1);
    }
}

#[tokio::test]
async fn a_severed_stream_lands_its_kill_when_the_cut_is_met() {
    let kills = Kills::new();
    let (_peer, stream) = tokio::io::duplex(64);
    let mut severed = kills.sever(stream);
    // A watched stream writes as its own does.
    let plain = tokio::io::join(tokio::io::empty(), tokio::io::sink());
    assert!(!kills.watch(plain, kills.next()).is_write_vectored());
    assert!(kills.watch(Reset, kills.next()).is_write_vectored());
    kills.kill();
    assert_eq!(kills.landed(), 0, "nothing has met the cut yet");
    severed
        .write_all(b"x")
        .await
        .expect_err("the stream is cut");
    assert_eq!(kills.landed(), 1);
    severed
        .read(&mut [0; 1])
        .await
        .expect_err("the stream is cut");
    assert_eq!(kills.landed(), 1);
}

#[tokio::test]
async fn a_kill_says_whether_it_landed_on_a_process_or_on_a_connection_it_cut() {
    let kills = Kills::new();
    let (_peer, stream) = tokio::io::duplex(64);
    let mut severed = kills.sever(stream);
    let (peer, stream) = tokio::io::duplex(64);
    let mut watched = kills.watch(stream, kills.next());
    kills.kill();
    assert_eq!((kills.landed(), kills.cut()), (0, 0));
    drop(peer);
    assert_eq!(watched.read(&mut [0; 1]).await.expect("the end"), 0);
    // A connection that ended by itself after the kill: what held its other end is gone.
    assert_eq!((kills.landed(), kills.cut()), (1, 0));
    severed
        .write_all(b"x")
        .await
        .expect_err("the stream is cut");
    // A connection this host cut: it says nothing of what held its other end.
    assert_eq!((kills.landed(), kills.cut()), (2, 1));
}
