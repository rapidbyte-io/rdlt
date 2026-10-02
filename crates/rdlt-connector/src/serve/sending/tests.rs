use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::Sending;

const SEND: Duration = Duration::from_secs(10);

#[tokio::test(start_paused = true)]
async fn a_write_its_peer_takes_nothing_of_fails_at_the_send_wait() {
    let (connection, mut peer) = tokio::io::duplex(1024);
    let mut sending = Sending::within(connection, SEND);
    // The pipe takes what it holds, and then the peer takes nothing.
    sending.write_all(&[0; 1024]).await.unwrap();
    let started = tokio::time::Instant::now();
    let error = sending.write_all(&[0; 1024]).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(started.elapsed(), SEND);
    let mut taken = [0; 1024];
    peer.read_exact(&mut taken).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_peer_that_takes_a_little_at_a_time_keeps_its_writes_going() {
    let (connection, mut peer) = tokio::io::duplex(1024);
    let mut sending = Sending::within(connection, SEND);
    let taking = tokio::spawn(async move {
        let mut taken = vec![0; 8 * 1024];
        for chunk in taken.chunks_mut(512) {
            tokio::time::sleep(SEND / 2).await;
            peer.read_exact(chunk).await.unwrap();
        }
        taken.len()
    });
    // Each write waits less than the send wait for the peer to take some.
    sending.write_all(&[1; 8 * 1024]).await.unwrap();
    sending.flush().await.unwrap();
    assert_eq!(taking.await.unwrap(), 8 * 1024);
}
