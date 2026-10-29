use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{CHUNK, Piped, Sent, pipe};

/// Runs `work` on a runtime of its own.
fn running<F: Future>(work: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime builds")
        .block_on(work)
}

/// The two ends of a data stream over HTTP/2, each reading the body the other sends.
fn ends() -> (Piped<Sent>, Piped<Sent>) {
    let ((first, first_sent), (second, second_sent)) = (pipe(), pipe());
    (first.reading(second_sent), second.reading(first_sent))
}

#[test]
fn a_write_longer_than_a_chunk_crosses_a_chunk_at_a_time_and_whole() {
    running(async {
        let (mut writing, mut reading) = ends();
        let sent: Vec<u8> = (0..CHUNK * 3 + 7)
            .map(|at| u8::try_from(at % 251).expect("a byte"))
            .collect();
        let written = writing.write(&sent).await.expect("the other end reads");
        assert_eq!(written, CHUNK, "one write takes one chunk at most");
        let rest = async {
            writing.write_all(&sent[written..]).await?;
            writing.shutdown().await
        };
        let mut read = Vec::new();
        let (rest, all) = tokio::join!(rest, reading.read_to_end(&mut read));
        rest.expect("the rest is written");
        all.expect("the stream ends once shut");
        assert!(read == sent, "{} bytes read of {}", read.len(), sent.len());
    });
}

#[test]
fn a_write_after_the_other_end_went_fails_as_a_broken_pipe() {
    running(async {
        let (mut writing, reading) = ends();
        drop(reading);
        let failed = writing.write(b"frame").await.expect_err("nothing reads it");
        assert_eq!(failed.kind(), std::io::ErrorKind::BrokenPipe);
    });
}

/// A body that fails as it is read.
struct Failing;

impl Body for Failing {
    type Data = Bytes;
    type Error = &'static str;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, &'static str>>> {
        Poll::Ready(Some(Err("the call was reset")))
    }
}

#[test]
fn a_body_that_fails_fails_the_read_saying_why() {
    running(async {
        let (end, _sent) = pipe();
        let mut reading = end.reading(Failing);
        let mut buffer = [0; 16];
        let failed = reading
            .read(&mut buffer)
            .await
            .expect_err("the body failed");
        assert!(
            failed.to_string().contains("the call was reset"),
            "{failed}"
        );
    });
}
