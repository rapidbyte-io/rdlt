use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use super::Wire;

/// What a [`Recorder`] was given: its bytes, and how often it was flushed and shut down.
#[derive(Debug, Default, PartialEq, Eq)]
struct Record {
    written: Vec<u8>,
    flushes: u32,
    shutdowns: u32,
}

/// A stream that records every write, vectored or not, and says whether it writes vectored.
struct Recorder {
    record: Arc<Mutex<Record>>,
    vectored: bool,
}

impl Recorder {
    fn record(&self) -> std::sync::MutexGuard<'_, Record> {
        self.record.lock().expect("the record is never poisoned")
    }
}

impl AsyncRead for Recorder {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Recorder {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.record().written.extend_from_slice(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.record().flushes += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.record().shutdowns += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let mut record = self.record();
        let mut count = 0;
        for buffer in buffers {
            record.written.extend_from_slice(buffer);
            count += buffer.len();
        }
        Poll::Ready(Ok(count))
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
}

#[tokio::test]
async fn a_wire_reads_its_stream() {
    let (mut peer, inner) = tokio::io::duplex(64);
    let mut wire = Wire::new(Box::new(inner), None);
    peer.write_all(b"hello").await.expect("the peer writes");
    let mut read = [0; 5];
    wire.read_exact(&mut read).await.expect("the wire reads");
    assert_eq!(&read, b"hello");
    assert!(format!("{wire:?}").contains("spawned: false"), "{wire:?}");
}

#[tokio::test]
async fn a_wire_writes_its_stream_vectored_as_the_stream_writes() {
    for vectored in [true, false] {
        let record = Arc::new(Mutex::new(Record::default()));
        let recorder = Recorder {
            record: Arc::clone(&record),
            vectored,
        };
        let mut wire = Wire::new(Box::new(recorder), None);
        assert_eq!(wire.is_write_vectored(), vectored);
        let buffers = [
            std::io::IoSlice::new(b"one "),
            std::io::IoSlice::new(b"two"),
        ];
        assert_eq!(
            wire.write_vectored(&buffers)
                .await
                .expect("the wire writes"),
            7
        );
        wire.write_all(b" three").await.expect("the wire writes");
        wire.flush().await.expect("the wire flushes");
        wire.shutdown().await.expect("the wire shuts down");
        let expected = Record {
            written: b"one two three".to_vec(),
            flushes: 1,
            shutdowns: 1,
        };
        assert_eq!(*record.lock().expect("not poisoned"), expected);
    }
}
