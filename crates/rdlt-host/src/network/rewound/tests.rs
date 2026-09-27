use tokio::io::{AsyncReadExt as _, AsyncWrite as _, AsyncWriteExt as _};

use super::Rewound;
use crate::sink::Sink;

#[tokio::test]
async fn the_bytes_read_ahead_are_read_again_first_however_small_the_reads() {
    let (mut peer, inner) = tokio::io::duplex(64);
    peer.write_all(b" and the rest")
        .await
        .expect("the peer writes");
    drop(peer);
    let mut rewound = Rewound::new(b"first bytes".to_vec(), inner);
    let mut read = Vec::new();
    let mut piece = [0; 3];
    loop {
        let count = rewound.read(&mut piece).await.expect("the stream reads");
        if count == 0 {
            break;
        }
        read.extend_from_slice(&piece[..count]);
    }
    assert_eq!(read, b"first bytes and the rest");
}

#[tokio::test]
async fn writes_reach_the_stream_vectored_as_it_writes_them() {
    for vectored in [true, false] {
        let mut rewound = Rewound::new(Vec::new(), Sink::new(vectored));
        assert_eq!(rewound.is_write_vectored(), vectored);
        let buffers = [
            std::io::IoSlice::new(b"one "),
            std::io::IoSlice::new(b"two"),
        ];
        let written = rewound
            .write_vectored(&buffers)
            .await
            .expect("the stream writes");
        rewound
            .write_all(b" three")
            .await
            .expect("the stream writes");
        rewound.flush().await.expect("the stream flushes");
        rewound.shutdown().await.expect("the stream shuts down");
        assert_eq!(written, 7);
        assert_eq!(rewound.inner.written, b"one two three");
        assert_eq!((rewound.inner.flushes, rewound.inner.shutdowns), (1, 1));
    }
}
