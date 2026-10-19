use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{Stalls, Withheld};
use crate::seed::{Seed, run};

/// An HTTP/2 frame of `kind` on `stream`, whose payload is `length` bytes of `fill`.
fn frame(kind: u8, stream: u32, length: u8, fill: u8) -> Vec<u8> {
    let mut frame = vec![0, 0, length, kind, 0];
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend(std::iter::repeat_n(fill, usize::from(length)));
    frame
}

#[test]
fn a_stalled_transport_drops_what_answers_a_later_call_and_writes_everything_else() {
    let (settings, ping) = (frame(0x4, 0, 6, 1), frame(0x6, 0, 8, 4));
    let (handshake, configured) = (frame(0x1, 1, 3, 2), frame(0x0, 3, 1, 3));
    let window = frame(0x8, 5, 4, 9);
    let withheld = [
        frame(0x1, 5, 4, 5),
        frame(0x9, 5, 2, 6),
        frame(0x0, 7, 2, 7),
        frame(0x3, 9, 4, 8),
    ];
    let given = [
        settings.clone(),
        handshake.clone(),
        withheld[0].clone(),
        ping.clone(),
        withheld[1].clone(),
        withheld[2].clone(),
        configured.clone(),
        withheld[3].clone(),
        window.clone(),
    ]
    .concat();
    let expected = [settings, handshake, ping, configured, window].concat();
    let (written, noted) = run(Seed::new(1), |_| async move {
        let (near, far) = tokio::io::duplex(1 << 16);
        let mut stalled = Withheld::new(near, Arc::new(Stalls::default()));
        // A few bytes at a time, so frames and their heads are split across writes.
        for piece in given.chunks(5) {
            stalled.write_all(piece).await.expect("the write goes");
        }
        stalled.flush().await.expect("the flush goes");
        let noted = stalled.since.is_some();
        drop(stalled);
        let mut written = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(1),
            far.take(1 << 16).read_to_end(&mut written),
        )
        .await
        .expect("the far end reads to its end")
        .expect("the read goes");
        (written, noted)
    });
    assert_eq!(written, expected);
    assert!(noted, "the first withheld frame is noted");
}

#[test]
fn a_stalled_connection_kept_open_past_the_patience_is_a_violation() {
    let judged = |kept: Duration| {
        run(Seed::new(1), move |_| async move {
            let stalls = Arc::new(Stalls::default());
            stalls.judge_by(Duration::from_millis(100));
            let (near, far) = tokio::io::duplex(1 << 10);
            let mut stalled = Withheld::new(near, Arc::clone(&stalls));
            stalled
                .write_all(&frame(0x1, 5, 4, 5))
                .await
                .expect("the write goes");
            tokio::time::sleep(kept).await;
            drop(far);
            let mut rest = Vec::new();
            tokio::time::timeout(Duration::from_secs(1), stalled.read_to_end(&mut rest))
                .await
                .expect("the end is read")
                .expect("the read goes");
            stalls.violations()
        })
    };
    assert_eq!(judged(Duration::from_millis(150)), Vec::<String>::new());
    let late = judged(Duration::from_millis(500));
    assert!(
        late.len() == 1 && late[0].contains("beyond its heartbeat's patience of 100ms"),
        "{late:?}"
    );
}
