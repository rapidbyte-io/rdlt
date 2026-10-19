use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{Stalls, Withheld};
use crate::seed::{Seed, run};

/// How long a test waits on the paused clock for a write, a flush or a read to end.
const WAIT: Duration = Duration::from_secs(1);

/// An HTTP/2 frame of `kind` on `stream`, whose payload is `length` bytes of `fill`.
fn frame(kind: u8, stream: u32, length: usize, fill: u8) -> Vec<u8> {
    let [_, high, middle, low] = u32::try_from(length)
        .ok()
        .filter(|length| *length < 1 << 24)
        .expect("a frame's length fits in its head's three bytes")
        .to_be_bytes();
    let mut frame = vec![high, middle, low, kind, 0];
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend(std::iter::repeat_n(fill, length));
    frame
}

#[test]
fn a_stalled_transport_drops_what_answers_a_later_call_and_writes_everything_else() {
    let mut acknowledged = frame(0x4, 0, 0, 0);
    acknowledged[4] = 0x1;
    // Each frame as given, and whether it is written on.
    let frames = [
        (frame(0x4, 0, 6, 1), true),
        (frame(0x1, 1, 3, 2), true),
        (frame(0x1, 5, 4, 5), false),
        (frame(0x6, 0, 8, 4), true),
        (frame(0x9, 5, 2, 6), false),
        (frame(0x0, 7, 2, 7), false),
        (frame(0x0, 3, 1, 3), true),
        (frame(0x3, 9, 4, 8), false),
        // Longer than one byte of the head can say.
        (frame(0x0, 7, 300, 10), false),
        // A settings acknowledgement, whose head ends the frame.
        (acknowledged, true),
        // The stream's reserved bit is not part of its number, so this answers the configuration.
        (frame(0x1, 0x8000_0003, 4, 11), true),
        (frame(0x8, 5, 4, 9), true),
    ];
    let given: Vec<u8> = frames.iter().flat_map(|(frame, _)| frame.clone()).collect();
    let expected: Vec<u8> = frames
        .iter()
        .filter(|(_, kept)| *kept)
        .flat_map(|(frame, _)| frame.clone())
        .collect();
    let (written, noted) = run(Seed::new(1), |_| async move {
        let (near, far) = tokio::io::duplex(1 << 16);
        let mut stalled = Withheld::new(near, Arc::new(Stalls::default()));
        // A few bytes at a time, so frames and their heads are split across writes.
        for piece in given.chunks(5) {
            tokio::time::timeout(WAIT, stalled.write_all(piece))
                .await
                .expect("the write ends")
                .expect("the write goes");
        }
        tokio::time::timeout(WAIT, stalled.flush())
            .await
            .expect("the flush ends")
            .expect("the flush goes");
        let noted = stalled.since.is_some();
        drop(stalled);
        let mut written = Vec::new();
        tokio::time::timeout(WAIT, far.take(1 << 16).read_to_end(&mut written))
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
            tokio::time::timeout(WAIT, stalled.write_all(&frame(0x1, 5, 4, 5)))
                .await
                .expect("the write ends")
                .expect("the write goes");
            tokio::time::sleep(kept).await;
            drop(far);
            let mut rest = Vec::new();
            tokio::time::timeout(WAIT, (&mut stalled).take(1 << 10).read_to_end(&mut rest))
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
