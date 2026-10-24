//! A log in a local directory whose files a crash or a disk tore, cut, changed or grew: replayed
//! exactly, or refused with a typed error, never read as something it is not.

use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;

use bytes::Bytes;
use proptest::prelude::*;

use super::{FRAME_BYTES, load, logged, pipeline};
use crate::error::codes;
use crate::wal::frame::{HEAD, PREAMBLE, payload_len};
use crate::wal::memory::MemoryWal;
use crate::wal::scan::{Scanned, scan};
use crate::wal::{Chunk, LocalWal, WalStore};

/// What happens to one file of a log.
#[derive(Clone, Debug)]
enum Damage {
    /// Chunk `.0`, of the chunks in order, cut to `.1` thousandths of its length.
    Cut(usize, u16),
    /// Chunk `.0` cut to the `.1`th of its frame boundaries, wrapping, its end included.
    CutAtFrame(usize, usize),
    /// The byte `.1` thousandths into chunk `.0` changed by `.2`, never by nothing.
    Changed(usize, u16, u8),
    /// Bytes written after the end of chunk `.0`.
    Grown(usize, Vec<u8>),
    /// A file a crash left staging chunk `.0` with token `.1`, holding `.2`.
    Torn(u64, u64, Vec<u8>),
}

/// A position in thousandths, the first and the last of them as likely as the rest together.
fn permille() -> impl Strategy<Value = u16> {
    prop_oneof![Just(0), Just(1000), 0_u16..=1000]
}

fn damage() -> impl Strategy<Value = Damage> {
    prop_oneof![
        (0_usize..4, permille()).prop_map(|(chunk, permille)| Damage::Cut(chunk, permille)),
        (0_usize..4, any::<usize>()).prop_map(|(chunk, frame)| Damage::CutAtFrame(chunk, frame)),
        (0_usize..4, permille(), 1_u8..=255)
            .prop_map(|(chunk, permille, flip)| Damage::Changed(chunk, permille, flip)),
        (0_usize..4, proptest::collection::vec(any::<u8>(), 1..32))
            .prop_map(|(chunk, bytes)| Damage::Grown(chunk, bytes)),
        (
            0_u64..4,
            any::<u64>(),
            proptest::collection::vec(any::<u8>(), 0..64)
        )
            .prop_map(|(number, token, bytes)| Damage::Torn(number, token, bytes)),
    ]
}

/// `from`'s chunks of the scan tests' log, published to a log under `base`.
async fn published(from: &MemoryWal, base: &Path) -> LocalWal {
    let wal = LocalWal::new(base.join("logs"));
    for (chunk, bytes) in from.stored(&pipeline()) {
        match wal.open_log(&pipeline(), chunk.load).await {
            Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => {
                panic!("the log opens: {error}")
            }
            _ => {}
        }
        let mut staged = wal.stage(&pipeline(), chunk).await.expect("stages");
        staged.append(bytes).await.expect("appends");
        staged.publish().await.expect("publishes");
    }
    wal
}

/// Whether two scans read the same log: its header, tables, batches, commits and receipts.
fn same(one: &Scanned, other: &Scanned) -> bool {
    one.header == other.header
        && one.tables == other.tables
        && one.batches == other.batches
        && one.commits == other.commits
        && one.received == other.received
}

/// The offsets in `bytes`, a whole chunk, where its preamble ends, each frame ends and the chunk
/// ends.
fn boundaries(bytes: &[u8]) -> Vec<u64> {
    let mut at = PREAMBLE;
    let mut found = vec![at as u64];
    while at < bytes.len() {
        let payload = payload_len(&bytes[at..at + HEAD]).expect("a frame head");
        at += HEAD + usize::try_from(payload).expect("a payload that fits");
        found.push(at as u64);
    }
    found
}

/// Does `damage` to the files in `dir` of a log whose chunks are `chunks`.
fn damaged(dir: &Path, chunks: &[(Chunk, Bytes)], damage: &Damage) {
    let name = |at: usize| dir.join(format!("{:08}.wal", chunks[at % chunks.len()].0.number));
    let cut = |at: usize, to: &dyn Fn(u64) -> u64| {
        let path = name(at);
        let len = std::fs::metadata(&path).expect("a chunk").len();
        let opened = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("opens");
        opened.set_len(to(len)).expect("cuts");
    };
    match damage {
        Damage::Cut(at, permille) => cut(*at, &|len| len * u64::from(*permille) / 1000),
        Damage::CutAtFrame(at, frame) => {
            let found = boundaries(&chunks[at % chunks.len()].1);
            cut(*at, &|_| found[frame % found.len()]);
        }
        Damage::Changed(at, permille, flip) => {
            let path = name(*at);
            let mut bytes = std::fs::read(&path).expect("reads");
            if bytes.is_empty() {
                return;
            }
            let at = (bytes.len() - 1) * usize::from(*permille) / 1000;
            bytes[at] ^= flip;
            std::fs::write(&path, bytes).expect("writes");
        }
        Damage::Grown(at, extra) => {
            let mut opened = std::fs::OpenOptions::new()
                .append(true)
                .open(name(*at))
                .expect("opens");
            opened.write_all(extra).expect("grows");
        }
        Damage::Torn(number, token, bytes) => {
            let file = format!("{number:08}.{token:016x}.part");
            let mut staged = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(dir.join(file))
                .expect("a staging file");
            staged.write_all(bytes).expect("writes");
        }
    }
}

/// Whether every chunk file in `dir` holds the bytes of its chunk in `chunks`.
fn unchanged(dir: &Path, chunks: &[(Chunk, Bytes)]) -> bool {
    chunks.iter().all(|(chunk, bytes)| {
        std::fs::read(dir.join(format!("{:08}.wal", chunk.number))).is_ok_and(|read| read == *bytes)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(32)))]

    /// A local log whose chunk files are cut, changed or grown is refused as unreadable or
    /// foreign, and one that only gained the files a crash leaves staging is replayed exactly as
    /// it was written.
    #[test]
    fn a_torn_cut_or_changed_local_log_is_replayed_exactly_or_refused_typed(
        damages in proptest::collection::vec(damage(), 1..4),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        runtime.block_on(async {
            let whole = logged(false).await;
            let base = tempfile::tempdir().expect("a temporary directory");
            let local = published(&whole, base.path()).await;
            let expected = scan(whole.as_ref(), &pipeline(), load(), FRAME_BYTES)
                .await
                .expect("the log written reads");
            let copied = scan(&local, &pipeline(), load(), FRAME_BYTES)
                .await
                .expect("the log published reads");
            prop_assert!(same(&copied, &expected));
            let dir = local.pipeline_dir(&pipeline()).join(load().to_string());
            let chunks = whole.stored(&pipeline());
            for damage in &damages {
                damaged(&dir, &chunks, damage);
            }
            let whole_still = unchanged(&dir, &chunks);
            match scan(&local, &pipeline(), load(), FRAME_BYTES).await {
                Ok(read) => {
                    prop_assert!(whole_still, "a damaged log read: {:?}", damages);
                    prop_assert!(same(&read, &expected));
                }
                Err(error) => {
                    prop_assert!(!whole_still, "a whole log refused: {error}, {:?}", damages);
                    prop_assert!(
                        matches!(error.code(), Some(codes::WAL_UNREADABLE | codes::WAL_FOREIGN)),
                        "{error}"
                    );
                }
            }
            Ok(())
        })?;
    }
}
