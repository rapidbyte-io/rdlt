use rdlt_connector::{Cursor, Partition, PartitionId, PartitionState, ReadMode, StreamName};

use super::resumed;
use crate::error::{ErrorKind, codes};

fn partition(unbounded: bool) -> Partition {
    let declared = Partition::new(PartitionId::parse("p0").expect("a valid id"));
    if unbounded {
        declared.unbounded()
    } else {
        declared
    }
}

#[test]
fn a_partition_starts_from_its_cursor_or_its_beginning_whatever_its_read() {
    let stream = StreamName::new("s").expect("a valid name");
    let cursor = Cursor::encode(1, &5_u64).expect("a cursor encodes");
    for read in [ReadMode::Full, ReadMode::Incremental, ReadMode::Cdc] {
        for unbounded in [false, true] {
            let committed = PartitionState::Cursor(cursor.clone());
            let from = resumed(&stream, partition(unbounded), Some(&committed), read);
            let from = from.expect("a cursor resumes");
            assert_eq!(from.map(|(_, at)| at), Some(Some(cursor.clone())));
            let fresh = resumed(&stream, partition(unbounded), None, read).expect("it starts");
            assert_eq!(fresh.map(|(_, at)| at), Some(None));
        }
    }
}

#[test]
fn a_finished_partition_is_passed_over_unless_it_never_ends_and_the_read_is_not_full() {
    let stream = StreamName::new("s").expect("a valid name");
    let done = PartitionState::Done;
    for (read, unbounded, refused) in [
        (ReadMode::Full, false, false),
        (ReadMode::Full, true, false),
        (ReadMode::Incremental, false, false),
        (ReadMode::Incremental, true, true),
        (ReadMode::Cdc, false, false),
        (ReadMode::Cdc, true, true),
    ] {
        match resumed(&stream, partition(unbounded), Some(&done), read) {
            Ok(from) => {
                assert!(!refused, "{read:?} unbounded {unbounded}: not refused");
                assert!(from.is_none(), "{read:?} unbounded {unbounded}: read again");
            }
            Err(error) => {
                assert!(refused, "{read:?} unbounded {unbounded}: refused {error:?}");
                assert_eq!(error.code(), Some(codes::CURSOR_MISSING));
                assert_eq!(error.kind(), ErrorKind::Source);
                assert!(!error.is_retryable());
                assert_eq!(error.stream(), Some(&stream));
            }
        }
    }
}
