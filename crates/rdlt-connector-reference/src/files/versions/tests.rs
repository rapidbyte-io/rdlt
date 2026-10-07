use std::io::{self, ErrorKind};

use super::{name, newest_read};
use crate::rooted::Dir;

#[test]
fn a_version_removed_once_listed_is_superseded_and_any_other_failure_is_reported() {
    let base = crate::scratch::tempdir().unwrap();
    for version in [1, 2] {
        std::fs::write(base.path().join(name(version)), version.to_string()).unwrap();
    }
    let dir = Dir::ambient(base.path()).unwrap();
    // Version 2 is removed between its listing and its read, as a newer one superseded it.
    let mut reads = Vec::new();
    let read = newest_read(&dir, |version| {
        reads.push(version);
        if version == 2 {
            std::fs::remove_file(base.path().join(name(2))).unwrap();
            return Err(ErrorKind::NotFound.into());
        }
        std::fs::read(base.path().join(name(version)))
    });
    assert_eq!(read.unwrap(), Some((1, b"1".to_vec())));
    assert_eq!(reads, [2, 1]);
    // A read that fails otherwise is the answer, and not tried again.
    let mut tries = 0;
    let failed = newest_read(&dir, |_| {
        tries += 1;
        Err(io::Error::other("the disk failed"))
    });
    assert_eq!(failed.unwrap_err().kind(), ErrorKind::Other);
    assert_eq!(tries, 1);
}
