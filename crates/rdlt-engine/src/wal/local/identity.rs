//! A local store's identity: a file in its base naming it, written once, by whoever asks first.

use std::ffi::OsStr;
use std::io::{self, Read as _, Write as _};
use std::sync::atomic::{AtomicU32, Ordering};

use rdlt_connector::LoadId;

use super::dir::Dir;

/// The name of the file in a base that names its store.
const IDENTITY: &str = "store";

/// Counts the identities the process proposes, so no two of its proposals take one name.
static PROPOSALS: AtomicU32 = AtomicU32::new(0);

/// The identity of the store whose base is `base`: what its file says, or `proposed`, written
/// whole and linked in where no file of that name exists, durably.
pub(super) fn identity(base: &Dir, proposed: LoadId) -> io::Result<LoadId> {
    if let Some(identity) = read(base)? {
        return Ok(identity);
    }
    let token =
        u64::from(std::process::id()) << 32 | u64::from(PROPOSALS.fetch_add(1, Ordering::Relaxed));
    let part = format!(".{IDENTITY}.{token:016x}.part");
    let mut file = base.create(&part)?;
    let written = file
        .write_all(proposed.to_string().as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| base.link(&part, IDENTITY));
    base.remove_file(OsStr::new(&part))?;
    match written {
        Ok(()) => base.sync().map(|()| proposed),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => read(base)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "the store's identity")),
        Err(error) => Err(error),
    }
}

/// What the identity file in `base` says, where there is one.
fn read(base: &Dir) -> io::Result<Option<LoadId>> {
    let Some(file) = base.open(IDENTITY)? else {
        return Ok(None);
    };
    let mut text = String::new();
    file.take(64).read_to_string(&mut text)?;
    let identity = text.trim().parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} does not name a store", base.at(IDENTITY).display()),
        )
    })?;
    Ok(Some(identity))
}
