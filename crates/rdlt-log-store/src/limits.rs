//! Limits the log store's configuration holds an object store to.

use std::time::Duration;

/// How long credentials resolved from secrets are used before they are resolved again, so a
/// rotated secret is taken up: five minutes.
pub const CREDENTIALS_FRESH: Duration = Duration::from_secs(300);

/// How long opening a connection to the object store may take: ten seconds.
pub const CONNECT: Duration = Duration::from_secs(10);

/// Bytes: the shortest part of an upload S3 takes, every part but the last, 5 MiB.
pub const PART_BYTES_LEAST: u64 = 5 << 20;

/// Bytes: the longest part of an upload S3 takes, 5 GiB.
pub const PART_BYTES_MOST: u64 = 5 << 30;

/// Bytes: what is read of the body of a store's refusal, to tell one that passes from one that
/// does not.
pub const REFUSAL_BYTES: usize = 4 << 10;
