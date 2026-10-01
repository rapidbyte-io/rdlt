//! Every limit the certification of a connector over the wire keeps, beside those of
//! `rdlt_connector::testing`.

use std::time::Duration;

/// The longest a read-back of one table takes: one that takes longer fails the clause, rather
/// than hold the certification.
pub(crate) const READ_BACK_TIME: Duration = Duration::from_secs(30);

/// The most a read-back decodes of one table, in bytes of batch frames: a destination that sends
/// more fails the clause, rather than size this process's memory.
pub(crate) const PUBLISHED_BYTES: usize = 64 << 20;

/// The most rows a read-back decodes of one table: rows of a bit, or of nothing, cost no bytes,
/// so they are counted too, and no clause reads back a fiftieth as many.
pub(crate) const PUBLISHED_ROWS: usize = 100_000;
