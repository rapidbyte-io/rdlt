//! How a stream's rows reach its table.

use rdlt_connector::Fills;

/// How a stream's rows reach its table.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Insert every row.
    Append,
    /// Fill a hidden generation of the table and swap it in once the stream is fully read.
    Replace,
    /// Upsert by key: a row replaces the table's row with the same key.
    Merge,
    /// Keep every version of each key: a row that changes its key's data closes the
    /// key's current version and becomes the current one.
    History,
}

impl WriteMode {
    /// Where a full read's rows land as they commit: its generation for a replace stream, the
    /// table for any other.
    pub(crate) fn fills(self) -> Fills {
        match self {
            Self::Replace => Fills::Generation,
            Self::Append | Self::Merge | Self::History => Fills::Table,
        }
    }
}
