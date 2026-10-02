//! The positions a host may report committed to a source that binds them: the checkpoints the
//! source sent that host, and where the host's latest read of each partition started.
//!
//! A source moves what it keeps outside the engine, a replication slot or a consumer group, when
//! its host reports a position committed. A checkpoint is the source's statement of a position.
//! Where a read started is the host's, which the source vouched for by accepting the read: it
//! is noted only then, and a source that started elsewhere says where with its first checkpoint.
//! A host is heard for either, of the same stream and partition, and for nothing else of its
//! choosing.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasher as _, RandomState};
use std::sync::{Mutex, PoisonError};

use crate::cursor::Cursor;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{PartitionId, StreamName};
use crate::limits::MAX_ACKNOWLEDGEABLE;

/// The code of the error a report is refused with, of a committed position its host was
/// neither sent nor asked to read from.
pub const POSITION_UNSENT: &str = "position_unsent";

/// The positions each host was sent or read from, remembered by a keyed hash of each.
///
/// They are remembered for as long as the value lives, a served connector's for its process, so
/// a host that dials again reports what it committed. Beyond a limit for each host the oldest
/// checkpoint is forgotten; where reads started is kept apart, a start for each partition.
pub struct Sent {
    /// Keyed at random for the process, so no host can choose a position that passes for another.
    hashing: RandomState,
    hosts: Mutex<BTreeMap<Option<String>, Positions>>,
    /// Positions: how many are remembered for one host.
    limit: usize,
}

impl std::fmt::Debug for Sent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Sent")
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl Default for Sent {
    fn default() -> Self {
        Self::remembering(MAX_ACKNOWLEDGEABLE)
    }
}

/// The positions of one host: how many times each checkpoint was noted and in what order, and
/// where the latest read of each partition started.
#[derive(Default)]
struct Positions {
    times: HashMap<u64, u32>,
    order: VecDeque<u64>,
    /// By partition, apart from the checkpoints, so that no number of those forgets a start.
    starts: HashMap<u64, u64>,
    /// The partitions with a start, oldest start first.
    started: VecDeque<u64>,
}

impl Sent {
    /// Remembers `limit` positions for each host, one at least; beyond them, the oldest is
    /// forgotten.
    pub fn remembering(limit: usize) -> Self {
        Self {
            hashing: RandomState::new(),
            hosts: Mutex::new(BTreeMap::new()),
            limit: limit.max(1),
        }
    }

    fn hash(&self, stream: &StreamName, partition: &PartitionId, cursor: &Cursor) -> u64 {
        self.hashing.hash_one((
            stream.namespace(),
            stream.name(),
            partition.as_str(),
            cursor.version(),
            &cursor.bytes()[..],
        ))
    }

    fn place(&self, stream: &StreamName, partition: &PartitionId) -> u64 {
        self.hashing
            .hash_one((stream.namespace(), stream.name(), partition.as_str()))
    }

    /// Remembers `cursor` of `partition` of `stream` for `host`, none where the host has no
    /// name: a checkpoint a read sent it.
    pub fn note(
        &self,
        host: Option<&str>,
        stream: &StreamName,
        partition: &PartitionId,
        cursor: &Cursor,
    ) {
        let hash = self.hash(stream, partition, cursor);
        let mut hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        let noted = hosts.entry(host.map(str::to_owned)).or_default();
        if noted.order.len() >= self.limit
            && let Some(oldest) = noted.order.pop_front()
            && let Some(times) = noted.times.get_mut(&oldest)
        {
            *times -= 1;
            if *times == 0 {
                noted.times.remove(&oldest);
            }
        }
        noted.order.push_back(hash);
        *noted.times.entry(hash).or_default() += 1;
    }

    /// Remembers that `host`'s read of `partition` of `stream` started from `cursor`, in place
    /// of where its last read of the partition started.
    ///
    /// It is for the caller to say so only once the source accepted the read there. Beyond the
    /// limit of partitions for a host, the start noted longest ago is forgotten.
    pub fn started(
        &self,
        host: Option<&str>,
        stream: &StreamName,
        partition: &PartitionId,
        cursor: &Cursor,
    ) {
        let place = self.place(stream, partition);
        let hash = self.hash(stream, partition, cursor);
        let mut hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        let noted = hosts.entry(host.map(str::to_owned)).or_default();
        noted.started.retain(|started| *started != place);
        if noted.started.len() >= self.limit
            && let Some(oldest) = noted.started.pop_front()
        {
            noted.starts.remove(&oldest);
        }
        noted.started.push_back(place);
        noted.starts.insert(place, hash);
    }

    /// Whether `cursor` of `partition` of `stream` is remembered for `host`.
    pub fn knows(
        &self,
        host: Option<&str>,
        stream: &StreamName,
        partition: &PartitionId,
        cursor: &Cursor,
    ) -> bool {
        let hash = self.hash(stream, partition, cursor);
        let place = self.place(stream, partition);
        let hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        hosts.get(&host.map(str::to_owned)).is_some_and(|noted| {
            noted.times.contains_key(&hash) || noted.starts.get(&place) == Some(&hash)
        })
    }

    /// Admits `host`'s report that `cursors` of `stream` are committed, where each is
    /// remembered for it.
    ///
    /// # Errors
    ///
    /// A transient error with the code [`POSITION_UNSENT`] where one is not: a source started
    /// again remembers nothing, and a host that reads again is heard for where its read starts.
    /// The report is refused whole.
    pub fn admit(
        &self,
        host: Option<&str>,
        stream: &StreamName,
        cursors: &[(PartitionId, Cursor)],
    ) -> Result<()> {
        let known = cursors
            .iter()
            .all(|(partition, cursor)| self.knows(host, stream, partition, cursor));
        if known {
            return Ok(());
        }
        let message = "a position reported committed is none this host was sent or read from";
        Err(ConnectorError::new(ConnectorErrorKind::Transient, message).with_code(POSITION_UNSENT))
    }
}
