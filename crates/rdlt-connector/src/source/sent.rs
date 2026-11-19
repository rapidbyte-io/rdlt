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

use parking_lot::Mutex;

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
/// checkpoint is forgotten; each partition's newest checkpoint and where its latest read started
/// are kept apart, so a partition that went quiet is heard for where it stands however many
/// checkpoints other partitions send.
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
/// for each partition its newest checkpoint and where its latest read started.
#[derive(Default)]
struct Positions {
    times: HashMap<u64, u32>,
    order: VecDeque<u64>,
    /// By partition, apart from the checkpoints, so that no number of those forgets either.
    latest: HashMap<u64, Latest>,
    /// The partitions kept, in the order their latest read started or, for a partition never
    /// read from a start, its first checkpoint was noted.
    kept: VecDeque<u64>,
}

/// A partition's newest checkpoint and where its latest read started, each a position's hash.
#[derive(Clone, Copy, Default)]
struct Latest {
    checkpoint: Option<u64>,
    start: Option<u64>,
}

impl Positions {
    /// Keeps `latest` for the partition `place`, last among the partitions kept, forgetting the
    /// first of them beyond `limit`.
    fn keep(&mut self, place: u64, latest: Latest, limit: usize) {
        if self.kept.len() >= limit
            && let Some(oldest) = self.kept.pop_front()
        {
            self.latest.remove(&oldest);
        }
        self.kept.push_back(place);
        self.latest.insert(place, latest);
    }
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
    /// name: a checkpoint a read sent it, the partition's newest.
    pub fn note(
        &self,
        host: Option<&str>,
        stream: &StreamName,
        partition: &PartitionId,
        cursor: &Cursor,
    ) {
        let place = self.place(stream, partition);
        let hash = self.hash(stream, partition, cursor);
        let mut hosts = self.hosts.lock();
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
        if let Some(latest) = noted.latest.get_mut(&place) {
            latest.checkpoint = Some(hash);
        } else {
            let latest = Latest {
                checkpoint: Some(hash),
                start: None,
            };
            noted.keep(place, latest, self.limit);
        }
    }

    /// Remembers that `host`'s read of `partition` of `stream` started from `cursor`, in place
    /// of where its last read of the partition started.
    ///
    /// It is for the caller to say so only once the source accepted the read there. Beyond the
    /// limit of partitions for a host, the partition whose read started, or that was first sent
    /// a checkpoint, longest ago is forgotten.
    pub fn started(
        &self,
        host: Option<&str>,
        stream: &StreamName,
        partition: &PartitionId,
        cursor: &Cursor,
    ) {
        let place = self.place(stream, partition);
        let hash = self.hash(stream, partition, cursor);
        let mut hosts = self.hosts.lock();
        let noted = hosts.entry(host.map(str::to_owned)).or_default();
        noted.kept.retain(|kept| *kept != place);
        let latest = Latest {
            start: Some(hash),
            ..noted.latest.remove(&place).unwrap_or_default()
        };
        noted.keep(place, latest, self.limit);
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
        let hosts = self.hosts.lock();
        hosts.get(&host.map(str::to_owned)).is_some_and(|noted| {
            let latest = noted.latest.get(&place).copied().unwrap_or_default();
            noted.times.contains_key(&hash)
                || latest.checkpoint == Some(hash)
                || latest.start == Some(hash)
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
