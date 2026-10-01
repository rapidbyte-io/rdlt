//! The positions a host may report committed: the checkpoints the reads of this process sent it.
//!
//! A source moves what it keeps outside the engine, a replication slot or a consumer group, when
//! its host reports a position committed. A host is therefore heard only for a checkpoint that a
//! read of the same stream and partition sent that host. The checkpoints are remembered for the
//! process, not for a connection: a host that dials again reports what it committed.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasher as _, RandomState};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use rdlt_wire::tonic::Status;
use rdlt_wire::tonic::codegen::tokio_stream::Stream;

use super::service::Answer;
use crate::limits::MAX_ACKNOWLEDGEABLE;
use crate::wire::v1;

/// The checkpoints sent to each host, remembered by a keyed hash of each.
pub(super) struct Acknowledgeable {
    /// Keyed at random for the process, so no host can choose a position that passes for another.
    hashing: RandomState,
    hosts: Mutex<BTreeMap<Option<Arc<str>>, Sent>>,
    /// Checkpoints: how many are remembered for one host.
    limit: usize,
}

impl Default for Acknowledgeable {
    fn default() -> Self {
        Self::remembering(MAX_ACKNOWLEDGEABLE)
    }
}

/// The checkpoints sent to one host: how many times each, and in what order.
#[derive(Default)]
struct Sent {
    times: HashMap<u64, u32>,
    order: VecDeque<u64>,
}

/// A stream and one of its partitions, as a read or a report names them.
#[derive(Clone, Debug)]
pub(super) struct Read {
    /// The stream's namespace, where it has one, and its name.
    pub(super) stream: (Option<String>, String),
    pub(super) partition: String,
}

impl Acknowledgeable {
    /// Remembers `limit` checkpoints for each host, one at least; beyond them, the oldest is
    /// forgotten.
    pub(super) fn remembering(limit: usize) -> Self {
        Self {
            hashing: RandomState::new(),
            hosts: Mutex::new(BTreeMap::new()),
            limit: limit.max(1),
        }
    }

    fn hash(&self, read: &Read, cursor: &v1::Cursor) -> u64 {
        self.hashing.hash_one((
            &read.stream,
            &read.partition,
            cursor.version,
            &cursor.bytes[..],
        ))
    }

    /// Remembers that `read` sent `host` the checkpoint `cursor`.
    pub(super) fn sent(&self, host: Option<&Arc<str>>, read: &Read, cursor: &v1::Cursor) {
        let hash = self.hash(read, cursor);
        let mut hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        let sent = hosts.entry(host.cloned()).or_default();
        if sent.order.len() >= self.limit
            && let Some(oldest) = sent.order.pop_front()
            && let Some(times) = sent.times.get_mut(&oldest)
        {
            *times -= 1;
            if *times == 0 {
                sent.times.remove(&oldest);
            }
        }
        sent.order.push_back(hash);
        *sent.times.entry(hash).or_default() += 1;
    }

    /// Whether a read of `read`'s stream and partition sent `host` the checkpoint `cursor`, and
    /// it is remembered.
    pub(super) fn was_sent(
        &self,
        host: Option<&Arc<str>>,
        read: &Read,
        cursor: &v1::Cursor,
    ) -> bool {
        let hash = self.hash(read, cursor);
        let hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        hosts
            .get(&host.cloned())
            .is_some_and(|sent| sent.times.contains_key(&hash))
    }
}

/// A read's frames, each checkpoint among them remembered as sent to the read's host as it
/// passes.
pub(super) struct Noted {
    frames: Answer<v1::ReadFrame>,
    served: Arc<super::Served>,
    host: Option<Arc<str>>,
    read: Read,
}

impl Noted {
    pub(super) fn new(
        frames: Answer<v1::ReadFrame>,
        served: Arc<super::Served>,
        host: Option<Arc<str>>,
        read: Read,
    ) -> Self {
        Self {
            frames,
            served,
            host,
            read,
        }
    }
}

impl Stream for Noted {
    type Item = Result<v1::ReadFrame, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let frame = self.frames.as_mut().poll_next(context);
        if let Poll::Ready(Some(Ok(v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Checkpoint(checkpoint)),
        }))) = &frame
            && let Some(cursor) = &checkpoint.cursor
        {
            self.served
                .acknowledgeable
                .sent(self.host.as_ref(), &self.read, cursor);
        }
        frame
    }
}
