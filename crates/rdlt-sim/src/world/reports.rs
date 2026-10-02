//! What a simulated source remembers of the reports it may hear, as a served connector does,
//! and what it was told: the oracle checks every committed position reached it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use rdlt_connector::{
    ConnectorError, ConnectorErrorKind, Cursor, PartitionId, Result, Sent, StreamName,
};
use serde::Serialize;

/// The cursor format every simulated stream writes.
const VERSION: u16 = 1;

/// A source's memory of what its host may report committed, and its record of what it heard.
#[derive(Debug, Default)]
pub(crate) struct Reports {
    /// The checkpoints the source sent and where the reads it accepted started, since it was
    /// last started: a host is heard for these alone, as a served connector hears it.
    sent: Mutex<Arc<Sent>>,
    /// The partitions read since the source was last started, by stream and partition.
    read: Mutex<BTreeSet<(String, String)>>,
    /// The position each partition was last told is committed, by stream and partition.
    told: Mutex<BTreeMap<(String, String), u64>>,
    /// Whether the source refuses every report.
    refusing: AtomicBool,
    /// How many reports it refused while it did.
    refused: AtomicU64,
}

impl Reports {
    /// Starts the source again, as a connector spawned for each run is: it remembers nothing it
    /// sent, and no partition has been read.
    pub(crate) fn restart(&self) {
        *self.sent.lock() = Arc::default();
        self.read.lock().clear();
    }

    /// Notes that the source accepted a read of `partition` of `stream` from `cursor`, none
    /// where its host gave none: as a served connector, it is then heard for that start.
    pub(crate) fn started<C: Serialize>(
        &self,
        stream: &str,
        partition: &PartitionId,
        cursor: Option<&C>,
    ) {
        let key = (stream.to_owned(), partition.to_string());
        self.read.lock().insert(key);
        let Some(cursor) = cursor else {
            return;
        };
        let name = StreamName::new(stream).expect("valid stream name");
        let cursor = Cursor::encode(VERSION, cursor).expect("a simulated cursor encodes");
        self.sent.lock().started(None, &name, partition, &cursor);
    }

    /// Notes that a read of `partition` of `stream` sends the checkpoint `cursor`.
    pub(crate) fn note<C: Serialize>(&self, stream: &str, partition: &PartitionId, cursor: &C) {
        let name = StreamName::new(stream).expect("valid stream name");
        let cursor = Cursor::encode(VERSION, cursor).expect("a simulated cursor encodes");
        self.sent.lock().note(None, &name, partition, &cursor);
    }

    /// Hears the report that `position`, encoded as `cursor`, of `partition` of `stream` is
    /// committed.
    ///
    /// # Errors
    ///
    /// The report is refused where the source was neither asked to read from the position nor
    /// sent it, and where the source refuses every report.
    pub(crate) fn hear<C: Serialize>(
        &self,
        stream: &str,
        partition: &PartitionId,
        cursor: &C,
        position: u64,
    ) -> Result<()> {
        let name = StreamName::new(stream).expect("valid stream name");
        let encoded = Cursor::encode(VERSION, cursor).expect("a simulated cursor encodes");
        let sent = Arc::clone(&self.sent.lock());
        sent.admit(None, &name, &[(partition.clone(), encoded)])?;
        if self.refusing.load(Ordering::SeqCst) {
            self.refused.fetch_add(1, Ordering::SeqCst);
            let message = "the source refuses every report";
            return Err(ConnectorError::new(ConnectorErrorKind::Transient, message));
        }
        let key = (stream.to_owned(), partition.to_string());
        self.told.lock().insert(key, position);
        Ok(())
    }

    /// Makes the source refuse every report, or hear them again; answers how many it refused
    /// since it was last asked.
    pub(crate) fn refuse(&self, refusing: bool) -> u64 {
        self.refusing.store(refusing, Ordering::SeqCst);
        self.refused.swap(0, Ordering::SeqCst)
    }

    /// The partitions read since the source was last started, each with the position it was
    /// last told is committed.
    pub(crate) fn read(&self) -> Vec<((String, String), Option<u64>)> {
        let told = self.told.lock();
        let read = self.read.lock();
        read.iter()
            .map(|key| (key.clone(), told.get(key).copied()))
            .collect()
    }
}
