//! Pushes gathered into batches of a useful size before they are shredded and written (spec
//! §7.3), so batch size does not mirror how a source chunks its data.

#[cfg(test)]
mod tests;

use std::time::Instant;

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::Permit;

use crate::config::BatchPolicy;

/// A push the coalescer takes.
pub(crate) enum Pushed {
    Json(Bytes),
    Arrow(RecordBatch),
}

/// Pushes gathered for one write, all of one kind.
#[derive(Debug)]
pub(crate) enum Unit {
    /// JSON pushes, shredded together.
    Json(Vec<Bytes>),
    /// Arrow batches of one schema, written as one.
    Arrow(Vec<RecordBatch>),
}

/// Pushes gathered, with the permits that hold their bytes.
pub(crate) struct Flushed {
    pub(crate) unit: Unit,
    pub(crate) permits: Vec<Permit>,
    /// Bytes of the pushes.
    pub(crate) bytes: u64,
}

/// Gathers a partition's pushes until a threshold of its [`BatchPolicy`] is reached.
pub(crate) struct Coalescer {
    policy: BatchPolicy,
    pending: Option<Pending>,
}

struct Pending {
    flushed: Flushed,
    rows: u64,
    since: Instant,
}

impl Coalescer {
    /// An empty coalescer following `policy`.
    pub(crate) fn new(policy: BatchPolicy) -> Self {
        Self {
            policy,
            pending: None,
        }
    }

    /// Adds `pushed`, which arrived at `now` holding `permit`; returns what to write first, in
    /// order: the pushes gathered before when `pushed` cannot join them, and every push gathered
    /// once a threshold is reached.
    pub(crate) fn add(&mut self, pushed: Pushed, permit: Permit, now: Instant) -> Vec<Flushed> {
        let mut flushed = Vec::new();
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| !joins(&pending.flushed.unit, &pushed))
        {
            flushed.extend(self.flush());
        }
        // A JSON push's rows are only known once it is shredded, so JSON counts by its bytes.
        let (bytes, rows) = match &pushed {
            Pushed::Json(json) => (json.len(), 0),
            Pushed::Arrow(batch) => (batch.get_array_memory_size(), batch.num_rows()),
        };
        let (bytes, rows) = (count(bytes), count(rows));
        let pending = self.pending.get_or_insert_with(|| Pending {
            flushed: Flushed {
                unit: match pushed {
                    Pushed::Json(_) => Unit::Json(Vec::new()),
                    Pushed::Arrow(_) => Unit::Arrow(Vec::new()),
                },
                permits: Vec::new(),
                bytes: 0,
            },
            rows: 0,
            since: now,
        });
        match (&mut pending.flushed.unit, pushed) {
            (Unit::Json(gathered), Pushed::Json(json)) => gathered.push(json),
            (Unit::Arrow(gathered), Pushed::Arrow(batch)) => gathered.push(batch),
            _ => unreachable!("a push joins only pushes of its kind"),
        }
        pending.flushed.permits.push(permit);
        pending.flushed.bytes += bytes;
        pending.rows += rows;
        if pending.flushed.bytes >= self.policy.target_bytes().get()
            || pending.rows >= self.policy.max_rows().get()
        {
            flushed.extend(self.flush());
        }
        flushed
    }

    /// When the pushes gathered have waited long enough to be written anyway.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.pending
            .as_ref()
            .map(|pending| pending.since + self.policy.max_latency())
    }

    /// Every push gathered, if any.
    pub(crate) fn flush(&mut self) -> Option<Flushed> {
        self.pending.take().map(|pending| pending.flushed)
    }
}

/// Whether `pushed` may join the pushes of `unit`: JSON joins JSON, and Arrow joins Arrow of the
/// same schema.
fn joins(unit: &Unit, pushed: &Pushed) -> bool {
    match (unit, pushed) {
        (Unit::Json(_), Pushed::Json(_)) => true,
        (Unit::Arrow(batches), Pushed::Arrow(batch)) => batches
            .first()
            .is_none_or(|first| first.schema() == batch.schema()),
        _ => false,
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
