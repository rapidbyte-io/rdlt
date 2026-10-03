//! The limits an engine's configuration admits within, and the least memory it may be given.

use rdlt_wire::Limits;

use super::{EngineConfig, EngineConfigBuilder};
use crate::budget::{Shares, admitted, least};
use crate::error::Error;
use crate::limits::{MEMORY_BELOW_MINIMUM, TABLE_RECORDS};

impl EngineConfig {
    /// The limits on what connectors send that this configuration admits: the lesser, for each
    /// limit, of the limit configured and of what the memory budget's shares hold.
    ///
    /// A host advertises them at its handshake, so a connector cuts and bounds what it sends to
    /// what will be admitted, and a source in the engine's process is held to them where it
    /// emits. The engine refuses for its budget only what passes them.
    pub fn limits(&self) -> Limits {
        let shares = Shares::of(self.memory.get());
        self.limits.lesser(&admitted(shares, self.partitions.get()))
    }

    /// Bytes: the most a pipeline's stored state may hold decoded in an open's answer, and a
    /// commit's request: [`Limits::state_bytes`] of [`EngineConfig::limits`], which the host
    /// advertises and the control share of the memory budget holds.
    pub fn state_limit(&self) -> u64 {
        self.limits().state_bytes
    }

    /// Tables: the most child tables a normalized stream's table may have: the lesser of
    /// [`GrowthLimits::child_tables`](super::GrowthLimits::child_tables) and as many tables of a
    /// few columns as the stored state limit holds, so their records never take it all.
    pub fn child_table_limit(&self) -> usize {
        let held = usize::try_from(self.state_limit() / TABLE_RECORDS).unwrap_or(usize::MAX);
        self.growth.child_tables().get().min(held.max(1))
    }

    /// Bytes: the least memory a configuration reading `partitions` partitions at once may have,
    /// below which its budget admits less than the protocol lets a peer go.
    pub fn least_memory(partitions: usize) -> u64 {
        least(partitions)
    }

    /// Checks that the memory budget admits the protocol's least frame.
    ///
    /// # Errors
    ///
    /// A `Config` error coded `memory_below_minimum`, naming the least memory that does.
    pub(super) fn admit_memory(&self) -> Result<(), Error> {
        let (memory, partitions) = (self.memory.get(), self.partitions.get());
        let least = least(partitions);
        if memory >= least {
            return Ok(());
        }
        Err(Error::config(format!(
            "memory of {memory} bytes admits less than the protocol's least frame with \
             {partitions} partitions read at once: the least memory that does is {least} bytes"
        ))
        .with_code(MEMORY_BELOW_MINIMUM))
    }
}

impl EngineConfigBuilder {
    /// The limits on what connectors send (default: the protocol's); each is lowered to what
    /// the memory budget admits, as [`EngineConfig::limits`] gives them.
    #[must_use]
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = Some(limits);
        self
    }
}
