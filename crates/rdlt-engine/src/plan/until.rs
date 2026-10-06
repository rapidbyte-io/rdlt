//! How long a run reads.

use std::time::Duration;

/// How long a run reads: until its source has caught up, forever, or for a while.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Until {
    /// Until each partition has caught up to where its source stood when its read started; then
    /// the run ends.
    #[default]
    Exhausted,
    /// Following every partition, waiting for more, until the run is stopped.
    Forever,
    /// Following every partition until this long after the run started; then the run commits
    /// what it read and ends.
    For(Duration),
}

impl Until {
    /// Whether reads of unbounded partitions follow them once caught up.
    pub fn follows(self) -> bool {
        !matches!(self, Self::Exhausted)
    }

    /// How long after it starts the run ends, where it ends at a deadline.
    pub fn deadline(self) -> Option<Duration> {
        match self {
            Self::For(duration) => Some(duration),
            Self::Exhausted | Self::Forever => None,
        }
    }
}
