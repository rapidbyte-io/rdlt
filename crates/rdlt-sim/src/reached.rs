//! The paths a simulation's runs reached, so a seed kept for a defect it found can say where the
//! defect was, and fail where the simulation has grown around it and it no longer gets there.

/// A path a seed's runs may reach, each where a kept seed's defect was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Reach {
    /// The network's fault driver stopped a connector while a run lasted.
    ConnectorStopped,
    /// The network's fault driver cut or held the link to a connector while a run lasted.
    LinkCut,
    /// A run's worker crashed part way.
    Crashed,
    /// A run met a refusal the model predicts.
    Refused,
    /// A run ran only to replay a log the runs before it left.
    Replayed,
    /// A run began with a log the runs before it left, while others were still to run.
    LogLeft,
    /// A run that follows the source was spared a refusal every other run must meet.
    Spared,
    /// A run followed the source for a while.
    Followed,
    /// A run waited for room in the log's share of the budget.
    LogWaited,
    /// A merge table's column widened over values written as another type.
    Widened,
    /// The destination's commit met a child table no row had reached.
    EmptyChild,
    /// A reset was refused as a stream the pipeline recorded nothing of.
    ResetRefused,
    /// A put or deletion of the logs' object store was drawn to land after its client gave up.
    LandedLate,
    /// A hostile host configured one connection twice at once.
    ConfiguredTwice,
    /// A hostile host opened sessions beyond a connection's share.
    SessionsBeyond,
    /// A connector started again under a version named after a secret.
    Renamed,
    /// A connector quoted one line of a secret of several lines alone.
    LineSaid,
}

impl Reach {
    /// Every path, in the order a timing line names them.
    const ALL: [Self; 17] = [
        Self::ConnectorStopped,
        Self::LinkCut,
        Self::Crashed,
        Self::Refused,
        Self::Replayed,
        Self::LogLeft,
        Self::Spared,
        Self::Followed,
        Self::LogWaited,
        Self::Widened,
        Self::EmptyChild,
        Self::ResetRefused,
        Self::LandedLate,
        Self::ConfiguredTwice,
        Self::SessionsBeyond,
        Self::Renamed,
        Self::LineSaid,
    ];

    fn bit(self) -> u32 {
        1 << u32::from(self as u8)
    }
}

/// The paths a simulation's runs reached.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Reached(u32);

impl Reached {
    /// Adds `reach`.
    pub(crate) fn add(&mut self, reach: Reach) {
        self.0 |= reach.bit();
    }

    /// The paths either reached.
    pub(crate) fn with(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Whether the runs reached `reach`.
    pub fn has(self, reach: Reach) -> bool {
        self.0 & reach.bit() != 0
    }

    /// The name of every path reached, for a timing line.
    pub(crate) fn names(self) -> Vec<String> {
        Reach::ALL
            .into_iter()
            .filter(|reach| self.has(*reach))
            .map(|reach| format!("{reach:?}"))
            .collect()
    }
}
