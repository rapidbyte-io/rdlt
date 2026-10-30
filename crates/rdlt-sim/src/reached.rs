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
    /// A run that follows the source was spared a refusal every other run must meet.
    Spared,
    /// A run followed the source for a while.
    Followed,
    /// A merge table's column widened over values written as another type.
    Widened,
    /// The destination's commit met a child table no row had reached.
    EmptyChild,
    /// A reset was refused as a stream the pipeline recorded nothing of.
    ResetRefused,
    /// A hostile host configured one connection twice at once.
    ConfiguredTwice,
    /// A hostile host opened sessions beyond a connection's share.
    SessionsBeyond,
    /// A connector started again under a version named after a secret.
    Renamed,
    /// A connector quoted one line of a secret of several lines alone.
    LineSaid,
    /// A plan named other partitions of a stream than the plan before it on one connection.
    Replanned,
    /// A connector that breaks the protocol was met as its fault says, on a healed network.
    Framing,
    /// A source that cannot read again served again rows a checkpoint had already sent, never
    /// told they were committed.
    ServedAgain,
    /// A report of committed cursors failed, which left its cursors owed to the next commit's.
    ReportOwed,
    /// A cursor owed to a partition recorded done was given up.
    OwedDropped,
    /// A commit the destination answered transiently was sent again in place.
    InPlaceRetried,
    /// A try of a commit answered transiently ended at the destination after a later commit
    /// landed, and was refused as stale.
    DelayedCopyRefused,
}

impl Reach {
    /// Every path, in the order a timing line names them.
    const ALL: [Self; 20] = [
        Self::ConnectorStopped,
        Self::LinkCut,
        Self::Crashed,
        Self::Refused,
        Self::Spared,
        Self::Followed,
        Self::Widened,
        Self::EmptyChild,
        Self::ResetRefused,
        Self::ConfiguredTwice,
        Self::SessionsBeyond,
        Self::Renamed,
        Self::LineSaid,
        Self::Replanned,
        Self::Framing,
        Self::ServedAgain,
        Self::ReportOwed,
        Self::OwedDropped,
        Self::InPlaceRetried,
        Self::DelayedCopyRefused,
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
