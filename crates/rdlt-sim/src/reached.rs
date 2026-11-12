//! The paths a simulation's runs reached, so a seed kept for a defect it found can say where the
//! defect was, and fail where the simulation has grown around it and it no longer gets there; and
//! the ways its streams were read and written that its checks held, so a long sweep fails where
//! one it must reach is drawn no more.

use crate::workload::Combination;

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
    /// A stream refused `cursor_missing` was reset with its tables, as the refusal says, and read
    /// again from its beginning.
    Remedied,
    /// A plain run without faults held a read of more than a checkpoint group, which a run with
    /// them may see refused, to meet no refusal.
    Exact,
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
    const ALL: [Self; 22] = [
        Self::ConnectorStopped,
        Self::LinkCut,
        Self::Crashed,
        Self::Refused,
        Self::Spared,
        Self::Followed,
        Self::Widened,
        Self::EmptyChild,
        Self::ResetRefused,
        Self::Remedied,
        Self::Exact,
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

/// The paths a simulation's runs reached, and the combinations its checks held.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Reached {
    paths: u32,
    combinations: u64,
}

impl Reached {
    /// Adds `reach`.
    pub(crate) fn add(&mut self, reach: Reach) {
        self.paths |= reach.bit();
    }

    /// Adds `combination`.
    pub(crate) fn add_combination(&mut self, combination: Combination) {
        self.combinations |= combination.bit();
    }

    /// The paths and combinations either reached.
    pub(crate) fn with(self, other: Self) -> Self {
        Self {
            paths: self.paths | other.paths,
            combinations: self.combinations | other.combinations,
        }
    }

    /// Whether the runs reached `reach`.
    pub fn has(self, reach: Reach) -> bool {
        self.paths & reach.bit() != 0
    }

    /// Whether the checks held `combination`.
    pub fn has_combination(self, combination: Combination) -> bool {
        self.combinations & combination.bit() != 0
    }

    /// The name of every path reached and every combination held, for a timing line.
    pub(crate) fn names(self) -> Vec<String> {
        let paths = Reach::ALL
            .into_iter()
            .filter(|reach| self.has(*reach))
            .map(|reach| format!("{reach:?}"));
        let combinations = Combination::listed()
            .into_iter()
            .filter(|combination| self.has_combination(*combination))
            .map(|combination| combination.to_string());
        paths.chain(combinations).collect()
    }
}
