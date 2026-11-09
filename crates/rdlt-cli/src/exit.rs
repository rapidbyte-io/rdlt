//! How the command line ends: one table of exit codes, from which the reference is generated.

#[cfg(test)]
mod tests;

/// What a run of the command line ended as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Exit {
    /// It did what it was asked.
    Success,
    /// A certification found a clause failed, or one that applies not observed.
    Findings,
    /// A document, the host's policy, a placement or a secret reference is wrong.
    Config,
    /// Data did not fit its table's schema under its policy.
    Schema,
    /// The source failed.
    Source,
    /// The destination failed.
    Destination,
    /// A transform failed.
    Transform,
    /// A newer run or session of the pipeline fenced this one.
    Fenced,
    /// The memory budget had no room for what the run holds.
    Memory,
    /// The command line is wrong.
    Usage,
    /// A defect in rdlt.
    Internal,
    /// A file, binary or stream could not be read or written.
    Io,
    /// Stopped by a signal: the code is 128 and the signal's number.
    Signalled(i32),
}

impl Exit {
    /// Every exit with its meaning, as `docs/cli.md` lists them.
    pub const TABLE: [(Exit, &'static str); 14] = [
        (Exit::Success, "success"),
        (
            Exit::Findings,
            "certify findings: a clause failed, or one that applies was not observed",
        ),
        (
            Exit::Config,
            "config: the document, the host's policy, a placement or a secret reference",
        ),
        (
            Exit::Schema,
            "schema: data does not fit its table's schema under its policy",
        ),
        (Exit::Source, "source: the source failed"),
        (Exit::Destination, "destination: the destination failed"),
        (Exit::Transform, "transform: a transform failed"),
        (
            Exit::Fenced,
            "fenced: a newer run or session of the pipeline opened",
        ),
        (
            Exit::Memory,
            "memory: the budget had no room for what the run holds",
        ),
        (Exit::Usage, "usage: the command line is wrong"),
        (Exit::Internal, "internal: a defect in rdlt"),
        (
            Exit::Io,
            "I/O: a file, binary or stream could not be read or written",
        ),
        (
            Exit::Signalled(130),
            "SIGINT: interrupted; the report says whether the stop was clean",
        ),
        (
            Exit::Signalled(143),
            "SIGTERM: terminated; the report says whether the stop was clean",
        ),
    ];

    /// The process's exit code.
    pub fn code(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::Findings => 1,
            Self::Config => 2,
            Self::Schema => 3,
            Self::Source => 4,
            Self::Destination => 5,
            Self::Transform => 6,
            Self::Fenced => 7,
            Self::Memory => 8,
            Self::Usage => 64,
            Self::Internal => 70,
            Self::Io => 74,
            Self::Signalled(status) => {
                u8::try_from(status).unwrap_or_else(|_| Self::Internal.code())
            }
        }
    }

    /// The exit of a failure of `kind`.
    pub fn of(kind: rdlt::ErrorKind) -> Self {
        use rdlt::ErrorKind as Kind;
        match kind {
            Kind::Config => Self::Config,
            Kind::Schema => Self::Schema,
            Kind::Source => Self::Source,
            Kind::Destination => Self::Destination,
            Kind::Transform => Self::Transform,
            Kind::Fenced => Self::Fenced,
            Kind::Memory => Self::Memory,
            _ => Self::Internal,
        }
    }
}

impl From<Exit> for std::process::ExitCode {
    fn from(exit: Exit) -> Self {
        Self::from(exit.code())
    }
}
