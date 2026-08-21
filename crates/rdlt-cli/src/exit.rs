//! The CLI's error taxonomy and THE exit-code table, stated once:
//! 0 success · 2 config · 3 schema contract · 4 source · 5 destination ·
//! 6 WAL/disk · 7 cancelled · 64 usage · 70 internal defect · 74 file
//! I/O. 70 is also the code for any `rdlt::error::Error` variant this build
//! does not know: "this binary cannot classify what happened" is a bug
//! to report, never an instruction to go and edit the pipeline
//! configuration.

use std::process::ExitCode;

use rdlt::error;

use crate::render;

pub(crate) enum Error {
    Usage(String),
    /// A file the CLI itself could not read or write. Distinct from
    /// `Usage`: the invocation was well-formed, the filesystem refused.
    Io(String),
    Run(error::Error),
}

impl Error {
    /// Render the error on stderr and hand back its exit code.
    /// Best-effort reporting: even with stderr closed, the CODE is the
    /// contract and must still be exited with.
    pub(crate) fn exit(self) -> ExitCode {
        let (message, code) = match self {
            Error::Io(message) => (message, 74),
            Error::Usage(message) => (message, 2),
            Error::Run(error) => (error.to_string(), code_for(&error)),
        };
        render::stderr::line(&format!("error: {message}"));
        ExitCode::from(code)
    }
}

/// The `rdlt::error::Error` half of the contract, alone so a pin can hold it.
pub(crate) fn code_for(error: &error::Error) -> u8 {
    match error {
        error::Error::Config { .. } => 2,
        error::Error::Schema(_) => 3,
        error::Error::Source { .. } => 4,
        error::Error::Destination { .. } => 5,
        error::Error::Wal { .. } => 6,
        error::Error::Io { .. } => 74,
        error::Error::Cancelled => 7,
        error::Error::Internal { .. } => 70,
        // NOT 2. Falling back to the config code tells a scripting
        // caller to fix their YAML for something the engine could not
        // classify, and a future variant would silently join it.
        _ => 70,
    }
}

impl From<error::Error> for Error {
    fn from(e: error::Error) -> Self {
        Error::Run(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exit-code contract, pinned variant by variant — scripts
    /// dispatch on these numbers.
    #[test]
    fn the_exit_code_contract_holds() {
        assert_eq!(code_for(&error::Error::config("x")), 2);
        assert_eq!(code_for(&error::Error::Cancelled), 7);
        assert_eq!(
            code_for(&error::Error::source(rdlt::id::StreamName::new("s"), "x")),
            4
        );
    }
}
