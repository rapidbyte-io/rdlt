//! The CLI's error taxonomy and THE exit-code table, stated once:
//! 0 success · 2 config · 3 schema contract · 4 source · 5 destination ·
//! 6 WAL/disk · 7 cancelled · 64 usage · 70 internal defect · 74 file
//! I/O. 70 is also the code for any `rdlt::Error` variant this build
//! does not know: "this binary cannot classify what happened" is a bug
//! to report, never an instruction to go and edit the pipeline
//! configuration.

use std::process::ExitCode;

use rdlt::pipeline_spec::SpecError;

use crate::render;

pub(crate) enum Error {
    Usage(String),
    /// A file the CLI itself could not read or write. Distinct from
    /// `Usage`: the invocation was well-formed, the filesystem refused.
    Io(String),
    Run(rdlt::Error),
}

impl Error {
    /// Render the error on stderr and hand back its exit code.
    /// Best-effort reporting: even with stderr closed, the CODE is the
    /// contract and must still be exited with.
    pub(crate) fn exit(self) -> ExitCode {
        match self {
            Error::Io(message) => {
                render::stderr::line(&format!("error: {message}"));
                ExitCode::from(74)
            }
            Error::Usage(message) => {
                render::stderr::line(&format!("error: {message}"));
                ExitCode::from(2)
            }
            Error::Run(error) => {
                render::stderr::line(&format!("error: {error}"));
                ExitCode::from(code_for(&error))
            }
        }
    }
}

/// The `rdlt::Error` half of the contract, alone so a pin can hold it.
pub(crate) fn code_for(error: &rdlt::Error) -> u8 {
    match error {
        rdlt::Error::Config { .. } => 2,
        rdlt::Error::Schema(_) => 3,
        rdlt::Error::Source { .. } => 4,
        rdlt::Error::Destination { .. } => 5,
        rdlt::Error::Wal { .. } => 6,
        rdlt::Error::Cancelled => 7,
        rdlt::Error::Internal { .. } => 70,
        // NOT 2. Falling back to the config code tells a scripting
        // caller to fix their YAML for something the engine could not
        // classify, and a future variant would silently join it.
        _ => 70,
    }
}

impl From<rdlt::Error> for Error {
    fn from(e: rdlt::Error) -> Self {
        Error::Run(e)
    }
}

impl From<SpecError> for Error {
    fn from(e: SpecError) -> Self {
        match e {
            // A spec-resolution problem is a config error (exit 2), the
            // same taxonomy the loud parse/IO paths use.
            SpecError::Resolve(message) => Error::Usage(message),
            // The builder's own typed error keeps its exit-code mapping.
            SpecError::Build(error) => Error::Run(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exit-code contract, pinned variant by variant — scripts
    /// dispatch on these numbers.
    #[test]
    fn the_exit_code_contract_holds() {
        assert_eq!(code_for(&rdlt::Error::config("x")), 2);
        assert_eq!(code_for(&rdlt::Error::Cancelled), 7);
        assert_eq!(
            code_for(&rdlt::Error::source(
                rdlt::prelude::StreamName::new("s"),
                "x"
            )),
            4
        );
    }
}
