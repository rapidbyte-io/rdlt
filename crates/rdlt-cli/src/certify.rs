//! `rdlt certify`.

use crate::Exit;
use crate::args::HostArgs;
use crate::output::Mode;

/// The certification's own flags.
#[derive(Debug, clap::Args)]
pub(crate) struct CertifyArgs;

pub(crate) fn certify(_: &CertifyArgs, _: &HostArgs, _: Mode) -> Exit {
    Exit::Usage
}
