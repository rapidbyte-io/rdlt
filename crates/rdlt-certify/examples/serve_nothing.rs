//! A connector binary that serves no role, as one built without its factories would.

use std::process::ExitCode;

use rdlt_connector::serve::Served;

fn main() -> ExitCode {
    Served::new().serve()
}
