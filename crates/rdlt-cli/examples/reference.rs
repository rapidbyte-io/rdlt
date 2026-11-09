//! Prints the CLI reference, for `docs/cli.md`.

#![forbid(unsafe_code)]

use std::io::Write as _;

fn main() -> std::io::Result<()> {
    std::io::stdout().write_all(rdlt_cli::reference().as_bytes())
}
