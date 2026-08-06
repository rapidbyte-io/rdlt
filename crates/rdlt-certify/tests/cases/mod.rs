//! Case modules for the one integration binary.

#[cfg(feature = "spawn-bins")]
mod support;
#[cfg(feature = "spawn-bins")]
mod test_certify_file_destination;
#[cfg(feature = "spawn-bins")]
mod test_certify_file_source;
// The CLI cases additionally need the `bin` feature: they spawn the
// certifier bin itself, whose path `CARGO_BIN_EXE_rdlt-certify` only
// exists when cargo builds the bin target alongside the tests.
#[cfg(all(feature = "spawn-bins", feature = "bin"))]
mod test_cli;
#[cfg(feature = "spawn-bins")]
mod test_kill_matrix;
mod test_report;
