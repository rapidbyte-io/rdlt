//! Case modules for the one integration binary; shared support lives
//! in `support`. The spawned cases run REAL pipelines over the built
//! reference connector, so they ride the `spawn-bins` feature and the
//! Makefile's RDLT_BUILD_CONNECTOR_BINS line.

mod support;
mod test_contract;
#[cfg(feature = "spawn-bins")]
mod test_spawned;
