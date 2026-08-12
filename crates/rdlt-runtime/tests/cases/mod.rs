//! Case modules for the one integration binary.

#[cfg(feature = "spawn-bins")]
mod test_e2e_spawned;
mod test_local_provider;
#[cfg(feature = "spawn-bins")]
mod test_spawned_bins;
