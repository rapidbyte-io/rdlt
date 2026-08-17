//! Case modules for the one integration binary; shared support lives
//! in `support`.

mod support;
#[cfg(feature = "spawn-bins")]
mod test_certify_wire;
mod test_conformance;
mod test_destination;
mod test_durability;
mod test_source;
