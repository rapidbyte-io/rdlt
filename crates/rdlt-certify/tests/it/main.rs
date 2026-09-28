//! Certification over the wire: connectors served in this process, spawned, and listening.

// The binary is built only with the kill clauses.
#[cfg(feature = "kill")]
mod cli;
mod faults;
mod killed;
mod listening;
mod read_back;
mod served;
mod spawned;
mod unmet;
mod unreached;
