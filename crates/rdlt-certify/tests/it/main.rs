//! Certification over the wire: connectors served in this process, spawned, and listening.

#![forbid(unsafe_code)]

mod cli;
mod faults;
mod killed;
mod listening;
mod probing;
mod read_back;
mod served;
mod spawned;
mod unmet;
mod unreached;
