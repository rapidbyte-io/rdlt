//! Schema registry and contract enforcement: [`registry`] holds each table's
//! current version and describes evolution as deltas; [`contract`] decides
//! what a policy does with a would-be change.

pub(crate) mod contract;
pub(crate) mod registry;
