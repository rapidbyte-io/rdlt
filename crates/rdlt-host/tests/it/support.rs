//! Connectors served over sockets for the tests: the reference connectors, scripted ones, and
//! fakes that break the protocol.

pub(crate) mod connectors;
pub(crate) mod fake;
pub(crate) mod served;

pub(crate) use fake::{Fake, Fault, serve_fake};
pub(crate) use served::{
    engine, memory_destination, memory_source, raw_client, served, served_within,
};
