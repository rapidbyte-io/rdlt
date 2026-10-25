//! Connectors served over sockets for the tests: the reference connectors and scripted ones.

pub(crate) mod connectors;
pub(crate) mod raw;
pub(crate) mod served;

pub(crate) use served::{
    engine, memory_destination, memory_source, paused_engine, piped, plane_client, raw_channel,
    raw_client, served, served_within, system_env,
};
