//! The engine loading through connectors served over sockets.

#![forbid(unsafe_code)]

mod acknowledged;
mod admission;
mod canary;
mod charged;
mod credit;
mod cuts;
mod decoded;
mod descriptors;
mod dictionaries;
mod flow;
mod frames;
mod grants;
mod groups;
mod guarded;
mod identity;
mod kills;
mod latency;
mod limits;
mod liveness;
mod loads;
mod mutual;
mod network;
mod networks;
mod placement;
mod process;
mod protocol;
mod published;
mod reads;
mod redial;
mod sandbox;
mod secrets;
mod sessions;
mod staging;
mod started;
mod support;
mod wires;

/// Tracks the heap's peak, for what a message holds before it is refused.
#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;
