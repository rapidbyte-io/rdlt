//! The S3 log store on real S3 servers, `RustFS` and `MinIO`, each in a container of its own: the
//! store's contract, the probe, and loads killed as they commit and run again.
//!
//! They need Docker, and run only where asked: the default test profile filters this binary
//! out, and `just containers` runs it, as CI's Linux job does.

#![forbid(unsafe_code)]

mod contract;
mod crashes;
mod servers;
