//! # rdlt-connector-iceberg — provider-agnostic Iceberg destination
//!
//! One Iceberg REST catalog protocol, many providers (Polaris/Snowflake
//! Open Catalog, Databricks UC, …). Engine commits map onto atomic Iceberg
//! snapshots with the commit identity in snapshot summary properties —
//! exactly-once readable from table history alone. The Iceberg mechanics
//! (manifests, metadata, field IDs, the REST commit machinery) come from
//! Apache iceberg-rust, wrapped at ONE boundary: nothing from that library
//! crosses this crate's public surface (contract ID1).
//!
//! Family layout: `dest/` (this crate is destination-only until a reading
//! feature exists); this façade re-exports the public surface.

pub mod dest;

pub use dest::IcebergDest;
pub use dest::config::{
    AuthOptions, IcebergConfig, PartitionField, PartitionTransform, S3Override, Secret,
    TableOptions, config_schema,
};
