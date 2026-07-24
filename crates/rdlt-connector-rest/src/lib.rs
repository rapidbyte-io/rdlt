//! # rdlt-connector-rest — REST API connector
//!
//! Currently source-only; the module layout mirrors the connector-family
//! convention (`rdlt-connector-postgres`): the source lives in [`source`],
//! and this root stays a thin façade with re-exports so existing import
//! paths keep working.

pub mod source;

/// The shared credential newtype, re-exported once at this crate's root.
pub use rdlt_connector::Secret;
pub use source::{
    Auth, PageContext, PageDecision, Pagination, Paginator, RestClient, RestConfig, RestSource,
    RestStream, config, config_schema,
};
