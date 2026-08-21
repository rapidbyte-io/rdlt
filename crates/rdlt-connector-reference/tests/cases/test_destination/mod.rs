//! The destination's own exactly-once pins, split by concern: the
//! receipt-driven [`replay`] law, the bounded [`refusals`], the
//! one-pipeline state slot and session lease ([`sessions`]), and the
//! config/part/state [`gates`]. Shared fixtures live here.

use rdlt_connector_reference::destination::config::Config;
use rdlt_connector_reference::destination::connector::Reference;
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::destination::Shell;
use rdlt_connector_sdk::spi::core::commit::{CommitReceipt, WriteMode};
use rdlt_connector_sdk::spi::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector_sdk::spi::destination::{Destination, OpenContext};
use rdlt_testkit::conformance::destination::TableProbe;
use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};
use serde_json::json;

use super::support::DirProbe;


/// The sdk shell over `dir` — the SPI face this crate's tests drive
/// in-process.
fn shell_over(dir: &std::path::Path) -> Shell<Reference> {
    Shell::<Reference>::from_value(json!({"path": dir})).expect("valid config")
}
mod replay;
mod refusals;
mod sessions;
mod gates;
