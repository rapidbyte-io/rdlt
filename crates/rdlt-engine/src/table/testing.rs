//! Helpers for tests elsewhere in the crate that need tables.

use std::sync::Arc;

use rdlt_connector::{Capabilities, StreamName};

use super::{LineageColumns, MetaNames, Resolver, Settings};
use crate::naming::Naming;
use crate::plan::StreamPlan;
use crate::policy::SchemaSettings;

/// A resolver of the stream `stream` for a destination with minimal capabilities.
pub(crate) fn resolver(stream: &str) -> Resolver {
    let capabilities = Capabilities::minimal();
    let naming = Naming::new(capabilities.identifiers.clone());
    let stream = StreamName::new(stream).expect("test stream names are valid");
    Resolver {
        settings: Settings {
            pipeline: SchemaSettings::default(),
            stream: StreamPlan::new(stream.clone()),
            key: Vec::new(),
        },
        stream,
        meta: MetaNames::assign(&naming, false, LineageColumns::None).expect("metadata names"),
        naming,
        capabilities: Arc::new(capabilities),
        root: None,
    }
}
