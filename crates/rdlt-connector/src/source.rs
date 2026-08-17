//! The source half of the SPI: [`Source`], its per-read request, and
//! the stream declarations a source offers.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::channel::RecordsOut;
use crate::core::{Cursor, LogicalType, StreamName};
use crate::error::SourceError;
use crate::spec::ConnectorSpec;

/// A data source: declares streams, then pushes records for one stream
/// per [`Source::read`] call.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Name, version, config JSON-schema.
    fn spec(&self) -> ConnectorSpec;

    /// A cheap connectivity probe: can this source reach what it reads
    /// from, with the credentials it holds?
    ///
    /// Distinct from [`Source::read`] on purpose — an operator wants "are
    /// the credentials right" answered in seconds, without moving data.
    /// Failures classify exactly as `read` would classify them: a bad
    /// credential is fatal, an unreachable host transient.
    ///
    /// The default body returns `Ok(())` WITHOUT probing anything, so a
    /// source that has not implemented it reports success trivially — a
    /// host needing a real answer must know which connectors implement
    /// the probe. Implementations replace it wholesale.
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }

    /// Discover the streams this source can read.
    ///
    /// Called once per run, before any [`Source::read`]. The host plans
    /// table ownership from the result, so two streams must not claim the
    /// same destination table.
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError>;

    /// Read ONE stream, pushing through `request.out`. The host schedules
    /// streams concurrently. Given `request.since`, the source MUST
    /// resume — never re-emit rows already covered by that cursor.
    /// Awaiting the push IS the flow control; treat
    /// [`ChannelClosed`](crate::channel::ChannelClosed) as cancellation
    /// and return promptly.
    async fn read(&self, request: ReadRequest) -> Result<(), SourceError>;
}

/// Everything a source needs to read one stream.
///
/// `#[non_exhaustive]`: out-of-crate struct-literal construction is
/// closed, so wire-era fields can arrive without a breaking change.
/// [`ReadRequest::new`] is the constructor — every consumer in the tree
/// builds through it — and the fields stay `pub` for read access.
#[derive(Debug)]
#[non_exhaustive]
pub struct ReadRequest {
    /// The one stream this call must read.
    pub stream: StreamSpec,
    /// Last committed cursor — only ever a cursor the destination
    /// previously committed.
    pub since: Option<Cursor>,
    /// Push handle.
    pub out: RecordsOut,
}

impl ReadRequest {
    /// Build a request. Present so a later field addition is not a
    /// breaking change for callers constructing through here rather than
    /// with a struct literal.
    pub fn new(stream: StreamSpec, since: Option<Cursor>, out: RecordsOut) -> Self {
        Self { stream, since, out }
    }
}

/// A source's declaration of one stream, consumed by host planning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSpec {
    /// The stream's name — also the destination table it owns.
    pub name: StreamName,
    /// Declared row identity: `_rdlt_id` becomes a key hash and `Merge`
    /// merges on it.
    ///
    /// Merge-key precedence: this and a run's `WriteMode::Merge { key }`
    /// name ONE identity, not two. The host requires them to agree (same
    /// columns, as a set) and rejects a mismatch typed at plan time —
    /// neither silently wins.
    pub primary_key: Option<Vec<String>>,
    /// The field carrying the incremental cursor, when the stream can
    /// resume.
    pub cursor_field: Option<String>,
    /// Per-column logical-type hints; they take precedence over
    /// inference.
    pub type_hints: BTreeMap<String, LogicalType>,
    /// This stream pushes already-structured Arrow batches (no per-row
    /// `_rdlt_id`). A structured stream may still `Merge`, but only on a
    /// declared `primary_key` that the write mode names exactly; a
    /// keyless structured stream cannot Merge (rejected at plan time).
    ///
    /// Serde-defaults to `false` so older serialized declarations
    /// deserialize unchanged.
    #[serde(default)]
    pub structured: bool,
}

impl StreamSpec {
    /// The minimum declaration: no key, no cursor, no hints.
    pub fn new(name: impl Into<StreamName>) -> Self {
        Self {
            name: name.into(),
            primary_key: None,
            cursor_field: None,
            type_hints: BTreeMap::new(),
            structured: false,
        }
    }

    /// Declare this stream as pushing already-structured Arrow batches.
    pub fn with_structured(mut self) -> Self {
        self.structured = true;
        self
    }

    /// Declare the columns identifying a row. Under `Merge` these must
    /// name the same set as the write mode's key — a mismatch is rejected
    /// at plan time rather than arbitrated.
    pub fn with_primary_key(mut self, key: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.primary_key = Some(key.into_iter().map(Into::into).collect());
        self
    }

    /// Declare the field carrying the incremental cursor, enabling
    /// resumable reads.
    pub fn with_cursor_field(mut self, field: impl Into<String>) -> Self {
        self.cursor_field = Some(field.into());
        self
    }

    /// Pin one column's logical type over what inference would choose —
    /// for data that is ambiguous on its own, like an ISO-8601 string
    /// that should be a timestamp rather than text.
    pub fn with_type_hint(mut self, column: impl Into<String>, hint: LogicalType) -> Self {
        self.type_hints.insert(column.into(), hint);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Probeless;

    #[async_trait]
    impl Source for Probeless {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("probeless", "0.0.0")
        }
        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![])
        }
        async fn read(&self, _request: ReadRequest) -> Result<(), SourceError> {
            Ok(())
        }
    }

    /// The default `check` is a trivial success BY CONTRACT: a host that
    /// needs a real probe must know the connector implements one.
    #[tokio::test]
    async fn the_default_check_reports_success_without_probing() {
        assert!(Probeless.check().await.is_ok());
    }

    /// Builders compose onto the minimum declaration, and an older wire
    /// form without `structured` deserializes to `false`.
    #[test]
    fn builders_compose_and_the_wire_form_defaults_structured_off() {
        let declared = StreamSpec::new("orders")
            .with_primary_key(["id"])
            .with_cursor_field("updated_at")
            .with_type_hint("updated_at", LogicalType::TimestampTz);
        assert_eq!(declared.primary_key.as_deref(), Some(&["id".into()][..]));
        assert_eq!(declared.cursor_field.as_deref(), Some("updated_at"));
        assert!(!declared.structured);
        assert!(StreamSpec::new("raw").with_structured().structured);

        let wire = serde_json::json!({
            "name": "orders",
            "primary_key": null,
            "cursor_field": null,
            "type_hints": {},
        });
        let parsed: StreamSpec = serde_json::from_value(wire).expect("older form parses");
        assert!(!parsed.structured, "absent `structured` defaults to false");
    }
}
