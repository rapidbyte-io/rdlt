//! A source that reads rows given inline in its configuration.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration of [`MemorySource`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemorySourceConfig {
    /// Each stream's rows, by stream name.
    pub streams: BTreeMap<String, Vec<serde_json::Value>>,
    /// Rows per push; a checkpoint follows each push.
    #[serde(default = "default_page_size")]
    pub page_size: usize,
}

fn default_page_size() -> usize {
    100
}

/// Reads the rows in its configuration as JSON, one checkpointed page at a time.
#[derive(Debug)]
pub struct MemorySource {
    streams: BTreeMap<String, Arc<Vec<serde_json::Value>>>,
    page_size: usize,
}

#[source(id = "io.rapidbyte.memory")]
impl SourceConnector for MemorySource {
    type Config = MemorySourceConfig;

    async fn connect(config: MemorySourceConfig, _context: &ConnectContext) -> Result<Self> {
        if config.page_size == 0 {
            return Err(ConnectorError::config("page_size must be at least 1"));
        }
        for name in config.streams.keys() {
            StreamName::new(name).config(format!("stream name {name:?}"))?;
        }
        let streams = config
            .streams
            .into_iter()
            .map(|(name, rows)| (name, Arc::new(rows)))
            .collect();
        Ok(Self {
            streams,
            page_size: config.page_size,
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        self.streams.keys().fold(Streams::new(), |streams, name| {
            streams.with(Rows { name: name.clone() })
        })
    }
}

struct Rows {
    name: String,
}

/// The index of the next row to read.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Offset {
    next: usize,
}

impl ReadStream<MemorySource> for Rows {
    type Cursor = Offset;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new(&self.name).expect("connect validated stream names"))
    }

    async fn read(
        &self,
        source: &MemorySource,
        _partition: &Partition,
        cursor: Offset,
        out: &mut Emitter<Offset>,
    ) -> Result<()> {
        let rows = source
            .streams
            .get(&self.name)
            .map(Arc::clone)
            .unwrap_or_default();
        let mut next = cursor.next.min(rows.len());
        while next < rows.len() {
            let end = (next + source.page_size).min(rows.len());
            out.rows(&rows[next..end]).await?;
            next = end;
            out.checkpoint(&Offset { next }).await?;
        }
        Ok(())
    }
}
