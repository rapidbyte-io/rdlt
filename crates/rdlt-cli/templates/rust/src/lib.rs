//! A source connector of one stream, `people`, whose cursor is the index of the next row.

#![forbid(unsafe_code)]

use rdlt_connector::prelude::*;
use serde_json::json;

/// The configuration the connector accepts; its JSON Schema is published.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct Config {
    /// Rows per push.
    page: usize,
}

/// The connector.
#[derive(Debug)]
pub struct Greetings {
    page: usize,
}

#[source(id = "io.example.greetings")]
impl SourceConnector for Greetings {
    type Config = Config;

    async fn connect(config: Config, _context: &ConnectContext) -> Result<Self> {
        if config.page == 0 {
            return Err(ConnectorError::config("page must be at least 1"));
        }
        Ok(Self { page: config.page })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(People)
    }
}

struct People;

const NAMES: [&str; 3] = ["ada", "grace", "edsger"];

impl ReadStream<Greetings> for People {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("people").expect("a valid stream name"))
    }

    async fn read(
        &self,
        source: &Greetings,
        _partition: &Partition,
        cursor: usize,
        out: &mut Emitter<usize>,
    ) -> Result<()> {
        let rows: Vec<_> = (0..)
            .zip(NAMES)
            .map(|(id, name)| json!({ "id": id, "name": name }))
            .collect();
        let mut next = cursor.min(rows.len());
        while next < rows.len() {
            let end = (next + source.page).min(rows.len());
            out.rows(&rows[next..end]).await?;
            next = end;
            // A checkpoint seals a segment. A full read, as this stream's, seals the rows after
            // the last one with its end; read incrementally, the stream's one partition never
            // ends, so a read ends at a checkpoint to land its last rows.
            if next < rows.len() {
                out.checkpoint(&next).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn greetings_certifies() {
        let config = serde_json::json!({ "page": 1 });
        rdlt_connector::testing::certify_source::<super::Greetings>(config)
            .await
            .assert_passed();
    }
}
