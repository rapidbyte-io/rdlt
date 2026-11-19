//! A source whose stream `limits` holds one row, the limits each read of it is told, a column a
//! limit, beside as many streams of the longest names as its configuration's `padding` says,
//! served as one connector binary: for the tests of what a placed connector is told.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use rdlt_connector::prelude::*;
use rdlt_connector::serve::Served;
use serde_json::json;

/// The source's configuration.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ToldConfig {
    /// Streams the catalog holds beside `limits`, each named in 256 bytes.
    #[serde(default)]
    padding: usize,
}

/// A source telling what its reads are told.
#[derive(Debug)]
struct Told {
    padding: usize,
}

#[source(id = "test.told")]
impl SourceConnector for Told {
    type Config = ToldConfig;

    async fn connect(config: ToldConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            padding: config.padding,
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        let mut streams = Streams::new().with(Telling);
        for index in 0..self.padding {
            streams = streams.with(Padding(index));
        }
        streams
    }
}

/// The stream `limits`.
struct Telling;

impl ReadStream<Told> for Telling {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("limits").expect("a valid stream name"))
    }

    async fn read(
        &self,
        _source: &Told,
        _partition: &Partition,
        _cursor: usize,
        out: &mut Emitter<usize>,
    ) -> Result<()> {
        let told = out.limits();
        let row = json!({
            "frame_bytes": told.frame_bytes,
            "batch_rows": told.batch_rows,
            "schema_columns": told.schema_columns,
            "nesting_depth": told.nesting_depth,
            "json_push_bytes": told.json_push_bytes,
            "cursor_bytes": told.cursor_bytes,
            "config_bytes": told.config_bytes,
            "control_string_bytes": told.control_string_bytes,
            "batch_values": told.batch_values,
            "schema_bytes": told.schema_bytes,
            "dictionary_bytes": told.dictionary_bytes,
            "catalog_bytes": told.catalog_bytes,
            "state_bytes": told.state_bytes,
            "control_message_bytes": told.control_message_bytes,
        });
        out.rows(&[row]).await
    }
}

/// A stream of the catalog's padding, never read.
struct Padding(usize);

impl ReadStream<Told> for Padding {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        let name = format!("{:0>256}", self.0);
        StreamSpec::new(StreamName::new(name).expect("a name of 256 digits is valid"))
    }

    async fn read(
        &self,
        _source: &Told,
        _partition: &Partition,
        _cursor: usize,
        _out: &mut Emitter<usize>,
    ) -> Result<()> {
        Ok(())
    }
}

fn main() -> ExitCode {
    Served::serve(|| Served::new().with_source(source_factory::<Told>()))
}
