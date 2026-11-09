//! A source that fails where its configuration says, saying its `token` as it fails: for tests of
//! exit codes and of secrets that must not leak.

use rdlt_connector::prelude::*;
use serde_json::json;

/// Where the source fails.
#[derive(Clone, Copy, Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Failing {
    /// Its check refuses, saying the token.
    Check,
    /// Its read fails, saying the token.
    Read,
    /// Its read panics, saying the token.
    Panic,
    /// It reads one row, the token in it.
    Never,
}

/// The source's configuration.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FailingConfig {
    /// Where it fails.
    pub fail: Failing,
    /// What it says as it fails.
    #[serde(default)]
    pub token: String,
}

/// A source that fails as its configuration says.
#[derive(Debug)]
pub struct FailingSource {
    fail: Failing,
    token: String,
}

#[source(id = "io.rapidbyte.failing")]
impl SourceConnector for FailingSource {
    type Config = FailingConfig;

    async fn connect(config: FailingConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            fail: config.fail,
            token: config.token,
        })
    }

    async fn check(&self) -> Result<()> {
        match self.fail {
            Failing::Check => Err(ConnectorError::config(format!(
                "the check refused {}",
                self.token
            ))),
            Failing::Read | Failing::Panic | Failing::Never => Ok(()),
        }
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Rows)
    }
}

struct Rows;

impl ReadStream<FailingSource> for Rows {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("rows").expect("a valid stream name"))
    }

    async fn read(
        &self,
        source: &FailingSource,
        _partition: &Partition,
        _cursor: usize,
        out: &mut Emitter<usize>,
    ) -> Result<()> {
        match source.fail {
            Failing::Read => Err(ConnectorError::internal(format!(
                "the read failed holding {}",
                source.token
            ))),
            Failing::Panic => panic!("the read panicked holding {}", source.token),
            Failing::Check | Failing::Never => {
                out.rows(&[json!({ "id": 1, "token": source.token })]).await
            }
        }
    }
}
