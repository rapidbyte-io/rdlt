//! A source that fails where its configuration says, saying its `token` as it fails, its stream
//! and partition named as it says: for tests of exit codes and of secrets that must not leak.

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
    /// Its read fails, saying the token, as something worth trying again.
    Transient,
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
    /// The name of its only stream; `rows` where it says none.
    #[serde(default)]
    pub stream: Option<String>,
    /// The id of its only partition; the SDK's single partition where it says none.
    #[serde(default)]
    pub partition: Option<String>,
}

/// A source that fails as its configuration says.
#[derive(Debug)]
pub struct FailingSource {
    fail: Failing,
    token: String,
    stream: StreamName,
    partition: Option<PartitionId>,
}

#[source(id = "io.rapidbyte.failing")]
impl SourceConnector for FailingSource {
    type Config = FailingConfig;

    async fn connect(config: FailingConfig, _context: &ConnectContext) -> Result<Self> {
        let stream = config.stream.as_deref().unwrap_or("rows");
        let partition = config.partition.map(PartitionId::parse).transpose();
        Ok(Self {
            fail: config.fail,
            token: config.token,
            stream: StreamName::new(stream).config("the stream's name")?,
            partition: partition.config("the partition's id")?,
        })
    }

    async fn check(&self) -> Result<()> {
        match self.fail {
            Failing::Check => Err(ConnectorError::config(format!(
                "the check refused {}",
                self.token
            ))),
            Failing::Read | Failing::Transient | Failing::Panic | Failing::Never => Ok(()),
        }
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Rows(self.stream.clone()))
    }
}

struct Rows(StreamName);

impl ReadStream<FailingSource> for Rows {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(self.0.clone())
    }

    async fn partitions(
        &self,
        source: &FailingSource,
        _state: &StreamState,
    ) -> Result<Vec<Partition>> {
        let planned = source
            .partition
            .clone()
            .map_or_else(Partition::single, Partition::new);
        Ok(vec![planned])
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
            Failing::Transient => Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                format!("the read failed holding {}", source.token),
            )),
            Failing::Panic => panic!("the read panicked holding {}", source.token),
            Failing::Check | Failing::Never => {
                out.rows(&[json!({ "id": 1, "token": source.token })]).await
            }
        }
    }
}
