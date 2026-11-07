//! A source pushing the Arrow batches a test gives it as the stream `people`, each push followed
//! by a checkpoint.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::{
    ConnectContext, ConnectorError, Emitter, Partition, ReadStream, Result, Source,
    SourceConnector, StreamName, StreamSpec, Streams, source_factory,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

/// What each source serves, by the name it is connected with.
static SERVED: LazyLock<Mutex<BTreeMap<String, Vec<RecordBatch>>>> = LazyLock::new(Mutex::default);

/// The source pushing `pushes`, registered as `name`.
pub(crate) async fn source(name: &str, pushes: Vec<RecordBatch>) -> Arc<dyn Source> {
    SERVED.lock().insert(name.to_owned(), pushes);
    let source = source_factory::<Served>()
        .connect(json!({ "name": name }), ConnectContext::new())
        .await
        .expect("the source is registered");
    Arc::from(source)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ServedConfig {
    name: String,
}

struct Served {
    pushes: Vec<RecordBatch>,
}

impl SourceConnector for Served {
    const ID: &'static str = "io.test.sql.served";
    const VERSION: &'static str = "0.0.0";
    type Config = ServedConfig;

    async fn connect(config: ServedConfig, _: &ConnectContext) -> Result<Self> {
        let pushes = SERVED.lock().get(&config.name).cloned();
        let pushes = pushes.ok_or_else(|| ConnectorError::config("no such source"))?;
        Ok(Self { pushes })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(People)
    }
}

/// The stream `people`.
struct People;

impl ReadStream<Served> for People {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("people").expect("a valid name"))
    }

    async fn read(
        &self,
        source: &Served,
        _: &Partition,
        next: usize,
        out: &mut Emitter<usize>,
    ) -> Result<()> {
        for (index, batch) in source.pushes.iter().enumerate().skip(next) {
            out.batch(batch.clone()).await?;
            out.checkpoint(&(index + 1)).await?;
        }
        Ok(())
    }
}
