//! A write in progress on a destination, for the clauses that send it a frame it must refuse.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::wire::v1;
use rdlt_connector::{
    ConnectorError, Field, LoadId, LogicalType, SchemaVersion, TableChange, TablePath, TableRef,
    TableSchema,
};
use rdlt_host::remote::Client;
use rdlt_wire::tonic::{Status, Streaming};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{Violation, error};

/// A write of a one-column table, its schema sent, waiting for batches.
pub(super) struct Writing {
    frames: mpsc::Sender<v1::WriteFrame>,
    acks: Streaming<v1::WriteAck>,
}

impl Writing {
    /// Opens a session on the connector `client` speaks to, creates a table of its own in it, and
    /// starts writing the table, its schema sent.
    pub(super) async fn start(client: &mut Client) -> Result<Self, Violation> {
        let (session, table) = created(client).await?;
        let (frames, receiver) = mpsc::channel(4);
        let start = v1::write_frame::Frame::Start(v1::WriteStart {
            session,
            table: Some(v1::TableRef::from(&table)),
        });
        let schema = v1::write_frame::Frame::Schema(v1::WriteSchema {
            version: 1,
            ipc_schema: rdlt_wire::Encoder::default().schema(&rows().schema()),
        });
        for frame in [start, schema] {
            // The receiver is open until it is dropped with the call, so these sends succeed.
            frames
                .send(v1::WriteFrame { frame: Some(frame) })
                .await
                .ok();
        }
        let acks = client
            .write(ReceiverStream::new(receiver))
            .await
            .map_err(|status| Violation::from(format!("the write failed: {}", error(&status))))?
            .into_inner();
        Ok(Self { frames, acks })
    }

    /// Sends `batch`, and a flush after it, and returns the error the write then fails with.
    pub(super) async fn refusal(
        mut self,
        batch: v1::WriteBatch,
    ) -> Result<ConnectorError, Violation> {
        // A flush, so a destination that took the batch says so rather than wait for more.
        let frames = [
            v1::write_frame::Frame::Batch(batch),
            v1::write_frame::Frame::Flush(v1::Unit {}),
        ];
        for frame in frames {
            // A write refused already takes no more frames; its refusal is read below.
            self.frames
                .send(v1::WriteFrame { frame: Some(frame) })
                .await
                .ok();
        }
        loop {
            match self.acks.message().await {
                Err(status) => return Ok(error(&status)),
                Ok(Some(v1::WriteAck {
                    ack: Some(v1::write_ack::Ack::Error(refusal)),
                })) => {
                    return ConnectorError::try_from(refusal).map_err(|invalid| {
                        Violation::from(format!("the write's error is malformed: {invalid}"))
                    });
                }
                Ok(
                    Some(v1::WriteAck {
                        ack: Some(v1::write_ack::Ack::Flushed(_)),
                    })
                    | None,
                ) => return Err(Violation::from("the write took the batch")),
                Ok(Some(_)) => {}
            }
        }
    }
}

/// A session opened on the connector `client` speaks to, and a table of its own created in it.
async fn created(client: &mut Client) -> Result<(u64, TableRef), Violation> {
    let failed =
        |what: &str, status: &Status| Violation::from(format!("{what} failed: {}", error(status)));
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let name = format!("certify_protocol_{run:x}");
    let opened = client
        .open(v1::OpenRequest {
            pipeline: name.clone(),
            load_id: LoadId::from_parts(SystemTime::now(), run)
                .as_bytes()
                .to_vec()
                .into(),
        })
        .await
        .map_err(|status| failed("the open", &status))?
        .into_inner();
    let table = TableRef {
        path: TablePath::new([name.as_str()]).expect("table paths are valid"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("the schema is valid");
    let create = TableChange::Create {
        table: table.clone(),
        schema,
    };
    client
        .apply_schema(v1::ApplySchemaRequest {
            session: opened.session,
            change: Some(v1::TableChange::from(&create)),
        })
        .await
        .map_err(|status| failed("creating the table", &status))?;
    Ok((opened.session, table))
}

/// A batch of the table's one column.
pub(super) fn rows() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    RecordBatch::try_from_iter([("id", ids)]).expect("the batch is valid")
}
