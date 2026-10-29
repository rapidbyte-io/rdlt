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
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::stream::{Call, Kind, Outgoing, Rows};
use rdlt_wire::tonic::Status;

use super::{Violation, error};
use rdlt_host::remote::RawCall;

/// A write of a one-column table, its schema sent, waiting for batches.
pub(super) struct Writing {
    streamed: RawCall,
}

impl Writing {
    /// Opens a session on the connector `client` speaks to, creates a table of its own in it, and
    /// starts writing the table, its schema sent, reading within `limits`.
    pub(super) async fn start(
        client: &mut Client,
        limits: &rdlt_wire::Limits,
    ) -> Result<Self, Violation> {
        let (session, table) = created(client).await?;
        let start = v1::WriteStart {
            session,
            table: Some(v1::TableRef::from(&table)),
        };
        let opened = RawCall::open(client, Call::Write, &start, limits).await;
        let mut streamed = opened
            .map_err(Violation::of)?
            .map_err(|refused| Violation::from(format!("the write was refused: {refused}")))?;
        let ipc_schema = rdlt_wire::Encoder::default()
            .schema(&rows().schema())
            .map_err(|error| Violation::from(format!("the schema does not encode: {error}")))?;
        let schema = v1::WriteFrame {
            frame: Some(v1::write_frame::Frame::Schema(v1::WriteSchema {
                version: 1,
                ipc_schema,
            })),
        };
        let schema = Outgoing::message(None, &schema).map_err(Violation::of)?;
        if !streamed.send(schema).await {
            return Err(Violation::from("the write ended before its schema"));
        }
        Ok(Self { streamed })
    }

    /// Sends a batch frame of `body`, and a flush after it, and returns the error the write then
    /// fails with.
    pub(super) async fn refusal(mut self, body: Bytes) -> Result<ConnectorError, Violation> {
        let kind = Kind::Batch {
            rows: Rows::Arrow,
            dictionary: false,
            tag: 1,
        };
        let batch = Outgoing::new(kind, vec![body]).map_err(Violation::of)?;
        // A flush, so a destination that took the batch says so rather than wait for more.
        let flush = v1::WriteFrame {
            frame: Some(v1::write_frame::Frame::Flush(v1::Unit {})),
        };
        let flush = Outgoing::message(None, &flush).map_err(Violation::of)?;
        // A write refused already takes no more frames; its refusal is read below.
        if self.streamed.send(batch).await {
            self.streamed.send(flush).await;
        }
        match self.streamed.refusal().await {
            Ok(Some(refused)) | Err(refused) => Ok(refused),
            Ok(None) => Err(Violation::from("the write took the batch")),
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
    // Short enough for any destination the destination clauses certify: 32 bytes and more.
    let name = format!("certify_{run:x}_p");
    let opened = client
        .rpc
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
        .rpc
        .apply_schema(v1::ApplySchemaRequest {
            session: opened.session,
            change: Some(v1::TableChange::from(&create)),
        })
        .await
        .map_err(|status| failed("creating the table", &status))?;
    Ok((opened.session, table))
}

/// A batch frame's body of the table's rows whose values buffer starts four bytes into the
/// message's body, where no eight-byte value may be read in place: its message otherwise as a
/// sender encodes it.
///
/// # Errors
///
/// A violation where the batch does not encode, which never happens.
pub(super) fn misaligned() -> Result<Bytes, Violation> {
    let unencoded = |error: rdlt_wire::WireError| Violation::from(format!("{error}"));
    let mut encoder = rdlt_wire::Encoder::default();
    encoder.schema(&rows().schema()).map_err(unencoded)?;
    let mut frames = encoder.batch(&rows()).map_err(unencoded)?;
    let frame = frames
        .pop()
        .ok_or_else(|| Violation::from("the rows encode to no frame"))?;
    // The values buffer, of three eight-byte values' bytes, starts four bytes
    // later, still within the body and after the buffer before it.
    let mut header = frame.header.to_vec();
    let length = 24_i64.to_le_bytes();
    let at = (0..header.len().saturating_sub(15))
        .find(|&at| header[at + 8..at + 16] == length && header[at..at + 8] != length)
        .ok_or_else(|| Violation::from("the rows' values buffer is not where it is written"))?;
    let moved = i64::from_le_bytes(header[at..at + 8].try_into().unwrap_or_default())
        .checked_add(4)
        .ok_or_else(|| Violation::from("the rows' values buffer lies at no offset"))?;
    header[at..at + 8].copy_from_slice(&moved.to_le_bytes());
    Ok(encapsulated(&header, &frame.body))
}

/// The bytes of an encapsulated IPC message of `header`, its metadata, and `body`.
fn encapsulated(header: &[u8], body: &[u8]) -> Bytes {
    let mut bytes = Vec::with_capacity(8 + header.len() + body.len());
    bytes.extend_from_slice(&[0xff; 4]);
    let len = i32::try_from(header.len()).unwrap_or(i32::MAX);
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(header);
    bytes.extend_from_slice(body);
    Bytes::from(bytes)
}

/// A batch of the table's one column.
pub(super) fn rows() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    RecordBatch::try_from_iter([("id", ids)]).expect("the batch is valid")
}
