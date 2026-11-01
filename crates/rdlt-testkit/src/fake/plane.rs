//! The fake's data plane: its reads and writes, each breaking the protocol as its fault says.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::wire::v1;
use rdlt_wire::plane::{Incoming, Plane, Serving};
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::tonic::Status;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt as _};

use super::{Answer, Fake, Fault, Granted};

/// What a flooding read's transport took of it, and when last.
#[derive(Debug)]
pub struct Polled {
    bytes: AtomicU64,
    last: Mutex<Option<Instant>>,
}

impl Polled {
    /// A count of nothing taken.
    #[must_use]
    #[expect(
        clippy::new_without_default,
        reason = "a test keeps one in a static, which only a const constructor builds"
    )]
    pub const fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            last: Mutex::new(None),
        }
    }

    pub(super) fn took(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).expect("a frame's length fits");
        self.bytes.fetch_add(bytes, Ordering::SeqCst);
        *self.last.lock() = Some(Instant::now());
    }

    /// Bytes: the frames the transport took.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::SeqCst)
    }

    /// Waits until the transport has taken a frame and then none for `quiet`.
    pub async fn settled(&self, quiet: Duration) {
        loop {
            tokio::time::sleep(quiet / 4).await;
            let last = *self.last.lock();
            if last.is_some_and(|last| last.elapsed() >= quiet) {
                return;
            }
        }
    }
}

/// The schema of a column of ids, and a batch frame of a mebibyte of them.
pub fn mebibyte() -> (v1::ReadFrame, v1::ReadFrame) {
    let values: Vec<i64> = (0..131_072).collect();
    let column = Arc::new(arrow_array::Int64Array::from(values));
    let batch = arrow_array::RecordBatch::try_from_iter([("id", column as _)]).expect("a batch");
    let mut encoder = rdlt_wire::Encoder::default();
    let schema = v1::read_frame::Frame::Schema(v1::SchemaFrame {
        schema_epoch: 1,
        ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
    });
    let mut frames = encoder.batch(&batch).expect("the batch encodes");
    let frame = frames.pop().expect("one frame");
    let batch = v1::read_frame::Frame::Batch(v1::BatchFrame {
        schema_epoch: 1,
        kind: v1::BatchKind::Arrow as i32,
        data_header: frame.header,
        padding: Bytes::new(),
        data_body: frame.body,
    });
    let message = |frame| v1::ReadFrame { frame: Some(frame) };
    (message(schema), message(batch))
}

/// Takes a read's controls until the host ends them.
async fn controlled(mut controls: Incoming<v1::ReadControl>) {
    while let Ok(Some(_)) = controls.message().await {}
}

/// Takes a read's controls until the host ends them, keeping each credit in `granted` and
/// answering it with the next of `frames`.
async fn granting(
    mut controls: Incoming<v1::ReadControl>,
    frames: Vec<v1::ReadFrame>,
    granted: &Granted,
    sent: tokio::sync::mpsc::Sender<Result<v1::ReadFrame, Status>>,
) {
    let mut frames = frames.into_iter();
    while let Ok(Some(control)) = controls.message().await {
        if let Some(v1::read_control::Control::Credit(credit)) = control.control {
            granted.lock().push(credit.bytes);
            if let Some(frame) = frames.next()
                && sent.send(Ok(frame)).await.is_err()
            {
                return;
            }
        }
    }
}

/// A frame of a lag.
fn lagged() -> v1::read_frame::Frame {
    v1::read_frame::Frame::Behind(v1::BehindFrame {
        records: 1,
        headroom: None,
    })
}

/// The IPC schema of one column of ids.
fn ids() -> Bytes {
    let arrow = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]);
    rdlt_wire::Encoder::default()
        .schema(&arrow)
        .expect("the schema encodes")
}

impl Fake {
    /// The frames a read sends, as the fault says.
    fn read_frames(&self) -> Vec<Result<v1::ReadFrame, Status>> {
        let schema = |ipc_schema| v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Schema(v1::SchemaFrame {
                schema_epoch: 1,
                ipc_schema,
            })),
        };
        if matches!(self.0, Fault::AnswersAhead) {
            let checkpoint = v1::CheckpointFrame {
                cursor: Some(v1::Cursor {
                    version: 1,
                    bytes: Bytes::from_static(b"c"),
                }),
                barrier: Some(u64::MAX),
            };
            vec![Ok(v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Checkpoint(checkpoint)),
            })]
        } else if matches!(self.0, Fault::StaleEpoch) {
            vec![Ok(schema(ids())), Ok(schema(ids()))]
        } else if let Fault::Schemas(count) | Fault::Chatters(count) = self.0 {
            let chatty = matches!(self.0, Fault::Chatters(_));
            (1..=count)
                .flat_map(|epoch| {
                    let schema = v1::read_frame::Frame::Schema(v1::SchemaFrame {
                        schema_epoch: epoch,
                        ipc_schema: ids(),
                    });
                    std::iter::once(schema).chain(chatty.then(lagged))
                })
                .map(|frame| Ok(v1::ReadFrame { frame: Some(frame) }))
                .collect()
        } else if let Fault::Lags(count) = self.0 {
            (0..count)
                .map(|_| {
                    Ok(v1::ReadFrame {
                        frame: Some(lagged()),
                    })
                })
                .collect()
        } else if let Fault::Sends(frames) = self.0 {
            let (ipc, frames) = frames();
            let batches = frames.into_iter().map(|frame| v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Batch(v1::BatchFrame {
                    schema_epoch: 1,
                    kind: v1::BatchKind::Arrow as i32,
                    data_header: frame.header,
                    padding: Bytes::new(),
                    data_body: frame.body,
                })),
            });
            std::iter::once(schema(ipc))
                .chain(batches)
                .map(Ok)
                .collect()
        } else {
            vec![Ok(schema(Bytes::from_static(b"not an IPC message")))]
        }
    }
}

impl Plane for Fake {
    fn write(&self, mut frames: Incoming<v1::WriteFrame>) -> Serving<'_, v1::WriteAck> {
        let fault = self.0;
        Box::pin(async move {
            if let Fault::Hoards(bytes) = fault {
                // The frames stay unread for as long as the write is open.
                let credit = v1::WriteAck {
                    ack: Some(v1::write_ack::Ack::Credit(v1::Credit { bytes })),
                };
                let acks = tokio_stream::iter([Ok(credit)])
                    .chain(tokio_stream::pending())
                    .map(move |ack| {
                        let _ = &frames;
                        ack
                    });
                return Ok(Box::pin(acks) as Answer<_>);
            }
            let Fault::Trickles(every, bytes) = fault else {
                return Err(Status::unimplemented("write"));
            };
            // The frames are taken and dropped, so the transport's windows never fill.
            tokio::spawn(async move { while let Ok(Some(_)) = frames.message().await {} });
            let (acks, sent) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    let credit = v1::WriteAck {
                        ack: Some(v1::write_ack::Ack::Credit(v1::Credit { bytes })),
                    };
                    if acks.send(Ok(credit)).await.is_err() {
                        return;
                    }
                }
            });
            Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(sent)) as Answer<_>)
        })
    }

    fn read(&self, controls: Incoming<v1::ReadControl>) -> Serving<'_, v1::ReadFrame> {
        Box::pin(async move {
            if matches!(self.0, Fault::Unstarted) {
                return Ok(Box::pin(answering(controls)) as Answer<_>);
            }
            if let Fault::Granted(frames, granted) = self.0 {
                let (sent, sending) = tokio::sync::mpsc::channel(1);
                tokio::spawn(granting(controls, frames(), granted, sent));
                let frames = tokio_stream::wrappers::ReceiverStream::new(sending);
                return Ok(Box::pin(frames) as Answer<_>);
            }
            if let Fault::Floods(polled) = self.0 {
                tokio::spawn(controlled(controls));
                let (schema, batch) = mebibyte();
                let frames = std::iter::once(schema).chain(std::iter::repeat(batch));
                let frames = tokio_stream::iter(frames).map(move |frame| {
                    polled.took(rdlt_wire::prost::Message::encoded_len(&frame));
                    Ok(frame)
                });
                return Ok(Box::pin(frames) as Answer<_>);
            }
            let sent = self.read_frames();
            let frames = tokio_stream::iter(sent)
                .chain(tokio_stream::pending::<Result<v1::ReadFrame, Status>>());
            Ok(Box::pin(frames) as Answer<_>)
        })
    }

    fn read_published(&self, _: v1::ReadPublishedRequest) -> Serving<'_, v1::ReadFrame> {
        Box::pin(async { Err(Status::unimplemented("read_published")) })
    }
}

/// A read with nothing to read: it answers a barrier `controls` ask for before its first credit
/// with a checkpoint, and ends at that credit.
fn answering(
    mut controls: Incoming<v1::ReadControl>,
) -> impl Stream<Item = Result<v1::ReadFrame, Status>> + Send {
    let (frames, sent) = tokio::sync::mpsc::channel(2);
    tokio::spawn(async move {
        let mut answered = Vec::new();
        while let Ok(Some(control)) = controls.message().await {
            match control.control {
                Some(v1::read_control::Control::Checkpoint(asked)) => {
                    answered.push(v1::read_frame::Frame::Checkpoint(v1::CheckpointFrame {
                        cursor: Some(v1::Cursor {
                            version: 1,
                            bytes: Bytes::new(),
                        }),
                        barrier: Some(asked.barrier),
                    }));
                }
                Some(v1::read_control::Control::Credit(_)) => break,
                _ => {}
            }
        }
        answered.push(v1::read_frame::Frame::Done(v1::Done {}));
        for frame in answered {
            let frame = v1::ReadFrame { frame: Some(frame) };
            if frames.send(Ok(frame)).await.is_err() {
                return;
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(sent)
}
