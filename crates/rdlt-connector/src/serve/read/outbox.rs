//! The frames a served read writes, queued: each event the source sends as the frames that carry
//! it, a batch cut to the host's frames a piece at a time.

use std::collections::VecDeque;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_wire::stream::{Kind, Outgoing, Rows};
use rdlt_wire::{Cut, Encoder, Limits};

use super::refused;
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::serve::noted::Note;
use crate::sink::{LogLevel, Push, SourceEvent};
use crate::wire::{frame_error, v1};

/// A frame queued to be written, and what writing it lets the host report.
pub(in crate::serve) struct Queued {
    pub(in crate::serve) frame: Outgoing,
    pub(in crate::serve) note: Note,
}

/// Frames waiting to be written, and what encodes them.
pub(in crate::serve) struct Outbox {
    frames: VecDeque<Queued>,
    encoder: Encoder,
    schema: Option<SchemaRef>,
    epoch: u64,
    /// The host's limits, which every frame written keeps within.
    host: Limits,
    /// The batch whose pieces are being written, and what its rows are.
    cutting: Option<(Cut, Rows)>,
}

impl Outbox {
    /// An empty outbox, whose frames keep within `host`'s limits.
    pub(in crate::serve) fn new(host: Limits) -> Self {
        Self {
            frames: VecDeque::new(),
            encoder: Encoder::default(),
            schema: None,
            epoch: 0,
            host,
            cutting: None,
        }
    }

    /// The next frame to write, where one is queued.
    pub(in crate::serve) fn next(&mut self) -> Option<Queued> {
        self.frames.pop_front()
    }

    /// Queues `message` as a message frame, noted as `note`.
    pub(in crate::serve) fn message(
        &mut self,
        message: v1::read_frame::Frame,
        note: Note,
    ) -> Result<(), ConnectorError> {
        let frame = v1::ReadFrame {
            frame: Some(message),
        };
        let frame = Outgoing::message(None, &frame).map_err(too_long)?;
        self.frames.push_back(Queued { frame, note });
        Ok(())
    }

    /// Queues `event` as the frames that carry it.
    pub(in crate::serve) fn event(&mut self, event: SourceEvent) -> Result<(), ConnectorError> {
        use v1::read_frame::Frame;
        match event {
            SourceEvent::Push(Push::Arrow(batch)) => self.batch(&batch, Rows::Arrow)?,
            SourceEvent::Push(Push::Changes(batch)) => self.batch(&batch, Rows::Changes)?,
            SourceEvent::Push(Push::Json(data)) => {
                self.host.admit_json(data.len()).map_err(refused)?;
                let frame = Outgoing::new(Kind::Json, vec![data]).map_err(too_long)?;
                self.frames.push_back(Queued {
                    frame,
                    note: Note::Data,
                });
            }
            SourceEvent::Checkpoint { cursor, answers } => {
                let wire = v1::Cursor::from(&cursor);
                self.host.admit_cursor(wire.bytes.len()).map_err(refused)?;
                let checkpoint = v1::CheckpointFrame {
                    cursor: Some(wire),
                    barrier: answers,
                };
                self.message(Frame::Checkpoint(checkpoint), Note::Checkpoint(cursor))?;
            }
            SourceEvent::Log { level, message } => {
                self.host.admit_string(&message).map_err(refused)?;
                let level = log_level(level) as i32;
                self.message(Frame::Log(v1::LogFrame { level, message }), Note::Nothing)?;
            }
            SourceEvent::Metric { name, value } => {
                self.host.admit_string(&name).map_err(refused)?;
                self.message(
                    Frame::Metric(v1::MetricFrame { name, value }),
                    Note::Nothing,
                )?;
            }
            SourceEvent::Replan => {
                self.message(Frame::Replan(v1::ReplanFrame {}), Note::Nothing)?;
            }
            SourceEvent::Behind { records } => {
                self.message(Frame::Behind(v1::BehindFrame { records }), Note::Nothing)?;
            }
        }
        Ok(())
    }

    /// Queues `batch`, after a schema frame opening a new epoch if its schema differs.
    pub(in crate::serve) fn batch(
        &mut self,
        batch: &RecordBatch,
        rows: Rows,
    ) -> Result<(), ConnectorError> {
        use v1::read_frame::Frame;
        if self.schema.as_ref() != Some(&batch.schema()) {
            self.epoch += 1;
            self.schema = None;
            let ipc_schema = self
                .encoder
                .schema(&batch.schema())
                .map_err(|error| frame_error(&error))?;
            self.schema = Some(batch.schema());
            let schema = v1::SchemaFrame {
                schema_epoch: self.epoch,
                ipc_schema,
            };
            self.message(Frame::Schema(schema), Note::Data)?;
        }
        self.cutting = Some((Cut::new(batch.clone(), self.host), rows));
        self.refill()
    }

    /// Whether a batch is still being cut: its next piece is queued by [`Outbox::refill`], and
    /// nothing else may be queued before its last.
    pub(in crate::serve) fn cutting(&self) -> bool {
        self.cutting.is_some()
    }

    /// Queues the next piece of the batch being cut.
    ///
    /// A batch beyond the host's limits goes as several, in order, each encoded only once the
    /// piece before it was written; a row beyond them fails the read.
    pub(in crate::serve) fn refill(&mut self) -> Result<(), ConnectorError> {
        let Some((cut, rows)) = &mut self.cutting else {
            return Ok(());
        };
        let rows = *rows;
        let piece = self.encoder.piece(cut);
        if cut.is_done() {
            self.cutting = None;
        }
        let pieces = piece.map_err(|error| frame_error(&error))?;
        let tag = self.epoch;
        for encoded in pieces.into_iter().flatten() {
            let dictionary = encoded.is_dictionary();
            let kind = Kind::Batch {
                rows,
                dictionary,
                tag,
            };
            let frame = Outgoing::new(kind, encoded.into_pieces()).map_err(too_long)?;
            self.frames.push_back(Queued {
                frame,
                note: Note::Data,
            });
        }
        Ok(())
    }
}

/// A frame longer than a head can say, which the host's limits keep any frame short of.
fn too_long(error: rdlt_wire::stream::TooLong) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
}

fn log_level(level: LogLevel) -> v1::LogLevel {
    match level {
        LogLevel::Error => v1::LogLevel::Error,
        LogLevel::Warn => v1::LogLevel::Warn,
        LogLevel::Info => v1::LogLevel::Info,
        LogLevel::Debug => v1::LogLevel::Debug,
    }
}
