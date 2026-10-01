//! Measures a batch or dictionary frame against its schema and the limits, as its receiver does
//! before decoding it and its sender before sending it.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_ipc::{Message, MetadataVersion, RecordBatch};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use flatbuffers::InvalidFlatbuffer;

use super::IpcFrame;
use super::framing::{framing, kind};
use super::shape::{Walked, walk};
use crate::error::{Frame, Problem, WireError};
use crate::limits::Limits;

/// What the frames in one schema hold: a batch's columns, and each dictionary's values.
#[derive(Debug)]
pub(super) struct Columns {
    schema: SchemaRef,
    /// Each dictionary's values as a batch of one column, by the dictionary's id.
    values: HashMap<i64, SchemaRef>,
}

impl Columns {
    /// The columns of frames in `schema`, as a receiver converts it from its message.
    pub(super) fn new(schema: SchemaRef) -> Self {
        let mut values = HashMap::new();
        dictionaries(schema.fields(), &mut values);
        Self { schema, values }
    }
}

/// A frame within the limits: what it describes, and the columns Arrow reads it as.
pub(super) struct Measured<'a> {
    /// Whether the frame is a record batch or a dictionary batch.
    pub(super) frame: Frame,
    /// The nodes and buffers it describes.
    pub(super) batch: RecordBatch<'a>,
    /// The id of the dictionary it replaces, when it is a dictionary batch.
    pub(super) dictionary: Option<i64>,
    /// The schema of its columns: the batch's, or a dictionary's values as one column.
    pub(super) columns: SchemaRef,
    /// Its buffers, and what it holds.
    pub(super) walked: Walked<'a>,
}

/// Measures `frame`, a batch or dictionary frame in the schema of `columns`.
///
/// # Errors
///
/// A [`WireError`] when the frame is too large, arrives before a schema, is malformed, or holds
/// more than `limits` allow.
pub(super) fn measured<'a>(
    columns: Option<&Columns>,
    limits: &Limits,
    frame: &'a IpcFrame,
) -> Result<Measured<'a>, WireError> {
    limits.admit_frame(frame.header.len().saturating_add(frame.body.len()))?;
    let message = message(Frame::Batch, &frame.header, limits.nesting_depth)?;
    let framed = framing(&message, frame.body.len())?;
    let malformed = |problem| WireError::malformed(framed.frame, problem);
    let Some(columns) = columns else {
        return Err(malformed(Problem::NoSchema));
    };
    // A batch's rows have a limit of their own; a dictionary's entries are values of its frame,
    // which the walk counts.
    if framed.dictionary.is_none() {
        let rows = u64::try_from(framed.batch.length()).unwrap_or(u64::MAX);
        Limits::admit("batch rows", limits.batch_rows, rows)?;
    }
    let columns = match framed.dictionary {
        None => Arc::clone(&columns.schema),
        Some(id) => match columns.values.get(&id) {
            Some(values) => Arc::clone(values),
            None => return Err(malformed(Problem::UnknownDictionary { id })),
        },
    };
    let types = columns.fields().iter().map(|field| field.data_type());
    let walked = walk(framed.frame, framed.batch, types, &frame.body, limits)?;
    Ok(Measured {
        frame: framed.frame,
        batch: framed.batch,
        dictionary: framed.dictionary,
        columns,
        walked,
    })
}

/// A `frame` whose header holds another kind of message.
pub(super) fn unexpected(frame: Frame, message: &Message<'_>) -> WireError {
    WireError::malformed(
        frame,
        Problem::Unexpected {
            found: kind(message),
        },
    )
}

/// The IPC message `header` holds, of the metadata version both ends speak.
///
/// It is verified to a depth that fits a schema nested `depth` levels, so a schema deeper than
/// that is refused by the nesting limit rather than as no message, and within what a message of
/// its length can hold: a table every four bytes, and sixteen times its bytes once every table
/// and string is counted wherever it repeats.
pub(super) fn message(frame: Frame, header: &[u8], depth: u64) -> Result<Message<'_>, WireError> {
    let depth = usize::try_from(depth).unwrap_or(usize::MAX);
    let options = flatbuffers::VerifierOptions {
        max_depth: depth.saturating_mul(4).saturating_add(64),
        max_tables: header.len() / 4,
        max_apparent_size: header.len().saturating_mul(16),
        ..flatbuffers::VerifierOptions::default()
    };
    let message =
        arrow_ipc::root_as_message_with_opts(&options, header).map_err(|error| match error {
            InvalidFlatbuffer::TooManyTables | InvalidFlatbuffer::ApparentSizeTooLarge => {
                WireError::malformed(frame, Problem::Inflated)
            }
            _ => WireError::malformed(frame, Problem::NotAMessage),
        })?;
    if message.version() != MetadataVersion::V5 {
        let found = message.version().0;
        return Err(WireError::malformed(frame, Problem::Version { found }));
    }
    Ok(message)
}

/// Records, for each dictionary `fields` or the fields nested in them use, its values as a batch
/// of one column; the first field naming an id gives its dictionary's type, as in Arrow's reader.
fn dictionaries(fields: &Fields, values: &mut HashMap<i64, SchemaRef>) {
    for field in fields {
        if let DataType::Dictionary(_, value) = field.data_type() {
            #[expect(
                deprecated,
                reason = "Arrow's reader finds a column's dictionary by this id"
            )]
            let id = field.dict_id();
            if let Some(id) = id {
                values.entry(id).or_insert_with(|| {
                    let column = Field::new("", value.as_ref().clone(), true);
                    Arc::new(Schema::new(vec![column]))
                });
            }
        }
        nested(field.data_type(), values);
    }
}

/// Records the dictionaries of the fields nested in a value of `data_type`.
fn nested(data_type: &DataType, values: &mut HashMap<i64, SchemaRef>) {
    match data_type {
        DataType::Struct(fields) => dictionaries(fields, values),
        DataType::Union(fields, _) => {
            let fields: Fields = fields.iter().map(|(_, field)| Arc::clone(field)).collect();
            dictionaries(&fields, values);
        }
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => dictionaries(&Fields::from(vec![Arc::clone(item)]), values),
        DataType::RunEndEncoded(ends, item) => {
            dictionaries(
                &Fields::from(vec![Arc::clone(ends), Arc::clone(item)]),
                values,
            );
        }
        DataType::Dictionary(_, value) => nested(value, values),
        _ => {}
    }
}
