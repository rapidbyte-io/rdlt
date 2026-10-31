//! Measures a batch or dictionary frame against its schema and the limits, as its receiver does
//! before decoding it and its sender before sending it.

use std::sync::Arc;

use arrow_ipc::{Message, MetadataVersion, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use flatbuffers::InvalidFlatbuffer;

use super::IpcFrame;
use super::framing::{framing, kind};
use super::held::schema_held_bytes;
use super::shape::{Walked, walk};
use crate::error::{Frame, Problem, WireError};
use crate::limits::{Limit, Limits, count};

/// What the frames in one schema hold: a batch's columns, and each dictionary's values.
#[derive(Debug)]
pub(super) struct Columns {
    schema: SchemaRef,
    /// The field giving each dictionary its type, by the dictionary's id, in the order of the ids.
    dictionaries: Box<[(i64, FieldRef)]>,
    /// Bytes: what the schema and `dictionaries` keep.
    held: u64,
}

impl Columns {
    /// The schema the frames are in.
    pub(super) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Bytes: what the columns keep while their schema lasts: the schema, and the field of each
    /// dictionary.
    pub(super) fn held_bytes(&self) -> u64 {
        self.held
    }

    /// The columns of frames in `schema`, as a receiver converts it from its message.
    pub(super) fn new(schema: SchemaRef) -> Self {
        let mut found = Vec::new();
        dictionaries(schema.fields(), &mut found);
        // The first field naming an id gives its dictionary's type, as in Arrow's reader.
        found.sort_by_key(|(id, _)| *id);
        found.dedup_by_key(|(id, _)| *id);
        let dictionaries = found.into_boxed_slice();
        let index = count(size_of_val::<[(i64, FieldRef)]>(&dictionaries));
        let held = schema_held_bytes(&schema).saturating_add(index);
        Self {
            schema,
            dictionaries,
            held,
        }
    }

    /// The columns of a frame of dictionary `id`, its values as a batch of one column: built for
    /// each frame, so no dictionary keeps a schema of its own.
    fn values(&self, id: i64) -> Option<SchemaRef> {
        let at = self
            .dictionaries
            .binary_search_by_key(&id, |(id, _)| *id)
            .ok()?;
        let (_, field) = self.dictionaries.get(at)?;
        let DataType::Dictionary(_, values) = field.data_type() else {
            return None;
        };
        let column = Field::new("", values.as_ref().clone(), true);
        Some(Arc::new(Schema::new(vec![column])))
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
    pub(super) walked: Walked,
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
        Limits::admit(Limit::BatchRows, limits.batch_rows, rows)?;
    }
    let columns = match framed.dictionary {
        None => Arc::clone(&columns.schema),
        Some(id) => match columns.values(id) {
            Some(values) => values,
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
    let message = verified(frame, header, depth.saturating_mul(4).saturating_add(64))?;
    if message.version() != MetadataVersion::V5 {
        let found = message.version().0;
        return Err(WireError::malformed(frame, Problem::Version { found }));
    }
    Ok(message)
}

/// The IPC message `header` holds, of whichever metadata version, verified as [`message`]
/// verifies it but to a depth of `depth` tables.
pub(super) fn verified(
    frame: Frame,
    header: &[u8],
    depth: usize,
) -> Result<Message<'_>, WireError> {
    let options = flatbuffers::VerifierOptions {
        max_depth: depth,
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
    Ok(message)
}

/// Records each dictionary field of `fields` and of the fields nested in them, with its id, in the
/// order they lie.
fn dictionaries(fields: &Fields, found: &mut Vec<(i64, FieldRef)>) {
    for field in fields {
        if let DataType::Dictionary(..) = field.data_type() {
            #[expect(
                deprecated,
                reason = "Arrow's reader finds a column's dictionary by this id"
            )]
            let id = field.dict_id();
            if let Some(id) = id {
                found.push((id, Arc::clone(field)));
            }
        }
        nested(field.data_type(), found);
    }
}

/// Records the dictionary fields nested in a value of `data_type`.
fn nested(data_type: &DataType, found: &mut Vec<(i64, FieldRef)>) {
    match data_type {
        DataType::Struct(fields) => dictionaries(fields, found),
        DataType::Union(fields, _) => {
            let fields: Fields = fields.iter().map(|(_, field)| Arc::clone(field)).collect();
            dictionaries(&fields, found);
        }
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => dictionaries(&Fields::from(vec![Arc::clone(item)]), found),
        DataType::RunEndEncoded(ends, item) => {
            dictionaries(
                &Fields::from(vec![Arc::clone(ends), Arc::clone(item)]),
                found,
            );
        }
        DataType::Dictionary(_, value) => nested(value, found),
        _ => {}
    }
}
