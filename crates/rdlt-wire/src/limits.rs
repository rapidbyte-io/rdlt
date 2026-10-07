//! Limits each end of a connection enforces on what it receives, and the typed refusal a value
//! beyond one gets.

#[cfg(test)]
mod tests;

use crate::v1;

/// Bytes: bounds one frame, its header and body together.
pub const FRAME_BYTES: u64 = 64 * 1024 * 1024;

/// Rows: bounds one Arrow batch.
pub const BATCH_ROWS: u64 = 1024 * 1024;

/// Values: bounds what one frame's columns hold together, nested values and the items of list
/// views included, whether or not they take bytes of its body.
///
/// A frame at [`FRAME_BYTES`] holds this many one-byte values.
pub const BATCH_VALUES: u64 = 64 * BATCH_ROWS;

/// Bytes: the least frame limit a peer may set.
///
/// A sender cuts a batch to its receiver's limits, and every frame costs it each buffer's
/// padding, whatever its rows hold: a receiver may not ask for frames so small that most of what
/// crosses is padding. One row of a schema at the column limit fits a frame of this size.
pub const MIN_FRAME_BYTES: u64 = 4 * 1024 * 1024;

/// Rows: the least rows a peer may limit a batch to, for the reason of [`MIN_FRAME_BYTES`].
pub const MIN_BATCH_ROWS: u64 = 1024;

/// Values: the least values a peer may limit a frame to, for the reason of
/// [`MIN_FRAME_BYTES`]: a hundred rows of a schema at the column limit.
pub const MIN_BATCH_VALUES: u64 = 1024 * 1024;

/// Bytes: the least a peer may limit the dictionaries one read or write holds at once to: a
/// dictionary of some thousands of short values.
pub const MIN_DICTIONARY_BYTES: u64 = 256 * 1024;

/// Columns: bounds one schema's width, counting nested fields.
pub const SCHEMA_COLUMNS: u64 = 10_000;

/// Levels: bounds how deep a schema's types nest, counting a top-level column as the first.
pub const NESTING_DEPTH: u64 = 64;

/// Bytes: bounds one schema message, and the names, metadata and time zones it carries, counted
/// wherever a field repeats them.
pub const SCHEMA_BYTES: u64 = 4 * 1024 * 1024;

/// Frames: the dictionaries one read or write holds at once take at most this many frames'
/// bytes, each dictionary counted by the allocation its frame decoded into.
pub const DICTIONARY_FRAMES: u64 = 1;

/// Frames: one write stages at most this many frames' bytes between two flushes, which a
/// destination's writer may keep until it flushes.
pub const STAGED_FRAMES: u64 = 4;

/// Bytes: bounds one JSON push.
pub const JSON_PUSH_BYTES: u64 = 64 * 1024 * 1024;

/// Bytes: bounds one cursor.
pub const CURSOR_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes: bounds the configuration document.
pub const CONFIG_BYTES: u64 = 8 * 1024 * 1024;

/// Bytes: bounds one string field of a control message.
pub const CONTROL_STRING_BYTES: u64 = 64 * 1024;

/// Bytes: bounds one catalog, a discovery's answer.
pub const CATALOG_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes: bounds what one message that carries state or a stream's positions holds decoded, as
/// its scan counts it, and so its bytes on the wire too: an open's answer, a plan and its
/// request, a commit's request and a report of committed positions.
pub const STATE_BYTES: u64 = 16 * 1024 * 1024;

/// Bytes: bounds any other control message.
pub const CONTROL_MESSAGE_BYTES: u64 = 256 * 1024;

/// Bytes: bounds a handshake and its answer, which come before either end knows the other's
/// limits, and a configuration's answer: a connector's spec, and a destination's identifier
/// rules at their limits within it, fit.
pub const HANDSHAKE_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes a message of a catalog, a schema change or any other control message may hold once
/// decoded, for each byte it takes on the wire.
///
/// A schema's columns, each a few bytes on the wire, hold tens decoded. A handshake, a
/// configuration and a read's start may hold 4 for each byte, and a frame of a read or a write 2:
/// its data is held at its length. State is bounded on what it holds decoded, so its bound on
/// the wire is the same.
pub const DECODED_PER_BYTE: usize = 16;

/// Bytes: what a message holds beside the field a limit bounds.
const MESSAGE_OVERHEAD: usize = 64 * 1024;

/// Bytes: bounds the text of a panic the decoder contained, as its error carries it.
pub const PANIC_TEXT_BYTES: u64 = 256;

/// Bytes: the least window of credit a receiver grants a call, and each call's transport window:
/// one frame at the least frame limit a peer may set.
pub const CREDIT_FLOOR: u64 = MIN_FRAME_BYTES;

/// The calls one connection may hold open at once: a host's reads, writes, heartbeat and
/// control calls.
pub const MAX_CALLS: u32 = 200;

/// Bytes: the most one transport frame carries; a ping waits for at most one.
pub const TRANSPORT_FRAME_BYTES: u32 = 1024 * 1024;

/// Bytes: the HTTP/2 connection window either end grants, HTTP/2's largest.
///
/// Credit and each stream's window bound what a peer sends, so frames the engine has not yet
/// taken never starve the connection's other streams, its heartbeat among them.
pub const CONNECTION_WINDOW: u32 = (1 << 31) - 1;

/// Bytes: the headers and trailers a host accepts in one response; a failed call's trailers carry
/// its error, whose message is at most [`CONTROL_STRING_BYTES`], in base64.
pub const HEADER_LIST_BYTES: u32 = 256 * 1024;

/// The code of every refusal.
pub const LIMIT_EXCEEDED: &str = "limit_exceeded";

/// The field each refusal names; a receiver that decodes a refusal keeps these.
pub const FIELDS: &[&str] = &[
    "frame bytes",
    "batch rows",
    "schema columns",
    "nesting depth",
    "json push bytes",
    "cursor bytes",
    "config bytes",
    "control string bytes",
    "batch values",
    "schema bytes",
    "view bytes",
    "dictionary bytes",
    "staged bytes",
    "catalog bytes",
    "state bytes",
    "control message bytes",
];

/// The code of a limit a peer set below the protocol's minimum.
pub const LIMIT_BELOW_MINIMUM: &str = "limit_below_minimum";

/// A limit a peer set below the protocol's minimum, refused at the handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the peer's limit of {field} is {actual}, below the protocol's minimum of {minimum}")]
pub struct Shortfall {
    /// Always [`LIMIT_BELOW_MINIMUM`].
    pub code: &'static str,
    /// The limit, as a [`Refusal`] names it.
    pub field: &'static str,
    /// The least a peer may set it to.
    pub minimum: u64,
    /// What the peer set.
    pub actual: u64,
}

/// The limits one end enforces on what it receives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Bytes in one frame.
    pub frame_bytes: u64,
    /// Rows in one batch.
    pub batch_rows: u64,
    /// Columns in one schema.
    pub schema_columns: u64,
    /// Levels of nesting.
    pub nesting_depth: u64,
    /// Bytes in one JSON push.
    pub json_push_bytes: u64,
    /// Bytes in one cursor.
    pub cursor_bytes: u64,
    /// Bytes in the configuration document.
    pub config_bytes: u64,
    /// Bytes in one string field of a control message.
    pub control_string_bytes: u64,
    /// Values in one frame.
    pub batch_values: u64,
    /// Bytes in one schema message.
    pub schema_bytes: u64,
    /// Bytes the dictionaries one read or write holds at once take, where that is less than
    /// [`DICTIONARY_FRAMES`] frames' bytes.
    pub dictionary_bytes: u64,
    /// Bytes in one catalog.
    pub catalog_bytes: u64,
    /// Bytes one message that carries state or a stream's positions holds decoded, as its
    /// [scan](crate::scan) counts it, and so may take on the wire.
    pub state_bytes: u64,
    /// Bytes in any other control message.
    pub control_message_bytes: u64,
}

/// What a message is, for the bytes it may hold before it is decoded: a decoder holds what a
/// message's fields become, many times what they took on the wire, so each kind of message is
/// bounded by what it carries.
///
/// [`Class::of_request`] and [`Class::of_answer`] say the class of each call's messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// A handshake or its answer.
    Handshake,
    /// A control message carrying none of the below.
    Control,
    /// A catalog.
    Catalog,
    /// State or a stream's positions.
    State,
    /// A configuration.
    Config,
    /// A schema change.
    Schema,
    /// A read's start, its cursor.
    Cursor,
    /// A stream of frames.
    Data,
}

impl Class {
    /// Bytes a message of the class may hold once decoded for each byte it takes on the wire.
    pub const fn decoded_per_byte(self) -> usize {
        match self {
            Self::Data => 2,
            Self::Handshake | Self::Config | Self::Cursor => 4,
            Self::State => 1,
            Self::Control | Self::Catalog | Self::Schema => DECODED_PER_BYTE,
        }
    }

    /// Every class.
    pub const ALL: [Self; 8] = [
        Self::Handshake,
        Self::Control,
        Self::Catalog,
        Self::State,
        Self::Config,
        Self::Schema,
        Self::Cursor,
        Self::Data,
    ];

    /// The class of a request of `method`, a method of the protocol's service.
    pub fn of_request(method: &str) -> Self {
        match method {
            "Handshake" => Self::Handshake,
            "Configure" => Self::Config,
            "ApplySchema" => Self::Schema,
            "Read" => Self::Cursor,
            "Plan" | "Committed" | "Commit" => Self::State,
            "Write" => Self::Data,
            _ => Self::Control,
        }
    }

    /// The class of an answer of `method`, a method of the protocol's service.
    pub fn of_answer(method: &str) -> Self {
        match method {
            // The configuration's answer carries the spec, as the handshake's does.
            "Handshake" | "Configure" => Self::Handshake,
            "Discover" => Self::Catalog,
            "Plan" | "Open" => Self::State,
            "Read" | "ReadPublished" => Self::Data,
            _ => Self::Control,
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            frame_bytes: FRAME_BYTES,
            batch_rows: BATCH_ROWS,
            schema_columns: SCHEMA_COLUMNS,
            nesting_depth: NESTING_DEPTH,
            json_push_bytes: JSON_PUSH_BYTES,
            cursor_bytes: CURSOR_BYTES,
            config_bytes: CONFIG_BYTES,
            control_string_bytes: CONTROL_STRING_BYTES,
            batch_values: BATCH_VALUES,
            schema_bytes: SCHEMA_BYTES,
            dictionary_bytes: FRAME_BYTES * DICTIONARY_FRAMES,
            catalog_bytes: CATALOG_BYTES,
            state_bytes: STATE_BYTES,
            control_message_bytes: CONTROL_MESSAGE_BYTES,
        }
    }
}

/// A value beyond a limit, refused where it was received.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{field} is {actual}, beyond the limit of {limit}")]
pub struct Refusal {
    /// Always [`LIMIT_EXCEEDED`].
    pub code: &'static str,
    /// What was measured, with its unit.
    pub field: &'static str,
    /// The limit.
    pub limit: u64,
    /// The value.
    pub actual: u64,
}

impl Limits {
    /// Admits `actual` of `field` if it is at most `limit`.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] naming `field` when `actual` exceeds `limit`.
    pub fn admit(field: &'static str, limit: u64, actual: u64) -> Result<(), Refusal> {
        if actual <= limit {
            return Ok(());
        }
        Err(Refusal {
            code: LIMIT_EXCEEDED,
            field,
            limit,
            actual,
        })
    }

    /// The most bytes a message of `class` may hold once decoded, as its
    /// [scan](crate::scan) counts it before it is decoded: so many times its wire bound, as
    /// [`DECODED_PER_BYTE`] says for each class.
    pub fn decoded(&self, class: Class) -> usize {
        self.decoding(class)
            .saturating_mul(class.decoded_per_byte())
    }

    /// The most bytes a message of any class may take on the wire: the most a decoder that takes
    /// every class's messages, or an encoder that sends them, is set to.
    pub fn largest(&self) -> usize {
        Class::ALL
            .iter()
            .map(|class| self.decoding(*class))
            .max()
            .unwrap_or(0)
    }

    /// The most bytes a message of `class` may take on the wire before it is decoded.
    pub fn decoding(&self, class: Class) -> usize {
        let bytes = |limit: u64| usize::try_from(limit).unwrap_or(usize::MAX);
        let around = |limit: u64| bytes(limit).saturating_add(MESSAGE_OVERHEAD);
        match class {
            Class::Handshake => bytes(HANDSHAKE_BYTES),
            Class::Control => bytes(self.control_message_bytes),
            Class::Catalog => bytes(self.catalog_bytes),
            Class::State => bytes(self.state_bytes),
            Class::Config => around(self.config_bytes),
            Class::Schema => around(self.schema_bytes),
            Class::Cursor => around(self.cursor_bytes),
            Class::Data => around(self.frame_bytes),
        }
    }

    /// Bytes: bounds the dictionaries one read or write holds at once: [`DICTIONARY_FRAMES`]
    /// frames' worth, or [`Limits::dictionary_bytes`] where that is less.
    pub fn held_dictionary_bytes(&self) -> u64 {
        let frames = self.frame_bytes.saturating_mul(DICTIONARY_FRAMES);
        frames.min(self.dictionary_bytes)
    }

    /// Bytes: bounds what one write stages between two flushes, [`STAGED_FRAMES`] frames' worth.
    pub fn staged_bytes(&self) -> u64 {
        self.frame_bytes.saturating_mul(STAGED_FRAMES)
    }

    /// Admits the dictionaries a decoder would hold, `bytes` together.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when they exceed [`Limits::held_dictionary_bytes`].
    pub fn admit_dictionaries(&self, bytes: u64) -> Result<(), Refusal> {
        Self::admit("dictionary bytes", self.held_dictionary_bytes(), bytes)
    }

    /// Admits what a write would have staged since its last flush, `bytes` together.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when it exceeds [`Limits::staged_bytes`].
    pub fn admit_staged(&self, bytes: u64) -> Result<(), Refusal> {
        Self::admit("staged bytes", self.staged_bytes(), bytes)
    }

    /// Admits a frame of `bytes`.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when the frame exceeds [`Limits::frame_bytes`].
    pub fn admit_frame(&self, bytes: usize) -> Result<(), Refusal> {
        Self::admit("frame bytes", self.frame_bytes, len(bytes))
    }

    /// The lesser of each of these limits and of `other`'s: what a sender keeps within, of its
    /// receiver's limits and its own.
    #[must_use]
    pub fn lesser(&self, other: &Self) -> Self {
        Self {
            frame_bytes: self.frame_bytes.min(other.frame_bytes),
            batch_rows: self.batch_rows.min(other.batch_rows),
            schema_columns: self.schema_columns.min(other.schema_columns),
            nesting_depth: self.nesting_depth.min(other.nesting_depth),
            json_push_bytes: self.json_push_bytes.min(other.json_push_bytes),
            cursor_bytes: self.cursor_bytes.min(other.cursor_bytes),
            config_bytes: self.config_bytes.min(other.config_bytes),
            control_string_bytes: self.control_string_bytes.min(other.control_string_bytes),
            batch_values: self.batch_values.min(other.batch_values),
            schema_bytes: self.schema_bytes.min(other.schema_bytes),
            dictionary_bytes: self.dictionary_bytes.min(other.dictionary_bytes),
            catalog_bytes: self.catalog_bytes.min(other.catalog_bytes),
            state_bytes: self.state_bytes.min(other.state_bytes),
            control_message_bytes: self.control_message_bytes.min(other.control_message_bytes),
        }
    }

    /// Admits these limits as a peer's: each limit a sender cuts batches to is at least the
    /// protocol's minimum, so no peer makes its sender send frames of mostly padding.
    ///
    /// # Errors
    ///
    /// A [`Shortfall`] naming the first limit below its minimum.
    pub fn admit_peer(&self) -> Result<(), Shortfall> {
        let floors = [
            ("frame bytes", MIN_FRAME_BYTES, self.frame_bytes),
            ("batch rows", MIN_BATCH_ROWS, self.batch_rows),
            ("batch values", MIN_BATCH_VALUES, self.batch_values),
            (
                "dictionary bytes",
                MIN_DICTIONARY_BYTES,
                self.dictionary_bytes,
            ),
        ];
        match floors
            .into_iter()
            .find(|(_, minimum, actual)| actual < minimum)
        {
            None => Ok(()),
            Some((field, minimum, actual)) => Err(Shortfall {
                code: LIMIT_BELOW_MINIMUM,
                field,
                minimum,
                actual,
            }),
        }
    }

    /// Admits a schema message of `bytes`.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when the message exceeds [`Limits::schema_bytes`].
    pub fn admit_schema(&self, bytes: usize) -> Result<(), Refusal> {
        Self::admit("schema bytes", self.schema_bytes, len(bytes))
    }

    /// Admits a string field of a control message.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when the string exceeds [`Limits::control_string_bytes`].
    pub fn admit_string(&self, value: &str) -> Result<(), Refusal> {
        Self::admit(
            "control string bytes",
            self.control_string_bytes,
            len(value.len()),
        )
    }

    /// Admits a JSON push.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when the push exceeds [`Limits::json_push_bytes`].
    pub fn admit_json(&self, bytes: usize) -> Result<(), Refusal> {
        Self::admit("json push bytes", self.json_push_bytes, len(bytes))
    }

    /// Admits a cursor.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when the cursor exceeds [`Limits::cursor_bytes`].
    pub fn admit_cursor(&self, bytes: usize) -> Result<(), Refusal> {
        Self::admit("cursor bytes", self.cursor_bytes, len(bytes))
    }

    /// Admits a configuration document.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when the document exceeds [`Limits::config_bytes`].
    pub fn admit_config(&self, bytes: usize) -> Result<(), Refusal> {
        Self::admit("config bytes", self.config_bytes, len(bytes))
    }
}

/// A length as the `u64` limits count in.
fn len(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

impl From<Limits> for v1::Limits {
    fn from(limits: Limits) -> Self {
        Self {
            frame_bytes: limits.frame_bytes,
            batch_rows: limits.batch_rows,
            schema_columns: limits.schema_columns,
            nesting_depth: limits.nesting_depth,
            json_push_bytes: limits.json_push_bytes,
            cursor_bytes: limits.cursor_bytes,
            config_bytes: limits.config_bytes,
            control_string_bytes: limits.control_string_bytes,
            batch_values: limits.batch_values,
            schema_bytes: limits.schema_bytes,
            dictionary_bytes: limits.dictionary_bytes,
            catalog_bytes: limits.catalog_bytes,
            state_bytes: limits.state_bytes,
            control_message_bytes: limits.control_message_bytes,
        }
    }
}

impl From<v1::Limits> for Limits {
    /// The limits a peer sent; a limit it left unset, as a peer from before that limit existed
    /// does, is the protocol's default.
    fn from(limits: v1::Limits) -> Self {
        let defaults = Self::default();
        let or = |sent: u64, default: u64| if sent == 0 { default } else { sent };
        Self {
            frame_bytes: or(limits.frame_bytes, defaults.frame_bytes),
            batch_rows: or(limits.batch_rows, defaults.batch_rows),
            schema_columns: or(limits.schema_columns, defaults.schema_columns),
            nesting_depth: or(limits.nesting_depth, defaults.nesting_depth),
            json_push_bytes: or(limits.json_push_bytes, defaults.json_push_bytes),
            cursor_bytes: or(limits.cursor_bytes, defaults.cursor_bytes),
            config_bytes: or(limits.config_bytes, defaults.config_bytes),
            control_string_bytes: or(limits.control_string_bytes, defaults.control_string_bytes),
            batch_values: or(limits.batch_values, defaults.batch_values),
            schema_bytes: or(limits.schema_bytes, defaults.schema_bytes),
            // A limit of its own the peer sets as it is, none below the protocol's least.
            dictionary_bytes: limits.dictionary_bytes,
            catalog_bytes: or(limits.catalog_bytes, defaults.catalog_bytes),
            state_bytes: or(limits.state_bytes, defaults.state_bytes),
            control_message_bytes: or(limits.control_message_bytes, defaults.control_message_bytes),
        }
    }
}
