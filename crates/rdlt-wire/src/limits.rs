//! Limits each end of a connection enforces on what it receives, and the typed refusal a value
//! beyond one gets.

#[cfg(test)]
mod tests;

use crate::v1;

/// Bytes: bounds one frame, its header and body together.
pub const FRAME_BYTES: u64 = 64 * 1024 * 1024;

/// Rows: bounds one Arrow batch.
pub const BATCH_ROWS: u64 = 1024 * 1024;

/// Columns: bounds one schema's width, counting nested fields.
pub const SCHEMA_COLUMNS: u64 = 10_000;

/// Levels: bounds how deep a schema's types nest, counting a top-level column as the first.
pub const NESTING_DEPTH: u64 = 64;

/// Bytes: bounds one JSON push.
pub const JSON_PUSH_BYTES: u64 = 64 * 1024 * 1024;

/// Bytes: bounds one cursor.
pub const CURSOR_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes: bounds the configuration document.
pub const CONFIG_BYTES: u64 = 8 * 1024 * 1024;

/// Bytes: bounds one string field of a control message.
pub const CONTROL_STRING_BYTES: u64 = 64 * 1024;

/// Bytes: the credit a receiver grants a sender by default, before the sender's frames spend it.
pub const CREDIT_WINDOW: u64 = 4 * 1024 * 1024;

/// The code of every refusal.
pub const LIMIT_EXCEEDED: &str = "limit_exceeded";

/// The field each refusal names, one per limit; a receiver that decodes a refusal keeps these.
pub const FIELDS: &[&str] = &[
    "frame bytes",
    "batch rows",
    "schema columns",
    "nesting depth",
    "json push bytes",
    "cursor bytes",
    "config bytes",
    "control string bytes",
    "values per node",
];

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

    /// The largest protocol message either end sends or accepts: a frame, and the fields around
    /// it.
    pub fn message_bytes(&self) -> usize {
        usize::try_from(self.frame_bytes)
            .unwrap_or(usize::MAX)
            .saturating_add(64 * 1024)
    }

    /// Admits a frame of `bytes`.
    ///
    /// # Errors
    ///
    /// A [`Refusal`] when the frame exceeds [`Limits::frame_bytes`].
    pub fn admit_frame(&self, bytes: usize) -> Result<(), Refusal> {
        Self::admit("frame bytes", self.frame_bytes, len(bytes))
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
        }
    }
}
