//! Bounds a schema message before it is converted: its columns, its nesting, each field's name,
//! and the text it carries, counted wherever a field repeats it.

#[cfg(test)]
mod tests;

use arrow_ipc::{Endianness, Field, KeyValue, Schema};
use flatbuffers::{ForwardsUOffset, Vector};

use crate::error::{Frame, Problem, WireError};
use crate::limits::Limits;

/// Admits `schema` if converting it holds no more than `limits` allow.
///
/// # Errors
///
/// A [`WireError`] naming the limit the schema exceeds, or a big-endian schema.
pub(super) fn admit(schema: Schema<'_>, limits: &Limits) -> Result<(), WireError> {
    if schema.endianness() != Endianness::Little {
        return Err(WireError::malformed(Frame::Schema, Problem::BigEndian));
    }
    let mut measured = Measured {
        limits,
        columns: 0,
        text: 0,
    };
    measured.metadata(schema.custom_metadata())?;
    measured.fields(schema.fields(), 1)
}

/// What a schema holds, counted so far.
struct Measured<'l> {
    limits: &'l Limits,
    columns: u64,
    text: u64,
}

impl Measured<'_> {
    /// Counts `fields`, at `depth` levels of nesting, and the fields nested in them.
    fn fields(
        &mut self,
        fields: Option<Vector<'_, ForwardsUOffset<Field<'_>>>>,
        depth: u64,
    ) -> Result<(), WireError> {
        for field in fields.into_iter().flatten() {
            Limits::admit("nesting depth", self.limits.nesting_depth, depth)?;
            self.columns += 1;
            Limits::admit("schema columns", self.limits.schema_columns, self.columns)?;
            let name = field.name().unwrap_or_default();
            self.limits.admit_string(name)?;
            self.text(name)?;
            let zone = field.type_as_timestamp().and_then(|time| time.timezone());
            self.text(zone.unwrap_or_default())?;
            self.metadata(field.custom_metadata())?;
            self.fields(field.children(), depth.saturating_add(1))?;
        }
        Ok(())
    }

    /// Counts the keys and values of `metadata`.
    fn metadata(
        &mut self,
        metadata: Option<Vector<'_, ForwardsUOffset<KeyValue<'_>>>>,
    ) -> Result<(), WireError> {
        for entry in metadata.into_iter().flatten() {
            self.text(entry.key().unwrap_or_default())?;
            self.text(entry.value().unwrap_or_default())?;
        }
        Ok(())
    }

    /// Counts `text`, which the converted schema holds a copy of.
    fn text(&mut self, text: &str) -> Result<(), WireError> {
        let bytes = u64::try_from(text.len()).unwrap_or(u64::MAX);
        self.text = self.text.saturating_add(bytes);
        Ok(Limits::admit(
            "schema bytes",
            self.limits.schema_bytes,
            self.text,
        )?)
    }
}
