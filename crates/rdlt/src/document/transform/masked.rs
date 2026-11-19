//! A mask's columns as a document writes them: a top-level column by its name as written, a dot
//! in it included, or a field within structs by its path, a list of names from its top-level
//! column down.

use std::borrow::Cow;
use std::fmt;

use rdlt_connector::ColumnPath;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq as _;
use serde::{Deserialize, Deserializer, Serializer};

use crate::document::read::{Listed, Text, refuse};

/// One of a mask's columns.
struct Named(ColumnPath);

impl<'de> Deserialize<'de> for Named {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading;

        impl<'de> Visitor<'de> for Reading {
            type Value = Named;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a column's name, or a list of names down through structs")
            }

            fn visit_str<E: de::Error>(self, name: &str) -> Result<Named, E> {
                Ok(Named(ColumnPath::from(name)))
            }

            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Named, S::Error> {
                let mut names = Vec::new();
                while let Some(Text(name)) = seq.next_element()? {
                    names.push(name);
                }
                let path = ColumnPath::new(names);
                path.map(Named)
                    .map_err(|_| de::Error::invalid_length(0, &self))
            }

            refuse!(bool int fraction);
        }

        deserializer.deserialize_any(Reading)
    }
}

/// A mask's columns, for `#[serde(deserialize_with)]`.
pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ColumnPath>, D::Error> {
    let Listed(named) = Listed::<Named>::deserialize(deserializer)?;
    Ok(named.into_iter().map(|Named(path)| path).collect())
}

/// Writes a mask's columns: a top-level column as its name, a field within structs as its path.
pub(super) fn serialize<S: Serializer>(
    columns: &[ColumnPath],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut written = serializer.serialize_seq(Some(columns.len()))?;
    for path in columns {
        let names: Vec<&str> = path.segments().collect();
        match names[..] {
            [name] => written.serialize_element(name)?,
            _ => written.serialize_element(&names)?,
        }
    }
    written.end()
}

/// A mask's column's schema: a top-level column's name, or a path through structs.
pub(super) struct ColumnText;

impl JsonSchema for ColumnText {
    fn schema_name() -> Cow<'static, str> {
        "MaskColumn".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A top-level column's name as written, a dot in it included, or a \
                            field within structs by its path: a list of names from its top-level \
                            column down.",
            "oneOf": [
                { "type": "string" },
                { "type": "array", "items": { "type": "string" }, "minItems": 1 }
            ]
        })
    }
}
