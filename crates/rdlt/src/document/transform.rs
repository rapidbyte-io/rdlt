//! A stream's transform steps as a document writes them: ops, SQL, and code the host registered.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use rdlt_engine::Literal;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser};

use super::read::{self, Mapped, Text, refuse};

/// One step of a stream's transforms, run on every batch after the steps before it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TransformStep {
    /// Renames top-level columns, each from its first name to its second, in the order written.
    #[serde(serialize_with = "renames::serialize")]
    #[schemars(with = "BTreeMap<String, String>")]
    Rename(Vec<(String, String)>),
    /// Masks the values of columns.
    Mask(MaskStep),
    /// Adds top-level columns, in the order written, each holding its value in every row: text,
    /// an integer or a boolean.
    #[serde(serialize_with = "constants::serialize")]
    #[schemars(with = "BTreeMap<String, ConstantText>")]
    Constant(Vec<(String, Literal)>),
    /// A query of the table `input`: its columns projected, its rows optionally filtered.
    Sql(String),
    /// The code transform the host registered by this name.
    Code(String),
}

/// The kinds of step, by the word a document writes each with.
#[derive(Clone, Copy)]
enum Kind {
    Rename,
    Mask,
    Constant,
    Sql,
    Code,
}

const KINDS: &[(&str, Kind)] = &[
    ("rename", Kind::Rename),
    ("mask", Kind::Mask),
    ("constant", Kind::Constant),
    ("sql", Kind::Sql),
    ("code", Kind::Code),
];

impl<'de> Deserialize<'de> for TransformStep {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading;

        impl<'de> Visitor<'de> for Reading {
            type Value = TransformStep;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a mapping of one kind of step to its settings")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<TransformStep, M::Error> {
                let Some(Text(kind)) = map.next_key::<Text>()? else {
                    return Err(de::Error::invalid_length(0, &self));
                };
                let step = match read::chosen(&kind, KINDS)? {
                    Kind::Rename => TransformStep::Rename(map.next_value::<Renames>()?.0),
                    Kind::Mask => TransformStep::Mask(map.next_value::<Mapped<MaskStep>>()?.0),
                    Kind::Constant => TransformStep::Constant(map.next_value::<Constants>()?.0),
                    Kind::Sql => TransformStep::Sql(map.next_value::<Text>()?.0),
                    Kind::Code => TransformStep::Code(map.next_value::<Text>()?.0),
                };
                if map.next_key::<IgnoredAny>()?.is_some() {
                    return Err(de::Error::invalid_length(2, &self));
                }
                Ok(step)
            }

            refuse!(bool int fraction text);
        }

        deserializer.deserialize_any(Reading)
    }
}

/// A rename's pairs, as a document writes them.
struct Renames(Vec<(String, String)>);

impl<'de> Deserialize<'de> for Renames {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        renames::deserialize(deserializer).map(Self)
    }
}

/// The columns a constant step adds, as a document writes them.
struct Constants(Vec<(String, Literal)>);

impl<'de> Deserialize<'de> for Constants {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        constants::deserialize(deserializer).map(Self)
    }
}

impl TransformStep {
    /// Renames each column from its first name to its second.
    pub fn rename(pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
        Self::Rename(
            pairs
                .into_iter()
                .map(|(from, to)| (from.into(), to.into()))
                .collect(),
        )
    }

    /// Adds each column, holding its value.
    pub fn constant(columns: impl IntoIterator<Item = (impl Into<String>, Literal)>) -> Self {
        Self::Constant(
            columns
                .into_iter()
                .map(|(column, value)| (column.into(), value))
                .collect(),
        )
    }
}

/// A mask of columns' values.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct MaskStep {
    /// The columns: top-level names, or paths through structs, a dot parting each field.
    #[serde(deserialize_with = "read::texts")]
    pub columns: Vec<String>,
    /// How each value is masked.
    pub with: MaskWith,
    /// The key of an `hmac_sha256` mask: a secret reference, `${env:NAME}`, `${file:/path}` or
    /// `${secret:name}`, which the host resolves; never the key itself.
    #[serde(
        default,
        deserialize_with = "read::optional_text",
        serialize_with = "referred",
        skip_serializing_if = "Option::is_none"
    )]
    pub key: Option<String>,
    /// The text a `redact` mask writes for each value; `***` where it says none.
    #[serde(
        default,
        deserialize_with = "read::optional_text",
        skip_serializing_if = "Option::is_none"
    )]
    pub text: Option<String>,
}

impl MaskStep {
    /// A mask of `columns`, each value masked as `with` says.
    pub fn new(columns: impl IntoIterator<Item = impl Into<String>>, with: MaskWith) -> Self {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
            with,
            key: None,
            text: None,
        }
    }

    /// The mask, keyed by the secret `key` refers to: `${env:NAME}`, `${file:/path}` or
    /// `${secret:name}`.
    #[must_use]
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// The mask, writing `text` for each value it redacts.
    #[must_use]
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }
}

/// Shows a key only as the reference it should be: one written out is a secret in the open.
impl fmt::Debug for MaskStep {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key = self
            .key
            .as_deref()
            .map(|key| if refers_to_secret(key) { key } else { "***" });
        formatter
            .debug_struct("MaskStep")
            .field("columns", &self.columns)
            .field("with", &self.with)
            .field("key", &key)
            .field("text", &self.text)
            .finish()
    }
}

/// Writes a mask's `key`, which a document holds only as a secret reference: a key written out
/// is refused, never written.
#[expect(
    clippy::ref_option,
    reason = "serde's `serialize_with` hands a field of `Option` by reference"
)]
fn referred<S: Serializer>(key: &Option<String>, serializer: S) -> Result<S::Ok, S::Error> {
    match key.as_deref() {
        Some(key) if !refers_to_secret(key) => Err(ser::Error::custom(
            "a mask's key is written out: a document holds a secret reference, ${env:NAME}, \
             ${file:/path} or ${secret:name}, never the key itself",
        )),
        key => key.serialize(serializer),
    }
}

/// Whether `text` is one secret reference and nothing else: `${env:NAME}`, `${file:/path}` or
/// `${secret:name}`.
pub(crate) fn refers_to_secret(text: &str) -> bool {
    let Some(reference) = text
        .strip_prefix("${")
        .and_then(|rest| rest.strip_suffix('}'))
    else {
        return false;
    };
    let Some((kind, name)) = reference.split_once(':') else {
        return false;
    };
    matches!(kind, "env" | "file" | "secret") && !name.is_empty() && !name.contains(['{', '}'])
}

/// How a mask replaces each value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MaskWith {
    /// Its `HMAC-SHA256` under the mask's key, as 64 lower-case hex digits.
    HmacSha256,
    /// The mask's text, for every value that is not null.
    Redact,
    /// Null.
    Null,
}

impl<'de> Deserialize<'de> for MaskWith {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let choices = [
            ("hmac_sha256", Self::HmacSha256),
            ("redact", Self::Redact),
            ("null", Self::Null),
        ];
        read::word(deserializer, &choices)
    }
}

/// A constant's schema: text, an integer or a boolean.
struct ConstantText;

impl JsonSchema for ConstantText {
    fn schema_name() -> Cow<'static, str> {
        "Constant".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A constant: text, an integer or a boolean.",
            "type": ["string", "integer", "boolean"],
            "minimum": i64::MIN,
            "maximum": i64::MAX
        })
    }
}

/// Renames, as a document writes them: a mapping of each column to its new name, in order.
mod renames {
    use std::fmt;

    use serde::de::{MapAccess, Visitor};
    use serde::ser::SerializeMap as _;
    use serde::{Deserializer, Serializer};

    use crate::document::read::{Text, refuse};

    pub(super) fn serialize<S: Serializer>(
        pairs: &[(String, String)],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(pairs.len()))?;
        for (from, to) in pairs {
            map.serialize_entry(from, to)?;
        }
        map.end()
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<(String, String)>, D::Error> {
        struct Reading;

        impl<'de> Visitor<'de> for Reading {
            type Value = Vec<(String, String)>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a mapping of each column to its new name")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut pairs = Vec::new();
                while let Some((Text(from), Text(to))) = map.next_entry::<Text, Text>()? {
                    pairs.push((from, to));
                }
                Ok(pairs)
            }

            refuse!(bool int fraction text);
        }

        deserializer.deserialize_any(Reading)
    }
}

/// Constant columns, as a document writes them: a mapping of each column to its value, in
/// order.
mod constants {
    use std::fmt;

    use rdlt_engine::Literal;
    use serde::de::{self, MapAccess, Unexpected, Visitor};
    use serde::ser::{self, SerializeMap as _};
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::document::read::{Text, refuse};

    /// One constant's value, read as text, an integer or a boolean.
    struct Read(Literal);

    impl<'de> Deserialize<'de> for Read {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserializer.deserialize_any(Reading).map(Read)
        }
    }

    /// Reads a constant, never quoting what it refuses.
    struct Reading;

    impl Visitor<'_> for Reading {
        type Value = Literal;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("text, an integer or a boolean")
        }

        fn visit_str<E: de::Error>(self, text: &str) -> Result<Literal, E> {
            Ok(Literal::Text(text.to_owned()))
        }

        fn visit_i64<E: de::Error>(self, integer: i64) -> Result<Literal, E> {
            Ok(Literal::Integer(integer))
        }

        fn visit_u64<E: de::Error>(self, integer: u64) -> Result<Literal, E> {
            let large = || E::invalid_value(Unexpected::Other("an integer beyond 64 bits"), &self);
            i64::try_from(integer)
                .map(Literal::Integer)
                .map_err(|_| large())
        }

        fn visit_bool<E: de::Error>(self, boolean: bool) -> Result<Literal, E> {
            Ok(Literal::Boolean(boolean))
        }

        refuse!(wide fraction);
    }

    pub(super) fn serialize<S: Serializer>(
        columns: &[(String, Literal)],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(columns.len()))?;
        for (column, literal) in columns {
            match literal {
                Literal::Text(text) => map.serialize_entry(column, text)?,
                Literal::Integer(integer) => map.serialize_entry(column, integer)?,
                Literal::Boolean(boolean) => map.serialize_entry(column, boolean)?,
                _ => return Err(ser::Error::custom("a constant a document cannot hold")),
            }
        }
        map.end()
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<(String, Literal)>, D::Error> {
        struct Columns;

        impl<'de> Visitor<'de> for Columns {
            type Value = Vec<(String, Literal)>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a mapping of each column to its value")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut columns = Vec::new();
                while let Some((Text(column), Read(value))) = map.next_entry::<Text, Read>()? {
                    columns.push((column, value));
                }
                Ok(columns)
            }

            refuse!(bool int fraction text);
        }

        deserializer.deserialize_any(Columns)
    }
}
