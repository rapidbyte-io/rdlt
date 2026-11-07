//! How a stream is read and written, and what its schema does: each a word, or a word with its
//! settings.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::ser::SerializeMap as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::read::{self, Mapped, Text, refuse};

/// How a stream is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReadMode {
    /// Every row, each time.
    Full,
    /// The rows after where the last run stopped, by the source's cursor.
    Incremental,
    /// The source's changes: inserts, updates and deletes.
    Cdc,
}

/// How a stream's rows reach its table: `append`, `replace`, `merge`, `history`, or
/// `{ merge: { key: [..] } }` and `{ history: { key: [..] } }`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteMode {
    /// Insert every row.
    Append,
    /// Fill a hidden generation of the table and swap it in once the stream is read.
    Replace,
    /// Upsert by `key`, or by the source's primary key where it is empty.
    Merge {
        /// The columns matched.
        key: Vec<String>,
    },
    /// Keep every version of each key, by `key` or by the source's primary key.
    History {
        /// The columns matched.
        key: Vec<String>,
    },
}

/// What the schema does with a value that would change its table, or that its column cannot
/// hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SchemaPolicy {
    /// Change the schema.
    Evolve,
    /// Fail with a schema error.
    Freeze,
    /// Drop the row, and count it.
    DiscardRow,
    /// Load the row with the value nulled, and count it.
    DiscardValue,
}

/// What happens to a change the destination cannot apply in place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnUnsupported {
    /// Add a sibling column for the values of the new type.
    VariantColumn,
    /// Fail with a schema error.
    Refuse,
}

/// How nested values are stored: `native`, `json`, `normalize`, or
/// `{ normalize: { max_depth } }`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Nested {
    /// As the destination's struct and list types, or as JSON where it has none.
    Native,
    /// As one JSON value per nested column.
    Json,
    /// Arrays as child tables and objects flattened, to `max_depth`; deeper ones as JSON.
    Normalize {
        /// How deep objects and arrays normalize.
        max_depth: u8,
    },
}

/// What a change stream's deletes do to its table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeleteMode {
    /// Remove the row.
    Hard,
    /// Keep the row, and record when it was deleted.
    Soft,
    /// Drop delete rows, and count them.
    Ignore,
}

/// What a change stream's truncates do to its table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnTruncate {
    /// Remove every row the source truncated.
    Apply,
    /// Drop truncates, and count them.
    Ignore,
}

/// What a stream does when its source no longer holds where a read would resume.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RetentionLoss {
    /// Fail the run: data is never skipped silently.
    Fail,
    /// Read again from the source's earliest, skipping what it dropped, and count it.
    Reset,
}

/// A column's fixed type: a word, or `{ decimal: { precision, scale } }`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Hint {
    /// `true` or `false`.
    Bool,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// A 32-bit float.
    Float32,
    /// A 64-bit float.
    Float64,
    /// UTF-8 text.
    Utf8,
    /// Bytes.
    Binary,
    /// A calendar date.
    Date,
    /// A 16-byte UUID.
    Uuid,
    /// Any JSON value.
    Json,
    /// An instant, in microseconds, shown in UTC.
    Timestamp,
    /// An exact decimal.
    Decimal {
        /// Its digits.
        precision: u8,
        /// Its digits after the point.
        scale: u8,
    },
}

const READS: &[(&str, ReadMode)] = &[
    ("full", ReadMode::Full),
    ("incremental", ReadMode::Incremental),
    ("cdc", ReadMode::Cdc),
];

const POLICIES: &[(&str, SchemaPolicy)] = &[
    ("evolve", SchemaPolicy::Evolve),
    ("freeze", SchemaPolicy::Freeze),
    ("discard_row", SchemaPolicy::DiscardRow),
    ("discard_value", SchemaPolicy::DiscardValue),
];

const UNSUPPORTED: &[(&str, OnUnsupported)] = &[
    ("variant_column", OnUnsupported::VariantColumn),
    ("refuse", OnUnsupported::Refuse),
];

const DELETES: &[(&str, DeleteMode)] = &[
    ("hard", DeleteMode::Hard),
    ("soft", DeleteMode::Soft),
    ("ignore", DeleteMode::Ignore),
];

const TRUNCATES: &[(&str, OnTruncate)] =
    &[("apply", OnTruncate::Apply), ("ignore", OnTruncate::Ignore)];

const RETENTION: &[(&str, RetentionLoss)] = &[
    ("fail", RetentionLoss::Fail),
    ("reset", RetentionLoss::Reset),
];

const HINTS: &[(&str, Hint)] = &[
    ("bool", Hint::Bool),
    ("int8", Hint::Int8),
    ("int16", Hint::Int16),
    ("int32", Hint::Int32),
    ("int64", Hint::Int64),
    ("float32", Hint::Float32),
    ("float64", Hint::Float64),
    ("utf8", Hint::Utf8),
    ("binary", Hint::Binary),
    ("date", Hint::Date),
    ("uuid", Hint::Uuid),
    ("json", Hint::Json),
    ("timestamp", Hint::Timestamp),
];

impl<'de> Deserialize<'de> for ReadMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        read::word(deserializer, READS)
    }
}

impl<'de> Deserialize<'de> for SchemaPolicy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        read::word(deserializer, POLICIES)
    }
}

impl<'de> Deserialize<'de> for OnUnsupported {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        read::word(deserializer, UNSUPPORTED)
    }
}

impl<'de> Deserialize<'de> for DeleteMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        read::word(deserializer, DELETES)
    }
}

impl<'de> Deserialize<'de> for OnTruncate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        read::word(deserializer, TRUNCATES)
    }
}

impl<'de> Deserialize<'de> for RetentionLoss {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        read::word(deserializer, RETENTION)
    }
}

/// A value written as a word, or as a mapping of one word to its settings.
enum Worded<T> {
    Word(String),
    Keyed(String, T),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Worded<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for Reading<T> {
            type Value = Worded<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a word, or a mapping of one word to its settings")
            }

            fn visit_str<E: de::Error>(self, word: &str) -> Result<Worded<T>, E> {
                Ok(Worded::Word(word.to_owned()))
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Worded<T>, M::Error> {
                let Some(Text(word)) = map.next_key::<Text>()? else {
                    return Err(de::Error::invalid_length(0, &self));
                };
                let Mapped(settings) = map.next_value::<Mapped<T>>()?;
                if map.next_key::<IgnoredAny>()?.is_some() {
                    return Err(de::Error::invalid_length(2, &self));
                }
                Ok(Worded::Keyed(word, settings))
            }

            refuse!(bool int fraction);
        }

        deserializer.deserialize_any(Reading(PhantomData))
    }
}

/// Writes `word` alone, or as a mapping of it to `settings`.
fn worded<S: Serializer>(
    serializer: S,
    word: &str,
    settings: Option<serde_json::Value>,
) -> Result<S::Ok, S::Error> {
    let Some(settings) = settings else {
        return serializer.serialize_str(word);
    };
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry(word, &settings)?;
    map.end()
}

/// The schema of `words`, or of a mapping of one of `keyed` to `settings`.
fn worded_schema(description: &str, words: &[&str], keyed: &[&str], settings: &Schema) -> Schema {
    let mut choices = vec![json_schema!({ "enum": words })];
    for word in keyed {
        choices.push(json_schema!({
            "type": "object",
            "properties": { *word: settings },
            "required": [word],
            "additionalProperties": false
        }));
    }
    json_schema!({ "description": description, "oneOf": choices })
}

/// The key a merge or history matches rows by.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Keyed {
    #[serde(default, deserialize_with = "read::texts")]
    key: Vec<String>,
}

/// What a write's word alone names, and the words that take a key.
#[derive(Clone, Copy)]
enum Writing {
    Append,
    Replace,
    Merge,
    History,
}

const WRITES: &[(&str, Writing)] = &[
    ("append", Writing::Append),
    ("replace", Writing::Replace),
    ("merge", Writing::Merge),
    ("history", Writing::History),
];

const KEYED_WRITES: &[(&str, Writing)] =
    &[("merge", Writing::Merge), ("history", Writing::History)];

impl WriteMode {
    /// A merge by `key`.
    pub fn merge(key: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Merge {
            key: key.into_iter().map(Into::into).collect(),
        }
    }

    /// History by `key`.
    pub fn history(key: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::History {
            key: key.into_iter().map(Into::into).collect(),
        }
    }
}

impl<'de> Deserialize<'de> for WriteMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (writing, key) = match Worded::<Keyed>::deserialize(deserializer)? {
            Worded::Word(word) => (read::chosen::<_, D::Error>(&word, WRITES)?, Vec::new()),
            Worded::Keyed(word, Keyed { key }) => {
                (read::chosen::<_, D::Error>(&word, KEYED_WRITES)?, key)
            }
        };
        Ok(match writing {
            Writing::Append => Self::Append,
            Writing::Replace => Self::Replace,
            Writing::Merge => Self::Merge { key },
            Writing::History => Self::History { key },
        })
    }
}

impl Serialize for WriteMode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (word, key) = match self {
            Self::Append => ("append", None),
            Self::Replace => ("replace", None),
            Self::Merge { key } => ("merge", Some(key)),
            Self::History { key } => ("history", Some(key)),
        };
        let settings = key
            .filter(|key| !key.is_empty())
            .map(|key| serde_json::json!({ "key": key }));
        worded(serializer, word, settings)
    }
}

impl JsonSchema for WriteMode {
    fn schema_name() -> Cow<'static, str> {
        "WriteMode".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        let keyed = json_schema!({
            "type": "object",
            "properties": { "key": { "type": "array", "items": { "type": "string" } } },
            "additionalProperties": false
        });
        let words: Vec<&str> = WRITES.iter().map(|(word, _)| *word).collect();
        let description = "How a stream's rows reach its table.";
        worded_schema(description, &words, &["merge", "history"], &keyed)
    }
}

/// How deep a normalization goes.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Depth {
    #[serde(deserialize_with = "read::count")]
    max_depth: u8,
}

/// The depth `normalize` alone means.
const NORMALIZED: u8 = 8;

const NESTINGS: &[(&str, Nested)] = &[
    ("native", Nested::Native),
    ("json", Nested::Json),
    (
        "normalize",
        Nested::Normalize {
            max_depth: NORMALIZED,
        },
    ),
];

impl<'de> Deserialize<'de> for Nested {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Worded::<Depth>::deserialize(deserializer)? {
            Worded::Word(word) => read::chosen(&word, NESTINGS),
            Worded::Keyed(word, Depth { max_depth }) => {
                let normalized = [("normalize", Self::Normalize { max_depth })];
                read::chosen(&word, &normalized)
            }
        }
    }
}

impl Serialize for Nested {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Native => worded(serializer, "native", None),
            Self::Json => worded(serializer, "json", None),
            Self::Normalize { max_depth } => {
                let settings = (*max_depth != NORMALIZED)
                    .then(|| serde_json::json!({ "max_depth": max_depth }));
                worded(serializer, "normalize", settings)
            }
        }
    }
}

impl JsonSchema for Nested {
    fn schema_name() -> Cow<'static, str> {
        "Nested".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        let depth = json_schema!({
            "type": "object",
            "properties": { "max_depth": { "type": "integer", "minimum": 0, "maximum": 255 } },
            "required": ["max_depth"],
            "additionalProperties": false
        });
        let words: Vec<&str> = NESTINGS.iter().map(|(word, _)| *word).collect();
        worded_schema(
            "How nested values are stored.",
            &words,
            &["normalize"],
            &depth,
        )
    }
}

/// A decimal's digits.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Digits {
    #[serde(deserialize_with = "read::count")]
    precision: u8,
    #[serde(deserialize_with = "read::count")]
    scale: u8,
}

impl<'de> Deserialize<'de> for Hint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Worded::<Digits>::deserialize(deserializer)? {
            Worded::Word(word) => read::chosen(&word, HINTS),
            Worded::Keyed(word, Digits { precision, scale }) => {
                let decimal = [("decimal", Self::Decimal { precision, scale })];
                read::chosen(&word, &decimal)
            }
        }
    }
}
