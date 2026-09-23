//! Validated identifiers shared by connectors and the engine.

#[cfg(test)]
mod tests;

use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Why a value was rejected as an identifier.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    /// The value is empty.
    #[error("{kind} is empty")]
    Empty {
        /// The identifier being built.
        kind: &'static str,
    },
    /// The value is longer than the identifier allows.
    #[error("{kind} is {actual} bytes; the limit is {max}")]
    TooLong {
        /// The identifier being built.
        kind: &'static str,
        /// Bytes allowed.
        max: usize,
        /// Bytes given.
        actual: usize,
    },
    /// The value contains a character the identifier does not allow.
    #[error("{kind} contains {character:?}, which is not allowed")]
    InvalidChar {
        /// The identifier being built.
        kind: &'static str,
        /// The first offending character.
        character: char,
    },
}

fn validate(
    kind: &'static str,
    value: &str,
    max: usize,
    allowed: impl Fn(char) -> bool,
) -> Result<(), IdError> {
    if value.is_empty() {
        return Err(IdError::Empty { kind });
    }
    if value.len() > max {
        return Err(IdError::TooLong {
            kind,
            max,
            actual: value.len(),
        });
    }
    match value.chars().find(|c| !allowed(*c)) {
        Some(character) => Err(IdError::InvalidChar { kind, character }),
        None => Ok(()),
    }
}

fn printable(c: char) -> bool {
    !c.is_control()
}

macro_rules! text_id {
    ($(#[$doc:meta])* $name:ident, $kind:literal, $max:literal, $allowed:expr) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Arc<str>);

        impl $name {
            /// Validates `value` as this identifier.
            pub fn parse(value: impl AsRef<str>) -> Result<Self, IdError> {
                let value = value.as_ref();
                validate($kind, value, $max, $allowed)?;
                Ok(Self(Arc::from(value)))
            }

            /// The identifier's text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0.to_string()
            }
        }
    };
}

text_id!(
    /// A pipeline's name, which all state is keyed on: 1–128 bytes of `[A-Za-z0-9._-]`.
    PipelineId,
    "pipeline id",
    128,
    |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
);

text_id!(
    /// A connector's identifier, such as `io.rapidbyte.postgres`: 1–128 bytes of `[a-z0-9._-]`.
    ConnectorId,
    "connector id",
    128,
    |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-')
);

text_id!(
    /// A source-defined slice of a stream: 1–256 bytes without control characters.
    PartitionId,
    "partition id",
    256,
    printable
);

impl PartitionId {
    /// `whole`: the id of the only partition of a stream that is not split.
    pub fn whole() -> Self {
        Self(Arc::from("whole"))
    }
}

/// A source-side stream name, with an optional namespace such as a database schema.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamName {
    namespace: Option<Arc<str>>,
    name: Arc<str>,
}

impl StreamName {
    /// A stream without a namespace: 1–256 bytes without control characters.
    pub fn new(name: impl AsRef<str>) -> Result<Self, IdError> {
        let name = name.as_ref();
        validate("stream name", name, 256, printable)?;
        Ok(Self {
            namespace: None,
            name: Arc::from(name),
        })
    }

    /// A stream inside `namespace`; both parts follow the rules of [`StreamName::new`].
    pub fn with_namespace(
        namespace: impl AsRef<str>,
        name: impl AsRef<str>,
    ) -> Result<Self, IdError> {
        let namespace = namespace.as_ref();
        validate("stream namespace", namespace, 256, printable)?;
        Ok(Self {
            namespace: Some(Arc::from(namespace)),
            ..Self::new(name)?
        })
    }

    /// The namespace, if any.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// The name within the namespace.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.namespace {
            Some(namespace) => write!(f, "{namespace}.{}", self.name),
            None => f.write_str(&self.name),
        }
    }
}

/// A logical destination table: the stream's root plus nested path segments.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct TablePath(Vec<Arc<str>>);

impl TablePath {
    /// Validates `segments` as a table path: at least one segment, each 1–256 bytes without
    /// control characters.
    pub fn new<S: AsRef<str>>(segments: impl IntoIterator<Item = S>) -> Result<Self, IdError> {
        let mut path = Vec::new();
        for segment in segments {
            let segment = segment.as_ref();
            validate("table path segment", segment, 256, printable)?;
            path.push(Arc::from(segment));
        }
        if path.is_empty() {
            return Err(IdError::Empty { kind: "table path" });
        }
        Ok(Self(path))
    }

    /// The path's segments, root first.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(AsRef::as_ref)
    }
}

impl fmt::Display for TablePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let joined: Vec<&str> = self.segments().collect();
        f.write_str(&joined.join("/"))
    }
}

impl TryFrom<Vec<String>> for TablePath {
    type Error = IdError;

    fn try_from(segments: Vec<String>) -> Result<Self, Self::Error> {
        Self::new(segments)
    }
}

impl From<TablePath> for Vec<String> {
    fn from(path: TablePath) -> Self {
        path.0.iter().map(ToString::to_string).collect()
    }
}

/// Identifies one attempt's load; time-ordered (a version 7 UUID).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LoadId(uuid::Uuid);

impl LoadId {
    /// Builds the id from a wall-clock time and random bits.
    ///
    /// The low 80 bits of `random` fill the id; the version and variant fields overwrite 6 of them
    /// (bits 76–79 and 62–63). Times before the Unix epoch count as the epoch.
    pub fn from_parts(at: SystemTime, random: u128) -> Self {
        let millis = at
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis());
        let millis = u64::try_from(millis).unwrap_or(u64::MAX);
        let mut bytes = [0u8; 10];
        bytes.copy_from_slice(&random.to_be_bytes()[6..]);
        Self(uuid::Builder::from_unix_timestamp_millis(millis, &bytes).into_uuid())
    }

    /// The id's 16 bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl fmt::Display for LoadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(f)
    }
}

/// The position of a commit within a load, starting at 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CommitSeq(NonZeroU64);

impl CommitSeq {
    /// The first commit of a load.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// The commit after this one.
    ///
    /// # Panics
    ///
    /// Panics after `u64::MAX` commits in one load, which no load reaches.
    #[must_use]
    pub fn next(self) -> Self {
        Self(
            self.0
                .checked_add(1)
                .expect("a load commits fewer than u64::MAX times"),
        )
    }

    /// The sequence number.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

macro_rules! counter_id {
    ($(#[$doc:meta])* $name:ident($int:ty)) => {
        $(#[$doc])*
        #[derive(
            Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(pub $int);

        impl $name {
            /// The value after this one.
            #[must_use]
            pub fn next(self) -> Self {
                Self(self.0.saturating_add(1))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

counter_id!(
    /// Identifies a sealed range of one partition's data within a load.
    SegmentId(u64)
);
counter_id!(
    /// The fencing token incremented at every destination open.
    Epoch(u64)
);
counter_id!(
    /// The version of a table's schema; starts at 1 and grows with each applied change.
    SchemaVersion(u32)
);
counter_id!(
    /// Identifies the hidden table generation a `replace` stream fills.
    GenerationId(u64)
);
