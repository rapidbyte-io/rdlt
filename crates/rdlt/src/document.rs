//! A pipeline's document: one model, which a builder builds and a file holds.

#[cfg(test)]
pub(crate) mod drawn;
mod fault;
mod hostile;
mod modes;
mod policies;
mod read;
mod side;
mod stream;
#[cfg(test)]
pub(crate) mod tests;
mod transform;
mod units;
mod until;

use std::fs::File;
use std::io;
use std::path::Path;

use rustix::fs::{Mode, OFlags};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use self::fault::Fault;
use crate::limits::{BUILD_WAIT, DOCUMENT_BYTES};
use crate::{Error, codes};

pub use modes::{
    DeleteMode, Hint, Nested, OnTruncate, OnUnsupported, ReadMode, RetentionLoss, SchemaPolicy,
    WriteMode,
};
pub use policies::{Commit, Resources, Retry};
pub use side::{Access, Connector, ConnectorReference, Side};
pub use stream::{Column, Defaults, SchemaRules, Stream};
pub(crate) use transform::{KEY_WRITTEN_OUT, refers_to_secret};
pub use transform::{MaskStep, MaskWith, TransformStep};
pub use until::Until;

/// A pipeline as its author describes it: one source, one destination, the streams it selects,
/// and how a run commits, uses resources, retries and reads.
///
/// ```
/// use rdlt::{Connector, Document, Side, Stream};
///
/// let source = Side::new(Connector::new("io.rapidbyte.memory"))
///     .config(rdlt::json!({ "streams": { "users": [{ "id": 1 }] } }));
/// let destination = Side::new(Connector::new("io.rapidbyte.memory"))
///     .config(rdlt::json!({ "store": "users" }));
/// let mut document = Document::new("users", source, destination);
/// document.streams.push(Stream::new("users"));
/// let written = serde_json::to_value(&document)?;
/// assert_eq!(serde_json::from_value::<Document>(written)?, document);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Document {
    /// The document's format: 1, the only one.
    #[serde(deserialize_with = "read::version")]
    #[schemars(extend("const" = 1))]
    pub version: u8,
    /// The pipeline's name.
    #[serde(deserialize_with = "read::text")]
    pub pipeline: String,
    /// Where rows come from.
    #[serde(deserialize_with = "read::mapping")]
    pub source: Side,
    /// Where rows go.
    #[serde(deserialize_with = "read::mapping")]
    pub destination: Side,
    /// What every stream takes where it says nothing.
    #[serde(
        default,
        deserialize_with = "read::mapping",
        skip_serializing_if = "Defaults::is_empty"
    )]
    pub defaults: Defaults,
    /// The streams loaded; a name holding `*` or `?` selects each stream of the source's catalog
    /// it matches, and a stream named outright wins over a pattern that matches it too.
    #[serde(deserialize_with = "read::mappings")]
    pub streams: Vec<Stream>,
    /// When a run commits; the engine's default where it says nothing.
    #[serde(
        default,
        deserialize_with = "read::optional_mapping",
        skip_serializing_if = "Option::is_none"
    )]
    pub commit: Option<Commit>,
    /// What a run may use.
    #[serde(
        default,
        deserialize_with = "read::mapping",
        skip_serializing_if = "Resources::is_empty"
    )]
    pub resources: Resources,
    /// How a run retries; the engine's default where it says nothing.
    #[serde(
        default,
        deserialize_with = "read::optional_mapping",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry: Option<Retry>,
    /// How long a run reads.
    #[serde(default, skip_serializing_if = "Until::is_exhausted")]
    pub until: Until,
}

impl Document {
    /// Pipeline `pipeline` from `source` to `destination`, selecting no stream yet.
    pub fn new(pipeline: impl Into<String>, source: Side, destination: Side) -> Self {
        Self {
            version: 1,
            pipeline: pipeline.into(),
            source,
            destination,
            defaults: Defaults::default(),
            streams: Vec::new(),
            commit: None,
            resources: Resources::default(),
            retry: None,
            until: Until::default(),
        }
    }

    /// The document `text` holds, YAML or JSON, read as hostile input: at most
    /// [`DOCUMENT_BYTES`](crate::limits::DOCUMENT_BYTES), nesting and nodes bounded, and no
    /// anchor, alias, tag, merge key, repeated key or second document.
    ///
    /// # Errors
    ///
    /// A `Config` error coded `document_*`, placed at `line:col` and named by its key path; it
    /// never quotes a value.
    ///
    /// ```
    /// let document = rdlt::Document::parse(
    ///     "version: 1\n\
    ///      pipeline: users\n\
    ///      source: { connector: io.rapidbyte.memory }\n\
    ///      destination: { connector: io.rapidbyte.memory }\n\
    ///      streams: [{ name: users }]\n",
    /// )?;
    /// assert_eq!(document.pipeline, "users");
    /// # Ok::<(), rdlt::Error>(())
    /// ```
    pub fn parse(text: &str) -> Result<Self, Error> {
        hostile_read(text, DOCUMENT)
    }

    /// The document `text` holds, read as [`parse`](Self::parse) reads it; its errors start with
    /// `origin`, where a file's path would stand.
    ///
    /// # Errors
    ///
    /// As `parse`.
    pub fn from_text(text: &str, origin: &str) -> Result<Self, Error> {
        hostile::read(text, DOCUMENT).map_err(|fault| refused(&fault, Some(origin)))
    }

    /// The document in the file at `path`, read as [`parse`](Self::parse) reads text and on a
    /// blocking task, so a stalled path holds up no runtime worker.
    ///
    /// A fault in its text starts with the file's name, and a file that cannot be read is named
    /// in its error.
    ///
    /// # Errors
    ///
    /// `document_unreadable` where the path names no regular file, the file cannot be read or is
    /// no UTF-8, or reading it takes past [`BUILD_WAIT`]; `document_too_large` past
    /// [`DOCUMENT_BYTES`], else as `parse`.
    pub async fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_owned();
        let reading = {
            let path = path.clone();
            tokio::task::spawn_blocking(move || regular_file_bytes(&path))
        };
        let unread = |cause: io::Error| Error::unreadable(&path, cause);
        let bytes = match tokio::time::timeout(BUILD_WAIT, reading).await {
            Ok(Ok(bytes)) => bytes.map_err(&unread)?,
            Ok(Err(joined)) => return Err(unread(io::Error::other(joined))),
            Err(_) => {
                let late = format!("not read within {BUILD_WAIT:?}");
                return Err(unread(io::Error::new(io::ErrorKind::TimedOut, late)));
            }
        };
        let origin = path.display().to_string();
        if bytes.len() > DOCUMENT_BYTES {
            return Err(refused(&Fault::too_large(DOCUMENT), Some(&origin)));
        }
        // The error holds the file's bytes; its position alone is kept.
        let text = String::from_utf8(bytes)
            .map_err(|error| Error::unreadable(&path, error.utf8_error()))?;
        Self::from_text(&text, &origin)
    }

    /// The document as YAML, every secret still a reference.
    ///
    /// # Errors
    ///
    /// `mask_key_literal` where a mask's key is written out: a document holds a secret reference,
    /// never the key, so such a document is never written. `document_too_large` or
    /// `document_too_deep` where its YAML is past what the reader takes, which nothing could then
    /// read back. `document_invalid` should anything else in it have no form in YAML.
    pub fn to_yaml(&self) -> Result<String, Error> {
        let written_out = |step: &TransformStep| {
            matches!(step, TransformStep::Mask(mask)
                if mask.key.as_deref().is_some_and(|key| !refers_to_secret(key)))
        };
        let steps = self.streams.iter().flat_map(|stream| &stream.transform);
        if steps.into_iter().any(written_out) {
            return Err(Error::config(codes::MASK_KEY_LITERAL, KEY_WRITTEN_OUT));
        }
        let written = serde_saphyr::to_string(self).map_err(|cause| {
            let message = "the document cannot be written as YAML";
            Error::config_caused(codes::DOCUMENT_INVALID, message, cause)
        })?;
        hostile::screen(&written, "the document's YAML").map_err(|fault| refused(&fault, None))?;
        Ok(written)
    }

    /// The JSON Schema every document satisfies, for editors and tools that check one.
    pub fn json_schema() -> serde_json::Value {
        schemars::schema_for!(Self).to_value()
    }
}

/// What a document's refusals call it.
const DOCUMENT: &str = "the document";

/// `text` as a `T`, a mapping, read as a document is, its faults as errors saying `noun`.
pub(crate) fn hostile_read<T: serde::de::DeserializeOwned>(
    text: &str,
    noun: &str,
) -> Result<T, Error> {
    hostile::read(text, noun).map_err(|fault| refused(&fault, None))
}

/// As [`hostile_read`], a text holding nothing being `T::default()`.
pub(crate) fn hostile_read_or_default<T: serde::de::DeserializeOwned + Default>(
    text: &str,
    noun: &str,
) -> Result<T, Error> {
    hostile::read_or_default(text, noun).map_err(|fault| refused(&fault, None))
}

/// The error `fault` is, its text naming `origin`.
fn refused(fault: &Fault, origin: Option<&str>) -> Error {
    Error::config(fault.code(), fault.render(origin))
}

/// The bytes of the regular file at `path`, at most one past a document's limit.
///
/// A pipe or a terminal is opened without waiting and refused on what was opened, so nothing can
/// hold the caller and no name is checked apart from the file it names.
fn regular_file_bytes(path: &Path) -> io::Result<Vec<u8>> {
    use std::io::Read as _;
    let flags = OFlags::RDONLY
        .union(OFlags::NONBLOCK)
        .union(OFlags::NOCTTY)
        .union(OFlags::CLOEXEC);
    let file = File::from(rustix::fs::open(path, flags, Mode::empty())?);
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "it is no regular file",
        ));
    }
    let limit = u64::try_from(DOCUMENT_BYTES)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::new();
    file.take(limit).read_to_end(&mut bytes)?;
    Ok(bytes)
}
