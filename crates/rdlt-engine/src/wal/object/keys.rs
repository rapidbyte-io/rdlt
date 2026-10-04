//! The names an object-store log's objects take beneath its prefix, and the only names its
//! listings accept.

use object_store::path::Path;
use rdlt_connector::{LoadId, PipelineId};

use crate::error::Error;
use crate::limits::{WAL_PREFIX_BYTES, WAL_PREFIX_INVALID};
use crate::wal::Chunk;

/// Where a store's objects are: beneath a prefix of one or more segments of `[A-Za-z0-9._-]`,
/// none of them `.` or `..`.
///
/// Beneath it, `store` names the store, `probe/` holds the startup probe's markers, and each
/// pipeline's objects are beneath `p.<pipeline>/`: its open logs' marks in `open/`, named by
/// load, and each load's chunks in `logs/<load>/`, `<number:08>.wal` and the parts' bodies
/// `<number:08>.<token:032x>.body`, and the marks of chunks deleted, `<number:08>.gone`.
#[derive(Clone, Debug)]
pub(super) struct Keys {
    prefix: Path,
}

/// What a name in a log's directory names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Name {
    /// The head of chunk `number`: the chunk, or a reference to the body it was uploaded as.
    Head(u64),
    /// A body uploaded for chunk `number`, `token` telling it from others.
    Body(u64, u128),
    /// The mark that chunk `number` was deleted, kept so the number stays known.
    Gone(u64),
}

impl Name {
    /// The chunk's number it names.
    pub(super) fn number(self) -> u64 {
        match self {
            Self::Head(number) | Self::Body(number, _) | Self::Gone(number) => number,
        }
    }
}

impl Keys {
    /// The keys beneath `prefix`.
    ///
    /// # Errors
    ///
    /// `wal_prefix_invalid` for a prefix that is empty, longer than [`WAL_PREFIX_BYTES`], or
    /// holds an empty, `.` or `..` segment or another character.
    pub(super) fn parse(prefix: &str) -> Result<Self, Error> {
        let invalid = |why: &str| {
            Error::config(format!("the log prefix {prefix:?} {why}")).with_code(WAL_PREFIX_INVALID)
        };
        if prefix.is_empty() {
            return Err(invalid("is empty"));
        }
        if prefix.len() > WAL_PREFIX_BYTES {
            return Err(invalid(&format!("is longer than {WAL_PREFIX_BYTES} bytes")));
        }
        for segment in prefix.split('/') {
            if segment.is_empty() || segment == "." || segment == ".." {
                return Err(invalid("holds an empty, `.` or `..` segment"));
            }
            let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');
            if !segment.chars().all(allowed) {
                return Err(invalid("holds a character beyond [A-Za-z0-9._-] and `/`"));
            }
        }
        let prefix = Path::from_iter(prefix.split('/'));
        Ok(Self { prefix })
    }

    /// The object naming the store.
    pub(super) fn identity(&self) -> Path {
        self.prefix.clone().join("store")
    }

    /// The directory of the startup probe's markers.
    pub(super) fn probes(&self) -> Path {
        self.prefix.clone().join("probe")
    }

    /// The startup probe's marker `token`.
    pub(super) fn probe(&self, token: u128) -> Path {
        self.probes().join(format!("{token:032x}"))
    }

    fn pipeline(&self, pipeline: &PipelineId) -> Path {
        self.prefix.clone().join(format!("p.{}", pipeline.as_str()))
    }

    /// The directory of `pipeline`'s open logs' marks.
    pub(super) fn marks(&self, pipeline: &PipelineId) -> Path {
        self.pipeline(pipeline).join("open")
    }

    /// The mark of `load`'s log of `pipeline`, there while it is open.
    pub(super) fn mark(&self, pipeline: &PipelineId, load: LoadId) -> Path {
        self.marks(pipeline).join(load.to_string())
    }

    /// The directory of `pipeline`'s logs.
    pub(super) fn logs(&self, pipeline: &PipelineId) -> Path {
        self.pipeline(pipeline).join("logs")
    }

    /// The directory of `load`'s log of `pipeline`.
    pub(super) fn log(&self, pipeline: &PipelineId, load: LoadId) -> Path {
        self.logs(pipeline).join(load.to_string())
    }

    /// The head of `chunk` of `pipeline`'s log.
    pub(super) fn head(&self, pipeline: &PipelineId, chunk: Chunk) -> Path {
        let name = format!("{:08}.wal", chunk.number);
        self.log(pipeline, chunk.load).join(name)
    }

    /// The mark that `chunk` of `pipeline`'s log was deleted.
    pub(super) fn gone(&self, pipeline: &PipelineId, chunk: Chunk) -> Path {
        let name = format!("{:08}.gone", chunk.number);
        self.log(pipeline, chunk.load).join(name)
    }

    /// A body of `chunk` of `pipeline`'s log, `token` telling it from others.
    pub(super) fn body(&self, pipeline: &PipelineId, chunk: Chunk, token: u128) -> Path {
        let name = format!("{:08}.{token:032x}.body", chunk.number);
        self.log(pipeline, chunk.load).join(name)
    }
}

/// The last segment of `path`, beneath `parent`, where it is directly beneath it.
pub(super) fn name_in<'a>(parent: &Path, path: &'a Path) -> Option<&'a str> {
    let rest = path
        .as_ref()
        .strip_prefix(parent.as_ref())?
        .strip_prefix('/')?;
    (!rest.contains('/')).then_some(rest)
}

/// The load `name` names, as only [`Keys::mark`] and [`Keys::log`] write it.
pub(super) fn parse_load(name: &str) -> Option<LoadId> {
    let load: LoadId = name.parse().ok()?;
    (load.to_string() == name).then_some(load)
}

/// What `name` names in a log's directory, as only [`Keys::head`], [`Keys::body`] and
/// [`Keys::gone`] write it.
pub(super) fn parse_name(name: &str) -> Option<Name> {
    let digits = |text: &str, radix: u32| {
        !text.is_empty()
            && text
                .chars()
                .all(|c| c.is_digit(radix) && !c.is_ascii_uppercase())
    };
    for (suffix, named) in [
        (".wal", Name::Head as fn(u64) -> Name),
        (".gone", Name::Gone),
    ] {
        if let Some(number) = name.strip_suffix(suffix) {
            let parsed: u64 = number.parse().ok().filter(|_| digits(number, 10))?;
            return (format!("{parsed:08}") == number).then(|| named(parsed));
        }
    }
    let (number, token) = name.strip_suffix(".body")?.split_once('.')?;
    if !digits(number, 10) || !digits(token, 16) {
        return None;
    }
    let (parsed, token) = (number.parse().ok()?, u128::from_str_radix(token, 16).ok()?);
    let canonical = format!("{parsed:08}.{token:032x}") == name.strip_suffix(".body")?;
    canonical.then_some(Name::Body(parsed, token))
}
