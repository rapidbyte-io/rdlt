//! The `--events` NDJSON sink: one JSON object per pipeline event, to a
//! file or to stdout. Advisory like the feed itself — a write failure
//! warns once at flush and never fails the run.

use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};

use rdlt::prelude::PipelineEvent;

use crate::{exit, render};

/// Where `--events` points, resolved before anything runs but opened
/// only once the pipeline is built — a document that refuses must not
/// truncate an event log from an earlier run.
pub(crate) enum Target {
    Stdout,
    File(PathBuf),
}

impl Target {
    /// Resolve the flag, or `None` without it. `--events -` claims
    /// stdout, which is the report's channel: the two machine outputs
    /// must never interleave, so it is refused unless `--report` gives
    /// the report its own destination.
    pub(crate) fn resolve(
        path: Option<PathBuf>,
        report_given: bool,
    ) -> Result<Option<Target>, exit::Error> {
        let Some(path) = path else {
            return Ok(None);
        };
        if path != Path::new("-") {
            return Ok(Some(Target::File(path)));
        }
        if report_given {
            Ok(Some(Target::Stdout))
        } else {
            Err(exit::Error::Usage(
                "`--events -` writes NDJSON to stdout, where the report would go —              add `--report <path>` to give the report its own destination"
                    .into(),
            ))
        }
    }

    pub(crate) fn open(self) -> Result<Sink, exit::Error> {
        match self {
            Target::Stdout => Ok(Sink::Stdout),
            Target::File(path) => {
                let file = File::create(&path)
                    .map_err(|e| exit::Error::Io(format!("creating {}: {e}", path.display())))?;
                Ok(Sink::File(BufWriter::new(file)))
            }
        }
    }
}

pub(crate) enum Sink {
    Stdout,
    File(BufWriter<File>),
}

impl Sink {
    pub(crate) fn write(&mut self, event: &PipelineEvent) {
        if let Ok(line) = serde_json::to_string(event) {
            match self {
                // Not `println!`: a closed consumer must degrade, not
                // panic the feed task.
                Sink::Stdout => {
                    let mut stdout = std::io::stdout().lock();
                    let _ = writeln!(stdout, "{line}");
                }
                Sink::File(w) => {
                    let _ = writeln!(w, "{line}");
                }
            }
        }
    }

    pub(crate) fn finish(self) {
        if let Sink::File(mut w) = self
            && let Err(e) = w.flush()
        {
            render::stderr::line(&format!("warning: flushing --events file: {e}"));
        }
    }
}
