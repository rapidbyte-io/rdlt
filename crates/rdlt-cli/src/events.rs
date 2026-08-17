//! The `--events` NDJSON sink: one JSON object per pipeline event, to a
//! file or to stdout. Advisory like the feed itself — a write failure
//! warns once at flush and never fails the run.

use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};

use rdlt::prelude::PipelineEvent;

use crate::{exit, render};

pub(crate) enum Sink {
    Stdout,
    File(BufWriter<File>),
}

impl Sink {
    /// Open the sink `--events` named, or `None` without the flag.
    /// `--events -` claims stdout, which is the report's channel: the
    /// two machine outputs must never interleave, so it is refused
    /// unless `--report` gives the report its own destination.
    pub(crate) fn open(
        path: Option<PathBuf>,
        report_given: bool,
    ) -> Result<Option<Sink>, exit::Error> {
        let Some(path) = path else {
            return Ok(None);
        };
        if path == Path::new("-") {
            if !report_given {
                return Err(exit::Error::Usage(
                    "`--events -` writes NDJSON to stdout, where the report would go —              add `--report <path>` to give the report its own destination"
                        .into(),
                ));
            }
            return Ok(Some(Sink::Stdout));
        }
        let file = File::create(&path)
            .map_err(|e| exit::Error::Io(format!("creating {}: {e}", path.display())))?;
        Ok(Some(Sink::File(BufWriter::new(file))))
    }

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
