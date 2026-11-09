//! `rdlt init`: a starter document.

use std::path::Path;

use tokio::io::AsyncWriteExt as _;

use crate::Exit;
use crate::output::{self, Failure, Mode};

/// A pipeline of the reference connectors: the generator's rows loaded into SQLite in `directory`,
/// which the destination is let write.
///
/// Both paths are absolute and quoted as JSON is, so a document holds any directory's name.
fn starter(directory: &str) -> String {
    let path = serde_json::Value::from(format!("{directory}/starter.db"));
    let directory = serde_json::Value::from(directory);
    format!(
        "# A starter pipeline: rows the generator makes, loaded into SQLite here.\n\
         # `rdlt check rdlt.yaml` builds it; `rdlt run rdlt.yaml` loads it, with the generator and\n\
         # SQLite connectors where your host finds them (`rdlt connectors`). A sandboxed connector\n\
         # writes only where its host lets it: pass `--writable` naming this directory, or\n\
         # `--trust-binaries`.\n\
         version: 1\n\
         pipeline: starter\n\
         source:\n  \
         connector: io.rapidbyte.generator\n  \
         config: {{ seed: 7, streams: [{{ name: events, rows: 1000 }}] }}\n\
         destination:\n  \
         connector: io.rapidbyte.sqlite\n  \
         config: {{ path: {path} }}\n  \
         access: {{ write: [{directory}] }}\n\
         streams:\n  \
         - name: events\n"
    )
}

/// The working directory, as the text a document holds.
fn working_directory() -> Result<String, Failure> {
    let reading = "reading the working directory";
    let directory = std::env::current_dir().map_err(|error| Failure::io(reading, &error))?;
    directory.to_str().map(ToOwned::to_owned).ok_or_else(|| {
        let error = std::io::Error::new(std::io::ErrorKind::InvalidData, "its path is not text");
        Failure::io(reading, &error)
    })
}

/// Writes the starter document at `path`, which must not exist.
pub(crate) async fn init(path: &Path, mode: Mode) -> Result<Exit, Failure> {
    let text = starter(&working_directory()?);
    let creating =
        |error: std::io::Error| Failure::io(&format!("creating {}", path.display()), &error);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(creating)?;
    let written = async {
        file.write_all(text.as_bytes()).await?;
        file.flush().await
    };
    if let Err(error) = written.await {
        // A failed write leaves no file to refuse the next attempt.
        tokio::fs::remove_file(path).await.ok();
        return Err(Failure::io(&format!("writing {}", path.display()), &error));
    }
    let wrote = || vec![format!("wrote {}", path.display())];
    let json = serde_json::json!({ "wrote": path.display().to_string() });
    output::result(mode, wrote, &json).await?;
    Ok(Exit::Success)
}
