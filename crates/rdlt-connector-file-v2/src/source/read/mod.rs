//! Reading: one module per format, plus the codec-checked open they
//! share.

pub(crate) mod csv;
pub(crate) mod jsonl;
pub(crate) mod parquet;

use rdlt_connector_sdk::spi::SourceError;

use crate::format::{Codec, GZIP_MAGIC, ZSTD_MAGIC};
use crate::source::cursor::FileTask;

/// Whole-file reads reopen the LIVE local file, not the listing's
/// snapshot — so re-stat immediately before reading and refuse if the
/// file moved under the plan (030 review, docket S11: a same-size
/// replacement between listing and read used to load new content
/// against the old plan silently this run). Staged S3 fetches ARE
/// snapshots and skip this.
pub(crate) fn verify_local_snapshot(task: &FileTask) -> Result<(), SourceError> {
    if task.read_path.is_some() {
        return Ok(());
    }
    let meta = std::fs::metadata(&task.path)
        .map_err(|e| SourceError::fatal(format!("stat `{}`: {e}", task.path)))?;
    let mtime_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64);
    let moved = meta.len() != task.size_units
        || (task.mtime_ms.is_some() && mtime_ms.is_some() && mtime_ms != task.mtime_ms);
    if moved {
        return Err(SourceError::fatal(format!(
            "file `{}` changed on disk between listing and read (listed {} bytes, found              {}); refusing to load content the plan does not describe — retry the run",
            task.path,
            task.size_units,
            meta.len()
        )));
    }
    Ok(())
}

/// Open a local file through its declared codec, checking the magic
/// bytes first — a name that lies about its compression fails loudly
/// with the observed bytes, never by parsing garbage.
pub(crate) fn open_decoded(
    path: &str,
    codec: Codec,
) -> Result<Box<dyn std::io::Read + Send>, SourceError> {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path)
        .map_err(|e| SourceError::fatal(format!("opening `{path}`: {e}")))?;
    match codec {
        Codec::Plain => Ok(Box::new(file)),
        Codec::Gzip | Codec::Zstd => {
            let mut magic = [0u8; 4];
            let mut got = 0;
            while got < 4 {
                match file.read(&mut magic[got..]) {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(SourceError::fatal(format!("reading `{path}`: {e}"))),
                }
            }
            let ok = match codec {
                Codec::Gzip => got >= 2 && magic[..2] == GZIP_MAGIC,
                Codec::Zstd => got >= 4 && magic == ZSTD_MAGIC,
                Codec::Plain => unreachable!("handled above"),
            };
            if !ok {
                return Err(SourceError::fatal(format!(
                    "file `{path}` does not match its compression extension (magic bytes \
                     {:02x?}) — fix the name or the content",
                    &magic[..got]
                )));
            }
            // The sniffed bytes rejoin the stream ahead of the file.
            let chained = std::io::Cursor::new(magic[..got].to_vec()).chain(file);
            match codec {
                Codec::Gzip => Ok(Box::new(flate2::read::GzDecoder::new(chained))),
                Codec::Zstd => Ok(Box::new(
                    zstd::stream::read::Decoder::new(chained)
                        .map_err(|e| SourceError::fatal(format!("zstd `{path}`: {e}")))?,
                )),
                Codec::Plain => unreachable!("handled above"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lying extension fails with the observed bytes — on both
    /// codecs — and an honest one decodes.
    #[test]
    fn lying_extensions_fail_with_the_observed_bytes() {
        use std::io::Read as _;
        let dir = tempfile::tempdir().expect("dir");
        let lying = dir.path().join("plain.jsonl.gz");
        std::fs::write(&lying, b"{}\n").expect("write");
        let Err(err) = open_decoded(lying.to_str().expect("utf8"), Codec::Gzip) else {
            panic!("magic mismatch must refuse");
        };
        let text = format!("{err}");
        assert!(
            text.contains("does not match its compression extension")
                && text.contains("fix the name or the content"),
            "{text}"
        );

        let honest = dir.path().join("real.jsonl.gz");
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, b"{\"a\":1}\n").expect("encode");
        std::fs::write(&honest, encoder.finish().expect("finish")).expect("write");
        let mut decoded = String::new();
        open_decoded(honest.to_str().expect("utf8"), Codec::Gzip)
            .expect("opens")
            .read_to_string(&mut decoded)
            .expect("decodes");
        assert_eq!(decoded, "{\"a\":1}\n");
    }
}
