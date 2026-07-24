//! The format layer — what each file format IS to rdlt, shared by the
//! source and (after the dest absorption) the destination side.

pub(crate) mod csv;
pub(crate) mod jsonl;
pub(crate) mod parquet;

use serde::{Deserialize, Serialize};

/// Shared streaming I/O slab (8 MiB): the reusable buffer the JSONL and CSV
/// readers fill per flush, and the source's file read buffer. One knob keeps
/// all three read paths on the same allocation size.
pub(crate) const SLAB_BYTES: usize = 8 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Format {
    Jsonl,
    Parquet,
    /// Record stream via NDJSON conversion: rows follow the type-inference lattice
    /// with `type_hints` overrides, and the whole file is one incremental unit.
    Csv,
}

/// CSV reader options: `csv: {delimiter, header, quote}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CsvOptions {
    /// Single-byte field delimiter (default `,`).
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// First row is the header (default true); without it, columns are
    /// `c0..cN`.
    #[serde(default = "default_true")]
    pub header: bool,
    /// Single-byte quote character (default `"`).
    #[serde(default = "default_quote")]
    pub quote: char,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: default_delimiter(),
            header: default_true(),
            quote: default_quote(),
        }
    }
}

impl CsvOptions {
    pub fn validate(&self, context: &str) -> Result<(), String> {
        for (name, value) in [("delimiter", self.delimiter), ("quote", self.quote)] {
            if !value.is_ascii() {
                return Err(format!(
                    "{context}: csv.{name} `{value}` must be a single ASCII byte"
                ));
            }
        }
        Ok(())
    }
}

fn default_delimiter() -> char {
    ','
}
fn default_quote() -> char {
    '"'
}
fn default_true() -> bool {
    true
}

/// Compression codec, decided per FILE by extension (R5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Codec {
    Plain,
    Gzip,
    Zstd,
}

impl Codec {
    pub(crate) fn is_plain(self) -> bool {
        self == Codec::Plain
    }
}

pub(crate) fn codec_of(path: &str) -> Codec {
    if path.ends_with(".gz") {
        Codec::Gzip
    } else if path.ends_with(".zst") {
        Codec::Zstd
    } else {
        Codec::Plain
    }
}

/// Open a LOCAL file through its codec, checking magic bytes first —
/// a codec/extension mismatch is a typed error naming the file, never
/// garbage rows (R5).
pub(crate) fn open_decoded(
    path: &str,
    codec: Codec,
) -> Result<Box<dyn std::io::Read + Send>, rdlt_connector::SourceError> {
    use rdlt_connector::SourceError;
    use std::io::Read;
    let mut file = std::fs::File::open(path)
        .map_err(|e| SourceError::fatal(format!("opening `{path}`: {e}")))?;
    match codec {
        Codec::Plain => Ok(Box::new(file)),
        Codec::Gzip | Codec::Zstd => {
            let mut magic = [0u8; 4];
            let got = {
                let mut filled = 0;
                while filled < 4 {
                    match file.read(&mut magic[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => {
                            return Err(SourceError::fatal(format!("reading `{path}`: {e}")));
                        }
                    }
                }
                filled
            };
            let ok = match codec {
                Codec::Gzip => got >= 2 && magic[..2] == [0x1f, 0x8b],
                Codec::Zstd => got >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd],
                Codec::Plain => unreachable!(),
            };
            if !ok {
                return Err(SourceError::fatal(format!(
                    "file `{path}` does not match its compression extension \
                     (magic bytes {:02x?}) — fix the name or the content",
                    &magic[..got]
                )));
            }
            let chained = std::io::Cursor::new(magic[..got].to_vec()).chain(file);
            match codec {
                Codec::Gzip => Ok(Box::new(flate2::read::GzDecoder::new(chained))),
                Codec::Zstd => Ok(Box::new(
                    zstd::stream::read::Decoder::new(chained)
                        .map_err(|e| SourceError::fatal(format!("zstd `{path}`: {e}")))?,
                )),
                Codec::Plain => unreachable!(),
            }
        }
    }
}
