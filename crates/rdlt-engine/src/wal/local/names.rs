//! The names a local log's directories and files take, and the only names its listings accept.

use std::ffi::OsStr;

use rdlt_connector::{LoadId, PipelineId};

/// The lower-case alphabet of base32 (RFC 4648), which no case-folding file system confuses.
const BASE32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// The name of `pipeline`'s directory, which no other pipeline's takes, on a file system that
/// folds case too: `p.` and the id where it holds no upper-case letter, `x.` and the id in
/// lower-case base32 otherwise, at most 207 bytes.
pub(super) fn pipeline(pipeline: &PipelineId) -> String {
    let id = pipeline.as_str();
    if id.bytes().any(|byte| byte.is_ascii_uppercase()) {
        format!("x.{}", base32(id.as_bytes()))
    } else {
        format!("p.{id}")
    }
}

/// `bytes` in lower-case base32, unpadded.
fn base32(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let (mut buffer, mut bits) = (0_u16, 0_u32);
    for byte in bytes {
        buffer = (buffer << 8) | u16::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(char::from(BASE32[usize::from((buffer >> bits) & 31)]));
        }
        buffer &= (1 << bits) - 1;
    }
    if bits > 0 {
        out.push(char::from(BASE32[usize::from((buffer << (5 - bits)) & 31)]));
    }
    out
}

/// The name of the file a load's directory holds while its log is open.
pub(super) const OPEN: &str = "open";

/// The name of `load`'s directory.
pub(super) fn load(load: LoadId) -> String {
    load.to_string()
}

/// The name of a file staging chunk `number`, `token` telling apart two that stage one number.
pub(super) fn part(number: u64, token: u64) -> String {
    format!("{number:08}.{token:016x}.part")
}

/// The name of chunk `number`'s file.
pub(super) fn chunk(number: u64) -> String {
    format!("{number:08}.wal")
}

/// Whether `name` is one a file system or a desktop makes beside what it is shown, as NFS keeps
/// a file removed while open (`.nfs…`) or a file browser its settings (`.DS_Store`): the store
/// never writes a name that begins with a dot, so it never takes one for its own.
pub(super) fn is_made_by_system(name: &OsStr) -> bool {
    name.as_encoded_bytes().first() == Some(&b'.')
}

/// The name of the directory `load`'s log is opened in before it takes its own name: begun with a
/// dot, so no listing reads it as a log.
pub(super) fn opening(load: LoadId) -> String {
    format!(".{load}.opening")
}

/// The load whose log `name` is being opened in, as only [`opening`] writes it.
pub(super) fn parse_opening(name: &OsStr) -> Option<LoadId> {
    let load = name.to_str()?.strip_prefix('.')?.strip_suffix(".opening")?;
    parse_load(OsStr::new(load))
}

/// The load `name` names, as only [`load`] writes it.
pub(super) fn parse_load(name: &OsStr) -> Option<LoadId> {
    let name = name.to_str()?;
    let load: LoadId = name.parse().ok()?;
    (self::load(load) == name).then_some(load)
}

/// Whether `name` names a file staging a chunk, as only [`part`] writes it.
pub(super) fn is_part(name: &OsStr) -> bool {
    let parsed = name.to_str().and_then(|name| {
        let (number, token) = name.strip_suffix(".part")?.split_once('.')?;
        let digits = |text: &str, radix| text.bytes().all(|byte| char::from(byte).is_digit(radix));
        if !digits(number, 10) || !digits(token, 16) {
            return None;
        }
        let parsed = part(number.parse().ok()?, u64::from_str_radix(token, 16).ok()?);
        Some(parsed == name)
    });
    parsed == Some(true)
}

/// The chunk `name` names, as only [`chunk`] writes it.
pub(super) fn parse_chunk(name: &OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let digits = name.strip_suffix(".wal")?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let number: u64 = digits.parse().ok()?;
    (chunk(number) == name).then_some(number)
}
