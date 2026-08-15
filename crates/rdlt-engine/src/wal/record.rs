//! The manifest's on-disk vocabulary: one record shape per line, versioned by
//! the `Run` header.

use rdlt_core::{
    Cursor, LoadId, PipelineId, SchemaDelta, StreamName, TableName, TableSchema, WriteMode,
};
use serde::{Deserialize, Serialize};

/// WAL format version, covering the manifest record shapes, the per-line
/// checksum trailer ([`encode_line`]) AND the segment container together —
/// they are one format, because a manifest line is only meaningful if it
/// verifies and the segment it names can be decoded.
///
/// The format version is 1 until the public release; in the unpublished
/// window format changes land IN PLACE — an unreadable or version-skewed
/// WAL degrades to re-extraction, which is always safe because the WAL is
/// a replayable buffer, never the source of truth. Version ceremony (bumps,
/// migration notes) starts at the public release.
///
/// A manifest at any other version is REFUSED, exact-match in both
/// directions: a skewed header's records cannot be trusted to mean what
/// this build thinks they mean, in either direction. Recovery degrades to
/// source re-extraction either way: slower, never wrong.
pub(crate) const WAL_FORMAT_VERSION: u32 = 1;

/// One manifest line. Order on disk IS the replay order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "rec", rename_all = "snake_case")]
pub(crate) enum WalRecord {
    /// First record of each run; identifies the load for recovery commits and
    /// carries the manifest format version. The version field is REQUIRED —
    /// a header without one is not a recognized manifest (this engine is
    /// greenfield: every writer stamps it), so it lands on the corruption
    /// arms rather than defaulting to anything.
    Run {
        format_version: u32,
        load_id: LoadId,
        pipeline: PipelineId,
    },
    Delta {
        schema: TableSchema,
        delta: SchemaDelta,
        mode: WriteMode,
    },
    Segment {
        table: TableName,
        file: String,
        rows: u64,
    },
    Checkpoint {
        stream: StreamName,
        cursor: Cursor,
    },
    Committed {
        commit_seq: u64,
    },
}

/// The checksum trailer's length: `|` plus blake3's 64 hex characters.
const TRAILER_LEN: usize = 1 + 64;

/// A manifest record is metadata, not a data container. Bound each line before
/// allocating it so a corrupted WAL cannot make recovery materialize an
/// arbitrarily large line.
///
/// The cap must sit ABOVE anything this engine's own writer can legitimately
/// append, or a run's own WAL becomes unscannable (`Damaged` — safe direction,
/// but a pure availability loss). The largest legitimate lines: a Delta for a
/// maximal table create, MEASURED at 1,352,213 bytes (the shred-time bounds
/// allow 4,096 source columns with identifiers at the destination's
/// `IdentRules` length, ~150 JSON bytes each, the schema serialized TWICE —
/// ~1.3 MiB); and a Checkpoint carrying a maximal cursor — the SPI's cursor
/// contract (`rdlt_connector::MAX_CURSOR_BYTES`, 4 MiB) plus the record
/// envelope and checksum trailer. Five mebibytes covers both with headroom
/// while still bounding what a hostile line can make recovery allocate. A
/// record past this cap is REFUSED AT WRITE TIME (see `Wal::append`), so an
/// honest run can never write a line its own recovery would refuse.
///
/// The constant lives HERE, beside the encoder, so the writer's refusal and
/// the scan's cap cannot drift apart (047 wave 5, 4L6).
pub(crate) const MAX_MANIFEST_LINE_BYTES: usize = 5 * 1024 * 1024;

/// Encode one manifest line: the record's JSON, then `|`, then the
/// blake3 hex digest of exactly those JSON bytes. This is an UNKEYED
/// damage detector, not authentication: it catches accidental/torn bit
/// changes that remain valid JSON, but a process able to write the WAL
/// directory can recompute it and can reorder intact lines. WAL directory
/// ownership is therefore the security boundary; checksums only support
/// safe recovery from storage damage. `|` never needs escaping because
/// the split is positional from the line's END ([`decode_line`]), not a
/// search.
pub(crate) fn encode_line(record: &WalRecord) -> Result<Vec<u8>, serde_json::Error> {
    let mut line = serde_json::to_vec(record)?;
    let digest = blake3::hash(&line);
    line.push(b'|');
    line.extend_from_slice(digest.to_hex().as_bytes());
    Ok(line)
}

/// One decoded manifest line, classified for the scan.
pub(crate) enum ManifestLine {
    /// Checksum verified, record decoded — the only arm a replay span may
    /// be built from.
    Record(WalRecord),
    /// A complete-looking trailer whose digest does not match, or a
    /// verified line this build cannot decode. Almost always corruption
    /// (an append's tear only ever SHORTENS the line, so a tear cannot
    /// complete a trailer) — the one exception is content-dependent: a
    /// record whose JSON itself contains `|` + 64 hex (real cursor shapes
    /// like `done|<blake3>` exist) has a tear position that LOOKS
    /// trailered, and its mismatching digest lands the torn FINAL line
    /// here instead of the truncation arm. That is a safe-direction
    /// misclassification, availability only: degrade to re-extraction,
    /// never acceptance. Damaged wherever it sits, the final line
    /// included.
    Corrupt(String),
    /// No complete checksum trailer. On the FINAL line this is the torn
    /// tail a crash mid-append leaves — truncated, as always. Anywhere
    /// else it is corruption, EXCEPT that the whole-line decode attempt
    /// rides along so the scan can recognize a Run header claiming a
    /// DIFFERENT format version (a pre-checksum dev-window manifest wrote
    /// bare lines) and refuse it as `Unsupported` by version rather than
    /// misreporting corruption.
    Untrailered(Option<WalRecord>),
}

/// Classify one manifest line. Byte-positional from the end — a cursor
/// string is free to contain `|` (and multi-byte characters) because the
/// trailer is fixed-width, never searched for.
pub(crate) fn decode_line(line: &str) -> ManifestLine {
    let bytes = line.as_bytes();
    if bytes.len() > TRAILER_LEN {
        let (json, trailer) = bytes.split_at(bytes.len() - TRAILER_LEN);
        if trailer[0] == b'|'
            && trailer[1..]
                .iter()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            if blake3::hash(json).to_hex().as_bytes() != &trailer[1..] {
                return ManifestLine::Corrupt(
                    "line checksum mismatch — the recorded digest does not match the \
                     line's bytes"
                        .to_owned(),
                );
            }
            return match serde_json::from_slice::<WalRecord>(json) {
                Ok(record) => ManifestLine::Record(record),
                // The digest matched, so these are the writer's bytes — a
                // record this build cannot decode, not a tear.
                Err(e) => ManifestLine::Corrupt(format!(
                    "a checksum-verified line does not decode as a record: {e}"
                )),
            };
        }
    }
    ManifestLine::Untrailered(serde_json::from_str(line).ok())
}

/// The one segment-name format, stated once for the writer and its
/// read-side gate: `{load_id}-{seq:06}.arrow`.
pub(crate) fn segment_file_name(load_id: &LoadId, seq: u64) -> String {
    format!("{load_id}-{seq:06}.arrow")
}

/// The read-side gate on a manifest-supplied segment name, applied BEFORE
/// any `dir.join`: `Path::join` replaces the base entirely when handed an
/// absolute component, and `../` escapes it, so a forged manifest could
/// otherwise point replay at any readable file. The check is derived from
/// what the writer actually produces — [`segment_file_name`] under the
/// run's own load id (minted hex-and-dash in `runtime::run::new_load_id`;
/// the manifest's `Run` header carries it) — so path punctuation of any
/// kind, a foreign load prefix, or a malformed sequence all mean the line
/// did not come from a writer. `{seq:06}` is a MINIMUM width: six or more
/// ASCII digits.
pub(crate) fn verify_segment_file(load_id: &LoadId, file: &str) -> Result<(), String> {
    if file.contains('/') || file.contains('\\') || file.contains("..") {
        return Err(format!(
            "segment name {file:?} carries path punctuation — the writer names \
             segments `{{load_id}}-{{seq:06}}.arrow` inside the WAL directory \
             itself, so this manifest was not written by one"
        ));
    }
    let seq = file
        .strip_prefix(load_id.as_str())
        .and_then(|rest| rest.strip_prefix('-'))
        .and_then(|rest| rest.strip_suffix(".arrow"));
    match seq {
        Some(seq) if seq.len() >= 6 && seq.bytes().all(|b| b.is_ascii_digit()) => Ok(()),
        _ => Err(format!(
            "segment name {file:?} does not match this run's \
             `{load_id:?}-{{seq:06}}.arrow` writer shape"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The version field is REQUIRED: every writer stamps it, so a Run
    /// header without one is not a recognized manifest. Defaulting it —
    /// to the current version, or to any other number — would let an
    /// unversioned header masquerade as a versioned one; refusing to
    /// decode routes it to the scan's corruption arms instead.
    #[test]
    fn a_run_header_without_a_version_field_does_not_decode() {
        assert!(
            serde_json::from_str::<WalRecord>(r#"{"rec":"run","load_id":"l","pipeline":"p"}"#)
                .is_err(),
            "an unversioned header must not decode as a Run record"
        );
    }

    /// The line envelope round-trips through its own decode, and the digest
    /// binds the CONTENT: any byte change lands in the Corrupt arm.
    #[test]
    fn a_line_round_trips_and_any_byte_flip_is_corrupt() {
        let record = WalRecord::Checkpoint {
            stream: StreamName::new("s"),
            cursor: Cursor::new(serde_json::json!("wm|41")), // `|` in content is legal
        };
        let line = String::from_utf8(encode_line(&record).expect("encode")).expect("utf8");
        assert!(
            matches!(
                decode_line(&line),
                ManifestLine::Record(WalRecord::Checkpoint { .. })
            ),
            "the writer's own line must verify and decode"
        );
        let flipped = line.replace("41", "47");
        assert_ne!(flipped, line);
        assert!(
            matches!(decode_line(&flipped), ManifestLine::Corrupt(_)),
            "different valid JSON under a stale digest is corruption"
        );
    }

    /// A tear only ever SHORTENS a line, so for a record whose content
    /// carries no `|`+hex run every proper prefix classifies as
    /// Untrailered (the torn-tail arm) — never as a verified record.
    /// This is NOT a universal claim: see the companion test below for
    /// the content-dependent exception, which misclassifies only in the
    /// safe direction.
    #[test]
    fn every_torn_prefix_of_a_hexless_record_classifies_as_untrailered() {
        let record = WalRecord::Committed { commit_seq: 7 };
        let line = String::from_utf8(encode_line(&record).expect("encode")).expect("utf8");
        for cut in 0..line.len() {
            assert!(
                matches!(decode_line(&line[..cut]), ManifestLine::Untrailered(_)),
                "a tear at byte {cut} must read as a torn tail"
            );
        }
    }

    /// THE CONTENT-DEPENDENT EXCEPTION (047 round 2, doc-truth pin): a
    /// record whose JSON contains `|` + 64 hex — a real in-house cursor
    /// shape — has ONE tear position that leaves a trailer-shaped tail.
    /// The digest then mismatches, so the torn line classifies Corrupt
    /// (→ Damaged → re-extraction) instead of truncating: a
    /// SAFE-DIRECTION misclassification, degrade never acceptance, and
    /// availability-only. No tear position may VERIFY.
    #[test]
    fn a_tear_inside_embedded_hex_content_degrades_never_verifies() {
        let embedded_hex = "a".repeat(64);
        let record = WalRecord::Checkpoint {
            stream: StreamName::new("s"),
            cursor: Cursor::new(serde_json::json!(format!("done|{embedded_hex}"))),
        };
        let line = String::from_utf8(encode_line(&record).expect("encode")).expect("utf8");
        let mut corrupt_tears = 0;
        for cut in 0..line.len() {
            match decode_line(&line[..cut]) {
                ManifestLine::Untrailered(_) => {}
                ManifestLine::Corrupt(_) => corrupt_tears += 1,
                ManifestLine::Record(_) => {
                    panic!("a tear at byte {cut} must never verify as a record")
                }
            }
        }
        assert!(
            corrupt_tears > 0,
            "the embedded `|`+hex shape has at least one trailer-mimicking tear \
             position — if this stops being true the Corrupt-arm doc can claim \
             tear-proof again"
        );
    }

    /// The segment-name gate accepts exactly what [`segment_file_name`]
    /// produces and refuses everything else — the round trip is the proof
    /// the two halves cannot drift.
    #[test]
    fn the_segment_gate_is_the_writers_inverse() {
        let load = LoadId::new("19fb4c7c381-1d0ae5-0");
        for seq in [0, 42, 999_999, 1_000_000] {
            verify_segment_file(&load, &segment_file_name(&load, seq))
                .expect("the writer's own name must verify");
        }
        for evil in [
            "../../etc/hostname",
            "/etc/hostname",
            "..\\x.arrow",
            "19fb4c7c381-1d0ae5-0-..0000.arrow",
            "other-000000.arrow",
            "19fb4c7c381-1d0ae5-0-0000.arrow",
            "19fb4c7c381-1d0ae5-0-00000a.arrow",
            "19fb4c7c381-1d0ae5-0-000000.parquet",
            "",
        ] {
            assert!(
                verify_segment_file(&load, evil).is_err(),
                "{evil:?} is not the writer's shape and must refuse"
            );
        }
    }
}
