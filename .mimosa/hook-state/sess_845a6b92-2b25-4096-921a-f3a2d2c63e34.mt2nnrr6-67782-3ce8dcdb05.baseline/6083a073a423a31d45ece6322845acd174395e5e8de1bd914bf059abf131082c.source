//! Part-file naming: `<table>-<load_id>-<commit_seq>-<digest>.jsonl`,
//! deterministic per commit so a retried publish overwrites its own
//! part instead of duplicating it, and gated so no name a host or a
//! source declares can steer the write outside the output directory.

use rdlt_connector_sdk::spi::core::id::{LoadId, TableName};
use rdlt_connector_sdk::spi::error::DestinationError;

/// The part filename for one table's rows in one commit. A table name
/// is the SOURCE's declaration — third-party input by the time it
/// reaches a destination — and `TableName` is deliberately unvalidated,
/// so the seat that turns one into a filename must judge it: refused
/// typed and FATAL (no retry changes a declared name) when it carries a
/// path separator, a `..` sequence, or a control character. The built
/// name is judged again because `load_id` is the host's input and must
/// not become an accidental path capability either. Engine hosts
/// normalize names before they get here, but a direct `Backend` driver
/// never passes that gate, so the safe pattern is modeled where the
/// filename is built.
pub(crate) fn name(
    table: &TableName,
    load_id: &LoadId,
    commit_seq: u64,
) -> Result<String, DestinationError> {
    // Both refusals quote through the bounded diagnostic render:
    // `{:?}` spells control bytes inert but keeps the FULL name, and a
    // direct `Backend` driver hands these in unbounded.
    let render = |name: &str| rdlt_connector_sdk::spi::gate::render_diagnostic(name, 256);
    if unsafe_for_filename(table.as_str()) {
        return Err(DestinationError::fatal(format!(
            "reference destination: table name `{}` cannot become a part filename — \
             names carrying path separators, `..`, or control characters are refused, \
             because a filename built from them could land outside the output directory",
            render(table.as_str())
        )));
    }
    let name = format!(
        "{table}-{load_id}-{commit_seq}-{}.jsonl",
        digest(table.as_str(), load_id.as_str(), commit_seq)
    );
    if unsafe_for_filename(&name) {
        return Err(DestinationError::fatal(format!(
            "reference destination: generated part filename `{}` is unsafe — path \
             separators, `..`, and control characters are refused",
            render(&name)
        )));
    }
    // 247 = the 255-byte NAME_MAX floor (every mainstream filesystem
    // accepts a 255-byte path component) MINUS the 8-byte `_staged-`
    // prefix, the largest decoration the store adds on the way to
    // publishing: a 248-byte name passes 255 bare but its staged
    // temporary does not, failing the WRITE with ENAMETOOLONG — an io
    // error the transient classifier would retry forever, though no
    // retry shortens a name. Wrong configuration refuses FATAL where
    // the name is built, judged at the bound its longest decorated
    // form must satisfy.
    const MAX_PART_NAME_BYTES: usize = 247;
    if name.len() > MAX_PART_NAME_BYTES {
        return Err(DestinationError::fatal(format!(
            "reference destination: generated part filename `{}` is {} bytes — over the \
             {MAX_PART_NAME_BYTES}-byte bound (the 255-byte filesystem name floor less \
             the 8-byte staging prefix); shorten the table name",
            render(&name),
            name.len()
        )));
    }
    Ok(name)
}

/// The one path-safety predicate: empty, a path separator, a `..`
/// sequence, or a control character.
fn unsafe_for_filename(name: &str) -> bool {
    name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name.chars().any(char::is_control)
}

/// A short hex digest of the whole `(table, load_id, commit_seq)`
/// tuple. The plain `{table}-{load}-{seq}` spelling collides across
/// dash-rich ids — `("a", "b-c")` and `("a-b", "c")` name one file, the
/// later publish silently overwriting the earlier tuple's rows — so the
/// inputs are length-prefixed and domain-separated to make the name
/// injective; the digest is a pure function of the tuple, so a retried
/// publish still overwrites its own part deterministically.
fn digest(table: &str, load_id: &str, commit_seq: u64) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"rdlt-reference:part:v1\0");
    for field in [table.as_bytes(), load_id.as_bytes()] {
        hasher.update(&(field.len() as u64).to_le_bytes());
        hasher.update(field);
    }
    hasher.update(&commit_seq.to_le_bytes());
    hasher.finalize().to_hex()[..8].to_owned()
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::spi::core::id::{LoadId, TableName};

    use super::name;

    /// The table gate and the built-name gate are the same predicate at
    /// two seats: a clean tuple names its part, a traversal-shaped table
    /// or load id is refused.
    #[test]
    fn generated_parts_gate_the_table_and_the_load_id() {
        let table = TableName::new("orders");
        assert!(name(&table, &LoadId::new("load"), 1).is_ok());
        assert!(name(&table, &LoadId::new("load/escape"), 1).is_err());
        assert!(name(&table, &LoadId::new("load..escape"), 1).is_err());
        assert!(name(&TableName::new("../evil"), &LoadId::new("load"), 1).is_err());
    }

    /// A path-safe but over-long table name refuses FATAL where the
    /// name is built: the filesystem would refuse the write with
    /// ENAMETOOLONG — an io error the transient classifier would retry
    /// forever — and no retry shortens a name. The boundary is 247
    /// bytes on the BUILT name: the 255-byte NAME_MAX floor less the
    /// 8-byte staging prefix, judged at the longest decorated form the
    /// name must survive.
    #[test]
    fn a_too_long_built_name_refuses_fatal_where_it_is_built() {
        let long_table = TableName::new("t".repeat(300));
        let refused = name(&long_table, &LoadId::new("load"), 1)
            .expect_err("a built name past the staged bound refuses");
        let rendered = refused.to_string();
        assert!(
            rendered.starts_with("fatal destination error: "),
            "fatal, not the transient retry bait ENAMETOOLONG would be: {rendered}"
        );
        assert!(
            rendered.contains("247") && rendered.contains("255") && rendered.contains("8-byte"),
            "the refusal carries the bound and its derivation: {rendered}"
        );
    }

    /// The boundary, both sides: a built name of exactly 247 bytes
    /// passes the gate (its staged form is exactly the 255-byte
    /// NAME_MAX floor), and one byte more refuses AT THE GATE — never
    /// left for the staging write to bounce as a transient. The suffix
    /// for `(load, 1)` is 22 bytes, so the table lengths are 225/226.
    #[test]
    fn the_bound_admits_247_and_refuses_248() {
        let admitted = name(&TableName::new("t".repeat(225)), &LoadId::new("load"), 1)
            .expect("a 247-byte built name passes");
        assert_eq!(admitted.len(), 247);
        let refused = name(&TableName::new("t".repeat(226)), &LoadId::new("load"), 1)
            .expect_err("a 248-byte built name refuses at the gate");
        assert!(
            refused.to_string().contains("248 bytes"),
            "the refusal names the offending length: {refused}"
        );
    }

    /// Both refusals quote the offending name bounded: a control byte
    /// arrives spelled out, and a multi-KiB name arrives truncated with
    /// the marker — never the full unbounded echo a direct `Backend`
    /// driver could otherwise plant.
    #[test]
    fn part_name_refusals_render_bounded_and_inert() {
        let hostile = format!("evil\u{1b}]52;c;A\u{7}/{}", "x".repeat(2000));
        let refused = name(&TableName::new(hostile), &LoadId::new("load"), 1)
            .expect_err("a hostile table name refuses");
        let rendered = refused.to_string();
        assert!(
            !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
            "no raw control byte survives the refusal: {rendered:?}"
        );
        assert!(
            rendered.contains("truncated from") && rendered.len() < 700,
            "the echo is bounded with the marker: {} bytes",
            rendered.len()
        );

        let long_load = LoadId::new(format!("load/{}", "y".repeat(2000)));
        let refused = name(&TableName::new("orders"), &long_load, 1)
            .expect_err("a hostile load id refuses at the built-name gate");
        assert!(
            refused.to_string().len() < 700,
            "the built-name echo is bounded too: {} bytes",
            refused.to_string().len()
        );
    }
}
