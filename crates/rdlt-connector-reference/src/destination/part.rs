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
    if unsafe_for_filename(table.as_str()) {
        return Err(DestinationError::fatal(format!(
            "reference destination: table name {:?} cannot become a part filename — \
             names carrying path separators, `..`, or control characters are refused, \
             because a filename built from them could land outside the output directory",
            table.as_str()
        )));
    }
    let name = format!(
        "{table}-{load_id}-{commit_seq}-{}.jsonl",
        digest(table.as_str(), load_id.as_str(), commit_seq)
    );
    if unsafe_for_filename(&name) {
        return Err(DestinationError::fatal(format!(
            "reference destination: generated part filename {name:?} is unsafe — path \
             separators, `..`, and control characters are refused"
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
}
