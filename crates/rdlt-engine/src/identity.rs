//! Deterministic row identity.
//!
//! `_rdlt_id` is a pure function of row content (keyless) or key values (keyed);
//! child ids mix parent id, position, and child content so nested changes produce new
//! child ids — which is what makes subtree merge see changed children. Determinism
//! and content-sensitivity are property-tested in the crate's integration suite.
//!
//! Hash inputs are domain-separated and length-prefixed: `("a","bc")` and `("ab","c")`
//! must not collide. The domain strings are part of the persisted identity of every
//! row ever loaded: changing one changes every `_rdlt_id`.

use std::fmt;

const DOMAIN_KEYLESS: &[u8] = b"rdlt:row-id:content:v1\0";
const DOMAIN_KEYED: &[u8] = b"rdlt:row-id:key:v1\0";
const DOMAIN_CHILD: &[u8] = b"rdlt:row-id:child:v1\0";

/// Deterministic row identity (`_rdlt_id`): content hash (keyless) or key hash (keyed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId([u8; 32]);

impl RowId {
    /// Wrap 32 raw hash bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex — the rendering that lands in the `_rdlt_id` column.
    pub fn to_hex(&self) -> String {
        let mut out = [0u8; 64];
        self.write_hex(&mut out);
        // `write_hex` emits only ASCII hex digits.
        String::from_utf8(out.to_vec()).expect("hex digits are ASCII")
    }

    /// Allocation-free hex into a stack buffer: the shredder lands three of
    /// these per row in Arrow builders, so the hot path must not allocate.
    pub fn write_hex(&self, out: &mut [u8; 64]) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (i, byte) in self.0.iter().enumerate() {
            out[i * 2] = HEX[(byte >> 4) as usize];
            out[i * 2 + 1] = HEX[(byte & 0x0f) as usize];
        }
    }
}

impl fmt::Display for RowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Streaming hasher for one row's identity. The shredder feeds canonical field bytes
/// in stable (schema) column order; no intermediate row materialization required.
#[derive(Debug)]
pub struct RowIdBuilder {
    hasher: blake3::Hasher,
}

impl RowIdBuilder {
    /// Identity for a row with no declared key: hash of full canonical content.
    /// Byte-identical rows collapse to the same id — documented dedup semantics under
    /// `Merge`.
    pub fn keyless() -> Self {
        Self::with_domain(DOMAIN_KEYLESS)
    }

    /// Identity for a keyed row: hash of the declared key fields only, so updates to
    /// non-key fields keep the id stable (what `Merge` merges on).
    pub fn keyed() -> Self {
        Self::with_domain(DOMAIN_KEYED)
    }

    fn with_domain(domain: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        Self { hasher }
    }

    /// Feed one field. `name` and canonical `value` bytes are length-prefixed.
    /// A null field must be recorded too (skipping it would make `{a:1}` and
    /// `{a:1, b:null}` collide only sometimes).
    pub fn field(&mut self, name: &str, value: FieldValue<'_>) -> &mut Self {
        self.update_lp(name.as_bytes());
        match value {
            FieldValue::Null => {
                self.hasher.update(&[0u8]);
            }
            FieldValue::Bytes(bytes) => {
                self.hasher.update(&[1u8]);
                self.update_lp(bytes);
            }
        }
        self
    }

    fn update_lp(&mut self, bytes: &[u8]) {
        self.hasher.update(&(bytes.len() as u64).to_le_bytes());
        self.hasher.update(bytes);
    }

    /// The accumulated row identity.
    pub fn finish(&self) -> RowId {
        RowId::from_bytes(*self.hasher.finalize().as_bytes())
    }
}

/// Canonical value bytes for identity hashing. The shredder renders values with the
/// same canonical encodings used for `Utf8` widening, so identity is stable across
/// type widenings of *other* columns.
#[derive(Debug, Clone, Copy)]
pub enum FieldValue<'a> {
    /// An absent value. Hashed distinctly from any byte string, so a NULL and
    /// the empty string never collide into the same identity.
    Null,
    /// The value's canonical bytes.
    Bytes(&'a [u8]),
}

/// Identity of a child row: parent id + position + the child's own content hash.
/// Position participates so reordered list elements are different rows.
pub fn child_row_id(parent: &RowId, pos: u64, child_content: &RowId) -> RowId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_CHILD);
    hasher.update(parent.as_bytes());
    hasher.update(&pos.to_le_bytes());
    hasher.update(child_content.as_bytes());
    RowId::from_bytes(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_rendering_is_lowercase_and_exact() {
        let id = RowId::from_bytes([0xAB; 32]);
        assert_eq!(id.to_hex(), "ab".repeat(32));
        let mut buf = [0u8; 64];
        id.write_hex(&mut buf);
        assert_eq!(&buf[..], id.to_hex().as_bytes());
        assert_eq!(id.to_string(), id.to_hex());
    }

    #[test]
    fn null_and_missing_are_distinct() {
        let mut with_null = RowIdBuilder::keyless();
        with_null
            .field("a", FieldValue::Bytes(b"1"))
            .field("b", FieldValue::Null);
        let mut without = RowIdBuilder::keyless();
        without.field("a", FieldValue::Bytes(b"1"));
        assert_ne!(with_null.finish(), without.finish());
    }

    #[test]
    fn field_boundaries_do_not_collide() {
        let mut a = RowIdBuilder::keyless();
        a.field("ab", FieldValue::Bytes(b"c"));
        let mut b = RowIdBuilder::keyless();
        b.field("a", FieldValue::Bytes(b"bc"));
        assert_ne!(a.finish(), b.finish());
    }

    #[test]
    fn keyed_and_keyless_domains_differ() {
        let mut keyed = RowIdBuilder::keyed();
        keyed.field("id", FieldValue::Bytes(b"1"));
        let mut keyless = RowIdBuilder::keyless();
        keyless.field("id", FieldValue::Bytes(b"1"));
        assert_ne!(keyed.finish(), keyless.finish());
    }

    #[test]
    fn child_id_changes_with_parent_pos_and_content() {
        let parent_a = RowIdBuilder::keyless().finish();
        let mut b = RowIdBuilder::keyless();
        b.field("x", FieldValue::Bytes(b"1"));
        let parent_b = b.finish();
        let content = parent_a;
        assert_ne!(
            child_row_id(&parent_a, 0, &content),
            child_row_id(&parent_b, 0, &content)
        );
        assert_ne!(
            child_row_id(&parent_a, 0, &content),
            child_row_id(&parent_a, 1, &content)
        );
    }
}
