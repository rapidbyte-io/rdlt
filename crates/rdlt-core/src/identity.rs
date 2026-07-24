//! Deterministic row identity.
//!
//! `_rdlt_id` is a pure function of row content (keyless) or key values (keyed);
//! child ids mix parent id, position, and child content so nested changes produce new
//! child ids — which is what makes subtree merge see changed children. Determinism is
//! property-tested in `tests/identity_props.rs`.
//!
//! Hash inputs are domain-separated and length-prefixed: `("a","bc")` and `("ab","c")`
//! must not collide.

use crate::ids::RowId;

const DOMAIN_KEYLESS: &[u8] = b"rdlt:row-id:content:v1\0";
const DOMAIN_KEYED: &[u8] = b"rdlt:row-id:key:v1\0";
const DOMAIN_CHILD: &[u8] = b"rdlt:row-id:child:v1\0";

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

    pub fn finish(&self) -> RowId {
        RowId::from_bytes(*self.hasher.finalize().as_bytes())
    }
}

/// Canonical value bytes for identity hashing. The shredder renders values with the
/// same canonical encodings used for `Utf8` widening, so identity is stable across
/// type widenings of *other* columns.
#[derive(Debug, Clone, Copy)]
pub enum FieldValue<'a> {
    Null,
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
