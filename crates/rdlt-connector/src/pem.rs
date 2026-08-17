//! [`Material`] — PEM material given inline or as a path to it.
//!
//! One shared type for every connector that takes a certificate, a trust
//! root, or a private key. The discriminator is the PEM armour itself:
//! a value whose first non-whitespace characters are `-----BEGIN` IS the
//! material; anything else is a filesystem path to it. Stated once, here,
//! because a connector reading the rule differently would treat a user's
//! key as a filename or their filename as a key — both fail confusingly.
//!
//! Serde-transparent, so config documents keep plain strings in and out;
//! the `schemars::JsonSchema` impl rides the crate's `schema` feature,
//! like [`crate::secret::Secret`].
//!
//! Deliberately NOT wrapped in a [`Secret`](crate::secret::Secret): a
//! path is not a credential, and inline material is usually a certificate
//! rather than a key. A connector taking a PRIVATE key pairs this with
//! its own secrecy handling — this type says where the bytes come from,
//! not how sensitive they are.

use serde::{Deserialize, Serialize};

/// PEM material: inline text, or a filesystem path to a file holding it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct Material(String);

/// A PATH renders as itself; INLINE material renders as a placeholder.
///
/// The asymmetry is the point, and each half matters. This type carries
/// private keys, and `Debug` is what panics, logs, and error reports
/// reach for — inline bytes must not survive it. A path is not a
/// credential, and it is exactly what an operator needs to see to fix an
/// unreadable-key report; hiding it would make that report useless.
impl std::fmt::Debug for Material {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_inline() {
            // Named, not starred: the reader learns WHAT is hidden and
            // that a value is present at all.
            formatter.write_str("Material(<inline PEM>)")
        } else {
            write!(formatter, "Material({:?})", self.0)
        }
    }
}

impl Material {
    /// Wrap a value, taking it as given: whether it is inline material
    /// or a path is decided by [`Material::is_inline`] at use, never
    /// stored.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Is the material inline rather than a path?
    ///
    /// True when the value opens with PEM armour. Leading whitespace is
    /// tolerated because YAML block scalars routinely introduce it.
    pub fn is_inline(&self) -> bool {
        self.0.trim_start().starts_with("-----BEGIN")
    }

    /// The PEM bytes, reading the file when this is a path.
    ///
    /// The error stays `io::Error` on purpose: what went wrong reading a
    /// file is the same everywhere, and each connector maps it into its
    /// own taxonomy with its own context (which field, which credential).
    pub fn read(&self) -> std::io::Result<Vec<u8>> {
        if self.is_inline() {
            Ok(self.0.as_bytes().to_vec())
        } else {
            std::fs::read(&self.0)
        }
    }

    /// The PEM text, reading the file when this is a path.
    ///
    /// For the libraries that want a `String`. Non-UTF-8 file contents
    /// surface as `InvalidData` rather than lossy text — a mangled key
    /// would otherwise fail later and further from its cause.
    pub fn read_to_string(&self) -> std::io::Result<String> {
        if self.is_inline() {
            Ok(self.0.clone())
        } else {
            std::fs::read_to_string(&self.0)
        }
    }
}

impl<T: Into<String>> From<T> for Material {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn armour_at_the_start_means_inline_and_anything_else_is_a_path() {
        assert!(Material::new("-----BEGIN PRIVATE KEY-----\nMIIB\n").is_inline());
        // YAML block scalars indent; the armour still decides.
        assert!(Material::new("\n  -----BEGIN CERTIFICATE-----\n").is_inline());
        assert!(!Material::new("/etc/ssl/key.pem").is_inline());
        assert!(!Material::new("relative/key.p8").is_inline());
        // A path CONTAINING the armour text is still a path: the check is
        // anchored at the start.
        assert!(!Material::new("/keys/-----BEGIN/x.pem").is_inline());
    }

    #[test]
    fn inline_material_reads_back_without_touching_the_filesystem() {
        let material = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n";
        assert_eq!(
            Material::new(material).read().expect("inline"),
            material.as_bytes()
        );
        assert_eq!(
            Material::new(material).read_to_string().expect("inline"),
            material
        );
    }

    #[test]
    fn a_path_reads_its_file_and_a_missing_one_surfaces_the_io_error() {
        let directory = std::env::temp_dir().join("rdlt-connector-pem-test");
        std::fs::create_dir_all(&directory).expect("temp dir");
        let path = directory.join("material.pem");
        std::fs::write(&path, b"-----BEGIN X-----\n").expect("write");
        let source = Material::new(path.to_string_lossy().into_owned());
        assert!(!source.is_inline());
        assert_eq!(source.read().expect("file"), b"-----BEGIN X-----\n");
        std::fs::remove_dir_all(&directory).ok();

        let missing = Material::new("/no/such/material.pem").read().unwrap_err();
        assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn the_wire_form_is_a_plain_string() {
        let source: Material = serde_json::from_str("\"/k.pem\"").expect("parses");
        assert_eq!(source, Material::new("/k.pem"));
        assert_eq!(
            serde_json::to_string(&source).expect("renders"),
            "\"/k.pem\""
        );
    }

    /// The `From` construction path (what a config field assignment
    /// uses) agrees with `new`.
    #[test]
    fn from_and_new_agree() {
        assert_eq!(Material::from("/k.pem"), Material::new("/k.pem"));
        assert_eq!(
            Material::from(String::from("/k.pem")),
            Material::new("/k.pem")
        );
    }

    /// Inline material never renders — certificates included, because the
    /// type cannot tell which it holds — while a path always does, and
    /// the guarantee survives a derived Debug on a holder.
    #[test]
    fn debug_hides_inline_material_and_shows_paths() {
        let key = Material::new("-----BEGIN PRIVATE KEY-----\nSECRET-BYTES\n");
        let rendered = format!("{key:?}");
        assert!(!rendered.contains("SECRET-BYTES"), "{rendered}");
        assert!(rendered.contains("inline"), "{rendered}");

        assert!(format!("{:?}", Material::new("/etc/rdlt/key.p8")).contains("/etc/rdlt/key.p8"));

        #[derive(Debug)]
        struct Holder {
            key: Material,
        }
        let holder = Holder {
            key: Material::new("-----BEGIN CERTIFICATE-----\nPUBLIC-ENOUGH\n"),
        };
        assert!(!format!("{holder:?}").contains("PUBLIC-ENOUGH"));
        // Redaction is a rendering property, not storage: the material
        // still reads back in full.
        assert!(
            holder
                .key
                .read_to_string()
                .expect("inline")
                .contains("PUBLIC-ENOUGH")
        );
    }
}
