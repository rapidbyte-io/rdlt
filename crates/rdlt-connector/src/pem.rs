//! [`PemSource`] — PEM material supplied as a path or inline.
//!
//! One shared type for every connector that takes a certificate, a trust root,
//! or a private key. The discriminator is the PEM armour itself: a value whose
//! first non-whitespace characters are `-----BEGIN` IS the material, and
//! anything else is a filesystem path to it. That rule needs to be stated once,
//! because a connector that read it differently would treat a user's key as a
//! filename or their filename as a key, and both fail confusingly.
//!
//! Serde-transparent, so config documents keep plain strings in and out. The
//! [`schemars::JsonSchema`] impl rides the crate's `schema` feature, like
//! [`crate::Secret`].
//!
//! This type does NOT wrap its value in a [`crate::Secret`], and that is
//! deliberate: a path is not a credential, and inline material is usually a
//! certificate rather than a key. A connector taking a PRIVATE key pairs this
//! with its own secrecy handling — the type says where the bytes come from,
//! not how sensitive they are.

use serde::{Deserialize, Serialize};

/// PEM material: inline text, or a filesystem path to a file containing it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct PemSource(pub String);

impl PemSource {
    /// Is the material inline rather than a path?
    ///
    /// True when the value carries PEM armour. Leading whitespace is tolerated
    /// because YAML block scalars routinely introduce it.
    pub fn is_inline(&self) -> bool {
        self.0.trim_start().starts_with("-----BEGIN")
    }

    /// The PEM bytes, reading the file when this is a path.
    ///
    /// The error is `io::Error` rather than anything connector-specific: what
    /// went wrong reading a file is the same everywhere, and each connector
    /// maps it into its own taxonomy with its own context (which field, which
    /// credential) attached.
    pub fn read(&self) -> std::io::Result<Vec<u8>> {
        if self.is_inline() {
            Ok(self.0.as_bytes().to_vec())
        } else {
            std::fs::read(&self.0)
        }
    }

    /// The PEM text, reading the file when this is a path.
    ///
    /// Convenience for the libraries that want a `String`; non-UTF-8 file
    /// contents surface as an `InvalidData` error rather than lossy text,
    /// because a mangled key fails later and further from the cause.
    pub fn read_to_string(&self) -> std::io::Result<String> {
        if self.is_inline() {
            Ok(self.0.clone())
        } else {
            std::fs::read_to_string(&self.0)
        }
    }
}

impl<T: Into<String>> From<T> for PemSource {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pem_armour_marks_material_as_inline() {
        assert!(PemSource::from("-----BEGIN PRIVATE KEY-----\nabc\n").is_inline());
        // YAML block scalars indent; the armour still decides.
        assert!(PemSource::from("\n  -----BEGIN CERTIFICATE-----\n").is_inline());
    }

    #[test]
    fn anything_else_is_a_path() {
        assert!(!PemSource::from("/etc/ssl/key.pem").is_inline());
        assert!(!PemSource::from("relative/key.p8").is_inline());
        // A path that merely CONTAINS the armour text is still a path: the
        // check is anchored at the start.
        assert!(!PemSource::from("/keys/-----BEGIN/x.pem").is_inline());
    }

    #[test]
    fn inline_material_reads_back_without_touching_the_filesystem() {
        let pem = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n";
        assert_eq!(PemSource::from(pem).read().unwrap(), pem.as_bytes());
        assert_eq!(PemSource::from(pem).read_to_string().unwrap(), pem);
    }

    #[test]
    fn a_path_reads_the_file() {
        let dir = std::env::temp_dir().join("rdlt-pem-source-test");
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("k.pem");
        std::fs::write(&path, b"-----BEGIN X-----\n").expect("write");
        let source = PemSource::from(path.to_string_lossy().into_owned());
        assert!(!source.is_inline());
        assert_eq!(source.read().unwrap(), b"-----BEGIN X-----\n");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_missing_path_surfaces_the_io_error() {
        let err = PemSource::from("/no/such/key.pem").read().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn the_wire_form_is_a_plain_string() {
        let source: PemSource = serde_json::from_str("\"/k.pem\"").expect("parse");
        assert_eq!(source, PemSource::from("/k.pem"));
        assert_eq!(
            serde_json::to_string(&source).expect("render"),
            "\"/k.pem\""
        );
    }
}
