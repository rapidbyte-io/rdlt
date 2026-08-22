//! [`Material`] — PEM given inline or as a path to it.
//!
//! Config mechanics, beside the SPI's `Secret`: it says where a
//! certificate, trust root or private key's BYTES come from, not what
//! they mean. The discriminator is the PEM armour itself — a value
//! whose first non-whitespace characters are `-----BEGIN` IS the
//! material; anything else is a filesystem path to it. Stated once,
//! here, because two connectors reading the rule differently would
//! treat one operator's key as a filename or their filename as a key,
//! and both fail confusingly.
//!
//! Serde-transparent, so config documents keep plain strings in and
//! out; the `schemars` impl rides the `schema` feature like the rest of
//! the config surface.
//!
//! Deliberately NOT a `Secret`: a path is not a credential, and inline
//! material is usually a certificate rather than a key. How sensitive
//! the bytes are is the connector's business; this says only where they
//! came from.

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
            formatter.write_str("Material::new(<inline PEM>)")
        } else {
            write!(formatter, "Material::new({:?})", self.0)
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

    /// The value exactly as configured — a path, or the inline material.
    ///
    /// For code that must COMPARE what two sources say (a connection
    /// string against a policy field, say). Not for reporting: use
    /// [`Material::describe`], which cannot leak a key into a message.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// How to name this source in a message an operator will read.
    ///
    /// A path renders as itself — that is what someone needs to fix a
    /// misconfiguration. Inline material renders as a placeholder: the
    /// bytes may be a private key, and an error message is exactly the
    /// place they must not appear. The same asymmetry `Debug` makes,
    /// available where a message is being built deliberately.
    pub fn describe(&self) -> String {
        if self.is_inline() {
            "<inline PEM>".to_string()
        } else {
            self.0.clone()
        }
    }

    /// Is the material inline rather than a path?
    ///
    /// True when the value opens with PEM armour. Leading whitespace is
    /// tolerated because YAML block scalars routinely introduce it.
    pub fn is_inline(&self) -> bool {
        self.0.trim_start().starts_with("-----BEGIN")
    }

    /// The greatest file a path-form value may name.
    ///
    /// PEM is kilobytes — a chain of certificates is a few, a key is
    /// one — so this is generous by orders of magnitude. It is the
    /// config document's own ceiling, chosen for the symmetry that
    /// makes it explainable: the path form admits exactly what the
    /// inline form could have carried, since an inline value arrives
    /// inside a document already held to this bound.
    pub const MAX_BYTES: u64 = 8 * 1024 * 1024;

    /// Read the path under the shared document discipline: opened
    /// non-blocking (a FIFO at the path is judged, not parked on), the
    /// handle's kind judged before a byte is read, the read bounded at
    /// [`Material::MAX_BYTES`] plus one so a file that grows under the
    /// reader refuses rather than slurps. A symlink is FOLLOWED and
    /// judged by what it points at: a certificate living behind
    /// `/etc/ssl/certs` is routinely a link, and refusing links would
    /// refuse honest configurations to catch nothing a kind check does
    /// not already catch.
    fn read_path(&self) -> std::io::Result<Vec<u8>> {
        rdlt_connector::core::fs::read_document(std::path::Path::new(&self.0), Self::MAX_BYTES)
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::InvalidInput => std::io::Error::new(
                    error.kind(),
                    format!("{error} — PEM material must be a file or inline text"),
                ),
                std::io::ErrorKind::InvalidData => std::io::Error::new(
                    error.kind(),
                    format!("{error} of PEM material — a certificate or key is kilobytes"),
                ),
                _ => error,
            })
    }

    /// The PEM bytes, reading the file when this is a path.
    ///
    /// The file is judged on the handle that is then read — its kind
    /// and its size: this type is what every connector taking a
    /// certificate reaches for, so an ungated open here would teach the
    /// hole rather than close it.
    ///
    /// The error stays `io::Error` on purpose: what went wrong reading a
    /// file is the same everywhere, and each connector maps it into its
    /// own taxonomy with its own context (which field, which credential).
    pub fn read(&self) -> std::io::Result<Vec<u8>> {
        if self.is_inline() {
            Ok(self.0.as_bytes().to_vec())
        } else {
            self.read_path()
        }
    }

    /// The PEM text, reading the file when this is a path — its handle
    /// judged like [`Material::read`]'s.
    ///
    /// For the libraries that want a `String`. Non-UTF-8 file contents
    /// surface as `InvalidData` rather than lossy text — a mangled key
    /// would otherwise fail later and further from its cause.
    pub fn read_to_string(&self) -> std::io::Result<String> {
        if self.is_inline() {
            Ok(self.0.clone())
        } else {
            String::from_utf8(self.read()?).map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.utf8_error())
            })
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

    /// A path is judged before it is opened. This type is the primitive
    /// every connector taking a certificate reaches for, so what it
    /// does with a hostile path is what all of them will do: a
    /// character device would read without end, a FIFO would park the
    /// opener until a writer appeared, a directory fails every read.
    /// None can be judged after opening.
    #[test]
    fn a_path_that_is_not_a_regular_file_refuses_before_the_open() {
        let refusal = Material::new("/dev/zero")
            .read()
            .expect_err("a character device is not PEM material");
        assert_eq!(refusal.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            refusal.to_string().contains("not a regular file"),
            "the refusal says what it judged: {refusal}"
        );

        let dir = tempfile::tempdir().expect("tempdir");
        let refusal = Material::new(dir.path().display().to_string())
            .read_to_string()
            .expect_err("a directory is not PEM material");
        assert_eq!(refusal.kind(), std::io::ErrorKind::InvalidInput);
    }

    /// The path form admits exactly what the inline form could have
    /// carried — a value past the document ceiling could not have
    /// arrived inline, and pointing at it does not make it admissible.
    #[test]
    fn a_file_past_the_ceiling_refuses_naming_the_bound() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("huge.pem");
        // Sparse: the ceiling is judged from the metadata, so the test
        // costs an inode rather than eight megabytes.
        let file = std::fs::File::create(&path).expect("create");
        file.set_len(Material::MAX_BYTES + 1).expect("size it");
        drop(file);

        let refusal = Material::new(path.display().to_string())
            .read()
            .expect_err("a file past the ceiling refuses");
        assert!(
            refusal
                .to_string()
                .contains(&Material::MAX_BYTES.to_string()),
            "the refusal names the ceiling: {refusal}"
        );

        // At the ceiling it reads: the bound admits what it says it does.
        let path = dir.path().join("big.pem");
        let file = std::fs::File::create(&path).expect("create");
        file.set_len(Material::MAX_BYTES).expect("size it");
        drop(file);
        Material::new(path.display().to_string())
            .read()
            .expect("a file AT the ceiling is admitted");
    }

    /// A FIFO at the path is judged and refused, not parked on: the
    /// open is non-blocking, so a pipe with no writer cannot hold the
    /// connector until one appears. The test would hang rather than
    /// fail if it ever did.
    #[test]
    fn a_fifo_at_the_path_refuses_without_waiting_for_a_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pipe.pem");
        let status = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .expect("mkfifo runs");
        assert!(status.success());
        let refusal = Material::new(path.display().to_string())
            .read()
            .expect_err("a FIFO is not PEM material");
        assert_eq!(refusal.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            refusal.to_string().contains("not a regular file"),
            "{refusal}"
        );
    }

    /// A file that grows past the ceiling is refused at the ceiling,
    /// not slurped: the read is bounded one past it whatever the
    /// metadata said when the file was opened.
    #[test]
    fn growth_past_the_ceiling_refuses_at_the_ceiling() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("growing.pem");
        let file = std::fs::File::create(&path).expect("create");
        file.set_len(Material::MAX_BYTES + 1).expect("grow it");
        drop(file);
        let refusal = Material::new(path.display().to_string())
            .read()
            .expect_err("grown past the ceiling");
        assert!(
            refusal
                .to_string()
                .contains(&Material::MAX_BYTES.to_string())
                && refusal.to_string().contains("PEM material"),
            "{refusal}"
        );
    }

    /// A symlink is followed and judged by what it points at: a
    /// certificate behind `/etc/ssl/certs` is routinely a link, so
    /// refusing links would refuse honest configurations while catching
    /// nothing the kind check misses.
    #[test]
    fn a_symlink_to_a_regular_file_is_followed_and_admitted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real.pem");
        std::fs::write(&real, b"-----BEGIN CERTIFICATE-----\n").expect("seed");
        let link = dir.path().join("link.pem");
        std::os::unix::fs::symlink(&real, &link).expect("link");

        let read = Material::new(link.display().to_string())
            .read()
            .expect("a link to a regular file is admitted");
        assert!(read.starts_with(b"-----BEGIN"));
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
