//! Throwaway certificates for tests of the protocol's TLS: a CA made at test time, and the
//! server and client certificates it issues, written as PEM files to a temporary directory.

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};

/// A certificate and its private key, as PEM files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Files {
    /// The certificate.
    pub cert: PathBuf,
    /// The private key.
    pub key: PathBuf,
}

/// A certificate authority made for one test, and the directory its files are written to.
pub struct Pki {
    dir: tempfile::TempDir,
    ca: CertifiedIssuer<'static, KeyPair>,
    name: String,
}

impl std::fmt::Debug for Pki {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Pki")
            .field("dir", &self.dir.path())
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Pki {
    /// A new CA named `name`, whose certificate is written to `<name>.pem`.
    ///
    /// # Panics
    ///
    /// When the CA cannot be made or written, which a test cannot recover from.
    pub fn new(name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().expect("a key is generated");
        let ca = CertifiedIssuer::self_signed(params, key).expect("the CA signs itself");
        let dir = tempfile::tempdir().expect("a temporary directory");
        std::fs::write(dir.path().join(format!("{name}.pem")), ca.pem())
            .expect("the CA is written");
        Self {
            dir,
            ca,
            name: name.to_owned(),
        }
    }

    /// The CA's certificate, its bundle.
    pub fn ca(&self) -> PathBuf {
        self.dir.path().join(format!("{}.pem", self.name))
    }

    /// The directory the files are written to.
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// A server certificate for `names`, host names or IP addresses, written as `<file>.pem` and
    /// `<file>.key`.
    pub fn server(&self, file: &str, names: &[&str]) -> Files {
        let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        self.issue(file, names, ExtendedKeyUsagePurpose::ServerAuth)
    }

    /// A client certificate, written as `<file>.pem` and `<file>.key`.
    pub fn client(&self, file: &str) -> Files {
        self.issue(file, Vec::new(), ExtendedKeyUsagePurpose::ClientAuth)
    }

    fn issue(&self, file: &str, names: Vec<String>, purpose: ExtendedKeyUsagePurpose) -> Files {
        let mut params = CertificateParams::new(names).expect("valid names");
        params.distinguished_name.push(DnType::CommonName, file);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![purpose];
        let key = KeyPair::generate().expect("a key is generated");
        let cert = params.signed_by(&key, &self.ca).expect("the CA signs it");
        let files = Files {
            cert: self.dir.path().join(format!("{file}.pem")),
            key: self.dir.path().join(format!("{file}.key")),
        };
        std::fs::write(&files.cert, cert.pem()).expect("the certificate is written");
        std::fs::write(&files.key, key.serialize_pem()).expect("the key is written");
        files
    }
}
