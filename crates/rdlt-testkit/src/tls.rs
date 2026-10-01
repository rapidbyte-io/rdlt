//! Throwaway certificates for tests of the protocol's TLS: a CA made at test time, and the
//! server and client certificates it issues, written as PEM files to a temporary directory.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, CertifiedIssuer, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyIdMethod, KeyPair, KeyUsagePurpose, RevokedCertParams,
    SanType, SerialNumber, date_time_ymd,
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
    /// The serial number of each certificate issued, by its file.
    issued: Mutex<BTreeMap<PathBuf, u64>>,
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
            issued: Mutex::new(BTreeMap::new()),
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
        let params = CertificateParams::new(names).expect("valid names");
        self.issue(file, params, ExtendedKeyUsagePurpose::ServerAuth)
    }

    /// A client certificate naming the host `file` as a DNS name, written as `<file>.pem` and
    /// `<file>.key`.
    pub fn client(&self, file: &str) -> Files {
        let params = CertificateParams::new(vec![file.to_owned()]).expect("a valid name");
        self.issue(file, params, ExtendedKeyUsagePurpose::ClientAuth)
    }

    /// A client certificate naming its host by the URI `uri` alone, written as `<file>.pem` and
    /// `<file>.key`.
    pub fn client_uri(&self, file: &str, uri: &str) -> Files {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
        let uri = uri.to_owned().try_into().expect("a valid URI");
        params.subject_alt_names = vec![SanType::URI(uri)];
        self.issue(file, params, ExtendedKeyUsagePurpose::ClientAuth)
    }

    /// A client certificate that names no host, written as `<file>.pem` and `<file>.key`.
    pub fn client_unnamed(&self, file: &str) -> Files {
        let params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
        self.issue(file, params, ExtendedKeyUsagePurpose::ClientAuth)
    }

    /// A client certificate naming the host `file` that expired long ago, written as
    /// `<file>.pem` and `<file>.key`.
    pub fn client_expired(&self, file: &str) -> Files {
        let mut params = CertificateParams::new(vec![file.to_owned()]).expect("a valid name");
        params.not_after = date_time_ymd(2001, 1, 1);
        self.issue(file, params, ExtendedKeyUsagePurpose::ClientAuth)
    }

    /// The CA's revocation list of the certificates `revoked`, current for as long as any test
    /// runs, written as `<file>.crl`.
    pub fn revoking(&self, file: &str, revoked: &[&Files]) -> PathBuf {
        self.revocations(file, revoked, 4096)
    }

    /// As [`Pki::revoking`], a list whose next update was due long ago.
    pub fn revoking_stale(&self, file: &str, revoked: &[&Files]) -> PathBuf {
        self.revocations(file, revoked, 2001)
    }

    /// The list of `revoked`, whose next update is due in the year `next_update`.
    fn revocations(&self, file: &str, revoked: &[&Files], next_update: i32) -> PathBuf {
        let issued = self.issued.lock().expect("no test panicked issuing");
        let revoked_certs = revoked
            .iter()
            .map(|files| RevokedCertParams {
                serial_number: SerialNumber::from(issued[&files.cert]),
                revocation_time: date_time_ymd(2000, 6, 1),
                reason_code: None,
                invalidity_date: None,
            })
            .collect();
        let list = CertificateRevocationListParams {
            this_update: date_time_ymd(2000, 1, 1),
            next_update: date_time_ymd(next_update, 1, 1),
            crl_number: SerialNumber::from(1_u64),
            issuing_distribution_point: None,
            revoked_certs,
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&self.ca)
        .expect("the CA signs its list");
        let path = self.dir.path().join(format!("{file}.crl"));
        std::fs::write(&path, list.pem().expect("the list encodes")).expect("the list is written");
        path
    }

    fn issue(
        &self,
        file: &str,
        mut params: CertificateParams,
        purpose: ExtendedKeyUsagePurpose,
    ) -> Files {
        params.distinguished_name.push(DnType::CommonName, file);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![purpose];
        let files = Files {
            cert: self.dir.path().join(format!("{file}.pem")),
            key: self.dir.path().join(format!("{file}.key")),
        };
        let serial = {
            let mut issued = self.issued.lock().expect("no test panicked issuing");
            let serial = u64::try_from(issued.len()).expect("few certificates") + 1;
            issued.insert(files.cert.clone(), serial);
            serial
        };
        params.serial_number = Some(SerialNumber::from(serial));
        let key = KeyPair::generate().expect("a key is generated");
        let cert = params.signed_by(&key, &self.ca).expect("the CA signs it");
        std::fs::write(&files.cert, cert.pem()).expect("the certificate is written");
        // A key is its owner's alone: the protocol's TLS refuses one others can read.
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&files.key)
            .and_then(|mut written| written.write_all(key.serialize_pem().as_bytes()))
            .expect("the key is written");
        files
    }
}
