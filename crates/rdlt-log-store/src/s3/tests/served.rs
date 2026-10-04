//! A store a test serves on a loopback address, over TLS or not, hearing each request's first
//! line and host and answering each alike.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpListener;

/// How the store answers every request.
#[derive(Clone, Copy)]
pub(super) enum Answer {
    /// As S3 does when it is too busy: a failure another attempt may not meet.
    Busy,
    /// As S3 does a request it takes as malformed: for good.
    Malformed,
}

/// Each request's first line and host, in order.
type Heard = Arc<Mutex<Vec<(String, Option<String>)>>>;

/// A store serving until the test ends.
pub(super) struct Served {
    pub(super) address: SocketAddr,
    heard: Heard,
    /// Handshakes completed and refused, over TLS.
    pub(super) completed: Arc<AtomicUsize>,
    pub(super) failed: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

impl Served {
    /// Each request's first line and host, in order.
    pub(super) fn heard(&self) -> Vec<(String, Option<String>)> {
        self.heard.lock().expect("not poisoned").clone()
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Serves on a loopback address, over TLS with `certificate` where there is one, answering as
/// `answer` says.
pub(super) async fn serve(certificate: Option<rdlt_testkit::tls::Files>, answer: Answer) -> Served {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let address = listener.local_addr().expect("an address");
    let acceptor = certificate.map(|files| {
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&files.cert)
            .expect("reads")
            .collect::<Result<_, _>>()
            .expect("certificates");
        let key = PrivateKeyDer::from_pem_file(&files.key).expect("a key");
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("TLS versions")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("a server configuration");
        tokio_rustls::TlsAcceptor::from(Arc::new(tls))
    });
    let heard = Arc::new(Mutex::new(Vec::new()));
    let (completed, failed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let counts = (
        Arc::clone(&completed),
        Arc::clone(&failed),
        Arc::clone(&heard),
    );
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            match &acceptor {
                None => answered(stream, answer, &counts.2).await,
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(stream) => {
                        counts.0.fetch_add(1, Ordering::SeqCst);
                        answered(stream, answer, &counts.2).await;
                    }
                    Err(_) => drop(counts.1.fetch_add(1, Ordering::SeqCst)),
                },
            }
        }
    });
    Served {
        address,
        heard,
        completed,
        failed,
        server,
    }
}

/// Reads a request from `stream`, notes its first line and host, and answers it.
async fn answered(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    answer: Answer,
    heard: &Mutex<Vec<(String, Option<String>)>>,
) {
    let mut request = vec![0; 8192];
    let read = stream.read(&mut request).await.unwrap_or(0);
    let text = String::from_utf8_lossy(&request[..read]).into_owned();
    let mut lines = text.lines();
    let first = lines.next().unwrap_or_default().to_owned();
    let host = lines
        .find_map(|line| {
            line.strip_prefix("host: ")
                .or_else(|| line.strip_prefix("Host: "))
        })
        .map(str::to_owned);
    heard.lock().expect("not poisoned").push((first, host));
    let answer: &[u8] = match answer {
        Answer::Busy => b"HTTP/1.1 503 Slow Down\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        Answer::Malformed => {
            b"HTTP/1.1 400 Bad Request\r\ncontent-length: 43\r\nconnection: close\r\n\r\n\
              <Error><Code>InvalidArgument</Code></Error>"
        }
    };
    stream.write_all(answer).await.ok();
    stream.shutdown().await.ok();
}
