//! The probe an object-store log makes of its store before it keeps a log there: a store that does
//! not refuse a second create of one name would run every log unfenced, and is refused.

use std::io;

use bytes::Bytes;
use object_store::PutPayload;

use super::Shared;
use super::fault::ObjectFault;

/// Probes the store: a marker created, created again, which must be refused, listed, which must
/// show it, uploaded in parts, then deleted, after which it must be found missing.
///
/// # Errors
///
/// [`ObjectFault::Unsupported`] naming what the store does not do; [`ObjectFault::Denied`] where
/// it refuses the credentials; and as the requests fail.
pub(super) async fn probe(shared: &Shared) -> io::Result<()> {
    let calls = &shared.calls;
    let marker = shared.keys.probe(shared.token());
    let body = shared.keys.probe(shared.token());
    let probed = async {
        calls
            .create(&marker, PutPayload::from_static(b"first"))
            .await?;
        match calls
            .create(&marker, PutPayload::from_static(b"second"))
            .await
        {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
            Ok(()) => return Err(unsupported("refuse a second create of one name")),
        }
        let listed = calls.list(&shared.keys.probes()).await?;
        if !listed.iter().any(|meta| meta.location == marker) {
            return Err(unsupported("list an object once it is written"));
        }
        let id = calls.begin(&body).await?;
        let part = calls
            .part(&body, &id, 0, PutPayload::from_static(b"part"))
            .await?;
        calls.complete(&body, &id, vec![part], 4).await?;
        let uploaded = calls.read(&body, 0..5).await?;
        if uploaded != Bytes::from_static(b"part") {
            return Err(unsupported("keep what an upload in parts uploaded"));
        }
        Ok(())
    };
    let probed = probed.await;
    // The markers go whatever the probe found, and are then found missing, not refused.
    let deleted = async {
        calls.delete(&marker).await?;
        calls.delete(&body).await
    };
    let deleted = deleted.await;
    probed?;
    deleted?;
    if calls.head(&marker).await?.is_some() {
        return Err(unsupported("delete an object"));
    }
    Ok(())
}

fn unsupported(what: &'static str) -> io::Error {
    ObjectFault::Unsupported { what, source: None }.into()
}
