//! The probe an object-store log makes of its store before it keeps a log there: a store that takes
//! two creates of one name would run every log unfenced, and is refused.

use std::io;

use bytes::Bytes;
use futures_util::future::join_all;
use object_store::PutPayload;
use object_store::path::Path;

use super::Shared;
use super::fault::ObjectFault;
use crate::limits::{OBJECT_PROBE_RACERS, OBJECT_PROBE_ROUNDS};

/// Probes the store:
///
/// - rounds of creates of one fresh name racing, of which exactly one must be taken each round;
/// - a marker created, then read back bearing its create's token, and listed;
/// - an upload in parts made and read back;
/// - every object deleted, then found missing, not refused.
///
/// A probe is a sample: a store must take only one of creates racing every time, which an
/// operator states of it and the probe checks as far as it can.
///
/// # Errors
///
/// [`ObjectFault::Unsupported`] naming what the store does not do; [`ObjectFault::Denied`] where
/// it refuses the credentials; and as the requests fail.
pub(super) async fn probe(shared: &Shared) -> io::Result<()> {
    let calls = &shared.calls;
    let mut made = Vec::new();
    let probed = probed(shared, &mut made).await;
    // What the probe made goes whatever it found, and is then found missing, not refused.
    let mut deleted = Ok(());
    for key in &made {
        deleted = deleted.and(calls.delete(key).await);
    }
    probed?;
    deleted?;
    for key in &made {
        if calls.head(key).await?.is_some() {
            return Err(unsupported("delete an object"));
        }
    }
    Ok(())
}

/// The probe's checks, each object it makes named in `made`.
async fn probed(shared: &Shared, made: &mut Vec<Path>) -> io::Result<()> {
    let calls = &shared.calls;
    for _ in 0..OBJECT_PROBE_ROUNDS {
        let raced = shared.keys.probe(shared.token());
        made.push(raced.clone());
        let creates = (0..OBJECT_PROBE_RACERS).map(|racer| {
            let payload = PutPayload::from(format!("racer {racer}"));
            calls.create(&raced, payload)
        });
        let mut taken = 0;
        for created in join_all(creates).await {
            match created {
                Ok(()) => taken += 1,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        if taken != 1 {
            return Err(unsupported(
                "take exactly one of creates of one name racing",
            ));
        }
    }
    let marker = shared.keys.probe(shared.token());
    made.push(marker.clone());
    calls
        .create(&marker, PutPayload::from_static(b"marker"))
        .await?;
    if calls.token(&marker).await?.is_none() {
        return Err(unsupported("keep an object's metadata"));
    }
    let listed = calls.list(&shared.keys.probes()).await?;
    if !listed.iter().any(|meta| meta.location == marker) {
        return Err(unsupported("list an object once it is written"));
    }
    let body = shared.keys.probe(shared.token());
    made.push(body.clone());
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
}

fn unsupported(what: &'static str) -> io::Error {
    ObjectFault::Unsupported { what, source: None }.into()
}
