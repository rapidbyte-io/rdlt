//! The bucket a world's logs are kept in: what it holds is the simulated world's, not the
//! engine's, so each object's bytes count as kept for as long as they live.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result,
};
use parking_lot::Mutex;

use crate::oracle::Kept;

/// A store whose objects, and the parts of its uploads, hold copies of what they were handed,
/// each counted as kept until its last reference goes.
pub(crate) struct Disk<S> {
    inner: S,
    /// The parts of each upload begun, by its place.
    uploads: Mutex<BTreeMap<MultipartId, Vec<Option<Bytes>>>>,
}

impl<S> Disk<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            uploads: Mutex::new(BTreeMap::new()),
        }
    }
}

/// A copy the bucket holds of what it was handed; its bytes go before what kept them.
struct Copied {
    bytes: Vec<u8>,
    _kept: Kept,
}

impl AsRef<[u8]> for Copied {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// A copy of `chunks`, counted as kept before it is made.
fn copied(chunks: &[Bytes]) -> Bytes {
    let len = chunks.iter().map(Bytes::len).sum();
    let kept = Kept::held(len);
    let mut bytes = Vec::with_capacity(len);
    for chunk in chunks {
        bytes.extend_from_slice(chunk);
    }
    Bytes::from_owner(Copied { bytes, _kept: kept })
}

impl<S> fmt::Debug for Disk<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Disk")
    }
}

impl<S> fmt::Display for Disk<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Disk")
    }
}

/// The error for an upload the store never began, or one missing a part.
fn refused(what: String) -> Error {
    Error::Generic {
        store: "Disk",
        source: what.into(),
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for Disk<S> {
    async fn put_opts(
        &self,
        key: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let copy = PutPayload::from_bytes(copied(payload.as_ref()));
        drop(payload);
        self.inner.put_opts(key, copy, opts).await
    }

    /// An upload of the inner store's: the logs upload through [`MultipartStore`] alone.
    async fn put_multipart_opts(
        &self,
        key: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }

    async fn get_opts(&self, key: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(key, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl<S: ObjectStore + MultipartStore> MultipartStore for Disk<S> {
    async fn create_multipart(&self, key: &Path) -> Result<MultipartId> {
        let id = self.inner.create_multipart(key).await?;
        self.uploads.lock().insert(id.clone(), Vec::new());
        Ok(id)
    }

    async fn put_part(
        &self,
        _key: &Path,
        id: &MultipartId,
        index: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        let copy = copied(payload.as_ref());
        drop(payload);
        let mut uploads = self.uploads.lock();
        let parts = uploads
            .get_mut(id)
            .ok_or_else(|| refused(format!("no upload {id}")))?;
        if index >= parts.len() {
            parts.resize(index + 1, None);
        }
        parts[index] = Some(copy);
        Ok(PartId {
            content_id: String::new(),
        })
    }

    /// Puts the parts together as one object, as the inner store would, from a copy counted as
    /// kept before it is made.
    async fn complete_multipart(
        &self,
        key: &Path,
        id: &MultipartId,
        _parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let parts = self
            .uploads
            .lock()
            .remove(id)
            .ok_or_else(|| refused(format!("no upload {id}")))?;
        let parts: Vec<Bytes> = parts
            .into_iter()
            .enumerate()
            .map(|(index, part)| part.ok_or_else(|| refused(format!("part {index} is missing"))))
            .collect::<Result<_>>()?;
        let whole = copied(&parts);
        drop(parts);
        self.inner.abort_multipart(key, id).await?;
        let options = PutOptions {
            mode: PutMode::Overwrite,
            ..PutOptions::default()
        };
        self.inner
            .put_opts(key, PutPayload::from_bytes(whole), options)
            .await
    }

    async fn abort_multipart(&self, key: &Path, id: &MultipartId) -> Result<()> {
        drop(self.uploads.lock().remove(id));
        self.inner.abort_multipart(key, id).await
    }
}
