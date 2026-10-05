//! A chunk an object-store log stages: held in memory up to a part, uploaded in parts beyond it,
//! and published by creating its head where its name is free, then asking whether its log is open.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::{MultipartId, PutPayload};
use rdlt_connector::{BoxFuture, PipelineId};

use super::Shared;
use super::calls::missing;
use super::fault::ObjectFault;
use super::head::{Kind, Reference};
use crate::wal::{Chunk, StagedChunk};

/// A staged chunk: what it holds not yet uploaded, and the upload in parts it began, if any.
pub(super) struct Staged {
    pub(super) shared: Arc<Shared>,
    pub(super) pipeline: PipelineId,
    pub(super) chunk: Chunk,
    /// Which of the store's stagings it is.
    pub(super) id: u64,
    pub(super) held: VecDeque<Bytes>,
    /// Bytes `held` holds.
    pub(super) holding: usize,
    /// Bytes appended in all.
    pub(super) len: u64,
    pub(super) upload: Option<Upload>,
}

/// An upload in parts of a chunk's body.
pub(super) struct Upload {
    token: u128,
    key: Path,
    id: MultipartId,
    parts: Vec<PartId>,
}

impl Staged {
    /// Uploads the first `bytes` held as the body's next part, beginning the upload first where
    /// none is.
    async fn upload(&mut self, bytes: usize) -> io::Result<()> {
        let shared = Arc::clone(&self.shared);
        let calls = &shared.calls;
        let payload = self.take(bytes);
        let upload = if let Some(upload) = self.upload.take() {
            upload
        } else {
            let token = shared.token();
            let key = shared.keys.body(&self.pipeline, self.chunk, token);
            let id = calls.begin(&key).await?;
            Upload {
                token,
                key,
                id,
                parts: Vec::new(),
            }
        };
        let uploaded = calls
            .part(&upload.key, &upload.id, upload.parts.len(), payload)
            .await;
        let mut upload = upload;
        match uploaded {
            Ok(part) => {
                upload.parts.push(part);
                self.upload = Some(upload);
                Ok(())
            }
            Err(error) => {
                calls.abort(&upload.key, &upload.id).await;
                Err(error)
            }
        }
    }

    /// The first `bytes` held, no longer held.
    fn take(&mut self, mut bytes: usize) -> PutPayload {
        let mut taken = Vec::new();
        while bytes > 0
            && let Some(first) = self.held.front_mut()
        {
            let piece = first.split_to(bytes.min(first.len()));
            bytes -= piece.len();
            taken.push(piece);
            if first.is_empty() {
                self.held.pop_front();
            }
        }
        self.holding = self.held.iter().map(Bytes::len).sum();
        PutPayload::from_iter(taken)
    }

    /// The head to publish: what is held where nothing was uploaded, otherwise a reference to
    /// the body, completed from what is held, with the body's key.
    async fn head(&mut self) -> io::Result<(PutPayload, Option<(Path, Reference)>)> {
        let Some(mut upload) = self.upload.take() else {
            let whole = self.take(self.holding);
            return Ok((whole, None));
        };
        let calls = &self.shared.calls;
        let last = PutPayload::from_iter(std::mem::take(&mut self.held));
        let body = async {
            let part = calls
                .part(&upload.key, &upload.id, upload.parts.len(), last)
                .await?;
            upload.parts.push(part);
            calls
                .complete(&upload.key, &upload.id, upload.parts.clone(), self.len)
                .await
        };
        if let Err(error) = body.await {
            calls.abort(&upload.key, &upload.id).await;
            return Err(error);
        }
        let reference = Reference {
            token: upload.token,
            len: self.len,
        };
        Ok((
            PutPayload::from(reference.encode()),
            Some((upload.key, reference)),
        ))
    }

    async fn publish_now(mut self) -> io::Result<()> {
        // A staging its log's stagings were deleted since is never published.
        if !self.shared.unstage(self.id) {
            if let Some(upload) = self.upload.take() {
                self.shared.calls.abort(&upload.key, &upload.id).await;
            }
            return Err(missing());
        }
        let (payload, uploaded) = self.head().await?;
        let body = uploaded.as_ref().map(|(key, _)| key);
        let shared = Arc::clone(&self.shared);
        let calls = &shared.calls;
        let head = shared.keys.head(&self.pipeline, self.chunk);
        match calls.create(&head, payload).await {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if let Some(body) = body {
                    calls.delete(body).await?;
                }
                return Err(error);
            }
            created => created?,
        }
        // Asked only once the head is there: a removal that closed the log after this answer
        // lists the log after it, and deletes the head with the rest.
        let mark = shared.keys.mark(&self.pipeline, self.chunk.load);
        if calls.head(&mark).await?.is_none() {
            calls.delete(&head).await?;
            if let Some(body) = body {
                calls.delete(body).await?;
            }
            return Err(missing());
        }
        let kind = uploaded.map_or(Kind::Whole, |(_, reference)| Kind::Parts(reference));
        shared.heads.note(&self.pipeline, self.chunk, kind);
        Ok(())
    }
}

impl StagedChunk for Staged {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            let most = self.shared.chunk_most();
            let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if self.len.saturating_add(len) > most {
                let what = "hold a chunk longer than its parts allow";
                return Err(ObjectFault::Unsupported { what, source: None }.into());
            }
            self.len += len;
            self.holding += bytes.len();
            self.held.push_back(bytes);
            let part = self.shared.calls.options.part_bytes.get();
            // A chunk of a part or less is published whole; past that, every full part is
            // uploaded as it fills, and the last with the publish.
            while self.holding > part {
                self.upload(part).await?;
            }
            Ok(())
        })
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        Box::pin(self.publish_now())
    }

    fn discard(mut self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        Box::pin(async move {
            self.shared.unstage(self.id);
            if let Some(upload) = self.upload.take() {
                self.shared.calls.abort(&upload.key, &upload.id).await;
            }
            Ok(())
        })
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        self.shared.unstage(self.id);
        // An upload no publish or discard ended is given up by the store's next request.
        if let Some(upload) = self.upload.take() {
            self.shared.abandoned.lock().push((upload.key, upload.id));
        }
    }
}
