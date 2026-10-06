//! The store's contract, operation by operation, as requests to the object store.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectMeta, PutPayload};
use rdlt_connector::{LoadId, PipelineId};

use super::Shared;
use super::calls::missing;
use super::head::{Kind, REFERENCE, Reference};
use super::keys::{Name, name_in, parse_load, parse_name};
use super::staged::Staged;
use crate::wal::{Chunk, Refusal, StagedChunk};

/// Bytes an identity's object holds at most: a load id's text, and room to spare.
const IDENTITY_BYTES: u64 = 64;

/// Begins `chunk` of `pipeline`'s log in `shared`'s store, where the log is open.
pub(super) async fn stage(
    shared: Arc<Shared>,
    pipeline: &PipelineId,
    chunk: Chunk,
) -> io::Result<Box<dyn StagedChunk>> {
    shared.reclaim().await;
    let mark = shared.keys.mark(pipeline, chunk.load);
    if shared.calls.head(&mark).await?.is_none() {
        return Err(missing());
    }
    let id = shared.staging(pipeline, chunk.load);
    Ok(Box::new(Staged {
        shared,
        pipeline: pipeline.clone(),
        chunk,
        id,
        held: VecDeque::new(),
        holding: 0,
        len: 0,
        upload: None,
    }))
}

/// The refusal of `meta`, an object the store never writes where it keeps a log's objects.
fn stray(meta: &Path) -> io::Error {
    Refusal::Stray {
        path: PathBuf::from(meta.to_string()),
    }
    .into()
}

impl Shared {
    pub(super) async fn identity(&self, proposed: LoadId) -> io::Result<LoadId> {
        if let Some(identity) = *self.identity.lock() {
            return Ok(identity);
        }
        let key = self.keys.identity();
        let identity = match self.calls.read(&key, 0..IDENTITY_BYTES).await {
            Ok(text) => named(&key, &text)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let text = PutPayload::from(proposed.to_string());
                match self.calls.create(&key, text).await {
                    Ok(()) => proposed,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        named(&key, &self.calls.read(&key, 0..IDENTITY_BYTES).await?)?
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        *self.identity.lock() = Some(identity);
        Ok(identity)
    }

    pub(super) async fn open_log(&self, pipeline: &PipelineId, load: LoadId) -> io::Result<()> {
        // A load whose removal left anything behind is not opened again.
        if !self
            .calls
            .list(&self.keys.log(pipeline, load))
            .await?
            .is_empty()
        {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        // Never tried again once an attempt's outcome is unknown: that attempt may land after the
        // log was removed, open it again, and with it chunks whose creates landed late too,
        // naming chunks the removal deleted. A mark landing so is a log no chunk was published in.
        let token = PutPayload::from(format!("{:032x}", self.token()));
        self.calls
            .create_once(&self.keys.mark(pipeline, load), token)
            .await
    }

    pub(super) async fn loads(&self, pipeline: &PipelineId) -> io::Result<Vec<LoadId>> {
        let marks = self.keys.marks(pipeline);
        let mut loads = BTreeSet::new();
        for meta in self.calls.list(&marks).await? {
            let load = name_in(&marks, &meta.location).and_then(parse_load);
            loads.insert(load.ok_or_else(|| stray(&meta.location))?);
        }
        Ok(loads.into_iter().collect())
    }

    pub(super) async fn leftovers(&self, pipeline: &PipelineId) -> io::Result<Vec<LoadId>> {
        // Listed before the marks: a log opened after it is not listed, so never taken for one
        // whose removal left something behind.
        let logs = self.keys.logs(pipeline);
        let mut held = BTreeSet::new();
        for dir in self.calls.dirs(&logs).await? {
            let load = name_in(&logs, &dir).and_then(parse_load);
            held.insert(load.ok_or_else(|| stray(&dir))?);
        }
        let open: BTreeSet<LoadId> = self.loads(pipeline).await?.into_iter().collect();
        Ok(held.difference(&open).copied().collect())
    }

    /// The heads and bodies of `load`'s log of `pipeline`, refusing any other object.
    async fn listed(
        &self,
        pipeline: &PipelineId,
        load: LoadId,
    ) -> io::Result<Vec<(Name, ObjectMeta)>> {
        let dir = self.keys.log(pipeline, load);
        let listed = self.calls.list(&dir).await?;
        listed
            .into_iter()
            .map(
                |meta| match name_in(&dir, &meta.location).and_then(parse_name) {
                    Some(name) => Ok((name, meta)),
                    None => Err(stray(&meta.location)),
                },
            )
            .collect()
    }

    pub(super) async fn chunks(
        &self,
        pipeline: &PipelineId,
        load: LoadId,
    ) -> io::Result<Vec<(u64, u64)>> {
        let mut heads = BTreeMap::new();
        for (name, meta) in self.listed(pipeline, load).await? {
            if let Name::Head(number) = name {
                heads.insert(number, meta.size);
            }
        }
        let mut chunks = Vec::with_capacity(heads.len());
        for (number, size) in heads {
            let chunk = Chunk { load, number };
            let kind = match self.heads.kind(pipeline, chunk) {
                Some(kind) => kind,
                None if size == REFERENCE as u64 => self.classify(pipeline, chunk).await?,
                None => Kind::Whole,
            };
            self.heads.note(pipeline, chunk, kind);
            let len = match kind {
                Kind::Whole => size,
                Kind::Parts(reference) => reference.len,
            };
            chunks.push((number, len));
        }
        Ok(chunks)
    }

    /// What `chunk`'s head is, read and noted.
    async fn classify(&self, pipeline: &PipelineId, chunk: Chunk) -> io::Result<Kind> {
        let head = self.keys.head(pipeline, chunk);
        let bytes = self.calls.read(&head, 0..REFERENCE as u64).await?;
        let kind = Reference::decode(&bytes).map_or(Kind::Whole, Kind::Parts);
        self.heads.note(pipeline, chunk, kind);
        Ok(kind)
    }

    pub(super) async fn read(
        &self,
        pipeline: &PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> io::Result<Bytes> {
        let kind = match self.heads.kind(pipeline, chunk) {
            Some(kind) => kind,
            None => self.classify(pipeline, chunk).await?,
        };
        let end = offset.saturating_add(len);
        match kind {
            Kind::Whole => {
                self.calls
                    .read(&self.keys.head(pipeline, chunk), offset..end)
                    .await
            }
            Kind::Parts(reference) => {
                let body = self.keys.body(pipeline, chunk, reference.token);
                let start = offset.min(reference.len);
                self.calls.read(&body, start..end.min(reference.len)).await
            }
        }
    }

    pub(super) async fn remove(&self, pipeline: &PipelineId, chunk: Chunk) -> io::Result<()> {
        // Its head names its body, so a removal lists nothing: its cost is the chunk's alone.
        let kind = match self.heads.kind(pipeline, chunk) {
            Some(kind) => Some(kind),
            None => match self.classify(pipeline, chunk).await {
                Ok(kind) => Some(kind),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            },
        };
        self.heads.forget(pipeline, chunk);
        // The head goes first: a chunk is gone once its head is, whatever of its body is left.
        self.calls.delete(&self.keys.head(pipeline, chunk)).await?;
        if let Some(Kind::Parts(reference)) = kind {
            let body = self.keys.body(pipeline, chunk, reference.token);
            self.calls.delete(&body).await?;
        }
        Ok(())
    }

    pub(super) async fn remove_log(&self, pipeline: &PipelineId, load: LoadId) -> io::Result<()> {
        self.reclaim().await;
        // Closed first: a publish that asks after this finds it closed and deletes its own
        // head, and one that asked before is listed below.
        self.calls.delete(&self.keys.mark(pipeline, load)).await?;
        let listed = self.listed(pipeline, load).await?;
        self.heads.forget_log(pipeline, load);
        for (_, meta) in listed {
            self.calls.delete(&meta.location).await?;
        }
        Ok(())
    }
}

/// The identity `text`, read from `key`.
fn named(key: &Path, text: &[u8]) -> io::Result<LoadId> {
    std::str::from_utf8(text)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{key} does not name a store"),
            )
        })
}
