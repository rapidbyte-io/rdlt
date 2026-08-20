//! The shredder: a source's pushes — raw JSON slabs or already-typed Arrow
//! batches — become typed, lineage-stamped Arrow batches under the
//! schema-change policy. Two producers, one per input: [`json`] parses a
//! slab into the [`arena`] and traverses it; [`arrow`] maps a batch's own
//! schema through [`types`] and casts by [`cast`]. Both meet in [`resolve`],
//! the shared resolve→policy→build pipeline, generic over the [`view`] seam
//! so the arena and the `&serde_json::Value` test view run ONE set of
//! semantics ([`infer`], [`canonical`], [`table`], [`build`]); [`limits`]
//! holds the caps every seat consults and [`slots`] the shared
//! keyed-vector lookup the per-key seats ride.

pub(crate) mod arena;
pub(crate) mod arrow;
pub(crate) mod build;
pub(crate) mod canonical;
mod cast;
pub(crate) mod infer;
pub(crate) mod json;
pub(crate) mod limits;
pub(crate) mod resolve;
mod slots;
mod table;
pub(crate) mod types;
pub(crate) mod view;
