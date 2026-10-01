//! Checks a frame's shape against its schema before Arrow reads it: its buffers are ordered,
//! disjoint and padded, each long enough for its node, its nodes and buffers are exactly those
//! its columns need, and its values and the bytes its views name are within the limits.

mod layout;
#[cfg(test)]
mod tests;
mod views;

use arrow_ipc::{Buffer, FieldNode, RecordBatch};
use arrow_schema::DataType;
use flatbuffers::VectorIter;

use self::layout::{Layout, layout};
use crate::error::{Frame, Part, Problem, WireError};
use crate::limits::Limits;

/// What a frame holds, measured before Arrow reads it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Shape {
    /// Values: every node's length and every list view's size, whether or not they take bytes.
    pub values: u64,
    /// Bytes: what the frame's views name in their data buffers, counted once a view.
    pub view_bytes: u64,
    /// Bytes: the allocation holding the frame's buffers, which what is decoded from it shares.
    pub held_bytes: u64,
}

/// A buffer of a frame's body, and the alignment Arrow needs of it.
#[derive(Clone, Copy, Debug)]
pub(super) struct Placed<'a> {
    /// The buffer's bytes.
    pub(super) bytes: &'a [u8],
    /// The alignment its column's type needs: a power of two.
    pub(super) alignment: usize,
}

/// A frame's nodes and buffers in order, and what it holds.
pub(super) struct Walked<'a> {
    /// Every node the frame describes, as Arrow is to read it: those under a run-end column of
    /// no values hold none.
    pub(super) nodes: Vec<FieldNode>,
    /// Every buffer the frame describes; those under a run-end column of no values are empty.
    pub(super) placed: Vec<Placed<'a>>,
    /// The values its nodes and list views declare.
    pub(super) values: u64,
    /// The bytes its views name.
    pub(super) view_bytes: u64,
}

/// Walks `batch`, a message of `frame`'s kind received with `body`, as columns of `types`.
///
/// # Errors
///
/// A [`WireError`] when the message's shape is not one its columns have, or it holds more than
/// `limits` allow.
pub(super) fn walk<'a, 't>(
    frame: Frame,
    batch: RecordBatch<'_>,
    types: impl IntoIterator<Item = &'t DataType>,
    body: &'a [u8],
    limits: &Limits,
) -> Result<Walked<'a>, WireError> {
    let mut walk = Walk {
        frame,
        body,
        limits,
        nodes: batch.nodes().unwrap_or_default().iter(),
        buffers: batch.buffers().unwrap_or_default().iter(),
        variadic: batch.variadicBufferCounts().unwrap_or_default().iter(),
        unread: 0,
        end: 0,
        read: Vec::new(),
        placed: Vec::new(),
        values: 0,
        view_bytes: 0,
    };
    walk.columns(types)?;
    let unused = if walk.nodes.next().is_some() {
        Some(Part::Node)
    } else if walk.buffers.next().is_some() {
        Some(Part::Buffer)
    } else {
        walk.variadic.next().map(|_| Part::VariadicCount)
    };
    if let Some(part) = unused {
        return Err(WireError::malformed(frame, Problem::Unused { part }));
    }
    Ok(Walked {
        nodes: walk.read,
        placed: walk.placed,
        values: walk.values,
        view_bytes: walk.view_bytes,
    })
}

/// A node: its position, how many values it declares and whether any is null.
struct Node {
    index: usize,
    length: u64,
    nulls: bool,
}

/// A walk in progress: what of the message is left, and what it held so far.
struct Walk<'a, 'm, 'l> {
    frame: Frame,
    body: &'a [u8],
    limits: &'l Limits,
    nodes: VectorIter<'m, FieldNode>,
    buffers: VectorIter<'m, Buffer>,
    variadic: VectorIter<'m, i64>,
    /// How many run-end columns of no values the walk is under: Arrow's writer describes a run
    /// ending at zero there, which its reader refuses, so what they hold is not read.
    unread: usize,
    /// Where the last buffer ended.
    end: u64,
    /// The nodes so far, as Arrow is to read them.
    read: Vec<FieldNode>,
    placed: Vec<Placed<'a>>,
    values: u64,
    view_bytes: u64,
}

impl<'a> Walk<'a, '_, '_> {
    fn malformed(&self, problem: Problem) -> WireError {
        WireError::malformed(self.frame, problem)
    }

    /// Counts `values` more values toward the frame's, but for those nothing reads.
    fn count(&mut self, values: u64) -> Result<(), WireError> {
        if self.unread > 0 {
            return Ok(());
        }
        self.values = self.values.saturating_add(values);
        Ok(Limits::admit(
            "batch values",
            self.limits.batch_values,
            self.values,
        )?)
    }

    /// The next node, its values counted.
    fn node(&mut self) -> Result<Node, WireError> {
        let index = self.read.len();
        let Some(node) = self.nodes.next() else {
            return Err(self.malformed(Problem::Missing { part: Part::Node }));
        };
        self.read.push(if self.unread == 0 {
            *node
        } else {
            FieldNode::new(0, 0)
        });
        let (length, nulls) = (node.length(), node.null_count());
        let counted = u64::try_from(length)
            .ok()
            .filter(|_| (0..=length).contains(&nulls));
        let Some(counted) = counted else {
            return Err(self.malformed(Problem::Node {
                index,
                length,
                nulls,
            }));
        };
        self.count(counted)?;
        Ok(Node {
            index,
            length: counted,
            nulls: nulls > 0,
        })
    }

    /// The next buffer's bytes: after the buffer before it, padded, within the body and of at
    /// least `needed` bytes.
    fn buffer(&mut self, needed: u64, alignment: usize) -> Result<&'a [u8], WireError> {
        let index = self.placed.len();
        let Some(buffer) = self.buffers.next() else {
            return Err(self.malformed(Problem::Missing { part: Part::Buffer }));
        };
        let (offset, length) = (buffer.offset(), buffer.length());
        let range = usize::try_from(offset)
            .ok()
            .zip(usize::try_from(length).ok())
            .and_then(|(start, length)| Some(start..start.checked_add(length)?));
        let Some(bytes) = range.and_then(|range| self.body.get(range)) else {
            let body = len(self.body);
            return Err(self.malformed(Problem::BufferOutOfBounds {
                index,
                offset,
                length,
                body,
            }));
        };
        let (offset, length) = (offset.unsigned_abs(), len(bytes));
        if offset < self.end {
            let end = self.end;
            return Err(self.malformed(Problem::BufferOverlaps { index, offset, end }));
        }
        if offset % 8 != 0 {
            return Err(self.malformed(Problem::BufferUnaligned { index, offset }));
        }
        if length < needed {
            return Err(self.malformed(Problem::BufferTooShort {
                index,
                length,
                needed,
            }));
        }
        self.end = offset.saturating_add(length);
        let kept = if self.unread == 0 { bytes } else { &[] };
        self.placed.push(Placed {
            bytes: kept,
            alignment,
        });
        Ok(bytes)
    }

    /// The validity buffer of `node`: a bit a value where the node has nulls.
    fn validity(&mut self, node: &Node) -> Result<(), WireError> {
        let needed = if node.nulls {
            node.length.div_ceil(8)
        } else {
            0
        };
        self.buffer(needed, 1).map(drop)
    }

    /// A buffer of `width` bytes for each of `values` values, aligned to `width`.
    fn each(&mut self, values: u64, width: u64) -> Result<&'a [u8], WireError> {
        let needed = values.saturating_mul(width);
        let bytes = self.buffer(needed, usize::try_from(width).unwrap_or(1))?;
        Ok(usize::try_from(needed)
            .ok()
            .and_then(|needed| bytes.get(..needed))
            .unwrap_or(bytes))
    }

    /// The offsets buffer of `node`: one more offset than values, or none for no values.
    fn offsets(&mut self, node: &Node, width: u64) -> Result<(), WireError> {
        let offsets = if node.length == 0 {
            0
        } else {
            node.length.saturating_add(1)
        };
        self.each(offsets, width).map(drop)
    }

    /// Walks one column of `data_type`, nested columns included: its length.
    fn column(&mut self, data_type: &DataType) -> Result<u64, WireError> {
        let node = self.node()?;
        match layout(data_type) {
            Layout::Null => {}
            Layout::Fixed { width, alignment } => {
                self.validity(&node)?;
                self.buffer(node.length.saturating_mul(width), alignment)?;
            }
            Layout::Bits => {
                self.validity(&node)?;
                self.buffer(node.length.div_ceil(8), 1)?;
            }
            Layout::Bytes { offset } => {
                self.validity(&node)?;
                self.offsets(&node, offset)?;
                self.buffer(0, 1)?;
            }
            Layout::Views => self.views(&node)?,
            Layout::List { offset, item } => {
                self.validity(&node)?;
                self.offsets(&node, offset)?;
                self.column(item)?;
            }
            Layout::ListView { large: false, item } => {
                self.list_views(&node, item, |bytes| i64::from(i32::from_le_bytes(bytes)))?;
            }
            Layout::ListView { large: true, item } => {
                self.list_views(&node, item, i64::from_le_bytes)?;
            }
            Layout::FixedSizeList(item) => {
                self.validity(&node)?;
                self.column(item)?;
            }
            Layout::Struct(fields) => {
                self.validity(&node)?;
                self.columns(fields.iter().map(|field| field.data_type()))?;
            }
            Layout::Union { fields, dense } => {
                self.each(node.length, 1)?;
                if dense {
                    self.each(node.length, 4)?;
                }
                self.columns(fields.iter().map(|(_, field)| field.data_type()))?;
            }
            Layout::RunEnd { ends, values } => {
                let empty = usize::from(node.length == 0);
                self.unread += empty;
                self.columns([ends, values])?;
                self.unread -= empty;
            }
        }
        Ok(node.length)
    }

    /// Walks a column of each of `types`, in order.
    fn columns<'t>(
        &mut self,
        types: impl IntoIterator<Item = &'t DataType>,
    ) -> Result<(), WireError> {
        for data_type in types {
            self.column(data_type)?;
        }
        Ok(())
    }

    /// Walks a column of string or binary views after its node: every view names bytes of the
    /// column's data buffers, and the frame's views name no more than a frame may hold.
    fn views(&mut self, node: &Node) -> Result<(), WireError> {
        let Some(count) = self.variadic.next() else {
            return Err(self.malformed(Problem::Missing {
                part: Part::VariadicCount,
            }));
        };
        if count < 0 {
            return Err(self.malformed(Problem::VariadicCount { count }));
        }
        self.validity(node)?;
        let views = self.each(node.length, 16)?;
        let mut data = Vec::new();
        for _ in 0..count {
            data.push(len(self.buffer(0, 1)?));
        }
        let named = views::named(views, &data).map_err(|index| {
            self.malformed(Problem::View {
                node: node.index,
                index,
            })
        })?;
        self.view_bytes = self.view_bytes.saturating_add(named);
        Ok(Limits::admit(
            "view bytes",
            self.limits.frame_bytes,
            self.view_bytes,
        )?)
    }

    /// Walks a column of list views after its node: every list view names items of its child,
    /// and the items they name count toward the frame's values.
    fn list_views<const N: usize>(
        &mut self,
        node: &Node,
        item: &DataType,
        read: fn([u8; N]) -> i64,
    ) -> Result<(), WireError> {
        let width = u64::try_from(N).unwrap_or(u64::MAX);
        self.validity(node)?;
        let offsets = self.each(node.length, width)?;
        let sizes = self.each(node.length, width)?;
        let child = self.column(item)?;
        let items = views::listed(offsets, sizes, child, read).map_err(|index| {
            self.malformed(Problem::ListView {
                node: node.index,
                index,
            })
        })?;
        self.count(items)
    }
}

/// A slice's length as the `u64` limits and offsets count in.
fn len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}
