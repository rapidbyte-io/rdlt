"""What a read is told, and the checkpoints it yields."""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from rdlt import _native
from rdlt.connector._cursor import Cursor


@dataclass(frozen=True, slots=True)
class Partition:
    """A partition of a stream, as its `partitions` answers it: its id, and whether it never ends.

    Only a full read finishes a partition that never ends: any other read leaves it at its last
    checkpoint, and the next run reads again what it yielded after that checkpoint. A read of a
    partition that ends finishes it where the read ends on rows, which commit with it.
    """

    id: str
    unbounded: bool = False

    @classmethod
    def single(cls) -> Partition:
        """The partition of a stream that is not split, `whole`, which never ends."""
        return cls("whole", unbounded=True)


class PartitionState(enum.Enum):
    """Where a partition stands that no cursor says, as the Rust SDK's `PartitionState` says it."""

    DONE = "done"
    """The partition is read to its end."""


@dataclass(frozen=True, slots=True)
class StreamState:
    """A stream's committed position, as the Rust SDK's `StreamState` is.

    `phase` is the stream's phase, `0` until a plan names another. `partitions` maps each
    partition's id to where it stands: the stream's cursor, where its next read resumes, `None`
    for a stream that declares no cursor, or `PartitionState.DONE`, read to its end.
    `generation` is the full read in progress, which a `replace` stream fills, and `completed`
    the generations of recently completed full reads, oldest first.
    """

    phase: int = 0
    partitions: Mapping[str, Any] = field(default_factory=dict[str, Any], hash=False)
    generation: int | None = None
    completed: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class Exclusion:
    """The top-level columns a stream excludes, by name, as the Rust SDK's `Exclusion` says them.

    A read may leave them out of what it yields, as a database source leaves them out of its
    query: whatever it yields, the engine drops them. A name no column has excludes nothing.
    """

    columns: tuple[str, ...] = ()
    """The columns it excludes, in order."""

    def is_empty(self) -> bool:
        """Whether it excludes nothing."""
        return not self.columns

    def contains(self, name: str) -> bool:
        """Whether it excludes the column `name`."""
        return name in self.columns


@dataclass(frozen=True, slots=True, kw_only=True)
class Limits:
    """The limits the engine holds a read's pushes to, as the Rust SDK's `Limits` says them.

    A read that keeps within them is refused nothing: rows encode to JSON pushes of at most
    `json_push_bytes`, each value nested at most `nesting_depth` levels, and a cursor at most
    `cursor_bytes`.
    """

    frame_bytes: int
    """Bytes in one frame."""
    batch_rows: int
    """Rows in one batch."""
    schema_columns: int
    """Columns in one schema."""
    nesting_depth: int
    """Levels a value nests, the row itself the first."""
    json_push_bytes: int
    """Bytes in one push of rows as JSON."""
    cursor_bytes: int
    """Bytes in one cursor."""
    config_bytes: int
    """Bytes in the configuration document."""
    control_string_bytes: int
    """Bytes in one string field of a control message."""
    batch_values: int
    """Values in one frame."""
    schema_bytes: int
    """Bytes in one schema message."""
    dictionary_bytes: int
    """Bytes the dictionaries one read holds at once take."""
    catalog_bytes: int
    """Bytes in one catalog."""
    state_bytes: int
    """Bytes one message that carries state or a stream's positions holds decoded."""
    control_message_bytes: int
    """Bytes in any other control message."""


class StreamContext:
    """What one partition's read is told: its partition, what it reads within, and when to stop.

    It is what a Rust read is told by its `&Partition` and asks its `Emitter`: Python's read
    yields what a Rust read emits.
    """

    def __init__(self, native: _native.StreamContext) -> None:
        self._native = native

    def checkpoint_due(self) -> bool:
        """Whether the engine waits for a checkpoint at the read's next safe point."""
        return self._native.checkpoint_due()

    @property
    def partition(self) -> Partition:
        """The partition read: `Partition.single()` for a stream that is not split."""
        return Partition(self._native.partition, unbounded=self._native.unbounded)

    @property
    def follows(self) -> bool:
        """Whether the read follows an unbounded partition once caught up."""
        return self._native.follows

    @property
    def resumes(self) -> bool:
        """Whether the read was given a cursor to start from."""
        return self._native.resumes

    @property
    def excluded(self) -> Exclusion:
        """The columns the stream excludes, which the read may leave out of what it yields.

        Whatever it yields, every batch is projected without them, and rows' values of them are
        skipped.
        """
        return Exclusion(tuple(self._native.excluded))

    @property
    def limits(self) -> Limits:
        """The limits the engine holds the read's pushes to."""
        return Limits(**dict(self._native.limits))

    def stopped(self) -> bool:
        """Whether the engine asked the read to stop, or stopped listening.

        A read that waits for data, as one following its partition does, asks between its waits
        and returns, as a Rust read selects on its emitter's `stopped()`: its thread then ends
        with the run, rather than at its next yield.
        """
        return self._native.stopped()


def checkpoint(cursor: Cursor) -> _native.Checkpoint:
    """A checkpoint to yield: what was yielded before it is sealed, and a restart resumes there.

    Read `full`, everything a read yields commits. Read incrementally, a partition that never ends,
    as a stream without `partitions` reads, commits only what is yielded before its last checkpoint,
    and the next run reads the rest again from there, so its read ends on a checkpoint.
    """
    return _native.Checkpoint(cursor.model_dump_json())
