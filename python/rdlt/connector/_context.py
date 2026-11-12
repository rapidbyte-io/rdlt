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


class StreamContext:
    """What one partition's read is told: its partition, and whether a checkpoint is due.

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


def checkpoint(cursor: Cursor) -> _native.Checkpoint:
    """A checkpoint to yield: what was yielded before it is sealed, and a restart resumes there.

    Read `full`, everything a read yields commits. Read incrementally, a partition that never ends,
    as a stream without `partitions` reads, commits only what is yielded before its last checkpoint,
    and the next run reads the rest again from there, so its read ends on a checkpoint.
    """
    return _native.Checkpoint(cursor.model_dump_json())
