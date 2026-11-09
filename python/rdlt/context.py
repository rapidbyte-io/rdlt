"""What a read is told, and the checkpoints it yields."""

from __future__ import annotations

from rdlt import _native
from rdlt.cursor import Cursor


class StreamContext:
    """What one partition's read is told: its partition, and whether a checkpoint is due."""

    def __init__(self, native: _native.StreamContext) -> None:
        self._native = native

    def checkpoint_due(self) -> bool:
        """Whether the engine waits for a checkpoint at the read's next safe point."""
        return self._native.checkpoint_due()

    @property
    def partition(self) -> str | None:
        """The partition read, where the stream has partitions."""
        return self._native.partition

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

    Only rows before a checkpoint commit: a resource ends on one, or the rows after it wait for
    the next run to read them again.
    """
    return _native.Checkpoint(cursor.model_dump_json())
