# pyright: reportPrivateUsage=false
"""A destination's sessions and table writers: the Rust SDK's `Session` and `TableWriter`."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from rdlt._answers import answered, nothing
from rdlt.connector._commit import CommitMeta, Receipt, told
from rdlt.model import Json, _parse

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True, slots=True)
class WriteStats:
    """What a table writer's `flush` staged: its rows, and its bytes as the destination counts."""

    rows: int
    bytes: int


@runtime_checkable
class TableWriter(Protocol):
    """Stages batches into one table, as the Rust SDK's `TableWriter` does: what `writer` answers.

    What it stages is invisible until a commit publishes it, and what it stages once a newer
    session opened is never published: its `flush` raises `Fenced`, or keeps it apart.
    """

    def write(self, segment: int, batch: pa.RecordBatch, /) -> None:
        """Stages `batch` as part of `segment`."""
        ...

    def flush(self) -> WriteStats:
        """Makes every write so far durable in staging, answering what it staged."""
        ...


@runtime_checkable
class Session(Protocol):
    """One load's session with a destination, as the Rust SDK's `Session` is: `Opened.session`.

    Each method is a plain function, which rdlt calls on a thread of its own, where it may block,
    and answers as its annotation says, `None` where it is called for what it does: anything else,
    a coroutine or a generator among them, fails the call, as `False` does where it answers `None`.
    """

    def apply_schema(self, change: Mapping[str, Json], /) -> None:
        """Applies a schema change before any write under the new version."""
        ...

    def writer(self, table: Mapping[str, Json], /) -> TableWriter:
        """A table writer for `table`."""
        ...

    def discard_staged(self) -> None:
        """Removes what sessions of the pipeline older than this one staged."""
        ...

    def commit(self, meta: CommitMeta, /) -> Receipt:
        """Publishes what `meta` says with its state changes, atomically, answering its receipt."""
        ...

    def close(self) -> None:
        """Ends the session."""
        ...


class _Writer:
    """An author's table writer, as the native side calls it, each call in the turn Rust gave it."""

    def __init__(self, writer: TableWriter) -> None:
        self._writer = writer
        self._name = type(writer).__name__

    def write(self, segment: int, batch: object) -> None:
        """Stages the native batch `batch`, read as pyarrow's, sharing its buffers."""
        import pyarrow as pa  # noqa: PLC0415

        written = self._writer.write(segment, pa.record_batch(cast("Any", batch)))
        nothing(f"{self._name}.write()", written)

    def flush(self) -> tuple[int, int]:
        """The rows and bytes the flush staged."""
        flushed = self._writer.flush()
        stats = answered(f"{self._name}.flush()", flushed, WriteStats, "rdlt.connector.WriteStats")
        return stats.rows, stats.bytes


class NativeSession:
    """An author's session, as the native side calls it: arguments and answers as the SDK's JSON.

    Each call comes in the turn the Rust SDK gave it as it made the call: a call the engine
    stopped waiting for still runs, and holds its turn until it returns.
    """

    def __init__(self, session: Session) -> None:
        self._session = session
        self._name = type(session).__name__

    def apply_schema(self, change: str) -> None:
        """Applies the schema change `change`."""
        nothing(f"{self._name}.apply_schema()", self._session.apply_schema(_parse(change)))

    def writer(self, table: str) -> _Writer:
        """A table writer of `table`."""
        made = answered(
            f"{self._name}.writer()",
            self._session.writer(_parse(table)),
            TableWriter,
            "a table writer, with write() and flush()",
        )
        return _Writer(made)

    def discard_staged(self) -> None:
        """Removes what older sessions staged."""
        nothing(f"{self._name}.discard_staged()", self._session.discard_staged())

    def commit(self, meta: str) -> str:
        """Commits `meta`, answering its receipt."""
        receipt = cast(
            "Mapping[str, Json]",
            answered(
                f"{self._name}.commit()",
                self._session.commit(told(_parse(meta))),
                Mapping,
                "its receipt, a mapping",
            ),
        )
        return json.dumps(dict(receipt))

    def close(self) -> None:
        """Ends the session."""
        nothing(f"{self._name}.close()", self._session.close())
