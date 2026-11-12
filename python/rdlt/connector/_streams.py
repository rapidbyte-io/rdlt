"""A Python read as the native source reads it: its spec, its partitions and its generator."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, Callable, Iterable, Sequence
from typing import Any

from rdlt import _native
from rdlt.connector._context import Partition, StreamContext
from rdlt.connector._cursor import Cursor

Read = Callable[..., Iterable[object] | AsyncIterable[object]]
"""A stream's read: takes its cursor first where it declares one, and `ctx` where it names it."""

Committed = Sequence[tuple[str, Any]]
"""Committed cursors: each partition's id, `whole` for a stream that is not split, and its cursor,
of the stream's `Cursor` type, or `None` for a stream that declares none."""

Acknowledged = Callable[[str], Cursor | None]
"""Where a stream stands for a partition outside the engine: the cursor it was last told is
committed, or `None` where it keeps none."""


class Stream:
    """One stream, as the native source holds it: `read` and `partitions` reach the author's code.

    They reach it weakly, so a function or a class that holds its pipeline can be collected with
    that pipeline; what may still read it holds it in the open.
    """

    def __init__(
        self,
        name: str,
        read: Read,
        primary_key: tuple[str, ...] | None,
        cursor: type[Cursor] | None,
        partitions: Callable[[], Sequence[str | Partition]] | None,
        *,
        committed: Callable[[Committed], None] | None,
        acknowledged: Acknowledged | None,
        cursor_version: int,
        takes_context: bool,
        replayable: bool,
    ) -> None:
        self._name = name
        self._read = read
        self._cursor = cursor
        self._partitions = partitions
        self._committed = committed
        self._acknowledged = acknowledged
        self._takes_context = takes_context
        self._spec = json.dumps(
            {
                "name": name,
                "primary_key": None if primary_key is None else list(primary_key),
                "read_modes": ["full", "incremental"] if cursor else ["full"],
                "checkpointing": "on_demand" if takes_context else "natural",
                "partitioned": partitions is not None,
                "replayable": replayable,
                "committed": committed is not None,
                "acknowledged": acknowledged is not None,
                "cursor_version": cursor_version,
            }
        )

    @property
    def spec(self) -> str:
        """The stream's description, as JSON."""
        return self._spec

    def partitions(self) -> list[tuple[str, bool]]:
        """Each partition and whether it never ends; none for a stream without partitions."""
        if self._partitions is None:
            return []
        found = self._partitions()
        if isinstance(found, str):
            message = f"stream {self._name!r}: partitions are a sequence, not one string"
            raise TypeError(message)
        return [
            (each.id, each.unbounded) if isinstance(each, Partition) else (each, False)
            for each in found
        ]

    def committed(self, cursors: Sequence[tuple[str, str | None]]) -> None:
        """Tells the author's code that `cursors`, each as JSON, are committed."""
        if self._committed is None:
            return
        of = self._cursor
        parsed = [
            (partition, None if of is None else _parsed(of, text)) for partition, text in cursors
        ]
        self._committed(parsed)

    def acknowledged(self, partition: str) -> str | None:
        """Where the author's code says the stream stands for `partition`, as JSON.

        Raises:
            TypeError: it answered neither the stream's cursor nor `None`.
        """
        if self._acknowledged is None:
            return None
        found = self._acknowledged(partition)
        if found is None:
            return None
        if self._cursor is None or not isinstance(found, self._cursor):
            expected = "no cursor" if self._cursor is None else self._cursor.__name__
            message = (
                f"stream {self._name!r}: acknowledged= answered {type(found).__name__}: it "
                f"answers the stream's cursor, {expected}, or None"
            )
            raise TypeError(message)
        return found.model_dump_json()

    def read(
        self, partition: str, cursor: str | None, context: _native.StreamContext
    ) -> Iterable[object] | AsyncIterable[object]:
        """Opens the read, its cursor parsed and the `ctx` it names built."""
        del partition
        arguments: list[object] = []
        if self._cursor is not None:
            arguments.append(_parsed(self._cursor, cursor))
        keywords: dict[str, object] = {"ctx": StreamContext(context)} if self._takes_context else {}
        return self._read(*arguments, **keywords)


def _parsed(of: type[Cursor], cursor: str | None) -> Cursor:
    """`cursor`, JSON, as an `of`; `of`'s default, where a read starts, for none."""
    return of() if cursor is None else of.model_validate_json(cursor)
