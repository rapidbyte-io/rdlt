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
of the stream's `Cursor` type."""


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
        takes_context: bool,
        replayable: bool,
    ) -> None:
        self._name = name
        self._read = read
        self._cursor = cursor
        self._partitions = partitions
        self._committed = committed
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
            (partition, None if of is None or text is None else of.model_validate_json(text))
            for partition, text in cursors
        ]
        self._committed(parsed)

    def read(
        self, partition: str, cursor: str | None, context: _native.StreamContext
    ) -> Iterable[object] | AsyncIterable[object]:
        """Opens the read, its cursor parsed and the `ctx` it names built."""
        del partition
        arguments: list[object] = []
        if self._cursor is not None:
            arguments.append(
                self._cursor() if cursor is None else self._cursor.model_validate_json(cursor)
            )
        keywords: dict[str, object] = {"ctx": StreamContext(context)} if self._takes_context else {}
        return self._read(*arguments, **keywords)
