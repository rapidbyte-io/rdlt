"""A Python read as the native source reads it: its spec, its partitions and its generator."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, Callable, Iterable, Sequence

from rdlt import _native
from rdlt.context import StreamContext
from rdlt.cursor import Cursor

Read = Callable[..., Iterable[object] | AsyncIterable[object]]
"""A stream's read: takes its cursor first where it declares one, and `ctx` where it names it."""


class Stream:
    """One stream, as the native source holds it: `read` and `partitions` reach the author's code.

    They reach it weakly, so a generator or a class that holds its pipeline can be collected with
    that pipeline; what may still read it holds it in the open.
    """

    def __init__(
        self,
        name: str,
        read: Read,
        primary_key: tuple[str, ...] | None,
        cursor: type[Cursor] | None,
        partitions: Callable[[], Sequence[str]] | None,
        *,
        takes_context: bool,
        resumes: bool,
    ) -> None:
        self._name = name
        self._read = read
        self._cursor = cursor
        self._partitions = partitions
        self._takes_context = takes_context
        self._spec = json.dumps(
            {
                "name": name,
                "primary_key": None if primary_key is None else list(primary_key),
                "read_modes": ["full", "incremental"] if cursor else ["full"],
                "checkpointing": "on_demand" if takes_context else "natural",
                "partitioned": partitions is not None,
                "resumes": resumes,
            }
        )

    @property
    def spec(self) -> str:
        """The stream's description, as JSON."""
        return self._spec

    def partitions(self) -> list[str]:
        """The partitions to read; none for a stream without partitions."""
        if self._partitions is None:
            return []
        found = self._partitions()
        if isinstance(found, str):
            message = f"stream {self._name!r}: partitions are a sequence, not one string"
            raise TypeError(message)
        return list(found)

    def read(
        self, partition: str | None, cursor: str | None, context: _native.StreamContext
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
