"""Python functions as the streams of an in-process source."""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterable, Callable, Collection, Iterable, Sequence
from dataclasses import dataclass

from rdlt import _native
from rdlt._pipeline import WriteMode
from rdlt.context import StreamContext
from rdlt.cursor import Cursor

RESOURCES = "io.rapidbyte.resources"
"""The connector id a pipeline's resources are placed under."""

Read = Callable[..., Iterable[object] | AsyncIterable[object]]


@dataclass(frozen=True, slots=True)
class Resource:
    """A function a pipeline reads as one stream: what it yields lands in the stream's table.

    It yields dicts (one row) or lists of dicts, Arrow data (anything with `__arrow_c_array__` or
    `__arrow_c_stream__`), and `rdlt.checkpoint(cursor)`. It takes its cursor first where it
    declares one, and `ctx`, a `StreamContext`, where it names that parameter; one that takes
    `ctx` answers the engine's barriers, checkpointing when `ctx.checkpoint_due()`.

    A resource with a cursor commits only what lies before a checkpoint: rows it yields after
    its last checkpoint are read again by the next run, from that checkpoint, so it ends on a
    `rdlt.checkpoint` to commit them; ending there, a later run with nothing new reads nothing.
    One that yields rows and never checkpoints fails the read: nothing it read could land.
    All of this is of incremental reads: read `full`, it starts again from the beginning each
    run, and a read that finishes commits what it yielded with no checkpoint.
    """

    function: Read
    name: str
    primary_key: tuple[str, ...] | None
    write: WriteMode | None
    cursor: type[Cursor] | None
    partitions: Callable[[], Sequence[str]] | None


def resource(
    name: str | None = None,
    *,
    primary_key: str | Sequence[str] | None = None,
    write: WriteMode | None = None,
    cursor: type[Cursor] | None = None,
    partitions: Callable[[], Sequence[str]] | None = None,
) -> Callable[[Read], Resource]:
    """Makes a function a stream of the pipeline it is given to as `source=`."""

    def made(function: Read) -> Resource:
        if partitions is not None and "ctx" not in inspect.signature(function).parameters:
            message = f"resource {function.__name__!r}: partitions= needs a ctx parameter"
            raise TypeError(message)
        keys = (primary_key,) if isinstance(primary_key, str) else primary_key
        return Resource(
            function=function,
            name=name or function.__name__,
            primary_key=None if keys is None else tuple(keys),
            write=write,
            cursor=cursor,
            partitions=partitions,
        )

    return made


class _Stream:
    """A resource, as the native source reads it."""

    def __init__(self, resource: Resource, *, resumes: bool) -> None:
        self._resource = resource
        self._resumes = resumes
        self._takes_context = "ctx" in inspect.signature(resource.function).parameters

    @property
    def spec(self) -> str:
        resource = self._resource
        return json.dumps(
            {
                "name": resource.name,
                "primary_key": None if resource.primary_key is None else list(resource.primary_key),
                "read_modes": ["full", "incremental"] if resource.cursor else ["full"],
                "checkpointing": "on_demand" if self._takes_context else "natural",
                "partitioned": resource.partitions is not None,
                "resumes": self._resumes,
            }
        )

    def partitions(self) -> list[str]:
        if self._resource.partitions is None:
            return []
        found = self._resource.partitions()
        if isinstance(found, str):
            message = f"resource {self._resource.name!r}: partitions are a sequence, not one string"
            raise TypeError(message)
        return list(found)

    def read(
        self, partition: str | None, cursor: str | None, context: _native.StreamContext
    ) -> Iterable[object] | AsyncIterable[object]:
        del partition
        arguments: list[object] = []
        declared = self._resource.cursor
        if declared is not None:
            arguments.append(declared() if cursor is None else declared.model_validate_json(cursor))
        keywords: dict[str, object] = {"ctx": StreamContext(context)} if self._takes_context else {}
        return self._resource.function(*arguments, **keywords)


class _Connected:
    def __init__(self, streams: list[_Stream]) -> None:
        self.streams = streams

    def check(self) -> None:
        """A function's source has nothing to check."""


def source_factory(
    resources: Sequence[Resource], resuming: Collection[str]
) -> _native.SourceFactory:
    """The source of `resources`; each named in `resuming` is read from its committed cursor."""
    streams = [_Stream(resource, resumes=resource.name in resuming) for resource in resources]
    connected = _Connected(streams)
    return _native.SourceFactory(
        RESOURCES, _native.__version__, json.dumps({"type": "object"}), lambda _config: connected
    )
