"""Python functions as the streams of an in-process source."""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterable, Callable, Collection, Iterable, Sequence
from dataclasses import dataclass

from rdlt import _native
from rdlt._pipeline import WriteMode
from rdlt._streams import Read, Stream
from rdlt._weak import weakly_reached
from rdlt.cursor import Cursor

RESOURCES = "io.rapidbyte.resources"
"""The connector id a pipeline's resources are placed under."""


@dataclass(frozen=True, slots=True, weakref_slot=True)
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


def _stream(resource: Resource, *, resumes: bool) -> Stream:
    """`resource` as the native source reads it, which it reaches through a weak reference."""
    reached = weakly_reached(resource, f"resource {resource.name!r}")

    def read(*arguments: object, **keywords: object) -> Iterable[object] | AsyncIterable[object]:
        return reached().function(*arguments, **keywords)

    def partitions() -> Sequence[str]:
        made = reached().partitions
        return [] if made is None else made()

    return Stream(
        resource.name,
        read,
        resource.primary_key,
        resource.cursor,
        None if resource.partitions is None else partitions,
        takes_context="ctx" in inspect.signature(resource.function).parameters,
        resumes=resumes,
    )


class _Connected:
    def __init__(self, streams: list[Stream]) -> None:
        self.streams = streams

    def check(self) -> None:
        """A function's source has nothing to check."""


def source_factory(
    resources: Sequence[Resource], resuming: Collection[str]
) -> _native.SourceFactory:
    """The source of `resources`; each named in `resuming` is read from its committed cursor."""
    streams = [_stream(resource, resumes=resource.name in resuming) for resource in resources]
    connected = _Connected(streams)
    return _native.SourceFactory(
        RESOURCES, _native.__version__, json.dumps({"type": "object"}), lambda _config: connected
    )
