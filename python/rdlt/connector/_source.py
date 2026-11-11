"""`@source` and `@stream`: functions and a class's methods as streams the Rust SDK reads."""

from __future__ import annotations

import inspect
import json
import types
from collections.abc import AsyncIterable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from rdlt import _native
from rdlt._weak import weakly_reached
from rdlt.connector._config import Config, validated
from rdlt.connector._context import Partition
from rdlt.connector._cursor import Cursor
from rdlt.connector._declared import Placement, declare, declared_as, package_version
from rdlt.connector._streams import Committed, Stream

T = TypeVar("T")

FUNCTIONS = "io.rapidbyte.functions"
"""The connector id of the in-process source a pipeline reads its stream functions as."""

_Answered = Sequence[str | Partition]
"""A stream's partitions, each an id or a `Partition`."""

_PartitionsOf = Callable[[Any], _Answered] | Callable[[], _Answered] | str
"""A partitioned stream's partitions: a function called as its stream is, or a method's name."""

_CommittedBy = Callable[[Any, Committed], None] | Callable[[Committed], None] | str
"""What a stream calls with its committed cursors: a function called as its stream is, or a
method's name."""


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ReadStream:
    """One stream of a source, which `@stream` makes of a function or of a source class's method.

    It stays what it was: a function's stream is called as the function, and a method's is the
    method, bound to its instance.
    """

    function: Callable[..., Any]
    name: str
    primary_key: tuple[str, ...] | None
    cursor: type[Cursor] | None
    partitions: _PartitionsOf | None
    committed: _CommittedBy | None
    replayable: bool
    takes_context: bool

    def __get__(self, instance: object, owner: type | None = None) -> Any:  # noqa: ANN401 - the author's method
        """The method bound to `instance`; on the class, the stream itself."""
        if instance is None:
            return self
        return types.MethodType(self.function, instance)

    def __call__(self, *arguments: object, **keywords: object) -> Any:  # noqa: ANN401 - the author's function
        """Calls the function or method, as it is called undecorated."""
        return self.function(*arguments, **keywords)


def stream(
    name: str | None = None,
    *,
    primary_key: str | Sequence[str] | None = None,
    cursor: type[Cursor] | None = None,
    partitions: _PartitionsOf | None = None,
    committed: _CommittedBy | None = None,
    replayable: bool = True,
) -> Callable[[Callable[..., Any]], Any]:
    """Makes a function a stream a pipeline reads as its source, or a method a stream of its class.

    It takes the cursor where one is declared, then `ctx` where it names it, and yields rows,
    Arrow data and `checkpoint(cursor)`. `partitions` answers the partitions, each an id or a
    `Partition`, called as the stream is, with the instance where it is a method, or names a
    method that does; `ctx.partition` then says which one is read. A method stays a method:
    `self.tickets(since, ctx)` calls it. It makes a `ReadStream`, typed `Any` so it is called and
    overridden as the function or method it was. How its rows land is the pipeline's to say, as
    `rdlt.stream(name, write=...)`, whatever its source.

    `committed`, a function called as `partitions` is or a method's name, takes the cursors each
    commit made durable, as `(partition id, cursor)` pairs, as a Rust stream's `committed` does: a
    source that acknowledges upstream, as a queue settling its messages, does it there.
    `replayable=False` declares a stream that cannot be read again from any committed cursor, as
    the Rust SDK's `StreamSpec::with_replayable` does.

    A stream without `partitions` reads `Partition.single()`, which never ends, as a Rust source's
    does, and so does a `Partition(id, unbounded=True)` that `partitions` answers. Read `full`,
    everything such a read yields commits. Read incrementally, it commits only what it yields
    before its last checkpoint, and the next run reads it again from there, so it ends on a
    `checkpoint` to commit its last rows; one that yields rows and no checkpoint after any fails
    the read, coded `checkpoint_missing`, since nothing it read could land. Any other partition
    `partitions` answers ends: a read that yields rows after its last checkpoint commits them and
    finishes the partition, which a later incremental run does not read again, and an incremental
    read that ends on a `checkpoint` leaves the partition there, for the next to read on from.
    Each full run reads every partition whole, so a stream without a cursor, read only in full, is
    read whole every run.
    """

    def made(function: Callable[..., Any]) -> ReadStream:
        takes_context = "ctx" in inspect.signature(function).parameters
        if partitions is not None and not takes_context:
            message = f"stream {function.__name__!r}: partitions= needs a ctx parameter"
            raise TypeError(message)
        keys = (primary_key,) if isinstance(primary_key, str) else primary_key
        return ReadStream(
            function=function,
            name=name or function.__name__,
            primary_key=None if keys is None else tuple(keys),
            cursor=cursor,
            partitions=partitions,
            committed=committed,
            replayable=replayable,
            takes_context=takes_context,
        )

    return made


@dataclass(frozen=True, slots=True)
class _Declared:
    """What `@source` declared of a class, which the class holds."""

    connector: type
    id: str
    version: str | None
    config: type[Config]
    streams: tuple[ReadStream, ...]


def _streams_of(connector: type) -> tuple[ReadStream, ...]:
    """The `@stream` methods of `connector` and of its bases.

    A subclass's own attribute of a stream's name replaces that stream.
    """
    found: dict[str, ReadStream] = {}
    for klass in reversed(connector.__mro__):
        for name, member in vars(klass).items():
            if isinstance(member, ReadStream):
                found[name] = member
            else:
                found.pop(name, None)
    return tuple(found.values())


def source(
    *,
    id: str,  # noqa: A002 - the connector's id, as documents name it
    config: type[Config],
    version: str | None = None,
) -> Callable[[type[T]], type[T]]:
    """Makes a class a source connector: built from its validated `config`, checked with `check()`.

    Each `@stream` method of the class and of its bases is one of its streams; a subclass's own
    attribute of a stream's name replaces that stream. A subclass is a source of its own only
    once `@source` makes it one. A class that has no `check`, or that `@destination` made, is
    refused here, where it is declared.
    """

    def made(connector: type[T]) -> type[T]:
        if not callable(getattr(connector, "check", None)):
            message = f"{connector.__name__} has no check(): a source checks it can read"
            raise TypeError(message)
        declare(
            connector, "source", _Declared(connector, id, version, config, _streams_of(connector))
        )
        return connector

    return made


def declares(connector: type) -> bool:
    """Whether `@source` made `connector` a source."""
    return declared_as(connector, "source") is not None


def declaration(connector: type) -> _Declared:
    """What `@source` declared of `connector`."""
    declared = declared_as(connector, "source")
    if declared is None:
        message = f"{connector.__name__} is no source: decorate it with @source"
        raise TypeError(message)
    return cast("_Declared", declared)


class _Connected:
    """One connection of a class's source, as the native side holds it.

    It holds nothing of the author's itself: its placement keeps the instance, under this object
    as a weak key, where the collector sees it, and letting go of this object releases it.
    """

    def __init__(self, placement: Callable[[], Placement[_Declared]]) -> None:
        self._placement = placement

    def _declared(self) -> _Declared:
        return self._placement().declared

    @property
    def streams(self) -> list[Stream]:
        """The streams it reads, built once as the native side connects."""
        return [
            self._stream(index, method) for index, method in enumerate(self._declared().streams)
        ]

    def _instance(self) -> Any:  # noqa: ANN401 - the author's class
        return self._placement().connections[self]

    def _stream(self, index: int, method: ReadStream) -> Stream:
        def read(
            *arguments: object, **keywords: object
        ) -> Iterable[object] | AsyncIterable[object]:
            function = self._declared().streams[index].function
            return function(self._instance(), *arguments, **keywords)

        def partitions() -> _Answered:
            of = self._declared().streams[index].partitions
            instance = self._instance()
            return getattr(instance, of)() if isinstance(of, str) else cast("Any", of)(instance)

        def committed(cursors: Committed) -> None:
            by = self._declared().streams[index].committed
            instance = self._instance()
            if isinstance(by, str):
                getattr(instance, by)(cursors)
            else:
                cast("Any", by)(instance, cursors)

        return Stream(
            method.name,
            read,
            method.primary_key,
            method.cursor,
            None if method.partitions is None else partitions,
            committed=None if method.committed is None else committed,
            takes_context=method.takes_context,
            replayable=method.replayable,
        )

    def check(self) -> None:
        """Verifies the source can read."""
        self._instance().check()


def placed(connector: type) -> tuple[_native.SourceFactory, Placement[_Declared]]:
    """The native source a class `@source` made, and the placement that keeps its instances.

    The native factory reaches the placement through a weak reference: what may still connect
    holds it in the open, as the host or the pipeline builder that placed the class does.
    """
    declared = declaration(connector)
    placement = Placement(declared)
    reached = weakly_reached(placement, f"connector {declared.id!r}")

    def connect(text: str) -> _Connected:
        found = reached()
        instance = found.declared.connector(validated(found.declared.config, text))
        connected = _Connected(reached)
        found.connections[connected] = instance
        return connected

    made = _native.SourceFactory(
        declared.id,
        declared.version or package_version(connector.__module__.split(".")[0]),
        json.dumps(declared.config.model_json_schema()),
        connect,
    )
    return made, placement


class _Functions:
    """The in-process source of a pipeline's stream functions, which has nothing to check."""

    def __init__(self, streams: list[Stream]) -> None:
        self.streams = streams

    def check(self) -> None:
        """A source of functions reads only what its functions yield."""


def _function_stream(made: ReadStream) -> Stream:
    """`made`, a function's stream, as the native source reads it, reaching it weakly."""
    for named in ("partitions", "committed"):
        if isinstance(getattr(made, named), str):
            message = f"stream {made.name!r}: {named}= names a method, and a function has no class"
            raise TypeError(message)
    reached = weakly_reached(made, f"stream {made.name!r}")

    def read(*arguments: object, **keywords: object) -> Iterable[object] | AsyncIterable[object]:
        return reached().function(*arguments, **keywords)

    def partitions() -> _Answered:
        return cast("Callable[[], _Answered]", reached().partitions)()

    def committed(cursors: Committed) -> None:
        cast("Callable[[Committed], None]", reached().committed)(cursors)

    return Stream(
        made.name,
        read,
        made.primary_key,
        made.cursor,
        None if made.partitions is None else partitions,
        committed=None if made.committed is None else committed,
        takes_context=made.takes_context,
        replayable=made.replayable,
    )


def functions_placed(streams: Sequence[ReadStream]) -> _native.SourceFactory:
    """The in-process source whose streams are `streams`, functions `@stream` made.

    The native source reaches each through a weak reference: what may still read it holds it in
    the open, as the pipeline builder does.
    """
    connected = _Functions([_function_stream(made) for made in streams])
    return _native.SourceFactory(
        FUNCTIONS, _native.__version__, json.dumps({"type": "object"}), lambda _config: connected
    )
