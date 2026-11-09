"""`@source` and `@stream`: a class's methods as the streams of a source the Rust SDK drives."""

from __future__ import annotations

import functools
import inspect
import json
import types
import weakref
from collections.abc import AsyncIterable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar, cast

from rdlt import _native
from rdlt._streams import Stream
from rdlt._weak import weakly_reached
from rdlt.connector._config import Config, validated
from rdlt.cursor import Cursor

T = TypeVar("T")

_PartitionsOf = Callable[[Any], Sequence[str]] | str
"""A partitioned stream's partitions: a method taking `self`, or the name of one."""

_DECLARATION = "__rdlt_source__"
"""The attribute `@source` leaves on a class: a class it did not decorate has none of its own."""


@dataclass(frozen=True, slots=True)
class _StreamMethod:
    """A method `@stream` made a stream, which stays the method it was."""

    function: Callable[..., Any]
    name: str
    primary_key: tuple[str, ...] | None
    cursor: type[Cursor] | None
    partitions: _PartitionsOf | None
    takes_context: bool

    def __get__(self, instance: object, owner: type | None = None) -> Any:  # noqa: ANN401 - the author's method
        """The method bound to `instance`; on the class, the stream itself."""
        if instance is None:
            return self
        return types.MethodType(self.function, instance)

    def __call__(self, *arguments: object, **keywords: object) -> Any:  # noqa: ANN401 - the author's method
        """Calls the method, as its class's attribute is called."""
        return self.function(*arguments, **keywords)


def stream(
    name: str | None = None,
    *,
    primary_key: str | Sequence[str] | None = None,
    cursor: type[Cursor] | None = None,
    partitions: _PartitionsOf | None = None,
) -> Callable[[Callable[..., Any]], Any]:
    """Makes a method a stream of its class's source.

    It takes the cursor where one is declared, then `ctx` where it names it, and yields rows,
    Arrow data and `checkpoint(cursor)`. `partitions` is a method, or a method's name, answering
    the partitions; `ctx.partition` then says which one is read. The method stays a method:
    `self.tickets(since, ctx)` calls it.

    A read that ends finishes its partition, as a Rust source's does: rows yielded after its last
    checkpoint commit with it, and the partition is not read again. An incremental read that is
    to be read again, from where it stopped, ends on a `checkpoint`.
    """

    def made(function: Callable[..., Any]) -> _StreamMethod:
        takes_context = "ctx" in inspect.signature(function).parameters
        if partitions is not None and not takes_context:
            message = f"stream {function.__name__!r}: partitions= needs a ctx parameter"
            raise TypeError(message)
        keys = (primary_key,) if isinstance(primary_key, str) else primary_key
        return _StreamMethod(
            function=function,
            name=name or function.__name__,
            primary_key=None if keys is None else tuple(keys),
            cursor=cursor,
            partitions=partitions,
            takes_context=takes_context,
        )

    return made


@dataclass(frozen=True, slots=True, weakref_slot=True)
class _Declared:
    """What `@source` declared of a class, which holds it: the native side reaches it weakly."""

    connector: type
    id: str
    version: str | None
    config: type[Config]
    streams: tuple[_StreamMethod, ...]
    connections: weakref.WeakKeyDictionary[_Connected, Any] = field(
        default_factory=weakref.WeakKeyDictionary[Any, Any]
    )
    """Each connection's instance of the class, for as long as the native side holds that
    connection: the collector sees what an instance holds, which the native side's own
    reference would hide."""


@functools.cache
def package_version(top: str) -> str:
    """The version of the distribution that installed the top-level package `top`, else `0.0.0`.

    It is read once a package: finding the distribution reads every one installed.
    """
    import importlib.metadata  # noqa: PLC0415

    for distribution in importlib.metadata.packages_distributions().get(top, []):
        return importlib.metadata.version(distribution)
    return "0.0.0"


def _streams_of(connector: type) -> tuple[_StreamMethod, ...]:
    """The `@stream` methods of `connector` and of its bases.

    A subclass's own attribute of a stream's name replaces that stream.
    """
    found: dict[str, _StreamMethod] = {}
    for klass in reversed(connector.__mro__):
        for name, member in vars(klass).items():
            if isinstance(member, _StreamMethod):
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
    once `@source` makes it one. A class that has no `check` is refused here, where it is
    declared.
    """

    def made(connector: type[T]) -> type[T]:
        if not callable(getattr(connector, "check", None)):
            message = f"{connector.__name__} has no check(): a source checks it can read"
            raise TypeError(message)
        declared = _Declared(connector, id, version, config, _streams_of(connector))
        setattr(connector, _DECLARATION, declared)
        return connector

    return made


def declaration(connector: type) -> _Declared:
    """What `@source` declared of `connector`."""
    declared = vars(connector).get(_DECLARATION)
    if declared is None:
        message = f"{connector.__name__} is no source: decorate it with @source"
        raise TypeError(message)
    return cast("_Declared", declared)


def registered_once(
    existing: tuple[tuple[str, type], ...], connector: type
) -> tuple[tuple[str, type], ...]:
    """`existing` with `connector` under its id; refused where a different class has that id.

    Raises:
        ValueError: another class answers to the id.
    """
    answers = declaration(connector).id
    for known, held in existing:
        if known == answers:
            if held is not connector:
                message = f"two different connector classes answer to {answers!r}"
                raise ValueError(message)
            return existing
    return (*existing, (answers, connector))


class _Connected:
    """One connection of a class's source, as the native side holds it.

    It holds nothing of the author's itself: the class keeps the instance, under this object as
    a weak key, where the collector sees it, and letting go of this object releases it.
    """

    def __init__(self, declared: Callable[[], _Declared]) -> None:
        self._declared = declared

    @property
    def streams(self) -> list[Stream]:
        """The streams it reads, built once as the native side connects."""
        return [
            self._stream(index, method) for index, method in enumerate(self._declared().streams)
        ]

    def _instance(self) -> Any:  # noqa: ANN401 - the author's class
        return self._declared().connections[self]

    def _stream(self, index: int, method: _StreamMethod) -> Stream:
        def read(
            *arguments: object, **keywords: object
        ) -> Iterable[object] | AsyncIterable[object]:
            function = self._declared().streams[index].function
            return function(self._instance(), *arguments, **keywords)

        def partitions() -> Sequence[str]:
            of = self._declared().streams[index].partitions
            instance = self._instance()
            return getattr(instance, of)() if isinstance(of, str) else cast("Any", of)(instance)

        return Stream(
            method.name,
            read,
            method.primary_key,
            method.cursor,
            None if method.partitions is None else partitions,
            takes_context=method.takes_context,
            resumes=False,
        )

    def check(self) -> None:
        """Verifies the source can read."""
        self._instance().check()


def factory(connector: type) -> _native.SourceFactory:
    """The native source `connector`, a class `@source` made, is.

    The native factory reaches the class through a weak reference: what may still connect holds
    the class in the open, as a host and a pipeline builder do.
    """
    declared = declaration(connector)
    reached = weakly_reached(declared, f"connector {declared.id!r}")

    def connect(text: str) -> _Connected:
        found = reached()
        instance = found.connector(validated(found.config, text))
        connected = _Connected(reached)
        found.connections[connected] = instance
        return connected

    return _native.SourceFactory(
        declared.id,
        declared.version or package_version(connector.__module__.split(".")[0]),
        json.dumps(declared.config.model_json_schema()),
        connect,
    )
