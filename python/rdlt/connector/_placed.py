"""What the native source reads of a Python source: a class's connections, or stream functions.

Each connection reads the streams its class declares or its `streams()` finds, and a pipeline's
stream functions are the streams of one source; the native side reaches each stream weakly.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterable, Callable, Iterable, Sequence
from functools import partial
from typing import Any, cast

from rdlt import _native
from rdlt._weak import weakly_reached
from rdlt.connector._config import validated
from rdlt.connector._context import PartitionPlan, StreamState
from rdlt.connector._cursor import Cursor
from rdlt.connector._declared import Placement, built, package_version
from rdlt.connector._source import Declared, ReadStream, declaration
from rdlt.connector._spec import StreamSpec, described
from rdlt.connector._streams import Committed, Stream, as_plan

FUNCTIONS = "io.rapidbyte.functions"
"""The connector id of the in-process source a pipeline reads its stream functions as."""


class _Connected:
    """One connection of a class's source, as the native side holds it.

    It holds nothing of the author's itself: its placement keeps the instance, and the streams it
    found, under this object as a weak key, where the collector sees them, and letting go of this
    object releases them.
    """

    def __init__(self, placement: Callable[[], Placement[Declared]]) -> None:
        self._placement = placement

    def _declared(self) -> Declared:
        return self._placement().declared

    @property
    def streams(self) -> list[Stream]:
        """The streams it reads: those its class declares, or those its `streams()` found.

        It is read once, as the native side connects.
        """
        declared = self._declared()
        if not declared.finds:
            return [
                _native_stream(method, partial(self._method, index), self._instance)
                for index, method in enumerate(declared.streams)
            ]
        found = _found(declared.connector, self._instance().streams())
        self._placement().found[self] = found
        return [
            _native_stream(made, partial(self._found, index), None)
            for index, made in enumerate(found)
        ]

    @property
    def discovers(self) -> bool:
        """Whether its class answers its catalog itself."""
        return self._declared().discovers

    def discover(self) -> str:
        """The catalog its `discover()` answers, each stream's spec as the native side reads it.

        Raises:
            TypeError: it answered something other than a sequence of `StreamSpec`s.
        """
        answered = self._instance().discover()
        specs: list[object] = [] if isinstance(answered, str) else list(answered)
        if isinstance(answered, str) or not all(isinstance(spec, StreamSpec) for spec in specs):
            message = (
                f"{self._declared().connector.__name__}.discover() answers a sequence of "
                "StreamSpecs, one a stream"
            )
            raise TypeError(message)
        return json.dumps([described(cast("StreamSpec", spec)) for spec in specs])

    def _instance(self) -> Any:  # noqa: ANN401 - the author's class
        return self._placement().connections[self]

    def _method(self, index: int) -> ReadStream:
        return self._declared().streams[index]

    def _found(self, index: int) -> ReadStream:
        return self._placement().found[self][index]

    def check(self) -> None:
        """Verifies the source can read."""
        self._instance().check()


def _found(connector: type, answered: object) -> tuple[ReadStream, ...]:
    """What `connector`'s `streams()` answered, the streams it reads.

    Raises:
        TypeError: it answered something other than a sequence of streams `stream(...)` made.
    """
    found: list[object] = (
        [] if isinstance(answered, str) else list(cast("Iterable[object]", answered))
    )
    if isinstance(answered, str) or not all(isinstance(made, ReadStream) for made in found):
        message = (
            f"{connector.__name__}.streams() answers a sequence of the streams stream(...) makes"
        )
        raise TypeError(message)
    return tuple(cast("list[ReadStream]", found))


def _native_stream(
    made: ReadStream, reached: Callable[[], ReadStream], instance: Callable[[], Any] | None
) -> Stream:
    """`made`, reached through `reached`, as the native source reads it.

    Where `instance` answers the instance of a class whose method it is, what it holds is called
    with that instance first, and a method's name is that instance's method; otherwise what it
    holds is called as it is.

    Raises:
        TypeError: a stream called as it is names a method: it has no instance to find it on.
    """
    shown = str(made.spec.name)
    if instance is None:
        for named in ("partitions", "plan", "committed", "acknowledged"):
            if isinstance(getattr(made, named), str):
                message = (
                    f"stream {shown!r}: {named}= names a method, and a stream called as it is "
                    "has no instance to find it on: give it the method itself"
                )
                raise TypeError(message)

    def bound(of: object) -> Callable[..., Any]:
        if instance is None:
            return cast("Callable[..., Any]", of)
        target = instance()
        if isinstance(of, str):
            return getattr(target, of)
        return partial(cast("Callable[..., Any]", of), target)

    def read(*arguments: object, **keywords: object) -> Iterable[object] | AsyncIterable[object]:
        return bound(reached().function)(*arguments, **keywords)

    whole = made.plan is not None

    def planned(state: StreamState) -> PartitionPlan:
        held = reached()
        answered = _given_state(bound(held.plan if whole else held.partitions), state)
        return as_plan(shown, answered, whole=whole)

    def committed(cursors: Committed) -> None:
        bound(reached().committed)(cursors)

    def acknowledged(partition: str) -> Cursor | None:
        return bound(reached().acknowledged)(partition)

    planner = made.plan if whole else made.partitions
    return Stream(
        made.spec,
        read,
        made.cursor,
        None if planner is None else planned,
        stated=planner is not None and _names_state(bound(planner)),
        committed=None if made.committed is None else committed,
        acknowledged=None if made.acknowledged is None else acknowledged,
        cursor_version=made.cursor_version,
        takes_context=made.takes_context,
    )


def placed(connector: type) -> tuple[_native.SourceFactory, Placement[Declared]]:
    """The native source a class `@source` made, and the placement that keeps its instances.

    The native factory reaches the placement through a weak reference: what may still connect
    holds it in the open, as the host or the pipeline builder that placed the class does.
    """
    declared = declaration(connector)
    placement = Placement(declared)
    reached = weakly_reached(placement, f"connector {declared.id!r}")

    def connect(text: str, host: str | None) -> _Connected:
        found = reached()
        instance = built(found.declared.connector, validated(found.declared.config, text), host)
        connected = _Connected(reached)
        found.connections[connected] = instance
        return connected

    made = _native.SourceFactory(
        declared.id,
        declared.version or package_version(connector.__module__.split(".")[0]),
        json.dumps(declared.config.model_json_schema()),
        declared.acknowledged,
        connect,
    )
    return made, placement


def _given_state(answering: Callable[..., object], state: StreamState) -> object:
    """What `answering` answers, given `state` where it names a `state` parameter."""
    named = inspect.signature(answering).parameters.get("state")
    if named is None:
        return answering()
    if named.kind is inspect.Parameter.POSITIONAL_ONLY:
        return answering(state)
    return answering(state=state)


def _names_state(answering: Callable[..., object]) -> bool:
    """Whether `answering` names a `state` parameter, so it is told the stream's state."""
    return "state" in inspect.signature(answering).parameters


class _Functions:
    """The in-process source of a pipeline's stream functions, which has nothing to check."""

    discovers = False
    """Its catalog is its streams' specs."""

    def __init__(self, streams: list[Stream]) -> None:
        self.streams = streams

    def check(self) -> None:
        """A source of functions reads only what its functions yield."""


def functions_placed(streams: Sequence[ReadStream]) -> _native.SourceFactory:
    """The in-process source whose streams are `streams`, functions `@stream` made.

    The native source reaches each through a weak reference: what may still read it holds it in
    the open, as the pipeline builder does.
    """
    connected = _Functions(
        [
            _native_stream(made, weakly_reached(made, f"stream {str(made.spec.name)!r}"), None)
            for made in streams
        ]
    )
    return _native.SourceFactory(
        FUNCTIONS,
        _native.__version__,
        json.dumps({"type": "object"}),
        False,  # noqa: FBT003 - the native factory's flag: a source of functions tells nothing
        lambda _config, _host: connected,
    )
