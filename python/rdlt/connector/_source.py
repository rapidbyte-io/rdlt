"""`@source` and `@stream`: functions and a class's methods as streams the Rust SDK reads."""

from __future__ import annotations

import inspect
import types
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from rdlt.connector._config import Config
from rdlt.connector._context import Partition, PartitionPlan, StreamState
from rdlt.connector._cursor import Cursor
from rdlt.connector._declared import declare, declared_as
from rdlt.connector._spec import Checkpointing, ReadWord, StreamSpec
from rdlt.connector._streams import Committed
from rdlt.model import ReadMode, StreamName

T = TypeVar("T")

_CURSOR_VERSIONS = range(2**16)
"""The formats a cursor may be in, as the Rust SDK's `u16` holds them."""

_Answered = Sequence[str | Partition]
"""A stream's partitions, each an id or a `Partition`."""

_PartitionsOf = (
    Callable[[Any], _Answered]
    | Callable[[], _Answered]
    | Callable[[Any, StreamState], _Answered]
    | Callable[[StreamState], _Answered]
    | str
)
"""A partitioned stream's partitions: a function called as its stream is, given the stream's
`StreamState` as `state` where it names one, or a method's name."""

_PlanOf = (
    Callable[[Any], PartitionPlan]
    | Callable[[], PartitionPlan]
    | Callable[[Any, StreamState], PartitionPlan]
    | Callable[[StreamState], PartitionPlan]
    | str
)
"""A stream's plan: a function called as its stream is, given the stream's `StreamState` as
`state` where it names one, or a method's name."""

_CommittedBy = Callable[[Any, Committed], None] | Callable[[Committed], None] | str
"""What a stream calls with its committed cursors: a function called as its stream is, or a
method's name."""

_AcknowledgedBy = Callable[[Any, str], Cursor | None] | Callable[[str], Cursor | None] | str
"""What says where a stream stands for a partition outside the engine: a function called as its
stream is, or a method's name."""


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ReadStream:
    """One stream of a source, which `stream` makes of a function, a method or any callable.

    It stays what it was: a function's stream is called as the function, and a method's is the
    method, bound to its instance. `spec` is what it says of itself, as a Rust stream's `spec()`
    answers its `StreamSpec`.
    """

    function: Callable[..., Any]
    spec: StreamSpec
    cursor: type[Cursor] | None
    cursor_version: int
    partitions: _PartitionsOf | None
    plan: _PlanOf | None
    committed: _CommittedBy | None
    acknowledged: _AcknowledgedBy | None
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
    name: str | StreamName | None = None,
    *,
    primary_key: str | Sequence[str] | None = None,
    cursor: type[Cursor] | None = None,
    cursor_version: int = 1,
    schema: object = None,
    read_modes: Sequence[ReadMode | ReadWord] | None = None,
    change_time: str | None = None,
    partitions: _PartitionsOf | None = None,
    plan: _PlanOf | None = None,
    committed: _CommittedBy | None = None,
    acknowledged: _AcknowledgedBy | None = None,
    replayable: bool = True,
) -> Callable[[Callable[..., Any]], Any]:
    """Makes a function a stream a pipeline reads as its source, or a method a stream of its class.

    It takes the cursor where one is declared, then `ctx` where it names it, and yields rows, Arrow
    data and `checkpoint(cursor)`, or, read as changes, `changes(batch)`. Called at run time, on a
    bound method, a closure or a `partial`, it makes a stream a source's `streams()` answers.
    `name`, the function's name unless given, is a name or a `rdlt.StreamName` within a namespace,
    such as a database schema. `partitions` answers the partitions, each an id or a `Partition`,
    called as the stream is, with the instance where it is a method, or names a method that does;
    `ctx.partition` then says which one is read. Where it names a `state` parameter, it is given the
    stream's committed `StreamState`, as a Rust stream's `partitions` is: its phase, and where each
    partition stands, a cursor or `PartitionState.DONE`. A method stays a method:
    `self.tickets(since, ctx)` calls it. It makes a `ReadStream`, typed `Any` so it is called and
    overridden as the function or method it was. How its rows land is the pipeline's to say, as
    `rdlt.stream(name, write=...)`, whatever its source. `cursor_version`, 1 unless given, is the
    format of its cursors, as a Rust stream's `CURSOR_VERSION` is: bumped when `cursor`'s shape
    changes incompatibly, so a cursor committed in another format fails the read, coded
    `cursor_version`, rather than restart it.

    Its spec is a Rust stream's `StreamSpec`. `schema`, any Arrow schema, anything with
    `__arrow_c_schema__`, is its declared schema, as `with_schema` declares it, refused here where
    Rust cannot take it. `read_modes` are how it can be read, `ReadMode`s or their names, as
    `with_read_modes` replaces them: full, and incremental where it has a cursor, unless given; a
    stream without a cursor is read only in full. `change_time` names the top-level column
    holding each change's source commit time, as `with_change_time` does. It checkpoints on demand
    where it names `ctx`, and at its own points otherwise.

    `plan`, called as `partitions` is or a method's name, answers a whole `PartitionPlan`, as a
    Rust stream's `plan` overrides its `partitions`, so a stream names one of the two. A stream
    read in phases, as a CDC snapshot and then its changes, plans so: once every partition of a
    phase is done, the next plan names the next phase, its partitions and where they start. A phase
    that ends, as a snapshot does, plans partitions that end; one that never ends is never done.
    Where a phase's partitions start comes from `state` alone, never from what the source was
    told, which trails what landed after a crash before the telling. Rust checks each plan: one
    naming a partition twice, more than 16384 of them, or starting one it does not name fails,
    coded `plan_invalid` in process, and served, where the host refuses it as it reads it,
    `invalid_message`, as a Rust source's does.

    `committed`, a function called as `partitions` is or a method's name, takes the cursors each
    commit made durable, as `(partition id, cursor)` pairs, as a Rust stream's `committed` does: a
    source that acknowledges upstream, as a queue settling its messages, does it there. A cursor
    may be told more than once, and one older than the last, so an acknowledgement never moves
    back for it. `acknowledged`, called as `committed` is or a method's name, takes a partition's
    id and answers where the stream stands for it outside the engine, as a Rust stream's
    `acknowledged` does: the cursor it was last told is committed, as a consumer group's committed
    offset keeps it beyond a connection, or `None` where it keeps none or was never told. Only
    certification asks, of a source `@source(acknowledged=True)` makes, from a connection of its
    own, and it answers the cursor `committed` was told as soon as that call returns.
    `replayable=False` declares a stream that cannot be read again from any committed cursor, as
    the Rust SDK's `StreamSpec::with_replayable` does.

    A stream without `partitions` reads `Partition.single()`, which never ends, as a Rust source's
    does, and so does a `Partition(id, unbounded=True)` that `partitions` answers. Read `full`,
    everything such a read yields commits, and an incremental read after it resumes from its last
    `checkpoint` only where it ended on one, and fails otherwise, coded `cursor_missing`, as no
    cursor follows its last rows. Read incrementally, it commits only what it yields before its
    last checkpoint, and the next run reads it again from there, so it ends on a `checkpoint` to
    commit its last rows; one that yields rows and no checkpoint after any fails the read, coded
    `checkpoint_missing`, since nothing it read could land. Any other partition `partitions`
    answers ends: a read that yields rows after its last checkpoint commits them and finishes the
    partition, which a later incremental run does not read again, and an incremental read that ends
    on a `checkpoint` leaves the partition there, for the next to read on from. Each full run reads
    every partition whole, so a stream without a cursor, read only in full, is read whole every
    run.

    Raises:
        TypeError: it names `plan` and `partitions`, either without a `ctx` parameter, says where
            it stands with no cursor, reads incrementally or as changes with none, has no name, or
            `schema` is no Arrow schema.
        ValueError: `cursor_version` is no `u16`, a read mode is none of `ReadMode`'s, or
            `schema` is one Rust cannot take.
    """

    def made(function: Callable[..., Any]) -> ReadStream:
        named = _named(name, function)
        shown = str(named)
        takes_context = "ctx" in inspect.signature(function).parameters
        if plan is not None and partitions is not None:
            message = (
                f"stream {shown!r}: plan= and partitions= both say what it reads, and plan= says "
                "it whole: a stream names one"
            )
            raise TypeError(message)
        planner = "plan" if plan is not None else "partitions" if partitions is not None else None
        if planner is not None and not takes_context:
            message = f"stream {shown!r}: {planner}= needs a ctx parameter"
            raise TypeError(message)
        if type(cursor_version) is not int or cursor_version not in _CURSOR_VERSIONS:
            message = (
                f"stream {shown!r}: cursor_version={cursor_version!r} is not a format from 0 "
                f"to {_CURSOR_VERSIONS[-1]}"
            )
            raise ValueError(message)
        if acknowledged is not None and cursor is None:
            message = (
                f"stream {shown!r}: acknowledged= answers a cursor, and the stream declares none"
            )
            raise TypeError(message)
        default = (ReadMode.FULL, ReadMode.INCREMENTAL) if cursor else (ReadMode.FULL,)
        spec = StreamSpec(
            named,
            schema=schema,
            primary_key=primary_key,
            read_modes=default if read_modes is None else read_modes,
            checkpointing=Checkpointing.ON_DEMAND if takes_context else Checkpointing.NATURAL,
            replayable=replayable,
            change_time=change_time,
        )
        cursored = [mode for mode in spec.read_modes if mode is not ReadMode.FULL]
        if cursored and cursor is None:
            message = (
                f"stream {shown!r}: read_modes= names {ReadMode(cursored[0]).value}, which reads "
                "from a cursor, and the stream declares none"
            )
            raise TypeError(message)
        return ReadStream(
            function=function,
            spec=spec,
            cursor=cursor,
            cursor_version=cursor_version,
            partitions=partitions,
            plan=plan,
            committed=committed,
            acknowledged=acknowledged,
            takes_context=takes_context,
        )

    return made


def _named(name: str | StreamName | None, function: Callable[..., Any]) -> str | StreamName:
    """The stream's name: `name`, or the function's own.

    Raises:
        TypeError: neither is given, as for a `partial`.
    """
    if name is not None:
        return name
    own = getattr(function, "__name__", None)
    if not isinstance(own, str):
        message = f"stream() of a {type(function).__name__} needs its name: it has none of its own"
        raise TypeError(message)
    return own


@dataclass(frozen=True, slots=True)
class Declared:
    """What `@source` declared of a class, which the class holds."""

    connector: type
    id: str
    version: str | None
    config: type[Config]
    acknowledged: bool
    streams: tuple[ReadStream, ...]
    finds: bool
    """Whether its `streams()` answers its streams as it connects."""
    discovers: bool
    """Whether its `discover()` answers its catalog."""


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


def _answers(connector: type, method: str) -> bool:
    """Whether `connector` defines `method`, as a method.

    Raises:
        TypeError: it defines it, as something else, or as one that is async.
    """
    defined = getattr(connector, method, None)
    if defined is None:
        return False
    if not inspect.isfunction(defined):
        message = f"{connector.__name__}.{method} is no method: a source answers {method}()"
        raise TypeError(message)
    if inspect.iscoroutinefunction(defined) or inspect.isasyncgenfunction(defined):
        message = (
            f"{connector.__name__}.{method} is async: a source answers {method}() as it is "
            "called, as a Rust source's does"
        )
        raise TypeError(message)
    return True


def source(
    *,
    id: str,  # noqa: A002 - the connector's id, as documents name it
    config: type[Config],
    version: str | None = None,
    acknowledged: bool = False,
) -> Callable[[type[T]], type[T]]:
    """Makes a class a source connector: built from its validated `config`, checked with `check()`.

    Its `__init__` takes the configuration, and a `ConnectContext` as `context` where it names
    one, as a Rust source's `connect(config, &ConnectContext)` does. `acknowledged=True` says its
    streams tell where they stand outside the engine, with their `acknowledged`, as the Rust SDK's
    `#[source(..., acknowledged)]` does: certification's `S-ACK` then checks that a stream's
    position moves only in `committed`, and tells it a few checkpoints are committed, so it runs
    against a slot or consumer group of its own.

    Its streams are its `streams()`'s answer, as a Rust source's `streams()` is: the streams this
    connection offers, each made by calling `stream(...)` at run time, as a database source offers
    one a table it finds. It is asked once a connection, as the connection is made, so a table
    made later is read from the next connection. A stream it answers is called as it is, as a
    function's stream is: a bound method, a closure or a `partial` holds what it calls. Without
    `streams()`, each `@stream` method of the class and of its bases is one of its streams; a
    subclass's own attribute of a stream's name replaces that stream. `discover()` answers its
    catalog, a `StreamSpec` for each stream, as a Rust source's `discover()` does: its streams'
    specs unless it has one. Rust checks either answer: two streams of one name, or more than
    65536, fail. A subclass is a source of its own only once `@source` makes it one.

    Raises:
        TypeError: the class has no `check`, `@destination` made it, its `streams` or `discover`
            is no method, or it has `streams()` and `@stream` methods, two answers to one question.
    """

    def made(connector: type[T]) -> type[T]:
        if not callable(getattr(connector, "check", None)):
            message = f"{connector.__name__} has no check(): a source checks it can read"
            raise TypeError(message)
        declared_streams = _streams_of(connector)
        finds = _answers(connector, "streams")
        if finds and declared_streams:
            methods = ", ".join(str(method.spec.name) for method in declared_streams)
            message = (
                f"{connector.__name__} answers its streams with streams(), so its @stream methods "
                f"({methods}) would go unread: streams() answers them too"
            )
            raise TypeError(message)
        declared = Declared(
            connector,
            id,
            version,
            config,
            acknowledged,
            declared_streams,
            finds=finds,
            discovers=_answers(connector, "discover"),
        )
        declare(connector, "source", declared)
        return connector

    return made


def declares(connector: type) -> bool:
    """Whether `@source` made `connector` a source."""
    return declared_as(connector, "source") is not None


def declaration(connector: type) -> Declared:
    """What `@source` declared of `connector`."""
    declared = declared_as(connector, "source")
    if declared is None:
        message = f"{connector.__name__} is no source: decorate it with @source"
        raise TypeError(message)
    return cast("Declared", declared)
