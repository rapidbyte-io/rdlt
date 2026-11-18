# pyright: reportPrivateUsage=false
"""`@destination`: a class's sessions and table writers as the Rust SDK's destination."""

from __future__ import annotations

import contextlib
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar, cast

from rdlt import _native
from rdlt._answers import answered, nothing
from rdlt._weak import weakly_reached
from rdlt.connector._capabilities import Capabilities
from rdlt.connector._commit import Receipt
from rdlt.connector._config import Config, validated
from rdlt.connector._declared import Placement, built, declare, declared_as, package_version
from rdlt.connector._session import NativeSession, Session
from rdlt.model import Json, _parse


@dataclass(frozen=True, slots=True)
class Opened:
    """What a destination's `open` answers: its `Session`, and what the open found.

    `epoch` is the epoch the open set, above every earlier open's of the pipeline, kept where
    a later open finds it; `landed` the receipt of the last commit that landed, as the session's
    `commit` answered it; `state` the pipeline's committed state records, each
    `{"key": ..., "value": ...}`, its value the JSON text a commit gave it.
    """

    session: Session
    epoch: int
    landed: Receipt | None
    state: Sequence[Mapping[str, Json]]


@dataclass(frozen=True, slots=True)
class OpenContext:
    """Who is opening a destination's session: the pipeline whose state it reads and writes."""

    pipeline: str


class DestinationConnector(Protocol):
    """A destination connector's class, as `@destination` takes it and the Rust SDK names it.

    Its instance answers `check()` and `open(context)` as typed here, and, where it has them,
    `capabilities()` a `Capabilities` and `published(table)` the `pyarrow.RecordBatch`es a table
    published.
    """

    def check(self) -> None:
        """Verifies the destination can be opened, raising where it cannot."""
        ...

    def open(self, context: OpenContext, /) -> Opened:
        """Opens a session for `context`'s pipeline at its next epoch."""
        ...


T = TypeVar("T", bound=DestinationConnector)


@dataclass(frozen=True, slots=True)
class _Declared:
    """What `@destination` declared of a class, which the class holds."""

    connector: type
    id: str
    version: str | None
    config: type[Config]


def destination(
    *,
    id: str,  # noqa: A002 - the connector's id, as documents name it
    config: type[Config],
    version: str | None = None,
) -> Callable[[type[T]], type[T]]:
    """Makes a class a destination connector, which the Rust SDK drives.

    The class is built from its validated `config`, and a `ConnectContext` as `context` where its
    `__init__` names one, asked what it can store with `capabilities()`, checked with `check()`,
    which answers `None` and raises where the destination cannot be opened, and opened once a
    load with `open(context)`, an `OpenContext`, answering `Opened`; for certification,
    `published(table)` yields the `pyarrow.RecordBatch`es a table published. `Opened.session` is
    a `Session`, whose `writer(table)` answers a `TableWriter`: their annotations say what each
    method answers. A commit is a `CommitMeta`, its segment sets `SegmentSet`s, and answers a
    `Receipt`; changes and tables are the Rust SDK's types as mappings, as their JSON is.

    Each method is a plain function, which rdlt calls on a thread of its own, where it may block.
    One that answers anything else than it states fails the call, as an internal error naming it:
    a coroutine, as a method written `async def` answers, a generator, as one with a `yield`
    does, or `False` where it answers `None`. Where the `Opened` an open answers cannot be read,
    its session is closed. A session whose open the engine stopped waiting for is let go, not
    closed, as a Rust session is dropped: its finalizers release what it holds.

    `capabilities()` answers a `Capabilities`, as `Capabilities.minimal()` starts it, and is
    `Capabilities.minimal()` where the class has none, as a Rust destination's `capabilities`
    answers what this instance can store, which may depend on its configuration or on what it
    connected to. It is asked once, as the instance is built, and capabilities rdlt cannot read
    fail it there. A class that has no `check` or `open`, whose `capabilities` is no method, or
    that `@source` made, is refused here.

    What the destination keeps, the Rust SDK's `Session` and `TableWriter` say whole:

    - Each open raises the pipeline's epoch above every earlier open's, durably, and answers the
      receipt its fence row keeps and the committed state.
    - What a table writer stages is kept by its session's epoch, invisible until committed;
      `discard_staged()` removes what older epochs staged. What a writer stages once a newer
      session opened is never published: its flush raises `Fenced`, or keeps it apart.
    - `commit(meta)` decides with `Fence(epoch, landed).decide(meta)`, from the fence row it reads
      under its transaction's write lock. Where it applies, it publishes what its epoch staged in
      a segment of `meta["segments"]`, as `segment in meta["segments"]` asks, removes what was
      staged in `meta["abandoned"]`, applies `meta["state_delta"]` and writes its receipt into
      the fence row, all in one transaction. The receipt is `{"key": meta["key"],
      "committed_at": {"secs_since_epoch": s, "nanos_since_epoch": n}, "rows": rows, "bytes":
      size}`, the rows and bytes it published.
    - A table belongs to the pipeline that created it: another's schema change or writer raises
      `InvalidConfig(..., code=TABLE_OWNED)`, and a column declared at another type than the
      table holds raises `InvalidData(..., code=SCHEMA_CONFLICT)`.

    A session's calls reach the class one at a time, and so do its table writers' where the
    destination runs one writer, as `Capabilities.minimal()` does, so such a destination needs no
    lock of its own. One whose `capabilities()` says it runs more says its table writers may be
    called at once: each writer's calls come one at a time, beside the other writers' and the
    session's `apply_schema`, `writer` and `discard_staged`, so what writers share is the author's
    to lock. Each call takes its turn where the Rust SDK makes it, so calls made one after another
    come in that order and calls made at once in either: `commit` and `close` begin once every
    call made before them has returned, and no call begins until they return. A call the engine
    stopped waiting for still runs on its thread and keeps its turn until it returns; one it
    stopped waiting for before its turn came is never made, so such a `commit` lands nothing and
    a session whose `close` was never made is let go, as one whose open was. A newer session may
    open meanwhile: what an older session's abandoned calls stage is the destination's to keep
    from a commit, by its fence and by publishing only its session's own epoch.
    """

    def made(connector: type[T]) -> type[T]:
        for method in ("check", "open"):
            if not callable(getattr(connector, method, None)):
                message = f"{connector.__name__} has no {method}(): a destination needs it"
                raise TypeError(message)
        said = getattr(connector, "capabilities", None)
        if said is not None and not callable(said):
            message = (
                f"{connector.__name__}.capabilities is no method: it is capabilities(self), "
                "answering a Capabilities"
            )
            raise TypeError(message)
        declare(connector, "destination", _Declared(connector, id, version, config))
        return connector

    return made


def declares(connector: type) -> bool:
    """Whether `@destination` made `connector` a destination."""
    return declared_as(connector, "destination") is not None


def declaration(connector: type) -> _Declared:
    """What `@destination` declared of `connector`."""
    declared = declared_as(connector, "destination")
    if declared is None:
        message = f"{connector.__name__} is no destination: decorate it with @destination"
        raise TypeError(message)
    return cast("_Declared", declared)


class _Connected:
    """One connection of a class's destination, as the native side holds it.

    It holds nothing of the author's itself: its placement keeps the instance, under this object
    as a weak key, where the collector sees it, and letting go of this object releases it.
    """

    def __init__(self, placement: Callable[[], Placement[_Declared]]) -> None:
        self._placement = placement

    def _instance(self) -> Any:  # noqa: ANN401 - the author's class
        return self._placement().connections[self]

    def _called(self, method: str) -> str:
        """The instance's `method`, as a failure names it."""
        return f"{self._placement().declared.connector.__name__}.{method}()"

    def capabilities(self) -> str:
        """What the instance says it can store, as the Rust SDK's `Capabilities` JSON.

        Raises:
            TypeError: it answered what is no `Capabilities`.
        """
        said = getattr(self._instance(), "capabilities", None)
        found = (
            Capabilities.minimal()
            if said is None
            else answered(
                self._called("capabilities"), said(), Capabilities, "rdlt.connector.Capabilities"
            )
        )
        return found._json()

    def check(self) -> None:
        """Verifies the destination can be opened."""
        nothing(self._called("check"), self._instance().check())

    def open(self, pipeline: str) -> tuple[NativeSession, str]:
        """Opens a session for `pipeline`, with what the open answers beside it as JSON.

        Raises:
            TypeError: it answered no `Opened`, or one rdlt cannot read, whose session it closed.
        """
        called = self._called("open")
        opened = answered(
            called, self._instance().open(OpenContext(pipeline)), Opened, "rdlt.connector.Opened"
        )
        try:
            told = _told(opened)
        except Exception as unreadable:
            _close_after(opened.session)
            message = f"{called} answered an Opened rdlt cannot read: {unreadable}"
            raise TypeError(message) from unreadable
        return NativeSession(opened.session), told

    def published(self, table: str) -> list[list[object]]:
        """Each batch `table` published, exported as the C Data Interface's capsules."""
        batches = cast(
            "Iterable[object]",
            answered(
                self._called("published"),
                self._instance().published(_parse(table)),
                Iterable,
                "the batches the table published",
            ),
        )
        exported: list[list[object]] = []
        for batch in batches:
            export = getattr(batch, "__arrow_c_array__", None)
            if export is None:
                message = (
                    f"{self._called('published')} yielded {type(batch).__name__}: it yields "
                    "batches, anything with __arrow_c_array__"
                )
                raise TypeError(message)
            exported.append(list(cast("Callable[[], Iterable[object]]", export)()))
        return exported


def _told(opened: Opened) -> str:
    """What `opened` says beside its session, as the native side reads it.

    Raises:
        TypeError: its session is no `Session`, or what it says is no JSON.
        ValueError: its receipt or a state record is no mapping.
    """
    if not isinstance(cast("object", opened.session), Session):
        message = "its session has no apply_schema, writer, discard_staged, commit or close"
        raise TypeError(message)
    landed = None if opened.landed is None else dict(opened.landed)
    state = [dict(record) for record in opened.state]
    return json.dumps({"epoch": opened.epoch, "landed": landed, "state": state})


def _close_after(session: object) -> None:
    """Closes `session`, whose open failed after it was made, so it lets go of what it holds.

    The open's failure matters more than any of closing, which is let go.
    """
    close = getattr(session, "close", None)
    if callable(close):
        with contextlib.suppress(Exception):
            nothing("close()", close())


def placed(connector: type) -> tuple[_native.DestinationFactory, Placement[_Declared]]:
    """The native destination a class `@destination` made, and the placement keeping its instances.

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

    made = _native.DestinationFactory(
        declared.id,
        declared.version or package_version(connector.__module__.split(".")[0]),
        json.dumps(declared.config.model_json_schema()),
        callable(getattr(connector, "published", None)),
        connect,
    )
    return made, placement
