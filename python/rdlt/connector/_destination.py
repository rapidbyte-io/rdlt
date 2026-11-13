# pyright: reportPrivateUsage=false
"""`@destination`: a class's sessions and table writers as the Rust SDK's destination."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from rdlt import _native
from rdlt._weak import weakly_reached
from rdlt.connector._capabilities import Capabilities
from rdlt.connector._config import Config, validated
from rdlt.connector._declared import Placement, built, declare, declared_as, package_version
from rdlt.model import Json, _parse

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Opened:
    """What a destination's `open` answers, beside its session.

    `epoch` is the epoch the open set, above every earlier open's of the pipeline, kept where
    a later open finds it; `landed` the receipt of the last commit that landed, as the session's
    `commit` answered it; `state` the pipeline's committed state records, each
    `{"key": ..., "value": ...}`, its value the JSON text a commit gave it.
    """

    session: Any
    epoch: int
    landed: Mapping[str, Json] | None
    state: Sequence[Mapping[str, Json]]


@dataclass(frozen=True, slots=True)
class OpenContext:
    """Who is opening a destination's session: the pipeline whose state it reads and writes."""

    pipeline: str


@dataclass(frozen=True, slots=True)
class WriteStats:
    """What a table writer's `flush` staged: its rows, and its bytes as the destination counts."""

    rows: int
    bytes: int


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
    and opened once a load with `open(context)`, an `OpenContext`, answering `Opened`; for
    certification, `published(table)` yields the `pyarrow.RecordBatch`es a table published. A
    session has `apply_schema(change)`, `writer(table)`, `discard_staged()`, `commit(meta)`
    answering its receipt, and `close()`; a table writer has `write(segment, batch)`, taking a
    `pyarrow.RecordBatch`, and `flush()`, answering the `WriteStats` it staged. Changes, tables,
    commits and receipts are the Rust SDK's types as mappings, as their JSON is.

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
      `meta["segments"]`, removes what was staged in `meta["abandoned"]`, applies
      `meta["state_delta"]` and writes its receipt into the fence row, all in one transaction.
      The receipt is `{"key": meta["key"], "committed_at": {"secs_since_epoch": s,
      "nanos_since_epoch": n}, "rows": rows, "bytes": size}`, the rows and bytes it published.
    - A table belongs to the pipeline that created it: another's schema change or writer raises
      `InvalidConfig(..., code=TABLE_OWNED)`, and a column declared at another type than the
      table holds raises `InvalidData(..., code=SCHEMA_CONFLICT)`.

    A session's calls reach the class one at a time, and so do its table writers' where the
    destination runs one writer, as `Capabilities.minimal()` does, so such a destination needs no
    lock of its own. One whose `capabilities()` says it runs more says its table writers may be
    called at once: each writer's calls come one at a time, beside the other writers' and the
    session's `apply_schema`, `writer` and `discard_staged`, so what writers share is the author's
    to lock. No writer's call runs while `commit` or `close` runs, a call the engine stopped
    waiting for included, and none begins until they return. A call the engine stopped waiting
    for still runs on its thread, and is waited for as any call is. A newer session may open
    meanwhile: what an older session's abandoned calls stage is the destination's to keep from a
    commit, by its fence and by publishing only its session's own epoch.
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


class _Calls:
    """When a session's calls and its table writers' reach the author.

    Where the destination runs one writer, as it does until `capabilities()` answers more, each
    call waits for every other: one at a time. Where it runs more, the session's calls come one at
    a time, each writer's one at a time, and writers' calls beside each other and beside the
    session's `apply_schema`, `writer` and `discard_staged`. A writer's call counts as in flight
    from the moment it is made, a call the engine stopped waiting for and one queued behind it
    included: `commit` and `close` start once none is, and none begins until they return.
    """

    def __init__(self, writers: int) -> None:
        self._at_once = writers > 1
        self._session = threading.Lock()
        """Held across each session call, and each writer's where one writer runs."""
        self._idle = threading.Condition()
        """Guards `_in_flight` and `_settling`."""
        self._in_flight = 0
        """Writers' calls in flight, where writers run at once."""
        self._settling = False
        """Whether `commit` or `close` is waiting for writers' calls, or running."""

    @contextmanager
    def session(self, *, settled: bool = False) -> Generator[None]:
        """A session call; where `settled`, once no writer's call is in flight, and none begins."""
        with self._session:
            if not (settled and self._at_once):
                yield
                return
            try:
                with self._idle:
                    self._settling = True
                    self._idle.wait_for(lambda: self._in_flight == 0)
                yield
            finally:
                with self._idle:
                    self._settling = False
                    self._idle.notify_all()

    @contextmanager
    def writer(self, own: threading.Lock) -> Generator[None]:
        """A call of the writer whose calls `own` keeps one at a time, counted before it waits."""
        if not self._at_once:
            with self._session:
                yield
            return
        with self._idle:
            self._idle.wait_for(lambda: not self._settling)
            self._in_flight += 1
        try:
            with own:
                yield
        finally:
            with self._idle:
                self._in_flight -= 1
                self._idle.notify_all()


class _Writer:
    """An author's table writer, as the native side calls it, when its session's calls say."""

    def __init__(self, writer: Any, calls: _Calls) -> None:  # noqa: ANN401 - the author's writer
        self._writer, self._calls = writer, calls
        self._own = threading.Lock()
        """Held across each of this writer's calls where writers run at once."""

    def write(self, segment: int, batch: object) -> None:
        """Stages the native batch `batch`, read as pyarrow's, sharing its buffers."""
        import pyarrow as pa  # noqa: PLC0415

        with self._calls.writer(self._own):
            self._writer.write(segment, pa.record_batch(cast("Any", batch)))

    def flush(self) -> tuple[int, int]:
        """The rows and bytes the flush staged."""
        with self._calls.writer(self._own):
            stats = self._writer.flush()
        if not isinstance(stats, WriteStats):
            message = (
                f"flush() answered {type(stats).__name__}: it answers rdlt.connector.WriteStats"
            )
            raise TypeError(message)
        return stats.rows, stats.bytes


class _Session:
    """An author's session, as the native side calls it: arguments and answers as the SDK's JSON.

    Its calls and its table writers' reach the author as `_Calls` says: a call the engine stopped
    waiting for still runs, and is waited for as any call is.
    """

    def __init__(self, session: Any, writers: int) -> None:  # noqa: ANN401 - the author's session
        self._session = session
        self._calls = _Calls(writers)

    def apply_schema(self, change: str) -> None:
        """Applies the schema change `change`."""
        parsed = _parse(change)
        with self._calls.session():
            self._session.apply_schema(parsed)

    def writer(self, table: str) -> _Writer:
        """A table writer of `table`."""
        parsed = _parse(table)
        with self._calls.session():
            return _Writer(self._session.writer(parsed), self._calls)

    def discard_staged(self) -> None:
        """Removes what older sessions staged."""
        with self._calls.session():
            self._session.discard_staged()

    def commit(self, meta: str) -> str:
        """Commits `meta`, answering its receipt, once no writer's call is in flight."""
        parsed = _parse(meta)
        with self._calls.session(settled=True):
            receipt = self._session.commit(parsed)
        return json.dumps(dict(receipt))

    def close(self) -> None:
        """Ends the session, once its calls in flight returned."""
        with self._calls.session(settled=True):
            self._session.close()


class _Connected:
    """One connection of a class's destination, as the native side holds it.

    It holds nothing of the author's itself: its placement keeps the instance, under this object
    as a weak key, where the collector sees it, and letting go of this object releases it.
    """

    def __init__(self, placement: Callable[[], Placement[_Declared]]) -> None:
        self._placement = placement
        self._writers = 1
        """The table writers the instance said it runs at once: one until `capabilities()`
        answered, which the native side asks and awaits before it opens a session."""

    def _instance(self) -> Any:  # noqa: ANN401 - the author's class
        return self._placement().connections[self]

    def capabilities(self) -> str:
        """What the instance says it can store, as the Rust SDK's `Capabilities` JSON.

        Raises:
            TypeError: it answered what is no `Capabilities`.
        """
        instance = self._instance()
        said = getattr(instance, "capabilities", None)
        found = Capabilities.minimal() if said is None else said()
        if not isinstance(found, Capabilities):
            message = (
                f"capabilities() answered {type(found).__name__}: it answers "
                "rdlt.connector.Capabilities"
            )
            raise TypeError(message)
        self._writers = found.max_parallel_writers
        return found._json()

    def check(self) -> None:
        """Verifies the destination can be opened."""
        self._instance().check()

    def open(self, pipeline: str) -> tuple[_Session, str]:
        """Opens a session for `pipeline`, with what the open answers beside it as JSON."""
        opened = self._instance().open(OpenContext(pipeline))
        if not isinstance(opened, Opened):
            message = f"open() answered {type(opened).__name__}: it answers rdlt.connector.Opened"
            raise TypeError(message)
        landed = None if opened.landed is None else dict(opened.landed)
        state = [dict(record) for record in opened.state]
        told = {"epoch": opened.epoch, "landed": landed, "state": state}
        return _Session(opened.session, self._writers), json.dumps(told)

    def published(self, table: str) -> list[list[object]]:
        """Each batch `table` published, exported as the C Data Interface's capsules."""
        batches = cast("Iterable[object]", self._instance().published(_parse(table)))
        exported: list[list[object]] = []
        for batch in batches:
            export = getattr(batch, "__arrow_c_array__", None)
            if export is None:
                message = (
                    f"published() yielded {type(batch).__name__}: it yields batches, anything "
                    "with __arrow_c_array__"
                )
                raise TypeError(message)
            exported.append(list(cast("Callable[[], Iterable[object]]", export)()))
        return exported


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
