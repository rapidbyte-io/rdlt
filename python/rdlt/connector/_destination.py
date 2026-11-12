# pyright: reportPrivateUsage=false
"""`@destination`: a class's sessions and table writers as the Rust SDK's destination."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
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

    A session's calls, its table writers' included, reach the class one at a time. A call the
    engine stopped waiting for still runs on its thread, the next call of its session, `close()`
    included, waits for it, and a newer session may open meanwhile: the fence, not the call's
    end, keeps its commit from landing.
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


class _Writer:
    """An author's table writer, as the native side calls it, one call of a session's at a time."""

    def __init__(
        self,
        writer: Any,  # noqa: ANN401 - the author's table writer
        calls: threading.Lock,
    ) -> None:
        self._writer, self._calls = writer, calls

    def write(self, segment: int, batch: object) -> None:
        """Stages the native batch `batch`, read as pyarrow's, sharing its buffers."""
        import pyarrow as pa  # noqa: PLC0415

        written = pa.record_batch(cast("Any", batch))
        with self._calls:
            self._writer.write(segment, written)

    def flush(self) -> tuple[int, int]:
        """The rows and bytes the flush staged."""
        with self._calls:
            stats = self._writer.flush()
        if not isinstance(stats, WriteStats):
            message = (
                f"flush() answered {type(stats).__name__}: it answers rdlt.connector.WriteStats"
            )
            raise TypeError(message)
        return stats.rows, stats.bytes


class _Session:
    """An author's session, as the native side calls it: arguments and answers as the SDK's JSON.

    Its calls and its table writers' reach the author one at a time, as the Rust SDK orders a
    session's: a call the engine stopped waiting for still runs, and the next waits for it.
    """

    def __init__(self, session: Any) -> None:  # noqa: ANN401 - the author's session
        self._session = session
        self._calls = threading.Lock()

    def apply_schema(self, change: str) -> None:
        """Applies the schema change `change`."""
        parsed = _parse(change)
        with self._calls:
            self._session.apply_schema(parsed)

    def writer(self, table: str) -> _Writer:
        """A table writer of `table`."""
        parsed = _parse(table)
        with self._calls:
            return _Writer(self._session.writer(parsed), self._calls)

    def discard_staged(self) -> None:
        """Removes what older sessions staged."""
        with self._calls:
            self._session.discard_staged()

    def commit(self, meta: str) -> str:
        """Commits `meta`, answering its receipt."""
        parsed = _parse(meta)
        with self._calls:
            receipt = self._session.commit(parsed)
        return json.dumps(dict(receipt))

    def close(self) -> None:
        """Ends the session, once its calls in flight returned."""
        with self._calls:
            self._session.close()


class _Connected:
    """One connection of a class's destination, as the native side holds it.

    It holds nothing of the author's itself: its placement keeps the instance, under this object
    as a weak key, where the collector sees it, and letting go of this object releases it.
    """

    def __init__(self, placement: Callable[[], Placement[_Declared]]) -> None:
        self._placement = placement

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
        return _Session(opened.session), json.dumps(told)

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
