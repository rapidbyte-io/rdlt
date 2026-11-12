"""An append-only destination kept in this process's memory, written as an author writes one.

The SDK's tests load into it and certify it. Each store keeps its pipelines' fence rows and
state, its tables with their owners and columns, and what sessions staged, under one lock: a
commit is atomic under it.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from threading import Lock
from typing import cast

import pyarrow as pa

from rdlt.connector import (
    SCHEMA_CONFLICT,
    TABLE_OWNED,
    Config,
    Fence,
    Fenced,
    InvalidConfig,
    InvalidData,
    OpenContext,
    Opened,
    WriteStats,
    destination,
)
from rdlt.model import Json

_Key = tuple[str, int, int]
"""Where a batch is staged: the pipeline and epoch of the session staging it, and its segment."""


def _object(value: Json) -> dict[str, Json]:
    return cast("dict[str, Json]", value)


def _list(value: Json) -> list[Json]:
    return cast("list[Json]", value)


@dataclass
class _Pipeline:
    """A pipeline's fence row and state: its newest epoch, its last receipt, its records."""

    epoch: int = 0
    landed: dict[str, Json] | None = None
    state: dict[str, dict[str, Json]] = field(default_factory=dict[str, dict[str, Json]])


@dataclass
class _Table:
    """A table: the pipeline that created it, its columns' types by name, its published rows."""

    owner: str
    columns: dict[str, Json] = field(default_factory=dict[str, Json])
    published: list[pa.RecordBatch] = field(default_factory=list[pa.RecordBatch])


@dataclass
class _Store:
    lock: Lock = field(default_factory=Lock)
    pipelines: dict[str, _Pipeline] = field(default_factory=dict[str, _Pipeline])
    tables: dict[str, _Table] = field(default_factory=dict[str, _Table])
    staged: dict[_Key, list[tuple[str, pa.RecordBatch]]] = field(
        default_factory=dict[_Key, list[tuple[str, pa.RecordBatch]]]
    )


_STORES: dict[str, _Store] = {}


def published(store: str, table: str) -> int:
    """How many rows `store` published to `table`."""
    held = _STORES.get(store)
    if held is None or table not in held.tables:
        return 0
    return sum(batch.num_rows for batch in held.tables[table].published)


def _column(nodes: Json) -> tuple[str, Json]:
    """A column's name and type, from its field: the field's nodes, its root naming it."""
    root, *nested = _list(nodes)
    return str(_object(root)["name"]), [_object(root)["type"], *nested]


def _within(segment: int, ranges: Json) -> bool:
    """Whether `segment` is in `ranges`, a segment set: its runs of consecutive ids."""
    spans = cast("list[dict[str, int]]", ranges)
    return any(span["first"] <= segment <= span["last"] for span in spans)


class MemoryConfig(Config):
    """Which store a connection writes to; connections naming one store share it."""

    store: str


class _Session:
    def __init__(self, store: _Store, pipeline: str, epoch: int) -> None:
        self.store, self.pipeline, self.epoch = store, pipeline, epoch

    def _claimed(self, name: str) -> _Table:
        """The table `name`, created for this session's pipeline where it is new."""
        table = self.store.tables.setdefault(name, _Table(owner=self.pipeline))
        if table.owner != self.pipeline:
            message = f"table {name} belongs to pipeline {table.owner}"
            raise InvalidConfig(message, code=TABLE_OWNED)
        return table

    def apply_schema(self, change: Mapping[str, Json]) -> None:
        ((kind, detail),) = change.items()
        fields = _object(detail)
        name = str(_object(fields["table"])["name"])
        if kind == "create":
            columns = dict(_column(nodes) for nodes in _list(_object(fields["schema"])["fields"]))
        elif kind == "add_column":
            columns = dict([_column(fields["field"])])
        else:
            message = f"table {name} widens no column in place"
            raise InvalidData(message, code=SCHEMA_CONFLICT)
        with self.store.lock:
            table = self._claimed(name)
            for column, kept in columns.items():
                if table.columns.get(column, kept) != kept:
                    message = f"table {name} holds column {column} at another type"
                    raise InvalidData(message, code=SCHEMA_CONFLICT)
            table.columns.update(columns)

    def writer(self, table: Mapping[str, Json]) -> _Writer:
        name = str(table["name"])
        with self.store.lock:
            self._claimed(name)
        return _Writer(self, name)

    def discard_staged(self) -> None:
        with self.store.lock:
            for key in list(self.store.staged):
                if key[0] == self.pipeline and key[1] < self.epoch:
                    del self.store.staged[key]

    def commit(self, meta: Mapping[str, Json]) -> Mapping[str, Json]:
        with self.store.lock:
            fence = self.store.pipelines[self.pipeline]
            landed = Fence(fence.epoch, fence.landed).decide(meta)
            if landed is not None:
                return landed
            rows = size = 0
            for key in [key for key in self.store.staged if key[:2] == (self.pipeline, self.epoch)]:
                if _within(key[2], meta["segments"]):
                    for name, batch in self.store.staged.pop(key):
                        self.store.tables[name].published.append(batch)
                        rows, size = rows + batch.num_rows, size + batch.nbytes
                elif _within(key[2], meta["abandoned"]):
                    del self.store.staged[key]
            for change in _list(meta["state_delta"]):
                ((kind, detail),) = _object(change).items()
                if kind == "put":
                    record = _object(detail)
                    fence.state[str(record["key"])] = record
                else:
                    fence.state.pop(str(detail), None)
            now = time.time_ns()
            receipt: dict[str, Json] = {
                "key": meta["key"],
                "committed_at": {
                    "secs_since_epoch": now // 1_000_000_000,
                    "nanos_since_epoch": now % 1_000_000_000,
                },
                "rows": rows,
                "bytes": size,
            }
            fence.landed = receipt
            return receipt

    def close(self) -> None:
        """Nothing is held open."""


class _Writer:
    """Buffers a table's batches, and stages them on flush."""

    def __init__(self, session: _Session, table: str) -> None:
        self._session, self._table = session, table
        self._buffered: list[tuple[int, pa.RecordBatch]] = []

    def write(self, segment: int, batch: pa.RecordBatch) -> None:
        """Buffers `batch` for its segment."""
        self._buffered.append((segment, batch))

    def flush(self) -> WriteStats:
        """Stages what is buffered, unless a newer session opened."""
        session = self._session
        with session.store.lock:
            epoch = session.store.pipelines[session.pipeline].epoch
            if epoch != session.epoch:
                message = f"pipeline {session.pipeline} is at epoch {epoch}"
                raise Fenced(message)
            rows = size = 0
            for segment, batch in self._buffered:
                key = (session.pipeline, session.epoch, segment)
                session.store.staged.setdefault(key, []).append((self._table, batch))
                rows, size = rows + batch.num_rows, size + batch.nbytes
            self._buffered.clear()
        return WriteStats(rows, size)


@destination(
    id="io.example.memory",
    config=MemoryConfig,
    version="1.0.0",
)
class Memory:
    """Keeps what it loads in the store its configuration names."""

    def __init__(self, config: MemoryConfig) -> None:
        self.store = _STORES.setdefault(config.store, _Store())

    def check(self) -> None:
        """Memory is always there."""

    def open(self, context: OpenContext) -> Opened:
        """A session of the pipeline at its next epoch."""
        pipeline = context.pipeline
        with self.store.lock:
            fence = self.store.pipelines.setdefault(pipeline, _Pipeline())
            fence.epoch += 1
            session = _Session(self.store, pipeline, fence.epoch)
            return Opened(session, fence.epoch, fence.landed, list(fence.state.values()))

    def published(self, table: Mapping[str, Json]) -> Iterator[pa.RecordBatch]:
        """Every batch `table` published."""
        with self.store.lock:
            held = self.store.tables.get(str(table["name"]))
            batches = [] if held is None else list(held.published)
        yield from batches
