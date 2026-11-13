"""A destination that writes every way the engine has, in one sqlite3 file, as an author would.

It keeps what the Rust SDK's memory destination keeps, in the store `warehouse_store` holds, and
merges with `rdlt.connector.merge`, in Rust. Every call works in a transaction of its own
connection, so its table writers flush at once, and a commit is one transaction. Its store outlives
its process, so a host that kills it finds what it committed.

It widens no column in place: a widen joins two types on the engine's lattice, which a Python
destination does not hold, so a widen is refused and the engine routes what the column cannot
hold elsewhere.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Iterator, Mapping
from contextlib import closing
from typing import cast

import pyarrow as pa

from rdlt.connector import (
    SCHEMA_CONFLICT,
    Capabilities,
    Config,
    DeleteModes,
    Fence,
    Fenced,
    InvalidData,
    MergeKey,
    OpenContext,
    Opened,
    WriteModes,
    WriteStats,
    destination,
    merge,
    to_arrow,
)
from rdlt.model import Json
from tests.connectors.warehouse_store import (
    Store,
    connected,
    json_list,
    json_object,
    made,
    transaction,
)

_MERGE_KEY_INVALID = "merge_key_invalid"


def _changed(schema: dict[str, Json] | None, kind: str, change: dict[str, Json]) -> dict[str, Json]:
    """The schema of a table that has `schema` once the change `kind` applies.

    A column already there at its type changes nothing, and a column added is nullable.
    """
    name = str(json_object(change["table"])["name"])
    if kind == "create" and schema is None:
        return json_object(change["schema"])
    if schema is None:
        message = f"table {name} does not exist"
        raise InvalidData(message)
    if kind == "create":
        declared = json_list(json_object(change["schema"])["fields"])
    elif kind == "add_column":
        declared = [change["field"]]
    else:
        message = f"table {name} widens no column in place"
        raise InvalidData(message, code=SCHEMA_CONFLICT)
    fields = list(json_list(schema["fields"]))
    for field in declared:
        root, *nested = json_list(field)
        column = json_object(root)["name"]
        held = next((kept for kept in fields if _column(kept)[0] == column), None)
        if held is None:
            fields.append([{**json_object(root), "nullable": True}, *nested])
        elif _column(held)[1] != _column(field)[1]:
            message = f"table {name} already has column {column} at another type"
            raise InvalidData(message, code=SCHEMA_CONFLICT)
    return {"fields": fields}


def _column(field: Json) -> tuple[str, list[Json]]:
    """A field's name and its type: the field's nodes, its root naming it."""
    root, *nested = json_list(field)
    return str(json_object(root)["name"]), [json_object(root)["type"], *nested]


def _holds_key(name: str, key: MergeKey, schema: dict[str, Json] | None) -> None:
    """Refuses `key` unless it names a key column, and only columns the table has where known.

    Of a child table's key columns, the first alone is its rows' own: its root's id.
    """
    if not key["columns"]:
        message = f"table {name} is merged by a key of no column"
        raise InvalidData(message, code=_MERGE_KEY_INVALID)
    if schema is None:
        return
    keys = key["columns"][:1] if key["root"] is not None else key["columns"]
    deletion = None if key["changes"] is None else key["changes"]["deletion"]
    soft = [] if not isinstance(deletion, Mapping) else [str(deletion["Soft"]["at"])]
    history = [] if key["history"] is None else list(key["history"].values())
    held = {_column(field)[0] for field in json_list(schema["fields"])}
    for column in [*keys, key["seq"], *soft, *history]:
        if column not in held:
            message = f"table {name} has no column {column}, which its merge key names"
            raise InvalidData(message, code=_MERGE_KEY_INVALID)


class WarehouseConfig(Config):
    """The file the store is kept in."""

    path: str


class _Writer:
    """Buffers a table's batches, and stages them on flush unless a newer session opened."""

    def __init__(self, session: _Session, table: Mapping[str, Json]) -> None:
        self._session, self._name = session, str(table["name"])
        generation = table["generation"]
        self._generation = None if generation is None else str(generation)
        self._buffered: list[tuple[int, pa.RecordBatch]] = []

    def write(self, segment: int, batch: pa.RecordBatch) -> None:
        """Buffers `batch` for its segment."""
        self._buffered.append((segment, batch))

    def flush(self) -> WriteStats:
        """Stages what is buffered under the session's epoch, every batch a merge table admits."""
        session = self._session
        with transaction(session.path) as store:
            current = store.epoch(session.pipeline)
            if current != session.epoch:
                message = (
                    f"pipeline {session.pipeline} is at epoch {current}; this table writer's "
                    f"session opened at {session.epoch}"
                )
                raise Fenced(message)
            table = store.owned(session.pipeline, self._name)
            if table.merge is not None:
                stored = None if table.schema is None else to_arrow(table.schema)
                for _, batch in self._buffered:
                    merge.admitted(batch, stored, table.merge)
            rows = size = 0
            for segment, batch in self._buffered:
                row = (session.pipeline, session.epoch, segment, self._name, self._generation)
                store.stage(row, batch)
                rows, size = rows + batch.num_rows, size + batch.nbytes
        self._buffered.clear()
        return WriteStats(rows, size)


class _Session:
    def __init__(self, path: str, pipeline: str, epoch: int) -> None:
        self.path, self.pipeline, self.epoch = path, pipeline, epoch

    def apply_schema(self, change: Mapping[str, Json]) -> None:
        """Applies `change` to its table, which this session's pipeline owns or claims."""
        ((kind, detail),) = change.items()
        fields = json_object(detail)
        with transaction(self.path) as store:
            table = store.claimed(self.pipeline, self.epoch, json_object(fields["table"]))
            store.reschemed(table.name, _changed(table.schema, kind, fields))

    def writer(self, table: Mapping[str, Json]) -> _Writer:
        """A writer of `table`; a merge table's key must name columns it has."""
        merge.refuse_history_generation(table)
        name = str(table["name"])
        with transaction(self.path) as store:
            if table["merge"] is not None:
                held = store.table(name)
                key = cast("MergeKey", table["merge"])
                _holds_key(name, key, None if held is None else held.schema)
            store.claimed(self.pipeline, self.epoch, table)
        return _Writer(self, table)

    def discard_staged(self) -> None:
        """Removes what older sessions of the pipeline staged."""
        with transaction(self.path) as store:
            store.discarded(self.pipeline, self.epoch)

    def commit(self, meta: Mapping[str, Json]) -> Mapping[str, Json]:
        """Publishes `meta`'s segments with its state, in one transaction with its receipt.

        Only what this session's epoch staged is published, so what an older session's abandoned
        calls stage, however late, never is.
        """
        with transaction(self.path) as store:
            epoch, kept = store.fence(self.pipeline)
            landed = Fence(epoch, kept).decide(meta)
            if landed is not None:
                return landed
            key = json_object(meta["key"])
            if key["epoch"] != self.epoch:
                message = (
                    f"a session of epoch {self.epoch} cannot apply commit {key['seq']} "
                    f"of epoch {key['epoch']}"
                )
                raise Fenced(message)
            rows, size = store.publish(self.pipeline, self.epoch, meta)
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
            store.committed(self.pipeline, meta, receipt)
            return receipt

    def close(self) -> None:
        """Nothing is held open between calls."""


def published(path: str, table: str) -> pa.Table:
    """Every row the store at `path` published to `table`, read back as certification reads it."""
    batches = Warehouse(WarehouseConfig(path=path)).published({"name": table})
    return pa.Table.from_batches(
        [pa.record_batch(cast("pa.RecordBatch", batch)) for batch in batches]
    )


@destination(id="io.example.warehouse", config=WarehouseConfig, version="1.0.0")
class Warehouse:
    """Keeps what it loads, every write mode, in the sqlite3 file its configuration names."""

    def __init__(self, config: WarehouseConfig) -> None:
        self.path = config.path
        made(self.path)

    def capabilities(self) -> Capabilities:
        """Every write and delete mode, drops, and four writers at once, of the scalar types.

        It takes partial updates and change merges, and adds columns but never widens one.
        """
        return dataclasses.replace(
            Capabilities.minimal(),
            write_modes=WriteModes(append=True, replace=True, merge=True, history=True),
            delete_modes=DeleteModes(hard=True, soft=True),
            partial_updates=True,
            merge_changes=True,
            drop_tables=True,
            max_parallel_writers=4,
        )

    def check(self) -> None:
        """The file opens."""
        connected(self.path).close()

    def open(self, context: OpenContext) -> Opened:
        """A session of the pipeline at its next epoch, which the fence row keeps."""
        pipeline = context.pipeline
        with transaction(self.path) as store:
            epoch, landed, state = store.opened(pipeline)
        return Opened(_Session(self.path, pipeline, epoch), epoch, landed, state)

    def published(self, table: Mapping[str, Json]) -> Iterator[object]:
        """Every batch `table` published, with every column of the table at its types."""
        name = str(table["name"])
        with closing(connected(self.path)) as database:
            store = Store(database)
            held = store.table(name)
            batches = [] if held is None else store.kept(name, "published")
        if held is None or held.schema is None:
            yield from batches
            return
        yield from merge.read_back(to_arrow(held.schema), batches)
