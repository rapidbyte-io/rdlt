"""An append-only destination over one sqlite3 file, written as an author writes one.

Its store outlives its process, so a host that kills it finds what it committed: certification's
kill clause runs against its launcher. Batches are kept as Arrow IPC; the fence rows, state,
tables, staging and published rows live in one file, and a commit is one transaction.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable, Iterator, Mapping
from typing import TypeVar, cast

import pyarrow as pa

from rdlt.connector import (
    Config,
    Fenced,
    InvalidConfig,
    InvalidData,
    Opened,
    answer,
    destination,
    minimal_capabilities,
)
from rdlt.model import Json

_TABLES = """
create table if not exists fence (pipeline text primary key, epoch integer not null, landed text);
create table if not exists state (
    pipeline text, key text, record text, primary key (pipeline, key)
);
create table if not exists tables (name text primary key, owner text not null, columns text);
create table if not exists staged (
    pipeline text, epoch integer, segment integer, tbl text, ipc blob
);
create table if not exists published (tbl text, ipc blob);
"""

T = TypeVar("T")


def _connected(path: str) -> sqlite3.Connection:
    """The store at `path`, its tables made where they are not; each statement commits alone."""
    database = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    database.executescript(_TABLES)
    return database


def _within(database: sqlite3.Connection, work: Callable[[], T]) -> T:
    """`work`'s answer, in one transaction that takes the store's write lock first."""
    database.execute("begin immediate")
    try:
        done = work()
    except BaseException:
        database.execute("rollback")
        raise
    database.execute("commit")
    return done


def _ipc(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as stream:
        stream.write_batch(batch)  # pyright: ignore[reportUnknownMemberType]
    return sink.getvalue().to_pybytes()


def _batch(ipc: bytes) -> pa.RecordBatch:
    return pa.ipc.open_stream(ipc).read_next_batch()


def _in(segment: int, ranges: Json) -> bool:
    """Whether `segment` is in `ranges`, a segment set: its runs of consecutive ids."""
    spans = cast("list[dict[str, int]]", ranges)
    return any(span["first"] <= segment <= span["last"] for span in spans)


def _columns(change: Mapping[str, Json]) -> tuple[str, dict[str, Json]]:
    """The table a schema change names, and the columns it declares, by name, at their types."""
    ((kind, detail),) = change.items()
    fields = cast("dict[str, Json]", detail)
    name = str(cast("dict[str, Json]", fields["table"])["name"])
    if kind == "create":
        declared = cast(
            "list[list[dict[str, Json]]]", cast("dict[str, Json]", fields["schema"])["fields"]
        )
    elif kind == "add_column":
        declared = [cast("list[dict[str, Json]]", fields["field"])]
    else:
        message = f"table {name} widens no column in place"
        raise InvalidData(message, code="schema_conflict")
    return name, {str(root["name"]): [root["type"], *rest] for root, *rest in declared}


def published(path: str, table: str) -> int:
    """How many rows the store at `path` published to `table`."""
    database = _connected(path)
    try:
        held = database.execute("select ipc from published where tbl = ?", (table,)).fetchall()
    finally:
        database.close()
    return sum(_batch(ipc).num_rows for (ipc,) in held)


class SqliteConfig(Config):
    """The file the store is kept in."""

    path: str


class _Writer:
    """Buffers a table's batches, and stages them on flush unless a newer session opened."""

    def __init__(self, session: _Session, table: str) -> None:
        self._session, self._table = session, table
        self._buffered: list[tuple[int, pa.RecordBatch]] = []

    def write(self, segment: int, batch: pa.RecordBatch) -> None:
        """Buffers `batch` for its segment."""
        self._buffered.append((segment, batch))

    def flush(self) -> tuple[int, int]:
        """Stages what is buffered, under the session's epoch."""
        session = self._session

        def staged() -> tuple[int, int]:
            (epoch,) = session.database.execute(
                "select epoch from fence where pipeline = ?", (session.pipeline,)
            ).fetchone()
            if epoch != session.epoch:
                message = f"pipeline {session.pipeline} is at epoch {epoch}"
                raise Fenced(message)
            rows = size = 0
            for segment, batch in self._buffered:
                row = (session.pipeline, session.epoch, segment, self._table, _ipc(batch))
                session.database.execute("insert into staged values (?, ?, ?, ?, ?)", row)
                rows, size = rows + batch.num_rows, size + batch.nbytes
            return rows, size

        flushed = _within(session.database, staged)
        self._buffered.clear()
        return flushed


class _Session:
    def __init__(self, path: str, pipeline: str, epoch: int) -> None:
        self.database = _connected(path)
        self.pipeline, self.epoch = pipeline, epoch

    def _claimed(self, name: str) -> dict[str, Json]:
        """The columns of the table `name`, created for this session's pipeline where it is new."""
        row = self.database.execute(
            "select owner, columns from tables where name = ?", (name,)
        ).fetchone()
        if row is None:
            self.database.execute("insert into tables values (?, ?, '{}')", (name, self.pipeline))
            return {}
        if row[0] != self.pipeline:
            message = f"table {name} belongs to pipeline {row[0]}"
            raise InvalidConfig(message, code="table_owned")
        return cast("dict[str, Json]", json.loads(row[1]))

    def apply_schema(self, change: Mapping[str, Json]) -> None:
        """Adds the columns `change` declares, refusing one the table holds at another type."""
        name, columns = _columns(change)

        def applied() -> None:
            held = self._claimed(name)
            for column, kept in columns.items():
                if held.get(column, kept) != kept:
                    message = f"table {name} holds column {column} at another type"
                    raise InvalidData(message, code="schema_conflict")
            held.update(columns)
            self.database.execute(
                "update tables set columns = ? where name = ?", (json.dumps(held), name)
            )

        _within(self.database, applied)

    def writer(self, table: Mapping[str, Json]) -> _Writer:
        """A writer of `table`, which this session's pipeline owns."""
        name = str(table["name"])
        _within(self.database, lambda: self._claimed(name))
        return _Writer(self, name)

    def discard_staged(self) -> None:
        """Removes what older sessions of the pipeline staged."""
        self.database.execute(
            "delete from staged where pipeline = ? and epoch < ?", (self.pipeline, self.epoch)
        )

    def commit(self, meta: Mapping[str, Json]) -> Mapping[str, Json]:
        """Publishes `meta`'s segments with its state, in one transaction with its receipt."""
        return _within(self.database, lambda: self._committed(meta))

    def _committed(self, meta: Mapping[str, Json]) -> Mapping[str, Json]:
        epoch, kept = self.database.execute(
            "select epoch, landed from fence where pipeline = ?", (self.pipeline,)
        ).fetchone()
        landed = None if kept is None else cast("dict[str, Json]", json.loads(kept))
        after = None if meta["after"] is None else cast("dict[str, Json]", meta["after"])
        decided = answer(epoch, landed, cast("dict[str, Json]", meta["key"]), after)
        if decided == "landed" and landed is not None:
            return landed
        if decided == "fenced":
            message = f"pipeline {self.pipeline} is at epoch {epoch}"
            raise Fenced(message)
        if decided == "stale":
            message = "the commit expects another commit landed"
            raise Fenced(message, code="commit_stale")
        rows = size = 0
        staged = self.database.execute(
            "select rowid, segment, tbl, ipc from staged where pipeline = ? and epoch = ?",
            (self.pipeline, self.epoch),
        ).fetchall()
        for rowid, segment, table, ipc in staged:
            if _in(segment, meta["segments"]):
                batch = _batch(ipc)
                self.database.execute("insert into published values (?, ?)", (table, ipc))
                rows, size = rows + batch.num_rows, size + batch.nbytes
            elif not _in(segment, meta["abandoned"]):
                continue
            self.database.execute("delete from staged where rowid = ?", (rowid,))
        for change in cast("list[dict[str, Json]]", meta["state_delta"]):
            ((kind, detail),) = change.items()
            if kind == "put":
                record = cast("dict[str, Json]", detail)
                row = (self.pipeline, str(record["key"]), json.dumps(record))
                self.database.execute("insert or replace into state values (?, ?, ?)", row)
            else:
                self.database.execute(
                    "delete from state where pipeline = ? and key = ?", (self.pipeline, detail)
                )
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
        self.database.execute(
            "update fence set landed = ? where pipeline = ?", (json.dumps(receipt), self.pipeline)
        )
        return receipt

    def close(self) -> None:
        """Closes the session's connection."""
        self.database.close()


@destination(
    id="io.example.pysqlite",
    config=SqliteConfig,
    capabilities=minimal_capabilities(),
    version="1.0.0",
)
class PySqlite:
    """Keeps what it loads in the sqlite3 file its configuration names."""

    def __init__(self, config: SqliteConfig) -> None:
        self.path = config.path
        _connected(self.path).close()

    def check(self) -> None:
        """The file opens."""
        _connected(self.path).close()

    def open(self, pipeline: str) -> Opened:
        """A session of `pipeline` at its next epoch, which the fence row keeps."""
        database = _connected(self.path)

        def opened() -> tuple[int, dict[str, Json] | None, list[dict[str, Json]]]:
            row = database.execute(
                "select epoch, landed from fence where pipeline = ?", (pipeline,)
            ).fetchone()
            epoch = 1 if row is None else row[0] + 1
            database.execute(
                "insert into fence values (?, ?, null) "
                "on conflict(pipeline) do update set epoch = excluded.epoch",
                (pipeline, epoch),
            )
            landed = None if row is None or row[1] is None else json.loads(row[1])
            records = database.execute(
                "select record from state where pipeline = ?", (pipeline,)
            ).fetchall()
            return epoch, landed, [json.loads(record) for (record,) in records]

        try:
            epoch, landed, state = _within(database, opened)
        finally:
            database.close()
        return Opened(_Session(self.path, pipeline, epoch), epoch, landed, state)

    def published(self, table: Mapping[str, Json]) -> Iterator[pa.RecordBatch]:
        """Every batch `table` published."""
        database = _connected(self.path)
        try:
            held = database.execute(
                "select ipc from published where tbl = ? order by rowid", (str(table["name"]),)
            ).fetchall()
        finally:
            database.close()
        for (ipc,) in held:
            yield _batch(ipc)
