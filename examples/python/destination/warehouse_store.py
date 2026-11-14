"""The warehouse's store: its sqlite3 tables, and what one transaction reads and writes of them.

It keeps what the reference memory destination, `MemoryDestination`, keeps: each table's owner,
schema and merge key, its published rows, the tombstones of rows a change stream removed, its
replace generations and what sessions staged, and each pipeline's fence row and state. Rows are
kept as Arrow IPC and merged with `rdlt.connector.merge`, in Rust.
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from collections.abc import Generator, Mapping, Sequence
from contextlib import closing, contextmanager
from typing import cast

import pyarrow as pa

from rdlt.connector import (
    TABLE_OWNED,
    Fenced,
    InvalidConfig,
    InvalidData,
    MergeKey,
    merge,
    to_arrow,
)
from rdlt.model import Json

_TABLES = """
create table if not exists fence (pipeline text primary key, epoch integer not null, landed text);
create table if not exists state (
    pipeline text, key text, record text, primary key (pipeline, key)
);
create table if not exists tables (
    name text primary key, owner text not null, schema text, merge text
);
create table if not exists names (
    pipeline text, path text, name text, primary key (pipeline, path)
);
create table if not exists staged (
    pipeline text, epoch integer, segment integer, tbl text, generation text, ipc blob
);
create table if not exists kept (tbl text, kind text, generation text, ipc blob);
"""
"""The store's tables.

`kept` holds each table's `published` rows, its `tombstone`s and each replace `generation`'s rows,
in the order they were kept. A generation is kept as its text: its id may be beyond what an
sqlite3 integer holds.
"""


def json_object(value: Json) -> dict[str, Json]:
    """`value`, a JSON object."""
    return cast("dict[str, Json]", value)


def json_list(value: Json) -> list[Json]:
    """`value`, a JSON array."""
    return cast("list[Json]", value)


def _ipc(batch: object) -> bytes:
    """`batch`, anything with `__arrow_c_array__`, as Arrow IPC."""
    written = pa.record_batch(cast("pa.RecordBatch", batch))
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, written.schema) as stream:
        stream.write_batch(written)  # pyright: ignore[reportUnknownMemberType]
    return sink.getvalue().to_pybytes()


def _batch(kept: bytes) -> pa.RecordBatch:
    return pa.ipc.open_stream(kept).read_next_batch()


def _within(segment: int, ranges: Json) -> bool:
    """Whether `segment` is in `ranges`, a segment set: its runs of consecutive ids."""
    spans = cast("list[dict[str, int]]", ranges)
    return any(span["first"] <= segment <= span["last"] for span in spans)


def made(path: str) -> None:
    """Makes the store's tables at `path`, where they are not."""
    with closing(connected(path)) as database:
        database.executescript(_TABLES)


def connected(path: str) -> sqlite3.Connection:
    """The store at `path`, for one call on one thread; each statement commits alone."""
    return sqlite3.connect(path, timeout=30, isolation_level=None)


@contextmanager
def transaction(path: str) -> Generator[Store]:
    """The store at `path`, in one transaction of a connection of its own.

    It takes the write lock first, commits where the block returns, and rolls back where it raises.
    """
    with closing(connected(path)) as database:
        database.execute("begin immediate")
        try:
            yield Store(database)
        except BaseException:
            database.execute("rollback")
            raise
        database.execute("commit")


@dataclasses.dataclass
class Table:
    """A table: its name, the pipeline that owns it, its schema once created, its merge key."""

    name: str
    owner: str
    schema: dict[str, Json] | None
    merge: MergeKey | None


Staged = list[tuple[str | None, pa.RecordBatch]]
"""What a commit stages for a table, each batch for the table itself or for a generation."""


class Store:
    """The store, within one transaction."""

    def __init__(self, database: sqlite3.Connection) -> None:
        self.database = database

    def opened(
        self, pipeline: str
    ) -> tuple[int, Mapping[str, Json] | None, list[Mapping[str, Json]]]:
        """Raises `pipeline`'s epoch above every earlier open's, durably, answering what it opens.

        That is the epoch, the receipt its fence row keeps and the pipeline's state records.
        """
        row = self.database.execute(
            "select epoch, landed from fence where pipeline = ?", (pipeline,)
        ).fetchone()
        epoch = 1 if row is None else row[0] + 1
        self.database.execute(
            "insert into fence values (?, ?, null) "
            "on conflict(pipeline) do update set epoch = excluded.epoch",
            (pipeline, epoch),
        )
        landed = None if row is None or row[1] is None else json.loads(row[1])
        records = self.database.execute(
            "select record from state where pipeline = ?", (pipeline,)
        ).fetchall()
        state = cast("list[Mapping[str, Json]]", [json.loads(record) for (record,) in records])
        return epoch, landed, state

    def fence(self, pipeline: str) -> tuple[int, Mapping[str, Json] | None]:
        """`pipeline`'s fence row: its epoch and the receipt of its last commit that landed."""
        epoch, landed = self.database.execute(
            "select epoch, landed from fence where pipeline = ?", (pipeline,)
        ).fetchone()
        return epoch, None if landed is None else cast("Mapping[str, Json]", json.loads(landed))

    def epoch(self, pipeline: str) -> int:
        """`pipeline`'s newest epoch, 0 before its first open."""
        row = self.database.execute("select epoch from fence where pipeline = ?", (pipeline,))
        found = row.fetchone()
        return 0 if found is None else int(found[0])

    def committed(
        self, pipeline: str, meta: Mapping[str, Json], receipt: Mapping[str, Json]
    ) -> None:
        """Applies `meta`'s state changes and keeps `receipt` as the last commit's that landed."""
        for change in json_list(meta["state_delta"]):
            ((kind, detail),) = json_object(change).items()
            if kind == "put":
                record = json_object(detail)
                row = (pipeline, str(record["key"]), json.dumps(record))
                self.database.execute("insert or replace into state values (?, ?, ?)", row)
            else:
                self.database.execute(
                    "delete from state where pipeline = ? and key = ?", (pipeline, detail)
                )
        self.database.execute(
            "update fence set landed = ? where pipeline = ?", (json.dumps(receipt), pipeline)
        )

    def discarded(self, pipeline: str, epoch: int) -> None:
        """Removes what `pipeline`'s sessions older than `epoch` staged."""
        self.database.execute(
            "delete from staged where pipeline = ? and epoch < ?", (pipeline, epoch)
        )

    def stage(self, row: tuple[str, int, int, str, str | None], batch: object) -> None:
        """Stages `batch` as `row` says: its pipeline, epoch, segment, table and generation."""
        self.database.execute("insert into staged values (?, ?, ?, ?, ?, ?)", (*row, _ipc(batch)))

    def table(self, name: str) -> Table | None:
        """The table `name`, where a pipeline claimed it."""
        row = self.database.execute(
            "select owner, schema, merge from tables where name = ?", (name,)
        ).fetchone()
        if row is None:
            return None
        owner, schema, key = row
        loaded = None if schema is None else json.loads(schema)
        return Table(name, owner, loaded, None if key is None else json.loads(key))

    def reschemed(self, name: str, schema: dict[str, Json]) -> None:
        """Keeps `schema` as the table `name`'s."""
        self.database.execute(
            "update tables set schema = ? where name = ?", (json.dumps(schema), name)
        )

    def claimed(self, pipeline: str, epoch: int, table: Mapping[str, Json]) -> Table:
        """The table `table` names, claimed for `pipeline`'s session at `epoch` where unowned.

        Its path's name and its merge key are recorded. Another pipeline's table is refused, and
        so is any claim by a session a newer one fenced: its late call would otherwise record its
        own merge key over the newer session's.
        """
        name = str(table["name"])
        held = self.table(name)
        if held is not None and held.owner != pipeline:
            message = f"table {name} belongs to pipeline {held.owner}"
            raise InvalidConfig(message, code=TABLE_OWNED)
        if self.epoch(pipeline) != epoch:
            message = f"pipeline {pipeline} has a session newer than epoch {epoch}"
            raise Fenced(message)
        key = None if table["merge"] is None else json.dumps(table["merge"])
        self.database.execute(
            "insert into tables values (?, ?, null, ?) on conflict(name) do update set merge = ?",
            (name, pipeline, key, key),
        )
        self.database.execute(
            "insert or replace into names values (?, ?, ?)",
            (pipeline, json.dumps(table["path"]), name),
        )
        return cast("Table", self.table(name))

    def owned(self, pipeline: str, name: str) -> Table:
        """The table `name`, which `pipeline` owns; one dropped since belongs to no pipeline."""
        held = self.table(name)
        if held is None:
            message = f"table {name} belongs to no pipeline, so no pipeline's session changes it"
            raise InvalidConfig(message, code="table_unowned")
        if held.owner != pipeline:
            message = f"table {name} belongs to pipeline {held.owner}"
            raise InvalidConfig(message, code=TABLE_OWNED)
        return held

    def named(self, pipeline: str, path: Json) -> str | None:
        """The table `pipeline` registered for `path`, where it still exists."""
        row = self.database.execute(
            "select names.name from names join tables on tables.name = names.name "
            "where pipeline = ? and path = ?",
            (pipeline, json.dumps(path)),
        ).fetchone()
        return None if row is None else str(row[0])

    def kept(self, name: str, kind: str) -> list[pa.RecordBatch]:
        """The batches `name` keeps of `kind`, in the order they were kept."""
        rows = self.database.execute(
            "select ipc from kept where tbl = ? and kind = ? order by rowid", (name, kind)
        )
        return [_batch(kept) for (kept,) in rows.fetchall()]

    def keep(
        self, name: str, kind: str, batches: Sequence[object], generation: str | None = None
    ) -> None:
        """Keeps `batches` in `name` as `kind`, of `generation` where it is a generation's."""
        for batch in batches:
            row = (name, kind, generation, _ipc(batch))
            self.database.execute("insert into kept values (?, ?, ?, ?)", row)

    def staged(self, name: str, pipeline: str, epoch: int, segments: Json) -> Staged:
        """What `pipeline`'s session at `epoch` staged in `name` in `segments`, in their order."""
        rows = self.database.execute(
            "select segment, generation, ipc from staged "
            "where tbl = ? and pipeline = ? and epoch = ? order by segment, rowid",
            (name, pipeline, epoch),
        ).fetchall()
        return [
            (generation, _batch(kept))
            for segment, generation, kept in rows
            if _within(segment, segments)
        ]

    def publish(self, pipeline: str, epoch: int, meta: Mapping[str, Json]) -> tuple[int, int]:
        """Publishes what `meta`'s segments staged, answering the rows and bytes it published.

        Only what this session's epoch staged is published: an older session's, however late it
        staged, is the next `discarded`'s. It swaps in the generations `meta` finishes and drops
        the tables it drops.
        """
        self._owns(pipeline, meta)
        plans = self._plans(pipeline, epoch, meta)
        held = self.database.execute(
            "select rowid, segment from staged where pipeline = ? and epoch = ?", (pipeline, epoch)
        ).fetchall()
        for rowid, segment in held:
            if _within(segment, meta["segments"]) or _within(segment, meta["abandoned"]):
                self.database.execute("delete from staged where rowid = ?", (rowid,))
        rows = size = 0
        for name, staged, merged in plans:
            for generation, batch in staged:
                rows, size = rows + batch.num_rows, size + batch.nbytes
                if merged is None:
                    kind = "published" if generation is None else "generation"
                    self.keep(name, kind, [batch], generation)
            if merged is not None:
                self.database.execute(
                    "delete from kept where tbl = ? and kind in ('published', 'tombstone')", (name,)
                )
                self.keep(name, "published", merged.rows)
                self.keep(name, "tombstone", merged.tombstones)
        for path, generation in cast("list[tuple[Json, int]]", meta["finish_generations"]):
            name = self.named(pipeline, path)
            if name is None:
                continue
            self.database.execute(
                "delete from kept where tbl = ? and not (kind = 'generation' and generation = ?)",
                (name, str(generation)),
            )
            self.database.execute(
                "update kept set kind = 'published', generation = null where tbl = ?", (name,)
            )
        for dropped in json_list(meta["drop_tables"]):
            name = str(json_object(dropped)["name"])
            self.database.execute("delete from tables where name = ?", (name,))
            self.database.execute("delete from names where name = ?", (name,))
            self.database.execute("delete from kept where tbl = ?", (name,))
            self.database.execute("delete from staged where tbl = ?", (name,))
        return rows, size

    def _owns(self, pipeline: str, meta: Mapping[str, Json]) -> None:
        """Refuses `meta` where a table it swaps a generation into, or drops, is another's."""
        swapped = [path for path, _ in cast("list[tuple[Json, int]]", meta["finish_generations"])]
        names = [self.named(pipeline, path) for path in swapped]
        dropped = [str(json_object(table)["name"]) for table in json_list(meta["drop_tables"])]
        for name in [*names, *dropped]:
            if name is not None and self.table(name) is not None:
                self.owned(pipeline, name)

    def _plans(
        self, pipeline: str, epoch: int, meta: Mapping[str, Json]
    ) -> list[tuple[str, Staged, merge.Merged | None]]:
        """What publishing `meta` does to each table: what it staged, and a merge table's rows.

        A child table the commit lists follows its root's staged rows, even where it staged none.
        """

        def staged(name: str) -> Staged:
            return self.staged(name, pipeline, epoch, meta["segments"])

        children = {
            str(json_object(child)["table"]): cast("MergeKey", json_object(child)["merge"])
            for child in json_list(meta["child_tables"])
        }
        plans: list[tuple[str, Staged, merge.Merged | None]] = []
        for (name,) in self.database.execute("select name from tables order by name").fetchall():
            listed = children.get(name)
            root = None if listed is None else listed["root"]
            if root is None or not staged(root["table"]):
                listed = None
            own = staged(name)
            if not own and listed is None:
                continue
            table = self.owned(pipeline, name)
            key = table.merge or listed
            if key is None:
                plans.append((name, own, None))
                continue
            root = key["root"]
            roots = [] if root is None else [batch for _, batch in staged(root["table"])]
            plans.append((name, own, self._merged(table, own, key, roots)))
        return plans

    def _merged(
        self, table: Table, staged: Staged, key: MergeKey, roots: list[pa.RecordBatch]
    ) -> merge.Merged:
        """The table's rows and tombstones once `staged` merges in by `key`.

        A child table's rows follow `roots`, what its root staged.
        """
        if table.schema is not None:
            schema: object = to_arrow(table.schema)
        elif staged:
            schema = staged[0][1].schema
        else:
            message = f"table {table.name} merges nothing"
            raise InvalidData(message)
        incoming = [batch for _, batch in staged]
        published = self.kept(table.name, "published")
        root = key["root"]
        if root is None:
            buried = self.kept(table.name, "tombstone")
            return merge.merge_sparse(schema, published, buried, incoming, key)
        rows = merge.merge_children_sparse(schema, published, incoming, key, root, roots)
        return merge.Merged(rows, [])
