"""A source over a SQLite database whose streams are the tables it finds there, one a table.

Each stream's name, key, cursor, schema and read are its table's: a table keyed by an integer
resumes after the last key it read, one keyed by text after the last text, and each declares the
schema its columns' types say. A database the configuration attaches is a schema of its own, whose
tables are streams in its namespace, as a database source's schemas are. Each stream reads one
partition, which ends once its last row is read.
"""

from __future__ import annotations

import itertools
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from functools import partial

import pyarrow as pa
import pydantic

import rdlt
from rdlt.connector import Config, Cursor, ReadStream, StreamContext, checkpoint, source, stream

PAGE = 10
"""Rows a batch holds."""

ARROW = {"INTEGER": pa.int64(), "TEXT": pa.string(), "REAL": pa.float64()}
"""The Arrow type of each column type the source reads."""

MAIN = "main"
"""The schema of the database the configuration names, whose tables have no namespace."""


class TablesConfig(Config):
    """The database file whose tables are the streams, and others attached, by schema."""

    path: str
    attached: dict[str, str] = pydantic.Field(default_factory=dict[str, str])


class AfterNumber(Cursor):
    """Where a table keyed by an integer resumes: after the last key read."""

    after: int = -1


class AfterText(Cursor):
    """Where a table keyed by text resumes: after the last key read."""

    after: str = ""


@source(id="io.example.tables", config=TablesConfig, version="1.0.0")
class Tables:
    """Reads each table it finds a page a batch, checkpointing every other page and when due."""

    def __init__(self, config: TablesConfig) -> None:
        self.config = config

    def _database(self) -> closing[sqlite3.Connection]:
        database = sqlite3.connect(self.config.path, timeout=30)
        for schema, path in self.config.attached.items():
            database.execute("ATTACH DATABASE ? AS ?", (path, schema))
        return closing(database)

    def check(self) -> None:
        """The database opens and lists its tables."""
        with self._database() as database:
            database.execute("SELECT name FROM sqlite_master").fetchall()

    def streams(self) -> list[ReadStream]:
        """One stream a table, the main database's first, each schema's in its namespace."""
        with self._database() as database:
            return [
                self._table(database, schema, name)
                for schema in (MAIN, *self.config.attached)
                for (name,) in database.execute(
                    f'SELECT name FROM "{schema}".sqlite_master WHERE type = ? ORDER BY name',
                    ("table",),
                ).fetchall()
            ]

    def _table(self, database: sqlite3.Connection, schema: str, table: str) -> ReadStream:
        columns = database.execute(f'PRAGMA "{schema}".table_info("{table}")').fetchall()
        key, kind = next((column[1], column[2]) for column in columns if column[5] == 1)
        arrow = pa.schema(
            [pa.field(column[1], ARROW[column[2]], nullable=column[5] == 0) for column in columns]
        )
        cursor = AfterNumber if kind == "INTEGER" else AfterText
        name = table if schema == MAIN else rdlt.StreamName(table, namespace=schema)
        read = partial(self.rows, f'"{schema}"."{table}"', key, arrow)
        made: ReadStream = stream(
            name, primary_key=key, cursor=cursor, schema=arrow, partitions=lambda: ["rows"]
        )(read)
        return made

    def rows(
        self,
        table: str,
        key: str,
        schema: pa.Schema,
        after: AfterNumber | AfterText,
        ctx: StreamContext,
    ) -> Iterator[object]:
        """Reads `table` after `after`, in its key's order, ending on a checkpoint.

        Ending on one leaves its partition there, so the next run reads the rows added since.
        """
        names = schema.names
        last, sealed = after.after, True
        with self._database() as database:
            for page in itertools.count():
                found = database.execute(
                    f'SELECT * FROM {table} WHERE "{key}" > ? ORDER BY "{key}" LIMIT ?',
                    (last, PAGE),
                ).fetchall()
                if found:
                    yield pa.RecordBatch.from_pylist(
                        [dict(zip(names, row, strict=True)) for row in found], schema=schema
                    )
                    last, sealed = found[-1][names.index(key)], False
                if not sealed and (not found or ctx.checkpoint_due() or page % 2 == 1):
                    yield checkpoint(after.model_copy(update={"after": last}))
                    sealed = True
                if not found:
                    return
