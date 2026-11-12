"""A source that tells where it stands outside the engine, as a queue's consumer group does.

Its messages are numbered, and where each partition stands is kept in a SQLite file the
configuration names, outside any connection: a connection told a cursor is committed settles it
there, and any other connection reads it back, as a broker keeps a group's committed offset.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import closing

from rdlt.connector import (
    CURSOR_UNISSUED,
    Config,
    Cursor,
    InvalidData,
    StreamContext,
    checkpoint,
    source,
    stream,
)

PAGE = 10
"""Messages a push holds."""


class InboxConfig(Config):
    """How many messages the inbox holds, and the file that keeps where it stands."""

    messages: int
    offsets: str


class Next(Cursor):
    """Where a read resumes: the next message's number."""

    message: int = 0


@source(id="io.example.inbox", config=InboxConfig, version="1.0.0", acknowledged=True)
class Inbox:
    """Reads its messages a page a push, checkpointing every other page and when one is due."""

    def __init__(self, config: InboxConfig) -> None:
        self.config = config
        with self._offsets() as offsets:
            offsets.execute(
                "CREATE TABLE IF NOT EXISTS settled (partition TEXT PRIMARY KEY, message INTEGER)"
            )

    def _offsets(self) -> closing[sqlite3.Connection]:
        return closing(sqlite3.connect(self.config.offsets, timeout=30, isolation_level=None))

    def check(self) -> None:
        """The inbox's file opens."""
        with self._offsets():
            pass

    def planned(self) -> list[str]:
        """Its partitions: one, which ends once its last message is read."""
        return ["mail"]

    @stream(
        primary_key=["id"],
        cursor=Next,
        partitions="planned",
        committed="settled",
        acknowledged="standing",
    )
    def messages(self, start: Next, ctx: StreamContext) -> Iterator[object]:
        """Reads the messages from `start`, refusing a cursor past every message it holds."""
        if start.message > self.config.messages:
            message = f"message {start.message} is past the inbox's last"
            raise InvalidData(message, code=CURSOR_UNISSUED)
        pages = range(start.message, self.config.messages, PAGE)
        for page, first in enumerate(pages):
            last = min(first + PAGE, self.config.messages)
            yield [{"id": n, "body": f"message {n}"} for n in range(first, last)]
            if ctx.checkpoint_due() or last == self.config.messages or page % 2 == 1:
                yield checkpoint(Next(message=last))

    def settled(self, cursors: Sequence[tuple[str, Next]]) -> None:
        """Settles each partition up to the cursor committed, never back."""
        with self._offsets() as offsets:
            for partition, cursor in cursors:
                offsets.execute(
                    "INSERT INTO settled VALUES (?, ?) ON CONFLICT (partition) DO UPDATE "
                    "SET message = max(message, excluded.message)",
                    (partition, cursor.message),
                )

    def standing(self, partition: str) -> Next | None:
        """Where `partition` stands: the last cursor settled, or none."""
        with self._offsets() as offsets:
            found = offsets.execute(
                "SELECT message FROM settled WHERE partition = ?", (partition,)
            ).fetchone()
        return None if found is None else Next(message=found[0])
