"""A source connector of one stream, `numbers`, whose cursor is the next row's number."""

from __future__ import annotations

from collections.abc import Iterator

from rdlt.connector import Config, Cursor, checkpoint, source, stream

PAGE = 10
"""Rows a push holds."""


class GreetingsConfig(Config):
    """The configuration the connector accepts; its JSON Schema is published."""

    rows: int
    """How many rows the stream holds."""


class Position(Cursor):
    """Where the stream resumes: the next row's number."""

    n: int = 0


@source(id="io.example.greetings", config=GreetingsConfig)
class Greetings:
    """The connector: built from its configuration, checked, then read."""

    def __init__(self, config: GreetingsConfig) -> None:
        self.rows = config.rows

    def check(self) -> None:
        """Checks the source can be read, raising `InvalidConfig` or `Unauthorized` where not."""

    @stream(primary_key=["n"], cursor=Position)
    def numbers(self, start: Position) -> Iterator[object]:
        """Reads the rows after `start`, a page a push, each page checkpointed."""
        for first in range(start.n, self.rows, PAGE):
            last = min(first + PAGE, self.rows)
            yield [{"n": n} for n in range(first, last)]
            yield checkpoint(Position(n=last))
