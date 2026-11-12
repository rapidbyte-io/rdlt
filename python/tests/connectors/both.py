"""A connector of both roles under one id, which one launcher serves.

Its source is the tickets source, and its destination the sqlite3 destination.
"""

from __future__ import annotations

from rdlt.connector import destination, source
from tests.connectors.pysqlite import PySqlite, SqliteConfig
from tests.connectors.tickets import Tickets, TicketsConfig


@source(id="io.example.both", config=TicketsConfig, version="1.0.0")
class BothSource(Tickets):
    """The tickets source, as the source of `io.example.both`."""


@destination(id="io.example.both", config=SqliteConfig, version="1.0.0")
class BothDestination(PySqlite):
    """The sqlite3 destination, as the destination of `io.example.both`."""


CONNECTORS = (BothSource, BothDestination)
"""What its `rdlt.connectors` entry point names: a source and a destination, which one launcher
serves."""
