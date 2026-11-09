"""Write a connector in Python: its configuration, its streams, and the Rust SDK's choreography."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from rdlt.connector._config import Config
from rdlt.connector._failures import (
    ConnectorFailure,
    InvalidConfig,
    InvalidData,
    RateLimited,
    Transient,
    Unauthorized,
    Unsupported,
)
from rdlt.connector._secret import Secret
from rdlt.connector._source import source, stream
from rdlt.context import StreamContext, checkpoint
from rdlt.cursor import Cursor
from rdlt.model import Json


@dataclass(frozen=True, slots=True)
class Local:
    """A connector class placed in process, trusted as the program's own code."""

    connector: type
    """The class, which `@source` made."""
    config: Mapping[str, Json] | None = field(default=None, repr=False, hash=False)
    """Its configuration, which may hold a credential written inline and is never shown."""


def local(connector: type, *, config: Mapping[str, Json] | None = None) -> Local:
    """`connector`, a class `@source` made, placed in process with `config`."""
    return Local(connector, config)


__all__ = [
    "Config",
    "ConnectorFailure",
    "Cursor",
    "InvalidConfig",
    "InvalidData",
    "Local",
    "RateLimited",
    "Secret",
    "StreamContext",
    "Transient",
    "Unauthorized",
    "Unsupported",
    "checkpoint",
    "local",
    "source",
    "stream",
]
