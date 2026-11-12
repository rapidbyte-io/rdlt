"""Write a connector in Python: its configuration, its reads or writes, the SDK's choreography."""

from __future__ import annotations

import signal
import sys
from collections.abc import Callable, Sequence

from rdlt import _native
from rdlt.connector._capabilities import (
    Capabilities,
    DeleteModes,
    IdentifierRules,
    NestedSupport,
    SchemaChanges,
    WriteModes,
)
from rdlt.connector._config import Config
from rdlt.connector._context import (
    Exclusion,
    Headroom,
    Limits,
    Partition,
    PartitionState,
    StreamContext,
    StreamState,
    checkpoint,
)
from rdlt.connector._cursor import Cursor
from rdlt.connector._declared import ConnectContext
from rdlt.connector._destination import OpenContext, Opened, WriteStats, destination
from rdlt.connector._destination import declares as declares_destination
from rdlt.connector._destination import placed as placed_destination
from rdlt.connector._failures import (
    COLUMN_MISSING,
    COMMIT_STALE,
    CURSOR_UNISSUED,
    RETENTION_LOST,
    SCHEMA_CONFLICT,
    TABLE_OWNED,
    ConnectorError,
    Fenced,
    InvalidConfig,
    InvalidData,
    RateLimited,
    Transient,
    Unauthorized,
    Unsupported,
)
from rdlt.connector._landed import Answer, Fence, answer
from rdlt.connector._secret import Secret
from rdlt.connector._source import ReadStream, source, stream
from rdlt.connector._source import declares as declares_source
from rdlt.connector._source import placed as placed_source


def serve(*connectors: type, args: Sequence[str] | None = None) -> int:
    """Serves `connectors` as a connector binary does, until its host stops it; the exit status.

    `connectors` are a class `@source` made, a class `@destination` made, or one of each, whose
    roles one process serves, as the Rust SDK's `Served::new().with_source(..)
    .with_destination(..)` does; `args` are the arguments after the program's name, the process's
    own by default. A launcher ends with `raise SystemExit(serve(Tickets))`. Spawned by a host it
    serves the socket `--rdlt-fd` names; run with `--listen` and the TLS flags it serves hosts
    over mutual TLS. It runs on the main thread, and answers Ctrl-C as a connector binary does:
    spawned, it ignores it, since its host stops it; listening, it stops. Arguments it cannot read
    are said on its standard error before it serves, and fail it.

    Raises:
        TypeError: no class is given, two of one role are, or one neither decorator made.
    """
    unmade = [made.__name__ for made in connectors if not _declares_a_role(made)]
    if unmade or not connectors:
        message = (
            f"{', '.join(unmade) or 'nothing'} is no connector: serve() takes a class @source "
            "made, a class @destination made, or one of each"
        )
        raise TypeError(message)
    source = _one(connectors, "source", declares_source)
    destination = _one(connectors, "destination", declares_destination)
    # Held while it serves: the native factories reach what keeps their instances weakly.
    sourced = None if source is None else placed_source(source)
    destined = None if destination is None else placed_destination(destination)
    arguments = list(sys.argv[1:] if args is None else args)
    # The server takes the signals itself, and calls the handler it found after its own: Python's
    # would raise `KeyboardInterrupt` once the server returned.
    held = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        said = _native.serve(
            arguments,
            None if sourced is None else sourced[0],
            None if destined is None else destined[0],
        )
    finally:
        if held is not None:
            signal.signal(signal.SIGINT, held)
    return 0 if said is None else _failed(said)


def _declares_a_role(connector: type) -> bool:
    """Whether `@source` or `@destination` made `connector`."""
    return declares_source(connector) or declares_destination(connector)


def _one(connectors: Sequence[type], role: str, declares: Callable[[type], bool]) -> type | None:
    """The class of `connectors` that serves `role`, where one does.

    Raises:
        TypeError: two of them serve it.
    """
    found = [made for made in connectors if declares(made)]
    if len(found) > 1:
        named = " and ".join(made.__name__ for made in found)
        message = f"{named} are both a {role}: serve() serves one of each role"
        raise TypeError(message)
    return found[0] if found else None


def _failed(said: str) -> int:
    """Says on standard error why serving failed, and answers the exit status that fails."""
    print(said, file=sys.stderr)  # noqa: T201 - a connector reports why it failed on its standard error
    return 1


__all__ = [
    "COLUMN_MISSING",
    "COMMIT_STALE",
    "CURSOR_UNISSUED",
    "RETENTION_LOST",
    "SCHEMA_CONFLICT",
    "TABLE_OWNED",
    "Answer",
    "Capabilities",
    "Config",
    "ConnectContext",
    "ConnectorError",
    "Cursor",
    "DeleteModes",
    "Exclusion",
    "Fence",
    "Fenced",
    "Headroom",
    "IdentifierRules",
    "InvalidConfig",
    "InvalidData",
    "Limits",
    "NestedSupport",
    "OpenContext",
    "Opened",
    "Partition",
    "PartitionState",
    "RateLimited",
    "ReadStream",
    "SchemaChanges",
    "Secret",
    "StreamContext",
    "StreamState",
    "Transient",
    "Unauthorized",
    "Unsupported",
    "WriteModes",
    "WriteStats",
    "answer",
    "checkpoint",
    "destination",
    "serve",
    "source",
    "stream",
]
