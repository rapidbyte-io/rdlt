"""Write a connector in Python: its configuration, its reads or writes, the SDK's choreography."""

from __future__ import annotations

import signal
import sys
from collections.abc import Sequence

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


def serve(connector: type, args: Sequence[str] | None = None) -> int:
    """Serves `connector` as a connector binary does, until its host stops it; the exit status.

    `connector` is a class `@source` or `@destination` made, and `args` the arguments after the
    program's name, the process's own by default. A launcher ends with
    `raise SystemExit(serve(Tickets))`. Spawned by a host it serves the socket `--rdlt-fd` names;
    run with `--listen` and the TLS flags it serves hosts over mutual TLS. It runs on the main
    thread, and answers Ctrl-C as a connector binary does: spawned, it ignores it, since its host
    stops it; listening, it stops. Arguments it cannot read, and a destination's capabilities
    rdlt cannot read, are said on its standard error before it serves, and fail it.
    """
    placed: tuple[_native.SourceFactory | _native.DestinationFactory, object]
    if declares_destination(connector):
        try:
            placed = placed_destination(connector)
        except ValueError as refused:
            return _failed(str(refused))
    elif declares_source(connector):
        placed = placed_source(connector)
    else:
        message = f"{connector.__name__} is no connector: decorate it with @source or @destination"
        raise TypeError(message)
    arguments = list(sys.argv[1:] if args is None else args)
    # The server takes the signals itself, and calls the handler it found after its own: Python's
    # would raise `KeyboardInterrupt` once the server returned.
    held = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        said = _native.serve(arguments, placed[0])
    finally:
        if held is not None:
            signal.signal(signal.SIGINT, held)
    return 0 if said is None else _failed(said)


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
