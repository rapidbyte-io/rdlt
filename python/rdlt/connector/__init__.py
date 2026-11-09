"""Write a connector in Python: its configuration, its streams, and the Rust SDK's choreography."""

from __future__ import annotations

import signal
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from rdlt import _native
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
from rdlt.connector._source import factory, source, stream
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


def serve(connector: type, argv: Sequence[str] | None = None) -> int:
    """Serves `connector` as a connector binary does, until its host stops it; the exit status.

    A launcher ends with `raise SystemExit(serve(Tickets))`. Spawned by a host it serves the
    socket `--rdlt-fd` names; run with `--listen` and the TLS flags it serves hosts over mutual
    TLS. It runs on the main thread, and answers Ctrl-C as a connector binary does: spawned, it
    ignores it, since its host stops it; listening, it stops.
    """
    made = factory(connector)
    arguments = list(sys.argv[1:] if argv is None else argv)
    # The server takes the signals itself, and calls the handler it found after its own: Python's
    # would raise `KeyboardInterrupt` once the server returned.
    held = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        said = _native.serve(arguments, made)
    finally:
        if held is not None:
            signal.signal(signal.SIGINT, held)
    if said is None:
        return 0
    print(said, file=sys.stderr)  # noqa: T201 - a connector reports why it failed on its standard error
    return 1


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
    "serve",
    "source",
    "stream",
]
