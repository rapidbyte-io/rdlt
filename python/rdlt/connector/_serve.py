"""`serve`: connector classes served as a connector binary serves its factories."""

from __future__ import annotations

import signal
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

from rdlt import _native
from rdlt.connector._destination import declares as declares_destination
from rdlt.connector._destination import placed as placed_destination
from rdlt.connector._source import declares as declares_source
from rdlt.connector._source import placed as placed_source


@dataclass(frozen=True, slots=True)
class _Probed:
    """A class `serve` serves with the certification probes it answers, as its launcher asked."""

    connector: type
    role: str


def acknowledging_source_factory(connector: type) -> _Probed:
    """`connector`, a class `@source` made, as `serve` serves it telling where it stands.

    It tells where its streams stand outside the engine where `@source(acknowledged=True)` says
    they do, as a Rust binary whose `main` serves `acknowledging_source_factory` does, so
    certification at its endpoint checks `S-ACK`. A class handed to `serve` itself tells nothing,
    and the launcher `rdlt connector install` writes refuses this, as a Rust release serves no
    probe.

    Raises:
        TypeError: `connector` is no source.
    """
    if not declares_source(connector):
        message = f"{connector.__name__} is no source: decorate it with @source"
        raise TypeError(message)
    return _Probed(connector, "source")


def readable_destination_factory(connector: type) -> _Probed:
    """`connector`, a class `@destination` made, as `serve` serves it reading back what it wrote.

    It answers certification's read-back through its `published(table)`, as a Rust binary whose
    `main` serves `readable_destination_factory` does, so certification at its endpoint compares
    what it published. A class handed to `serve` itself reads nothing back, and the launcher
    `rdlt connector install` writes refuses this, as a Rust release serves no probe.

    Raises:
        TypeError: `connector` is no destination, or has no `published`.
    """
    if not declares_destination(connector):
        message = f"{connector.__name__} is no destination: decorate it with @destination"
        raise TypeError(message)
    if not callable(getattr(connector, "published", None)):
        message = f"{connector.__name__} has no published(table): it reads nothing back"
        raise TypeError(message)
    return _Probed(connector, "destination")


def serve(*connectors: type | _Probed, args: Sequence[str] | None = None) -> int:
    """Serves `connectors` as a connector binary does, until its host stops it; the exit status.

    `connectors` are a class `@source` made, a class `@destination` made, or one of each, whose
    roles one process serves, as the Rust SDK's `Served::new().with_source(..)
    .with_destination(..)` does; `args` are the arguments after the program's name, the process's
    own by default. A launcher ends with `raise SystemExit(serve(Tickets))`. Spawned by a host it
    serves the socket `--rdlt-fd` names; run with `--listen` and the TLS flags it serves hosts
    over mutual TLS. It runs on the main thread, and answers Ctrl-C as a connector binary does:
    spawned, it ignores it, since its host stops it; listening, it stops. Arguments it cannot read
    are said on its standard error before it serves, and fail it.

    A class answers no certification probe, as a Rust binary's plain factory answers none. One
    the launcher hands it as `acknowledging_source_factory(C)` or `readable_destination_factory(C)`
    answers those its class can, as a Rust binary's `main` serving those factories does.

    Raises:
        TypeError: no class is given, two of one role are, or one neither decorator made.
    """
    roles = [_role_of(made) for made in connectors]
    unmade = [_named(made) for made, role in zip(connectors, roles, strict=True) if role is None]
    if unmade or not connectors:
        message = (
            f"{', '.join(unmade) or 'nothing'} is no connector: serve() takes a class @source "
            "made, a class @destination made, or one of each"
        )
        raise TypeError(message)
    served = [role for role in roles if role is not None]
    source = _one(served, "source")
    destination = _one(served, "destination")
    # Held while it serves: the native factories reach what keeps their instances weakly.
    sourced = None if source is None else (placed_source(source[0]), source[1])
    destined = None if destination is None else (placed_destination(destination[0]), destination[1])
    arguments = list(sys.argv[1:] if args is None else args)
    # The server takes the signals itself, and calls the handler it found after its own: Python's
    # would raise `KeyboardInterrupt` once the server returned.
    held = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        said = _native.serve(
            arguments,
            None if sourced is None else (sourced[0][0], sourced[1]),
            None if destined is None else (destined[0][0], destined[1]),
        )
    finally:
        if held is not None:
            signal.signal(signal.SIGINT, held)
    return 0 if said is None else _failed(said)


def installed(served: object) -> int:
    """Serves what an installed connector's entry point names: a class, or a pair of one of each.

    The launcher `rdlt connector install` writes calls it, so it serves no certification probe, as
    a Rust release serves none: a probed factory, alone or in the tuple, is for a launcher's own
    code.

    Raises:
        TypeError: the entry point names anything but a class or a tuple of classes.
    """
    classes = cast("tuple[object, ...]", served if isinstance(served, tuple) else (served,))
    unserved = [_named(made) for made in classes if not isinstance(made, type)]
    if unserved:
        message = (
            f"the entry point names {', '.join(unserved)}: an installed connector serves a class, "
            "or a tuple of a source class and a destination class, and a probed factory is served "
            "by a launcher's own code alone"
        )
        raise TypeError(message)
    return serve(*cast("tuple[type, ...]", classes))


def _named(made: object) -> str:
    """How `made` is named where it is refused: a class by its name, a probed one as it was made."""
    if isinstance(made, _Probed):
        made_by = (
            "acknowledging_source_factory"
            if made.role == "source"
            else "readable_destination_factory"
        )
        return f"{made_by}({made.connector.__name__})"
    return str(getattr(made, "__name__", repr(made)))


def _role_of(made: object) -> tuple[type, str, bool] | None:
    """`made`'s class, the role it serves and whether its probes are; none where it serves none."""
    if isinstance(made, _Probed):
        return made.connector, made.role, True
    if isinstance(made, type) and declares_source(made):
        return made, "source", False
    if isinstance(made, type) and declares_destination(made):
        return made, "destination", False
    return None


def _one(served: Sequence[tuple[type, str, bool]], role: str) -> tuple[type, bool] | None:
    """The class of `served` that serves `role`, with whether its probes are, where one does.

    Raises:
        TypeError: two of them serve it.
    """
    found = [(made, probed) for made, serves, probed in served if serves == role]
    if len(found) > 1:
        named = " and ".join(made.__name__ for made, _ in found)
        message = f"{named} are both a {role}: serve() serves one of each role"
        raise TypeError(message)
    return found[0] if found else None


def _failed(said: str) -> int:
    """Says on standard error why serving failed, and answers the exit status that fails."""
    print(said, file=sys.stderr)  # noqa: T201 - a connector reports why it failed on its standard error
    return 1
