# pyright: reportPrivateUsage=false
"""Certify connectors and transforms in pytest: each clause a test of its own."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from os import PathLike
from typing import TYPE_CHECKING

from rdlt import _native
from rdlt._pipeline import Connector
from rdlt._transform import Transform
from rdlt.connector import _destination, _placed
from rdlt.host import Tls
from rdlt.model import Json

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True, slots=True)
class ClauseResult:
    """One clause's result."""

    clause: str
    statement: str
    outcome: str
    """`passed`, `failed`, `inapplicable` (it does not apply, by what was declared) or
    `unobserved` (it applies, and what it requires was not seen)."""
    reason: str | None
    note: str | None


@dataclass(frozen=True, slots=True)
class Report:
    """What a certification found, as `rdlt_certify`'s `Report` says it.

    `connector` is the connector's id, and `results` each clause's result, in clause order.
    """

    connector: str
    results: tuple[ClauseResult, ...]


class Certification:
    """A certification, as pytest holds it: run once, when its first clause's test runs."""

    def __init__(self, role: str, run: Callable[[], str], held: object = None) -> None:
        self.clauses: list[tuple[str, str]] = _native.clauses(role)
        self._run = run
        # What the native side reaches only weakly, held until the certification runs.
        self._held = held
        self._report: Report | None = None

    def report(self) -> Report:
        """The certification's report, running it if it has not run."""
        if self._report is None:
            found = json.loads(self._run())
            results = tuple(
                ClauseResult(
                    entry["clause"],
                    entry["statement"],
                    entry["outcome"],
                    entry["reason"],
                    entry["note"],
                )
                for entry in found["results"]
            )
            self._report = Report(found["connector"], results)
        return self._report

    def result(self, clause: str) -> ClauseResult:
        """`clause`'s result, running the certification if it has not run.

        Raises:
            KeyError: the certification holds no clause `clause`.
        """
        results = self.report().results
        for result in results:
            if result.clause == clause:
                return result
        held = ", ".join(result.clause for result in results)
        message = f"no clause {clause!r} in this certification, whose clauses are {held}"
        raise KeyError(message)


def certify_source(
    connector: type | Connector,
    *,
    config: Mapping[str, Json],
    launcher: str | PathLike[str] | None = None,
    remote: Tls | None = None,
) -> Certification:
    """The source clauses, and the protocol's, against `connector`.

    A class `@source` made is served over an in-process socket pair. An `rdlt.Connector` naming
    an id and an `endpoint`, `grpcs://host:port`, is the connector listening there, reached over
    mutual TLS with `remote`, the host's certificate and key and the CA the connector's certificate
    chains to, as rdlt_certify's `Target::listening(remote, reference)` reaches it. `S-ACK` asks
    the connector where it stands, which a Python connector answers there where its launcher
    serves it as `acknowledging_source_factory(C)`.

    The kill clause spawns `launcher`, the installed connector, where it is given, and cuts the
    connections otherwise. It observes a kill only in a load that outlasts several commits, about
    a megabyte of rows, and a smaller one leaves it unobserved, which fails its test.

    Raises:
        TypeError: a reference that names no endpoint, or names what is no part of one, or a
            class given `remote`.
    """
    if isinstance(connector, Connector):
        listening = _listening(connector, remote)
        return Certification(
            "source", lambda: _native.certify_source(listening, json.dumps(config), launcher)
        )
    _served(connector, remote)
    made, placement = _placed.placed(connector)
    return Certification(
        "source", lambda: _native.certify_source(made, json.dumps(config), launcher), placement
    )


def certify_destination(
    connector: type | Connector,
    *,
    config: Mapping[str, Json],
    launcher: str | PathLike[str] | None = None,
    remote: Tls | None = None,
) -> Certification:
    """The destination clauses, and the protocol's, against `connector`.

    A class `@destination` made is served over an in-process socket pair and read back through
    its `published`. An `rdlt.Connector` naming an id and an `endpoint` is the connector listening
    there, reached over mutual TLS with `remote`, as `certify_source` reaches one, and read back
    through it where it reads back, as a Python connector does there where its launcher serves it
    as `readable_destination_factory(C)`; otherwise the clauses that compare what it published go
    unobserved, which fails their tests.

    The kill clause spawns `launcher`, the installed connector, where it is given, and cuts the
    connections otherwise. It loads rows of its own and reads them back, so it observes a kill
    only where they are read back and, with a launcher, kept where a process outliving the killed
    one finds them, as a file or a database keeps them; otherwise it goes unobserved or fails,
    which fails its test.

    Raises:
        TypeError: a reference that names no endpoint, or names what is no part of one, or a
            class given `remote`.
    """
    if isinstance(connector, Connector):
        listening = _listening(connector, remote)
        return Certification(
            "destination",
            lambda: _native.certify_destination(listening, json.dumps(config), launcher),
        )
    _served(connector, remote)
    made, placement = _destination.placed(connector)
    return Certification(
        "destination",
        lambda: _native.certify_destination(made, json.dumps(config), launcher),
        placement,
    )


def _listening(reference: Connector, remote: Tls | None) -> tuple[str, str, Tls]:
    """The connector `reference` names listening at its endpoint, reached with `remote`.

    Raises:
        TypeError: it names no endpoint, or names what is no part of one, or `remote` is none.
    """
    if reference.endpoint is None or remote is None:
        message = "a listening connector is certified at its endpoint=, reached with remote="
        raise TypeError(message)
    named = {
        "config": reference.config,
        "access": reference.access,
        "version": reference.version,
        "digest": reference.digest,
    }
    unread = [name for name, value in named.items() if value is not None]
    if unread:
        message = (
            f"a listening connector is named by its id and endpoint alone, not its "
            f"{' or '.join(unread)}: its configuration is config="
        )
        raise TypeError(message)
    return reference.reference, reference.endpoint, remote


def _served(connector: type, remote: Tls | None) -> None:
    """Refuses `remote` for `connector`, a class served in this process.

    Raises:
        TypeError: `remote` is given.
    """
    if remote is not None:
        message = (
            f"{connector.__name__} is served in this process: remote= reaches a connector "
            "listening at an endpoint, named by an rdlt.Connector"
        )
        raise TypeError(message)


def certify_transform(transform: Transform, *, inputs: Sequence[pa.RecordBatch]) -> Certification:
    """The `T` clauses against `transform`, over `inputs` and the batches drawn from them."""
    return Certification(
        "transform", lambda: _native.certify_transform(transform._to_native(), list(inputs))
    )


__all__ = [
    "Certification",
    "ClauseResult",
    "Report",
    "certify_destination",
    "certify_source",
    "certify_transform",
]
