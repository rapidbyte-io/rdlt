# pyright: reportPrivateUsage=false
"""Certify connectors and transforms in pytest: each clause a test of its own."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from os import PathLike
from typing import TYPE_CHECKING

from rdlt import _native
from rdlt._transform import Transform
from rdlt.connector import _destination, _source
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
    connector: type, *, config: Mapping[str, Json], launcher: str | PathLike[str] | None = None
) -> Certification:
    """The source clauses, and the protocol's, against `connector`.

    It is served over an in-process socket pair; the kill clause spawns `launcher`, the installed
    connector, where it is given, and cuts the in-process connections otherwise. The kill clause
    observes a kill only in a load that outlasts several commits, about a megabyte of rows, and a
    smaller one leaves it unobserved, which fails its test.
    """
    made, placement = _source.placed(connector)
    return Certification(
        "source", lambda: _native.certify_source(made, json.dumps(config), launcher), placement
    )


def certify_destination(
    connector: type, *, config: Mapping[str, Json], launcher: str | PathLike[str] | None = None
) -> Certification:
    """The destination clauses, and the protocol's, against `connector`.

    It is served over an in-process socket pair and read back through `published`; the kill
    clause spawns `launcher`, the installed connector, where it is given, and cuts the in-process
    connections otherwise. The kill clause loads rows of its own and reads them back in this
    process, so it observes a kill only where the class has `published` and, with a launcher,
    keeps what it published where a process outliving the killed one finds it, as a file or a
    database does; otherwise it goes unobserved or fails, which fails its test.
    """
    made, placement = _destination.placed(connector)
    return Certification(
        "destination",
        lambda: _native.certify_destination(made, json.dumps(config), launcher),
        placement,
    )


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
