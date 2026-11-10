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


class Certification:
    """A certification, run once when its first clause's test runs."""

    def __init__(self, role: str, run: Callable[[], str], held: object = None) -> None:
        self.clauses: list[tuple[str, str]] = _native.clauses(role)
        self._run = run
        # What the native side reaches only weakly, held until the certification runs.
        self._held = held
        self._results: dict[str, ClauseResult] | None = None

    def result(self, clause: str) -> ClauseResult:
        """`clause`'s result, running the certification if it has not run."""
        if self._results is None:
            report = json.loads(self._run())
            self._results = {
                entry["clause"]: ClauseResult(
                    entry["clause"],
                    entry["statement"],
                    entry["outcome"],
                    entry["reason"],
                    entry["note"],
                )
                for entry in report["results"]
            }
        return self._results[clause]


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
    "certify_destination",
    "certify_source",
    "certify_transform",
]
