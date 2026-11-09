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
from rdlt.connector._source import factory
from rdlt.model import Json

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True, slots=True)
class ClauseResult:
    """One clause's result."""

    clause: str
    statement: str
    outcome: str
    """`passed`, `failed`, `inapplicable` or `unobserved`."""
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
    connector, where it is given, and cuts the in-process connections otherwise.
    """
    made = factory(connector)
    return Certification(
        "source", lambda: _native.certify_source(made, json.dumps(config), launcher), connector
    )


def certify_transform(transform: Transform, *, inputs: Sequence[pa.RecordBatch]) -> Certification:
    """The `T` clauses against `transform`, over `inputs` and the batches drawn from them."""
    return Certification(
        "transform", lambda: _native.certify_transform(transform._to_native(), list(inputs))
    )


__all__ = ["Certification", "ClauseResult", "certify_source", "certify_transform"]
