# pyright: reportPrivateUsage=false, reportUnusedFunction=false
"""The exceptions rdlt raises: `Error`, and one subclass for each kind of failure."""

from __future__ import annotations

from rdlt.model import ErrorReport, Outcome, _parse


class Error(Exception):
    """A failure building, checking or running a pipeline, as the facade reports it."""

    def __init__(self, report: ErrorReport, outcome: Outcome | None = None) -> None:
        super().__init__(report.message)
        self._report = report
        self._outcome = outcome

    def __str__(self) -> str:
        """The failure's message, and what it came from where that says more."""
        report = self._report
        first = report.causes[0] if report.causes else None
        if first is None or first in report.message:
            return report.message
        return f"{report.message}: {first}"

    def __reduce__(self) -> tuple[type[Error], tuple[ErrorReport, Outcome | None]]:
        return type(self), (self._report, self._outcome)

    @property
    def outcome(self) -> Outcome | None:
        """What the run committed before the error ended it; `None` outside a run."""
        return self._outcome

    @property
    def kind(self) -> str:
        """The failure's kind, as the facade names it."""
        return self._report.kind

    @property
    def code(self) -> str | None:
        """The engine's code for the failure."""
        return self._report.code

    @property
    def stream(self) -> str | None:
        """The stream the failure belongs to, where it belongs to one."""
        return self._report.stream

    @property
    def retryable(self) -> bool:
        """Whether a new attempt could have succeeded."""
        return self._report.retryable

    def report(self) -> ErrorReport:
        """The whole report: who failed, the connector's code, and every cause."""
        return self._report


class ConfigError(Error):
    """The configuration is invalid, or a connector refused it."""


class SchemaError(Error):
    """Data does not match its table's schema."""


class SourceError(Error):
    """The source failed: a Python source's exception among the ways."""


class DestinationError(Error):
    """The destination failed."""


class TransformError(Error):
    """A transform failed: a Python transform's exception among the ways."""


class FencedError(Error):
    """A newer run opened the pipeline; this run may not commit."""


class MemoryBudgetError(Error):
    """The memory budget had no room for what the run holds, for as long as a request waits."""


class StoppedError(Error):
    """The run was stopped or cancelled before it finished."""


class InternalError(Error):
    """A bug in rdlt."""


_CLASSES: dict[str, type[Error]] = {
    "config": ConfigError,
    "schema": SchemaError,
    "source": SourceError,
    "destination": DestinationError,
    "transform": TransformError,
    "fenced": FencedError,
    "memory": MemoryBudgetError,
    "cancelled": StoppedError,
    "internal": InternalError,
}


def _from_report(report: str, outcome: str | None = None) -> Error:
    """The exception `report`, an `ErrorReport` as JSON, describes, with the run's outcome."""
    parsed = ErrorReport.from_fields(_parse(report))
    ended = None if outcome is None else Outcome.from_fields(_parse(outcome))
    return _CLASSES.get(parsed.kind, Error)(parsed, ended)
