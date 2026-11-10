"""The pytest plugin: a `Certification` bound to a `test_*` name is one test per clause.

A clause the connector breaks fails its test, as does one that went unobserved: a certification
is complete only once every clause that applies was seen to be met. A clause that does not apply,
by what the connector declares, skips.
"""

from __future__ import annotations

from typing import Any

import pytest

from rdlt.testing import Certification, ClauseResult


class ClauseFailed(Exception):  # noqa: N818 - a clause's failure, reported as its test's
    """A clause the connector breaks, or one that went unobserved."""

    def __init__(self, result: ClauseResult) -> None:
        super().__init__(result.clause)
        self.result = result

    def said(self) -> str:
        """The clause and its statement, then why its test failed."""
        result = self.result
        if result.outcome == "unobserved":
            return (
                f"{result.clause}: {result.statement}\n"
                f"it went unobserved: {result.reason}\n"
                "certifying with a fixture that shows this behaviour observes it"
            )
        return f"{result.clause}: {result.statement}\n{result.reason}"


class ClauseItem(pytest.Item):
    """One clause of a certification."""

    def __init__(
        self,
        *,
        certification: Certification,
        clause: str,
        **kwargs: Any,  # noqa: ANN401 - pytest's nodes pass what they pass
    ) -> None:
        super().__init__(**kwargs)  # pyright: ignore[reportUnknownMemberType]
        self.certification = certification
        self.clause = clause

    def runtest(self) -> None:
        """Runs the clause, reading its result from the certification's one run."""
        result = self.certification.result(self.clause)
        if result.outcome in {"failed", "unobserved"}:
            raise ClauseFailed(result)
        if result.outcome == "inapplicable":
            pytest.skip(f"{self.clause} does not apply: {result.reason}")

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: Any = None,  # noqa: ANN401 - pytest's own
    ) -> Any:  # noqa: ANN401 - pytest's own
        """The clause and its statement, then why it failed."""
        if isinstance(excinfo.value, ClauseFailed):
            return excinfo.value.said()
        return super().repr_failure(excinfo, style)

    def reportinfo(self) -> tuple[Any, int, str]:
        """The clause, where pytest reports the test."""
        return self.path, 0, f"certify {self.clause}"


def pytest_pycollect_makeitem(
    collector: pytest.Collector, name: str, obj: object
) -> list[ClauseItem] | None:
    """Expands a certification into its clauses' items."""
    if not isinstance(obj, Certification) or not name.startswith("test"):
        return None
    return [
        ClauseItem.from_parent(  # pyright: ignore[reportUnknownMemberType]
            collector, name=f"{name}[{clause}]", certification=obj, clause=clause
        )
        for clause, _ in obj.clauses
    ]
