"""The pytest plugin: a `Certification` bound to a `test_*` name is one test per clause."""

from __future__ import annotations

from typing import Any

import pytest

from rdlt.testing import Certification, ClauseResult


class ClauseFailed(Exception):  # noqa: N818 - a clause's failure, reported as its test's
    """A clause the connector breaks."""

    def __init__(self, result: ClauseResult) -> None:
        super().__init__(result.clause)
        self.result = result


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
        if result.outcome == "failed":
            raise ClauseFailed(result)
        if result.outcome != "passed":
            pytest.skip(f"{self.clause} is {result.outcome}: {result.reason}")

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: Any = None,  # noqa: ANN401 - pytest's own
    ) -> Any:  # noqa: ANN401 - pytest's own
        """The clause and its statement, then why it failed."""
        if isinstance(excinfo.value, ClauseFailed):
            result = excinfo.value.result
            return f"{result.clause}: {result.statement}\n{result.reason}"
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
