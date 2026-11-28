"""The pytest plugin: a `Certification` bound to a `test_*` name is a test per clause and verdict.

A clause the connector breaks fails its test, as does one that went unobserved: a certification
is complete only once every clause that applies was seen to be met. A clause that does not apply,
by what the connector declares, skips. The verdict's test, `[verdict]`, fails where the report's
verdict is not `passed`, as `rdlt certify` exits 1: so a certification that certified nothing, as
one of a role the connector does not serve, fails though every clause skips.
"""

from __future__ import annotations

from typing import Any

import pytest

from rdlt.testing import Certification, ClauseResult, Report


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
        if result.note is not None:
            # A kill clause's note says whether a process was killed or a connection cut.
            self.add_report_section("call", "note", result.note)
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


class VerdictFailed(Exception):  # noqa: N818 - a verdict's failure, reported as its test's
    """A certification whose verdict is not `passed`."""

    def __init__(self, report: Report) -> None:
        super().__init__(report.verdict)
        self.report = report

    def said(self) -> str:
        """The verdict, what each outcome counts, and where nothing passed, that nothing was."""
        outcomes = [result.outcome for result in self.report.results]
        counted = ", ".join(
            f"{outcomes.count(outcome)} {outcome}"
            for outcome in ("passed", "failed", "unobserved", "inapplicable")
        )
        said = f"the certification of {self.report.connector} is {self.report.verdict}: {counted}"
        if "passed" not in outcomes:
            said += "\nnothing was certified: no clause passed"
        return said


class VerdictItem(pytest.Item):
    """A certification's verdict, as rdlt_certify's report says it."""

    def __init__(
        self,
        *,
        certification: Certification,
        **kwargs: Any,  # noqa: ANN401 - pytest's nodes pass what they pass
    ) -> None:
        super().__init__(**kwargs)  # pyright: ignore[reportUnknownMemberType]
        self.certification = certification

    def runtest(self) -> None:
        """Fails where the certification's verdict is not `passed`."""
        report = self.certification.report()
        if report.verdict != "passed":
            raise VerdictFailed(report)

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: Any = None,  # noqa: ANN401 - pytest's own
    ) -> Any:  # noqa: ANN401 - pytest's own
        """The verdict, and what the clauses' outcomes count."""
        if isinstance(excinfo.value, VerdictFailed):
            return excinfo.value.said()
        return super().repr_failure(excinfo, style)

    def reportinfo(self) -> tuple[Any, int, str]:
        """The verdict, where pytest reports the test."""
        return self.path, 0, "certify verdict"


def pytest_pycollect_makeitem(
    collector: pytest.Collector, name: str, obj: object
) -> list[pytest.Item] | None:
    """Expands a certification into its clauses' items, then its verdict's."""
    if not isinstance(obj, Certification) or not name.startswith("test"):
        return None
    items: list[pytest.Item] = [
        ClauseItem.from_parent(  # pyright: ignore[reportUnknownMemberType]
            collector, name=f"{name}[{clause}]", certification=obj, clause=clause
        )
        for clause, _ in obj.clauses
    ]
    verdict = VerdictItem.from_parent(  # pyright: ignore[reportUnknownMemberType]
        collector, name=f"{name}[verdict]", certification=obj
    )
    return [*items, verdict]
