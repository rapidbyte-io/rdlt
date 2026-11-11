"""What a certification found, clause by clause."""

from __future__ import annotations

from rdlt.testing import Certification


def outcomes(certification: Certification) -> dict[str, list[str]]:
    """Each outcome's clauses, in clause order, every reason a clause did not apply given."""
    found: dict[str, list[str]] = {}
    for clause, _ in certification.clauses:
        result = certification.result(clause)
        if result.outcome == "inapplicable":
            assert result.reason, clause
        found.setdefault(result.outcome, []).append(clause)
    return found
