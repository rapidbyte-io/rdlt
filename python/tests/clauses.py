"""What a certification found, clause by clause."""

from __future__ import annotations

from rdlt.testing import Certification


def outcomes(certification: Certification) -> dict[str, list[str]]:
    """Each outcome's clauses, in clause order, every reason a clause did not apply given."""
    found: dict[str, list[str]] = {}
    for result in certification.report().results:
        if result.outcome == "inapplicable":
            assert result.reason, result.clause
        found.setdefault(result.outcome, []).append(result.clause)
    return found
