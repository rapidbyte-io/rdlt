"""What a certification found, clause by clause."""

from __future__ import annotations

from rdlt.testing import Certification

KILLED_PAGES = 4_000
"""Pages of ten tickets a kill clause's load reads: about 40,000 rows, a megabyte, as
rdlt-certify's own kill fixtures are, a load that outlasts the commits K-SOURCE's kills follow, so
they land while the source is still read."""

SOURCE_UNTOLD = ["S-ACK"]
"""The clauses a source that does not tell where it stands outside the engine is not held to."""

SOURCE_PASSED = [
    "P-HANDSHAKE",
    "P-ORDER",
    "P-ROLE",
    "P-LIMITS",
    "P-HEARTBEAT",
    "P-MALFORMED",
    "P-CREDIT",
    "S-CHECK",
    "S-DISCOVER",
    "S-PLAN",
    "S-RESUME",
    "S-PARTITION",
    "S-STOP",
    "S-BARRIER",
    "K-SOURCE",
]
"""The clauses the tickets source passes, which every source that reads a partition that ends,
checkpointing when asked, passes."""

DESTINATION_UNMET = [
    "P-CREDIT",
    "D-REPLACE",
    "D-MERGE",
    "D-DELETE",
    "D-PARTIAL",
    "D-TRUNCATE",
    "D-HIST",
    "D-CHILDREN",
    "D-WRITERS",
    "D-DROP",
]
"""The clauses the append-only sqlite3 destination declares it does not meet, each saying why.

A destination grants credit and spends none; the rest are what its capabilities leave out.
"""


def outcomes(certification: Certification) -> dict[str, list[str]]:
    """Each outcome's clauses, in clause order, every reason a clause did not apply given."""
    found: dict[str, list[str]] = {}
    for result in certification.report().results:
        if result.outcome == "inapplicable":
            assert result.reason, result.clause
        found.setdefault(result.outcome, []).append(result.clause)
    return found
