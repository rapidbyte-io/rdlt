"""Python connectors served with `--listen`, certified at their endpoints over mutual TLS."""

from __future__ import annotations

from pathlib import Path

import pytest

import rdlt
from rdlt.testing import certify_destination, certify_source
from tests.clauses import (
    DESTINATION_PASSED,
    DESTINATION_UNMET,
    KILLED_PAGES,
    SOURCE_PASSED,
    outcomes,
)
from tests.connectors.tickets import Tickets
from tests.launchers import TESTS
from tests.tls import listening, pki

DESTINATION = TESTS.parent / "examples" / "python" / "destination"
"""The destination example's directory, which its warehouse is imported from."""


def test_a_source_served_with_its_probe_is_certified_at_its_endpoint_s_ack_included(
    tmp_path: Path,
) -> None:
    found = pki(tmp_path)
    offsets = str(tmp_path / "offsets.db")
    with listening(
        found, "tests.connectors.inbox", "acknowledging_source_factory(Inbox)"
    ) as endpoint:
        certification = certify_source(
            rdlt.Connector("io.example.inbox", endpoint=endpoint),
            config={"messages": KILLED_PAGES * 10, "offsets": offsets},
            remote=found.host,
        )
        assert certification.report().connector == "io.example.inbox"
    assert outcomes(certification) == {"passed": [*SOURCE_PASSED[:-1], "S-ACK", "K-SOURCE"]}
    # Its connections cut, as a host cuts a connector it cannot kill.
    killed = certification.result("K-SOURCE")
    assert killed.note is not None
    assert killed.note.startswith("cut: "), killed


def test_a_destination_served_with_its_probe_is_certified_at_its_endpoint_read_back_included(
    tmp_path: Path,
) -> None:
    found = pki(tmp_path)
    with listening(
        found, "tests.connectors.pysqlite", "readable_destination_factory(PySqlite)"
    ) as endpoint:
        certification = certify_destination(
            rdlt.Connector("io.example.pysqlite", endpoint=endpoint),
            config={"path": str(tmp_path / "listening.db")},
            remote=found.host,
        )
        assert certification.report().connector == "io.example.pysqlite"
    found_outcomes = outcomes(certification)
    assert set(found_outcomes) == {"passed", "inapplicable"}, found_outcomes
    assert found_outcomes["inapplicable"] == DESTINATION_UNMET, found_outcomes
    assert certification.result("D-COMMIT").outcome == "passed"


def test_a_destination_writing_every_mode_passes_every_clause_at_its_endpoint(
    tmp_path: Path,
) -> None:
    # Its writers are called at once by the served SDK's write tasks, as a host's are.
    found = pki(tmp_path)
    served = "readable_destination_factory(Warehouse)"
    with listening(found, "warehouse", served, beside=DESTINATION) as endpoint:
        certification = certify_destination(
            rdlt.Connector("io.example.warehouse", endpoint=endpoint),
            config={"path": str(tmp_path / "listening.db")},
            remote=found.host,
        )
        # A certification runs as its report is first asked for: while the warehouse listens.
        report = certification.report()
    unpassed = {
        result.clause: result.reason for result in report.results if result.outcome != "passed"
    }
    expected = {"passed": DESTINATION_PASSED, "inapplicable": ["P-CREDIT"]}
    assert outcomes(certification) == expected, unpassed


def test_a_class_served_as_it_is_answers_no_probe_at_its_endpoint(tmp_path: Path) -> None:
    found = pki(tmp_path)
    offsets = str(tmp_path / "offsets.db")
    with listening(found, "tests.connectors.inbox", "Inbox") as endpoint:
        certification = certify_source(
            rdlt.Connector("io.example.inbox", endpoint=endpoint),
            config={"messages": 60, "offsets": offsets},
            remote=found.host,
        )
        told = certification.result("S-ACK")
    assert told.outcome == "inapplicable", told


@pytest.mark.parametrize(
    "reference",
    [
        rdlt.Connector("io.example.tickets"),
        rdlt.Connector("io.example.tickets", endpoint="grpcs://localhost:1", config={"pages": 1}),
        rdlt.Connector("io.example.tickets", endpoint="grpcs://localhost:1", version="^1"),
    ],
    ids=["no endpoint", "a configuration", "a version"],
)
def test_a_listening_connector_is_named_by_its_id_and_its_endpoint_alone(
    reference: rdlt.Connector, tmp_path: Path
) -> None:
    with pytest.raises(TypeError, match="endpoint"):
        certify_source(reference, config={}, remote=pki(tmp_path).host)


def test_a_connector_served_in_this_process_is_reached_with_no_remote(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="remote"):
        certify_source(Tickets, config={}, remote=pki(tmp_path).host)
