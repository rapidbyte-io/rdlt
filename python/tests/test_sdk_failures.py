"""A connector's declared failures reach the engine as their kinds, with their retry hints."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

import rdlt
from rdlt.connector import InvalidData, RateLimited, Transient


def into_sqlite(tmp_path: Path) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(tmp_path / "out.db")},
        access=rdlt.Access(write=[tmp_path]),
    )


def test_a_rate_limit_s_retry_after_is_the_engine_s_retry_delay(
    tmp_path: Path, connectors: Path
) -> None:
    calls: list[int] = []

    @rdlt.connector.stream()
    def limited() -> Iterator[object]:
        calls.append(1)
        if len(calls) == 1:
            raise RateLimited("HTTP 429", retry_after=2.0, code="http_429")
        yield {"n": 1}

    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    run = (
        rdlt.pipeline(
            "limited",
            source=limited,
            destination=into_sqlite(tmp_path),
            retry=rdlt.Retry(max_attempts=3),
        )
        .build(host)
        .start()
    )
    events = run.events()
    assert run.wait().report.status is rdlt.RunStatus.SUCCEEDED
    retries = [
        event.detail
        for event in events
        if isinstance(event, rdlt.Event) and event.kind == "retrying"
    ]
    assert retries, "the rate limit was retried"
    waited = retries[0]["retry_in"]
    assert isinstance(waited, dict)
    seconds = waited["secs"]
    assert isinstance(seconds, int)
    assert seconds >= 2


def test_invalid_data_fails_the_run_as_data_and_is_not_retried(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.connector.stream()
    def malformed() -> Iterator[object]:
        raise InvalidData("row 7 has no id", code="missing_id")
        yield {}

    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    built = rdlt.pipeline(
        "malformed",
        source=malformed,
        destination=into_sqlite(tmp_path),
        retry=rdlt.Retry(max_attempts=3),
    )
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host).run()
    report = raised.value.report()
    assert report.connector_code == "missing_id"
    assert report.causes == ("row 7 has no id",)
    assert not raised.value.retryable


@pytest.mark.parametrize("code", ["HTTP 429", "Http429", "", "rate limited", "naïve"])
def test_a_failure_s_code_is_one_the_protocol_admits(code: str) -> None:
    with pytest.raises(ValueError, match="code"):
        RateLimited("slow down", code=code)


def test_a_code_of_lower_case_letters_digits_and_separators_is_kept() -> None:
    assert Transient("the API is restarting", code="api.down-1_2").code == "api.down-1_2"


@pytest.mark.parametrize("code", ["connector_lost", "deadline_exceeded", "tls", "transport"])
def test_a_failure_may_not_take_a_code_the_host_keeps_for_itself(code: str) -> None:
    with pytest.raises(ValueError, match="host"):
        Transient("the API is restarting", code=code)
