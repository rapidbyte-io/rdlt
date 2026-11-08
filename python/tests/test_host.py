"""A host allows what its operator says, and a pipeline built against it gets no more."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pytest

import rdlt
from tests.documents import generated


def test_a_document_builds_and_checks_against_the_host(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    document = generated("checked", 10, tmp_path / "out.db")
    pipeline = rdlt.Pipeline.from_document(document).build(host)
    pipeline.check()
    assert pipeline.document()["pipeline"] == "checked"


@pytest.mark.skipif(sys.platform != "linux", reason="only Linux has the sandbox that checks roots")
def test_a_write_outside_every_writable_root_is_refused(tmp_path: Path, connectors: Path) -> None:
    (tmp_path / "elsewhere").mkdir()
    # Sandboxed: a trusted binary has the host's own access, so nothing it asks for is checked.
    host = rdlt.Host(connectors=connectors, writable=[tmp_path / "elsewhere"])
    document = generated("outside", 10, tmp_path / "out.db")
    with pytest.raises(rdlt.ConfigError) as refused:
        rdlt.Pipeline.from_document(document).build(host)
    assert refused.value.code == "grant_outside"


def test_a_yaml_document_builds_from_its_path(tmp_path: Path, connectors: Path) -> None:
    database = tmp_path / "out.db"
    path = tmp_path / "rows.yaml"
    path.write_text(
        "version: 1\n"
        "pipeline: from-path\n"
        "source:\n"
        "  connector: io.rapidbyte.generator\n"
        "  config: { seed: 1, streams: [{ name: rows, rows: 5 }] }\n"
        "destination:\n"
        "  connector: io.rapidbyte.sqlite\n"
        f"  config: {{ path: {database} }}\n"
        f"  access: {{ write: [{tmp_path}] }}\n"
        "streams: [{ name: rows }]\n"
    )
    host = rdlt.Host(connectors=[connectors], writable=[tmp_path], trust_binaries=True)
    rdlt.Pipeline.from_path(path).build(host).check()


def test_a_document_that_cannot_be_read_is_refused_as_unreadable(
    tmp_path: Path, connectors: Path
) -> None:
    host = rdlt.Host(connectors=connectors, trust_binaries=True)
    with pytest.raises(rdlt.ConfigError) as refused:
        rdlt.Pipeline.from_path(tmp_path / "absent.yaml").build(host)
    assert refused.value.code == "document_unreadable"


def test_a_document_of_another_version_is_refused(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, trust_binaries=True)
    document = generated("versioned", 1, tmp_path / "out.db")
    document["version"] = 2
    with pytest.raises(rdlt.ConfigError) as refused:
        rdlt.Pipeline.from_document(document).build(host)
    assert refused.value.code == "document_version"


def test_a_name_that_is_no_text_is_refused() -> None:
    host = rdlt.Host(allow_env=[1])  # type: ignore[list-item]  # pyright: ignore[reportArgumentType]
    with pytest.raises(TypeError):
        rdlt.Pipeline.from_document({}).build(host)


def test_a_named_secret_resolves_through_the_store(tmp_path: Path, connectors: Path) -> None:
    store = {"out": str(tmp_path / "out.db")}
    host = rdlt.Host(
        connectors=connectors, writable=[tmp_path], trust_binaries=True, secret_store=store.get
    )
    document = generated("stored", 10, tmp_path / "out.db")
    document["destination"] = {
        "connector": "io.rapidbyte.sqlite",
        "config": {"path": "${secret:out}"},
        "access": {"write": [str(tmp_path)]},
    }
    rdlt.Pipeline.from_document(document).build(host).check()


def test_a_sandboxed_connector_shares_the_network_only_where_the_host_allows_it(
    tmp_path: Path, connectors: Path
) -> None:
    document = generated("networked", 10, tmp_path / "out.db")
    document["source"] = {
        "connector": "io.rapidbyte.generator",
        "config": {"seed": 1, "streams": [{"name": "rows", "rows": 1}]},
        "access": {"network": True},
    }
    # The refusal comes before any connector is placed, so the sandbox itself is not needed.
    with pytest.raises(rdlt.ConfigError) as refused:
        rdlt.Pipeline.from_document(document).build(rdlt.Host(connectors=connectors))
    assert refused.value.code == "placement_refused"


def test_a_host_that_allows_the_network_does_not_refuse_it(
    tmp_path: Path, connectors: Path
) -> None:
    document = generated("networked", 10, tmp_path / "out.db")
    document["source"] = {
        "connector": "io.rapidbyte.generator",
        "config": {"seed": 1, "streams": [{"name": "rows", "rows": 1}]},
        "access": {"network": True},
    }
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], allow_network=True)
    code = None
    try:
        rdlt.Pipeline.from_document(document).build(host)
    except rdlt.Error as error:
        code = error.code
    assert code != "placement_refused"


def test_a_tls_file_that_cannot_be_read_refuses_the_build(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(
        connectors=connectors,
        trust_binaries=True,
        remote=rdlt.Tls(
            cert=tmp_path / "host.pem", key=tmp_path / "host.key", ca=tmp_path / "ca.pem"
        ),
    )
    document = generated("tls", 1, tmp_path / "out.db")
    with pytest.raises(rdlt.ConfigError) as refused:
        rdlt.Pipeline.from_document(document).build(host)
    assert refused.value.code == "tls_unreadable"


def test_a_host_builds_every_pipeline_on_the_same_native_host() -> None:
    host = rdlt.Host()
    assert host._native() is host._native()  # pyright: ignore[reportPrivateUsage]


def test_a_host_takes_a_build_wait() -> None:
    assert rdlt.Host(build_wait=timedelta(seconds=30)).build_wait == timedelta(seconds=30)


def test_a_host_shows_nothing_of_its_secret_store() -> None:
    host = rdlt.Host(secret_store={"name": "hunter2-secret"}.get)
    assert "hunter2-secret" not in repr(host)


def test_a_single_path_or_name_is_one_entry_not_its_letters() -> None:
    host = rdlt.Host(writable="/data", allow_env="DB_PASSWORD", allow_files="/run/secrets")
    assert host.writable == ("/data",)
    assert host.allow_env == ("DB_PASSWORD",)
    assert host.allow_files == ("/run/secrets",)


def test_a_builder_and_a_pipeline_show_only_their_name(tmp_path: Path, connectors: Path) -> None:
    document = generated("named", 1, tmp_path / "out.db")
    builder = rdlt.Pipeline.from_document(document)
    assert repr(builder) == "PipelineBuilder('named')"
    assert "PipelineBuilder(path=" in repr(rdlt.Pipeline.from_path("orders.yaml"))
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    assert repr(builder.build(host)) == "Pipeline('named')"
