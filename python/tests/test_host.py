"""A host allows what its operator says, and a pipeline built against it gets no more."""

from __future__ import annotations

import gc
import os
import subprocess
import sys
import textwrap
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest

import rdlt
from tests.documents import generated

NO_SANDBOX = ("sandbox_missing", "sandbox_unavailable", "sandbox_unsupported")
"""The codes a build answers with where no sandbox can be made here."""


def without_sandbox(why: str) -> None:
    """Skips a test that needs a sandbox, or fails it where one is required: CI must not skip."""
    if os.environ.get("RDLT_REQUIRE_SANDBOX", "0") != "0":
        pytest.fail(f"a sandbox is required here, and none can be made: {why}")
    pytest.skip(f"no sandbox here: {why}")


def test_a_document_builds_and_checks_against_the_host(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    document = generated("checked", 10, tmp_path / "out.db")
    pipeline = rdlt.Pipeline.from_document(document).build(host)
    pipeline.check()
    assert pipeline.document()["pipeline"] == "checked"


def test_a_write_outside_every_writable_root_is_refused(tmp_path: Path, connectors: Path) -> None:
    (tmp_path / "elsewhere").mkdir()
    # Sandboxed: a trusted binary has the host's own access, so nothing it asks for is checked.
    host = rdlt.Host(connectors=connectors, writable=[tmp_path / "elsewhere"])
    document = generated("outside", 10, tmp_path / "out.db")
    with pytest.raises(rdlt.ConfigError) as refused:
        rdlt.Pipeline.from_document(document).build(host)
    if refused.value.code in NO_SANDBOX:
        without_sandbox(str(refused.value))
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
    try:
        rdlt.Pipeline.from_document(document).build(host)
    except rdlt.Error as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))


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


class SlowPath:
    """A path whose text takes a moment to give, as one read from a network mount does."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def __fspath__(self) -> str:
        """Gives the path, the GIL released for a moment first."""
        time.sleep(0.02)
        return str(self._path)


def test_concurrent_first_builds_of_a_host_get_one_handle(tmp_path: Path) -> None:
    host = rdlt.Host(connectors=SlowPath(tmp_path), writable=[tmp_path])
    barrier = threading.Barrier(8)

    def first_build() -> object:
        barrier.wait(30)
        return host._native()  # pyright: ignore[reportPrivateUsage]

    with ThreadPoolExecutor(8) as pool:
        handles = [built.result(30) for built in [pool.submit(first_build) for _ in range(8)]]
    assert all(handle is handles[0] for handle in handles)


def test_a_host_takes_a_build_wait() -> None:
    assert rdlt.Host(build_wait=timedelta(seconds=30)).build_wait == timedelta(seconds=30)


class Holder:
    """A secret store that holds the pipeline it serves, as a job object's method would."""

    pipeline: rdlt.Pipeline | None = None

    def store(self, name: str) -> str | None:
        """Answers the one secret it has."""
        return {"only": "text"}.get(name)


def test_a_secret_store_holding_its_own_pipeline_is_collected(
    tmp_path: Path, connectors: Path
) -> None:
    holder = Holder()
    host = rdlt.Host(
        connectors=connectors, writable=[tmp_path], trust_binaries=True, secret_store=holder.store
    )
    document = generated("cycle", 1, tmp_path / "out.db")
    holder.pipeline = rdlt.Pipeline.from_document(document).build(host)
    collected = weakref.ref(holder)
    del holder, host
    gc.collect()
    assert collected() is None


def test_a_built_pipeline_and_its_run_keep_the_secret_store(
    tmp_path: Path, connectors: Path
) -> None:
    holder = Holder()
    host = rdlt.Host(
        connectors=connectors, writable=[tmp_path], trust_binaries=True, secret_store=holder.store
    )
    document = generated("kept", 1, tmp_path / "out.db")
    pipeline = rdlt.Pipeline.from_document(document).build(host)
    kept = weakref.ref(holder)
    del holder, host
    gc.collect()
    assert kept() is not None, "a built pipeline restarts its connectors with the store"
    run = pipeline.start()
    del pipeline
    gc.collect()
    assert kept() is not None, "a run in progress restarts its connectors with the store"
    run.wait()


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


def test_a_first_build_held_at_a_fork_leaves_the_child_refused_not_hung(
    tmp_path: Path, connectors: Path
) -> None:
    script = textwrap.dedent(
        f"""
        import os, pathlib, signal, sys, warnings
        warnings.simplefilter("ignore")
        import rdlt, rdlt.host
        sys.path.insert(0, {str(Path(__file__).parents[1])!r})
        from tests.documents import generated
        settings = dict(
            connectors={str(connectors)!r},
            writable=[{str(tmp_path)!r}],
            trust_binaries=True,
        )
        document = generated("forked", 1, pathlib.Path({str(tmp_path / "out.db")!r}))
        rdlt.Pipeline.from_document(document).build(rdlt.Host(**settings))
        fresh = rdlt.Host(**settings)
        rdlt.host._first_build.acquire()
        child = os.fork()
        if child == 0:
            signal.alarm(20)
            try:
                rdlt.Pipeline.from_document(document).build(fresh)
            except rdlt.InternalError:
                os._exit(0)
            os._exit(3)
        rdlt.host._first_build.release()
        _, status = os.waitpid(child, 0)
        sys.exit(os.waitstatus_to_exitcode(status))
        """
    )
    finished = subprocess.run(
        [sys.executable, "-c", script], timeout=60, check=False, capture_output=True
    )
    assert finished.returncode == 0, finished.stderr
