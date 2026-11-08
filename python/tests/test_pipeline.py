"""A pipeline built in code is the document it prints, and fails as that document would."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from datetime import timedelta
from pathlib import Path

import pytest

import rdlt
from rdlt import Json
from tests.documents import generated


def built_in_code(tmp_path: Path) -> rdlt.PipelineBuilder:
    database = tmp_path / "out.db"
    return rdlt.pipeline(
        "coded",
        source=rdlt.Connector(
            "io.rapidbyte.generator",
            config={"seed": 7, "streams": [{"name": "rows", "rows": 10}]},
        ),
        destination=rdlt.Connector(
            "io.rapidbyte.sqlite",
            config={"path": str(database)},
            access=rdlt.Access(write=[tmp_path]),
        ),
        streams=[rdlt.stream("rows", write=rdlt.append())],
        commit=rdlt.Commit(every=timedelta(seconds=10), rows=1_000, size="64MiB"),
        resources=rdlt.Resources(memory="64MiB", writers=2, partitions=4),
        retry=rdlt.Retry(max_attempts=3, initial=timedelta(milliseconds=100)),
    )


def test_a_builder_is_the_document_it_prints(tmp_path: Path) -> None:
    document = built_in_code(tmp_path).document()
    assert document["pipeline"] == "coded"
    assert document["streams"] == [{"name": "rows", "write": "append"}]
    assert document["commit"] == {"every": "10s", "rows": 1000, "bytes": "64MiB"}
    assert document["resources"] == {"memory": "64MiB", "writers": 2, "partitions": 4}
    assert document["retry"] == {"max_attempts": 3, "initial": "100ms"}
    assert document["destination"] == {
        "connector": "io.rapidbyte.sqlite",
        "config": {"path": str(tmp_path / "out.db")},
        "access": {"write": [str(tmp_path)]},
    }


def test_a_builder_reaches_every_field_of_a_stream_and_of_the_document() -> None:
    document = rdlt.pipeline(
        "everything",
        source=rdlt.Connector("io.rapidbyte.generator", version="^1.2", digest="abc"),
        destination=rdlt.Connector("io.rapidbyte.sqlite", endpoint="grpcs://db.example:7443"),
        streams=[
            rdlt.stream(
                "t",
                read="incremental",
                write="append",
                deletes="soft",
                on_truncate="ignore",
                on_retention_loss="reset",
                schema=rdlt.SchemaRules(policy="freeze", on_unsupported="refuse"),
                nested={"normalize": {"max_depth": 2}},
                columns={"email": rdlt.Column(policy="discard_value", hint="utf8")},
                include=["email"],
                exclude=["x"],
                transform=[{"sql": "SELECT * FROM input"}],
            )
        ],
        defaults=rdlt.Defaults(read=rdlt.ReadMode.FULL, write=rdlt.merge("id"), nested="json"),
        until="forever",
    ).document()
    assert document["source"] == {
        "connector": {"id": "io.rapidbyte.generator", "version": "^1.2", "digest": "abc"}
    }
    assert document["destination"] == {
        "connector": {"id": "io.rapidbyte.sqlite", "endpoint": "grpcs://db.example:7443"}
    }
    assert document["streams"] == [
        {
            "name": "t",
            "read": "incremental",
            "write": "append",
            "deletes": "soft",
            "on_truncate": "ignore",
            "on_retention_loss": "reset",
            "schema": {"policy": "freeze", "on_unsupported": "refuse"},
            "nested": {"normalize": {"max_depth": 2}},
            "columns": {"email": {"policy": "discard_value", "hint": "utf8"}},
            "include": ["email"],
            "exclude": ["x"],
            "transform": [{"sql": "SELECT * FROM input"}],
        }
    ]
    assert document["defaults"] == {
        "read": "full",
        "write": {"merge": {"key": ["id"]}},
        "nested": "json",
    }
    assert document["until"] == "forever"


def test_until_takes_a_duration() -> None:
    document = rdlt.pipeline(
        "bounded",
        source=rdlt.Connector("io.rapidbyte.generator"),
        destination=rdlt.Connector("io.rapidbyte.sqlite"),
        until=timedelta(minutes=10),
    ).document()
    assert document["until"] == "600s"


def test_a_merge_and_a_history_name_their_key_and_need_one() -> None:
    merged = rdlt.pipeline(
        "merged",
        source=rdlt.Connector("io.rapidbyte.generator"),
        destination=rdlt.Connector("io.rapidbyte.sqlite"),
        streams=[
            rdlt.stream("t", write=rdlt.merge("id", "region")),
            rdlt.stream("u", write=rdlt.history("id")),
        ],
    )
    assert merged.document()["streams"] == [
        {"name": "t", "write": {"merge": {"key": ["id", "region"]}}},
        {"name": "u", "write": {"history": {"key": ["id"]}}},
    ]
    with pytest.raises(ValueError, match="key"):
        rdlt.merge()
    with pytest.raises(ValueError, match="key"):
        rdlt.history()


def test_a_built_pipeline_discovers_its_source_s_streams(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    catalog = built_in_code(tmp_path).build(host).discover()
    assert [stream.name for stream in catalog.streams] == ["rows"]
    assert rdlt.ReadMode.FULL in catalog.streams[0].read_modes


def test_a_reset_of_a_stream_never_committed_is_refused(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    pipeline = built_in_code(tmp_path).build(host)
    with pytest.raises(rdlt.ConfigError):
        pipeline.reset(["rows"])


def test_an_unknown_stream_in_the_builder_fails_the_build(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    document = generated("unknown", 10, tmp_path / "out.db")
    document["streams"] = [{"name": "absent", "write": "append"}]
    with pytest.raises(rdlt.ConfigError):
        rdlt.Pipeline.from_document(document).build(host)


def test_an_empty_include_is_a_restriction_the_plan_refuses(
    tmp_path: Path, connectors: Path
) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    document = generated("empty-include", 10, tmp_path / "out.db")
    document["streams"] = [{"name": "rows", "include": []}]
    with pytest.raises(rdlt.ConfigError):
        rdlt.Pipeline.from_document(document).build(host)


def test_a_connector_takes_its_id_alone_by_position() -> None:
    with pytest.raises(TypeError):
        rdlt.Connector("io.rapidbyte.generator", {"seed": 1})  # type: ignore[misc]  # pyright: ignore[reportCallIssue]


def test_a_connector_shows_neither_its_configuration_nor_its_endpoint() -> None:
    shown = repr(
        rdlt.Connector(
            "io.example.tickets",
            config={"token": "hunter2-secret"},
            endpoint="grpcs://user:hunter2-secret@host:7443",
        )
    )
    assert "hunter2-secret" not in shown
    assert "io.example.tickets" in shown


def test_a_write_mode_shows_what_it_writes() -> None:
    assert repr(rdlt.merge("id")) == "WriteMode({'merge': {'key': ['id']}})"
    assert repr(rdlt.append()) == "WriteMode('append')"


def test_a_stream_is_named_by_words_as_a_document_is() -> None:
    document = rdlt.stream("t", read="full", write="replace")
    assert document.read == "full"
    assert document.write == "replace"


def test_a_stream_and_its_access_are_hashable() -> None:
    assert hash(rdlt.stream("t", include=["id"], write=rdlt.merge("id"))) is not None
    assert hash(rdlt.Access(read=["/data"])) is not None


def test_a_single_column_or_path_is_one_entry_not_its_letters() -> None:
    document = rdlt.pipeline(
        "single",
        source=rdlt.Connector("io.rapidbyte.generator"),
        destination=rdlt.Connector("io.rapidbyte.sqlite", access=rdlt.Access(read="/data")),
        streams=[rdlt.stream("t", include="id", exclude="secret")],
    ).document()
    assert document["streams"] == [{"name": "t", "include": ["id"], "exclude": ["secret"]}]
    assert document["destination"] == {
        "connector": "io.rapidbyte.sqlite",
        "access": {"read": ["/data"]},
    }


def test_a_path_like_grant_is_its_path() -> None:
    class Odd:
        def __fspath__(self) -> str:
            return "/really/here"

        def __str__(self) -> str:
            return "not-a-path"

    document = rdlt.pipeline(
        "odd",
        source=rdlt.Connector("io.rapidbyte.generator"),
        destination=rdlt.Connector("io.rapidbyte.sqlite", access=rdlt.Access(write=[Odd()])),
    ).document()
    assert document["destination"] == {
        "connector": "io.rapidbyte.sqlite",
        "access": {"write": ["/really/here"]},
    }


def test_text_beyond_the_basic_multilingual_plane_reaches_the_document(
    tmp_path: Path, connectors: Path
) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    document = generated("wide", 1, tmp_path / "out.db")
    wide = str(tmp_path / "out-\U0001f600.db")
    document["destination"] = {
        "connector": "io.rapidbyte.sqlite",
        "config": {"path": wide},
        "access": {"write": [str(tmp_path)]},
    }
    built = rdlt.Pipeline.from_document(document).build(host).document()
    assert built["destination"] == document["destination"]


def test_a_lone_surrogate_is_refused_quoting_nothing(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    document = generated("lonely", 1, tmp_path / "out.db")
    document["destination"] = {
        "connector": "io.rapidbyte.sqlite",
        "config": {"path": "/secret-token-\udc80"},
    }
    with pytest.raises(rdlt.ConfigError) as refused:
        rdlt.Pipeline.from_document(document).build(host)
    assert refused.value.code == "document_invalid"
    assert "secret-token" not in str(refused.value)
    assert "secret-token" not in repr(refused.value)


def test_a_mask_key_written_out_is_refused_quoting_nothing(
    tmp_path: Path, connectors: Path
) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    document = generated("masked", 1, tmp_path / "out.db")
    mask: dict[str, Json] = {
        "mask": {"columns": ["name"], "with": "hmac_sha256", "key": "hunter2-literal"}
    }
    document["streams"] = [{"name": "rows", "transform": [mask]}]
    with pytest.raises(rdlt.ConfigError) as refused:
        rdlt.Pipeline.from_document(document).build(host)
    assert refused.value.code == "mask_key_literal"
    assert "hunter2-literal" not in str(refused.value)


def test_a_forked_child_is_refused(tmp_path: Path, connectors: Path) -> None:
    script = textwrap.dedent(
        f"""
        import os, sys, warnings
        warnings.simplefilter("ignore")
        import rdlt
        sys.path.insert(0, {str(Path(__file__).parents[1])!r})
        from tests.documents import generated
        host = rdlt.Host(
            connectors={str(connectors)!r},
            writable=[{str(tmp_path)!r}],
            trust_binaries=True,
        )
        document = generated("forked", 1, __import__("pathlib").Path({str(tmp_path / "out.db")!r}))
        rdlt.Pipeline.from_document(document).build(host)
        child = os.fork()
        if child == 0:
            try:
                rdlt.Pipeline.from_document(document).build(host)
            except rdlt.InternalError:
                os._exit(0)
            os._exit(3)
        _, status = os.waitpid(child, 0)
        sys.exit(os.waitstatus_to_exitcode(status))
        """
    )
    finished = subprocess.run(
        [sys.executable, "-c", script], timeout=60, check=False, capture_output=True
    )
    assert finished.returncode == 0, finished.stderr
