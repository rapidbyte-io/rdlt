# pyright: reportPrivateUsage=false
"""Pipelines: described by a document or a builder, built against a host, then checked and run."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from rdlt import _native
from rdlt.host import Host, StrPath
from rdlt.model import Json, _parse


class PipelineBuilder:
    """A pipeline described and not yet built: `build(host)` checks it against the host."""

    def __init__(
        self, *, document: Mapping[str, Json] | None = None, path: Path | None = None
    ) -> None:
        self._document = None if document is None else dict(document)
        self._path = path

    def __repr__(self) -> str:
        if self._document is None:
            return f"PipelineBuilder(path={self._path!r})"
        return f"PipelineBuilder({self._document.get('pipeline')!r})"

    def build(self, host: Host) -> Pipeline:
        """Validates, places and handshakes every connector; after it no configuration error occurs.

        Raises:
            rdlt.Error: the subclass for the first mistake found, which never quotes a value.
        """
        native = host._native()
        if self._path is not None:
            return Pipeline(_native.Pipeline.build_path(self._path, native))
        text = json.dumps(self._document, ensure_ascii=False)
        return Pipeline(_native.Pipeline.build(text, native))


class Pipeline:
    """A built pipeline."""

    def __init__(self, native: _native.Pipeline) -> None:
        self._native = native
        self._name = cast("str", _parse(native.document())["pipeline"])

    def __repr__(self) -> str:
        return f"Pipeline({self._name!r})"

    @staticmethod
    def from_path(path: StrPath) -> PipelineBuilder:
        """The pipeline the YAML or JSON document at `path` describes."""
        return PipelineBuilder(path=Path(path))

    @staticmethod
    def from_document(document: Mapping[str, Json]) -> PipelineBuilder:
        """The pipeline `document`, a document as a mapping, describes."""
        return PipelineBuilder(document=document)

    def document(self) -> dict[str, Json]:
        """The built pipeline as its document, every secret still a reference."""
        return _parse(self._native.document())

    def check(self) -> None:
        """Validates, places and handshakes again; no data moves.

        Raises:
            rdlt.Error: the subclass for what failed.
        """
        self._native.check()
