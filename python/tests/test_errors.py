"""The exception classes."""

import builtins
import pickle

import rdlt

KINDS = (
    rdlt.ConfigError,
    rdlt.SchemaError,
    rdlt.SourceError,
    rdlt.DestinationError,
    rdlt.TransformError,
    rdlt.FencedError,
    rdlt.MemoryBudgetError,
    rdlt.StoppedError,
    rdlt.InternalError,
)


def test_each_kind_is_an_error() -> None:
    for kind in KINDS:
        assert issubclass(kind, rdlt.Error)
        assert issubclass(kind, Exception)


def test_an_error_survives_a_pickle() -> None:
    report = rdlt.ErrorReport(
        kind="source",
        origin="source",
        stream="orders",
        code="source_failed",
        connector_code="ApiError",
        message="stream orders failed",
        causes=("gone",),
        retryable=True,
    )
    restored = pickle.loads(pickle.dumps(rdlt.SourceError(report)))  # noqa: S301
    assert isinstance(restored, rdlt.SourceError)
    assert restored.report() == report
    assert str(restored) == "stream orders failed"


def test_no_class_shadows_a_built_in() -> None:
    for kind in (rdlt.Error, *KINDS):
        assert not hasattr(builtins, kind.__name__), kind.__name__
