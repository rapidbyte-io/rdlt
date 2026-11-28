"""What a Python read's values land as: each value a stream yields, followed to its table."""

from __future__ import annotations

import datetime
import uuid
from collections.abc import Callable, Iterator, Mapping
from decimal import Decimal
from typing import Any, cast

import pyarrow as pa
import pytest

import rdlt
from rdlt.connector import stream
from tests.connectors import memory

# Each column of a row: the value it holds, and the Arrow type and value it lands as.
_LANDED: dict[str, tuple[object, pa.DataType, object]] = {
    "decimal": (Decimal("12.50"), pa.string(), "12.50"),
    "decimal_past_a_float": (Decimal("12345678901234567.89"), pa.string(), "12345678901234567.89"),
    "decimal_whole": (Decimal("100"), pa.string(), "100"),
    "decimal_exponent": (Decimal("1E+2"), pa.string(), "1E+2"),
    "decimal_small": (Decimal("-0.000000001"), pa.string(), "-1E-9"),
    "decimal_past_76_digits": (Decimal("9" * 80), pa.string(), "9" * 80),
    "integer": (2**63 - 1, pa.int64(), 2**63 - 1),
    "integer_past_64_bits": (2**80, pa.decimal128(38, 0), Decimal(2**80)),
    "float": (1.5, pa.float64(), 1.5),
    "flag": (True, pa.bool_(), True),
    "text": ("text", pa.string(), "text"),
    "date": (datetime.date(2026, 11, 6), pa.string(), "2026-11-06"),
    "naive": (datetime.datetime(2026, 11, 6, 12, 30), pa.string(), "2026-11-06T12:30:00"),
    "aware": (
        datetime.datetime(2026, 11, 6, 12, 30, tzinfo=datetime.UTC),
        pa.string(),
        "2026-11-06T12:30:00+00:00",
    ),
    "time": (datetime.time(12, 30), pa.string(), "12:30:00"),
    "uuid": (uuid.UUID(int=1), pa.string(), "00000000-0000-0000-0000-000000000001"),
    "struct": ({"n": 1, "note": None}, pa.string(), '{"n":1,"note":null}'),
    "list": ([1, None, 3], pa.string(), "[1,null,3]"),
}


def landed(
    store: str, *items: object, columns: Mapping[str, rdlt.Column] | None = None
) -> pa.Table:
    """The table `store` holds once a stream yielding `items`, its `columns` set so, loaded."""

    @stream(name="rows")
    def rows() -> Iterator[object]:
        yield from items

    rdlt.pipeline(
        store,
        source=rows,
        destination=rdlt.local(memory.Memory, config={"store": store}),
        streams=[rdlt.stream("rows", columns=columns)],
        retry=rdlt.Retry(max_attempts=1),
    ).build(rdlt.Host()).run()
    return memory.table(store, "rows")


def types(table: pa.Table) -> dict[str, pa.DataType]:
    """Each column's type in `table`, by name."""
    return dict(zip(table.schema.names, table.schema.types, strict=True))


def test_each_value_a_row_holds_lands_as_its_type_says_and_none_as_null() -> None:
    row = {name: value for name, (value, _, _) in _LANDED.items()}
    table = landed("values", row, dict.fromkeys(_LANDED))
    for name, (_, arrow, value) in _LANDED.items():
        assert (name, types(table)[name]) == (name, arrow)
        assert (name, table[name].to_pylist()) == (name, [value, None])


def test_decimals_of_every_scale_land_as_one_column_of_their_text_across_runs() -> None:
    landed("scales", {"d": Decimal(5)})
    table = landed("scales", {"d": Decimal("5.5")}, {"d": Decimal("5.50")})
    assert [name for name in table.schema.names if not name.startswith("_rdlt")] == ["d"]
    assert types(table)["d"] == pa.string()
    assert sorted(cast("list[str]", table["d"].to_pylist())) == ["5", "5.5", "5.50"]


def test_decimals_yielded_as_arrow_land_as_decimals_of_their_precision_and_scale() -> None:
    exact = Decimal("12345678901234567.89")
    table = landed(
        "arrow-decimals",
        pa.table({"d": pa.array([exact], pa.decimal128(20, 2))}),
        pa.Table.from_pylist([{"d": Decimal("12.50")}, {"d": Decimal("1.5")}]),
    )
    assert types(table)["d"] == pa.decimal128(20, 2)
    landed_as = sorted(cast("list[Decimal]", table["d"].to_pylist()))
    assert landed_as == [Decimal("1.50"), Decimal("12.50"), exact]


def test_a_hinted_decimal_column_does_not_read_a_decimal_s_text() -> None:
    hinted: dict[str, rdlt.Json] = {"decimal": {"precision": 10, "scale": 2}}
    table = landed("hinted", {"d": Decimal("12.50")}, columns={"d": rdlt.Column(hint=hinted)})
    assert (types(table)["d"], table["d"].to_pylist()) == (pa.decimal128(10, 2), [None])
    assert (types(table)["d__json"], table["d__json"].to_pylist()) == (pa.string(), ['"12.50"'])
    frozen = {"d": rdlt.Column(hint=hinted, policy="freeze")}
    with pytest.raises(rdlt.SchemaError) as raised:
        landed("hinted-frozen", {"d": Decimal("12.50")}, columns=frozen)
    assert raised.value.report().code == "schema_frozen"


def test_a_pandas_timestamp_lands_as_its_isoformat() -> None:
    pd = pytest.importorskip("pandas")
    stamp = pd.Timestamp("2026-01-01 00:00:00.000000001")
    table = landed("timestamp", {"at": stamp})
    assert table["at"].to_pylist() == [stamp.isoformat()]


def test_a_pandas_frame_yielded_as_it_is_lands_its_missing_values_as_nulls_in_its_types() -> None:
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {
            "f": [1.5, float("nan")],
            "t": [pd.Timestamp("2026-01-01"), pd.NaT],
            "i": pd.array([1, pd.NA], dtype="Int64"),
        }
    )
    table = landed("frame", frame)
    # The frame's own Arrow types, whose time unit is the installed pandas's to choose.
    exported = types(pa.Table.from_pandas(frame))
    assert [types(table)[name] for name in "fti"] == [exported[name] for name in "fti"]
    assert [types(table)[name] for name in "fi"] == [pa.float64(), pa.int64()]
    assert pa.types.is_timestamp(types(table)["t"])
    assert [table[name].null_count for name in "fti"] == [1, 1, 1]


def test_a_table_s_struct_column_holding_a_null_entry_fails_the_read() -> None:
    column = pa.table({"payload": [{"n": 1}, None, {"n": 3}]})["payload"]
    with pytest.raises(rdlt.SourceError) as raised:
        landed("struct-column", column)
    assert raised.value.report().connector_code == "value_error"


def _text(offsets: list[int], values: bytes) -> pa.RecordBatch:
    """A batch of one text column built from its buffers as they are given, which nothing checks."""
    made = pa.array(offsets, pa.int32()).buffers()[1]
    text = pa.Array.from_buffers(pa.utf8(), len(offsets) - 1, [None, made, pa.py_buffer(values)])
    return pa.record_batch([text], names=["s"])


def _keyed(key: int) -> pa.RecordBatch:
    """A batch of one dictionary column whose one key is `key`, into a dictionary of one value."""
    keys = pa.array([key], pa.int32())
    column = cast(
        "pa.Array[Any]",
        pa.DictionaryArray.from_arrays(keys, pa.array(["a"]), safe=False),  # pyright: ignore[reportUnknownMemberType]
    )
    return pa.record_batch([column], names=["s"])


@pytest.mark.parametrize(
    ("store", "made", "code"),
    [
        ("text-not-utf8", lambda: _text([0, 2], b"\xff\xfe"), "value_error"),
        ("offsets-backwards", lambda: _text([0, 2, 1], b"ab"), "value_error"),
        ("key-past-its-dictionary", lambda: _keyed(5), "value_error"),
        (
            "text-not-utf8-in-a-table",
            lambda: pa.Table.from_batches([_text([0, 2], b"\xff\xfe")]),
            "arrow_stream",
        ),
    ],
)
def test_arrow_whose_buffers_break_its_types_fails_the_read_with_its_code(
    store: str, made: Callable[[], object], code: str
) -> None:
    with pytest.raises(rdlt.SourceError) as raised:
        landed(store, made())
    assert raised.value.report().connector_code == code


def _pandas(name: str) -> Callable[[], object]:
    """The value pandas names `name`, skipping where pandas is not installed."""
    return lambda: getattr(pytest.importorskip("pandas"), name)


def _numpy(name: str, *arguments: object) -> Callable[[], object]:
    """What numpy's `name` makes of `arguments`, skipping where numpy is not installed."""
    return lambda: getattr(pytest.importorskip("numpy"), name)(*arguments)


@pytest.mark.parametrize(
    ("store", "missing", "code"),
    [
        ("pandas-nat", _pandas("NaT"), "json_not_a_time"),
        ("pandas-na", _pandas("NA"), "json_unsupported_type"),
        ("nan", lambda: float("nan"), "json_not_finite"),
        ("nan-in-a-list", lambda: [1.0, float("nan")], "json_not_finite"),
        ("decimal-nan", lambda: Decimal("NaN"), "json_not_finite"),
        ("numpy-nan", _numpy("float64", "nan"), "json_not_finite"),
        ("numpy-nat", _numpy("datetime64", "NaT", "s"), "json_unsupported_type"),
        ("numpy-timedelta-nat", _numpy("timedelta64", "NaT", "s"), "json_unsupported_type"),
        ("numpy-array-holding-none", _numpy("array", [None, 1]), "json_unsupported_type"),
    ],
)
def test_a_missing_value_other_than_none_fails_the_read_with_its_code(
    store: str, missing: Callable[[], object], code: str
) -> None:
    value = missing()
    with pytest.raises(rdlt.SourceError) as raised:
        landed(store, {"at": value})
    assert raised.value.report().connector_code == code
