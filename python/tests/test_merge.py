"""The merge kit: a destination's rows merged by key in Rust, over Arrow handed over as it is."""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable, Iterable
from typing import Any, cast

import pyarrow as pa
import pytest

from rdlt import connector
from rdlt.connector import ConnectorError, InvalidData
from rdlt.model import Json

SCHEMA = pa.schema([("id", pa.int64()), ("seq", pa.int64()), ("value", pa.utf8())])
"""A merge table's columns: its key, the sequence that orders its rows, and a value."""

KEY: connector.MergeKey = {
    "columns": ["id"],
    "seq": "seq",
    "root": None,
    "changes": None,
    "history": None,
}
"""The table's merge key, as a session is told it in `table["merge"]`."""

TABLE: dict[str, Json] = {
    "fields": [
        [{"name": "id", "nullable": False, "type": "int64"}],
        [{"name": "seq", "nullable": False, "type": "int64"}],
        [{"name": "value", "nullable": True, "type": "utf8"}],
    ]
}
"""The table's schema, as a `create` change carries it."""


def rows(ids: Iterable[int], *, seq: int, value: str) -> pa.RecordBatch:
    """A batch of `ids`, each at sequence `seq` holding `value`."""
    keys = list(ids)
    return pa.record_batch(
        {"id": keys, "seq": [seq] * len(keys), "value": [value] * len(keys)}, schema=SCHEMA
    )


def arrow(schema: object) -> pa.Schema:
    """`schema`, anything with `__arrow_c_schema__`, as pyarrow's."""
    return pa.schema(cast("Any", schema))


def read(batches: Iterable[object], schema: object = SCHEMA) -> pa.Table:
    """`batches`, the kit's answer, as one pyarrow table of every column of `schema`."""
    whole = [pa.record_batch(batch) for batch in connector.merge.read_back(schema, list(batches))]
    return pa.Table.from_batches(whole, schema=arrow(schema))


def values(table: pa.Table) -> dict[int, str | None]:
    """Each row's value, by its key."""
    keyed = zip(table["id"].to_pylist(), table["value"].to_pylist(), strict=True)
    return cast("dict[int, str | None]", dict(keyed))


def test_a_merge_keeps_one_row_per_key_over_a_table_of_many_batches() -> None:
    published = [rows(range(start, start + 10), seq=1, value="old") for start in (0, 10, 20)]
    incoming = [rows([5, 15, 25, 30], seq=2, value="new"), rows([15], seq=3, value="newest")]
    merged = connector.merge.merge_sparse(SCHEMA, published, [], incoming, KEY)
    table = read(merged.rows)
    expected = dict.fromkeys(range(30), "old") | {5: "new", 15: "newest", 25: "new", 30: "new"}
    assert (table.num_rows, values(table)) == (31, expected)
    assert merged.tombstones == []


@pytest.mark.parametrize(
    ("batch", "code", "named"),
    [
        (pa.record_batch({"seq": [1], "value": ["a"]}), "merge_key_invalid", "id"),
        (pa.record_batch({"id": [1], "value": ["a"]}), "merge_key_invalid", "seq"),
        (
            pa.record_batch({"id": [1], "seq": pa.array([None], pa.int64()), "value": ["a"]}),
            "sequence_missing",
            "sequence",
        ),
    ],
    ids=["no-key", "no-sequence", "a-row-without-its-sequence"],
)
def test_rows_a_merge_cannot_key_are_refused_with_rust_s_code(
    batch: pa.RecordBatch, code: str, named: str
) -> None:
    with pytest.raises(InvalidData) as merging:
        connector.merge.merge_sparse(SCHEMA, [rows([1], seq=1, value="old")], [], [batch], KEY)
    assert (merging.value.code, named in str(merging.value)) == (code, True)
    with pytest.raises(InvalidData) as staging:
        connector.merge.admitted(batch, SCHEMA, KEY)
    assert staging.value.code == code
    connector.merge.admitted(rows([1], seq=1, value="kept"), SCHEMA, KEY)


@pytest.mark.parametrize(
    ("call", "refused"),
    [
        (
            lambda: connector.merge.merge_sparse(SCHEMA, [{"id": 1}], [], [], KEY),
            r"published.*__arrow_c_array__.*dict",
        ),
        (
            lambda: connector.merge.merge_sparse(TABLE, [], [], [], KEY),
            r"schema.*__arrow_c_schema__.*dict",
        ),
        (
            lambda: connector.merge.admitted([1, 2], SCHEMA, KEY),
            r"batch.*__arrow_c_array__.*list",
        ),
        (lambda: connector.merge.read_back(SCHEMA, ["rows"]), r"rows.*__arrow_c_array__.*str"),
    ],
    ids=["rows-as-dicts", "schema-as-its-json", "a-list-as-a-batch", "text-as-rows"],
)
def test_what_is_no_arrow_is_refused_naming_what_the_kit_takes(
    call: Callable[[], object], refused: str
) -> None:
    with pytest.raises(TypeError, match=refused):
        call()


def test_a_change_stream_s_changes_apply_in_sequence_order_and_bury_what_they_delete() -> None:
    changes: connector.ChangeColumns = {"op": "_op", "unchanged": None, "deletion": "Hard"}
    key: connector.MergeKey = {**KEY, "changes": changes}
    written = arrow(connector.merge.written_schema(SCHEMA, changes))
    fields = [pa.field(field, kind) for field, kind in [("id", pa.int64()), ("seq", pa.int64())]]
    fields += [pa.field("value", pa.utf8()), pa.field("_op", pa.int8(), nullable=False)]
    assert written == pa.schema(fields)
    # Inserts of 1 and 2, then a delete of 1 and an update of 2, a batch apart.
    inserted = {"id": [1, 2], "seq": [1, 1], "value": ["a", "b"], "_op": [0, 0]}
    changed = {"id": [1, 2], "seq": [2, 2], "value": [None, "c"], "_op": [2, 1]}
    incoming = [pa.record_batch(columns, schema=written) for columns in (inserted, changed)]
    merged = connector.merge.merge_sparse(SCHEMA, [], [], incoming, key)
    assert values(read(merged.rows)) == {2: "c"}
    buried = connector.merge.tombstone_schema(SCHEMA, key)
    assert arrow(buried).names == ["id", "seq"]
    stones = pa.Table.from_batches([pa.record_batch(stone) for stone in merged.tombstones])
    assert stones["id"].to_pylist() == [1]
    # The inserts sent again change nothing: the delete's tombstone and the update outrank them.
    again = connector.merge.merge_sparse(SCHEMA, merged.rows, merged.tombstones, [incoming[0]], key)
    assert values(read(again.rows)) == {2: "c"}


def test_a_child_table_holds_the_children_of_each_root_s_winning_row_alone() -> None:
    root: connector.RootKey = {"table": "orders", "id": "id", "seq": "seq"}
    key: connector.MergeKey = {**KEY, "columns": ["order_id"], "root": root}
    schema = pa.schema([("order_id", pa.int64()), ("seq", pa.int64()), ("item", pa.utf8())])
    published = [
        pa.record_batch({"order_id": [1, 1, 2], "seq": [1, 1, 1], "item": ["a", "b", "c"]})
    ]
    # Order 1 is published again, twice in the commit: its row at sequence 3 wins.
    roots = [pa.record_batch({"id": [1, 1], "seq": [2, 3]})]
    incoming = [pa.record_batch({"order_id": [1, 1], "seq": [2, 3], "item": ["x", "y"]})]
    children = connector.merge.merge_children_sparse(schema, published, incoming, key, root, roots)
    table = pa.Table.from_batches([pa.record_batch(child) for child in children])
    assert sorted(cast("list[str]", table["item"].to_pylist())) == ["c", "y"]


def test_a_table_read_back_gives_each_batch_every_column_at_the_table_s_types() -> None:
    schema = connector.to_arrow(TABLE)
    keyed = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("seq", pa.int64(), nullable=False),
    ]
    assert arrow(schema) == pa.schema([*keyed, pa.field("value", pa.utf8())])
    batches = [
        pa.record_batch({"id": [1], "seq": [1]}),
        pa.record_batch({"id": pa.array([2], pa.int32()), "seq": [1], "value": ["b"]}),
    ]
    read_back = [pa.record_batch(batch) for batch in connector.merge.read_back(schema, batches)]
    assert [batch.schema for batch in read_back] == [arrow(schema)] * 2
    assert [batch.column("value").to_pylist() for batch in read_back] == [[None], ["b"]]


def test_a_table_schema_rdlt_cannot_read_is_refused() -> None:
    with pytest.raises(ValueError, match="schema"):
        connector.to_arrow({"fields": [[{"name": "id", "nullable": False, "type": "int99"}]]})


@pytest.mark.parametrize(
    ("column", "code"),
    [(pa.array([1, 300]), "value_unholdable"), (pa.array(["1"]), "type_unconvertible")],
    ids=["a-value-too-wide", "text-as-a-number"],
)
def test_rows_a_type_cannot_keep_are_refused_before_a_column_takes_it(
    column: pa.Array[pa.Scalar[pa.DataType]], code: str
) -> None:
    narrow = pa.schema([("n", pa.int8())])
    connector.merge.holds([pa.record_batch({"n": [1, 2]})], narrow)
    with pytest.raises(InvalidData) as refused:
        connector.merge.holds([pa.record_batch({"n": column})], narrow)
    assert refused.value.code == code


def test_folded_joins_the_smallest_batches_past_the_number_it_is_given() -> None:
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])
    batches = [
        pa.record_batch({"a": [1, 2, 3]}),
        pa.record_batch({"b": [4]}),
        pa.record_batch({"a": [5], "b": [6]}),
    ]
    folded = [pa.record_batch(batch) for batch in connector.merge.folded(schema, batches, 2, 100)]
    assert sorted(batch.num_rows for batch in folded) == [2, 3]
    with pytest.raises(ValueError, match="shapes"):
        connector.merge.folded(schema, batches, 0, 100)


def test_a_replace_generation_of_a_history_table_is_refused() -> None:
    history: connector.HistoryColumns = {
        "valid_from": "valid_from",
        "valid_to": "valid_to",
        "is_current": "is_current",
        "row_hash": "row_hash",
    }
    table: dict[str, object] = {
        "path": ["prices"],
        "name": "prices",
        "version": 1,
        "generation": 3,
        "merge": {**KEY, "history": history},
    }
    with pytest.raises(ConnectorError) as refused:
        connector.merge.refuse_history_generation(cast("dict[str, Json]", table))
    assert type(refused.value) is ConnectorError
    connector.merge.refuse_history_generation(
        cast("dict[str, Json]", {**table, "generation": None})
    )


def test_the_kit_takes_streams_of_batches_and_answers_batches_any_arrow_library_reads() -> None:
    old = [rows([1, 2], seq=1, value="old"), rows([3], seq=1, value="old")]
    incoming = [rows([2], seq=2, value="new")]
    merged = connector.merge.merge_sparse(SCHEMA, [pa.Table.from_batches(old)], [], incoming, KEY)
    assert all(hasattr(batch, "__arrow_c_array__") for batch in merged.rows)
    assert values(read(merged.rows)) == {1: "old", 2: "new", 3: "old"}


@pytest.mark.skipif(
    not getattr(sys, "_is_gil_enabled", lambda: True)(), reason="no GIL to let go of"
)
def test_a_merge_lets_go_of_the_gil_while_it_merges() -> None:
    # The kit's own handles, whose exports hold the GIL: only the merge lets go of it.
    schema = connector.to_arrow(TABLE)
    starts = range(0, 400_000, 50_000)
    old = [rows(range(start, start + 50_000), seq=1, value="old") for start in starts]
    published = connector.merge.read_back(schema, old)
    incoming = connector.merge.read_back(schema, [rows(range(0, 400_000, 2), seq=2, value="new")])
    merging, seen = threading.Event(), list[bool]()
    woken = threading.Event()

    def watch() -> None:
        woken.wait(timeout=60)
        seen.append(merging.is_set())

    interval = sys.getswitchinterval()
    watcher = threading.Thread(target=watch)
    watcher.start()
    # No thread takes the GIL from this one before it lets go of it itself.
    sys.setswitchinterval(60)
    try:
        merging.set()
        woken.set()
        connector.merge.merge_sparse(schema, published, [], incoming, KEY)
        merging.clear()
    finally:
        sys.setswitchinterval(interval)
        woken.set()
        watcher.join(timeout=60)
    assert seen == [True]
