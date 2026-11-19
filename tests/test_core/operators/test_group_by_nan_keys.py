"""A NaN group key (ITL-1234).

Developer decision, November 17, 2026: all rows whose ``by`` key is NaN form one group,
as the joins match a NaN key; before, each such row was a group of its own, in the
input's row order. A NaN key sorts after every number and before null, as a NaN tag
does in the member order. ``0.0`` and ``-0.0`` are one key. The hashes of a grouped row
read a float's bits, so a group emits, for each float key, its members' value with the
smallest bit pattern: the emitted key never depends on which row came first.

Every test runs every order of the input rows: the values, the bits of every float,
the record ids, the folds and every hash must be the same for all of them.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import struct
from typing import Any, cast

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.channels import Channel
from orcapod.core.operators import GroupBy
from orcapod.core.streams.base import StreamBase
from orcapod.databases import InMemoryArrowDatabase
from orcapod.logical_types import make_arrow_extension_type
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.core_protocols import StreamProtocol


# Each float key value by name, as its bit pattern at each width: the canonical quiet
# NaN, NaN with the sign bit set, NaN with a payload, a signaling NaN (smaller bits than
# the quiet one), -0.0, 0.0 and 1.0.
BITS: dict[str, dict[int, int]] = {
    "nan": {16: 0x7E00, 32: 0x7FC00000, 64: 0x7FF8000000000000},
    "-nan": {16: 0xFE00, 32: 0xFFC00000, 64: 0xFFF8000000000000},
    "payload_nan": {16: 0x7E01, 32: 0x7FC00001, 64: 0x7FF8000000000001},
    "signaling_nan": {16: 0x7C01, 32: 0x7F800001, 64: 0x7FF0000000000001},
    "-0": {16: 0x8000, 32: 0x80000000, 64: 0x8000000000000000},
    "0": {16: 0x0, 32: 0x0, 64: 0x0},
    "1": {16: 0x3C00, 32: 0x3F800000, 64: 0x3FF0000000000000},
}
FLOAT = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}
UNSIGNED = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}
WIDTHS = [16, 32, 64]

# One key column ``k``; ``t`` and ``u`` are non-key tags that order the members; ``rid``
# names each row, so a reordered input keeps every record id.
ONE_KEY_ROWS: list[dict[str, Any]] = [
    {"k": "nan", "t": 2, "u": 1, "rid": "r1", "v": 1},
    {"k": "-nan", "t": 1, "u": 2, "rid": "r2", "v": 2},
    {"k": "payload_nan", "t": 3, "u": 0, "rid": "r3", "v": 3},
    {"k": "-0", "t": 1, "u": 1, "rid": "r4", "v": 4},
    {"k": "0", "t": 0, "u": 2, "rid": "r5", "v": 5},
    {"k": None, "t": 0, "u": 0, "rid": "r6", "v": 6},
]

# Two key columns, both floats.
TWO_KEY_ROWS: list[dict[str, Any]] = [
    {"g": "0", "k": "nan", "t": 1, "u": 0, "rid": "r1", "v": 1},
    {"g": "0", "k": "-nan", "t": 0, "u": 1, "rid": "r2", "v": 2},
    {"g": "nan", "k": "nan", "t": 1, "u": 0, "rid": "r3", "v": 3},
    {"g": "-0", "k": "1", "t": 0, "u": 0, "rid": "r4", "v": 4},
    {"g": "-nan", "k": None, "t": 0, "u": 0, "rid": "r5", "v": 5},
    {"g": "payload_nan", "k": "-nan", "t": 0, "u": 1, "rid": "r6", "v": 6},
]


def _source(
    rows: list[dict[str, Any]],
    keys: list[str],
    width: int,
    *,
    data_tag_key: bool = False,
) -> StreamBase:
    """The rows as a source; with ``data_tag_key``, the keys are data-tags too."""
    columns: dict[str, Any] = {
        name: pa.array(
            [None if r[name] is None else BITS[r[name]][width] for r in rows],
            UNSIGNED[width],
        ).view(FLOAT[width])
        for name in keys
    }
    for name in ("t", "u", "rid", "v"):
        columns[name] = [r[name] for r in rows]
    return op.sources.ArrowTableSource(
        pa.table(columns),
        tag_columns=[*keys, "t", "u"],
        tag_only_columns=["t", "u"] if data_tag_key else True,
        source_id="s",
        record_id_columns="rid",
    )


def _bits(value: float) -> int:
    bits: int = struct.unpack("<Q", struct.pack("<d", value))[0]
    return bits


def _with_bits(value: Any) -> Any:
    """Replace every float by its bit pattern, so a comparison sees NaN and -0.0."""
    if isinstance(value, float):
        return f"float:{_bits(value):016x}"
    if isinstance(value, list):
        return [_with_bits(v) for v in value]
    if isinstance(value, dict):
        return {k: _with_bits(v) for k, v in value.items()}
    return value


def _fingerprint(stream: StreamProtocol) -> dict[str, Any]:
    """The rows (floats by their bits), the row hashes and the stream's hashes."""
    return {
        "rows": [
            json.dumps(_with_bits(row), sort_keys=True, default=str)
            for row in stream.as_table(all_info=True).to_pylist()
        ],
        "row_hashes": [
            (tag.content_hash().to_string(), data.content_hash().to_string())
            for tag, data in stream.iter_data()
        ],
        "content_hash": stream.content_hash().to_string(),
        "pipeline_hash": stream.pipeline_hash().to_string(),
    }


def _key_bits(stream: StreamProtocol, name: str, width: int) -> list[int | None]:
    """The emitted key values of one column, as bit patterns at the column's width."""
    column = stream.as_table().column(name).combine_chunks()
    assert column.type == FLOAT[width]
    return column.view(UNSIGNED[width]).to_pylist()


@pytest.mark.parametrize("width", WIDTHS)
@pytest.mark.parametrize(
    ("member_order", "members"),
    [
        # 0.0 group by t (r5, r4); NaN group by t (r2, r1, r3); null group.
        (None, [[5, 4], [2, 1, 3], [6]]),
        # by u first: 0.0 group (r4, r5); NaN group (r3, r1, r2).
        (["u"], [[4, 5], [3, 1, 2], [6]]),
    ],
    ids=["default", "member_order_u"],
)
def test_nan_keys_form_one_group_in_every_row_order(
    member_order: list[str] | None, members: list[list[int]], width: int
) -> None:
    """Three NaN keys with different bits are one group, after 0.0 and before null."""
    seen: dict[str, Any] = {}
    for rows in itertools.permutations(ONE_KEY_ROWS):
        grouped = _source(list(rows), ["k"], width).group_by(
            "k", member_order=member_order
        )
        assert grouped.as_table().column("v").to_pylist() == members
        # 0.0 over -0.0 and the canonical NaN over the others, whichever came first.
        assert _key_bits(grouped, "k", width) == [
            BITS["0"][width],
            BITS["nan"][width],
            None,
        ]
        fingerprint = _fingerprint(grouped)
        if not seen:
            seen = fingerprint
        assert fingerprint == seen


@pytest.mark.parametrize("width", WIDTHS)
@pytest.mark.parametrize(
    ("member_order", "members"),
    [
        (None, [[4], [2, 1], [6, 3], [5]]),
        (["u"], [[4], [1, 2], [3, 6], [5]]),
    ],
    ids=["default", "member_order_u"],
)
def test_two_keys_compare_element_wise_in_every_row_order(
    member_order: list[str] | None, members: list[list[int]], width: int
) -> None:
    """Key tuples compare element by element, NaN equal to NaN, in name order.

    Each key column picks its own emitted value.
    """
    seen: dict[str, Any] = {}
    for rows in itertools.permutations(TWO_KEY_ROWS):
        # The order of ``by`` is not identity: either order gives the same result.
        by = ["k", "g"] if rows[0]["rid"] < "r4" else ["g", "k"]
        grouped = _source(list(rows), ["g", "k"], width).group_by(
            by, member_order=member_order
        )
        assert grouped.as_table().column("v").to_pylist() == members
        # A group whose members agree emits their value (-0.0 and the sign-set NaN
        # here); a mixed group emits the smallest bit pattern.
        assert _key_bits(grouped, "g", width) == [
            BITS[name][width] for name in ("-0", "0", "nan", "-nan")
        ]
        assert _key_bits(grouped, "k", width) == [
            BITS["1"][width],
            BITS["nan"][width],
            BITS["nan"][width],
            None,
        ]
        fingerprint = _fingerprint(grouped)
        if not seen:
            seen = fingerprint
        assert fingerprint == seen


@pytest.mark.parametrize("width", [32, 64])
def test_nan_keys_group_the_same_sync_and_async(width: int) -> None:
    seen: list[str] = []
    for rows in (ONE_KEY_ROWS, ONE_KEY_ROWS[::-1], ONE_KEY_ROWS[3:] + ONE_KEY_ROWS[:3]):
        for orchestrator in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
            job = PipelineJob(name="nan_keys", database=InMemoryArrowDatabase())
            with job:
                _ = _source(list(rows), ["k"], width).group_by("k", label="grouped")
            job.run(orchestrator=orchestrator())
            out = job.nodes["grouped"].as_table(all_info=True)
            assert out.column("v").to_pylist() == [[5, 4], [2, 1, 3], [6]]
            keys = out.column("k").combine_chunks().view(UNSIGNED[width])
            assert keys.to_pylist() == [BITS["0"][width], BITS["nan"][width], None]
            fingerprint = json.dumps(
                _with_bits(out.to_pylist()), sort_keys=True, default=str
            )
            if not seen:
                seen = [fingerprint]
            assert fingerprint == seen[0], orchestrator.__name__


@pytest.mark.parametrize("width", WIDTHS)
@pytest.mark.parametrize("name", ["-nan", "payload_nan", "signaling_nan", "-0"])
def test_a_group_whose_members_agree_keeps_their_key_bits(
    name: str, width: int
) -> None:
    """The emitted key is always a value a member holds, bit for bit.

    A ``float32`` signaling NaN comes back quiet through a Python float, so the key is
    taken from the Arrow column.
    """
    rows = [dict(r, k=name) for r in ONE_KEY_ROWS[:2]]
    grouped = _source(rows, ["k"], width).group_by("k")
    assert _key_bits(grouped, "k", width) == [BITS[name][width]]


# A signaling NaN among the NaN keys: its bits are the smallest, so it is emitted.
SIGNALING_ROWS: list[dict[str, Any]] = [
    {"k": "nan", "t": 1, "u": 0, "rid": "r1", "v": 1},
    {"k": "signaling_nan", "t": 2, "u": 0, "rid": "r2", "v": 2},
    {"k": "-nan", "t": 3, "u": 0, "rid": "r3", "v": 3},
    {"k": "payload_nan", "t": 4, "u": 0, "rid": "r4", "v": 4},
    {"k": "0", "t": 0, "u": 0, "rid": "r5", "v": 5},
]


@pytest.mark.parametrize("data_tag_key", [False, True], ids=["tag_key", "data_tag_key"])
@pytest.mark.parametrize("width", WIDTHS)
def test_a_signaling_nan_key_is_emitted_with_its_bits(
    width: int, data_tag_key: bool
) -> None:
    """In every row order, the NaN group emits the signaling NaN, bit for bit.

    With a data-tag key, the key's data hash and folded source-info token are the
    same for every row order too.
    """
    seen: dict[str, Any] = {}
    for rows in itertools.permutations(SIGNALING_ROWS):
        grouped = _source(list(rows), ["k"], width, data_tag_key=data_tag_key)
        grouped = grouped.group_by("k")
        assert grouped.as_table().column("v").to_pylist() == [[5], [1, 2, 3, 4]]
        assert _key_bits(grouped, "k", width) == [
            BITS["0"][width],
            BITS["signaling_nan"][width],
        ]
        fingerprint = _fingerprint(grouped)
        if not seen:
            seen = fingerprint
        assert fingerprint == seen


@pytest.mark.parametrize("data_tag_key", [False, True], ids=["tag_key", "data_tag_key"])
@pytest.mark.parametrize("width", WIDTHS)
def test_a_signaling_nan_key_groups_the_same_sync_and_async(
    width: int, data_tag_key: bool
) -> None:
    seen: list[str] = []
    for rows in (SIGNALING_ROWS, SIGNALING_ROWS[::-1]):
        for orchestrator in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
            job = PipelineJob(name="signaling_nan", database=InMemoryArrowDatabase())
            with job:
                _ = _source(
                    list(rows), ["k"], width, data_tag_key=data_tag_key
                ).group_by("k", label="grouped")
            job.run(orchestrator=orchestrator())
            node = job.nodes["grouped"]
            out = node.as_table(all_info=True)
            keys = out.column("k").combine_chunks().view(UNSIGNED[width])
            assert keys.to_pylist() == [BITS["0"][width], BITS["signaling_nan"][width]]
            fingerprint = json.dumps(
                {
                    "rows": _with_bits(out.to_pylist()),
                    "hashes": [
                        (
                            tag.content_hash().to_string(),
                            data.content_hash().to_string(),
                        )
                        for tag, data in node.iter_data()
                    ],
                },
                sort_keys=True,
                default=str,
            )
            if not seen:
                seen = [fingerprint]
            assert fingerprint == seen[0], orchestrator.__name__


# ── a float-backed logical type, and a key that is not a float ──────────────

# A logical (extension) type over float64 storage, like ``example.celsius`` in
# docs/concepts/logical-type-identifiers.md.
FLOAT_BACKED_CLASS = make_arrow_extension_type(
    "test.group_by_float_key", pa.float64(), b"v1"
)
# The made class takes no arguments; the stubs see pyarrow's base constructor.
FLOAT_BACKED = FLOAT_BACKED_CLASS()  # pyright: ignore[reportCallIssue]

LOGICAL_ROWS: list[dict[str, Any]] = [
    {"k": "nan", "rid": "r1", "v": 1},
    {"k": "-nan", "rid": "r2", "v": 2},
    {"k": "payload_nan", "rid": "r3", "v": 3},
    {"k": "-0", "rid": "r4", "v": 4},
    {"k": "0", "rid": "r5", "v": 5},
    {"k": None, "rid": "r6", "v": 6},
]


def _logical_source(rows: list[dict[str, Any]]) -> StreamBase:
    storage = pa.array(
        [None if r["k"] is None else BITS[r["k"]][64] for r in rows], pa.uint64()
    ).view(pa.float64())
    table = pa.table(
        {
            "k": pa.ExtensionArray.from_storage(FLOAT_BACKED, storage),
            "rid": [r["rid"] for r in rows],
            "v": [r["v"] for r in rows],
        }
    )
    return op.sources.ArrowTableSource(
        table,
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
        record_id_columns="rid",
    )


def _logical_key_bits(table: pa.Table) -> list[int | None]:
    column = table.column("k").combine_chunks()
    assert column.type == FLOAT_BACKED  # the output keeps the logical type
    assert isinstance(column, pa.ExtensionArray)
    return column.storage.view(pa.uint64()).to_pylist()


def test_a_float_backed_logical_key_emits_the_smallest_bits() -> None:
    """The bits are read from the extension's float storage, in every row order."""
    seen: dict[str, Any] = {}
    for rows in itertools.permutations(LOGICAL_ROWS):
        grouped = _logical_source(list(rows)).group_by("k")
        table = grouped.as_table()
        assert table.column("v").to_pylist() == [[4, 5], [1, 2, 3], [6]]
        assert _logical_key_bits(table) == [BITS["0"][64], BITS["nan"][64], None]
        fingerprint = _fingerprint(grouped)
        if not seen:
            seen = fingerprint
        assert fingerprint == seen


# The pipeline's Polars round trip reads the unregistered test type as its storage.
@pytest.mark.filterwarnings("ignore:Extension type 'test.group_by_float_key'")
def test_a_float_backed_logical_key_groups_the_same_sync_and_async() -> None:
    seen: list[str] = []
    for rows in (LOGICAL_ROWS, LOGICAL_ROWS[::-1]):
        for orchestrator in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
            job = PipelineJob(name="logical_key", database=InMemoryArrowDatabase())
            with job:
                _ = _logical_source(list(rows)).group_by("k", label="grouped")
            job.run(orchestrator=orchestrator())
            node = job.nodes["grouped"]
            out = node.as_table(all_info=True)
            assert _logical_key_bits(out) == [BITS["0"][64], BITS["nan"][64], None]
            fingerprint = json.dumps(
                {
                    "rows": _with_bits(out.to_pylist()),
                    "hashes": [
                        (
                            tag.content_hash().to_string(),
                            data.content_hash().to_string(),
                        )
                        for tag, data in node.iter_data()
                    ],
                },
                sort_keys=True,
                default=str,
            )
            if not seen:
                seen = [fingerprint]
            assert fingerprint == seen[0], orchestrator.__name__


def test_a_time64_ns_key_is_rebuilt_from_its_python_value() -> None:
    """Pins today's grouping of a key that is not a float, whatever the row order.

    ``GroupBy`` compares keys as Python values, and a ``time64[ns]`` value reads as a
    ``datetime.time`` of microseconds: 1 ns and 2 ns form one group, which emits
    the Python value rebuilt (0 ns), as on ``main``. Grouping by the exact value is
    ITL-1250. Only a float key is taken from the Arrow column.
    """
    for values in ([1, 2], [2, 1]):
        table = pa.table(
            {"k": pa.array(values, pa.int64()).cast(pa.time64("ns")), "v": values}
        )
        source = op.sources.ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="s"
        )
        out = source.group_by("k").as_table()
        assert out.column("k").cast(pa.int64()).to_pylist() == [0]
        members = cast(list[list[int]], out.column("v").to_pylist())
        assert sorted(members[0]) == sorted(values)


# ── a dictionary-encoded float key ──────────────────────────────────────────

DICTIONARY_ROWS: list[dict[str, Any]] = [
    {"k": "nan", "rid": "r1", "v": 1},
    {"k": "-nan", "rid": "r2", "v": 2},
    {"k": "payload_nan", "rid": "r3", "v": 3},
    {"k": "-0", "rid": "r4", "v": 4},
    {"k": "0", "rid": "r5", "v": 5},
    {"k": None, "rid": "r6", "v": 6},
]


def _dictionary_source(rows: list[dict[str, Any]], width: int) -> StreamBase:
    values = pa.array(
        [None if r["k"] is None else BITS[r["k"]][width] for r in rows],
        UNSIGNED[width],
    ).view(FLOAT[width])
    table = pa.table(
        {
            "k": values.dictionary_encode(),
            "rid": [r["rid"] for r in rows],
            "v": [r["v"] for r in rows],
        }
    )
    return op.sources.ArrowTableSource(
        table,
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
        record_id_columns="rid",
    )


def _dictionary_key_bits(table: pa.Table, width: int) -> list[int | None]:
    column = table.column("k").combine_chunks()
    assert pa.types.is_dictionary(column.type)  # the output keeps the encoding
    assert column.type.value_type == FLOAT[width]
    return column.cast(FLOAT[width]).view(UNSIGNED[width]).to_pylist()


@pytest.mark.parametrize("width", [32, 64])
def test_a_dictionary_encoded_float_key_emits_the_smallest_bits(width: int) -> None:
    """A dictionary over floats is chosen by its values' bits, in every row order."""
    seen: dict[str, Any] = {}
    for rows in itertools.permutations(DICTIONARY_ROWS):
        grouped = _dictionary_source(list(rows), width).group_by("k")
        table = grouped.as_table()
        assert table.column("v").to_pylist() == [[4, 5], [1, 2, 3], [6]]
        assert _dictionary_key_bits(table, width) == [
            BITS["0"][width],
            BITS["nan"][width],
            None,
        ]
        # ``all_info`` sorts by the tags, which a dictionary tag cannot be yet
        # (ITL-1264): the rows come in key order already, with every system tag and
        # source token.
        fingerprint = {
            "rows": [
                json.dumps(_with_bits(row), sort_keys=True, default=str)
                for row in grouped.as_table(
                    columns={"system_tags": True, "source": True}
                ).to_pylist()
            ],
            "row_hashes": _row_identity(grouped.iter_data()),
            "content_hash": grouped.content_hash().to_string(),
        }
        if not seen:
            seen = fingerprint
        assert fingerprint == seen


async def _async_grouped_rows(source: StreamBase) -> list[Any]:
    """The rows of ``GroupBy("k")``'s async path, over a channel.

    A pipeline job cannot carry a dictionary-typed tag yet (its stored-table sort and
    nullability restore refuse a dictionary; ITL-1264), so the operator's async path
    is driven directly, as ``test_async_sync_equivalence.py`` does.
    """
    channel: Channel[Any] = Channel(buffer_size=64)
    output: Channel[Any] = Channel(buffer_size=64)
    for row in source.iter_data():
        await channel.writer.send(row)
    await channel.writer.close()
    await GroupBy("k").async_execute(
        [channel.reader],
        output.writer,
        input_pipeline_hashes=[source.pipeline_hash()],
    )
    return await output.reader.collect()


def _row_identity(rows: Any) -> list[tuple[str, str, Any]]:
    """Each row's tag hash, data hash and key bits (as hex), in row order."""
    out: list[tuple[str, str, Any]] = []
    for tag, data in rows:
        key = tag.as_table().column("k").combine_chunks()
        key = key.cast(key.type.value_type)
        unsigned = UNSIGNED[key.type.bit_width]
        out.append(
            (
                tag.content_hash().to_string(),
                data.content_hash().to_string(),
                key.view(unsigned).to_pylist(),
            )
        )
    return out


@pytest.mark.parametrize("width", [32, 64])
def test_a_dictionary_encoded_float_key_groups_the_same_sync_and_async(
    width: int,
) -> None:
    seen: list[Any] = []
    for rows in (DICTIONARY_ROWS, DICTIONARY_ROWS[::-1]):
        source = _dictionary_source(list(rows), width)
        sync = _row_identity(source.group_by("k").iter_data())
        asynchronous = _row_identity(asyncio.run(_async_grouped_rows(source)))
        assert sorted(asynchronous, key=repr) == sorted(sync, key=repr)
        assert [bits for _, _, bits in sync] == [
            [BITS["0"][width]],
            [BITS["nan"][width]],
            [None],
        ]
        if not seen:
            seen = sync
        assert sync == seen


def _null_dictionary_source(indices: list[int | None], rids: list[str]) -> StreamBase:
    """A dictionary key whose null is a null index in one row and an index to a null
    entry in the other: both read as null."""
    key = pa.DictionaryArray.from_arrays(
        pa.array(indices, pa.int32()), pa.array([1.0, None], pa.float64())
    )
    table = pa.table({"k": key, "rid": rids, "v": [int(r[1:]) for r in rids]})
    return op.sources.ArrowTableSource(
        table,
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
        record_id_columns="rid",
    )


@pytest.mark.parametrize(
    ("indices", "rids"),
    [([None, 1], ["r1", "r2"]), ([1, None], ["r2", "r1"])],
    ids=["null_index_first", "null_entry_first"],
)
def test_a_null_dictionary_key_is_emitted_as_a_null(
    indices: list[int | None], rids: list[str]
) -> None:
    """Whichever null comes first, the group's key is a null index.

    ``DropNulls`` reads the top-level validity, which an index to a null entry does
    not carry: without this, the row order decided whether the group was dropped.
    """
    source = _null_dictionary_source(indices, rids)
    grouped = source.group_by("k")
    key = grouped.as_table().column("k").combine_chunks()
    assert isinstance(key, pa.DictionaryArray)
    assert key.indices.to_pylist() == [None]
    assert grouped.drop_nulls(["k"]).as_table().num_rows == 0
    for rows in (list(grouped.iter_data()), asyncio.run(_async_grouped_rows(source))):
        [(tag, _)] = rows
        tag_key = tag.as_table().column("k").combine_chunks()
        assert isinstance(tag_key, pa.DictionaryArray)
        assert tag_key.indices.to_pylist() == [None]
