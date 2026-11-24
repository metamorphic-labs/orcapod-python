"""The codec of a SpiralDB store table: names, types, rows and the manifest (ITL-1446).

No SpiralDB and no fake: ``as_spiral_returns`` does to encoded rows what SpiralDB does
(columns and struct fields sorted by name, rows in another order and other chunks, other
list item names, small or large variants).
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import json
import math
import random
import subprocess
import sys
import unicodedata
import uuid
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.databases import spiral_store as store
from orcapod.logical_types.canonical_form import declaration_form_schema
from orcapod.utils.arrow_utils import normalize_to_large_types
from tests.test_databases.type_matrix import (
    ROWS,
    Row,
    build_table,
    field_difference,
)
from tests.test_databases.type_matrix import (
    _same_values as same_values,  # pyright: ignore[reportPrivateUsage]
)

KEY = store.KEY_COLUMN
NAME_KEY, METADATA_KEY = b"ARROW:extension:name", b"ARROW:extension:metadata"

chunked: Any = pa.chunked_array  # the stubs type ``type=`` as None
map_of: Any = pa.map_  # the stubs lack fields and keys_sorted
schema_of: Any = pa.schema  # the stubs take no mix of fields and pairs
# The stubs take literal units and zones only.
timestamp: Any = pa.timestamp
duration: Any = pa.duration


# ------------------------------------------------------------------------- helpers


def ids(n: int) -> Any:
    return pa.array([f"r{i:04d}".encode() for i in range(n)], pa.large_binary())


def keyed(columns: dict[str, Any], fields: list[Any] | None = None) -> pa.Table:
    """A table of ``columns`` (arrays) with a key column in the middle."""
    n = len(next(iter(columns.values())))
    fields = fields or [pa.field(k, v.type) for k, v in columns.items()]
    half = len(fields) // 2
    fields = [*fields[:half], pa.field(KEY, pa.large_binary(), False), *fields[half:]]
    arrays = [ids(n) if f.name == KEY else columns[f.name] for f in fields]
    return pa.Table.from_arrays(arrays, schema=schema_of(fields))


def _returned(array: Any, small: bool) -> Any:
    """One array as SpiralDB may give it back."""
    t = pa.types
    if t.is_struct(array.type):
        assert array.null_count == 0
        order = sorted(
            range(array.type.num_fields), key=lambda i: array.type.field(i).name
        )
        children = [_returned(array.field(i), small) for i in order]
        names = [array.type.field(i).name for i in order]
        return pa.StructArray.from_arrays(
            children, fields=[pa.field(n, c.type) for n, c in zip(names, children)]
        )
    mask = array.is_null() if array.null_count else None
    if t.is_large_list(array.type):
        items = _returned(array.flatten(), small)
        offsets = array.offsets
        offsets = pa.array(offsets.to_numpy() - offsets.to_numpy()[0], pa.int64())
        target = (pa.list_ if small else pa.large_list)(pa.field("element", items.type))
        cls: Any = pa.ListArray if small else pa.LargeListArray
        return cls.from_arrays(
            offsets.cast(pa.int32() if small else pa.int64()),
            items,
            type=target,
            mask=mask,
        )
    if small and t.is_large_string(array.type):
        return array.cast(pa.string())
    if small and t.is_large_binary(array.type):
        return array.cast(pa.binary())
    return array


def as_spiral_returns(rows: pa.Table, seed: int = 0, small: bool = False) -> pa.Table:
    """Encoded rows as a scan gives them: a set of rows, columns sorted by name."""
    rng = random.Random(seed)
    order = list(range(rows.num_rows))
    rng.shuffle(order)
    columns: dict[str, Any] = {}
    for name in sorted(rows.column_names):
        whole = rows.column(name).combine_chunks().take(pa.array(order, pa.int64()))
        cut = rng.randint(0, len(whole))
        pieces = [_returned(p, small) for p in (whole[:cut], whole[cut:])]
        columns[name] = chunked(pieces, type=pieces[0].type)
    return pa.table(columns)


def by_id(rows: pa.Table) -> pa.Table:
    """Rows sorted by key: a store is a set of rows."""
    keys = rows.column(KEY).to_pylist()
    return rows.take(
        pa.array(sorted(range(len(keys)), key=lambda i: keys[i]), pa.int64())
    )


def entries(
    schema: pa.Schema, path: tuple[str, ...] = ("a", "b"), **extra: bytes
) -> dict[str, bytes]:
    chunks, header = store.manifest_entries(store.StoreManifest(path, schema))
    return {**extra, **chunks, **header}


def read(metadata: dict[str, bytes]) -> store.StoreManifest:
    manifest = store.read_manifest(metadata)
    assert manifest is not None
    return manifest


def large_type(arrow_type: Any) -> Any:
    return normalize_to_large_types(arrow_type, keep_null=True)


def large(schema: pa.Schema) -> pa.Schema:
    return schema_of(
        [f.with_type(normalize_to_large_types(f.type, keep_null=True)) for f in schema],
        metadata=cast(Any, schema.metadata),
    )


def assert_same(got: pa.Table, expected: pa.Table) -> None:
    """Schema exactly (every depth); values bit for bit, as sets of rows."""
    assert got.schema.names == expected.schema.names
    assert got.schema.metadata == expected.schema.metadata
    for a, b in zip(expected.schema, got.schema):
        assert field_difference(a, b, a.name) is None
    got, expected = by_id(got), by_id(expected)
    for name in expected.column_names:
        left, right = got.column(name), expected.column(name)
        assert same_values(left.combine_chunks(), right.combine_chunks()), name
    got.validate()


def round_trip(rows: pa.Table, *, live: bool = False) -> pa.Table:
    """Write, return as SpiralDB does, and restore through the manifest: every slice,
    both variants."""
    expected_schema = large(rows.schema)
    schema = read(entries(rows.schema)).schema
    if live:
        converter = get_default_context().type_converter
        schema = declaration_form_schema(schema, converter)
    else:
        expected_schema = store.logical_schema(rows.schema)
    result = None
    for piece in (rows, rows.slice(1), rows.slice(rows.num_rows), rows.slice(0, 1)):
        encoded = store.encode_rows(piece)
        assert encoded.schema.equals(
            store.physical_schema(rows.schema), check_metadata=True
        )
        _assert_physical(encoded.schema)
        encoded.validate(full=True)
        for seed, small in ((1, False), (2, True)):
            got = store.decode_rows(as_spiral_returns(encoded, seed, small), schema)
            want = pa.Table.from_arrays(
                [
                    chunked([_as_target(c, f) for c in col.chunks], type=f.type)
                    for col, f in zip(piece.columns, expected_schema)
                ],
                schema=expected_schema,
            )
            assert_same(got, want)
            result = result or got
    assert result is not None
    return result


def _as_target(chunk: Any, field: Any) -> Any:
    """What was written, under the expected field (its storage, its large variants)."""
    if chunk.type == field.type:
        return chunk
    if isinstance(chunk, pa.ExtensionArray):
        chunk = chunk.storage
    return chunk if chunk.type == field.type else chunk.cast(field.type)


def _assert_physical(schema: pa.Schema) -> None:
    """SpiralDB's known rules on names and types, at every depth."""

    def walk(field: Any, named: bool) -> None:
        assert field.nullable and not field.metadata
        if named and field.name != KEY:
            assert field.name and not field.name.startswith(("__", "$"))
            assert not set(field.name) & set(".$*")
        t = field.type
        assert not (
            pa.types.is_fixed_size_binary(t)
            or pa.types.is_duration(t)
            or pa.types.is_null(t)
            or isinstance(t, pa.BaseExtensionType)
            or (pa.types.is_timestamp(t) and t.tz is not None)
        )
        for i in range(t.num_fields):
            walk(t.field(i), pa.types.is_struct(t))

    for f in schema:
        walk(f, True)


def refused(rows: pa.Table, *words: str) -> store.UnstorableInSpiralError:
    with pytest.raises(store.UnstorableInSpiralError) as info:
        store.encode_rows(rows)
    for word in words:
        assert word in str(info.value)
    return info.value


# --------------------------------------------------------------------------- names


@pytest.mark.parametrize(
    ("name", "stored"),
    [
        ("x", "x"),
        ("_source_x", "_source_x"),
        ("_tag_source_id::ab", "_tag_source_id::ab"),
        ("a;b", "a;b"),
        ("é", "é"),
        ("a b", "a b"),
        ("__data_context", "ox___data_context"),
        ("a.b", "ox_a;2Eb"),
        ("a;2Eb", "a;2Eb"),
        ("ox_a;2Eb", "ox_ox_a;3B2Eb"),
        ("ox_", "ox_ox_"),
        ("", "ox_"),
        ("$a*\x00\x7f\n", "ox_;24a;2A;00;7F;0A"),
    ],
)
def test_the_names_of_the_design(name: str, stored: str) -> None:
    assert store.encode_name(name) == stored
    assert store.decode_name(stored) == name


def adversarial_names(count: int) -> set[str]:
    rng = random.Random(1446)
    pieces = (
        ["_", "__", "ox_", "ox", ".", ";", ";2E", ";3B", ";2e", ";G", "$", "*", "%2E"]
        + ["a", "B", "é", "é", "::", " ", "\n", "\x00", "\x7f", "\x80", "{", '"']
        + ["x_", "o", "\U0001f600", " ", "2E", "3B"]
    )
    names = {"", KEY, "ox_", "a.b", "a;2Eb", "ox_a;2Eb", "ox_order", "Å", "Å"}
    names |= {chr(i) for i in range(0x300)} | {f"a{chr(i)}b" for i in range(0x300)}
    while len(names) < count:
        names.add("".join(rng.choices(pieces, k=rng.randint(0, 8))))
    return names


def test_names_round_trip_and_are_injective() -> None:
    names = adversarial_names(210_000)
    assert len(names) >= 200_000
    encoded = {name: store.encode_name(name) for name in names}
    assert len(set(encoded.values())) == len(names)
    for name, stored in encoded.items():
        assert store.decode_name(stored) == name
        assert stored and not stored.startswith("__")
        assert not any(c in ".$*\x7f" or ord(c) < 32 for c in stored)


def test_an_already_stored_name_is_itself_encoded_injectively() -> None:
    names = sorted(adversarial_names(5_000))
    once = [store.encode_name(n) for n in names]
    twice = [store.encode_name(n) for n in once]
    assert len(set(twice)) == len(names)
    assert [store.decode_name(store.decode_name(t)) for t in twice] == names


@pytest.mark.parametrize(
    "stored",
    [
        "",
        "__x",
        "a.b",
        "a\n",
        "ox_a",
        "ox_a;2eb",
        "ox_a;zz.",
        "ox_;",
        "ox_a;61",
        "ox_order",
    ],
)
def test_a_name_the_codec_never_writes_is_not_decoded(stored: str) -> None:
    with pytest.raises(store.StoredRowsError):
        store.decode_name(stored)


@pytest.mark.parametrize(
    "names", [["a.b", "a;2Eb", "ox_a;2Eb", "__a"], ["", "ox_", "$"]]
)
@pytest.mark.parametrize("depth", [0, 1, 2, 3])
def test_names_in_rows_at_every_depth(names: list[str], depth: int) -> None:
    for order in (names, list(reversed(names))):
        columns = {n: pa.array([i, i + 1], pa.int64()) for i, n in enumerate(order)}
        arrays, fields = (
            list(columns.values()),
            [pa.field(n, pa.int64()) for n in order],
        )
        for level in range(depth):
            inner = pa.StructArray.from_arrays(arrays, fields=fields)
            if level % 2:
                inner = pa.LargeListArray.from_arrays(
                    pa.array([0, 1, 2], pa.int64()), inner
                )
            arrays = [inner, pa.array([5, 6])]
            fields = [pa.field("s.t", inner.type), pa.field("__z", pa.int64())]
        _ = round_trip(keyed({f.name: a for f, a in zip(fields, arrays)}, fields))


@pytest.mark.parametrize("nested", [False, True])
def test_a_stored_name_never_matches_a_logical_name(nested: bool) -> None:
    """``E("a.b")`` is ``ox_a;2Eb``, which is also another column's own name."""
    values = {"a.b": 1, "ox_a;2Eb": 2, "__record": 3, "ox___record": 4}
    for order in (list(values), list(reversed(values))):
        if nested:
            ty = pa.struct([(n, pa.int64()) for n in order])
            table = keyed({"s": pa.array([values], ty)})
            assert round_trip(table).column("s").to_pylist() == [values]
        else:
            got = round_trip(keyed({n: pa.array([values[n]]) for n in order}))
            assert {n: got.column(n)[0].as_py() for n in values} == values


def test_the_key_is_not_a_value_column_of_the_same_name() -> None:
    inner = pa.array([{KEY: 7}], pa.struct([(KEY, pa.int64())]))
    table = keyed({"ox_" + KEY: pa.array([1]), "s": inner})
    got = round_trip(table)
    assert got.column(KEY).to_pylist() == ids(1).to_pylist()
    assert got.column("ox_" + KEY).to_pylist() == [1]
    encoded = store.encode_rows(table)
    renamed = encoded.rename_columns(
        ["ox_" + KEY if n == KEY else n for n in encoded.column_names]
    )
    with pytest.raises(store.StoredRowsError, match="missing"):
        store.decode_rows(renamed, table.schema)


@pytest.mark.parametrize("name", ["\ud800", "a\udfff", "__\ud800.b"])
def test_a_lone_surrogate_is_refused(name: str) -> None:
    with pytest.raises(store.UnstorableInSpiralError, match="UTF-8"):
        store.encode_name(name)
    for stored in (name, f"ox_{name}"):
        with pytest.raises(store.StoredRowsError):
            store.decode_name(stored)


def test_a_name_that_is_not_utf8_is_refused_at_every_depth() -> None:
    bad: Any = pa.field(cast(Any, b"a\xff"), pa.int8())
    for field in (
        bad,
        pa.field("s", pa.struct([bad])),
        pa.field("l", pa.list_(pa.struct([bad]))),
        pa.field("i", pa.list_(bad)),
    ):
        with pytest.raises(store.UnstorableInSpiralError, match="UTF-8"):
            store.logical_schema(schema_of([field, (KEY, pa.large_binary())]))


def wrappings(inner: Any) -> list[Any]:
    """``inner`` alone, below each kind of nesting, and as a logical type's storage."""
    return [
        inner,
        pa.list_(inner),
        pa.list_(inner, 2),
        pa.struct([("s", pa.large_list(inner))]),
        Labelled(inner, "test.storage", b""),
        Labelled(pa.large_list(inner), "test.storage", b""),
    ]


def test_duplicate_sibling_names_are_refused_at_every_depth() -> None:
    twice = pa.struct([("a.b", pa.int8()), ("c", pa.int8()), ("a.b", pa.int8())])
    for ty in wrappings(twice):
        with pytest.raises(store.UnstorableInSpiralError, match="more than once"):
            store.logical_schema(schema_of([("x", ty), (KEY, pa.large_binary())]))
    same_in_two_structs = pa.struct([("a", pa.struct([("a", pa.int8())]))])
    _ = store.logical_schema(
        schema_of([("a", same_in_two_structs), (KEY, pa.large_binary())])
    )


def test_names_the_service_would_merge_have_a_hook_and_no_guessed_rule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairs = [("A", "a"), ("e\u0301", "\u00e9")]
    schemas = [
        schema_of([(x, pa.int8()), (y, pa.int8()), (KEY, pa.large_binary())])
        for x, y in pairs
    ] + [
        schema_of([("s", ty), (KEY, pa.large_binary())])
        for x, y in pairs
        for ty in wrappings(pa.struct([(x, pa.int8()), (y, pa.int8())]))
    ]
    for schema in schemas:
        _ = store.logical_schema(schema)
    monkeypatch.setattr(
        store,
        "_service_form",
        lambda s: unicodedata.normalize("NFC", s).casefold(),
    )
    for schema in schemas:
        with pytest.raises(store.UnstorableInSpiralError, match="more than once"):
            store.logical_schema(schema)


@pytest.mark.parametrize("as_storage", [False, True])
def test_a_struct_parent_must_be_valid_also_in_a_logical_types_storage(
    as_storage: bool,
) -> None:
    struct = pa.struct([pa.field("a.b", pa.int8(), False)])
    for ty, value in ((struct, None), (pa.large_list(struct), [{"a.b": 1}, None])):
        array = pa.array([value], ty)
        if as_storage:
            live = Labelled(ty, "test.parent", b"")
            array = pa.ExtensionArray.from_storage(live, array)
        refused(keyed({"c": array}), "'c'", "null struct")


# --------------------------------------------------------------------------- types

REFUSED_ROWS = {
    "halffloat (float16)": "float16",
    "float (float32)": "negative zero",
    "double (float64)": "negative zero",
    "decimal256(40, 2)": "decimal256",
    "decimal256(76, 10)": "decimal256",
    "date64": "date64",
    "time32[s]": "time",
    "time32[ms]": "time",
    "time64[us]": "time",
    "time64[ns]": "time",
    "large_list<float16>": "float16",
    "struct<> (no fields)": "no fields",
    "map<string, int64>": "map",
    "map<string, int64>, keys sorted": "map",
    "map<k: string, v: int64>, keys sorted": "map",
    "map<string, int64 {unit: m}> (item metadata)": "map",
    "map<string {unit: m}, int64> (key metadata)": "map",
    "map<string, struct<a: int64 {unit: m}>> (metadata inside the item)": "map",
}
REFUSED_BY_VALUE = {"float (float32)", "double (float64)"}
# The matrix's second column holds a null row.
NULL_STRUCT_ROWS = {
    "struct<a: int32, b: string not null>",
    "struct<f: fixed_size_list<int64>[2]>",
    "struct<a: int32 {unit: s}>",
    "user class, exact (dataclass)",
    "user class, structural (orcapod.dataclass)",
}


def test_the_expectations_name_rows_of_the_matrix() -> None:
    assert set(REFUSED_ROWS) | NULL_STRUCT_ROWS <= {r.label for r in ROWS}


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize("row", ROWS, ids=lambda r: r.label)
def test_every_row_of_the_type_matrix_is_kept_or_refused(
    row: Row, tmp_path: Path
) -> None:
    """Two columns of each type (the second nullable, with a null), both forms of a
    logical type, and the column naming its refusal."""
    if row.label.startswith("torch"):
        _ = pytest.importorskip("torch")
    rows = build_table(row, tmp_path).rename_columns([KEY, "x", "y"])
    if row.label in REFUSED_ROWS:
        refused(rows, REFUSED_ROWS[row.label], "'x'")
        return
    if row.label in NULL_STRUCT_ROWS:
        refused(rows, "null struct", "'y'")
        rows = rows.drop_columns(["y"])
    _ = round_trip(rows)
    if row.group != "struct and map" or not row.label.startswith("uuid"):
        # Arrow's own uuid is no Orcapod logical type: it stays annotated.
        _ = round_trip(rows, live=True)


def scalar_cases() -> list[tuple[str, pa.DataType, list[Any]]]:
    D = decimal.Decimal
    return [
        ("int8", pa.int8(), [-128, 127, 0]),
        ("uint64", pa.uint64(), [0, 2**64 - 1, 2**63]),
        ("int64", pa.int64(), [-(2**63), 2**63 - 1, 0]),
        ("bool", pa.bool_(), [True, False, True]),
        ("float32", pa.float32(), [1.5, 0.0, -3.25]),
        ("float64", pa.float64(), [0.1, math.inf, 5e-324]),
        ("decimal128", pa.decimal128(38, 4), [D("1.2500"), D("-9.0001"), D("0")]),
        (
            "date32",
            pa.date32(),
            [dt.date(1, 1, 1), dt.date(9999, 12, 31), dt.date(1970, 1, 1)],
        ),
        ("large_string", pa.large_string(), ["", "é\x00", "x" * 100]),
        ("string", pa.string(), ["", "é\x00", "y"]),
        ("string_view", pa.string_view(), ["", "é", "z" * 50]),
        ("binary", pa.binary(), [b"", b"\x00\xff", b"a"]),
        ("fsb", pa.binary(3), [b"abc", b"\x00\x00\x00", b"\xff\xfe\xfd"]),
        ("fsb0", pa.binary(0), [b"", b"", b""]),
        ("null", pa.null(), [None, None, None]),
        *(
            (f"ts_{u}_{z}", timestamp(u, tz=z), [-1, 0, 2**40 + 1])
            for u in ("s", "ms", "us", "ns")
            for z in (None, "UTC", "Asia/Kathmandu", "-03:30")
        ),
        *(
            (f"dur_{u}", duration(u), [-1, 0, 2**40 + 1])
            for u in ("s", "ms", "us", "ns")
        ),
    ]


@pytest.mark.parametrize("case", scalar_cases(), ids=lambda c: c[0])
@pytest.mark.parametrize("nullable", [True, False])
def test_every_type_at_every_nesting_depth(
    case: tuple[str, pa.DataType, list[Any]], nullable: bool
) -> None:
    """Each kept type alone and below lists, fixed-size lists, structs and their
    combinations, in several columns at once, in two column orders."""
    _, arrow_type, values = case
    nullable = nullable or pa.types.is_null(arrow_type)
    if nullable:
        values = [*values, None]
    else:
        values = [*values, values[0]]
    leaf = pa.field("v.1", arrow_type, nullable, {"unit": "m"})
    item = leaf.with_name("odd item")
    v, n = values, len(values)
    shapes: dict[str, tuple[pa.DataType, list[Any]]] = {
        "plain": (arrow_type, v),
        "list": (pa.list_(item), [v, [], None, v[:1]]),
        "large": (pa.large_list(item), [None, v[:2], v, []]),
        "fixed": (pa.list_(item, 2), [v[:2], None, v[2:], v[1:3]]),
        "struct": (
            pa.struct([leaf, pa.field("__b", pa.int8())]),
            [{"v.1": x, "__b": i} for i, x in enumerate(v)],
        ),
        "struct_list": (
            pa.struct([pa.field("z", pa.list_(item)), pa.field("a", pa.int8())]),
            [{"z": x, "a": 1} for x in ([], v, None, v[:1])],
        ),
        "list_struct": (
            pa.list_(pa.field("i", pa.struct([pa.field("z", pa.int8()), leaf]))),
            [[{"z": 1, "v.1": v[0]}], None, [], [{"z": None, "v.1": x} for x in v]],
        ),
        "list_list": (pa.list_(pa.list_(item)), [[v, [], None], [], None, [v[:1]]]),
        "fixed_fixed": (
            pa.list_(pa.list_(item, 1), 2),
            [[v[:1], None], None, [v[1:2], v[2:3]], [v[:1], v[:1]]],
        ),
        "list_fixed": (pa.list_(pa.list_(item, 1)), [[v[:1], None], [], None, [v[3:]]]),
        "deep": (
            pa.list_(
                pa.struct([pa.field("s", pa.struct([pa.field("l", pa.list_(item))]))])
            ),
            [[{"s": {"l": v}}, {"s": {"l": None}}], [], None, [{"s": {"l": []}}]],
        ),
    }
    if not nullable or pa.types.is_null(arrow_type):
        # pyarrow cannot cast a null fixed-size list over non-null items, or a sliced
        # list of structs with a null-typed field, so the test could not
        # state what it expects of the small variants.
        shapes = {k: (large_type(t), rows) for k, (t, rows) in shapes.items()}
    columns = {k: pa.array(rows, t) for k, (t, rows) in shapes.items()}
    assert all(len(c) == n for c in columns.values())
    fields = [
        pa.field(k, c.type, nullable if k == "plain" else True, {"k": k})
        for k, c in columns.items()
    ]
    table = keyed(columns, fields)
    _ = round_trip(table)
    _ = round_trip(table.select(list(reversed(table.column_names))))


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
@pytest.mark.parametrize(
    "zone", ["UTC", "Europe/Berlin", "Asia/Kathmandu", "+05:45", "-03:30"]
)
def test_zone_aware_timestamps_are_exact(unit: str, zone: str) -> None:
    """Extremes of the integer, the far past and sub-second ticks."""
    ticks = [-(2**63), 2**63 - 1, -(2**63) + 1, 0, -1, 1, -(10**15) - 7, 10**15 + 3]
    ticks += [-62135596800 * {"s": 1, "ms": 10**3, "us": 10**6, "ns": 1}[unit] - 1]
    values = [*ticks, None]
    ts = timestamp(unit, tz=zone)
    plain: Any = pa.array(values, pa.int64()).cast(ts)
    columns = {
        "t": plain,
        "l": pa.array([[x, None] for x in values], pa.list_(pa.int64())).cast(
            pa.list_(ts)
        ),
        "s": pa.StructArray.from_arrays([plain], fields=[pa.field("t", ts)]),
        "f": pa.array([[x] for x in values], pa.list_(pa.int64(), 1)).cast(
            pa.list_(ts, 1)
        ),
    }
    got = by_id(round_trip(keyed(columns)))
    assert got.column("t").cast(pa.int64()).to_pylist() == values
    assert got.schema.field("t").type == ts
    encoded = store.encode_rows(keyed(columns))
    assert encoded.schema.field("t").type == timestamp(unit)
    assert encoded.column("t").cast(pa.int64()).to_pylist() == values


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_durations_are_exact(unit: str) -> None:
    values = [-(2**63), 2**63 - 1, -1, 0, None]
    column: Any = pa.array(values, pa.int64()).cast(duration(unit))
    got = by_id(round_trip(keyed({"d": column})))
    assert got.column("d").cast(pa.int64()).to_pylist() == values


def _float(bits: int, width: int) -> Any:
    ints = np.array([bits], dtype=f"u{width // 8}")
    return pa.array(ints).view(pa.float32() if width == 32 else pa.float64())


FLOATS = {
    32: {
        "kept": [0, 1, 0x80000001, 0x7F800000, 0xFF800000, 0x7FC00000, 0x7F7FFFFF],
        "-0.0": [0x80000000],
        "NaN": [0x7FC00001, 0xFFC00000, 0x7F800001, 0xFF800001, 0x7FFFFFFF],
    },
    64: {
        "kept": [0, 1, (1 << 63) + 1, 0x7FF << 52, 0xFFF << 52, 0x7FF8 << 48],
        "-0.0": [1 << 63],
        "NaN": [(0x7FF8 << 48) + 1, 0xFFF8 << 48, (0x7FF << 52) + 1, (1 << 63) - 1],
    },
}


@pytest.mark.parametrize("width", [32, 64])
@pytest.mark.parametrize("nested", ["plain", "list", "fixed", "struct"])
def test_float_bits_are_kept_or_refused(width: int, nested: str) -> None:
    def column(array: Any) -> Any:
        if nested == "list":
            return pa.ListArray.from_arrays(
                pa.array(range(len(array) + 1), pa.int32()), array
            )
        if nested == "fixed":
            return pa.FixedSizeListArray.from_arrays(array, 1)
        if nested == "struct":
            return pa.StructArray.from_arrays([array], names=["f"])
        return array

    kept: Any = pa.concat_arrays([_float(b, width) for b in FLOATS[width]["kept"]])
    _ = round_trip(keyed({"f": column(kept)}))
    for bits in FLOATS[width]["-0.0"]:
        refused(keyed({"f": column(_float(bits, width))}), "'f'", "negative zero")
    for bits in FLOATS[width]["NaN"]:
        refused(keyed({"f": column(_float(bits, width))}), "'f'", "NaN")


def test_a_hidden_negative_zero_is_not_refused_and_not_written() -> None:
    """The value slot of a null row, and the items of a null list, are no values."""
    raw: Any = pa.array(np.array([-0.0, 1.0]), mask=np.array([True, False]))
    lists = pa.ListArray.from_arrays(
        pa.array([0, 1, 2], pa.int32()),
        pa.array(np.array([-0.0, 1.0])),
        mask=pa.array([True, False]),
    )
    table = keyed({"f": raw, "l": lists})
    _ = round_trip(table)
    chunk: Any = store.encode_rows(table).column("l").chunk(0)
    items = chunk.values.to_pylist()
    assert items == [1.0]


@pytest.mark.parametrize(
    "make",
    [
        lambda: pa.array([None, {"a": 1}], pa.struct([("a", pa.int8())])),
        lambda: pa.array(
            [[{"a": 1}, None], []], pa.list_(pa.struct([("a", pa.int8())]))
        ),
        lambda: pa.array(
            [{"s": None}, {"s": {"a": 1}}],
            pa.struct([("s", pa.struct([("a", pa.int8())]))]),
        ),
        lambda: pa.array(
            [[{"a": 1}], None], pa.list_(pa.struct([("a", pa.int8())]), 1)
        ),
    ],
)
def test_a_null_struct_at_any_depth_is_refused(make: Any) -> None:
    refused(keyed({"ok": pa.array([1, 2]), "bad": make()}), "'bad'", "null")


def test_a_null_struct_hidden_by_a_null_list_is_not_written() -> None:
    items = pa.array([None, {"a": 1}], pa.struct([("a", pa.int8())]))
    lists = pa.ListArray.from_arrays(
        pa.array([0, 1, 2], pa.int32()), items, mask=pa.array([True, False])
    )
    _ = round_trip(keyed({"l": lists}))


def test_a_null_under_a_non_null_field_is_refused_at_any_depth() -> None:
    not_null: Any = pa.field("v", pa.int64(), nullable=False)
    for arrow_type, value in (
        (pa.list_(not_null), [[1, None]]),
        (pa.list_(not_null, 2), [[1, None]]),
        (pa.struct([not_null]), [{"v": None}]),
        (pa.list_(pa.struct([not_null])), [[{"v": 1}, {"v": None}]]),
    ):
        table = keyed({"x": pa.array(value, arrow_type)})
        error = refused(table, "'x'", "non-nullable")
        assert error.path[0] == "x" and len(error.path) > 1
    refused(
        keyed(
            {"x": pa.array([None], pa.int8())},
            [not_null.with_name("x").with_type(pa.int8())],
        ),
        "'x'",
    )


def test_a_null_list_over_non_null_items_is_kept() -> None:
    item = pa.field("item", pa.int64(), nullable=False)
    fixed = pa.array([[1, 2], None, [3, 4]], pa.list_(item, 2))
    _ = round_trip(keyed({"a": fixed, "b": pa.array([[1], None, []], pa.list_(item))}))


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.float16(),
        pa.decimal32(5, 2),
        pa.decimal64(10, 2),
        pa.decimal256(40, 2),
        pa.date64(),
        pa.time32("s"),
        pa.time64("ns"),
        pa.month_day_nano_interval(),
        pa.dictionary(pa.int32(), pa.string()),
        pa.run_end_encoded(pa.int32(), pa.string()),
        pa.list_view(pa.int64()),
        pa.large_list_view(pa.int64()),
        pa.map_(pa.string(), pa.int64()),
        map_of(pa.field("k", pa.string(), False), pa.field("v", pa.int8())),
        pa.sparse_union([pa.field("a", pa.int8())]),
        pa.struct([]),
    ],
    ids=str,
)
def test_a_type_the_store_does_not_keep_is_refused_at_every_depth(
    arrow_type: pa.DataType,
) -> None:
    for wrapped in (
        arrow_type,
        pa.list_(arrow_type),
        pa.list_(arrow_type, 2),
        pa.struct([("ok", pa.int8()), ("s", arrow_type)]),
        pa.list_(pa.struct([("s", pa.large_list(arrow_type))])),
    ):
        schema = schema_of(
            [("a", pa.int64()), ("bad", wrapped), (KEY, pa.large_binary())]
        )
        for call in (store.physical_schema, store.logical_schema, entries):
            with pytest.raises(store.UnstorableInSpiralError, match="'bad'"):
                call(schema)


def test_what_awaits_a_live_probe_is_listed() -> None:
    assert set(store.AWAITING_LIVE_PROBE) == {
        "float16", "decimal256", "date64", "time", "map", "-0.0", "NaN bits",
    }  # fmt: skip


@pytest.mark.parametrize(
    ("fields", "words"),
    [
        ([("a", pa.int8())], "no key column"),
        ([(KEY, pa.large_binary())], "no column beside"),
        ([(KEY, pa.int64()), ("a", pa.int8())], "must be binary"),
        ([(KEY, pa.binary()), ("a", pa.int8()), ("a", pa.int8())], "more than once"),
        ([(KEY, pa.binary()), (KEY, pa.binary()), ("a", pa.int8())], "more than once"),
        (
            [
                (KEY, pa.binary()),
                ("a", pa.struct([("x", pa.int8()), ("x", pa.int8())])),
            ],
            "'a'",
        ),
    ],
)
def test_a_table_the_store_cannot_hold_is_refused(fields: Any, words: str) -> None:
    with pytest.raises(store.UnstorableInSpiralError, match=words):
        store.physical_schema(schema_of(fields))


# ------------------------------------------------------------------- logical types


_ALIVE: list[Any] = []
"""Arrow asks the class to read a type whose Python object is gone: keep them all."""


class Labelled(pa.ExtensionType):
    """An extension type registered or not, whose reading may fail."""

    def __init__(self, storage: Any, name: str, metadata: bytes) -> None:
        self._metadata = metadata
        _ALIVE.append(self)
        super().__init__(storage, name)

    def __arrow_ext_serialize__(self) -> bytes:
        return self._metadata

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
        raise AssertionError("the codec reads identifiers, never registered types")


@pytest.mark.parametrize("metadata", [b"", b'{"version":1}', b"\xff\x00\xfe"])
@pytest.mark.parametrize("registered", [False, True])
def test_a_logical_type_is_kept_by_its_identifier(
    metadata: bytes, registered: bool
) -> None:
    """Live and annotated give one schema; a registered type is never consulted."""
    storage = pa.struct(
        [pa.field("z.1", timestamp("us", tz="UTC")), ("a", pa.binary(2))]
    )
    name = f"test.itl1446.{metadata.hex()}"
    live = Labelled(storage, name, metadata)
    values = [{"z.1": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), "a": b"xy"}] * 2
    stored = pa.array(values, storage)
    annotated = pa.field(
        "c",
        storage,
        True,
        {NAME_KEY: name.encode(), METADATA_KEY: metadata, b"k": b"v"},
    )  # pyright: ignore[reportArgumentType]
    if registered:
        pa.register_extension_type(cast(Any, live))
    try:
        one = keyed(
            {"c": pa.ExtensionArray.from_storage(live, stored), "d": pa.array([1, 2])},
            [pa.field("c", live, True, {"k": "v"}), pa.field("d", pa.int64())],
        )
        two = keyed(
            {"c": stored, "d": pa.array([1, 2])}, [annotated, pa.field("d", pa.int64())]
        )
        assert store.encode_rows(one).equals(store.encode_rows(two))
        assert_same(round_trip(one), round_trip(two))
        schema = read(entries(one.schema)).schema
        assert schema.field("c").metadata == annotated.metadata
        assert pa.types.is_struct(schema.field("c").type)
        back = store.decode_rows(as_spiral_returns(store.encode_rows(one)), one.schema)
        assert by_id(back).column("c").chunk(0).type == live
    finally:
        if registered:
            pa.unregister_extension_type(name)


def test_another_identifier_is_another_schema() -> None:
    def manifest(name: str, metadata: bytes) -> store.StoreManifest:
        live = Labelled(pa.large_string(), name, metadata)
        return store.StoreManifest(
            ("p",), schema_of([("c", live), (KEY, pa.large_binary())])
        )

    base = manifest("n", b"1")
    assert store.manifest_difference(base, manifest("n", b"1")) is None
    for other in (manifest("n", b"2"), manifest("m", b"1"), manifest("n", b"")):
        difference = store.manifest_difference(base, other)
        assert difference is not None and "'c'" in difference


def test_a_logical_type_below_a_plain_type_is_refused() -> None:
    live = Labelled(pa.int8(), "test.nested", b"")
    annotated = pa.field("v", pa.int8(), metadata={NAME_KEY: b"n"})
    inners: Any = (pa.field("v", live), annotated, pa.field("v", pa.uuid()))
    for inner in inners:
        for arrow_type in (
            pa.list_(inner),
            pa.list_(inner, 2),
            pa.struct([inner]),
            pa.list_(pa.struct([inner])),
            map_of(pa.string(), inner),
        ):
            schema = schema_of([("c", arrow_type), (KEY, pa.large_binary())])
            with pytest.raises(store.UnstorableInSpiralError, match="nested"):
                store.logical_schema(schema)
    # A live type whose field is annotated as another one is two logical types.
    both = pa.field("c", live, metadata={NAME_KEY: b"n"})
    with pytest.raises(store.UnstorableInSpiralError, match="nested"):
        store.logical_schema(schema_of([both, (KEY, pa.large_binary())]))


def test_arrows_own_uuid_is_kept_as_its_identifier_and_restored_live() -> None:
    values = [uuid.UUID(int=i + 1).bytes for i in range(2)]
    column = pa.array(values, pa.uuid())
    table = keyed({"u": column, "v": column})
    schema = read(entries(table.schema)).schema
    assert schema.field("u").metadata == {NAME_KEY: b"arrow.uuid", METADATA_KEY: b""}
    back = store.decode_rows(as_spiral_returns(store.encode_rows(table)), table.schema)
    assert_same(back, table)


# ------------------------------------------------------------------- rows, as read


def _stored(table: pa.Table) -> pa.Table:
    return as_spiral_returns(store.encode_rows(table))


def test_rows_that_are_not_what_was_written_are_refused() -> None:
    inner = pa.struct([pa.field("a.b", pa.int8(), False), pa.field("c", pa.int8())])
    fields = [
        pa.field("n", pa.int64(), False),
        pa.field("f", pa.binary(2)),
        pa.field("z", pa.null()),
        pa.field("t", timestamp("us", tz="UTC")),
        pa.field("s", inner),
    ]
    table = keyed(
        {
            "n": pa.array([1]),
            "f": pa.array([b"ab"], pa.binary(2)),
            "z": pa.nulls(1),
            "t": pa.array([1], timestamp("us", tz="UTC")),
            "s": pa.array([{"a.b": 1, "c": 2}], inner),
        },
        fields,
    )
    schema, good = table.schema, _stored(table)
    assert_same(store.decode_rows(good, schema), large_table(table))

    def bad(name: str, array: Any, *words: str) -> None:
        rows = good.set_column(good.column_names.index(name), name, array)
        with pytest.raises(store.StoredRowsError) as info:
            store.decode_rows(rows, schema)
        assert all(w in str(info.value) for w in (f"'{name}'", *words))

    bad("n", pa.array([None], pa.int64()), "null")
    bad("n", pa.array([1], pa.int32()), "int32")
    bad("f", pa.array([b"abc"], pa.large_binary()), "2 bytes")
    bad("z", pa.array([0], pa.int8()), "nulls")
    bad("t", pa.array([1], timestamp("ms")), "timestamp[ms]")
    bad("t", pa.array([1], pa.int64()), "int64")
    bad("s", pa.array([{"c": 1}], pa.struct([("c", pa.int8())])), "missing")
    bad(
        "s",
        pa.array(
            [{"c": 1, "ox_a;2Eb": 1, "x": 1}],
            pa.struct([("c", pa.int8()), ("ox_a;2Eb", pa.int8()), ("x", pa.int8())]),
        ),
        "not expected ['x']",
    )
    bad("s", pa.array([1]), "int64")
    bad(
        "s",
        pa.array([None], pa.struct([("c", pa.int8()), ("ox_a;2Eb", pa.int8())])),
        "null struct",
    )
    bad(
        "s",
        pa.array(
            [{"c": 1, "ox_a;2Eb": None}],
            pa.struct([("c", pa.int8()), ("ox_a;2Eb", pa.int8())]),
        ),
        "a.b",
        "null",
    )
    for rows in (good.drop_columns(["n"]), good.append_column("m", pa.array([1]))):
        with pytest.raises(store.StoredRowsError, match="'(n|m)'"):
            store.decode_rows(rows, schema)
    repeated = pa.Table.from_arrays(
        [*good.columns, good.column("n")], names=[*good.column_names, "n"]
    )
    with pytest.raises(store.StoredRowsError, match="repeated"):
        store.decode_rows(repeated, schema)


def large_table(table: pa.Table) -> pa.Table:
    return table.cast(large(table.schema))


@pytest.mark.parametrize("size", [0, 3])
def test_a_fixed_size_list_of_another_size_is_refused(size: int) -> None:
    table = keyed({"l": pa.array([[1, 2]], pa.list_(pa.int8(), 2))})
    other = pa.array([[1] * size], pa.list_(pa.int8(), size))
    rows = _stored(table).set_column(1, "l", other)
    with pytest.raises(store.StoredRowsError, match="'l'"):
        store.decode_rows(rows, table.schema)


# ------------------------------------------------------------------------ manifest


def wide_schema(columns: int = 200) -> pa.Schema:
    """Several tags, data columns and nested ones, each with metadata."""
    fields: list[Any] = [pa.field(KEY, pa.large_binary(), False)]
    for i in range(columns - 1):
        arrow_type: Any = [
            pa.int64(),
            timestamp("ns", tz="Asia/Kathmandu"),
            pa.struct([pa.field("__a.b", pa.string(), False, {"depth": "1"})]),
            pa.list_(pa.field("odd", pa.binary(16), False, {"unit": "m" * 30})),
            Labelled(pa.large_string(), f"test.wide.{i}", b'{"version":1}'),
        ][i % 5]
        metadata = {"orcapod:data_tag": "1", "note": f"{i:040d}", b"\xff": b"\x00\xfe"}
        fields.append(
            pa.field(f"__col.{i}", arrow_type, i % 3 > 0, cast(Any, metadata))
        )
    return schema_of(fields, metadata={"table": "x" * 100})


def test_a_manifest_of_200_columns_with_metadata() -> None:
    schema = wide_schema()
    assert len(schema) == 200
    path = ("run", "a/b", "", "é", '"', "\ud800")
    metadata = entries(schema, path, other=b"kept")
    ours = {k: v for k, v in metadata.items() if k.startswith(store.MANIFEST_PREFIX)}
    assert 3 < len(ours) <= 1024
    for key, value in ours.items():
        assert len(key) <= 90 and key.isascii()
        assert len(key) + len(value) <= 8192
    manifest = read(metadata)
    assert manifest.record_path == path
    assert manifest.schema.equals(store.logical_schema(schema), check_metadata=True)
    for a, b in zip(store.logical_schema(schema), manifest.schema):
        assert field_difference(a, b, a.name) is None


def test_a_manifest_does_not_depend_on_the_order_of_the_metadata() -> None:
    metadata = entries(wide_schema(), other=b"x")
    items = list(metadata.items())
    for seed in range(5):
        random.Random(seed).shuffle(items)
        assert read(dict(items)) == read(metadata)


def test_a_payload_of_another_pyarrow_is_the_same_manifest(monkeypatch: Any) -> None:
    """Other bytes (field metadata written in another order) decode to one schema: the
    two manifests agree, and every reader takes the one of the smallest digest."""
    schema = wide_schema(20)
    first = entries(schema)

    def reordered(field: Any) -> Any:
        t = field.type
        if pa.types.is_struct(t):
            t = pa.struct([reordered(f) for f in t])
        elif pa.types.is_large_list(t):
            t = pa.large_list(reordered(t.value_field))
        metadata = dict(reversed(list((field.metadata or {}).items())))
        return pa.field(field.name, t, field.nullable, cast(Any, metadata or None))

    original = store.logical_schema
    monkeypatch.setattr(
        store,
        "logical_schema",
        lambda s: schema_of([reordered(f) for f in original(s)], metadata=s.metadata),
    )
    second = entries(schema)
    monkeypatch.undo()
    assert not set(first) & set(second)
    both = read({**second, **first})
    assert store.manifest_difference(both, read(first)) is None
    assert store.manifest_difference(read(first), read(second)) is None
    smallest = min(
        k for k in {**first, **second} if "/" not in k[len(store.MANIFEST_PREFIX) :]
    )
    expected = read(first if smallest in first else second)
    assert list(both.schema.field(1).metadata or {}) == list(
        expected.schema.field(1).metadata or {}
    )


def test_every_row_of_the_matrix_round_trips_in_one_manifest(tmp_path: Path) -> None:
    fields: list[Any] = [pa.field(KEY, pa.large_binary(), False)]
    for row in ROWS:
        if row.label in REFUSED_ROWS.keys() - REFUSED_BY_VALUE:
            continue
        if row.provisional:
            continue
        for name in ("x", "y"):
            fields.append(
                build_table(row, tmp_path)
                .schema.field(name)
                .with_name(f"{row.label}.{name}")
            )
    for order in (fields, list(reversed(fields))):
        schema = schema_of(order)
        got = read(entries(schema)).schema
        assert got.names == schema.names
        for a, b in zip(store.logical_schema(schema), got):
            assert field_difference(a, b, a.name) is None


def test_metadata_keys_that_look_like_the_marks_are_kept() -> None:
    metadata = {
        b"orcapod:key:ARROW:extension:name": b"1",
        b"orcapod:key:": b"2",
        b"orcapod:key:orcapod:key:x": b"3",
        b"ARROW:extension:other": b"4",
        NAME_KEY: b"n",
        METADATA_KEY: b"m",
    }
    field = pa.field("a", pa.int8(), metadata=cast(Any, metadata))
    schema = schema_of([field, (KEY, pa.large_binary())])
    assert read(entries(schema)).schema.field("a").metadata == metadata


BASE: list[Any] = [
    pa.field("a", pa.struct([pa.field("x", pa.int8()), pa.field("y", pa.int8())])),
    pa.field("l", pa.large_list(pa.field("item", timestamp("us", tz="UTC")))),
    pa.field("f", pa.list_(pa.field("item", pa.int8()), 2)),
    pa.field(KEY, pa.large_binary(), False),
]


def test_the_order_of_columns_is_no_difference() -> None:
    one = entries(schema_of(BASE))
    for order in ([3, 2, 1, 0], [1, 0, 3, 2]):
        other = entries(schema_of([BASE[i] for i in order]))
        assert read({**one, **other}) == read({**other, **one})


@pytest.mark.parametrize(
    "changed",
    [
        BASE[0].with_name("b"),
        BASE[0].with_nullable(False),
        BASE[0].with_metadata({"k": "v"}),
        BASE[0].with_type(pa.struct([("x", pa.int8()), ("z", pa.int8())])),
        BASE[0].with_type(pa.struct([("y", pa.int8()), ("x", pa.int8())])),
        BASE[0].with_type(pa.struct([("x", pa.int8())])),
        BASE[0].with_type(
            pa.struct([pa.field("x", pa.int8(), False), ("y", pa.int8())])
        ),
        BASE[0].with_type(
            pa.struct([pa.field("x", pa.int8(), metadata={"k": "v"}), ("y", pa.int8())])
        ),
        BASE[0].with_type(pa.int8()),
        BASE[1].with_type(pa.large_list(pa.field("other", timestamp("us", tz="UTC")))),
        BASE[1].with_type(pa.large_list(timestamp("us", tz="Etc/UTC"))),
        BASE[1].with_type(pa.large_list(timestamp("us"))),
        BASE[1].with_type(pa.large_list(timestamp("ms", tz="UTC"))),
        BASE[2].with_type(pa.list_(pa.int8(), 3)),
        BASE[2].with_type(pa.large_list(pa.int8())),
        pa.field("a", pa.null()),
    ],
    ids=str,
)
def test_two_manifests_that_disagree_are_refused(changed: Any) -> None:
    index = next((i for i, f in enumerate(BASE) if f.name == changed.name), 0)
    fields = list(BASE)
    fields[index] = changed
    one, other = entries(schema_of(BASE)), entries(schema_of(fields))
    assert store.manifest_difference(read(one), read(other)) is not None
    for both in ({**one, **other}, {**other, **one}):
        with pytest.raises(store.ManifestConflictError) as info:
            store.read_manifest(both)
        for digest in (k for k in both if k.count("/") == 1):
            assert digest.split("/")[1] in str(info.value)


def test_the_record_path_and_schema_metadata_are_compared() -> None:
    schema = schema_of(BASE)
    one = entries(schema, ("a", "b"))
    for other in (
        entries(schema, ("a/b",)),
        entries(schema, ("b", "a")),
        entries(schema.with_metadata({"k": "v"}), ("a", "b")),
        entries(schema_of([*BASE, ("more", pa.int8())]), ("a", "b")),
    ):
        with pytest.raises(store.ManifestConflictError):
            store.read_manifest({**one, **other})


def test_three_manifests_one_of_which_disagrees_are_refused() -> None:
    for seed in range(6):
        parts = [
            entries(schema_of(BASE)),
            entries(schema_of(list(reversed(BASE)))),
            entries(schema_of([*BASE[:3], pa.field(KEY, pa.large_binary())])),
        ]
        random.Random(seed).shuffle(parts)
        with pytest.raises(store.ManifestConflictError):
            store.read_manifest({k: v for part in parts for k, v in part.items()})


def test_a_table_without_a_manifest_has_none() -> None:
    assert store.read_manifest({}) is None
    assert store.read_manifest({"__arrow_metadata__": b"{}", "x": b""}) is None
    chunks, header = store.manifest_entries(
        store.StoreManifest(("a",), schema_of(BASE))
    )
    assert store.read_manifest(chunks) is None  # the header never came
    assert read({**chunks, **header}) is not None
    # A second writer's chunks without their header change nothing.
    more, _ = store.manifest_entries(store.StoreManifest(("z",), schema_of(BASE)))
    assert read({**chunks, **header, **more}).record_path == ("a",)


def _rewritten(metadata: dict[str, bytes], **changes: Any) -> dict[str, bytes]:
    key = next(k for k in metadata if k.count("/") == 1)
    return {
        **metadata,
        key: json.dumps({**json.loads(metadata[key]), **changes}).encode(),
    }


def _payload(**changes: Any) -> dict[str, bytes]:
    """A manifest whose payload is changed, under the digest it then has."""
    chunks, _ = store.manifest_entries(store.StoreManifest(("a",), schema_of(BASE)))
    body = {**json.loads(b"".join(chunks.values())), **changes}
    payload = json.dumps(body).encode()
    digest = store._digest(payload)  # pyright: ignore[reportPrivateUsage]
    header = {
        "chunks": 1,
        "format": "orcapod.spiral_store",
        "sha256": digest,
        "version": 1,
    }
    return {
        f"{store.MANIFEST_PREFIX}{digest}": json.dumps(header).encode(),
        f"{store.MANIFEST_PREFIX}{digest}/0": payload,
    }


def _ipc(schema: pa.Schema) -> str:
    return base64.b64encode(schema.serialize().to_pybytes()).decode()


def test_the_changed_payload_helper_builds_a_readable_manifest() -> None:
    assert read(_payload()).record_path == ("a",)


@pytest.mark.parametrize(
    "make",
    [
        lambda m: {k: v for k, v in m.items() if not k.endswith("/0")},
        lambda m: {k: (v + b" " if k.endswith("/0") else v) for k, v in m.items()},
        lambda m: {k: (b"[]" if k.count("/") == 1 else v) for k, v in m.items()},
        lambda m: {k: (b"\xff" if k.count("/") == 1 else v) for k, v in m.items()},
        lambda m: _rewritten(m, format="other"),
        lambda m: _rewritten(m, version=2),
        lambda m: _rewritten(m, version=True),
        lambda m: _rewritten(m, chunks=0),
        lambda m: _rewritten(m, chunks="1"),
        lambda m: _rewritten(m, chunks=10**9),
        lambda m: _rewritten(m, chunks=2),
        lambda m: _rewritten(m, sha256="0" * 64),
        lambda m: {**m, store.MANIFEST_PREFIX + "other": b""},
        lambda m: {**m, store.MANIFEST_PREFIX + "0" * 64 + "/01": b""},
        lambda m: _payload(version=2),
        lambda m: _payload(record_path="a"),
        lambda m: _payload(record_path=[1]),
        lambda m: _payload(schema=1),
        lambda m: _payload(schema="!!"),
        lambda m: _payload(schema=base64.b64encode(b"junk").decode()),
        lambda m: _payload(schema=_ipc(schema_of([("a", pa.int8())]))),
        lambda m: _payload(schema=_ipc(schema_of([*BASE, ("m", pa.float16())]))),
    ],
)
def test_a_manifest_that_cannot_be_read_in_full_is_an_error(make: Any) -> None:
    metadata = make(entries(schema_of(BASE)))
    with pytest.raises(store.ManifestError):
        store.read_manifest(metadata)


def test_a_manifest_over_the_limit_is_refused() -> None:
    field = pa.field("a", pa.int8(), metadata={"k": "v" * 7_000_000})
    with pytest.raises(store.ManifestError, match="1024"):
        entries(schema_of([field, (KEY, pa.large_binary())]))
    small = pa.field("a", pa.int8(), metadata={"k": "v" * 1_000_000})
    schema = schema_of([small, (KEY, pa.large_binary())])
    assert read(entries(schema)).schema.equals(schema, check_metadata=True)


def test_writing_the_same_manifest_twice_changes_nothing() -> None:
    one = entries(wide_schema())
    assert entries(wide_schema()) == one
    assert entries(read(one).schema) == one


def test_the_codec_needs_no_pyspiral() -> None:
    code = (
        "import sys, orcapod.databases.spiral_store\n"
        "assert not [m for m in sys.modules if m.split('.')[0] == 'spiral']"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0
