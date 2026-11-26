"""The codec of a SpiralDB store table: names, types, rows and the manifest (ITL-1446, ITL-1497).

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
import re
import subprocess
import sys
from collections.abc import Callable
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


PATH = ("run", "fn", "pdb_v1")


def new(schema: pa.Schema, path: tuple[str, ...] = PATH) -> store.StoreManifest:
    return store.new_manifest(path, "function_pipeline", schema)


def entries(schema: pa.Schema, path: tuple[str, ...] = PATH) -> bytes:
    """The recorded manifest of a table of ``schema`` at ``path``."""
    return store.manifest_bytes(new(schema, path))


def read(recorded: bytes, path: tuple[str, ...] = PATH) -> store.StoreManifest:
    return store.read_manifest_bytes(recorded, path)


def decode(rows: pa.Table, schema: pa.Schema) -> pa.Table:
    """Decode through a manifest read back, giving ``schema`` (live types kept)."""
    return store.decode_rows(rows, read(entries(schema)), large(schema))


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
    manifest = read(entries(rows.schema))
    schema = manifest.schema
    if live:
        converter = get_default_context().type_converter
        schema = declaration_form_schema(schema, converter)
    else:
        expected_schema = store.logical_schema(rows.schema)
    result = None
    assert rows.num_rows
    for piece in (rows, rows.slice(1), rows.slice(0, 1)):
        if not piece.num_rows:
            continue
        encoded = store.encode_rows(piece)
        assert encoded.schema.equals(
            store.physical_schema(rows.schema), check_metadata=True
        )
        _assert_physical(encoded.schema)
        encoded.validate()  # full: pyarrow refuses a date64 off midnight
        for seed, small in ((1, False), (2, True)):
            got = store.decode_rows(
                as_spiral_returns(encoded, seed, small), manifest, schema
            )
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

    def walk(field: Any, named: bool, in_map: bool = False) -> None:
        assert (field.nullable or in_map) and not field.metadata
        if named and field.name != KEY:
            assert field.name and not field.name.startswith(("__", "$"))
            assert not set(field.name) & set(".$*\0")
        t = field.type
        assert not (
            pa.types.is_fixed_size_binary(t)
            or pa.types.is_duration(t)
            or pa.types.is_interval(t)
            or isinstance(t, pa.BaseExtensionType)
            # spiral-sdk 0.16.78: a time of day outside one day is refused, so times are
            # written as integers; timestamps, zoned or not, are written as they are.
            or pa.types.is_time(t)
        )
        if pa.types.is_map(t):
            walk(t.key_field, False, in_map=True)
            walk(t.item_field, False)
            return
        for i in range(t.num_fields):
            walk(t.field(i), pa.types.is_struct(t))

    for f in schema:
        walk(f, True)


def refused(rows: pa.Table, *words: str) -> store.UnstorableInSpiralError:
    with pytest.raises(store.UnstorableInSpiralError) as info:
        _ = store.encode_rows(rows)
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
    assert decoded(stored) == name


def decoded(stored: str) -> str:
    """The inverse of ``encode_name``, which only the tests need."""
    if not stored.startswith("ox_"):
        return stored
    return re.sub(";([0-9A-F]{2})", lambda m: chr(int(m[1], 16)), stored[3:])


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
        assert decoded(stored) == name
        assert stored and not stored.startswith("__")
        assert not any(c in ".$*\x7f" or ord(c) < 32 for c in stored)


def test_an_already_stored_name_is_itself_encoded_injectively() -> None:
    names = sorted(adversarial_names(5_000))
    once = [store.encode_name(n) for n in names]
    twice = [store.encode_name(n) for n in once]
    assert len(set(twice)) == len(names)
    assert [decoded(decoded(t)) for t in twice] == names


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
        _ = decode(renamed, table.schema)


@pytest.mark.parametrize("name", ["\ud800", "a\udfff", "__\ud800.b"])
def test_a_lone_surrogate_is_refused(name: str) -> None:
    with pytest.raises(store.UnstorableInSpiralError, match="UTF-8"):
        _ = store.encode_name(name)
    with pytest.raises(store.UnstorableInSpiralError, match="UTF-8"):
        _ = new(schema_of([("a", pa.int8()), (KEY, pa.large_binary())]), (name,))


def test_a_name_that_is_not_utf8_is_refused_at_every_depth() -> None:
    bad: Any = pa.field(cast(Any, b"a\xff"), pa.int8())
    for field in (
        bad,
        pa.field("s", pa.struct([bad])),
        pa.field("l", pa.list_(pa.struct([bad]))),
        pa.field("i", pa.list_(bad)),
    ):
        with pytest.raises(store.UnstorableInSpiralError, match="UTF-8"):
            _ = store.logical_schema(schema_of([field, (KEY, pa.large_binary())]))


def wrappings(inner: Any) -> list[Any]:
    """``inner`` alone, below each kind of nesting, and as a logical type's storage."""
    return [
        inner,
        pa.list_(inner),
        pa.struct([("s", pa.large_list(inner))]),
        Labelled(inner, "test.storage", b""),
        Labelled(pa.large_list(inner), "test.storage", b""),
    ]


def test_duplicate_sibling_names_are_refused_at_every_depth() -> None:
    twice = pa.struct([("a.b", pa.int8()), ("c", pa.int8()), ("a.b", pa.int8())])
    for ty in wrappings(twice):
        with pytest.raises(store.UnstorableInSpiralError, match="more than once"):
            _ = store.logical_schema(schema_of([("x", ty), (KEY, pa.large_binary())]))
    same_in_two_structs = pa.struct([("a", pa.struct([("a", pa.int8())]))])
    _ = store.logical_schema(
        schema_of([("a", same_in_two_structs), (KEY, pa.large_binary())])
    )


def test_names_that_differ_only_in_case_or_normal_form_are_kept_apart() -> None:
    """The service folds neither (measured live)."""
    names = ["A", "a", "a.B", "a.b", "__A", "__a", "\u00e9", "e\u0301", "a.\u00e9"]
    for order in (names, list(reversed(names))):
        inner = pa.array(
            [{n: i for i, n in enumerate(order)}],
            pa.struct([(n, pa.int8()) for n in order]),
        )
        columns = {n: pa.array([i]) for i, n in enumerate(order)}
        _ = round_trip(keyed({**columns, "s": inner}))


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
        _ = refused(keyed({"c": array}), "'c'", "null struct")


# --------------------------------------------------------------------------- types

REFUSED_ROWS = {
    "struct<> (no fields)": "no fields",
    "fixed_size_binary[4]": "fixed-size binary",
    "uuid (Arrow's canonical extension type)": "fixed-size binary",
    "map<k: string, v: int64>, keys sorted": "named key and value",
    "map<string, int64 {unit: m}> (item metadata)": "metadata inside a map",
    "map<string {unit: m}, int64> (key metadata)": "metadata inside a map",
    "map<string, struct<a: int64 {unit: m}>> (metadata inside the item)": "metadata inside a map",
    **{r.label: "duration" for r in ROWS if r.label.startswith("duration")},
}
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
        _ = refused(rows, REFUSED_ROWS[row.label], "'x'")
        return
    if row.label in NULL_STRUCT_ROWS:
        _ = refused(rows, "null struct", "'y'")
        rows = rows.drop_columns(["y"])
    _ = round_trip(rows)
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
        ("float16", pa.float16(), [0.5, -0.0, math.nan]),
        ("decimal128", pa.decimal128(38, 4), [D("1.2500"), D("-9.0001"), D("0")]),
        ("decimal256", pa.decimal256(70, 4), [D("1.2500"), D("-9.0001"), D("0")]),
        ("date64", pa.date64(), [-(2**63), 1, 2**63 - 1]),
        ("decimal32", pa.decimal32(9, 2), [D("1.25"), D("-9999999.99"), D("0")]),
        (
            "decimal64",
            pa.decimal64(18, 2),
            [D("1.25"), D("-9999999999999999.99"), D("0")],
        ),
        ("null", pa.null(), [None, None, None]),
        ("time32_s", pa.time32("s"), [0, 86_399, 1]),
        ("time32_ms", pa.time32("ms"), [0, 86_399_999, 1]),
        ("time64_us", pa.time64("us"), [0, 86_400 * 10**6 - 1, 1]),
        ("time64_ns", pa.time64("ns"), [0, 86_400 * 10**9 - 1, 1]),
        (
            "date32",
            pa.date32(),
            [dt.date(1, 1, 1), dt.date(9999, 12, 31), dt.date(1970, 1, 1)],
        ),
        ("large_string", pa.large_string(), ["", "é\x00", "x" * 100]),
        ("string", pa.string(), ["", "é\x00", "y"]),
        ("binary", pa.binary(), [b"", b"\x00\xff", b"a"]),
        *(
            (f"ts_{u}_{z}", timestamp(u, tz=z), [-1, 0, 2**40 + 1])
            for u in ("s", "ms", "us", "ns")
            for z in (None, "UTC", "Asia/Kathmandu", "-03:30")
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
    plain_item = item.remove_metadata()
    v, n = values, len(values)
    shapes: dict[str, tuple[pa.DataType, list[Any]]] = {
        "plain": (arrow_type, v),
        "list": (pa.list_(item), [v, [], None, v[:1]]),
        "large": (pa.large_list(item), [None, v[:2], v, []]),
        "fixed": (pa.list_(item, 2), [v[:2], None, v[2:], v[1:3]]),
        "fixed_fixed": (
            pa.list_(pa.list_(item, 1), 2),
            [[v[:1], None], None, [v[1:2], v[2:3]], [v[:1], v[:1]]],
        ),
        "fixed0": (pa.list_(item, 0), [[], None, [], []]),
        "list_fixed": (pa.list_(pa.list_(item, 1)), [[v[:1], None], [], None, [v[3:]]]),
        "map": (
            map_of(pa.string(), item.with_name("value").remove_metadata()),
            [[("b", v[0]), ("a", v[1])], None, [], [("", x) for x in v]],
        ),
        "map_sorted_list": (
            map_of(pa.binary(), pa.list_(plain_item), keys_sorted=True),
            [None, [(b"a", v), (b"b", None)], [(b"", [])], []],
        ),
        "list_map": (
            pa.list_(map_of(pa.int16(), plain_item.with_name("value"))),
            [[[(1, v[0])], None, []], None, [], [[(2, x)] for x in v]],
        ),
        "map_struct": (
            map_of(
                pa.string(),
                pa.struct(
                    [pa.field("z", pa.int8()), pa.field("a.b", arrow_type, nullable)]
                ),
            ),
            [
                None,
                [("k", {"z": 1, "a.b": v[0]})],
                [],
                [("", {"z": None, "a.b": v[1]})],
            ],
        ),
        "fixed_struct": (
            pa.list_(pa.struct([pa.field("z", pa.int8()), leaf]), 1),
            [[{"z": 1, "v.1": x}] for x in v],
        ),
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
        "deep": (
            pa.list_(
                pa.struct([pa.field("s", pa.struct([pa.field("l", pa.list_(item))]))])
            ),
            [[{"s": {"l": v}}, {"s": {"l": None}}], [], None, [{"s": {"l": []}}]],
        ),
    }
    if not nullable or pa.types.is_null(arrow_type):
        # pyarrow cannot cast a null fixed-size list over non-null items, or a sliced
        # list of structs with a null-typed field, so the test
        # could not state what it expects of the small variants.
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
    "zone", [None, "UTC", "Europe/Berlin", "Asia/Kathmandu", "+05:45", "-03:30"]
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
    }
    got = by_id(round_trip(keyed(columns)))
    assert got.column("t").cast(pa.int64()).to_pylist() == values
    assert got.schema.field("t").type == ts
    encoded = store.encode_rows(keyed(columns))
    # Written as they are (measured live with spiral-sdk 0.16.78: every int64, every
    # unit, zoned or not, bit for bit).
    assert encoded.schema.field("t").type == ts
    assert encoded.column("t").cast(pa.int64()).to_pylist() == values


def _float(bits: int, width: int) -> Any:
    ints = np.array([bits], dtype=f"u{width // 8}")
    return pa.array(ints).view(pa.float32() if width == 32 else pa.float64())


FLOATS = {
    32: [0, 0x80000000, 1, 0x80000001, 0x7F800000, 0xFF800000, 0x7FC00000, 0x7F7FFFFF]
    + [0x7FC00001, 0xFFC00000, 0x7F800001, 0xFF800001, 0x7FFFFFFF],
    64: [0, 1 << 63, 1, (1 << 63) + 1, 0x7FF << 52, 0xFFF << 52, 0x7FF8 << 48]
    + [(0x7FF8 << 48) + 1, 0xFFF8 << 48, (0x7FF << 52) + 1, (1 << 63) - 1],
}


@pytest.mark.parametrize("width", [16, 32, 64])
@pytest.mark.parametrize("nested", ["plain", "list", "fixed", "struct"])
def test_float_bits_are_kept(width: int, nested: str) -> None:
    """Negative zero and every NaN payload, signalling ones included, bit for bit."""
    if width == 16:  # every float16 there is
        array: Any = pa.array(np.arange(2**16, dtype=np.uint16)).view(pa.float16())
    else:
        array = pa.concat_arrays([_float(b, width) for b in FLOATS[width]])
    if nested == "list":
        array = pa.ListArray.from_arrays(
            pa.array(range(len(array) + 1), pa.int32()), array
        )
    elif nested == "fixed":
        array = pa.FixedSizeListArray.from_arrays(array, 1)
    elif nested == "struct":
        array = pa.StructArray.from_arrays([array], names=["f"])
    _ = round_trip(keyed({"f": array}))


def test_datetime_max_and_min_are_kept() -> None:
    """pyspiral's service refused a timestamp after 9999-12-30; spiral-sdk's keeps it."""
    utc = dt.timezone.utc
    values = [
        dt.datetime.max,
        dt.datetime.min,
        dt.datetime(9999, 12, 30, 22, 0, 1),
        None,
    ]
    table = keyed(
        {
            "naive": pa.array(values, timestamp("us")),
            "utc": pa.array(
                [v and v.replace(tzinfo=utc) for v in values], timestamp("us", tz="UTC")
            ),
        }
    )
    got = by_id(round_trip(table))
    assert got.column("naive").to_pylist() == values


@pytest.mark.parametrize("arrow_type", [pa.time32("s"), pa.time32("ms")], ids=str)
def test_every_time32_is_kept(arrow_type: Any) -> None:
    """Also one outside a day, which the service refuses as a time."""
    ticks = [0, 86_399, 86_400, 86_400_000, -1, -(2**31), 2**31 - 1, None]
    array: Any = pa.array(ticks, pa.int32()).view(arrow_type)
    got: Any = by_id(round_trip(keyed({"t": array})))
    assert got.column("t").combine_chunks().view(pa.int32()).to_pylist() == ticks


@pytest.mark.parametrize("arrow_type", [pa.time64("us"), pa.time64("ns")], ids=str)
@pytest.mark.parametrize("nested", ["plain", "list", "fixed", "struct", "map"])
def test_every_time64_is_kept(arrow_type: Any, nested: str) -> None:
    ticks = [0, 86_400 * 10**6, 86_400 * 10**9, -1, -(2**63), 2**63 - 1]
    array: Any = pa.array(ticks, pa.int64()).view(arrow_type)
    offsets: Any = pa.array(range(len(ticks) + 1), pa.int32())
    if nested == "list":
        array = pa.ListArray.from_arrays(offsets, array)
    elif nested == "fixed":
        array = pa.FixedSizeListArray.from_arrays(array, 2)
    elif nested == "struct":
        array = pa.StructArray.from_arrays([array], names=["t"])
    elif nested == "map":
        array = pa.MapArray.from_arrays(offsets, pa.array(list("abcdef")), array)
    _ = round_trip(keyed({"t": array}))


def test_what_a_null_list_or_map_hides_is_not_written() -> None:
    raw = pa.array([-1, 5], pa.int64()).view(pa.time64("us"))
    offsets: Any = pa.array([0, 1, 2], pa.int32())
    mask = pa.array([True, False])
    lists = pa.ListArray.from_arrays(offsets, raw, mask=mask)
    maps = pa.MapArray.from_arrays(offsets, pa.array(["hidden", "b"]), raw, mask=mask)
    table = keyed({"l": lists, "m": maps})
    _ = round_trip(table)
    encoded: Any = store.encode_rows(table)
    assert len(encoded.column("l").chunk(0).values) == 1
    assert encoded.column("m").chunk(0).keys.to_pylist() == ["b"]


def test_no_rows_are_never_written() -> None:
    _ = refused(keyed({"a": pa.array([1])}).slice(1), "no rows")


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
    ],
)
def test_a_null_struct_at_any_depth_is_refused(make: Any) -> None:
    _ = refused(keyed({"ok": pa.array([1, 2]), "bad": make()}), "'bad'", "null")


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
        (pa.struct([not_null]), [{"v": None}]),
        (pa.list_(pa.struct([not_null])), [[{"v": 1}, {"v": None}]]),
    ):
        table = keyed({"x": pa.array(value, arrow_type)})
        error = refused(table, "'x'", "non-nullable")
        assert error.path[0] == "x" and len(error.path) > 1
    _ = refused(
        keyed(
            {"x": pa.array([None], pa.int8())},
            [not_null.with_name("x").with_type(pa.int8())],
        ),
        "'x'",
    )


def test_a_null_list_over_non_null_items_is_kept() -> None:
    item = pa.field("item", pa.int64(), nullable=False)
    _ = round_trip(keyed({"b": pa.array([[1], None, []], pa.list_(item))}))


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.string_view(),
        pa.binary_view(),
        pa.binary(4),
        pa.duration("us"),
        pa.month_day_nano_interval(),
        pa.dictionary(pa.int32(), pa.string()),
        pa.run_end_encoded(pa.int32(), pa.string()),
        pa.list_view(pa.int64()),
        pa.large_list_view(pa.int64()),
        map_of(pa.field("k", pa.string(), False), pa.field("v", pa.int8())),
        pa.sparse_union([pa.field("a", pa.int8())]),
        pa.struct([]),
        map_of(pa.string(), pa.field("value", pa.int8(), metadata={"k": "v"})),
        map_of(pa.string(), pa.list_(pa.field("i", pa.int8(), metadata={"k": "v"}))),
    ],
    ids=str,
)
def test_a_type_the_store_does_not_keep_is_refused_at_every_depth(
    arrow_type: pa.DataType,
) -> None:
    for wrapped in (
        arrow_type,
        pa.list_(arrow_type),
        pa.struct([("ok", pa.int8()), ("s", arrow_type)]),
        pa.list_(pa.struct([("s", pa.large_list(arrow_type))])),
    ):
        schema = schema_of(
            [("a", pa.int64()), ("bad", wrapped), (KEY, pa.large_binary())]
        )
        for call in (store.physical_schema, store.logical_schema, entries):
            with pytest.raises(store.UnstorableInSpiralError, match="'bad'"):
                _ = call(schema)


def test_every_refusal_is_in_one_table() -> None:
    assert [r.what for r in store.REFUSALS] == [
        "a null struct",
        "a struct with no fields",
        "a write of no rows",
        "a map whose fields are not named key and value",
        "field metadata inside a map",
        "a string view or binary view",
        "a fixed-size binary",
        "a duration",
        "an interval",
    ]
    assert all(r.why for r in store.REFUSALS)


def test_lifting_a_type_is_one_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """A type shown to be kept natively moves from one table to the other."""
    matches = pa.types.is_duration
    table = keyed({"d": pa.array([1, None, -2], duration("us"))})
    _ = refused(table, "duration", "refuses")
    monkeypatch.setattr(
        store,
        "REFUSALS",
        tuple(r for r in store.REFUSALS if r.matches is not matches),
    )
    _ = refused(table, "keeps no type duration")
    monkeypatch.setattr(store, "_NATIVE", (*store._NATIVE, matches))  # pyright: ignore[reportPrivateUsage]
    encoded = store.encode_rows(table)
    assert_same(decode(as_spiral_returns(encoded), table.schema), table)


def test_a_null_fixed_size_list_of_structs_is_kept() -> None:
    """Measured live with spiral-sdk 0.16.78: the null row's struct slots are accepted."""
    struct = pa.struct([("a", pa.int8())])
    for ty, value in (
        (pa.list_(struct, 1), [[{"a": 1}], None]),
        (pa.list_(pa.list_(struct, 1), 1), [[[{"a": 1}]], None]),
    ):
        rows = keyed({"f": pa.array(value, ty)})
        assert by_id(round_trip(rows)).column("f").to_pylist() == value


@pytest.mark.parametrize("nullable", [True, False])
def test_a_null_key_is_refused_whatever_the_schema_says(nullable: bool) -> None:
    key = pa.array([b"a", None], pa.large_binary())
    schema = schema_of([pa.field(KEY, pa.large_binary(), nullable), ("a", pa.int8())])
    column = pa.array([1, 2], pa.int8())
    _ = refused(pa.Table.from_arrays([key, column], schema=schema), KEY)
    with pytest.raises(store.StoredRowsError, match="null key"):
        _ = decode(pa.table({KEY: key, "a": column}), schema)


@pytest.mark.parametrize(
    ("fields", "words"),
    [
        ([("a", pa.int8())], "key column"),
        ([(KEY, pa.large_binary())], "no column beside"),
        ([(KEY, pa.int64()), ("a", pa.int8())], "must be binary"),
        ([(KEY, pa.binary()), ("a", pa.int8()), ("a", pa.int8())], "more than once"),
        ([(KEY, pa.binary()), (KEY, pa.binary()), ("a", pa.int8())], "key column"),
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
        _ = store.physical_schema(schema_of(fields))


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
        [pa.field("z.1", timestamp("us", tz="UTC")), ("a", pa.binary())]
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
        back = decode(as_spiral_returns(store.encode_rows(one)), one.schema)
        assert by_id(back).column("c").chunk(0).type == live
    finally:
        if registered:
            pa.unregister_extension_type(name)


def test_another_identifier_is_another_schema() -> None:
    def schema(name: str, metadata: bytes) -> pa.Schema:
        live = Labelled(pa.large_string(), name, metadata)
        return schema_of([("c", live), (KEY, pa.large_binary())])

    base = schema("n", b"1")
    assert store.schema_difference(base, schema("n", b"1")) is None
    for other in (schema("n", b"2"), schema("m", b"1"), schema("n", b"")):
        difference = store.schema_difference(base, other)
        assert difference is not None and "'c'" in difference


def test_a_logical_type_below_a_plain_type_is_refused() -> None:
    live = Labelled(pa.int8(), "test.nested", b"")
    annotated = pa.field("v", pa.int8(), metadata={NAME_KEY: b"n"})
    inners: Any = (pa.field("v", live), annotated, pa.field("v", pa.uuid()))
    for inner in inners:
        for arrow_type in (
            pa.list_(inner),
            pa.struct([inner]),
            pa.list_(pa.struct([inner])),
            map_of(pa.string(), inner),
        ):
            schema = schema_of([("c", arrow_type), (KEY, pa.large_binary())])
            with pytest.raises(store.UnstorableInSpiralError, match="nested"):
                _ = store.logical_schema(schema)
    # A live type whose field is annotated as another one is two logical types.
    both = pa.field("c", live, metadata={NAME_KEY: b"n"})
    with pytest.raises(store.UnstorableInSpiralError, match="nested"):
        _ = store.logical_schema(schema_of([both, (KEY, pa.large_binary())]))


# ------------------------------------------------------------------- rows, as read


def _stored(table: pa.Table) -> pa.Table:
    return as_spiral_returns(store.encode_rows(table))


def large_table(table: pa.Table) -> pa.Table:
    return table.cast(large(table.schema))


def test_rows_that_are_not_what_was_written_are_refused() -> None:
    inner = pa.struct([pa.field("a.b", pa.int8(), False), pa.field("c", pa.int8())])
    fields = [
        pa.field("n", pa.int64(), False),
        pa.field("t", timestamp("us", tz="UTC")),
        pa.field("s", inner),
        pa.field("l", pa.list_(inner)),
        pa.field("f", pa.list_(pa.int8(), 2)),
        pa.field("m", pa.map_(pa.string(), pa.int8())),
    ]
    table = keyed(
        {
            "n": pa.array([1]),
            "t": pa.array([1], timestamp("us", tz="UTC")),
            "s": pa.array([{"a.b": 1, "c": 2}], inner),
            "l": pa.array([[{"a.b": 1, "c": 2}]], pa.list_(inner)),
            "f": pa.array([[1, 2]], pa.list_(pa.int8(), 2)),
            "m": pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int8())),
        },
        fields,
    )
    schema, good = table.schema, _stored(table)
    manifest = new(schema)
    assert_same(decode(good, schema), large_table(table))
    store.require_stored_schema(good.schema, manifest)

    def bad(name: str, array: Any, *words: str, value: bool = False) -> None:
        rows = good.set_column(good.column_names.index(name), name, array)
        calls: list[Any] = [lambda: decode(rows, schema)]
        if not value:
            calls.append(lambda: store.require_stored_schema(rows.schema, manifest))
        for call in calls:
            with pytest.raises(store.StoredRowsError) as info:
                call()
            assert all(w in str(info.value) for w in (f"'{name}'", *words))

    stored = pa.struct([("c", pa.int8()), ("ox_a;2Eb", pa.int8())])
    more = pa.struct([*stored, pa.field("x", pa.int8())])
    raw = pa.struct([("c", pa.int8()), ("a.b", pa.int8())])
    bad("n", pa.array([None], pa.int64()), "null", value=True)
    bad("n", pa.array([1], pa.int32()), "int32")
    bad("t", pa.array([1], timestamp("us")), "timestamp[us]")
    bad("t", pa.array([1], timestamp("us", tz="Europe/Paris")), "Europe/Paris")
    bad("t", pa.array([1], timestamp("ms", tz="UTC")), "timestamp[ms")
    bad("t", pa.array([1], pa.int64()), "int64")
    bad("t", pa.array([1], pa.int32()), "int32")
    bad("s", pa.array([{"c": 1}], pa.struct([("c", pa.int8())])), "missing")
    bad("s", pa.array([{"c": 1}], more), "not expected ['x']")
    bad("s", pa.array([{"c": 1}], raw), "not expected ['a.b']")
    bad("s", pa.array([1]), "int64")
    bad("s", pa.array([[]], pa.list_(pa.int8())), "list")
    bad("s", pa.array([None], stored), "null struct", value=True)
    bad("s", pa.array([{"c": 1}], stored), "a.b", "null", value=True)
    bad("l", pa.array([[{"c": 1}]], pa.list_(stored)), "a.b", "null", value=True)
    bad("l", pa.array([[{"c": 1}]], pa.list_(raw)), "not expected ['a.b']")
    bad("l", pa.array([1]), "int64")
    bad("f", pa.array([[1, 2, 3]], pa.list_(pa.int8(), 3)), "[3]")
    bad("f", pa.array([[1, 2]], pa.list_(pa.int16(), 2)), "int16")
    bad("f", pa.array([[1, 2]], pa.list_(pa.int8())), "list")
    bad("m", pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int16())), "int16")
    bad("m", pa.array([[(b"a", 1)]], pa.map_(pa.binary(), pa.int8())), "binary")
    bad("m", pa.array([1]), "int64")
    for rows in (good.drop_columns(["n"]), good.append_column("m", pa.array([1]))):
        with pytest.raises(store.StoredRowsError, match="'(n|m)'"):
            _ = decode(rows, schema)
    repeated = pa.Table.from_arrays(
        [*good.columns, good.column("n")], names=[*good.column_names, "n"]
    )
    with pytest.raises(store.StoredRowsError, match="repeated"):
        _ = decode(repeated, schema)


def test_rows_are_given_the_recorded_schema_only() -> None:
    table = keyed({"a": pa.array([1]), "b": pa.array(["x"])})
    manifest, stored = new(table.schema), _stored(table)
    for schema in (
        table.schema.set(0, pa.field("a", pa.int32())),
        table.schema.remove(0),
        schema_of(list(reversed(list(table.schema)))),
        table.schema.set(0, pa.field("a", pa.int64(), metadata={"k": "v"})),
    ):
        with pytest.raises(store.ManifestError, match="not the recorded schema"):
            _ = store.decode_rows(stored, manifest, schema)


def test_a_table_with_a_manifest_and_no_rows_reads_as_no_rows() -> None:
    schema = wide_schema(20)
    rows = store.empty_rows(read(entries(schema)))
    assert rows.num_rows == 0
    assert rows.schema.equals(store.logical_schema(schema), check_metadata=True)


# ------------------------------------------------------------------------ manifest


def wide_schema(columns: int = 200) -> pa.Schema:
    """Several tags, data columns and nested ones, each with metadata."""
    fields: list[Any] = [pa.field(KEY, pa.large_binary(), False)]
    for i in range(columns - 1):
        arrow_type: Any = [
            pa.int64(),
            timestamp("ns", tz="Asia/Kathmandu"),
            pa.struct([pa.field("__a.b", pa.string(), False, {"depth": "1"})]),
            pa.list_(pa.field("odd", pa.binary(), False, {"unit": "m" * 30})),
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
    path = ("run", "a", "b", "", "é", '"', "\u2028")
    recorded = entries(schema, path)
    assert recorded.isascii()
    manifest = read(recorded, path)
    assert manifest == new(schema, path)
    assert manifest.record_path == path
    assert manifest.kind == "function_pipeline"
    assert manifest.schema.equals(store.logical_schema(schema), check_metadata=True)
    for a, b in zip(store.logical_schema(schema), manifest.schema):
        assert field_difference(a, b, a.name) is None


def test_a_payload_of_another_pyarrow_is_the_same_schema(monkeypatch: Any) -> None:
    """Other bytes (field metadata written in another order) decode to one schema."""
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
    assert first != second
    one, two = read(first).schema, read(second).schema
    assert list(one.field(1).metadata or {}) != list(two.field(1).metadata or {})
    assert store.schema_difference(one, two) is None


def test_every_row_of_the_matrix_round_trips_in_one_manifest(tmp_path: Path) -> None:
    fields: list[Any] = [pa.field(KEY, pa.large_binary(), False)]
    for row in ROWS:
        if row.label in REFUSED_ROWS:
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
    pa.field("f", pa.large_list(pa.field("item", pa.int8()))),
    pa.field(KEY, pa.large_binary(), False),
]
MAPPED: list[Any] = [*BASE, pa.field("x", map_of(pa.string(), pa.int8()))]


def test_the_order_of_columns_is_no_difference() -> None:
    for order in ([3, 2, 1, 0], [1, 0, 3, 2]):
        other = schema_of([BASE[i] for i in order])
        assert store.schema_difference(schema_of(BASE), other) is None


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
        BASE[2].with_type(pa.large_list(pa.int16())),
        BASE[2].with_type(pa.list_(pa.int8(), 2)),
        pa.field("x", map_of(pa.string(), pa.int8(), keys_sorted=True)),
        BASE[2].with_type(pa.large_list(pa.large_list(pa.int8()))),
    ],
    ids=str,
)
def test_another_schema_is_a_difference(changed: Any) -> None:
    index = next((i for i, f in enumerate(MAPPED) if f.name == changed.name), 0)
    fields = list(MAPPED)
    fields[index] = changed
    one, other = read(entries(schema_of(MAPPED))).schema, schema_of(fields)
    for a, b in ((one, other), (other, one)):
        assert store.schema_difference(a, b) is not None


def test_schema_metadata_and_more_columns_are_a_difference() -> None:
    schema = schema_of(BASE)
    for other in (
        schema.with_metadata({"k": "v"}),
        schema_of([*BASE, ("more", pa.int8())]),
    ):
        assert store.schema_difference(schema, other) is not None


def test_a_manifest_of_another_path_is_refused() -> None:
    recorded = entries(schema_of(BASE), ("a", "b"))
    for path in (("a",), ("b", "a"), ("a", "b", ""), ("a_b",), ()):
        with pytest.raises(store.ManifestError, match="recorded as"):
            _ = store.read_manifest_bytes(recorded, path)


@pytest.mark.parametrize("part", ["a/b", "a\0", "\udc00"])
def test_a_path_component_the_store_cannot_name_is_refused(part: str) -> None:
    with pytest.raises(store.UnstorableInSpiralError):
        _ = new(schema_of(BASE), ("x", part))


def test_stored_names_come_from_the_manifest_not_from_the_rule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A table written under an earlier naming rule is still read by its own names."""
    table = keyed(
        {
            "a.b": pa.array([1]),
            "ox_a;2Eb": pa.array([2]),
            "s": pa.array([{"c.d": 3}], pa.struct([("c.d", pa.int64())])),
        }
    )
    metadata, stored = entries(table.schema), _stored(table)

    def renamed(name: str) -> str:
        return "new_" + name.encode().hex()

    monkeypatch.setattr(store, "encode_name", renamed)
    manifest = read(metadata)
    assert "ox_a;2Eb" in manifest.physical.names
    got = store.decode_rows(stored, manifest)
    assert [got.column(n)[0].as_py() for n in table.column_names if n != KEY] == [
        1, 2, {"c.d": 3},
    ]  # fmt: skip


def _payload(**changes: Any) -> bytes:
    """The recorded manifest of ``BASE`` with fields of its JSON changed."""
    body = {**json.loads(entries(schema_of(BASE))), **changes}
    return json.dumps(body).encode()


def _ipc(schema: pa.Schema) -> str:
    return base64.b64encode(schema.serialize().to_pybytes()).decode()


_A = pa.struct([("x", pa.int8()), ("y", pa.int8())])
_TWICE = pa.struct([("x", pa.int8()), ("x", pa.int8())])
_B = pa.large_binary()
_L = pa.large_list(timestamp("us"))


def _physical(index: int, field: Any) -> Any:
    """The stored schema of ``BASE`` with one column changed."""
    return store.physical_schema(schema_of(BASE)).set(index, field)


def test_the_changed_payload_helper_builds_a_readable_manifest() -> None:
    assert read(_payload()) == new(schema_of(BASE))


_BROKEN_MANIFESTS: list[Callable[[bytes], bytes]] = [
    lambda m: b"",
    lambda m: b"[]",
    lambda m: b"\xff",
    lambda m: m + b" junk",
    lambda m: b'"text"',
    lambda m: _payload(format="other"),
    lambda m: _payload(format=None),
    lambda m: _payload(version=1),
    lambda m: _payload(version=False),
    lambda m: _payload(kind=1),
    lambda m: _payload(record_path=["other"]),
    lambda m: _payload(physical=_ipc(schema_of([("a", pa.int8())]))),
    lambda m: _payload(physical=_ipc(_physical(0, pa.field("a", pa.int8())))),
    lambda m: _payload(physical=_ipc(_physical(0, pa.field(KEY, _A)))),
    lambda m: _payload(physical=_ipc(_physical(3, pa.field("k", _B)))),
    lambda m: _payload(physical=_ipc(_physical(1, pa.field("f", _L)))),
    lambda m: _payload(physical=_ipc(_physical(0, pa.field("a", _TWICE)))),
    lambda m: _payload(record_path="a"),
    lambda m: _payload(record_path=[1]),
    lambda m: _payload(schema=1),
    lambda m: _payload(schema="!!"),
    lambda m: _payload(schema=base64.b64encode(b"junk").decode()),
    lambda m: _payload(schema=_ipc(schema_of([("a", pa.int8())]))),
    lambda m: _payload(schema=_ipc(schema_of([*BASE, ("m", pa.binary(2))]))),
]


@pytest.mark.parametrize("make", _BROKEN_MANIFESTS)
def test_a_manifest_that_cannot_be_read_in_full_is_an_error(
    make: Callable[[bytes], bytes],
) -> None:
    recorded = make(entries(schema_of(BASE)))
    with pytest.raises(store.ManifestError):
        _ = store.read_manifest_bytes(recorded, PATH)


def test_a_large_manifest_is_kept() -> None:
    """A schema row's cell has no metadata entry's limit: no chunks, no bound."""
    field = pa.field("a", pa.int8(), metadata={"k": "v" * 7_000_000})
    schema = schema_of([field, (KEY, pa.large_binary())])
    assert read(entries(schema)).schema.equals(schema, check_metadata=True)


def test_writing_the_same_manifest_twice_changes_nothing() -> None:
    one = entries(wide_schema())
    assert entries(wide_schema()) == one
    assert store.manifest_bytes(read(one)) == one


def test_the_codec_needs_no_spiral_client() -> None:
    code = (
        "import sys, orcapod.databases.spiral_store\n"
        "assert not [m for m in sys.modules if m.split('.')[0] == 'spiral']"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0


_NARROWED_MAP = """
import pyarrow as pa
from orcapod.databases import spiral_store as store

maps = pa.map_(pa.string(), pa.int64())
rows = pa.table(
    {
        store.KEY_COLUMN: pa.array([b"a", b"b", b"c"], pa.large_binary()),
        "m": pa.array([[("x", 1), ("y", None)], None, []], maps),
        "s": pa.array([{"m": [("x", 1)]}] * 3, pa.struct([("m", maps)])),
        "l": pa.array([[[("x", 1)]], None, [[]]], pa.list_(maps)),
    }
)
manifest = store.new_manifest(("p", "rdb_v1"), "k", rows.schema)
got = store.decode_rows(store.encode_rows(rows), manifest, rows.schema)
assert got.schema == rows.schema, got.schema
assert got.to_pylist() == rows.to_pylist(), got.to_pylist()
"""


def test_a_map_given_back_in_small_variants_is_rebuilt_not_cast() -> None:
    """Review of PR 590, finding 1: pyarrow aborts the process casting a map from large to
    small variants. Run in a subprocess, so an abort fails this test only."""
    done = subprocess.run(
        [sys.executable, "-c", _NARROWED_MAP],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr[-2000:]


def test_a_record_id_repeated_in_one_write_is_refused() -> None:
    """Review of PR 590, finding 2: refused before anything is written."""
    rows = keyed({"a": pa.array([1, 2])})
    rows = rows.set_column(
        rows.column_names.index(KEY), KEY, pa.array([b"x", b"x"], pa.large_binary())
    )
    refused(rows, "repeated")


# ------------------------------------------------- the manifest in a table's metadata


def test_a_manifest_in_chunks_round_trips_within_the_entry_limit() -> None:
    manifest = new(wide_schema(), PATH)
    entries_ = store.manifest_entries(manifest)
    assert len(entries_) > 2  # several chunks and the header
    for key, value in entries_.items():
        assert len(key.encode()) + len(value) <= 8192
    others = {"orcapod.path": b"[]", "someone.else": b"x"}
    assert store.read_manifest({**others, **entries_}, PATH) == manifest


def test_a_table_without_a_manifest_header_has_none() -> None:
    entries_ = store.manifest_entries(new(schema_of(BASE)))
    chunks = {k: v for k, v in entries_.items() if k != store.MANIFEST_KEY}
    assert store.read_manifest({}, PATH) is None
    assert store.read_manifest(chunks, PATH) is None


def test_chunks_of_an_earlier_longer_manifest_are_not_read() -> None:
    longer = store.manifest_entries(new(wide_schema(), PATH))
    shorter = store.manifest_entries(new(schema_of(BASE), PATH))
    assert len(longer) > len(shorter)
    assert store.read_manifest({**longer, **shorter}, PATH) == new(schema_of(BASE))


def _header_changed(entries_: dict[str, bytes], **changes: Any) -> dict[str, bytes]:
    header = {**json.loads(entries_[store.MANIFEST_KEY]), **changes}
    return {**entries_, store.MANIFEST_KEY: json.dumps(header).encode()}


_BROKEN_ENTRIES: list[Callable[[dict[str, bytes]], dict[str, bytes]]] = [
    lambda e: {k: v for k, v in e.items() if k != f"{store.MANIFEST_KEY}/0"},
    lambda e: {**e, f"{store.MANIFEST_KEY}/0": e[f"{store.MANIFEST_KEY}/0"] + b" "},
    lambda e: {**e, store.MANIFEST_KEY: b"\xff"},
    lambda e: {**e, store.MANIFEST_KEY: b"[]"},
    lambda e: _header_changed(e, format="other"),
    lambda e: _header_changed(e, chunks=0),
    lambda e: _header_changed(e, chunks="1"),
    lambda e: _header_changed(e, chunks=10**6),
    lambda e: _header_changed(e, sha256="0" * 64),
]


@pytest.mark.parametrize("make", _BROKEN_ENTRIES)
def test_a_manifest_in_chunks_that_cannot_be_read_is_an_error(
    make: Callable[[dict[str, bytes]], dict[str, bytes]],
) -> None:
    with pytest.raises(store.ManifestError):
        _ = store.read_manifest(make(store.manifest_entries(new(wide_schema()))), PATH)


# --------------------------------------- CodeRabbit on PR 590 (b433047e)


def test_a_schema_override_that_is_not_a_store_schema_is_a_manifest_error() -> None:
    """``decode_rows`` raises its documented ``ManifestError``, also for a schema that
    is no store schema at all (no key column)."""
    table = keyed({"a": pa.array([1])})
    manifest = new(table.schema)
    without_key = table.schema.remove(table.schema.get_field_index(KEY))
    with pytest.raises(store.ManifestError):
        _ = store.decode_rows(_stored(table), manifest, without_key)


@pytest.mark.parametrize("where", ["schema", "physical"])
def test_a_recorded_name_that_is_not_utf8_is_a_manifest_error(where: str) -> None:
    """``read_manifest`` raises only ``ManifestError``, also for a name of bytes: in the
    logical schema, and in a stored schema of the right shape (so that the name, not the
    shape, is what is refused)."""
    if where == "schema":
        bad: Any = pa.field(cast(Any, b"a\xff"), pa.int8())
        recorded = _payload(schema=_ipc(schema_of([bad, (KEY, pa.large_binary())])))
    else:
        first = store.physical_schema(schema_of(BASE)).field(0)
        renamed: Any = pa.field(cast(Any, b"a\xff"), first.type, first.nullable)
        recorded = _payload(physical=_ipc(_physical(0, renamed)))
    with pytest.raises(store.ManifestError, match="UTF-8"):
        _ = store.read_manifest_bytes(recorded, PATH)
