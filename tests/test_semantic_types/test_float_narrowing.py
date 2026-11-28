"""Python floats entering a column declared ``np.float32`` (ITL-924).

The values are converted as Arrow converts them, with no check of each value. Orcapod
issues one ``ValueConversionWarning`` per process for the declared type. Dict keys that
collide after conversion to the stored key type raise, naming the column and both keys.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest

from orcapod.core.datagrams import Datagram
from orcapod.core.function_pod import function_pod
from orcapod.errors import CanonicalOrderError, ValueConversionWarning
from orcapod.semantic_types.float_narrowing import reset_float_narrowing_warnings
from orcapod.sources import DictSource, ListSource

# Every kind of value: inexact, underflowing, overflowing, exact, NaN, infinite, -0.0.
VALUES = [0.1, 1e-50, 1e300, 0.5, float("nan"), float("inf"), -0.0]

# Keys that stay distinct in float32 (no NaN: a NaN key has no canonical order).
KEYS = [0.1, 1e300, 0.5, -0.0]

SCHEMA: dict[str, Any] = {
    "x": np.float32,
    "y": np.float32,
    "items": list[np.float32],
    "by_key": dict[np.float32, int],
    "by_name": dict[str, np.float32],
}


@pytest.fixture(autouse=True)
def _as_a_new_process() -> Iterator[None]:
    """Each test starts with no narrowing warning issued, as a new process does."""
    reset_float_narrowing_warnings()
    yield
    reset_float_narrowing_warnings()


def _bits(values: list[Any]) -> list[bytes]:
    """The float32 bit patterns of ``values``."""
    return [np.float32(v).tobytes() for v in values]


def _column(table: pa.Table, name: str) -> list[Any]:
    """The values of one column."""
    return cast("list[Any]", table.column(name).to_pylist())


def _as_arrow_narrows(values: list[float]) -> list[bytes]:
    """The float32 bit patterns Arrow stores for Python floats."""
    return _bits(pa.array(values, pa.float32()).to_pylist())


def _rows(convert: Any = float) -> list[dict[str, Any]]:
    """One row per value, each holding it in every position of ``SCHEMA``."""
    return [
        {
            "id": i,
            "x": convert(v),
            "y": convert(v),
            "items": [convert(v), convert(v)],
            "by_key": {convert(k): n for n, k in enumerate(KEYS)},
            "by_name": {"a": convert(v), "b": convert(v)},
        }
        for i, v in enumerate(VALUES)
    ]


def _source(rows: list[dict[str, Any]], columns: list[str] | None = None) -> DictSource:
    names = columns or list(SCHEMA)
    return DictSource(
        [{"id": row["id"], **{name: row[name] for name in names}} for row in rows],
        tag_columns=["id"],
        tag_only_columns=True,
        data_schema={"id": int, **{name: SCHEMA[name] for name in names}},
        source_id="floats",
    )


def _narrowing_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [
        str(w.message) for w in caught if issubclass(w.category, ValueConversionWarning)
    ]


def _assert_stored_as_arrow_narrows(table: pa.Table) -> None:
    """Every stored value is the float32 Arrow gives for the Python float."""
    expected = _as_arrow_narrows(VALUES)
    assert table.schema.field("x").type == pa.float32()
    assert _bits(_column(table, "x")) == expected
    assert _bits(_column(table, "y")) == expected
    for row, bits in zip(_column(table, "items"), expected):
        assert _bits(row) == [bits, bits]
    for row, bits in zip(_column(table, "by_name"), expected):
        assert [(e["key"], np.float32(e["value"]).tobytes()) for e in row] == [
            ("a", bits),
            ("b", bits),
        ]
    stored_keys = sorted(_as_arrow_narrows(KEYS))
    for row in _column(table, "by_key"):
        assert sorted(_bits([e["key"] for e in row])) == stored_keys


@pytest.mark.parametrize(
    "columns",
    [
        ["x", "y", "items", "by_key", "by_name"],
        ["by_name", "by_key", "items", "y", "x"],
    ],
    ids=["declared-order", "reversed-order"],
)
def test_dict_source_accepts_python_floats_with_one_warning(
    columns: list[str],
) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        table = _source(_rows(), columns).as_table()
    messages = _narrowing_warnings(caught)
    assert len(messages) == 1
    assert "numpy.float32" in messages[0]
    assert "float" in messages[0]
    _assert_stored_as_arrow_narrows(table)


def test_a_second_source_does_not_warn_again() -> None:
    with pytest.warns(ValueConversionWarning):
        _source(_rows())
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _source(_rows(), ["y", "x"])
        ListSource(
            "x", [0.1, 1e-50], data_schema={"element_index": int, "x": np.float32}
        )
        Datagram({"x": 0.1}, python_schema={"x": np.float32}).as_table()
    assert _narrowing_warnings(caught) == []


def test_numpy_float32_values_never_warn() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        table = _source(_rows(np.float32)).as_table()
        ListSource(
            "x",
            [np.float32(0.1), np.float32(0.5)],
            data_schema={"element_index": int, "x": np.float32},
        )
        given: dict[str, Any] = {"x": np.float32(0.1), "by_key": {np.float32(0.5): 1}}
        datagram = Datagram(
            given, python_schema={"x": np.float32, "by_key": dict[np.float32, int]}
        )
        datagram.as_table()
        changed: dict[str, Any] = {"x": np.float32(0.25)}
        datagram.update(**changed).as_table()
    assert _narrowing_warnings(caught) == []
    _assert_stored_as_arrow_narrows(table)


def test_an_arrow_float32_table_never_warns() -> None:
    from orcapod.sources import ArrowTableSource

    table = pa.table(
        {"id": pa.array([1, 2]), "x": pa.array([0.1, 0.5], type=pa.float32())}
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        stored = ArrowTableSource(
            table, tag_columns=["id"], tag_only_columns=True, source_id="arrow"
        ).as_table()
    assert _narrowing_warnings(caught) == []
    assert _column(stored, "x") == _column(table, "x")


def test_list_source_accepts_python_floats_with_one_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        source = ListSource(
            "x", VALUES, data_schema={"element_index": int, "x": np.float32}
        )
        nested = ListSource(
            "items",
            [VALUES, VALUES[:2]],
            data_schema={"element_index": int, "items": list[np.float32]},
        )
    assert len(_narrowing_warnings(caught)) == 1
    expected = _as_arrow_narrows(VALUES)
    assert _bits(_column(source.as_table(), "x")) == expected
    assert _bits(_column(nested.as_table(), "items")[0]) == expected


def test_datagram_accepts_python_floats_with_one_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        tables = [
            Datagram(
                {name: row[name] for name in SCHEMA}, python_schema=SCHEMA
            ).as_table()
            for row in _rows()
        ]
    assert len(_narrowing_warnings(caught)) == 1
    _assert_stored_as_arrow_narrows(pa.concat_tables(tables))


def test_datagram_update_accepts_python_floats_with_one_warning() -> None:
    given: dict[str, Any] = {
        "x": np.float32(0.25),
        "y": np.float32(0.25),
        "items": [np.float32(0.25)],
        "by_key": {np.float32(0.25): 0},
        "by_name": {"a": np.float32(0.25)},
    }
    start = Datagram(given, python_schema=SCHEMA)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        tables = [
            start.update(**{name: row[name] for name in SCHEMA}).as_table()
            for row in _rows()
        ]
    assert len(_narrowing_warnings(caught)) == 1
    _assert_stored_as_arrow_narrows(pa.concat_tables(tables))


def test_the_warning_as_an_error_raises_for_every_narrowed_value() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        for _ in range(2):
            with pytest.raises(ValueConversionWarning, match=r"numpy\.float32"):
                _source(_rows(), ["x"])


COLLIDING = {1e-50: 1, 2e-50: 2}


def _assert_names_column_and_both_keys(error: BaseException) -> None:
    text = str(error)
    assert "'by_key'" in text
    assert "1e-50" in text
    assert "2e-50" in text


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
class TestKeysThatCollideAfterNarrowing:
    """``1e-50`` and ``2e-50`` are both stored as the float32 key ``0.0``."""

    def test_dict_source_raises(self) -> None:
        with pytest.raises(CanonicalOrderError) as raised:
            DictSource(
                [{"id": 1, "x": 0.5, "by_key": COLLIDING}],
                tag_columns=["id"],
                tag_only_columns=True,
                data_schema={
                    "id": int,
                    "x": np.float32,
                    "by_key": dict[np.float32, int],
                },
            )
        _assert_names_column_and_both_keys(raised.value)

    def test_list_source_raises(self) -> None:
        with pytest.raises(CanonicalOrderError) as raised:
            ListSource(
                "by_key",
                [{0.5: 1}, COLLIDING],
                data_schema={"element_index": int, "by_key": dict[np.float32, int]},
            )
        _assert_names_column_and_both_keys(raised.value)

    def test_datagram_raises(self) -> None:
        with pytest.raises(CanonicalOrderError) as raised:
            Datagram(
                {"by_key": COLLIDING}, python_schema={"by_key": dict[np.float32, int]}
            )
        _assert_names_column_and_both_keys(raised.value)

    def test_datagram_update_raises_and_leaves_the_datagram_as_it_was(self) -> None:
        start = Datagram(
            {"by_key": {0.5: 1}}, python_schema={"by_key": dict[np.float32, int]}
        )
        with pytest.raises(CanonicalOrderError) as raised:
            start.update(by_key=COLLIDING)
        _assert_names_column_and_both_keys(raised.value)
        assert start.as_dict() == {"by_key": {0.5: 1}}

    def test_nested_dict_keys_raise(self) -> None:
        with pytest.raises(CanonicalOrderError) as raised:
            Datagram(
                {"by_key": [{"a": COLLIDING}]},
                python_schema={"by_key": list[dict[str, dict[np.float32, int]]]},
            ).as_table()
        _assert_names_column_and_both_keys(raised.value)

    def test_keys_distinct_in_float32_are_all_stored(self) -> None:
        table = Datagram(
            {"by_key": {0.1: 1, 0.5: 2, 1e-50: 3}},
            python_schema={"by_key": dict[np.float32, int]},
        ).as_table()
        entries = _column(table, "by_key")[0]
        assert [e["value"] for e in entries] == [3, 1, 2]
        assert _bits([e["key"] for e in entries]) == _as_arrow_narrows(
            [1e-50, 0.1, 0.5]
        )

    def test_a_nan_key_is_still_refused(self) -> None:
        with pytest.raises(CanonicalOrderError, match="NaN"):
            Datagram(
                {"by_key": {float("nan"): 1}},
                python_schema={"by_key": dict[np.float32, int]},
            )

    def test_float64_keys_that_differ_are_not_narrowed(self) -> None:
        table = Datagram(
            {"by_key": COLLIDING}, python_schema={"by_key": dict[float, int]}
        ).as_table()
        assert [e["key"] for e in _column(table, "by_key")[0]] == [
            1e-50,
            2e-50,
        ]


def test_function_output_declared_float32_still_rejects_a_python_float() -> None:
    """Function outputs keep their own rule: a Python float is not converted."""

    def half(x: int) -> Any:
        return 0.5

    half.__annotations__["return"] = np.float32
    pod: Any = function_pod(output_keys="y")(half)
    source = DictSource(
        [{"id": 1, "x": 1}], tag_columns=["id"], tag_only_columns=True, source_id="s"
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(Exception, match=r"numpy\.float32"):
            pod.pod(source).as_table()
    assert _narrowing_warnings(caught) == []


def test_an_accepted_narrower_output_is_stored_unchanged_without_a_warning() -> None:
    """``np.float16`` returned for ``np.float32`` is held as ``np.float32`` (exact)."""

    def half(x: int) -> Any:
        return np.float16(0.5)

    half.__annotations__["return"] = np.float32
    pod: Any = function_pod(output_keys="y")(half)
    source = DictSource(
        [{"id": 1, "x": 1}], tag_columns=["id"], tag_only_columns=True, source_id="s"
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        stream = pod.pod(source)
        table = stream.as_table()
        held = [data.as_dict()["y"] for _, data in stream.iter_data()]
    assert _narrowing_warnings(caught) == []
    assert table.schema.field("y").type == pa.float32()
    assert _bits(_column(table, "y")) == _bits([0.5])
    assert [type(value) for value in held] == [np.float32]
    assert held == [0.5]


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
class TestSetElementsThatCollideAfterNarrowing:
    """``{1e-50, 2e-50}`` would be stored as two ``0.0`` elements and read as one."""

    def _assert_names_column_and_both_elements(self, error: BaseException) -> None:
        text = str(error)
        assert "'members'" in text
        assert "1e-50" in text
        assert "2e-50" in text

    def test_datagram_raises(self) -> None:
        with pytest.raises(CanonicalOrderError) as raised:
            Datagram(
                {"members": {1e-50, 2e-50}},
                python_schema={"members": set[np.float32]},
            ).as_table()
        self._assert_names_column_and_both_elements(raised.value)

    def test_dict_source_raises(self) -> None:
        with pytest.raises(CanonicalOrderError) as raised:
            DictSource(
                [{"id": 1, "members": {1e-50, 2e-50}}],
                tag_columns=["id"],
                tag_only_columns=True,
                data_schema={"id": int, "members": set[np.float32]},
            )
        self._assert_names_column_and_both_elements(raised.value)

    def test_a_set_given_for_a_list_column_raises(self) -> None:
        given: dict[str, Any] = {"members": {1e-50, 2e-50}}
        with pytest.raises(CanonicalOrderError) as raised:
            Datagram(given, python_schema={"members": list[np.float32]}).as_table()
        self._assert_names_column_and_both_elements(raised.value)

    def test_elements_distinct_in_float32_are_all_stored(self) -> None:
        table = Datagram(
            {"members": {0.5, 0.1, 1e-50}}, python_schema={"members": set[np.float32]}
        ).as_table()
        assert _bits(_column(table, "members")[0]) == _as_arrow_narrows(
            [1e-50, 0.1, 0.5]
        )

    def test_numpy_float32_elements_are_stored(self) -> None:
        given: dict[str, Any] = {"members": {np.float32(0.5), np.float32(0.1)}}
        table = Datagram(given, python_schema={"members": set[np.float32]}).as_table()
        assert _bits(_column(table, "members")[0]) == _bits([0.1, 0.5])
