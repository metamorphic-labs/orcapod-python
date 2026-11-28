"""Python floats entering a column declared ``np.float32`` (ITL-924).

The values are converted as Arrow converts them, with no check of each value. Orcapod
issues one ``ValueConversionWarning`` per process for the declared type. Dict keys that
collide after conversion to the stored key type raise, naming the column and both keys.
"""

from __future__ import annotations

import os
import subprocess
import sys
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest
from typing_extensions import override

from orcapod import PipelineJob
from orcapod.core.datagrams import Data, Datagram
from orcapod.core.function_pod import function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import CanonicalOrderError, OutputTypeError, ValueConversionWarning
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.semantic_types.canonical_order import _sorted_by_rank
from orcapod.semantic_types.float_narrowing import reset_float_narrowing_warnings
from orcapod.sources import DictSource, ListSource
from orcapod.types import DataType, DataValue

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


@pytest.mark.parametrize("materialized", [False, True])
def test_datagram_update_accepts_python_floats_with_one_warning(
    materialized: bool,
) -> None:
    given: dict[str, Any] = {
        "x": np.float32(0.25),
        "y": np.float32(0.25),
        "items": [np.float32(0.25)],
        "by_key": {np.float32(0.25): 0},
        "by_name": {"a": np.float32(0.25)},
    }
    start = Datagram(given, python_schema=SCHEMA)
    if materialized:
        _ = start.as_table()
        assert start.schema()["x"] is np.float32
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        tables = [
            start.update(**{name: row[name] for name in SCHEMA}).as_table()
            for row in _rows()
        ]
    assert len(_narrowing_warnings(caught)) == 1
    _assert_stored_as_arrow_narrows(pa.concat_tables(tables))


@pytest.mark.parametrize("warn_first", [False, True], ids=["fresh", "after-ignore"])
def test_the_warning_as_an_error_raises_for_every_narrowed_value(
    warn_first: bool,
) -> None:
    if warn_first:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ValueConversionWarning)
            _ = _source(_rows(), ["x"])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        for _ in range(2):
            with pytest.raises(ValueConversionWarning, match=r"numpy\.float32"):
                _ = _source(_rows(), ["x"])


def test_reentrant_showwarning_reports_narrowing_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[type[Warning]] = []

    def showwarning(_message: Warning, category: type[Warning], *_args: object) -> None:
        seen.append(category)
        if len(seen) == 1:
            _ = _source(_rows(), ["x"])

    with warnings.catch_warnings():
        warnings.simplefilter("always", ValueConversionWarning)
        monkeypatch.setattr(warnings, "showwarning", showwarning)
        _ = _source(_rows(), ["x"])
    assert seen == [ValueConversionWarning]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
@pytest.mark.parametrize("action", ["default", "once"])
def test_forked_child_warns_on_its_first_narrowing(action: str) -> None:
    # Keep the fork out of the pytest runner and exercise the same warning site
    # in both processes.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os
import sys
import warnings

import numpy as np
from orcapod.core.datagrams import Datagram
from orcapod.errors import ValueConversionWarning

def convert():
    Datagram({"x": 0.1}, python_schema={"x": np.float32}).as_table()

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter(sys.argv[1], ValueConversionWarning)
    convert()
    assert len(caught) == 1
    caught.clear()
    pid = os.fork()
    if pid == 0:
        convert()
        convert()
        os._exit(0 if len(caught) == 1 else 1)
    _, status = os.waitpid(pid, 0)
    raise SystemExit(os.waitstatus_to_exitcode(status))
""",
            action,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


COLLIDING = {1e-50: 1, 2e-50: 2}


@pytest.mark.parametrize(
    "operation",
    [
        "with-columns-old",
        "with-columns-new",
        "mixed",
        "select",
        "drop",
        "rename",
        "copy-columns",
    ],
)
def test_arrow_transform_keeps_float32_declarations(operation: str) -> None:
    schema: dict[str, DataType] = {
        "x": np.float32,
        "keys": dict[np.float32, int],
        "members": set[np.float32],
    }
    values: dict[str, Any] = {
        "x": np.float32(0.5),
        "keys": {np.float32(0.5): 1},
        "members": {np.float32(0.5)},
    }
    start = Data({**values, "other": 1}, python_schema=schema)
    if operation != "mixed":
        _ = start.as_table()
    prefix = ""
    if operation == "with-columns-old":
        result = start.with_columns(extra=2)
    elif operation == "with-columns-new":
        result = start.with_columns(
            {f"new_{k}": t for k, t in schema.items()},
            **{f"new_{k}": values[k] for k in schema},
        )
        prefix = "new_"
    elif operation == "mixed":
        result = start.with_columns({"extra": pa.float32()}, extra=0.5)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", ValueConversionWarning)
            arrow_only = result.update(extra=0.1).as_table()
        assert _narrowing_warnings(caught) == []
        assert _bits(_column(arrow_only, "extra")) == _as_arrow_narrows([0.1])
    elif operation == "select":
        result = start.update(other=2).select(*schema)
    elif operation == "drop":
        result = start.update(other=2).drop("other")
    elif operation == "rename":
        result = start.rename({k: f"new_{k}" for k in schema})
        prefix = "new_"
    else:
        result = Data({"extra": 2}).with_columns_of(
            start.with_columns(side=3), list(schema)
        )

    before = result.as_table()
    with pytest.warns(ValueConversionWarning, match="numpy.float32"):
        updated = result.update(**{f"{prefix}x": 1e-50}).as_table()
    assert updated.schema.equals(before.schema, check_metadata=True)
    assert _bits(_column(updated, f"{prefix}x")) == _as_arrow_narrows([1e-50])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ValueConversionWarning)
        for key, value in (("keys", COLLIDING), ("members", set(COLLIDING))):
            name = f"{prefix}{key}"
            with pytest.raises(CanonicalOrderError) as raised:
                _ = result.update(**{name: value})
            assert all(
                text in str(raised.value) for text in (repr(name), "1e-50", "2e-50")
            )
    assert result.as_table().equals(before)


@pytest.mark.parametrize("value", [True, False])
def test_materialized_update_preserves_arrow_integer_conversion(value: bool) -> None:
    given: dict[str, Any] = {
        "count": np.int32(2),
        "x": np.float32(0.5),
        "mixed": {np.float32(0.5): np.int32(2)},
    }
    row = Datagram(
        given,
        python_schema={
            "count": np.int32,
            "x": np.float32,
            "mixed": dict[np.float32, np.int32],
        },
    )
    table = row.as_table()
    updated = row.update(count=value).as_table()
    assert _column(updated, "count") == [int(value)]
    assert updated.schema.equals(table.schema, check_metadata=True)
    with pytest.warns(ValueConversionWarning, match="numpy.float32"):
        mixed = row.update(mixed={0.1: value}).as_table()
    assert mixed.schema.equals(table.schema, check_metadata=True)
    assert _column(mixed, "mixed") == [
        [{"key": float(np.float32(0.1)), "value": int(value)}]
    ]


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
@pytest.mark.parametrize("container", [dict, set])
def test_materialized_datagram_update_refuses_collisions(container: type) -> None:
    if container is dict:
        declaration = dict[np.float32, int]
        original = {0.5: 0}
        duplicate = COLLIDING
    else:
        declaration = set[np.float32]
        original = {0.5}
        duplicate = set(COLLIDING)
    start = Datagram(
        {"x": original, "y": 1}, python_schema={"x": declaration, "y": int}
    )
    table = start.as_table()
    assert start.schema()["x"] == declaration
    with pytest.raises(CanonicalOrderError) as raised:
        _ = start.update(x=duplicate)
    assert all(text in str(raised.value) for text in ("'x'", "1e-50", "2e-50"))
    assert start.as_table().equals(table)


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
@pytest.mark.parametrize(
    ("declaration", "value"),
    [
        (dict[np.float32 | None, int], {None: 0, **COLLIDING}),
        (set[np.float32 | None], {None, *COLLIDING}),
    ],
)
def test_optional_float32_keys_and_elements_refuse_collisions(
    declaration: DataType, value: DataValue
) -> None:
    with pytest.raises(CanonicalOrderError) as raised:
        _ = Datagram({"x": value}, python_schema={"x": declaration}).as_table()
    assert all(text in str(raised.value) for text in ("'x'", "1e-50", "2e-50"))


class _FloatWithOverride(float):
    @override
    def __float__(self) -> float:
        return 9.0


class _Float64WithOverride(np.float64):
    hide_width: bool = True

    @property
    @override
    def dtype(self) -> Any:
        return np.dtype(np.float32 if self.hide_width else np.float64)


def test_float32_subclass_cannot_invent_narrowing() -> None:
    class Float32WithWideDtype(np.float32):
        @property
        @override
        def dtype(self) -> Any:
            return np.dtype(np.float64)

    value: Any = Float32WithWideDtype(0.5)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ValueConversionWarning)
        table = Datagram({"x": value}, python_schema={"x": np.float32}).as_table()
    assert _narrowing_warnings(caught) == []
    assert _bits(_column(table, "x")) == _as_arrow_narrows([0.5])


@pytest.mark.parametrize("hide_width", [False, True])
def test_numpy_float_subclass_narrows_based_on_type(hide_width: bool) -> None:
    value = _Float64WithOverride(0.1)
    value.hide_width = hide_width
    row = Datagram({"x": value}, python_schema={"x": np.float32})
    with pytest.warns(ValueConversionWarning, match="numpy.float32"):
        table = row.as_table()
    assert _bits(_column(table, "x")) == _as_arrow_narrows([0.1])


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
@pytest.mark.parametrize("container", [dict, set])
def test_numpy_float_subclass_cannot_hide_colliding_keys(container: type) -> None:
    first, second = _Float64WithOverride(1e-50), _Float64WithOverride(2e-50)
    declaration = dict[np.float32, int] if container is dict else set[np.float32]
    value = {first: 1, second: 2} if container is dict else {first, second}
    with pytest.raises(CanonicalOrderError) as raised:
        _ = Datagram({"x": value}, python_schema={"x": declaration}).as_table()
    assert all(text in str(raised.value) for text in ("'x'", "1e-50", "2e-50"))


def test_narrowing_warning_documents_its_scope_and_dict_key_timing() -> None:
    assert "per process and declared type" in (ValueConversionWarning.__doc__ or "")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ValueConversionWarning)
        row = Datagram(
            {"keys": {0.1: 1}}, python_schema={"keys": dict[np.float32, int]}
        )
        assert len(_narrowing_warnings(caught)) == 1
        _ = row.as_table()
    assert len(_narrowing_warnings(caught)) == 1


@dataclass
class _Reading:
    level: np.float32


def test_declared_dataclass_field_warns_on_wider_float() -> None:
    reading: Any = _Reading(cast(np.float32, _Float64WithOverride(0.1)))
    with pytest.warns(ValueConversionWarning, match="numpy.float32"):
        table = DictSource(
            [{"id": 1, "reading": reading}],
            tag_columns=["id"],
            tag_only_columns=True,
            data_schema={"id": int, "reading": _Reading},
        ).as_table()
    assert _bits([_column(table, "reading")[0]["level"]]) == _as_arrow_narrows([0.1])


@pytest.mark.parametrize(
    "reverse", [False, True], ids=["declared-order", "reversed-order"]
)
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_float32_tag_and_output_narrowing_in_job(
    reverse: bool, orchestrator: Any
) -> None:
    def half(x: int) -> Any:
        return np.float16(-0.0) if x == 1 else 0.5

    half.__annotations__["return"] = np.float32
    pod: Any = function_pod(output_keys="result")(half)
    columns: dict[str, DataType] = {"id": int, "tag": np.float32, "x": int, "y": int}
    names = list(reversed(columns)) if reverse else list(columns)
    tags = ["tag", "id"] if reverse else ["id", "tag"]
    values: list[dict[str, Any]] = [
        {"id": i, "tag": _Float64WithOverride(0.1), "x": i, "y": i * 2} for i in (1, 2)
    ]
    with pytest.warns(ValueConversionWarning, match="numpy.float32") as caught:
        source = DictSource(
            [{name: row[name] for name in names} for row in values],
            tag_columns=tags,
            tag_only_columns=True,
            data_schema={name: columns[name] for name in names},
        )
    assert len(caught) == 1

    # The accepted output must stay quiet independently of the ingress warning.
    reset_float_narrowing_warnings()
    errors: list[BaseException] = []

    class Observer(NoOpObserver):
        @override
        def contextualize(self, *identity_path: str) -> Observer:
            return self

        @override
        def on_data_crash(
            self, node_label: Any, tag: Any, data: Any, error: Any
        ) -> None:
            errors.append(error)

    job = PipelineJob(name="float32-tags", database=InMemoryArrowDatabase())
    with job:
        pod.pod(source, label="half")
    with warnings.catch_warnings(record=True) as emitted:
        warnings.simplefilter("always", ValueConversionWarning)
        _ = job.run(orchestrator=orchestrator(), observer=Observer())
        table = job.nodes["half"].as_table()
    assert _narrowing_warnings(emitted) == []
    assert len(errors) == 1
    assert isinstance(errors[0], OutputTypeError)
    assert _column(table, "id") == [1]
    assert (
        table.schema.field("tag").type
        == table.schema.field("result").type
        == pa.float32()
    )
    assert _bits(_column(table, "tag")) == _as_arrow_narrows([0.1])
    assert _bits(_column(table, "result")) == _as_arrow_narrows([-0.0])


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
@pytest.mark.parametrize("container", [dict, set])
def test_float_subclass_keys_and_elements_store_the_base_value(container: type) -> None:
    subclass = _FloatWithOverride(0.1)
    if container is dict:
        declaration = dict[np.float32, int]
        given = {subclass: 1, 0.5: 2}
        plain = {0.1: 1, 0.5: 2}
    else:
        declaration = set[np.float32]
        given = {subclass, 0.5}
        plain = {0.1, 0.5}
    schema = {"x": declaration}
    table = Datagram({"x": given}, python_schema=schema).as_table()
    assert table.equals(Datagram({"x": plain}, python_schema=schema).as_table())


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
@pytest.mark.parametrize("key_type", [float, np.float32])
def test_float_subclass_nan_key_is_refused(key_type: type) -> None:
    with pytest.raises(CanonicalOrderError, match=r"Column 'x': .*NaN"):
        _ = Datagram(
            {"x": {_FloatWithOverride(float("nan")): 1}},
            python_schema={"x": dict[key_type, int]},
        )


def test_canonical_sorter_reports_original_collision_positions() -> None:
    # The sorter knows ranks and payloads. Its caller can use the positions
    # to look up the original values when adding the error context.
    with pytest.raises(CanonicalOrderError) as raised:
        _ = _sorted_by_rank([((1, 2), "first"), ((1, 0), "second"), ((1, 2), "third")])
    assert getattr(raised.value, "positions", None) == (0, 2)
    assert getattr(raised.value, "stored", None) == 2


def test_arrow_only_datagram_update_keeps_arrow_float32() -> None:
    source = pa.table({"x": pa.array([0.5], type=pa.float32())})
    start = Datagram(source)
    assert start.schema()["x"] == float | None
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ValueConversionWarning)
        updated = start.update(x=0.1).as_table()
    assert _narrowing_warnings(caught) == []
    assert updated.schema.equals(source.schema, check_metadata=True)
    assert _bits(_column(updated, "x")) == _as_arrow_narrows([0.1])


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
