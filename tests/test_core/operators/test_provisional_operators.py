"""Provisional operators: the list, the markers, the warning and the record of uses (ITL-1266)."""

from __future__ import annotations

import inspect
import warnings
from collections.abc import Iterator
from typing import Any, ClassVar

import pyarrow as pa
import pytest

import orcapod.operators as operators
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.operators.provisional import (
    PROVISIONAL_OPERATORS,
    provisional_operator_name,
    reset_provisional_operator_warnings,
    warn_provisional_operator,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.errors import ProvisionalHashWarning
from orcapod.operators import PolarsFilter
from orcapod.sources import ArrowTableSource
from orcapod.utils.provisional import recording_provisional_uses


@pytest.fixture(autouse=True)
def fresh_process() -> Iterator[None]:
    """Each test starts as a new process would: no provisional operator has warned."""
    reset_provisional_operator_warnings()
    yield
    reset_provisional_operator_warnings()


def _provisional_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [
        str(w.message) for w in caught if issubclass(w.category, ProvisionalHashWarning)
    ]


def _exported_operators() -> dict[str, type]:
    return {
        name: obj
        for name, obj in vars(operators).items()
        if name in operators.__all__
        and inspect.isclass(obj)
        and issubclass(obj, StaticOutputOperatorPod)
    }


class _ProvisionalA(UnaryOperator):
    """A provisional test operator."""

    PROVISIONAL: ClassVar[bool] = True

    def unary_static_process(self, stream: Any) -> Any:
        return stream

    def validate_unary_input(self, stream: Any) -> None:
        return None

    def unary_output_schema(self, stream: Any, **kwargs: Any) -> Any:
        return stream.output_schema(**kwargs)

    def identity_structure(self) -> Any:
        return (type(self).__name__,)


class _ProvisionalB(_ProvisionalA):
    """A second provisional test operator, marked on its own class."""

    PROVISIONAL: ClassVar[bool] = True


class _SubclassOfA(_ProvisionalA):
    """Provisional through its base only."""


def _stream() -> ArrowTableSource:
    table = pa.table(
        {
            "subject": ["s1", "s2", "s3"],
            "session": [1, 2, 1],
            "age": [2, 4, 6],
            "weight": [20.5, 22.25, 19.0],
        }
    )
    return ArrowTableSource(
        table, tag_columns=["subject", "session"], tag_only_columns=True
    )


# ---------------------------------------------------------------------------
# The list and the markers agree
# ---------------------------------------------------------------------------


def test_the_list_and_the_markers_agree() -> None:
    exported = _exported_operators()
    assert set(PROVISIONAL_OPERATORS) <= set(exported)
    marked = {
        name
        for name, cls in exported.items()
        if cls.__dict__.get("PROVISIONAL") is True
    }
    assert marked == set(PROVISIONAL_OPERATORS)
    for name, cls in exported.items():
        expected = name if name in PROVISIONAL_OPERATORS else None
        assert provisional_operator_name(cls) == expected, name


def test_polars_filter_is_provisional_and_names_its_issue() -> None:
    assert dict(PROVISIONAL_OPERATORS) == {"PolarsFilter": "ITL-1247"}
    assert "PROVISIONAL_OPERATORS" in operators.__all__


def test_a_subclass_is_provisional_as_its_nearest_marked_base() -> None:
    assert provisional_operator_name(_ProvisionalA) == "_ProvisionalA"
    assert provisional_operator_name(_ProvisionalB) == "_ProvisionalB"
    assert provisional_operator_name(_SubclassOfA) == "_ProvisionalA"

    class _UserFilter(PolarsFilter):
        pass

    assert provisional_operator_name(_UserFilter) == "PolarsFilter"


# ---------------------------------------------------------------------------
# The warning: at construction, once per operator per process
# ---------------------------------------------------------------------------


def test_constructing_polars_filter_warns_once_per_process() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = PolarsFilter(constraints={"subject": "s1"})
        _ = PolarsFilter(constraints={"subject": "s2", "session": 1})
    messages = _provisional_warnings(caught)
    assert len(messages) == 1
    message = messages[0]
    assert message.startswith("PolarsFilter is provisional:")
    for part in (
        "not frozen with data context v1",
        "stored paths and record ids",
        "every step downstream of PolarsFilter",
        "'Provisional operators' in docs/concepts/operators.md",
        "category=orcapod.errors.ProvisionalHashWarning",
    ):
        assert part in message


def test_the_warning_points_at_the_code_that_built_the_operator() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = PolarsFilter()
    (found,) = [w for w in caught if issubclass(w.category, ProvisionalHashWarning)]
    assert found.filename == __file__


def test_the_stream_method_warns_too() -> None:
    import polars as pl

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        filtered = _stream().polars_filter(pl.col("age") > 3, session=1)
    assert len(_provisional_warnings(caught)) == 1
    assert filtered.as_table()["subject"].to_pylist() == ["s3"]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        filtered = _stream().polars_filter(pl.col("age") > 3, pl.col("weight") > 20)
        rows = filtered.as_table()
    assert _provisional_warnings(caught) == []
    assert rows["subject"].to_pylist() == ["s2"]


@pytest.mark.parametrize(
    "order", [(_ProvisionalA, _ProvisionalB), (_ProvisionalB, _ProvisionalA)]
)
def test_two_provisional_operators_warn_once_each_in_any_order(
    order: tuple[type, type],
) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for cls in (*order, *order):
            _ = cls()
    messages = _provisional_warnings(caught)
    assert [m.split(" ", 1)[0] for m in messages] == [cls.__name__ for cls in order]


def test_a_subclass_warns_under_the_name_of_its_provisional_base() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = _SubclassOfA()
        _ = _ProvisionalA()
    messages = _provisional_warnings(caught)
    assert len(messages) == 1
    assert messages[0].startswith("_ProvisionalA is provisional:")


def test_an_operator_that_is_not_provisional_does_not_warn() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = operators.SelectDataColumns(columns=["age", "weight"])
        _ = operators.Join()
    assert _provisional_warnings(caught) == []


def test_under_an_error_filter_every_construction_raises() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", ProvisionalHashWarning)
        for _ in range(2):
            with pytest.raises(ProvisionalHashWarning):
                _ = PolarsFilter()


# ---------------------------------------------------------------------------
# The record of uses
# ---------------------------------------------------------------------------


def test_construction_is_recorded_even_after_the_warning() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _ = PolarsFilter()
        with recording_provisional_uses() as used:
            _ = PolarsFilter(constraints={"subject": "s1"})
    assert used == {"PolarsFilter"}


@pytest.mark.parametrize("names", [("A", "B"), ("B", "A")])
def test_a_record_holds_every_name_used_in_any_order(names: tuple[str, str]) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with recording_provisional_uses() as used:
            for name in (*names, *names):
                warn_provisional_operator(name)
    assert used == {"A", "B"}


def test_nested_records_each_see_what_was_used_while_they_were_open() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with recording_provisional_uses() as outer:
            warn_provisional_operator("A")
            with recording_provisional_uses() as inner:
                warn_provisional_operator("B")
            # An equal set, open beside it, is a different record.
            with (
                recording_provisional_uses() as first,
                recording_provisional_uses() as second,
            ):
                pass
            warn_provisional_operator("C")
    assert outer == {"A", "B", "C"}
    assert inner == {"B"}
    assert first == second == set()


def test_a_record_closed_by_an_error_stops_recording() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with pytest.raises(RuntimeError), recording_provisional_uses() as used:
            raise RuntimeError("case failed")
        warn_provisional_operator("A")
    assert used == set()


def test_recording_changes_no_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with recording_provisional_uses():
            _ = PolarsFilter()
        with recording_provisional_uses():
            _ = PolarsFilter()
    assert len(_provisional_warnings(caught)) == 1
