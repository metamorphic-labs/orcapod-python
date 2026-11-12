"""Declared field order is part of a user class's identity (ITL-881).

A dataclass compares its fields as a tuple in declared order, and a pydantic model
dumps, iterates and serializes in declared order, so pod code can observe the order.
Two classes with the same fields in another order are therefore different types.

Identity is observed through the public interface: the Arrow type of a ``DictSource``
column, the row's ``content_hash`` and the source's ``content_hash`` and
``pipeline_hash``. Names under ``itl881.order.`` are used by no other test, because
declarations are process-wide.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pyarrow as pa
import pydantic
import pytest

import orcapod as op
from orcapod import function_pod
from orcapod.sources import DictSource


@op.register_type(name="itl881.order.Data")
@dataclasses.dataclass
class DataXY:
    x: int
    y: int


@op.register_type(name="itl881.order.Data")
@dataclasses.dataclass
class DataYX:
    y: int
    x: int


@op.register_type(name="itl881.order.Model")
class ModelXY(pydantic.BaseModel):
    x: int
    y: int


@op.register_type(name="itl881.order.Model")
class ModelYX(pydantic.BaseModel):
    y: int
    x: int


@op.register_type(name="itl881.order.Outer")
@dataclasses.dataclass
class OuterOfXY:
    inner: DataXY


@op.register_type(name="itl881.order.Outer")
@dataclasses.dataclass
class OuterOfYX:
    inner: DataYX


PAIRS: dict[str, tuple[Any, Any]] = {
    "dataclass": (DataXY(x=1, y=2), DataYX(x=1, y=2)),
    "pydantic": (ModelXY(x=1, y=2), ModelYX(x=1, y=2)),
    "list": ([DataXY(x=1, y=2)], [DataYX(x=1, y=2)]),
    "nested": (OuterOfXY(DataXY(x=1, y=2)), OuterOfYX(DataYX(x=1, y=2))),
}


def _source(value: Any) -> DictSource:
    return DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"], source_id="s")


def _column_type(value: Any) -> pa.ExtensionType:
    column_type = _source(value).as_table().schema.field("p").type
    assert isinstance(column_type, pa.ExtensionType)
    return column_type


def _type_name(value: Any) -> str:
    return _column_type(value).extension_name


def _row_hash(value: Any) -> str:
    _, data = next(iter(_source(value).iter_data()))
    return data.content_hash().to_string()


@pytest.fixture(params=list(PAIRS))
def pair(request: pytest.FixtureRequest) -> tuple[Any, Any]:
    """Two values that differ only in the declared field order of their class."""
    return PAIRS[request.param]


def test_other_field_order_is_another_type(pair: tuple[Any, Any]) -> None:
    first, second = pair
    assert _type_name(first) != _type_name(second)


def test_other_field_order_has_other_hashes(pair: tuple[Any, Any]) -> None:
    first, second = pair
    assert _row_hash(first) != _row_hash(second)
    assert _source(first).content_hash() != _source(second).content_hash()
    assert _source(first).pipeline_hash() != _source(second).pipeline_hash()


@pytest.mark.parametrize(
    ("value", "names"),
    [
        (DataXY(x=1, y=2), ["x", "y"]),
        (DataYX(x=1, y=2), ["y", "x"]),
        (ModelXY(x=1, y=2), ["x", "y"]),
        (ModelYX(x=1, y=2), ["y", "x"]),
    ],
)
def test_stored_struct_lists_fields_in_declared_order(
    value: Any, names: list[str]
) -> None:
    storage = _column_type(value).storage_type
    assert isinstance(storage, pa.StructType)
    assert storage.names == names


def test_values_of_both_orders_read_back_as_their_own_class(
    pair: tuple[Any, Any],
) -> None:
    for value in pair:
        _, data = next(iter(_source(value).iter_data()))
        read = data.as_dict()["p"]
        assert read == value
        assert type(read) is type(value)


def test_pods_over_the_two_orders_have_different_pipeline_hashes() -> None:
    @function_pod(output_keys="q")
    def keep_xy(p: DataXY) -> DataXY:
        return p

    @function_pod(output_keys="q")
    def keep_yx(p: DataYX) -> DataYX:
        return p

    xy = keep_xy.pod(_source(DataXY(x=1, y=2)))
    yx = keep_yx.pod(_source(DataYX(x=1, y=2)))
    assert xy.pipeline_hash() != yx.pipeline_hash()
    assert (
        xy.as_table().schema.field("q").type.extension_name
        != yx.as_table().schema.field("q").type.extension_name
    )


@op.register_type(name="itl881.order.Same")
@dataclasses.dataclass
class SameFirst:
    a: int
    b: str


@op.register_type(name="itl881.order.Same")
@dataclasses.dataclass
class SameSecond:
    a: int
    b: str


def test_same_fields_in_the_same_order_are_one_type() -> None:
    with pytest.warns(UserWarning, match="same identity"):
        assert _type_name(SameFirst(1, "x")) == _type_name(SameSecond(1, "x"))
    assert _row_hash(SameFirst(1, "x")) == _row_hash(SameSecond(1, "x"))
