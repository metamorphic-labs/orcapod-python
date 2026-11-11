"""Public API for user class identity: ``op.register_type`` (ITL-881).

Identity is observed through the public interface: the Arrow extension name of a
``DictSource`` column (``as_table``), the source's ``pipeline_hash`` and values read back
with ``iter_data``. Every test declares names under ``itl881.`` that no other test uses,
because declarations are process-wide.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pydantic
import pytest

import orcapod as op
from orcapod.errors import RecursiveTypeError
from orcapod.sources import DictSource


def _source(value: Any) -> DictSource:
    return DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"])


def _type_name(value: Any) -> str:
    return _source(value).as_table().schema.field("p").type.extension_name


def _name_part(value: Any) -> str:
    return _type_name(value).split("@")[0]


# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------


@op.register_type(name="itl881.Declared")
@dataclasses.dataclass
class Declared:
    x: int


@dataclasses.dataclass
class Undeclared:
    x: int


@op.register_type
@dataclasses.dataclass
class BareDecorated:
    x: int


@dataclasses.dataclass
class CalledLater:
    x: int


def test_declared_name_is_the_name_component() -> None:
    assert _name_part(Declared(1)) == "itl881.Declared"


def test_module_class_defaults_to_module_path() -> None:
    assert _name_part(Undeclared(1)) == f"{__name__}.Undeclared"


def test_bare_decorator_uses_the_naming_setting() -> None:
    assert _name_part(BareDecorated(1)) == f"{__name__}.BareDecorated"


def test_call_form_returns_the_class() -> None:
    assert op.register_type(CalledLater, name="itl881.CalledLater") is CalledLater
    assert _name_part(CalledLater(1)) == "itl881.CalledLater"


@pytest.mark.parametrize("name", ["", "a@b", "a b", "orcapod.Point"])
def test_invalid_names_raise(name: str) -> None:
    with pytest.raises(ValueError):
        op.register_type(name=name)(Undeclared)


def test_non_dataclass_raises_type_error() -> None:
    class Plain:
        x: int = 0

    with pytest.raises(TypeError, match="dataclasses and pydantic models"):
        op.register_type(Plain, name="itl881.Plain")


def test_local_class_raises() -> None:
    @dataclasses.dataclass
    class Local:
        x: int

    with pytest.raises(ValueError, match="inside a function"):
        op.register_type(Local, name="itl881.Local")


@dataclasses.dataclass
class UsedFirst:
    x: int


def test_declaring_after_use_under_another_name_raises() -> None:
    _type_name(UsedFirst(1))
    with pytest.raises(ValueError, match="already registered as"):
        op.register_type(UsedFirst, name="itl881.UsedFirst")


@op.register_type(qualified=False)
@dataclasses.dataclass
class ShortNamed:
    x: int


def test_qualified_false_names_a_module_class_by_its_class_name() -> None:
    assert _name_part(ShortNamed(1)) == "ShortNamed"


def test_name_and_qualified_together_raise() -> None:
    with pytest.raises(ValueError, match="either name"):
        op.register_type(Undeclared, name="itl881.X", qualified=False)


@dataclasses.dataclass
class DeclaredTwice:
    x: int


def test_redeclaring_with_the_same_name_is_a_no_op_and_another_name_raises() -> None:
    op.register_type(DeclaredTwice, name="itl881.Twice")
    assert op.register_type(DeclaredTwice, name="itl881.Twice") is DeclaredTwice
    with pytest.raises(ValueError, match="already registered as 'itl881.Twice'"):
        op.register_type(DeclaredTwice, name="itl881.Other")


@dataclasses.dataclass
class NotMutated:
    x: int


def test_registration_does_not_modify_the_class() -> None:
    before = dict(vars(NotMutated))
    params = NotMutated.__dataclass_params__  # pyright: ignore[reportAttributeAccessIssue]
    op.register_type(NotMutated, name="itl881.NotMutated")
    _type_name(NotMutated(1))
    assert dict(vars(NotMutated)) == before
    assert NotMutated.__dataclass_params__ is params  # pyright: ignore[reportAttributeAccessIssue]


# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------


@op.register_type(name="itl881.Shape")
@dataclasses.dataclass
class ShapeInt:
    x: int
    y: int


@op.register_type(name="itl881.Shape")
@dataclasses.dataclass
class ShapeFloat:
    x: int
    y: float


@op.register_type(name="itl881.Shape")
class ShapeModel(pydantic.BaseModel):
    x: int
    y: int


def test_same_name_different_field_type_is_a_different_type() -> None:
    a, b = _source(ShapeInt(1, 2)), _source(ShapeFloat(1, 2.0))
    assert _type_name(ShapeInt(1, 2)) != _type_name(ShapeFloat(1, 2.0))
    assert a.pipeline_hash() != b.pipeline_hash()


def test_dataclass_and_pydantic_model_are_different_types() -> None:
    assert _type_name(ShapeInt(1, 2)) != _type_name(ShapeModel(x=1, y=2))


@op.register_type(name="itl881.Defaults")
@dataclasses.dataclass
class DefaultsA:
    x: int = 0

    def method(self) -> int:
        return self.x

    @property
    def doubled(self) -> int:
        return 2 * self.x


@op.register_type(name="itl881.Defaults")
@dataclasses.dataclass
class DefaultsB:
    """Same fields as DefaultsA; different default, no methods."""

    x: int = 5


def test_defaults_methods_and_properties_are_not_identity() -> None:
    first = _type_name(DefaultsA(1))
    with pytest.warns(UserWarning, match="same identity"):
        second = _type_name(DefaultsB(1))
    assert first == second


@op.register_type(name="itl881.Computed")
class ComputedA(pydantic.BaseModel):
    x: int

    @pydantic.computed_field
    @property
    def doubled(self) -> int:
        return 2 * self.x


@op.register_type(name="itl881.Computed")
class ComputedB(pydantic.BaseModel):
    x: int


def test_pydantic_computed_fields_are_not_identity() -> None:
    first = _type_name(ComputedA(x=1))
    with pytest.warns(UserWarning, match="same identity"):
        second = _type_name(ComputedB(x=1))
    assert first == second


# ---------------------------------------------------------------------------
# Nested classes (P1) and registration order
# ---------------------------------------------------------------------------


@op.register_type(name="itl881.InnerA")
@dataclasses.dataclass
class InnerA:
    value: int


@op.register_type(name="itl881.InnerB")
@dataclasses.dataclass
class InnerB:
    value: int


@op.register_type(name="itl881.InnerA")
@dataclasses.dataclass
class InnerAFloat:
    value: float


@op.register_type(name="itl881.Outer")
@dataclasses.dataclass
class OuterA:
    inner: InnerA


@op.register_type(name="itl881.Outer")
@dataclasses.dataclass
class OuterB:
    inner: InnerB


@op.register_type(name="itl881.Outer")
@dataclasses.dataclass
class OuterAFloat:
    inner: InnerAFloat


def test_nested_class_contributes_its_name() -> None:
    assert _type_name(OuterA(InnerA(1))) != _type_name(OuterB(InnerB(1)))


def test_nested_class_contributes_its_fields() -> None:
    assert _type_name(OuterA(InnerA(1))) != _type_name(OuterAFloat(InnerAFloat(1.0)))


@dataclasses.dataclass
class LateInner:
    value: int


@op.register_type(name="itl881.OrderOuter")
@dataclasses.dataclass
class EarlyOuter:
    inner: LateInner


# Declared after the outer class was decorated, before either is used.
op.register_type(LateInner, name="itl881.OrderInner")


@op.register_type(name="itl881.OrderInner")
@dataclasses.dataclass
class FirstInner:
    value: int


@op.register_type(name="itl881.OrderOuter")
@dataclasses.dataclass
class SecondOuter:
    inner: FirstInner


def test_registration_order_does_not_matter() -> None:
    late_order = _type_name(EarlyOuter(LateInner(1)))
    with pytest.warns(UserWarning, match="same identity"):
        natural_order = _type_name(SecondOuter(FirstInner(1)))
    assert late_order == natural_order


def test_nested_values_round_trip() -> None:
    _, data = next(iter(_source(OuterA(InnerA(3))).iter_data()))
    value = data.as_dict()["p"]
    assert value == OuterA(InnerA(3))
    assert type(value.inner) is InnerA


@dataclasses.dataclass
class Node:
    value: int
    next: Node | None = None


@dataclasses.dataclass
class CycleA:
    b: CycleB | None = None


@dataclasses.dataclass
class CycleB:
    a: CycleA | None = None


def test_self_referential_class_is_rejected_naming_the_field() -> None:
    with pytest.raises(RecursiveTypeError, match=r"Node\.next -> Node"):
        _source(Node(1))


def test_cyclic_classes_are_rejected_naming_the_path() -> None:
    with pytest.raises(RecursiveTypeError, match=r"CycleA\.b -> CycleB\.a -> CycleA"):
        _source(CycleA())


# ---------------------------------------------------------------------------
# Subclasses
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Base:
    x: int


@dataclasses.dataclass
class Child(Base):
    z: int = 0


def test_subclass_has_its_own_identity_and_keeps_its_fields() -> None:
    assert _type_name(Base(1)) != _type_name(Child(1, 2))
    _, data = next(iter(_source(Child(1, 2)).iter_data()))
    assert data.as_dict()["p"] == Child(1, 2)
