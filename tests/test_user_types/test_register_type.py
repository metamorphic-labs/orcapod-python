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
    """Return a one-row ``DictSource`` holding ``value`` in data column ``p``."""
    return DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"])


def _type_name(value: Any) -> str:
    """Return the Arrow extension name of the column holding ``value``."""
    return _source(value).as_table().schema.field("p").type.extension_name


def _name_part(value: Any) -> str:
    """Return the name component of the extension name, the part before ``@``."""
    return _type_name(value).split("@")[0]


# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------


@op.register_type(name="itl881.Declared")
@dataclasses.dataclass
class Declared:
    """Declared under ``itl881.Declared``."""

    x: int


@dataclasses.dataclass
class Undeclared:
    """Module-level class with no declared name."""

    x: int


@op.register_type
@dataclasses.dataclass
class BareDecorated:
    """Registered with the bare decorator, so named by default."""

    x: int


@dataclasses.dataclass
class CalledLater:
    """Registered by a ``register_type`` call inside a test."""

    x: int


def test_declared_name_is_the_name_component() -> None:
    """A declared name is the name component of the type's identity."""
    assert _name_part(Declared(1)) == "itl881.Declared"


def test_module_class_defaults_to_module_path() -> None:
    """An undeclared module class is named by its module path and class name."""
    assert _name_part(Undeclared(1)) == f"{__name__}.Undeclared"


def test_bare_decorator_uses_the_naming_setting() -> None:
    """The bare decorator names the class by the default, qualified name."""
    assert _name_part(BareDecorated(1)) == f"{__name__}.BareDecorated"


def test_call_form_returns_the_class() -> None:
    """The call form returns the class and declares the name."""
    assert op.register_type(CalledLater, name="itl881.CalledLater") is CalledLater
    assert _name_part(CalledLater(1)) == "itl881.CalledLater"


@pytest.mark.parametrize("name", ["", "a@b", "a b", "orcapod.Point"])
def test_invalid_names_raise(name: str) -> None:
    """Empty, reserved, ``@`` and whitespace names raise ``ValueError``."""
    with pytest.raises(ValueError):
        op.register_type(name=name)(Undeclared)


def test_non_dataclass_raises_type_error() -> None:
    """A class that is neither a dataclass nor a pydantic model raises ``TypeError``."""

    class Plain:
        """Plain class, neither a dataclass nor a pydantic model."""

        x: int = 0

    with pytest.raises(TypeError, match="dataclasses and pydantic models"):
        op.register_type(Plain, name="itl881.Plain")


def test_local_class_raises() -> None:
    """Registering a class defined inside a function raises ``ValueError``."""

    @dataclasses.dataclass
    class Local:
        """Dataclass defined inside a function."""

        x: int

    with pytest.raises(ValueError, match="inside a function"):
        op.register_type(Local, name="itl881.Local")


@dataclasses.dataclass
class UsedFirst:
    """Used under its default name before a name is declared."""

    x: int


def test_declaring_after_use_under_another_name_raises() -> None:
    """Declaring another name for a class already used raises ``ValueError``."""
    _type_name(UsedFirst(1))
    with pytest.raises(ValueError, match="already registered as"):
        op.register_type(UsedFirst, name="itl881.UsedFirst")


@op.register_type(qualified_name=False)
@dataclasses.dataclass
class ShortNamed:
    """Registered with ``qualified_name=False``."""

    x: int


def test_qualified_name_false_names_a_module_class_by_its_class_name() -> None:
    """``qualified_name=False`` names a module class by its class name alone."""
    assert _name_part(ShortNamed(1)) == "ShortNamed"


def test_name_and_qualified_name_together_raise() -> None:
    """Passing both ``name`` and ``qualified_name`` raises ``ValueError``."""
    with pytest.raises(ValueError, match="either name"):
        op.register_type(Undeclared, name="itl881.X", qualified_name=False)


@dataclasses.dataclass
class DeclaredTwice:
    """Declared more than once inside a test."""

    x: int


def test_redeclaring_with_the_same_name_is_a_no_op_and_another_name_raises() -> None:
    """Redeclaring the same name returns the class; another name raises."""
    op.register_type(DeclaredTwice, name="itl881.Twice")
    assert op.register_type(DeclaredTwice, name="itl881.Twice") is DeclaredTwice
    with pytest.raises(ValueError, match="already registered as 'itl881.Twice'"):
        op.register_type(DeclaredTwice, name="itl881.Other")


@dataclasses.dataclass
class NotMutated:
    """Class whose attributes are compared before and after registration."""

    x: int


def test_registration_does_not_modify_the_class() -> None:
    """Registering and using a class leaves its attributes unchanged."""
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
    """Declared under ``itl881.Shape`` with an ``int`` field ``y``."""

    x: int
    y: int


@op.register_type(name="itl881.Shape")
@dataclasses.dataclass
class ShapeFloat:
    """Declared under ``itl881.Shape`` with a ``float`` field ``y``."""

    x: int
    y: float


@op.register_type(name="itl881.Shape")
class ShapeModel(pydantic.BaseModel):
    """Pydantic model under ``itl881.Shape`` with the fields of ``ShapeInt``."""

    x: int
    y: int


def test_same_name_different_field_type_is_a_different_type() -> None:
    """One name with a different field type is another type and pipeline hash."""
    a, b = _source(ShapeInt(1, 2)), _source(ShapeFloat(1, 2.0))
    assert _type_name(ShapeInt(1, 2)) != _type_name(ShapeFloat(1, 2.0))
    assert a.pipeline_hash() != b.pipeline_hash()


def test_dataclass_and_pydantic_model_are_different_types() -> None:
    """A dataclass and a pydantic model with one name and fields are two types."""
    assert _type_name(ShapeInt(1, 2)) != _type_name(ShapeModel(x=1, y=2))


@op.register_type(name="itl881.Defaults")
@dataclasses.dataclass
class DefaultsA:
    """Declared under ``itl881.Defaults`` with a default, a method and a property."""

    x: int = 0

    def method(self) -> int:
        """Return ``x``."""
        return self.x

    @property
    def doubled(self) -> int:
        """Return twice ``x``."""
        return 2 * self.x


@op.register_type(name="itl881.Defaults")
@dataclasses.dataclass
class DefaultsB:
    """Same fields as DefaultsA; different default, no methods."""

    x: int = 5


def test_defaults_methods_and_properties_are_not_identity() -> None:
    """Defaults, methods and properties are not part of the identity."""
    first = _type_name(DefaultsA(1))
    with pytest.warns(UserWarning, match="same identity"):
        second = _type_name(DefaultsB(1))
    assert first == second


@op.register_type(name="itl881.Computed")
class ComputedA(pydantic.BaseModel):
    """Pydantic model under ``itl881.Computed`` with a computed field."""

    x: int

    @pydantic.computed_field
    @property
    def doubled(self) -> int:
        """Return twice ``x``."""
        return 2 * self.x


@op.register_type(name="itl881.Computed")
class ComputedB(pydantic.BaseModel):
    """Pydantic model under ``itl881.Computed`` without the computed field."""

    x: int


def test_pydantic_computed_fields_are_not_identity() -> None:
    """Pydantic computed fields are not part of the identity."""
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
    """Nested class declared under ``itl881.InnerA``."""

    value: int


@op.register_type(name="itl881.InnerB")
@dataclasses.dataclass
class InnerB:
    """Nested class declared under ``itl881.InnerB`` with the fields of ``InnerA``."""

    value: int


@op.register_type(name="itl881.InnerA")
@dataclasses.dataclass
class InnerAFloat:
    """Nested class declared under ``itl881.InnerA`` with a ``float`` field."""

    value: float


@op.register_type(name="itl881.Outer")
@dataclasses.dataclass
class OuterA:
    """Declared under ``itl881.Outer``; nests ``InnerA``."""

    inner: InnerA


@op.register_type(name="itl881.Outer")
@dataclasses.dataclass
class OuterB:
    """Declared under ``itl881.Outer``; nests ``InnerB``."""

    inner: InnerB


@op.register_type(name="itl881.Outer")
@dataclasses.dataclass
class OuterAFloat:
    """Declared under ``itl881.Outer``; nests ``InnerAFloat``."""

    inner: InnerAFloat


def test_nested_class_contributes_its_name() -> None:
    """An outer class's identity changes with the name of its nested class."""
    assert _type_name(OuterA(InnerA(1))) != _type_name(OuterB(InnerB(1)))


def test_nested_class_contributes_its_fields() -> None:
    """An outer class's identity changes with the fields of its nested class."""
    assert _type_name(OuterA(InnerA(1))) != _type_name(OuterAFloat(InnerAFloat(1.0)))


@dataclasses.dataclass
class LateInner:
    """Nested class declared only after ``EarlyOuter`` was decorated."""

    value: int


@op.register_type(name="itl881.OrderOuter")
@dataclasses.dataclass
class EarlyOuter:
    """Declared under ``itl881.OrderOuter`` before its nested class is declared."""

    inner: LateInner


# Declared after the outer class was decorated, before either is used.
op.register_type(LateInner, name="itl881.OrderInner")


@op.register_type(name="itl881.OrderInner")
@dataclasses.dataclass
class FirstInner:
    """Nested class declared under ``itl881.OrderInner`` before its outer class."""

    value: int


@op.register_type(name="itl881.OrderOuter")
@dataclasses.dataclass
class SecondOuter:
    """Declared under ``itl881.OrderOuter`` after its nested class."""

    inner: FirstInner


def test_registration_order_does_not_matter() -> None:
    """Declaring a nested class before or after its outer class gives one identity."""
    late_order = _type_name(EarlyOuter(LateInner(1)))
    with pytest.warns(UserWarning, match="same identity"):
        natural_order = _type_name(SecondOuter(FirstInner(1)))
    assert late_order == natural_order


def test_nested_values_round_trip() -> None:
    """A nested value reads back equal, with the inner value as its own class."""
    _, data = next(iter(_source(OuterA(InnerA(3))).iter_data()))
    value = data.as_dict()["p"]
    assert value == OuterA(InnerA(3))
    assert type(value.inner) is InnerA


@dataclasses.dataclass
class Node:
    """Class that refers to itself."""

    value: int
    next: Node | None = None


@dataclasses.dataclass
class CycleA:
    """Class that refers to ``CycleB``, which refers back."""

    b: CycleB | None = None


@dataclasses.dataclass
class CycleB:
    """Class that refers to ``CycleA``, which refers back."""

    a: CycleA | None = None


def test_self_referential_class_is_rejected_naming_the_field() -> None:
    """A self-referential class raises ``RecursiveTypeError`` naming the field."""
    with pytest.raises(RecursiveTypeError, match=r"Node\.next -> Node"):
        _source(Node(1))


def test_cyclic_classes_are_rejected_naming_the_path() -> None:
    """Mutually referential classes raise ``RecursiveTypeError`` naming the path."""
    with pytest.raises(RecursiveTypeError, match=r"CycleA\.b -> CycleB\.a -> CycleA"):
        _source(CycleA())


# ---------------------------------------------------------------------------
# Subclasses
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Base:
    """Base class of ``Child``."""

    x: int


@dataclasses.dataclass
class Child(Base):
    """Subclass of ``Base`` with one more field."""

    z: int = 0


def test_subclass_has_its_own_identity_and_keeps_its_fields() -> None:
    """A subclass has its own identity and keeps its fields."""
    assert _type_name(Base(1)) != _type_name(Child(1, 2))
    _, data = next(iter(_source(Child(1, 2)).iter_data()))
    assert data.as_dict()["p"] == Child(1, 2)


# ---------------------------------------------------------------------------
# Optional nested classes
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class OptionalInner:
    """A class nested in ``OptionalOuter`` as an optional field."""

    v: int


@dataclasses.dataclass
class OptionalOuter:
    """A class whose nested class may be ``None``."""

    child: OptionalInner | None


class OptionalOuterModel(pydantic.BaseModel):
    """A pydantic model whose nested class may be ``None``."""

    child: OptionalInner | None


@pytest.mark.parametrize(
    "value",
    [
        OptionalOuter(OptionalInner(1)),
        OptionalOuter(None),
        OptionalOuterModel(child=OptionalInner(1)),
        OptionalOuterModel(child=None),
    ],
    ids=["dataclass-set", "dataclass-none", "pydantic-set", "pydantic-none"],
)
def test_optional_nested_class_reads_back(value: Any) -> None:
    """A nested class stored as ``None`` reads back as ``None``, not a ``TypeError``."""
    _, data = next(iter(_source(value).iter_data()))
    assert data.as_dict()["p"] == value
