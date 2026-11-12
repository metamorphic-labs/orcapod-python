"""Identity record, identity string, type tokens and naming policy of user classes (ITL-881).

These tests target ``orcapod.logical_types.user_class_identity`` directly: they pin the
documented identity format, which is that module's own interface.
"""

from __future__ import annotations

import dataclasses
import datetime
import typing
from pathlib import Path
from typing import Literal, Optional

import pytest

from orcapod.contexts import get_default_context
from orcapod.logical_types import user_class_identity as uci


@pytest.fixture
def converter():
    """Return the default data context's type converter."""
    return get_default_context().type_converter


def _make_class(module: str, qualname: str = "Point") -> type:
    """Return a one-field dataclass with the given module and qualified name."""
    cls = dataclasses.make_dataclass(qualname, [("x", int)])
    cls.__module__ = module
    cls.__qualname__ = qualname
    return cls


class TestIdentityString:
    """The identity record, its canonical JSON and the identity string."""

    def test_documented_example(self) -> None:
        """The documented example record gives the documented identity string."""
        record = uci.identity_record(
            "dataclass", "Point", [("x", "int"), ("y", "float")]
        )
        assert uci.identity_string(record) == (
            "Point@76d961e40619a836535e3fdebdead7ea65aa9413955a889f3d5b51a7a94b65eb"
        )

    def test_canonical_json_is_compact_and_keeps_field_order(self) -> None:
        """Canonical JSON is compact, sorts keys and keeps the field order."""
        record = uci.identity_record("pydantic", "P", [("b", "int"), ("a", "str")])
        assert (
            uci.canonical_json(record)
            == '{"fields":[["b","int"],["a","str"]],"kind":"pydantic","name":"P"}'
        )

    def test_field_order_is_part_of_the_identity(self) -> None:
        """Records differing only in field order have different identity strings."""
        a = uci.identity_record("dataclass", "P", [("x", "int"), ("y", "str")])
        b = uci.identity_record("dataclass", "P", [("y", "str"), ("x", "int")])
        assert uci.identity_string(a) != uci.identity_string(b)

    @pytest.mark.parametrize(
        "other",
        [
            ("pydantic", "Point", [("x", "int"), ("y", "float")]),
            ("dataclass", "Point2", [("x", "int"), ("y", "float")]),
            ("dataclass", "Point", [("x", "int"), ("y", "int")]),
            ("dataclass", "Point", [("x", "int"), ("z", "float")]),
            ("dataclass", "Point", [("x", "int")]),
            ("dataclass", "Point", [("y", "float"), ("x", "int")]),
        ],
    )
    def test_each_component_changes_the_identity(
        self, other: tuple[str, str, list[tuple[str, str]]]
    ) -> None:
        """Changing the kind, the name or any field changes the identity string."""
        base = uci.identity_record("dataclass", "Point", [("x", "int"), ("y", "float")])
        assert uci.identity_string(base) != uci.identity_string(
            uci.identity_record(*other)
        )

    def test_describe_fields_keeps_the_given_order(self) -> None:
        """``describe_fields`` renders the fields in the order given."""
        assert (
            uci.describe_fields([["y", "float"], ["x", "int"]]) == "{y: float, x: int}"
        )


class TestTypeTokens:
    """Type tokens of field annotations."""

    @pytest.mark.parametrize(
        ("annotation", "token"),
        [
            (int, "int"),
            (str, "str"),
            (bool, "bool"),
            (bytes, "bytes"),
            (float, "float"),
            (datetime.datetime, "datetime.datetime"),
            (Path, "orcapod.path"),
            (int | None, "int | None"),
            (Optional[int], "int | None"),
            (None | int, "int | None"),
            (list[int], "list[int]"),
            (typing.List[int], "list[int]"),  # noqa: UP006 - the typing alias is the case under test
            (set[str], "set[str]"),
            (dict[str, float], "dict[str, float]"),
            (list[int | None], "list[int | None]"),
            (Literal["b", "a"], "Literal['a', 'b']"),
        ],
    )
    def test_token(self, converter, annotation: object, token: str) -> None:
        """Each annotation maps to its canonical type token."""
        assert uci.type_token(annotation, converter) == token


class TestNames:
    """Name validation, default names and declared names."""

    @pytest.mark.parametrize(
        "name", ["", "a@b", "a b", "orcapod.thing", "tab\tname", 3]
    )
    def test_invalid_names_rejected(self, name: object) -> None:
        """Empty, non-string, reserved, ``@`` and whitespace names are rejected."""
        with pytest.raises((ValueError, TypeError)):
            uci.validate_name(name)  # pyright: ignore[reportArgumentType]

    @pytest.mark.parametrize("name", ["Point", "geo.Point", "my-types.Point_2"])
    def test_valid_names_accepted(self, name: str) -> None:
        """Dotted names with letters, digits, ``-`` and ``_`` are accepted."""
        uci.validate_name(name)

    def test_module_class_defaults_to_qualified(self) -> None:
        """A module class is named by its module path and class name by default."""
        assert uci.resolve_name(_make_class("pkg.models")) == "pkg.models.Point"

    @pytest.mark.parametrize("module", ["__main__", "__mp_main__"])
    def test_main_class_defaults_to_name(self, module: str) -> None:
        """A class in ``__main__`` or ``__mp_main__`` is named by its class name."""
        assert uci.resolve_name(_make_class(module)) == "Point"

    def test_nested_main_class_uses_qualname(self) -> None:
        """A nested ``__main__`` class is named by its qualified name."""
        assert uci.resolve_name(_make_class("__main__", "Outer.Point")) == "Outer.Point"

    def test_default_name_qualified_name_false(self) -> None:
        """``qualified_name=False`` names a module class by its class name alone."""
        assert (
            uci.default_name(_make_class("pkg.models"), qualified_name=False) == "Point"
        )

    @pytest.mark.parametrize("module", ["__main__", "__mp_main__"])
    def test_default_name_qualified_name_true_for_main(self, module: str) -> None:
        """``qualified_name=True`` names a main-module class ``__main__.<name>``."""
        cls = _make_class(module)
        assert uci.default_name(cls, qualified_name=True) == "__main__.Point"

    def test_declared_names_are_nfc_normalized(self) -> None:
        """Composed and decomposed spellings of a name declare one NFC name."""
        composed, decomposed = "Caf\u00e9", "Cafe\u0301"
        first, second = _make_class("pkg.a"), _make_class("pkg.b")
        uci.declare(first, composed)
        uci.declare(second, decomposed)
        assert uci.resolve_name(first) == uci.resolve_name(second) == composed

    def test_declared_name_wins(self) -> None:
        """A declared name replaces the default name."""
        cls = _make_class("pkg.models")
        uci.declare(cls, "geo.Point")
        assert uci.resolve_name(cls) == "geo.Point"
