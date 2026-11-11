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
    return get_default_context().type_converter


def _make_class(module: str, qualname: str = "Point") -> type:
    cls = dataclasses.make_dataclass(qualname, [("x", int)])
    cls.__module__ = module
    cls.__qualname__ = qualname
    return cls


class TestIdentityString:
    def test_documented_example(self) -> None:
        record = uci.identity_record("dataclass", "Point", {"x": "int", "y": "float"})
        assert uci.identity_string(record) == (
            "Point@9117beaaf7ebe3ab0bd25de9448c92eb64aa070a58611517bd1e470a544584d8"
        )

    def test_canonical_json_is_sorted_and_compact(self) -> None:
        record = uci.identity_record("pydantic", "P", {"b": "int", "a": "str"})
        assert (
            uci.canonical_json(record)
            == '{"fields":{"a":"str","b":"int"},"kind":"pydantic","name":"P"}'
        )

    def test_field_order_does_not_matter(self) -> None:
        a = uci.identity_record("dataclass", "P", {"x": "int", "y": "str"})
        b = uci.identity_record("dataclass", "P", {"y": "str", "x": "int"})
        assert uci.identity_string(a) == uci.identity_string(b)

    @pytest.mark.parametrize(
        "other",
        [
            ("pydantic", "Point", {"x": "int", "y": "float"}),
            ("dataclass", "Point2", {"x": "int", "y": "float"}),
            ("dataclass", "Point", {"x": "int", "y": "int"}),
            ("dataclass", "Point", {"x": "int", "z": "float"}),
            ("dataclass", "Point", {"x": "int"}),
        ],
    )
    def test_each_component_changes_the_identity(
        self, other: tuple[str, str, dict[str, str]]
    ) -> None:
        base = uci.identity_record("dataclass", "Point", {"x": "int", "y": "float"})
        assert uci.identity_string(base) != uci.identity_string(
            uci.identity_record(*other)
        )

    def test_describe_fields(self) -> None:
        assert uci.describe_fields({"y": "float", "x": "int"}) == "{x: int, y: float}"


class TestTypeTokens:
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
        assert uci.type_token(annotation, converter) == token


class TestNames:
    @pytest.mark.parametrize(
        "name", ["", "a@b", "a b", "orcapod.thing", "tab\tname", 3]
    )
    def test_invalid_names_rejected(self, name: object) -> None:
        with pytest.raises((ValueError, TypeError)):
            uci.validate_name(name)  # pyright: ignore[reportArgumentType]

    @pytest.mark.parametrize("name", ["Point", "geo.Point", "my-types.Point_2"])
    def test_valid_names_accepted(self, name: str) -> None:
        uci.validate_name(name)

    def test_module_class_defaults_to_qualified(self) -> None:
        assert uci.resolve_name(_make_class("pkg.models")) == "pkg.models.Point"

    @pytest.mark.parametrize("module", ["__main__", "__mp_main__"])
    def test_main_class_defaults_to_name(self, module: str) -> None:
        assert uci.resolve_name(_make_class(module)) == "Point"

    def test_nested_main_class_uses_qualname(self) -> None:
        assert uci.resolve_name(_make_class("__main__", "Outer.Point")) == "Outer.Point"

    def test_default_name_qualified_false(self) -> None:
        assert uci.default_name(_make_class("pkg.models"), qualified=False) == "Point"

    def test_default_name_qualified_true_for_main(self) -> None:
        cls = _make_class("__main__")
        assert uci.default_name(cls, qualified=True) == "__main__.Point"

    def test_declared_name_wins(self) -> None:
        cls = _make_class("pkg.models")
        uci.declare(cls, "geo.Point")
        assert uci.resolve_name(cls) == "geo.Point"
