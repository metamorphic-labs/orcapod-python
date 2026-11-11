"""Deferred registration of optional value types (ITL-871).

A registry can name a value type by its qualified class name and extension name
without importing it; the type is loaded on the first lookup that needs it.
"""

from __future__ import annotations

import pytest

from orcapod.errors import MissingOptionalDependencyError
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    PythonTypeHandlerRegistry,
)
from orcapod.logical_types.registry import LogicalTypeRegistry

from . import deferred_fixture as fx

_MOD = fx.__name__


def _registry(requires: str = "polars") -> LogicalTypeRegistry:
    return LogicalTypeRegistry(
        deferred_logical_types=[
            {
                "python_type": f"{_MOD}.Thing",
                "extension_name": fx.THING_EXTENSION_NAME,
                "logical_type": f"{_MOD}.LogicalThing",
                "requires": requires,
            }
        ]
    )


def _handlers() -> PythonTypeHandlerRegistry:
    return PythonTypeHandlerRegistry(
        deferred_handlers=[
            {"python_type": f"{_MOD}.Thing", "handler": f"{_MOD}.ThingHandler"}
        ]
    )


def test_python_type_lookup_loads_the_deferred_type_once() -> None:
    registry = _registry()
    before = fx.LogicalThing.instances
    first = registry.get_by_python_type(fx.Thing)
    second = registry.get_by_python_type(fx.Thing)
    assert isinstance(first, fx.LogicalThing)
    assert second is first
    assert fx.LogicalThing.instances == before + 1
    assert registry.get_by_arrow_extension_name(fx.THING_EXTENSION_NAME) is first


def test_subclass_resolves_through_the_mro() -> None:
    assert isinstance(_registry().get_by_python_type(fx.SubThing), fx.LogicalThing)


def test_extension_name_lookup_loads_the_deferred_type() -> None:
    registry = _registry()
    lt = registry.get_by_arrow_extension_name(fx.THING_EXTENSION_NAME)
    assert isinstance(lt, fx.LogicalThing)
    assert registry.get_by_python_type(fx.Thing) is lt


def test_unrelated_lookups_load_nothing() -> None:
    registry = _registry()
    before = fx.LogicalThing.instances
    assert registry.get_by_python_type(complex) is None
    assert registry.get_by_arrow_extension_name("orcapod_test.unknown") is None
    assert fx.LogicalThing.instances == before


def test_missing_dependency_names_extension_and_module() -> None:
    registry = _registry(requires="orcapod_test_no_such_module")
    with pytest.raises(MissingOptionalDependencyError) as exc_info:
        registry.get_by_arrow_extension_name(fx.THING_EXTENSION_NAME)
    message = str(exc_info.value)
    assert fx.THING_EXTENSION_NAME in message
    assert "orcapod_test_no_such_module" in message
    assert isinstance(exc_info.value, ImportError)


def test_handler_lookup_loads_the_deferred_handler_once() -> None:
    handlers = _handlers()
    before = fx.ThingHandler.instances
    first = handlers.get_handler(fx.Thing("a"))
    assert isinstance(first, fx.ThingHandler)
    assert handlers.get_handler(fx.SubThing("b")) is first
    assert handlers.get_handler_for_type(fx.Thing) is first
    assert fx.ThingHandler.instances == before + 1


def test_handler_lookup_for_unrelated_type_loads_nothing() -> None:
    handlers = _handlers()
    before = fx.ThingHandler.instances
    assert handlers.get_handler(1.5) is None
    assert handlers.get_handler_for_type(complex) is None
    assert fx.ThingHandler.instances == before
