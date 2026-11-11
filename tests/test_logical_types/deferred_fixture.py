"""Fixture classes for the deferred-registration tests (ITL-871).

``Thing`` plays the role of an optional library's class (such as ``torch.Tensor``);
``LogicalThing`` and ``ThingHandler`` play the logical type and the semantic
handler that are only loaded on first use. Their ``instances`` counters show
when the registries instantiate them.
"""

from __future__ import annotations

from typing import Any, ClassVar

import polars as pl
import pyarrow as pa

from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.types import ContentHash

THING_EXTENSION_NAME = "orcapod_test.deferred_thing"


class Thing:
    """A value type registered lazily."""

    def __init__(self, text: str) -> None:
        self.text = text


class SubThing(Thing):
    """A subclass, resolved through the MRO."""


class LogicalThing(BaseLogicalType):
    """Logical type for ``Thing`` over ``large_string``."""

    instances: ClassVar[int] = 0
    _arrow_ext_class = make_arrow_extension_type(
        THING_EXTENSION_NAME, pa.large_string()
    )
    _polars_ext_class = make_polars_extension_type(
        THING_EXTENSION_NAME, pa.large_string()
    )
    logical_type_name: str = THING_EXTENSION_NAME
    python_type: type = Thing

    def __init__(self) -> None:
        LogicalThing.instances += 1

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        return self._arrow_ext_class()

    def get_polars_extension_type(self) -> pl.BaseExtension:
        return self._polars_ext_class()

    def python_to_storage(self, value: Any, converter: Any = None) -> str:
        return value.text

    def storage_to_python(self, storage_value: Any, converter: Any = None) -> Thing:
        return Thing(storage_value)


class ThingHandler:
    """Semantic handler for ``Thing``."""

    instances: ClassVar[int] = 0

    def __init__(self) -> None:
        ThingHandler.instances += 1

    def handle(self, obj: Any, hasher: Any) -> ContentHash:
        return ContentHash(method="thing_v1", digest=obj.text.encode())
