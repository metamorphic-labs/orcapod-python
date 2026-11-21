"""A schema hash is the hash of the declared schema (developer, November 21, 2026, ITL-640).

``hash_schema`` brings every extension type, in whatever form it arrives, to one form
before hashing: its storage type with its identifier (``ARROW:extension:name`` and
``ARROW:extension:metadata``) as field metadata, at any depth (list, large list and
fixed-size list items, struct children, map keys and items). It looks nothing up, and
no value handler rewrites a field: the identifier pins how the type's values are hashed.
So one logical type has one schema hash in its three forms:

- live: a ``pa.ExtensionType``, as Orcapod builds it;
- metadata: the storage type with the two keys, as a table read back from a store or
  passed through ``normalize_extension_columns`` carries it;
- unregistered: what a process reads when pyarrow has not registered the type.

The types are not a hand list: every built-in logical type of the default data context
is enumerated from a converter built fresh from the shipped registration
(``create_registry().get_context()``: ``registered_logical_type_names``,
``logical_type_factory_categories``), so a new built-in type is tested as soon as it is
registered, and what other code registers in this process (a native container made on
first use, a user class) never changes the set. ``hash_table`` is a separate procedure and is unchanged.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import re
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pydantic
import pytest

import orcapod as op
from orcapod import PipelineJob, function_pod
from orcapod.contexts import create_registry, get_default_context
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import MapData
from orcapod.core.streams.base import typed_arrow_schemas
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
from orcapod.databases.sqlite_connector import SQLiteConnector
from orcapod.errors import UnstorableColumnError
from orcapod.semantic_types.nested_storage import NestedTypeNotStorableError
from orcapod.sources import ArrowTableSource
from orcapod.system_constants import constants
from orcapod.types import Schema
from orcapod.utils.arrow_utils import normalize_extension_columns

pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")

NAME_KEY = b"ARROW:extension:name"
METADATA_KEY = b"ARROW:extension:metadata"


def _converter() -> Any:
    """The default data context's type converter."""
    return get_default_context().type_converter


def _hasher() -> Any:
    """The default data context's Arrow hasher."""
    return get_default_context().arrow_hasher


# ── the logical types, from the registry ─────────────────────────────────────

_BUILT_IN: Any = create_registry().get_context().type_converter
"""A converter built fresh from the shipped registration: the built-in types only."""

NAMED: tuple[str, ...] = _BUILT_IN.registered_logical_type_names()
"""Every built-in logical type with a fixed extension name."""

# Whether a column of the type is hashed by value in ``hash_table``. Only one test reads
# this pin: it makes a newly registered type, or a changed value handler, visible in
# review. Every other test takes the types from ``NAMED``.
EXPECTED_CLASSIFICATION: dict[str, bool] = {
    "orcapod.directory": True,
    "orcapod.file": True,
    "orcapod.numpy.ndarray": True,
    "orcapod.pandas.dataframe": True,
    "orcapod.pandas.series": True,
    "orcapod.path": False,
    "orcapod.polars.dataframe": True,
    "orcapod.polars.series": True,
    "orcapod.torch.tensor": True,
    "orcapod.upath": False,
    "orcapod.uuid": False,
}


@dataclasses.dataclass
class FormsExact:
    """An exact user class (module path and qualified name) with a logical-type field."""

    p: Path
    n: int


@op.dataclass
class FormsStructural:
    """A structural user class (name and fields) with a logical-type field."""

    p: Path
    n: int


class FormsModel(pydantic.BaseModel):
    """A pydantic model (an exact class)."""

    a: int
    b: str


# One example or more per logical type factory, keyed by the factory's category.
FACTORY_EXAMPLES: dict[str, dict[str, Any]] = {
    "list": {
        "list[File]": list[op.File],
        "list[UUID]": list[uuid.UUID],
        "list[list[Directory]]": list[list[op.Directory]],
    },
    "set": {
        "set[int]": set[int],
        "set[str]": set[str],
        "set[UUID]": set[uuid.UUID],
        "set[File]": set[op.File],
    },
    "orcapod.dataclass": {"exact class": FormsExact},
    "orcapod.structural": {"structural class": FormsStructural},
    "orcapod.pydantic": {"pydantic model": FormsModel},
}

FACTORY_CASES: dict[str, Any] = {
    f"{category}: {label}": python_type
    for category, examples in FACTORY_EXAMPLES.items()
    for label, python_type in examples.items()
}

CASES: tuple[str, ...] = (*NAMED, *FACTORY_CASES)


def _available_type(name: str) -> pa.ExtensionType | None:
    """The live extension type registered under ``name``; ``None`` when the optional
    package it needs (torch) is not installed."""
    try:
        logical_type = _converter().get_logical_type_by_arrow_extension_name(name)
    except ImportError:  # an optional package that is not installed
        return None
    assert logical_type is not None, name
    arrow_type = logical_type.get_arrow_extension_type()
    assert isinstance(arrow_type, pa.ExtensionType)
    return arrow_type


def _named_type(name: str) -> pa.ExtensionType:
    """The live extension type registered under ``name``; a test of one type is
    skipped when its optional package is not installed."""
    arrow_type = _available_type(name)
    if arrow_type is None:
        pytest.skip(f"{name}: its optional package is not installed")
    return arrow_type


def _available_names() -> list[str]:
    """The named types whose optional package, if any, is installed."""
    return [name for name in NAMED if _available_type(name) is not None]


def _arrow_type(python_type: Any) -> pa.DataType:
    """The live Arrow type of ``python_type`` (registering a user class first)."""
    if isinstance(python_type, type) and (
        dataclasses.is_dataclass(python_type)
        or issubclass(python_type, pydantic.BaseModel)
    ):
        _converter().register_python_class(python_type)
    return _converter().python_type_to_arrow_type(python_type)


def _case_type(case: str) -> pa.ExtensionType:
    """The live extension type of a case of ``CASES``."""
    arrow_type = (
        _arrow_type(FACTORY_CASES[case]) if case in FACTORY_CASES else _named_type(case)
    )
    assert isinstance(arrow_type, pa.ExtensionType), case
    return arrow_type


def _python_type(name: str) -> Any:
    """The Python type a named logical type reads as."""
    return _converter().arrow_type_to_python_type(_named_type(name))


def _value_hashed(arrow_type: pa.ExtensionType) -> bool:
    """Whether ``hash_table`` replaces the type's cells by their value hashes."""
    python_type = _converter().arrow_type_to_python_type(arrow_type)
    registry = get_default_context().semantic_hasher.type_handler_registry
    handler = registry.get_handler_for_type(python_type)
    return handler is not None and getattr(handler, "COLUMN_VALUE_HANDLER", True)


# ── the three forms, built independently of the code under test ──────────────


def _metadata_field(field: pa.Field[Any]) -> pa.Field[Any]:
    """``field`` with every extension type, at any depth, in the metadata form."""
    arrow_type = field.type
    if isinstance(arrow_type, pa.ExtensionType):
        metadata = cast("dict[bytes | str, bytes | str]", dict(field.metadata or {}))
        metadata[NAME_KEY] = arrow_type.extension_name.encode("utf-8")
        metadata[METADATA_KEY] = arrow_type.__arrow_ext_serialize__()
        return field.with_type(_metadata_type(arrow_type.storage_type)).with_metadata(
            metadata
        )
    return field.with_type(_metadata_type(arrow_type))


def _metadata_type(arrow_type: pa.DataType) -> pa.DataType:
    """``arrow_type`` with every nested extension type in the metadata form."""
    if pa.types.is_struct(arrow_type):
        return pa.struct([_metadata_field(child) for child in arrow_type])
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(_metadata_field(arrow_type.value_field))
    if pa.types.is_fixed_size_list(arrow_type):
        return pa.list_(_metadata_field(arrow_type.value_field), arrow_type.list_size)
    if pa.types.is_list(arrow_type):
        return pa.list_(_metadata_field(arrow_type.value_field))
    if pa.types.is_map(arrow_type):
        return pa.map_(
            _metadata_field(arrow_type.key_field),  # pyright: ignore[reportArgumentType]  # pyarrow takes a field; the stubs name only types
            _metadata_field(arrow_type.item_field),  # pyright: ignore[reportArgumentType]  # as above
        )
    return arrow_type


def _metadata_form(schema: pa.Schema) -> pa.Schema:
    """The schema with every extension type in the metadata (IPC) form."""
    return pa.schema([_metadata_field(field) for field in schema])


def _extension_types(arrow_type: pa.DataType) -> Iterator[pa.ExtensionType]:
    """Every extension type in ``arrow_type``, at any depth."""
    if isinstance(arrow_type, pa.ExtensionType):
        yield arrow_type
        arrow_type = arrow_type.storage_type
    if pa.types.is_struct(arrow_type):
        for child in arrow_type:
            yield from _extension_types(child.type)
    elif pa.types.is_map(arrow_type):
        yield from _extension_types(arrow_type.key_type)
        yield from _extension_types(arrow_type.item_type)
    elif (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        yield from _extension_types(arrow_type.value_type)


@contextmanager
def _unregistered(
    names: set[str], restore: dict[str, pa.ExtensionType]
) -> Iterator[None]:
    """Unregister ``names`` from pyarrow; register ``restore``'s types again after."""
    unregistered = []
    try:
        for name in sorted(names):
            try:
                pa.unregister_extension_type(name)
                unregistered.append(name)
            except KeyError:  # not registered: nothing to undo
                pass
        yield
    finally:
        for name in unregistered:
            pa.register_extension_type(restore[name])  # pyright: ignore[reportArgumentType]  # the stubs name PyExtensionType; pyarrow takes any ExtensionType


def _unregistered_form(
    schema: pa.Schema, registered: pa.Schema | None = None
) -> pa.Schema:
    """The schema as Arrow IPC gives it to a process that has not registered its types.

    ``registered`` holds the types pyarrow had registered under the names ``schema``
    uses (default ``schema`` itself); pass it when ``schema`` holds a look-alike type
    under a registered name.
    """
    restore = {
        t.extension_name: t
        for field in (registered or schema)
        for t in _extension_types(field.type)
    }
    names = {t.extension_name for field in schema for t in _extension_types(field.type)}
    with _unregistered(names, restore):
        read = pa.ipc.read_schema(schema.serialize())
    assert not any(any(_extension_types(field.type)) for field in read)
    return read


def _unregistered_table(table: pa.Table) -> pa.Table:
    """``table`` read back through Arrow IPC with its types unregistered."""
    restore = {
        t.extension_name: t
        for field in table.schema
        for t in _extension_types(field.type)
    }
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    with _unregistered(set(restore), restore):
        read = pa.ipc.open_stream(sink.getvalue()).read_all()
    assert not any(any(_extension_types(field.type)) for field in read.schema)
    return read


def _non_large(schema: pa.Schema) -> pa.Schema:
    """A metadata-form schema with every storage type in its non-large variant."""

    def field_of(field: pa.Field[Any]) -> pa.Field[Any]:
        """``field`` with its storage types in their non-large variants."""
        return field.with_type(type_of(field.type))

    def type_of(arrow_type: pa.DataType) -> pa.DataType:
        """``arrow_type`` in its non-large variant, at any depth."""
        if arrow_type == pa.large_string():
            return pa.string()
        if arrow_type == pa.large_binary():
            return pa.binary()
        if pa.types.is_large_list(arrow_type):
            return pa.list_(field_of(arrow_type.value_field))
        if pa.types.is_list(arrow_type):
            return pa.list_(field_of(arrow_type.value_field))
        if pa.types.is_fixed_size_list(arrow_type):
            return pa.list_(field_of(arrow_type.value_field), arrow_type.list_size)
        if pa.types.is_struct(arrow_type):
            return pa.struct([field_of(child) for child in arrow_type])
        if pa.types.is_map(arrow_type):
            return pa.map_(
                field_of(arrow_type.key_field),  # pyright: ignore[reportArgumentType]  # pyarrow takes a field; the stubs name only types
                field_of(arrow_type.item_field),  # pyright: ignore[reportArgumentType]  # as above
            )
        return arrow_type

    return pa.schema([field_of(field) for field in schema])


class _LookAlike(pa.ExtensionType):
    """An unregistered extension type with any identifier over any storage."""

    def __init__(self, name: str, metadata: bytes, storage: pa.DataType) -> None:
        """Hold an identifier (``name``, ``metadata``) over ``storage``."""
        self._serialized = metadata
        super().__init__(storage, name)

    def __arrow_ext_serialize__(self) -> bytes:
        """The extension metadata given."""
        return self._serialized

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> pa.ExtensionType:
        """Never called: the type is never registered."""
        raise NotImplementedError("never registered")


def _other_identifiers(arrow_type: pa.ExtensionType) -> dict[str, _LookAlike]:
    """Types over the same storage whose identifier differs from ``arrow_type``'s."""
    name = arrow_type.extension_name
    serialized = arrow_type.__arrow_ext_serialize__()
    others = {
        "another name": _LookAlike(
            f"{name}.other", serialized, arrow_type.storage_type
        ),
        "other metadata": _LookAlike(name, serialized + b" ", arrow_type.storage_type),
    }
    try:
        record = json.loads(serialized)
    except ValueError:
        record = None
    if isinstance(record, dict):
        if "version" in record:
            bumped = {**record, "version": record["version"] + 1}
            others["next version"] = _LookAlike(
                name, json.dumps(bumped).encode(), arrow_type.storage_type
            )
        if "ignore" in record:
            ignoring = {**record, "ignore": sorted({*record["ignore"], "byte_order"})}
            others["another ignore list"] = _LookAlike(
                name, json.dumps(ignoring).encode(), arrow_type.storage_type
            )
        if "fields" in record:
            changed = {**record, "fields": [*record["fields"], ["extra", "int"]]}
            others["another identity record"] = _LookAlike(
                name, json.dumps(changed).encode(), arrow_type.storage_type
            )
    return others


ORDERS = ("x w n", "n x w")


def _schema(
    arrow_type: pa.DataType, order: str = "x w n", x_nullable: bool = True
) -> pa.Schema:
    """Two columns of ``arrow_type`` (one nullable, one not) and a plain column."""
    fields = {
        "x": pa.field("x", arrow_type, nullable=x_nullable),
        "w": pa.field("w", arrow_type, nullable=False),
        "n": pa.field("n", pa.int32()),
    }
    return pa.schema([fields[name] for name in order.split()])


def _forms(schema: pa.Schema) -> dict[str, pa.Schema]:
    """The schema in the live, metadata and unregistered forms."""
    return {
        "live": schema,
        "metadata": _metadata_form(schema),
        "unregistered": _unregistered_form(schema),
    }


def _hash(schema: pa.Schema) -> Any:
    """``hash_schema`` of ``schema``."""
    return _hasher().hash_schema(schema)


# ── 1. every registered logical type is covered ──────────────────────────────


def test_every_registered_type_is_classified() -> None:
    """A new registered type, or a changed value handler, is seen here first.

    The inventory is compared before any type is loaded, so it holds without an
    optional package; each installed type's classification is then checked."""
    assert set(NAMED) == set(EXPECTED_CLASSIFICATION)
    for name in _available_names():
        assert _value_hashed(_named_type(name)) == EXPECTED_CLASSIFICATION[name], name


def test_every_logical_type_factory_has_an_example() -> None:
    """Each factory category is tested through ``FACTORY_EXAMPLES``."""
    assert set(_BUILT_IN.logical_type_factory_categories()) == set(FACTORY_EXAMPLES)


def test_the_cases_are_every_registered_type() -> None:
    """Every named type is a case, File and Directory among them."""
    assert set(NAMED) <= set(CASES)
    assert {"orcapod.file", "orcapod.directory"} <= set(NAMED)


# ── 2. one schema hash per type, in every form ───────────────────────────────


@pytest.mark.parametrize("case", CASES)
class TestOneTypeOneSchemaHash:
    """One schema hash per logical type: what must agree, what must differ."""

    def test_the_three_forms_give_one_hash(self, case: str) -> None:
        """Live, metadata and unregistered forms: one hash, in two column orders."""
        for order in ORDERS:
            forms = _forms(_schema(_case_type(case), order))
            hashes = {form: _hash(schema) for form, schema in forms.items()}
            assert len(set(hashes.values())) == 1, hashes

    def test_column_order_is_not_part_of_it(self, case: str) -> None:
        """Two column orders: one hash."""
        arrow_type = _case_type(case)
        assert len({_hash(_schema(arrow_type, order)) for order in ORDERS}) == 1

    def test_the_metadata_form_is_what_normalize_extension_columns_gives(
        self, case: str
    ) -> None:
        """The test's metadata form is the library's own."""
        schema = _schema(_case_type(case))
        normalized = normalize_extension_columns(schema.empty_table()).schema
        assert normalized == _metadata_form(schema)
        assert _hash(normalized) == _hash(schema)

    def test_large_and_non_large_storage_give_one_hash(self, case: str) -> None:
        """Large and non-large storage variants: one hash."""
        schema = _schema(_case_type(case))
        expected = _hash(schema)
        for form in ("metadata", "unregistered"):
            assert _hash(_non_large(_forms(schema)[form])) == expected, form

    def test_nullability_is_part_of_it_in_every_form(self, case: str) -> None:
        """A nullable and a non-null column differ; each agrees across forms."""
        arrow_type = _case_type(case)
        nullable = _forms(_schema(arrow_type, x_nullable=True))
        non_null = _forms(_schema(arrow_type, x_nullable=False))
        assert len({_hash(schema) for schema in nullable.values()}) == 1
        assert len({_hash(schema) for schema in non_null.values()}) == 1
        assert _hash(nullable["live"]) != _hash(non_null["live"])

    def test_a_look_alike_with_the_same_identifier_is_the_same_type(
        self, case: str
    ) -> None:
        """Nothing is looked up: the identifier and the storage type decide."""
        arrow_type = _case_type(case)
        look_alike = _LookAlike(
            arrow_type.extension_name,
            arrow_type.__arrow_ext_serialize__(),
            arrow_type.storage_type,
        )
        assert _hash(_schema(look_alike)) == _hash(_schema(arrow_type))

    def test_another_identifier_is_another_hash_in_every_form(self, case: str) -> None:
        """A different name or metadata differs from the type, in every form."""
        arrow_type = _case_type(case)
        base = _schema(arrow_type)
        expected = _hash(base)
        for label, other in _other_identifiers(arrow_type).items():
            schema = _schema(other)
            forms = {
                "live": schema,
                "metadata": _metadata_form(schema),
                "unregistered": _unregistered_form(schema, registered=base),
            }
            hashes = {form: _hash(given) for form, given in forms.items()}
            assert len(set(hashes.values())) == 1, (label, hashes)
            assert expected not in hashes.values(), label


def test_two_structural_classes_of_another_field_differ() -> None:
    """A user class's identity record is in its identifier."""

    @op.dataclass
    class FormsStructural2:
        """A structural class with another field than ``FormsStructural``."""

        p: Path
        m: int

    first = _schema(_arrow_type(FormsStructural))
    second = _schema(_arrow_type(FormsStructural2))
    for form in ("live", "metadata", "unregistered"):
        assert _hash(_forms(first)[form]) != _hash(_forms(second)[form])


def test_the_named_types_have_distinct_hashes() -> None:
    """Every installed named type has its own schema hash."""
    hashes = {name: _hash(_schema(_named_type(name))) for name in _available_names()}
    assert len(set(hashes.values())) == len(hashes)


# ── 3. containers of every type, in every form ───────────────────────────────


def _arrow_containers(arrow_type: pa.DataType) -> dict[str, pa.DataType]:
    """Arrow's own containers holding ``arrow_type``."""
    return {
        "list": pa.list_(arrow_type),
        "large_list": pa.large_list(arrow_type),
        "fixed_size_list": pa.list_(arrow_type, 2),
        "struct field": pa.struct(
            [pa.field("f", arrow_type), pa.field("m", pa.int8())]
        ),
        "map value": pa.map_(pa.large_string(), arrow_type),
        "two deep": pa.large_list(
            pa.struct([pa.field("f", pa.large_list(arrow_type))])
        ),
    }


def _safe(name: str) -> str:
    """``name`` usable in a Python class name."""
    return re.sub(r"\W", "_", name)


def _holder_classes(name: str) -> dict[str, Any]:
    """An exact and a structural user class with a field of the named type."""
    python_type = _python_type(name)
    exact = dataclasses.make_dataclass(
        f"HoldsExact_{_safe(name)}", [("f", python_type), ("n", int)]
    )
    exact.__module__ = __name__
    structural = op.dataclass(
        type(
            f"HoldsStructural_{_safe(name)}",
            (),
            {"__annotations__": {"f": python_type, "n": int}, "__module__": __name__},
        )
    )
    return {"exact class field": exact, "structural class field": structural}


def _orcapod_containers(name: str) -> dict[str, pa.DataType]:
    """Orcapod's containers of the named type: native containers and user classes."""
    python_type = _python_type(name)
    holders = _holder_classes(name)
    exact = holders["exact class field"]
    types: dict[str, Any] = {
        "list[T]": list[python_type],
        "set[T]": set[python_type],
        "list[list[T]]": list[list[python_type]],
        **holders,
        "list[exact class]": list[exact],
    }
    return {label: _arrow_type(python_type) for label, python_type in types.items()}


@pytest.mark.parametrize("name", NAMED)
class TestContainers:
    """Containers of every named type, Arrow's and Orcapod's, in every form."""

    def test_arrow_containers_give_one_hash_in_every_form(self, name: str) -> None:
        """Arrow containers of the type: one hash in every form and layout."""
        for label, container in _arrow_containers(_named_type(name)).items():
            for order in ORDERS:
                schema = _schema(container, order)
                hashes = {form: _hash(s) for form, s in _forms(schema).items()}
                hashes["non-large"] = _hash(_non_large(_metadata_form(schema)))
                assert len(set(hashes.values())) == 1, (label, order, hashes)

    def test_arrow_containers_of_another_identifier_differ(self, name: str) -> None:
        """Arrow containers of a look-alike identifier differ, in every form."""
        arrow_type = _named_type(name)
        for other_label, other in _other_identifiers(arrow_type).items():
            for label, container in _arrow_containers(arrow_type).items():
                base = _schema(container)
                schema = _schema(_arrow_containers(other)[label])
                for given in (
                    schema,
                    _metadata_form(schema),
                    _unregistered_form(schema, registered=base),
                ):
                    assert _hash(given) != _hash(base), (label, other_label)

    def test_orcapod_containers_give_one_hash_in_every_form(self, name: str) -> None:
        """Orcapod's containers and class fields: one hash in every form."""
        for label, container in _orcapod_containers(name).items():
            assert isinstance(container, pa.ExtensionType), label
            for order in ORDERS:
                schema = _schema(container, order)
                hashes = {form: _hash(s) for form, s in _forms(schema).items()}
                hashes["non-large"] = _hash(_non_large(_metadata_form(schema)))
                assert len(set(hashes.values())) == 1, (label, order, hashes)

    def test_a_dict_of_the_type_is_refused_at_definition(self, name: str) -> None:
        """A logical type cannot be a dict value yet (ITL-1150): Arrow covers the map."""
        with pytest.raises(NestedTypeNotStorableError):
            _converter().python_type_to_arrow_type(dict[str, _python_type(name)])


@pytest.mark.parametrize(
    "label",
    ["list[T]", "set[T]", "list[list[T]]", "exact class field", "list[exact class]"],
)
def test_containers_of_two_types_differ_in_every_form(label: str) -> None:
    """A container of one named type never hashes as a container of another."""
    per_type = {
        name: _schema(_orcapod_containers(name)[label]) for name in _available_names()
    }
    for form in ("live", "metadata", "unregistered"):
        hashes = {
            name: _hash(_forms(schema)[form]) for name, schema in per_type.items()
        }
        assert len(set(hashes.values())) == len(hashes), form


# ── 4. identity paths: sources, slots, saved jobs, sync and async ────────────

KINDS = ("orcapod.file", "orcapod.directory", "orcapod.numpy.ndarray")
TAGS = ("k", "t1", "t2")


def _values(kind: str, root: Path) -> list[Any]:
    """Four values of the kind; files and directories with two contents each."""
    values: list[Any] = []
    for index in range(4):
        if kind == "orcapod.file":
            path = root / f"f{index}.txt"
            path.write_text(f"content {index % 2}")
            values.append(op.File(path))
        elif kind == "orcapod.directory":
            path = root / f"d{index}"
            path.mkdir(exist_ok=True)
            (path / "x.txt").write_text(f"content {index % 2}")
            values.append(op.Directory(path))
        else:
            values.append(np.arange(index + 1, dtype=np.int16))
    return values


def _python_schemas(kind: str) -> tuple[Schema, Schema]:
    """The tag and data Python schemas of the identity-path tables."""
    python_type = _python_type(kind)
    return (
        Schema({"k": int, "t1": python_type, "t2": python_type}),
        Schema({"v1": python_type, "v2": python_type | None, "n": int}),
    )


def _declared_arrow(kind: str) -> pa.Schema:
    """The Arrow schema the Python declaration means: tags, then data."""
    tags, data = typed_arrow_schemas(SourceNode("src", *_python_schemas(kind)))
    return pa.schema([*tags, *data])


def _table(kind: str, root: Path) -> pa.Table:
    """Two columns of the kind among the tags, two among the data."""
    root.mkdir(parents=True, exist_ok=True)
    values = _values(kind, root)
    tags, data = _python_schemas(kind)
    rows = [
        {
            "k": index,
            "t1": values[index],
            "t2": values[(index + 1) % 4],
            "v1": values[(index + 2) % 4],
            "v2": values[(index + 3) % 4],
            "n": index * 10,
        }
        for index in range(3)
    ]
    table = _converter().python_dicts_to_arrow_table(
        rows, python_schema={**tags, **data}
    )
    return table.cast(_declared_arrow(kind))


def _source(table: pa.Table, source_id: str = "s") -> ArrowTableSource:
    """An in-memory source of ``table`` with tags ``k``, ``t1``, ``t2``."""
    return ArrowTableSource(
        table, tag_columns=list(TAGS), source_id=source_id, tag_only_columns=True
    )


def _system_tag_names(stream: Any) -> tuple[str, ...]:
    """The system-tag column names of ``stream``."""
    names = stream.as_table(columns={"system_tags": True}).column_names
    return tuple(n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX))


def _identity(stream: Any) -> tuple[Any, ...]:
    """Content hash, pipeline hash and system-tag names: what must be equal."""
    return (stream.content_hash(), stream.pipeline_hash(), _system_tag_names(stream))


def _read_back(database: Any, table: pa.Table) -> pa.Table:
    """``table`` written to ``database`` and read back, in its own column order."""
    record_ids = pa.array([f"r{i}".encode() for i in range(table.num_rows)])
    database.add_records(
        ("forms",),
        table.append_column("rid", record_ids.cast(pa.large_binary())),
        record_id_column="rid",
        flush=True,
    )
    stored = database.get_all_records(("forms",))
    assert stored is not None
    return stored.select(table.column_names)


SOURCE_FORMS: dict[str, Callable[[pa.Table, Path], pa.Table]] = {
    "in memory": lambda table, root: table,
    "columns reordered": lambda table, root: table.select(
        ["v2", "t2", "n", "k", "v1", "t1"]
    ),
    "in-memory database": lambda table, root: _read_back(
        InMemoryArrowDatabase(), table
    ),
    "delta": lambda table, root: _read_back(DeltaTableDatabase(root / "delta"), table),
    "metadata form": lambda table, root: normalize_extension_columns(table),
    "unregistered": lambda table, root: _unregistered_table(table),
}


@pytest.mark.parametrize("kind", KINDS)
class TestIdentityPaths:
    """File, Directory and ndarray columns along every identity path."""

    @pytest.mark.parametrize("form", list(SOURCE_FORMS))
    def test_a_source_has_one_identity_however_its_table_arrives(
        self, kind: str, form: str, tmp_path: Path
    ) -> None:
        """In memory, reordered, read back, metadata or unregistered: one identity."""
        table = _table(kind, tmp_path / "values")
        given = SOURCE_FORMS[form](table, tmp_path)
        assert _identity(_source(given)) == _identity(_source(table))

    def test_the_sqlite_connector_refuses_the_type(
        self, kind: str, tmp_path: Path
    ) -> None:
        """SQLite has no column type for it: refused loudly, never stored as text."""
        database = ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "s.db")))
        with pytest.raises(UnstorableColumnError):
            _read_back(database, _table(kind, tmp_path / "values"))

    def test_another_type_is_another_identity(self, kind: str, tmp_path: Path) -> None:
        """File, Directory and ndarray sources differ."""
        table = _table(kind, tmp_path / "values")
        others = [k for k in KINDS if k != kind]
        for other in others:
            other_table = _table(other, tmp_path / other)
            first, second = _identity(_source(table)), _identity(_source(other_table))
            assert first[0] != second[0] and first[1] != second[1]
            assert first[2] != second[2]

    def test_another_identifier_is_another_identity(
        self, kind: str, tmp_path: Path
    ) -> None:
        """The same table, its identifier's metadata changed: another source."""
        stored = normalize_extension_columns(_table(kind, tmp_path / "values"))

        def bumped(field: pa.Field[Any]) -> pa.Field[Any]:
            """``field`` with its identifier's version incremented."""
            metadata = dict(field.metadata or {})
            if NAME_KEY not in metadata:
                return field
            record = json.loads(metadata[METADATA_KEY])
            record["version"] += 1
            metadata[METADATA_KEY] = json.dumps(record).encode()
            return field.with_metadata(cast("dict[bytes | str, bytes | str]", metadata))

        other = stored.cast(pa.schema([bumped(field) for field in stored.schema]))
        first, second = _identity(_source(stored)), _identity(_source(other))
        assert first[1] != second[1] and first[2] != second[2]

    def _slots(self, kind: str, tmp_path: Path) -> dict[str, SourceNode]:
        """The slot declared in Python types, in each Arrow form, and from a stream."""
        tags, data = _python_schemas(kind)
        live = _declared_arrow(kind)
        tag_fields = [live.field(name) for name in TAGS]
        data_fields = [field for field in live if field.name not in TAGS]
        arrow_forms = {
            "live pa.Schema": live,
            "metadata pa.Schema": _metadata_form(live),
            "unregistered pa.Schema": _unregistered_form(live),
        }
        slots = {"python types": SourceNode("src", tags, data)}
        for label, schema in arrow_forms.items():
            slots[label] = SourceNode(
                "src",
                pa.schema([schema.field(f.name) for f in tag_fields]),
                pa.schema([schema.field(f.name) for f in data_fields]),
            )
        slots["from a stream"] = SourceNode.from_stream(
            _source(_table(kind, tmp_path / "values")), name="src"
        )
        return slots

    def test_a_slot_has_one_identity_however_it_is_declared(
        self, kind: str, tmp_path: Path
    ) -> None:
        """Every slot form: one identity, Python schema and Arrow schema."""
        slots = self._slots(kind, tmp_path)
        expected = typed_arrow_schemas(slots["python types"])
        for label, slot in slots.items():
            assert slot.pipeline_hash() == slots["python types"].pipeline_hash(), label
            assert slot.content_hash() == slots["python types"].content_hash(), label
            assert slot.output_schema() == slots["python types"].output_schema(), label
            assert typed_arrow_schemas(slot) == expected, label

    def test_a_job_has_one_identity_bound_run_saved_and_loaded(
        self, kind: str, tmp_path: Path
    ) -> None:
        """Every slot form, sync and async, before and after a save and load."""
        table = _table(kind, tmp_path / "values")
        seen: dict[tuple[str, str], tuple[Any, ...]] = {}
        for label, slot in self._slots(kind, tmp_path).items():
            for path in ("sync", "async"):
                root = tmp_path / _safe(label) / path
                database = DeltaTableDatabase(root / "db")
                job = PipelineJob(name="forms", database=database)
                with job:
                    renamed = MapData({"v1": "z"})(slot, label="renamed")
                    _count.pod(renamed, label="counted")
                job.bind(sources={"src": _source(table)})
                if path == "sync":
                    job.run()
                else:
                    asyncio.run(job.run_async())
                job.save(root / "job.json")
                loaded = PipelineJob.load(root / "job.json", database=database)
                identity = []
                for which in (job, loaded):
                    for node in ("renamed", "counted"):
                        identity += [
                            which.nodes[node].content_hash(),
                            which.nodes[node].pipeline_hash(),
                        ]
                    stored = which.nodes["counted"].as_source()
                    identity += [*_identity(stored), stored.as_table().num_rows]
                half = len(identity) // 2
                assert identity[:half] == identity[half:], (label, path)
                ran = [
                    _system_tag_names(job.nodes[node])
                    for node in ("renamed", "counted")
                ]
                assert all(len(names) == 2 for names in ran), ran
                seen[label, path] = (*identity, *ran)
        assert len(set(seen.values())) == 1, seen


@function_pod(output_keys="m")
def _count(n: int) -> int:
    """A function pod over ``n``, for the jobs of the identity paths."""
    return n + 1


# ── 5. value hashing is unchanged ─────────────────────────────────────────────


@pytest.mark.parametrize("kind", ["orcapod.file", "orcapod.directory"])
def test_a_file_or_directory_column_is_still_hashed_by_content(
    kind: str, tmp_path: Path
) -> None:
    """Two paths with one content: one table hash; as stored strings, two."""
    values = _values(kind, tmp_path)  # values 0 and 2 have one content
    python_type = _python_type(kind)
    tables = [
        _converter().python_dicts_to_arrow_table(
            [{"c": value}], python_schema={"c": python_type}
        )
        for value in (values[0], values[2])
    ]
    assert _hasher().hash_table(tables[0]) == _hasher().hash_table(tables[1])
    stored = [normalize_extension_columns(t) for t in tables]
    assert _hasher().hash_table(stored[0]) != _hasher().hash_table(stored[1])
