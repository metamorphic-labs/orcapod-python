"""A logical type is one thing in every Arrow representation (ITL-640, Codex reviews of PR 454).

A logical type's identifier is its extension name plus its extension metadata, however a
field carries it: as a live ``pa.ExtensionType``, or as its storage type with the
``ARROW:extension:name`` / ``ARROW:extension:metadata`` field metadata (the storage, or
IPC, form of a table read back from a store; also what a process sees when pyarrow has
not registered the extension type). Two forms, one walker
(``logical_types.canonical_form``):

1. ``hash_schema`` hashes the storage form (its tests over every registered type are in
   ``test_schema_hash_storage_form.py``);
2. a read-only loaded operator (``Index``, ``Pick``) types ``node.as_source()`` from its
   saved Arrow types;
3. the saved-schema writer writes the storage form, so a saved schema keeps an
   identifier carried in either form, at any depth;
4. a schema declared in Arrow, and a saved schema read back, take the declaration form
   (``declaration_form_schema``): a metadata-form identifier at the top of a column
   declares that logical type;
5. an identifier nested in a plain list, struct or map stays in the storage form
   everywhere: Arrow cannot build an empty table of a nested live extension type.
"""

from __future__ import annotations

import dataclasses
import json
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest

import orcapod as op
from orcapod import PipelineJob
from orcapod.contexts import get_default_context
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import Index, MapData, Pick
from orcapod.core.sources import ArrowTableSource, PollingSource
from orcapod.core.streams.base import typed_arrow_schemas, typed_zero_row_table

from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    Pipeline,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.serialization import (
    arrow_schema_from_texts,
    arrow_type_texts,
    job_type_texts_at_version,
    pipeline_type_texts_at_version,
    type_text_at_version,
)
from orcapod.types import Cursor
from orcapod.utils.arrow_utils import normalize_extension_columns

from ..test_core.operators.test_async_sync_equivalence import source_a


@dataclasses.dataclass
class FormsExact:
    """An exact user class (module path and qualified name)."""

    a: int
    b: str


@op.dataclass
class FormsStructural:
    """A structural user class (name and fields)."""

    a: int
    b: str


def _converter() -> Any:
    """The default data context's type converter."""
    return get_default_context().type_converter


def _hasher() -> Any:
    """The default data context's Arrow hasher."""
    return get_default_context().arrow_hasher


# Every built-in logical type, the native containers, user classes of both kinds and a
# library value type with its ignore list.
LOGICAL: dict[str, Any] = {
    "File": op.File,
    "Directory": op.Directory,
    "Path": Path,
    "UUID": uuid.UUID,
    "list[File]": list[op.File],
    "list[Path]": list[Path],
    "set[int]": set[int],
    "set[UUID]": set[uuid.UUID],
    "exact class": FormsExact,
    "structural class": FormsStructural,
    "numpy.ndarray": np.ndarray,
}


def _schema(python_type: Any) -> pa.Schema:
    """Two columns of the logical type, live form, plus a plain column."""
    if isinstance(python_type, type) and dataclasses.is_dataclass(python_type):
        _converter().register_python_class(python_type)
    arrow_type = _converter().python_type_to_arrow_type(python_type)
    return pa.schema(
        [
            pa.field("x", arrow_type),
            pa.field("w", arrow_type, nullable=False),
            pa.field("n", pa.int32()),
        ]
    )


def _metadata_form(schema: pa.Schema) -> pa.Schema:
    """The same schema with every logical type in its IPC (metadata) form."""
    return normalize_extension_columns(schema.empty_table()).schema


def _extension_types(arrow_type: pa.DataType) -> list[pa.ExtensionType]:
    """Every live extension type in ``arrow_type``, at any depth."""
    found: list[pa.ExtensionType] = []
    if isinstance(arrow_type, pa.ExtensionType):
        found.append(arrow_type)
        arrow_type = arrow_type.storage_type
    if pa.types.is_struct(arrow_type):
        for i in range(arrow_type.num_fields):
            found += _extension_types(arrow_type.field(i).type)
    elif (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        found += _extension_types(arrow_type.value_type)
    return found


def _unregistered_form(schema: pa.Schema) -> pa.Schema:
    """The schema as a process reads it when pyarrow has not registered its types.

    The table goes through Arrow IPC while each of its extension types is unregistered
    from pyarrow, so the reader gets the storage types and the ``ARROW:extension:*``
    keys, as a store read in a fresh process does; the types are registered again
    afterwards.
    """
    types = {t.extension_name: t for f in schema for t in _extension_types(f.type)}
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(schema.empty_table())
    unregistered = []
    try:
        for name in types:
            try:
                pa.unregister_extension_type(name)
                unregistered.append(name)
            except KeyError:
                pass
        read = pa.ipc.open_stream(sink.getvalue()).read_all().schema
    finally:
        for name in unregistered:
            pa.register_extension_type(types[name])  # pyright: ignore[reportArgumentType]  # the stubs name PyExtensionType; pyarrow takes any ExtensionType
    assert not any(_extension_types(f.type) for f in read), "still registered"
    return read


def test_two_logical_types_still_differ() -> None:
    """The canonical form never merges two identifiers."""
    file_schema = _metadata_form(_schema(op.File))
    path_schema = _metadata_form(_schema(Path))
    assert _hasher().hash_schema(file_schema) != _hasher().hash_schema(path_schema)


# ── 3. the saved schema keeps the identifier in either form ──────────────────


@pytest.mark.parametrize("name", list(LOGICAL))
def test_the_writer_keeps_an_identifier_in_metadata_form(name: str) -> None:
    """Each form writes the live form's text, and reads back to its hash."""
    live = _schema(LOGICAL[name])
    for given in (live, _metadata_form(live), _unregistered_form(live)):
        texts = arrow_type_texts(given)
        assert texts == arrow_type_texts(live)
        restored = arrow_schema_from_texts(
            texts, _converter(), [f.name for f in given if f.nullable]
        )
        assert _hasher().hash_schema(restored) == _hasher().hash_schema(live)


def test_the_writer_keeps_a_nested_identifier_in_metadata_form() -> None:
    """A nested metadata-form identifier is written as its identifier."""
    path = _converter().python_type_to_arrow_type(Path)
    metadata = _metadata_form(pa.schema([pa.field("p", path)])).field("p").metadata
    assert metadata is not None
    child = pa.field("p", pa.large_string()).with_metadata(
        cast("dict[bytes | str, bytes | str]", metadata)
    )
    nested = pa.schema(
        [
            pa.field("s", pa.struct([child, pa.field("q", pa.int8())])),
            pa.field("l", pa.large_list(child)),
        ]
    )
    texts = arrow_type_texts(nested)
    assert "extension<orcapod.path;" in texts["s"]
    assert "extension<orcapod.path;" in texts["l"]


def test_a_saved_pipeline_keeps_a_metadata_form_slot(tmp_path: Path) -> None:
    """End to end: a slot declared in metadata form saves and loads one identity."""
    live = pa.schema(
        [
            pa.field("x", _converter().python_type_to_arrow_type(Path)),
            pa.field("w", pa.int64()),
        ]
    )
    tags = pa.schema(
        [pa.field("k", pa.int64(), nullable=False), pa.field("g", pa.int64())]
    )
    slot = SourceNode(name="src", tag_schema=tags, data_schema=_metadata_form(live))
    pipeline = Pipeline(name="forms")
    with pipeline:
        MapData({"x": "z"})(slot, label="renamed")
    path = tmp_path / "pipeline.json"
    pipeline.save(path)
    saved = json.loads(path.read_text())
    (config,) = [
        node["source_config"]
        for node in saved["nodes"].values()
        if "source_config" in node
    ]
    assert config["data_schema"]["x"].startswith("extension<orcapod.path;")
    loaded = Pipeline.load(path)
    assert (
        loaded.nodes["renamed"].pipeline_hash()
        == pipeline.nodes["renamed"].pipeline_hash()
    )


# ── 4. a metadata-form declaration declares that logical type ────────────────


def _path_table() -> pa.Table:
    """Two tags and two ``Path`` data columns, non-null."""
    converter = _converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"k": 1, "g": 10, "x": Path("/a"), "w": Path("/b")},
            {"k": 2, "g": 20, "x": Path("/c"), "w": Path("/d")},
        ],
        python_schema={"k": int, "g": int, "x": Path, "w": Path},
    )
    return table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))


class _TableImpl:
    """A polling implementation that returns one table, declaring a given schema."""

    def __init__(self, table: pa.Table, schema: pa.Schema) -> None:
        """Hold the one table to fetch and the schema to declare."""
        self._table = table
        self._schema = schema
        self._done = False

    def identity(self) -> Any:
        """This implementation's identity, from the declared column names."""
        return ("forms", tuple(self._schema.names))

    def to_config(self) -> None:
        """Not saved: a test implementation."""
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "_TableImpl":
        """Never called: the implementation is not saved."""
        raise NotImplementedError

    def schema(self) -> pa.Schema:
        """The declared schema."""
        return self._schema

    async def poll(self, cursor: Any = None) -> bool:
        """New data until the one table is fetched."""
        return not self._done

    async def fetch(self, cursor: Any = None) -> Any:
        """The one table, once."""
        self._done = True
        return Cursor.now(1), self._table

    async def close(self) -> None:
        """Nothing to close."""
        return None


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize("form", ["live", "metadata"])
def test_a_polling_source_declared_in_metadata_form_takes_its_rows(form: str) -> None:
    """A metadata-form ``Path`` declares ``Path`` and takes its rows."""
    table = _path_table()
    declared = table.schema if form == "live" else _metadata_form(table.schema)
    source = PollingSource(
        _TableImpl(table, declared),
        tag_columns=["k", "g"],
        record_id_columns=["k", "g"],
        source_id="forms",
    )
    assert source.output_schema()[1]["x"] is Path
    assert source.as_table().num_rows == 2


@pytest.mark.parametrize("form", ["live", "metadata"])
def test_a_slot_declared_in_metadata_form_binds_its_logical_type(form: str) -> None:
    """A metadata-form slot declares ``Path``, binds the source and runs."""
    table = _path_table()
    schema = table.schema if form == "live" else _metadata_form(table.schema)
    tags = pa.schema([schema.field("k"), schema.field("g")])
    data = pa.schema([schema.field("x"), schema.field("w")])
    slot = SourceNode(name="src", tag_schema=tags, data_schema=data)
    assert dict(slot.output_schema()[1]) == {"x": Path, "w": Path}
    job = PipelineJob(name="forms", database=InMemoryArrowDatabase())
    with job:
        MapData({"x": "z"})(slot, label="renamed")
    job.bind(
        sources={
            "src": ArrowTableSource(
                table, tag_columns=["k", "g"], tag_only_columns=True, source_id="t"
            )
        }
    )
    job.run()
    assert job.nodes["renamed"].as_table().num_rows == 2


def test_a_slot_declared_in_metadata_form_refuses_another_logical_type() -> None:
    """A metadata-form ``Path`` slot refuses plain strings."""
    table = _path_table()
    schema = _metadata_form(table.schema)
    slot = SourceNode(
        name="src",
        tag_schema=pa.schema([schema.field("k"), schema.field("g")]),
        data_schema=pa.schema([schema.field("x"), schema.field("w")]),
    )
    job = PipelineJob(name="forms", database=InMemoryArrowDatabase())
    with job:
        MapData({"x": "z"})(slot, label="renamed")
    strings = table.cast(
        pa.schema(
            [
                f.with_type(pa.large_string()) if f.name in ("x", "w") else f
                for f in table.schema
            ]
        )
    )
    with pytest.raises(SourceSpecMismatchError):
        job.bind(
            sources={
                "src": ArrowTableSource(
                    strings, tag_columns=["k", "g"], tag_only_columns=True
                )
            }
        )


# ── 2. a read-only loaded operator types as_source() from its saved types ────


def _index(stream: Any) -> Any:
    """A read-only ``Index`` operator over ``stream``."""
    return Index("l8", 0)(stream.select_data_columns(["l8", "i8"]), label="ro")


def _pick(stream: Any) -> Any:
    """A read-only ``Pick`` operator over ``stream``."""
    return Pick("d", "q")(stream.select_data_columns(["d", "i8"]), label="ro")


ORCHESTRATORS: dict[str, Callable[[], Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("build", [_index, _pick], ids=["Index", "Pick"])
def test_a_loaded_read_only_operator_keeps_its_arrow_types(
    tmp_path: Path, build: Callable[[Any], Any], orchestrator: str
) -> None:
    """After a save and load, ``as_source()`` keeps the narrow Arrow types."""
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="read_only", database=database)
    with job:
        build(source_a())
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    path = tmp_path / "job.json"
    job.save(path)
    loaded = PipelineJob.load(path, database=database)

    original = job.nodes["ro"].as_source()
    reloaded = loaded.nodes["ro"].as_source()
    assert reloaded.pipeline_hash() == original.pipeline_hash()
    assert reloaded.content_hash() == original.content_hash()
    assert reloaded.arrow_schemas == original.arrow_schemas
    tag_schema, data_schema = reloaded.arrow_schemas
    assert tag_schema.field("k").type == pa.int16()
    assert tag_schema.field("ta").type == pa.int8()
    assert data_schema.field("i8").type == pa.int8()


# ── 5. an identifier nested in a plain list, struct or map (Codex review 454b) ─
#
# Arrow cannot build an empty table with an extension type inside a list, struct or
# map. Such an identifier stays in the storage form in a declaration, in a saved
# schema read back and in the hash: never a nested live type, never dropped.

NESTED_IDENTIFIERS: dict[str, Any] = {"Path": Path, "File": op.File}


def _identifier_metadata(python_type: Any) -> dict[bytes | str, bytes | str]:
    """The ``ARROW:extension:*`` keys of ``python_type``'s logical type."""
    extension = _converter().python_type_to_arrow_type(python_type)
    assert isinstance(extension, pa.ExtensionType)
    return {
        b"ARROW:extension:name": extension.extension_name.encode("utf-8"),
        b"ARROW:extension:metadata": extension.__arrow_ext_serialize__(),
    }


def _nested_containers(child: pa.Field[Any]) -> dict[str, tuple[pa.DataType, Any]]:
    """Plain Arrow containers holding ``child``, each with two rows of values."""
    key = pa.field("key", pa.large_string(), nullable=False)
    return {
        "struct": (
            pa.struct([child, pa.field("q", pa.int8())]),
            [{child.name: "/a", "q": 1}, {child.name: "/b", "q": 2}],
        ),
        "large_list": (pa.large_list(child), [["/a", "/b"], ["/c"]]),
        "fixed_size_list": (pa.list_(child, 2), [["/a", "/b"], ["/c", "/d"]]),
        # The item keeps its own name and nullability (non-null here) through a
        # save and load (Codex review 3 of PR 454).
        "map value": (
            pa.map_(key, child),  # pyright: ignore[reportArgumentType]  # pyarrow takes fields; the stubs name only types
            [[("u", "/a")], [("u", "/b"), ("v", "/c")]],
        ),
        "map value, keys sorted": (
            pa.map_(key, child, True),  # pyright: ignore[reportArgumentType, reportCallIssue]  # as above
            [[("u", "/a")], [("u", "/b"), ("v", "/c")]],
        ),
    }


def _nested(
    container: str, identifier: str, live: bool = False
) -> tuple[pa.Schema, pa.Schema, pa.Table]:
    """Tags ``k``, ``g``; data ``x`` and ``w`` holding the identifier nested."""
    python_type = NESTED_IDENTIFIERS[identifier]
    stored = pa.field("p", pa.large_string(), nullable=False).with_metadata(
        _identifier_metadata(python_type)
    )
    child = (
        pa.field(
            "p", _converter().python_type_to_arrow_type(python_type), nullable=False
        )
        if live
        else stored
    )
    arrow_type, values = _nested_containers(child)[container]
    stored_type, _ = _nested_containers(stored)[container]
    tags = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("g", pa.int64(), nullable=False),
        ]
    )
    data = pa.schema([pa.field("x", arrow_type), pa.field("w", arrow_type)])
    table = pa.table(
        {
            "k": pa.array([1, 2], pa.int64()),
            "g": pa.array([10, 20], pa.int64()),
            "x": pa.array(values, stored_type),
            "w": pa.array(list(reversed(values)), stored_type),
        }
    ).cast(
        pa.schema(
            [
                *tags,
                pa.field("x", stored_type),
                pa.field("w", stored_type),
            ]
        )
    )
    return tags, data, table


# A source cannot hold a map column (the Arrow hasher refuses map data), so the map
# value is tested where a schema is enough: declaration, saved text and hash.
SOURCE_CONTAINERS = ("struct", "large_list", "fixed_size_list")
NESTED_CASES = [
    (container, identifier)
    for container in SOURCE_CONTAINERS
    for identifier in NESTED_IDENTIFIERS
]


def _data_schema(schema: pa.Schema) -> pa.Schema:
    """The ``x`` and ``w`` fields of ``schema``."""
    return pa.schema([schema.field("x"), schema.field("w")])


@pytest.mark.parametrize("identifier", list(NESTED_IDENTIFIERS))
@pytest.mark.parametrize(
    "container", [*SOURCE_CONTAINERS, "map value", "map value, keys sorted"]
)
@pytest.mark.parametrize("live", [False, True], ids=["storage child", "live child"])
def test_an_arrow_declared_slot_with_a_nested_identifier_works(
    container: str, identifier: str, live: bool
) -> None:
    """It types, hashes and saves; the identifier stays, in the storage form."""
    tags, data, table = _nested(container, identifier, live=live)
    stored = _data_schema(table.schema)
    slot = SourceNode(name="src", tag_schema=tags, data_schema=data)
    _, slot_data = typed_arrow_schemas(slot)
    assert slot_data == stored
    assert slot_data.empty_table().num_rows == 0
    other = SourceNode(name="src", tag_schema=tags, data_schema=stored)
    assert slot.pipeline_hash() == other.pipeline_hash()
    assert _hasher().hash_schema(data) == _hasher().hash_schema(stored)
    texts = arrow_type_texts(data)
    assert all("extension<" in text for text in texts.values())
    assert arrow_schema_from_texts(texts, _converter(), ["x", "w"]) == stored


@pytest.mark.parametrize(("container", "identifier"), NESTED_CASES)
class TestNestedIdentifier:
    """An identifier nested in a plain struct or list: the source, its slots, a save and load."""

    def _source(self, table: pa.Table) -> ArrowTableSource:
        """An in-memory source of ``table`` with tags ``k``, ``g``."""
        return ArrowTableSource(
            table, tag_columns=["k", "g"], tag_only_columns=True, source_id="nested"
        )

    @pytest.mark.parametrize("live", [False, True], ids=["storage child", "live child"])
    def test_an_arrow_declared_slot_is_the_source_and_its_stream_slot(
        self, container: str, identifier: str, live: bool
    ) -> None:
        """Declared slot, stream slot and source: one Python schema, Arrow schema and hash."""
        tags, data, table = _nested(container, identifier, live=live)
        source = self._source(table)
        declared = SourceNode(name="src", tag_schema=tags, data_schema=data)
        from_stream = SourceNode.from_stream(source, name="src")
        # One Python schema, one Arrow schema, one pipeline hash (coordinator's
        # condition on the declaration form, PR 454).
        assert declared.pipeline_hash() == from_stream.pipeline_hash()
        assert declared.pipeline_hash() == source.pipeline_hash()
        assert declared.output_schema() == from_stream.output_schema()
        assert declared.output_schema() == source.output_schema()
        assert typed_arrow_schemas(declared) == typed_arrow_schemas(from_stream)
        assert typed_arrow_schemas(declared) == typed_arrow_schemas(source)
        for tags_or_data in typed_arrow_schemas(declared):
            assert tags_or_data.empty_table().num_rows == 0

    def test_the_identifier_is_part_of_the_hash(
        self, container: str, identifier: str
    ) -> None:
        """Without its nested identifier, the source is another identity."""
        tags, data, table = _nested(container, identifier)
        plain = table.cast(
            pa.schema(
                [
                    pa.field(f.name, _without_identifiers(f.type), f.nullable)
                    for f in table.schema
                ]
            )
        )
        assert (
            self._source(plain).pipeline_hash() != self._source(table).pipeline_hash()
        )

    @pytest.mark.parametrize("live", [False, True], ids=["storage child", "live child"])
    def test_the_saved_text_reads_back_typed_and_hashed(
        self, container: str, identifier: str, live: bool
    ) -> None:
        """The saved text reads back to the stored schema, typed and hashed."""
        _, data, table = _nested(container, identifier, live=live)
        texts = arrow_type_texts(data)
        assert all("extension<" in text for text in texts.values())
        assert texts == arrow_type_texts(_data_schema(table.schema))
        restored = arrow_schema_from_texts(texts, _converter(), ["x", "w"])
        assert restored.empty_table().num_rows == 0
        assert restored == _data_schema(table.schema)
        assert _hasher().hash_schema(restored) == _hasher().hash_schema(data)
        source_types = self._source(table).output_schema()[1]
        restored_types = _converter().arrow_schema_to_python_schema(restored)
        assert dict(restored_types) == dict(source_types)

    def test_a_saved_slot_loads_and_binds(
        self, container: str, identifier: str, tmp_path: Path
    ) -> None:
        """Both slot forms save and load to the source's identity, and the loaded slot runs."""
        tags, data, table = _nested(container, identifier)
        source = self._source(table)
        slots = {
            "declared": SourceNode(name="src", tag_schema=tags, data_schema=data),
            "from a stream": SourceNode.from_stream(source, name="src"),
        }
        for label, slot in slots.items():
            pipeline = Pipeline(name="nested")
            with pipeline:
                MapData({"x": "z"})(slot, label="renamed")
            path = tmp_path / f"{label}.json"
            pipeline.save(path)
            loaded = Pipeline.load(path)
            assert (
                loaded.nodes["renamed"].pipeline_hash()
                == pipeline.nodes["renamed"].pipeline_hash()
            ), label
            loaded_slot = loaded.nodes["src"]
            # After the load: the source's Python schema, Arrow schema and hash.
            assert loaded_slot.pipeline_hash() == source.pipeline_hash(), label
            assert loaded_slot.output_schema() == source.output_schema(), label
            assert typed_arrow_schemas(loaded_slot) == typed_arrow_schemas(source)
            for tags_or_data in typed_arrow_schemas(loaded_slot):
                assert tags_or_data.empty_table().num_rows == 0, label
            # The loaded slot itself binds the source and runs.
            job = PipelineJob(name="nested", database=InMemoryArrowDatabase())
            with job:
                MapData({"x": "z"})(loaded_slot, label="renamed")
            job.bind(sources={"src": self._source(table)})
            job.run()
            assert job.nodes["renamed"].as_table().num_rows == 2, label


def _without_identifiers(arrow_type: pa.DataType) -> pa.DataType:
    """``arrow_type`` with every nested field's metadata removed."""
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [
                f.with_type(_without_identifiers(f.type)).remove_metadata()
                for f in arrow_type
            ]
        )
    if pa.types.is_large_list(arrow_type):
        item = arrow_type.value_field
        return pa.large_list(
            item.with_type(_without_identifiers(item.type)).remove_metadata()
        )
    if pa.types.is_fixed_size_list(arrow_type):
        item = arrow_type.value_field
        return pa.list_(
            item.with_type(_without_identifiers(item.type)).remove_metadata(),
            arrow_type.list_size,
        )
    if pa.types.is_map(arrow_type):
        return pa.map_(
            arrow_type.key_field.remove_metadata(),  # pyright: ignore[reportArgumentType]  # pyarrow takes fields; the stubs name only types
            arrow_type.item_field.remove_metadata(),  # pyright: ignore[reportArgumentType]  # as above
        )
    return arrow_type


# ── 6. every container text the writer produces reads back exactly ─────────────


def _identified(name: str, python_type: Any) -> pa.Field[Any]:
    """A non-null storage-form field carrying ``python_type``'s identifier."""
    return pa.field(name, pa.large_string(), nullable=False).with_metadata(
        _identifier_metadata(python_type)
    )


_KEY = pa.field("key", pa.large_string(), nullable=False)
_NON_NULL_INT8 = pa.field("value", pa.int8(), nullable=False)

CONTAINER_TEXTS: dict[str, pa.DataType] = {
    "list, nullable item": pa.list_(pa.field("item", pa.int8())),
    "list, non-null item named e": pa.list_(pa.field("e", pa.int8(), nullable=False)),
    "large_list, non-null item": pa.large_list(
        pa.field("item", pa.int8(), nullable=False)
    ),
    "fixed_size_list, non-null item": pa.list_(
        pa.field("item", pa.int8(), nullable=False), 2
    ),
    "struct, nullable and non-null children": pa.struct(
        [pa.field("a", pa.int8()), pa.field("b", pa.int16(), nullable=False)]
    ),
    "map, nullable value": pa.map_(pa.large_string(), pa.int8()),
    "map, non-null value": pa.map_(_KEY, _NON_NULL_INT8),  # pyright: ignore[reportArgumentType]  # pyarrow takes fields; the stubs name only types
    "map, keys sorted": pa.map_(_KEY, _NON_NULL_INT8, True),  # pyright: ignore[reportArgumentType, reportCallIssue]  # as above
    "map, key and value renamed": pa.map_(
        pa.field("k", pa.large_string(), nullable=False),  # pyright: ignore[reportArgumentType]  # as above
        pa.field("v", pa.int8()),  # pyright: ignore[reportArgumentType]  # as above
    ),
    "map, non-null Path value": pa.map_(_KEY, _identified("p", Path)),  # pyright: ignore[reportArgumentType]  # as above
    "map, keys sorted, non-null File value": pa.map_(
        _KEY,  # pyright: ignore[reportArgumentType]  # as above
        _identified("f", op.File),  # pyright: ignore[reportArgumentType]  # as above
        True,  # pyright: ignore[reportCallIssue]  # as above
    ),
    "map inside a list": pa.large_list(
        pa.field("item", pa.map_(_KEY, _NON_NULL_INT8), nullable=False)  # pyright: ignore[reportArgumentType]  # as above
    ),
    "list inside a map value": pa.map_(
        _KEY,  # pyright: ignore[reportArgumentType]  # as above
        pa.field(  # pyright: ignore[reportArgumentType]  # as above
            "value",
            pa.large_list(pa.field("item", pa.int8(), nullable=False)),
            nullable=False,
        ),
    ),
    "struct with a File child in a map value": pa.map_(
        _KEY,  # pyright: ignore[reportArgumentType]  # as above
        pa.field(  # pyright: ignore[reportArgumentType]  # as above
            "value",
            pa.struct([_identified("f", op.File), pa.field("q", pa.int8())]),
            nullable=False,
        ),
    ),
}


# A nested field name holding a character of the type grammar, or starting or ending
# with whitespace, is written JSON-quoted (CodeRabbit on PR 454); others as they are.
QUOTED_NAMES: dict[str, tuple[pa.DataType, str]] = {
    "map key named left:right": (
        pa.map_(
            pa.field("left:right", pa.large_string(), nullable=False),  # pyright: ignore[reportArgumentType]  # pyarrow takes fields; the stubs name only types
            pa.field("v", pa.int8()),  # pyright: ignore[reportArgumentType]  # as above
        ),
        'map<"left:right": large_string not null, v: int8>',
    ),
    "struct child named a:b": (
        pa.struct([pa.field("a:b", pa.int8())]),
        'struct<"a:b": int8>',
    ),
    "struct children named 'a, b' and c": (
        pa.struct([pa.field("a, b", pa.int8()), pa.field("c", pa.int8())]),
        'struct<"a, b": int8, c: int8>',
    ),
    "struct child named a<b": (
        pa.struct([pa.field("a<b", pa.int8(), nullable=False)]),
        'struct<"a<b": int8 not null>',
    ),
    "large list item named x>y": (
        pa.large_list(pa.field("x>y", pa.int8())),
        'large_list<"x>y": int8>',
    ),
    "fixed-size list item named (i)": (
        pa.list_(pa.field("(i)", pa.int8()), 2),
        'fixed_size_list<"(i)": int8>[2]',
    ),
    "struct child named q with a trailing space": (
        pa.struct([pa.field("q ", pa.int8())]),
        'struct<"q ": int8>',
    ),
    "struct child named with a quote": (
        pa.struct([pa.field('say "hi"', pa.int8())]),
        'struct<"say \\"hi\\"": int8>',
    ),
    "nested: a list of a struct with a:b": (
        pa.large_list(pa.field("item", pa.struct([pa.field("a:b", pa.int8())]))),
        'large_list<item: struct<"a:b": int8>>',
    ),
    "ordinary names stay as they are": (
        pa.struct([pa.field("key", pa.int8()), pa.field("value", pa.int8())]),
        "struct<key: int8, value: int8>",
    ),
}


@pytest.mark.parametrize("label", list(QUOTED_NAMES))
def test_a_nested_field_name_reads_back_exactly(label: str) -> None:
    """Names, quoted or not, survive the saved text exactly."""
    arrow_type, text = QUOTED_NAMES[label]
    schema = pa.schema([pa.field("c", arrow_type), pa.field("d", arrow_type)])
    texts = arrow_type_texts(schema)
    assert texts == {"c": text, "d": text}
    restored = arrow_schema_from_texts(texts, _converter(), ["c", "d"])
    assert restored.equals(schema, check_metadata=True)
    for name in schema.names:
        assert _named(restored.field(name).type) == _named(schema.field(name).type)
    assert _hasher().hash_schema(restored) == _hasher().hash_schema(schema)


NAMES = {
    "newline": "a\nb",
    "tab": "a\tb",
    "quote": 'say "hi"',
    "backslash": "back\\slash",
    "unicode": "ünïcødé 日本",
    "empty": "",
    "leading space": " lead",
    "trailing space": "trail ",
    "inner space": "in ner",
    "grammar characters": "a:b,c<d>e[f](g)",
    "plain": "plain_x1",
    "plain with dot and dash": "a.b-c",
}
PLAIN = {"plain", "plain with dot and dash"}


def _holding(name: str) -> dict[str, pa.DataType]:
    """Every container kind, at several depths, with a nested field named ``name``."""
    field = pa.field(name, pa.int8(), nullable=False)
    named_key: Any = pa.field(name, pa.large_string(), nullable=False)
    item: Any = pa.field("v", pa.int8())
    key: Any = pa.field("k", pa.large_string(), nullable=False)
    value: Any = field
    return {
        "list": pa.list_(field),
        "large_list": pa.large_list(field),
        "fixed_size_list": pa.list_(field, 2),
        "struct": pa.struct([field, pa.field("z", pa.int8())]),
        # pyarrow takes key and item fields; its stubs name only types (hence Any).
        "map key": pa.map_(named_key, item),
        "map item": pa.map_(key, value),
        "three deep": pa.large_list(
            pa.field("item", pa.struct([pa.field("s", pa.list_(field, 2))]))
        ),
    }


@pytest.mark.parametrize("name", list(NAMES))
def test_every_nested_field_name_reads_back_exactly(name: str) -> None:
    """One rule (CodeRabbit on PR 454): a plain name is written as it is, any other
    JSON-quoted; every name reads back exactly, in every container and depth."""
    for kind, arrow_type in _holding(NAMES[name]).items():
        schema = pa.schema([pa.field("c", arrow_type), pa.field("d", arrow_type)])
        texts = arrow_type_texts(schema)
        if name in PLAIN:
            assert '"' not in texts["c"], kind
        else:
            assert json.dumps(NAMES[name]) in texts["c"], kind
        restored = arrow_schema_from_texts(texts, _converter(), ["c", "d"])
        assert restored.equals(schema, check_metadata=True), kind
        for column in schema.names:
            restored_type = restored.field(column).type
            assert _named(restored_type) == _named(schema.field(column).type), kind
        assert _hasher().hash_schema(restored) == _hasher().hash_schema(schema), kind


def test_an_earlier_files_unquoted_names_still_read() -> None:
    """Names an earlier file wrote unquoted (an inner space, the empty name)."""
    restored = arrow_schema_from_texts(
        {
            "c": "struct<in ner: int8, : int16 not null>",
            "d": "large_list<my item: int8>",
        },
        _converter(),
        ["c", "d"],
    )
    assert restored.field("c").type == pa.struct(
        [pa.field("in ner", pa.int8()), pa.field("", pa.int16(), nullable=False)]
    )
    assert restored.field("d").type == pa.large_list(pa.field("my item", pa.int8()))


@pytest.mark.parametrize("label", list(CONTAINER_TEXTS))
def test_a_container_text_reads_back_exactly(label: str) -> None:
    """Names, nullability, identifiers and ``keys_sorted`` all survive the text."""
    schema = pa.schema(
        [
            pa.field("c", CONTAINER_TEXTS[label], nullable=False),
            pa.field("d", CONTAINER_TEXTS[label]),
        ]
    )
    restored = arrow_schema_from_texts(arrow_type_texts(schema), _converter(), ["d"])
    for name in ("c", "d"):
        assert restored.field(name).equals(schema.field(name), check_metadata=True)
        assert _named(restored.field(name).type) == _named(schema.field(name).type)
    assert _hasher().hash_schema(restored) == _hasher().hash_schema(schema)


def test_a_bare_map_text_of_an_earlier_file_still_reads() -> None:
    """``map<K, V>`` (written before the fields were kept) reads as Arrow's default."""
    restored = arrow_schema_from_texts(
        {"c": "map<large_string, int8>", "d": "map<large_string, int8, keys_sorted>"},
        _converter(),
        [],
    )
    assert restored.field("c").type == pa.map_(pa.large_string(), pa.int8())
    assert restored.field("d").type == pa.map_(pa.large_string(), pa.int8(), True)


@pytest.mark.parametrize("identifier", list(NESTED_IDENTIFIERS))
@pytest.mark.parametrize("container", ["map value", "map value, keys sorted"])
def test_a_slot_with_two_map_columns_saves_and_loads_exactly(
    container: str, identifier: str, tmp_path: Path
) -> None:
    """The reviewer's end-to-end case: a non-null map value keeps its identity."""
    tags, data, _ = _nested(container, identifier)
    slot = SourceNode(name="src", tag_schema=tags, data_schema=data)
    pipeline = Pipeline(name="maps")
    with pipeline:
        MapData({"x": "z"})(slot, label="renamed")
    path = tmp_path / "pipeline.json"
    pipeline.save(path)
    loaded = Pipeline.load(path)
    loaded_slot = loaded.nodes["src"]
    original_types, loaded_types = (
        typed_arrow_schemas(slot),
        typed_arrow_schemas(loaded_slot),
    )
    for before, after in zip(original_types, loaded_types):
        assert after.equals(before, check_metadata=True)
        assert _hasher().hash_schema(after) == _hasher().hash_schema(before)
    item = loaded_types[1].field("x").type.item_field
    assert not item.nullable and item.name == "p"
    # A fresh slot over the loaded types: the same identity and system-tag names.
    fresh = SourceNode(
        name="src", tag_schema=loaded_types[0], data_schema=loaded_types[1]
    )
    assert fresh.pipeline_hash() == slot.pipeline_hash()
    fresh_table, slot_table = typed_zero_row_table(fresh), typed_zero_row_table(slot)
    assert fresh_table is not None and slot_table is not None
    assert fresh_table.column_names == slot_table.column_names
    assert (
        loaded.nodes["renamed"].pipeline_hash()
        == pipeline.nodes["renamed"].pipeline_hash()
    )


# ── 7. older saved text reads exactly as the earlier reader read it ─────────────
#
# Format 0.4.0 quotes nested field names that are not plain; files of an earlier
# format wrote every name unquoted, so a quote there is part of the name. A load
# rewrites such text first (``saved_type_texts_at_version``; fifth Codex review of PR
# 454), and the reader never guesses.

OLD_NAMES = {
    "a quoted word": '"x"',
    "an escape spelled out": '"\\u0000"',
    "an inner quote": 'a"b',
    "a lone quote": '"',
    "two quotes": '""',
    "a quoted phrase": '"a b"',
    "a trailing quote": 'x"',
    "quotes and spaces": ' "y" ',
    "single quotes": "'q'",
    "json-looking": '"{}"',
    "three quotes": '"""',
    "a quoted item": '"item"',
    "an escaped quote": '\\"',
    "plain": "plain",
}


def _old_texts(name: str) -> dict[str, tuple[str, pa.DataType]]:
    """Text an earlier writer wrote for a nested field ``name``, and what an earlier
    reader read from it: the name with its surrounding whitespace removed."""
    read = name.strip()
    return {
        "struct": (f"struct<{name}: int8>", pa.struct([pa.field(read, pa.int8())])),
        "two children": (
            f"struct<{name}: int8, z: int16 not null>",
            pa.struct([pa.field(read, pa.int8()), pa.field("z", pa.int16(), False)]),
        ),
        "large_list": (
            f"large_list<{name}: int8 not null>",
            pa.large_list(pa.field(read, pa.int8(), nullable=False)),
        ),
        "fixed_size_list": (
            f"fixed_size_list<{name}: int8>[2]",
            pa.list_(pa.field(read, pa.int8()), 2),
        ),
        "nested": (
            f"large_list<item: struct<{name}: list<{name}: int8>>>",
            pa.large_list(
                pa.struct([pa.field(read, pa.list_(pa.field(read, pa.int8())))])
            ),
        ),
    }


def _named(arrow_type: pa.DataType) -> tuple[pa.DataType, tuple[str, ...]]:
    """``arrow_type`` with every nested field name, in order: Arrow's type equality
    does not compare a list item's name."""
    names: list[str] = []

    def walk(t: pa.DataType) -> None:
        if pa.types.is_struct(t):
            for child in t:
                names.append(child.name)
                walk(child.type)
        elif (
            pa.types.is_list(t)
            or pa.types.is_large_list(t)
            or pa.types.is_fixed_size_list(t)
        ):
            names.append(t.value_field.name)
            walk(t.value_type)
        elif pa.types.is_map(t):
            names.extend([t.key_field.name, t.item_field.name])
            walk(t.key_type)
            walk(t.item_type)

    walk(arrow_type)
    return arrow_type, tuple(names)


@pytest.mark.parametrize("version", ["0.1.0", "0.3.0"])
@pytest.mark.parametrize("name", list(OLD_NAMES))
def test_older_type_text_reads_as_the_earlier_reader_read_it(
    name: str, version: str
) -> None:
    for kind, (text, expected) in _old_texts(OLD_NAMES[name]).items():
        rewritten = {"c": type_text_at_version(text, version)}
        restored = arrow_schema_from_texts(rewritten, _converter(), ["c"])
        assert _named(restored.field("c").type) == _named(expected), kind


def test_the_format_version_decides_how_a_text_is_read() -> None:
    """One text, two meanings: the file's version says which, never a guess."""
    names = {}
    for version in ("0.3.0", "0.4.0"):
        texts = {"c": type_text_at_version('struct<"x": int8>', version)}
        names[version] = arrow_schema_from_texts(texts, _converter(), ["c"]).field("c")
    assert names["0.3.0"].type.field(0).name == '"x"'
    assert names["0.4.0"].type.field(0).name == "x"


def test_only_the_known_places_of_an_older_file_are_rewritten() -> None:
    """Node output schemas, slot and source schemas, a CSV schema, nested sources along
    inner_source and origin; a PollingSource's impl_config, an operator's config and
    any other value pass through byte for byte (sixth Codex review of PR 454)."""
    old = 'struct<"x": int8>'
    new = 'struct<"\\"x\\"": int8>'
    opaque = {
        "data_schema": {"request": old},
        "tag_schema": {"r": old},
        "output_schema": {"tag": {"t": old}},
        "schema": {"columns": {"c": old}},
        "deeper": [{"data_schema": {"q": old}}],
    }
    polling = {
        "source_type": "polling_source",
        "impl_module": "user.module",
        "impl_class": "Impl",
        "impl_config": opaque,
    }
    csv = {
        "source_type": "csv",
        "tag_schema": {"t": old},
        "data_schema": {"d": old},
        "schema": {"columns": {"c": old}, "nullable": []},
        "description": old,
    }
    pipeline: dict[str, Any] = {
        "orcapod_pipeline_version": "0.3.0",
        "nodes": {
            "slot": {
                "output_schema": {
                    "tag": {"t": old},
                    "data": {"d": old},
                    "nullable": {},
                },
                "source_config": {
                    "source_type": "node",
                    "tag_schema": {"t": old},
                    "data_schema": {"d": old},
                },
            },
            "op": {
                "output_schema": {"tag": {"t": old}, "data": {"d": old}},
                "config": opaque,
            },
        },
    }
    job: dict[str, Any] = {
        "orcapod_pipeline_job_version": "0.3.0",
        "pipeline": pipeline,
        "bindings": {
            "sources": {
                "a": {
                    "source_type": "cached",
                    "inner_source": {"source_type": "derived", "origin": csv},
                },
                "b": polling,
                "c": {"source_type": "delta_table", "schema": {"columns": {"c": old}}},
            }
        },
    }
    before = json.dumps(job, sort_keys=True)
    rewritten = pipeline_type_texts_at_version(pipeline, "0.3.0")
    slot, op = rewritten["nodes"]["slot"], rewritten["nodes"]["op"]
    assert slot["output_schema"]["tag"] == {"t": new}
    assert slot["output_schema"]["data"] == {"d": new}
    assert slot["source_config"]["tag_schema"] == {"t": new}
    assert op["output_schema"]["tag"] == {"t": new}
    assert json.dumps(op["config"], sort_keys=True) == json.dumps(
        opaque, sort_keys=True
    )
    sources = job_type_texts_at_version(job, "0.3.0")["bindings"]["sources"]
    origin = sources["a"]["inner_source"]["origin"]
    assert origin["tag_schema"] == {"t": new}
    assert origin["data_schema"] == {"d": new}
    assert origin["schema"]["columns"] == {"c": new}
    assert origin["description"] == old
    assert json.dumps(sources["b"], sort_keys=True) == json.dumps(
        polling, sort_keys=True
    )
    assert sources["c"]["schema"]["columns"] == {"c": old}  # not a CSV source
    # The embedded pipeline is Pipeline.load's to rewrite, under its own version.
    assert job_type_texts_at_version(job, "0.3.0")["pipeline"] is pipeline
    assert json.dumps(job, sort_keys=True) == before  # nothing changed in place
    assert pipeline_type_texts_at_version(pipeline, "0.4.0") is pipeline
    assert job_type_texts_at_version(job, "0.4.0") is job


def test_an_older_pipeline_file_loads_its_quoted_names_exactly(tmp_path: Path) -> None:
    """End to end: a 0.3.0 file whose slot names hold quotes loads them as written."""
    tags = pa.schema([pa.field("k", pa.int64(), nullable=False)])
    data = pa.schema(
        [
            pa.field("s", pa.struct([pa.field('"x"', pa.int8())])),
            pa.field("w", pa.large_list(pa.field('a"b', pa.int8()))),
        ]
    )
    slot = SourceNode(name="src", tag_schema=tags, data_schema=data)
    pipeline = Pipeline(name="quotes")
    with pipeline:
        MapData({"s": "t"})(slot, label="renamed")
    path = tmp_path / "pipeline.json"
    pipeline.save(path)
    saved = json.loads(path.read_text())
    assert saved["orcapod_pipeline_version"] == "0.4.0"
    # The same file as an earlier writer wrote it: names unquoted.
    old_texts = {"s": 'struct<"x": int8>', "w": 'large_list<a"b: int8>'}
    for descriptor in saved["nodes"].values():
        config = descriptor.get("source_config")
        if config is not None:
            config["data_schema"].update(old_texts)
        if "s" in descriptor["output_schema"]["data"]:
            descriptor["output_schema"]["data"].update(old_texts)
    saved["orcapod_pipeline_version"] = "0.3.0"
    old_path = tmp_path / "old.json"
    old_path.write_text(json.dumps(saved))
    loaded = Pipeline.load(old_path)
    assert typed_arrow_schemas(loaded.nodes["src"]) == typed_arrow_schemas(slot)
    # And a format this library does not know is refused, never guessed.
    saved["orcapod_pipeline_version"] = "0.5.0"
    old_path.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match="Unsupported pipeline format version '0.5.0'"):
        Pipeline.load(old_path)
