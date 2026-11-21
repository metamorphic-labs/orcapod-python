"""A logical type is one thing in every Arrow representation (ITL-640, Codex review of PR 454).

A logical type's identifier is its extension name plus its extension metadata, however a
field carries it: as a live ``pa.ExtensionType``, or as its storage type with the
``ARROW:extension:name`` / ``ARROW:extension:metadata`` field metadata (the IPC form of
a table read back from a store; also what a process sees when pyarrow has not registered
the extension type). Every place an Arrow schema enters a schema identity, a saved
schema or a declaration brings it to one form first
(``logical_types.canonical_form.canonical_logical_schema``), so:

1. ``hash_schema`` gives one hash for the live and the metadata form;
2. a read-only loaded operator (``Index``, ``Pick``) types ``node.as_source()`` from its
   saved Arrow types;
3. a saved schema keeps an identifier carried in either form, at any depth;
4. a schema declared in Arrow with a metadata-form field declares that logical type.
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

from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    Pipeline,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.serialization import arrow_schema_from_texts, arrow_type_texts
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
    return get_default_context().type_converter


def _hasher() -> Any:
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


@pytest.mark.parametrize("name", list(LOGICAL))
class TestOneSchemaHash:
    def test_live_and_metadata_form_give_one_hash(self, name: str) -> None:
        live = _schema(LOGICAL[name])
        metadata = _metadata_form(live)
        assert _hasher().hash_schema(live) == _hasher().hash_schema(metadata)

    def test_a_schema_read_without_registration_gives_the_same_hash(
        self, name: str
    ) -> None:
        live = _schema(LOGICAL[name])
        assert _hasher().hash_schema(live) == _hasher().hash_schema(
            _unregistered_form(live)
        )

    def test_slots_in_either_form_are_one_identity(self, name: str) -> None:
        live = _schema(LOGICAL[name])
        tags = pa.schema([pa.field("k", pa.int64(), nullable=False)])
        a = SourceNode(name="src", tag_schema=tags, data_schema=live)
        b = SourceNode(name="src", tag_schema=tags, data_schema=_metadata_form(live))
        assert a.pipeline_hash() == b.pipeline_hash()
        assert a.content_hash() == b.content_hash()


def test_two_logical_types_still_differ() -> None:
    """The canonical form never merges two identifiers."""
    file_schema = _metadata_form(_schema(op.File))
    path_schema = _metadata_form(_schema(Path))
    assert _hasher().hash_schema(file_schema) != _hasher().hash_schema(path_schema)


# ── 3. the saved schema keeps the identifier in either form ──────────────────


@pytest.mark.parametrize("name", list(LOGICAL))
def test_the_writer_keeps_an_identifier_in_metadata_form(name: str) -> None:
    live = _schema(LOGICAL[name])
    for given in (live, _metadata_form(live), _unregistered_form(live)):
        texts = arrow_type_texts(given)
        assert texts == arrow_type_texts(live)
        restored = arrow_schema_from_texts(
            texts, _converter(), [f.name for f in given if f.nullable]
        )
        assert _hasher().hash_schema(restored) == _hasher().hash_schema(live)


def test_the_writer_keeps_a_nested_identifier_in_metadata_form() -> None:
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
        self._table = table
        self._schema = schema
        self._done = False

    def identity(self) -> Any:
        return ("forms", tuple(self._schema.names))

    def to_config(self) -> None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "_TableImpl":
        raise NotImplementedError

    def schema(self) -> pa.Schema:
        return self._schema

    async def poll(self, cursor: Any = None) -> bool:
        return not self._done

    async def fetch(self, cursor: Any = None) -> Any:
        self._done = True
        return Cursor.now(1), self._table

    async def close(self) -> None:
        return None


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize("form", ["live", "metadata"])
def test_a_polling_source_declared_in_metadata_form_takes_its_rows(form: str) -> None:
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
    return Index("l8", 0)(stream.select_data_columns(["l8", "i8"]), label="ro")


def _pick(stream: Any) -> Any:
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
