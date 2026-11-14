"""No accepted bind changes a node's stored path or table schema (ITL-1112).

Developer, November 14, 2026: "for pipeline, all node's path/table schema should NOT change
regardless of what source you bind to the source nodes in the pipeline". One job is run over
every kind of accepted bind: a source of the declared schema, a narrower source into a
``T | None`` slot, sources of different kinds, through ``bind``, the ``bound_source`` setter
and ``PipelineJob.load``. After each, the stored table paths, the Arrow schema of every stored
table and of every node's ``as_table(all_info=True)`` (column names with system tags, types,
nullable flags, order) are those of the first bind: only values differ.

The bind check compares Python types, so two Arrow widths of one Python type (``int32`` and
``int64``) are not covered here (ITL-949).
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable
from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import MapData
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import ArrowTableSource, CSVSource, DictSource
from orcapod.types import Schema


@function_pod(output_keys="z")
def inc(y: int | None) -> int:
    """Return ``y + 1``, or 0 for a missing ``y``."""
    return 0 if y is None else y + 1


def _arrow(
    ids: list[int], xs: list[int | None], *, x_nullable: bool
) -> ArrowTableSource:
    """Return an Arrow source of tag ``id`` (non-null) and data ``x``."""
    schema = pa.schema(
        [pa.field("id", pa.int64(), False), pa.field("x", pa.int64(), x_nullable)]
    )
    return ArrowTableSource(
        pa.table({"id": ids, "x": xs}, schema=schema), tag_columns=["id"]
    )


def _csv(tmp_path: pathlib.Path, name: str, text: str) -> CSVSource:
    """Return a CSV source over *text*, written to *name* in *tmp_path*."""
    path = tmp_path / name
    _ = path.write_text(text)
    return CSVSource(str(path), tag_columns=["id"], source_id=name)


NODES = ("src", "renamed", "inc")
# A loaded job reads stored results only; its uncached operator has none to read.
LOADED_NODES = ("src", "inc")


def _snapshot(job: PipelineJob, nodes: tuple[str, ...] = NODES) -> dict[str, Any]:
    """Return the stored paths, stored table schemas and *nodes*' table schemas of *job*."""
    database = job.database
    assert database is not None
    paths = sorted(database.list_table_paths(()))
    stored = {}
    for path in paths:
        table = database.get_all_records(path)
        assert table is not None
        stored[path] = table.schema
    return {
        "paths": paths,
        "stored": stored,
        "nodes": {
            name: job.nodes[name].as_table(all_info=True).schema for name in nodes
        },
    }


def _assign(job: PipelineJob, source: Any) -> None:
    """Bind *source* to slot ``src`` by assigning the node's ``bound_source``."""
    job.nodes["src"].bound_source = source  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode


def _bind(job: PipelineJob, source: Any) -> None:
    """Bind *source* to slot ``src`` with ``PipelineJob.bind``."""
    job.bind(sources={"src": source})


def test_no_accepted_bind_changes_a_path_or_a_table_schema(
    tmp_path: pathlib.Path,
) -> None:
    """Every accepted bind leaves paths and every stored and node table schema as they were."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"x": int | None}),
    )
    job = PipelineJob(name="j", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = inc.pod(MapData({"x": "y"})(slot, label="renamed"), label="inc")

    binds: list[tuple[str, Callable[[PipelineJob, Any], None], Any]] = [
        ("declared schema", _bind, _arrow([0, 1], [1, None], x_nullable=True)),
        ("narrower arrow", _bind, _arrow([2, 3], [2, 3], x_nullable=False)),
        (
            "narrower dict",
            _assign,
            DictSource([{"id": 4, "x": 4}], tag_columns=["id"], source_id="d"),
        ),
        (
            "dict with a null",
            _assign,
            DictSource([{"id": 5, "x": None}, {"id": 6, "x": 6}], tag_columns=["id"]),
        ),
        ("csv with an empty cell", _bind, _csv(tmp_path, "a.csv", "id,x\n7,\n8,8\n")),
        ("narrower csv", _bind, _csv(tmp_path, "b.csv", "id,x\n9,9\n")),
    ]
    first: dict[str, Any] | None = None
    for label, bind, source in binds:
        bind(job, source)
        assert job.run().ok, label
        snapshot = _snapshot(job)
        if first is None:
            first = snapshot
        assert snapshot == first, label

    # The last bound source, the narrower CSV, is rebuilt from its file on load.
    job.save(str(tmp_path / "job.json"))
    loaded = PipelineJob.load(str(tmp_path / "job.json"))
    before = _snapshot(loaded, LOADED_NODES)
    assert before["paths"] == first["paths"]
    assert before["stored"] == first["stored"]
    assert before["nodes"]["src"] == first["nodes"]["src"]
    _bind(loaded, _arrow([0, 1], [1, None], x_nullable=True))
    # A loaded function node orders its meta columns unlike a fresh one, bind or not
    # (reported separately), so the loaded job is compared with itself.
    assert _snapshot(loaded, LOADED_NODES) == before
