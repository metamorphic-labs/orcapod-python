"""``cache_mode``: the read-only cache mode of an operator node (ITL-1218).

A plain ``OperatorNode`` caches nothing and answers ``CacheMode.OFF``; an
``OperatorJobNode`` answers the mode it was built or attached with. The mode changes only
through ``attach_databases`` (there is no setter), so a refused attach leaves it as it
was. The refusal over a restricted source is tested with the SpiralDB restriction tests
(``test_spiraldb_table_source_where.py``).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
from orcapod.core.operators import Join
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import PipelineJob
from orcapod.types import CacheMode

_MODES = list(CacheMode)


def _source(source_id: str, data: tuple[str, str]) -> op.sources.ArrowTableSource:
    """Two tags (``id``, ``k``) and two data columns, named by ``data``."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "k": pa.array(["a", "b", "c"], type=pa.large_string()),
            data[0]: pa.array([1.0, 2.0, 3.0], type=pa.float64()),
            data[1]: pa.array([10, 20, 30], type=pa.int64()),
        }
    )
    return op.sources.ArrowTableSource(
        table,
        tag_columns=["id", "k"],
        label=source_id,
        source_id=source_id,
        tag_only_columns=True,
    )


def _job(database: InMemoryArrowDatabase) -> PipelineJob:
    """A job joining two sources (op ``joined``), then renaming two columns."""
    job = PipelineJob(name="ops", database=database)
    with job:
        joined = Join()(
            _source("a", ("x", "y")), _source("b", ("z", "w")), label="joined"
        )
        _ = joined.map_data({"x": "x2", "w": "w2"}, label="rename")
    return job


@pytest.mark.parametrize("mode", _MODES, ids=lambda m: m.name)
def test_a_job_node_built_with_a_mode_answers_it(mode: CacheMode) -> None:
    left, right = _source("a", ("x", "y")), _source("b", ("z", "w"))
    node = OperatorJobNode(
        operator=Join(),
        input_streams=(left, right),
        pipeline_database=InMemoryArrowDatabase(),
        cache_mode=mode,
    )
    assert node.cache_mode is mode


def test_a_job_node_without_a_database_answers_off() -> None:
    node = OperatorJobNode(
        operator=Join(),
        input_streams=(_source("a", ("x", "y")), _source("b", ("z", "w"))),
    )
    assert node.cache_mode is CacheMode.OFF


def test_a_plain_operator_node_answers_off() -> None:
    job = _job(InMemoryArrowDatabase())
    for label in ("joined", "rename"):
        plain = job.nodes[label].as_node()
        assert isinstance(plain, OperatorNode)
        assert plain.cache_mode is CacheMode.OFF


@pytest.mark.parametrize("mode", _MODES, ids=lambda m: m.name)
def test_attach_databases_sets_the_mode_of_every_node(mode: CacheMode) -> None:
    database = InMemoryArrowDatabase()
    job = _job(database)
    for label in ("rename", "joined"):  # not in graph order
        assert job.nodes[label].cache_mode is CacheMode.OFF
        job.nodes[label].attach_databases(
            pipeline_database=database.at("ops"), cache_mode=mode
        )
    assert [job.nodes[label].cache_mode for label in ("joined", "rename")] == [
        mode,
        mode,
    ]


@pytest.mark.parametrize("first", _MODES, ids=lambda m: m.name)
@pytest.mark.parametrize("second", _MODES, ids=lambda m: m.name)
def test_a_later_attach_replaces_the_mode(first: CacheMode, second: CacheMode) -> None:
    database = InMemoryArrowDatabase()
    job = _job(database)
    node = job.nodes["joined"]
    node.attach_databases(pipeline_database=database.at("ops"), cache_mode=first)
    node.attach_databases(pipeline_database=database.at("ops"), cache_mode=second)
    assert node.cache_mode is second


class _UndeclaredDatabase:
    """A database adapter that does not say it keeps field metadata.

    It delegates every call to an in-memory database except that capability, so it
    cannot store the marker of a stream with data-tags: an attach that caches is refused.
    """

    def __init__(self, inner: InMemoryArrowDatabase) -> None:
        self._inner = inner

    def at(self, *path_components: str) -> _UndeclaredDatabase:
        return _UndeclaredDatabase(self._inner.at(*path_components))

    def __getattr__(self, name: str) -> object:
        if name == "keeps_field_metadata":
            raise AttributeError(name)
        return getattr(self._inner, name)


def _data_tag_source(
    source_id: str, data: tuple[str, str]
) -> op.sources.ArrowTableSource:
    """Two tags that are also data (the default), and two data columns."""
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "k": pa.array(["a", "b"], type=pa.large_string()),
            data[0]: pa.array([1.0, 2.0], type=pa.float64()),
            data[1]: pa.array([3, 4], type=pa.int64()),
        }
    )
    return op.sources.ArrowTableSource(
        table, tag_columns=["id", "k"], label=source_id, source_id=source_id
    )


@pytest.mark.parametrize("before", _MODES, ids=lambda m: f"from-{m.name}")
@pytest.mark.parametrize(
    "mode", [CacheMode.LOG, CacheMode.REPLAY], ids=lambda m: f"to-{m.name}"
)
def test_a_refused_attach_leaves_the_mode_unchanged(
    before: CacheMode, mode: CacheMode
) -> None:
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="tags", database=database)
    with job:
        _ = Join()(
            _data_tag_source("c", ("x", "y")),
            _data_tag_source("d", ("z", "w")),
            label="joined",
        )
    node = job.nodes["joined"]
    node.attach_databases(pipeline_database=database.at("tags"), cache_mode=before)

    with pytest.raises(InputValidationError, match="cannot store"):
        node.attach_databases(
            pipeline_database=_UndeclaredDatabase(InMemoryArrowDatabase()),  # pyright: ignore[reportArgumentType]  # duck-typed, as a third party's is
            cache_mode=mode,
        )

    assert node.cache_mode is before


def test_a_loaded_job_node_answers_the_mode_the_load_attached(tmp_path: Path) -> None:
    database = InMemoryArrowDatabase()
    job = _job(database)
    job.nodes["rename"].attach_databases(
        pipeline_database=database.at("ops"), cache_mode=CacheMode.LOG
    )
    job.run()
    job.save(tmp_path / "job.json")

    loaded = PipelineJob.load(tmp_path / "job.json", database=database)

    # A saved job records no cache mode: the load attaches its databases with OFF.
    for label in ("joined", "rename"):
        node = loaded.nodes[label]
        assert isinstance(node, OperatorJobNode)
        assert node.cache_mode is CacheMode.OFF
