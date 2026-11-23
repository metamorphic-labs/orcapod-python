"""ITL-1347 stage 2: a node's output stream equals the stream built from its rows.

Every job node's ``execute`` now returns its output stream, which the sync orchestrator
hands to every successor. Sources build it from their Arrow table and operators from
their result's Arrow table, with no rows made (the row-built form F,
``StaticOutputOperatorPod._row_built_form``). The governing rule of the spec
(``docs/dev/specs/itl-1347-job-table-transport.md``) is that nothing a run shows may
change. So each output stream must equal, in identity and byte for byte, the stream the
orchestrator built before stage 2 from the same node's rows
(``_materialize_upstream(rows, node)``):

- L2 (sources): rows are the source node's ``iter_data()``;
- L1 (operators): rows are the operator's result's ``iter_data()``, or the stored
  stream's under ``REPLAY``;
- L0 (function and side-effect nodes): rows are the output stream's own rows;
- L5 (held nodes): a held node's stream equals the one built from its rows;
- L7: every node's declared ``arrow_schema()`` equals the schema of its output.

The job uses every operator kind over several tag orders, table layouts and tag roles.
These tests are about the hand-off between nodes, so they wrap the nodes' ``execute``.
"""

from __future__ import annotations

import subprocess
import sys
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod, tap_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.nodes.source_node import SourceNodeBase
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod, at_edge
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import ProvisionalHashWarning
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.operators import Batch, MergeJoin, Rejoin, SelectDataColumns
from orcapod.side_effects import SideEffectJobNode
from orcapod.system_constants import constants
from orcapod.types import CacheMode
from tests.test_core.operators.several_items import (
    MAIN,
    P,
    Q,
    Arrangement,
    arrangements,
    make_source,
)

ALL_COLUMNS = {
    "meta": True,
    "source": True,
    "system_tags": True,
    "context": True,
    "content_hash": True,
}


@function_pod(output_keys="d1_doubled")
def doubled(d1: int) -> int:
    return 2 * d1


DELIVERED: list[int] = []


@tap_pod
def noted(d1_doubled: int, ctx: Any) -> None:
    DELIVERED.append(d1_doubled)


def build_job(arrangement: Arrangement, database: Any = None) -> Any:
    """A job in which every operator kind, a function node and a side effect run."""
    main = make_source(MAIN, arrangement)
    partner_p = make_source(P)
    partner_q = make_source(Q)
    job = PipelineJob(
        name="stream_transport", database=database or InMemoryArrowDatabase()
    )
    with job:
        joined = main.join(partner_p, label="joined")
        mapped = joined.map_data({"p1": "p1_renamed"}, label="mapped")
        no_nulls = mapped.drop_nulls(["d2"], label="no_nulls")
        no_nulls.select_data_columns(["d1", "p1_renamed"], label="after_no_nulls")
        main.group_by(["t1"], label="grouped").select_data_columns(
            ["d1"], label="after_grouped"
        )
        main.select_data_columns(["d1"]).batch(batch_size=2, label="batched")
        tagged = main.add_tags(k=1, label="tagged")
        twice = doubled.pod(tagged.select_data_columns(["d1"]), label="doubled")
        noted_stream: Any = noted
        noted_stream(twice, label="noted")
        main.semi_join(partner_q, label="semi").select_data_columns(
            ["d3"], label="after_semi"
        )
        MergeJoin()(
            main.select_data_columns(["d1"]),
            partner_p.map_data({"p1": "d1"}).select_data_columns(["d1"]),
        ).select_data_columns(["d1"], label="after_merged")
        Rejoin()(
            main.select_data_columns(["d1"]), main.select_data_columns(["d3"])
        ).select_data_columns(["d1"], label="after_rejoined")
        main.polars_filter(d1=999, label="nothing").select_data_columns(
            ["d1"], label="after_nothing"
        )
    return job


def _identity(stream: Any) -> tuple[Any, ...]:
    return (
        type(stream),
        stream.producer,
        tuple(id(u) for u in stream.upstreams),
        stream.pipeline_hash(),
        stream.content_hash(),
        stream.keys(),
        stream.output_schema(),
        stream.data_context_key,
    )


def assert_same_stream(got: Any, expected: Any) -> None:
    """Equal identity, and equal tables byte for byte (every column group).

    ``got`` is taken as a successor receives it (``at_edge``: an output held as its rows
    is built there).
    """
    got = at_edge(got)
    expected = at_edge(expected)
    assert _identity(got) == _identity(expected)
    got_table = got.as_table(columns=ALL_COLUMNS).combine_chunks()
    expected_table = expected.as_table(columns=ALL_COLUMNS).combine_chunks()
    assert got_table.schema.equals(expected_table.schema, check_metadata=True)
    assert got_table.equals(expected_table, check_metadata=True)


def _rebuilt(rows: list[Any], node: Any) -> Any:
    return StaticOutputOperatorPod._materialize_upstream(rows, node)  # pyright: ignore[reportPrivateUsage]  # the path before stage 2


@pytest.fixture
def outputs(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    """Wrap every node kind's ``execute``; check each output against the row path."""
    seen: dict[str, Any] = {}

    source_execute = SourceNodeBase.execute

    def source(self: Any, **kwargs: Any) -> Any:
        out = source_execute(self, **kwargs)
        assert_same_stream(out, _rebuilt(list(self.iter_data()), self))  # L2
        seen[self.label] = out
        return out

    operator_execute = OperatorJobNode.execute

    def operator(self: Any, *inputs: Any, **kwargs: Any) -> Any:
        cached = self.get_cached_output()
        if cached is not None:
            rows = list(cached.iter_data())
        else:
            rows = list(self._operator.process(*inputs).iter_data())  # pyright: ignore[reportPrivateUsage]  # the operator's own result, as before stage 2
        out = operator_execute(self, *inputs, **kwargs)
        assert_same_stream(out, _rebuilt(rows, self))  # L1
        seen[self.label] = out
        return out

    def by_construction(original: Any) -> Any:
        def execute(self: Any, *args: Any, **kwargs: Any) -> Any:
            out = original(self, *args, **kwargs)
            assert_same_stream(out, _rebuilt(list(out.iter_data()), self))  # L0
            seen[self.label] = out
            return out

        return execute

    monkeypatch.setattr(SourceNodeBase, "execute", source)
    monkeypatch.setattr(OperatorJobNode, "execute", operator)
    monkeypatch.setattr(
        FunctionJobNode, "execute", by_construction(FunctionJobNode.execute)
    )
    monkeypatch.setattr(
        SideEffectJobNode, "execute", by_construction(SideEffectJobNode.execute)
    )
    yield seen


def _run(job: Any) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        job.run()


@pytest.mark.parametrize("arrangement", arrangements(MAIN), ids=lambda a: a.id)
def test_every_output_stream_equals_the_row_rebuild(
    arrangement: Arrangement, outputs: dict[str, Any]
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        job = build_job(arrangement)
    _run(job)
    assert {"joined", "nothing", "doubled", "noted", "semi", "batched"} <= set(outputs)


def test_a_replayed_operator_output_equals_the_row_rebuild(
    outputs: dict[str, Any],
) -> None:
    """REPLAY hands on the stored rows' stream, equal to the one built from them."""
    store = InMemoryArrowDatabase()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        job = build_job(arrangements(MAIN)[0], store)
    node = job.nodes["joined"]
    node.attach_databases(
        pipeline_database=store.at("replay"), cache_mode=CacheMode.LOG
    )
    _run(job)
    node.attach_databases(
        pipeline_database=store.at("replay"), cache_mode=CacheMode.REPLAY
    )
    node.clear_cache()
    outputs.clear()
    _run(job)
    assert "joined" in outputs


def test_a_held_node_hands_on_the_stream_built_from_its_rows(
    outputs: dict[str, Any],
) -> None:
    """L5: a second run holds every node but one; held streams feed it as before."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        job = build_job(arrangements(MAIN)[0])
    DELIVERED.clear()
    _run(job)
    first = sorted(DELIVERED)
    assert first
    job.nodes["noted"].clear_cache()
    outputs.clear()
    DELIVERED.clear()
    _run(job)
    # Only the cleared side effect ran; its input came from the held function node,
    # and it delivered the same values again (CodeRabbit on #510).
    assert set(outputs) == {"noted"}
    assert sorted(DELIVERED) == first


def _without_exceptions(schema: pa.Schema) -> list[tuple[str, Any, bool, Any]]:
    """Fields in order as (name, type, nullable, metadata), the spec's exceptions applied.

    Decided by the values of a complete table, not by the description: a system-tag or
    source-info field's flag (stated nullable) and a source-info field's type (left out).
    A ``null`` column is ``large_string`` in rows (ITL-1319). Column order counts on the
    sync path (Codex on PR #485).
    """
    prefixes = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
    return [
        (
            field.name,
            None
            if field.name.startswith(constants.SOURCE_PREFIX)
            else pa.large_string()
            if pa.types.is_null(field.type)
            else field.type,
            True if field.name.startswith(prefixes) else field.nullable,
            field.metadata,
        )
        for field in schema
    ]


@pytest.mark.parametrize(
    "arrangement", arrangements(MAIN)[::4], ids=lambda arrangement: arrangement.id
)
def test_declared_arrow_schema_equals_the_output(
    arrangement: Arrangement, outputs: dict[str, Any]
) -> None:
    """L7: ``arrow_schema()`` of every node equals the schema of its output stream."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        job = build_job(arrangement)
    _run(job)
    for label, stream in outputs.items():
        node = job.nodes[label]
        output = stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
        assert _without_exceptions(node.arrow_schema()) == _without_exceptions(
            output.schema
        ), label
        assert node.arrow_types_declared, label


def test_a_loaded_job_describes_its_nodes(
    tmp_path: Path, outputs: dict[str, Any]
) -> None:
    """L7 for a loaded job: each loaded node's ``arrow_schema()`` equals its output.

    A loaded node describes itself from the Arrow type texts its saved file holds
    (ITL-640). The job is run and saved in another process, so that the loaded job
    holds no run of this process.
    """
    script = (
        "import sys, warnings\n"
        "from pathlib import Path\n"
        "from orcapod.databases import DeltaTableDatabase\n"
        "from tests.test_core.operators.several_items import MAIN, arrangements\n"
        "from tests.test_pipeline.test_stream_transport_equivalence import build_job\n"
        "warnings.simplefilter('ignore')\n"
        "root = Path(sys.argv[1])\n"
        "job = build_job(arrangements(MAIN)[0], DeltaTableDatabase(root / 'store'))\n"
        "job.run()\n"
        "job.save(root / 'job.json')\n"
    )
    repository = Path(__file__).resolve().parents[2]
    subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)], check=True, cwd=repository
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        loaded = PipelineJob.load(tmp_path / "job.json")
        # In-memory sources are not saved: bind them again.
        loaded.bind(
            sources={
                "s": make_source(MAIN, arrangements(MAIN)[0]),
                "p": make_source(P),
                "q": make_source(Q),
            }
        )
        loaded.run(load_only=True)
    assert {"joined", "doubled", "noted", "semi", "batched"} <= set(outputs)
    for label, stream in outputs.items():
        node = loaded.nodes[label]
        output = stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
        assert _without_exceptions(node.arrow_schema()) == _without_exceptions(
            output.schema
        ), label
        assert node.arrow_types_declared, label


def _poller(fetched_order: tuple[str, ...]) -> Any:
    """A ``PollingSource`` declaring tags ``k, j`` and data ``b, a``.

    It fetches a table with its columns in ``fetched_order``: a valid source whose
    declared order is not its rows' (Codex on PR #485).
    """
    from orcapod.core.sources import PollingSource
    from tests.test_hashing.test_logical_type_forms import (
        _TableImpl,  # pyright: ignore[reportPrivateUsage]  # the one test polling implementation
    )

    columns = {
        "k": pa.array(["1", "2"], pa.large_string()),
        "j": pa.array(["x", "y"], pa.large_string()),
        "a": pa.array([1, 2], pa.int64()),
        "b": pa.array([3, 4], pa.int64()),
    }
    declared = pa.schema(
        [pa.field(name, columns[name].type, False) for name in ("k", "j", "b", "a")]
    )
    fetched = pa.table({name: columns[name] for name in fetched_order})
    fetched = fetched.cast(pa.schema([f.with_nullable(False) for f in fetched.schema]))
    return PollingSource(
        _TableImpl(fetched, declared),
        tag_columns=["k", "j"],
        record_id_columns=["k", "j"],
        source_id="poller",
    )


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize(
    "fetched_order",
    [("j", "k", "a", "b"), ("k", "j", "b", "a"), ("k", "b", "j", "a")],
    ids="-".join,
)
def test_a_poller_fetching_in_another_order_hands_on_its_rows(
    fetched_order: tuple[str, ...], outputs: dict[str, Any]
) -> None:
    """L2 for a poller whose fetched column order differs from its declared order.

    The source node's output equals its rows rebuilt (checked by ``outputs``), and the
    operator after it keeps and stores its columns in the rows' order, as on ``main``.
    """
    # Built before the job captures: importing the polling implementation's module
    # inside ``with job`` would capture the sources that module builds.
    poller = _poller(fetched_order)
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="poller_order", database=database)
    with job:
        SelectDataColumns(["a", "b"])(poller, label="selected")
    job.nodes["selected"].attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    _run(job)
    node = job.nodes["selected"]
    rows_order = [
        *job.nodes["selected"].keys()[0],
        *(name for name in fetched_order if name in ("a", "b")),
    ]
    source_order = [f"{constants.SOURCE_PREFIX}{n}" for n in rows_order[2:]]
    kept = node.as_table(columns={"source": True}).column_names
    assert kept == [*rows_order, *source_order]
    stored = node.get_all_records(columns={"source": True})
    assert stored is not None
    assert [name for name in stored.column_names if not name.startswith("__")] == [
        *rows_order,
        *source_order,
    ]
    assert "poller" in outputs


@function_pod(output_keys="z")
def plus_one(d1_doubled: int) -> int:
    return d1_doubled + 1


class _CountsDataStarts(NoOpObserver):
    """Counts ``label``'s data starts; reads only (an observer that changes the job
    during a run is unsupported, ITL-1369)."""

    def __init__(self, label: str) -> None:
        self._label = label
        self.data_starts = 0

    def contextualize(self, *identity_path: str) -> _CountsDataStarts:
        return self

    def on_data_start(self, node_label: str, tag: Any, data: Any) -> None:
        if node_label == self._label:
            self.data_starts += 1


def test_an_async_held_node_sends_the_rows_the_plan_took() -> None:
    """The async run sends a held node's rows as planned: the node after it, cleared
    before the run, receives every row of the held node (Codex on PR #485; the
    observer only reads, CodeRabbit on #510)."""
    from orcapod.sources import DictSource

    job = PipelineJob(name="held_snapshot", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(
            [{"k": i, "d1": i} for i in range(3)],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        plus_one.pod(doubled.pod(source, label="twice"), label="plus")
    job.run()
    job.nodes["plus"].clear_cache()
    observer = _CountsDataStarts("plus")
    summary = job.run(orchestrator=AsyncPipelineOrchestrator(), observer=observer)
    assert summary.nodes["twice"].held == 3
    assert observer.data_starts == 3


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
def test_describing_a_poller_that_has_not_polled_reads_nothing() -> None:
    """L7: ``arrow_schema()`` of a poller that has not polled never fetches.

    It answers from the declared schema, the same every time, with each data column's
    source info, and ``arrow_types_declared`` stays false (Codex on PR #485).
    """
    from orcapod.core.sources import PollingSource
    from orcapod.types import Schema
    from tests.test_hashing.test_logical_type_forms import (
        _TableImpl,  # pyright: ignore[reportPrivateUsage]  # the one test polling implementation
    )

    fetches: list[int] = []

    class Counting(_TableImpl):
        async def fetch(self, cursor: Any = None) -> Any:
            fetches.append(1)
            return await super().fetch(cursor)

        def schema(self) -> Any:
            return Schema({"k": str, "a": int, "b": int})

    table = pa.table(
        {
            "k": pa.array(["1"], pa.large_string()),
            "a": pa.array([1]),
            "b": pa.array([2]),
        }
    )
    poller = PollingSource(
        Counting(table, table.schema),
        tag_columns=["k"],
        record_id_columns=["k"],
        source_id="unpolled",
    )
    first = poller.arrow_schema()
    second = poller.arrow_schema()
    assert fetches == []
    assert first.equals(second, check_metadata=True)
    assert [n for n in first.names if n.startswith(constants.SOURCE_PREFIX)] == [
        "_source_a",
        "_source_b",
    ]
    assert not poller.arrow_types_declared


def test_an_operator_stream_declares_its_tags_in_its_rows_order() -> None:
    """A plain operator stream's ``arrow_schema()`` lists its tags as its rows do.

    Tags declared ``b, a`` over a table laid out ``a, b`` (Codex on PR #485).
    """
    from orcapod.core.sources import ArrowTableSource

    table = pa.table(
        {
            "a": pa.array([1, 2]),
            "b": pa.array([3, 4]),
            "w": pa.array([5, 6]),
            "x": pa.array([7, 8]),
        }
    )
    source = ArrowTableSource(
        table, tag_columns=["b", "a"], tag_only_columns=True, source_id="s"
    )
    stream = SelectDataColumns(["x"])(source)
    built, _, _ = StaticOutputOperatorPod._rows_to_table(list(stream.iter_data()))  # pyright: ignore[reportPrivateUsage]  # the rows' order
    assert stream.arrow_schema().names[:2] == built.column_names[:2]


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
def test_a_poller_that_fetched_describes_its_rows_order() -> None:
    """L7: after a fetch in another order than declared, the description follows the rows.

    A poller declaring ``k, j, b, a`` that fetched ``j, k, a, b`` lists its data as its
    rows do (Codex on PR #485).
    """
    poller = _poller(("j", "k", "a", "b"))
    rows = list(poller.iter_data())
    built, _, _ = StaticOutputOperatorPod._rows_to_table(rows)  # pyright: ignore[reportPrivateUsage]  # the rows' order
    assert poller.arrow_types_declared
    assert _without_exceptions(poller.arrow_schema()) == _without_exceptions(
        built.schema
    )


def test_a_value_decided_source_info_depth_is_a_named_exception() -> None:
    """L7: ``Batch`` over null list tokens; the description and the rows agree but for it.

    The rows give ``[None, None]`` one list level (``large_list<large_string>``); the
    description states the declared token type. A source-info type is decided by the
    values of a complete table and is one of the description's named exceptions.
    """
    from orcapod.core.streams import ArrowTableStream
    from orcapod.sources import DictSource

    table = DictSource(
        [{"k": 1, "x": 1}, {"k": 2, "x": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
    ).as_table(columns={"source": True, "system_tags": True})
    position = table.schema.get_field_index("_source_x")
    strings = pa.large_list(pa.large_string())
    raw = ArrowTableStream(
        table.set_column(
            position,
            pa.field("_source_x", strings, nullable=True),
            pa.array([None, None], strings),
        ),
        tag_columns=["k"],
    )
    batched = Batch(batch_size=2)(raw)
    built, _, _ = StaticOutputOperatorPod._rows_to_table(list(batched.iter_data()))  # pyright: ignore[reportPrivateUsage]  # the rows
    declared = batched.arrow_schema()
    assert declared.field("_source_x").type != built.schema.field("_source_x").type
    assert _without_exceptions(declared) == _without_exceptions(built.schema)


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
def test_describing_an_undeclared_poller_that_has_not_polled_reads_nothing() -> None:
    """L7: a poller with no declared schema is described with no column, and not polled.

    Its columns are known only from rows; the description never fetches, its modified
    time does not move and ``arrow_types_declared`` stays false (Codex on PR #485).
    """
    from orcapod.core.sources import PollingSource
    from tests.test_hashing.test_logical_type_forms import (
        _TableImpl,  # pyright: ignore[reportPrivateUsage]  # the one test polling implementation
    )

    fetches: list[int] = []

    class Undeclared(_TableImpl):
        async def fetch(self, cursor: Any = None) -> Any:
            fetches.append(1)
            return await super().fetch(cursor)

        def schema(self) -> Any:
            return None

    table = pa.table(
        {"k": pa.array(["1"], pa.large_string()), "a": pa.array([1], pa.int64())}
    )
    poller = PollingSource(
        Undeclared(table, table.schema),
        tag_columns=["k"],
        record_id_columns=["k"],
        source_id="undeclared",
    )
    modified = poller.last_modified
    assert poller.arrow_schema() == pa.schema([])
    assert poller.arrow_schema() == pa.schema([])
    assert fetches == []
    assert poller.last_modified == modified
    assert not poller.arrow_types_declared
