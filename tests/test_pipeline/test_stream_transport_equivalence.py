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

import warnings
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod, tap_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.nodes.source_node import SourceNodeBase
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import ProvisionalHashWarning
from orcapod.operators import MergeJoin, Rejoin
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


@tap_pod
def noted(d1_doubled: int, ctx: Any) -> None:
    return None


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
    """Equal identity, and equal tables byte for byte (every column group)."""
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
    _run(job)
    job.nodes["noted"].clear_cache()
    outputs.clear()
    _run(job)
    # Only the cleared side effect ran; its input came from the held function node.
    assert set(outputs) == {"noted"}


def _without_exceptions(schema: pa.Schema) -> dict[str, tuple[Any, bool, Any]]:
    """Fields by name as (type, nullable, metadata), the spec's two exceptions applied.

    A source-info field's flag is decided by a table's tokens (stated nullable), and a
    ``null`` column is ``large_string`` in rows (ITL-1319). Columns are matched by name:
    the declared (zero-row) form can list them in another order than the rows do (a
    data-tag's source info, an operator's tags under another layout), as on ``main``,
    and column order is not a result (developer, 2026-11-22).
    """
    fields: dict[str, tuple[Any, bool, Any]] = {}
    for field in schema:
        kind = pa.large_string() if pa.types.is_null(field.type) else field.type
        nullable = (
            True if field.name.startswith(constants.SOURCE_PREFIX) else field.nullable
        )
        fields[field.name] = (kind, nullable, field.metadata)
    return fields


@pytest.mark.parametrize("arrangement", arrangements(MAIN)[::4], ids=lambda a: a.id)
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
