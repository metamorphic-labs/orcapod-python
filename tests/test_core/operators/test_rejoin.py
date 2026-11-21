"""Rejoin: the branches of one stream put back together, row by origin row (ITL-1336).

A diamond (one stream split into branches, then put back together) is expected to use
``Rejoin``: it matches every system-tag column and keeps those columns unchanged, so the
number of system-tag columns stays constant however many diamonds are stacked, where
``Join`` (unchanged) doubles them. The sources here have two tags and several data
columns; diamonds go through filters, renames, ``AddTags`` and function pods.
"""

from __future__ import annotations

import asyncio
import dataclasses
import itertools
import warnings
from pathlib import Path
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pytest

import orcapod.contexts as contexts
from orcapod import function_pod
from orcapod.core.operators import GroupBy, Join, Rejoin
from orcapod.core.sources import DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError, ProvisionalHashWarning
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.system_constants import constants

pytestmark = [
    pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning"),
    pytest.mark.filterwarnings(
        "ignore::orcapod.core.data_tags.DataTagMaterializationWarning"
    ),
]

ROWS = [
    {"subject": f"s{i % 3}", "unit": i, "rate": float(i), "depth": 10 * i, "area": "V1"}
    for i in range(8)
]
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


def _source(source_id: str = "units", *, data_tags: bool = False) -> DictSource:
    """Eight rows: tags ``subject`` and ``unit``; data ``rate``, ``depth``, ``area``."""
    return DictSource(
        ROWS,
        tag_columns=["subject", "unit"],
        tag_only_columns=not data_tags,
        source_id=source_id,
    )


def _system_tags(stream: Any) -> list[str]:
    names = stream.as_table(all_info=True).column_names
    return sorted(
        name for name in names if name.startswith(constants.SYSTEM_TAG_PREFIX)
    )


def _user_rows(stream: Any) -> list[dict[str, Any]]:
    """Rows of the user columns, sorted by tags, column order ignored."""
    rows = [dict(sorted(row.items())) for row in stream.as_table().to_pylist()]
    return sorted(rows, key=lambda row: (row["subject"], row["unit"]))


@function_pod(output_keys="doubled")
def double(rate: float) -> float:
    """Twice the rate."""
    return 2 * rate


# ---------------------------------------------------------------------------
# Diamonds
# ---------------------------------------------------------------------------


def test_a_two_branch_diamond_keeps_the_system_tags_and_equals_join() -> None:
    """A filter in one branch and a rename in the other: the rejoined rows are Join's
    rows on these unique tags, with the source's system-tag columns, unchanged."""
    source = _source()
    fast = source.select_data_columns(["rate"]).polars_filter(pl.col("rate") > 2)
    renamed = source.select_data_columns(["depth", "area"]).map_data(
        {"depth": "depth_um"}
    )
    rejoined = fast.rejoin(renamed)
    joined = fast.join(renamed)
    assert _user_rows(rejoined) == _user_rows(joined)
    assert len(_user_rows(rejoined)) == 5
    assert _system_tags(rejoined) == _system_tags(source)
    assert len(_system_tags(joined)) == 2 * len(_system_tags(source))
    # Each row keeps its origin row's system-tag values.
    full = rejoined.as_table(all_info=True)
    origin = source.as_table(all_info=True)
    by_unit = {row["unit"]: row for row in origin.to_pylist()}
    for row in full.to_pylist():
        for name in _system_tags(source):
            assert row[name] == by_unit[row["unit"]][name]


@pytest.mark.parametrize("data_tags", [False, True], ids=["plain", "data_tags"])
def test_a_three_branch_diamond_in_every_input_order(data_tags: bool) -> None:
    """Three branches (AddTags, a function pod, a filter); every input order gives the
    same rows, content hash and pipeline hash."""
    source = _source(data_tags=data_tags)
    tags, data = source.keys()
    kept = [name for name in data if name in tags]
    a = source.select_data_columns([*kept, "area"]).add_tags(cohort="A")
    b = double.pod(source.select_data_columns([*kept, "rate"]))
    c = source.select_data_columns([*kept, "depth"]).polars_filter(pl.col("depth") > 0)
    results = [Rejoin()(*order) for order in itertools.permutations((a, b, c))]
    first = results[0]
    assert _user_rows(first) and all(row["cohort"] == "A" for row in _user_rows(first))
    for result in results[1:]:
        assert _user_rows(result) == _user_rows(first)
        assert result.content_hash() == first.content_hash()
        assert result.pipeline_hash() == first.pipeline_hash()
        assert result.keys() == first.keys()
    assert _system_tags(first) == _system_tags(source)
    if data_tags:
        # Every data-tag is one column, with the source's source info.
        table = first.as_table(columns={"source": True})
        assert table.column_names.count("unit") == 1
        origin = source.as_table(columns={"source": True})
        token_of = dict(
            zip(
                origin.column("unit").to_pylist(),
                origin.column("_source_unit").to_pylist(),
            )
        )
        for unit, token in zip(
            table.column("unit").to_pylist(), table.column("_source_unit").to_pylist()
        ):
            assert token == token_of[unit]


@pytest.mark.parametrize("depth", [1, 3, 8])
def test_stacked_diamonds_keep_a_constant_number_of_system_tags(depth: int) -> None:
    """``depth`` diamonds in a row: Rejoin keeps the source's system-tag columns; Join
    doubles them at every diamond (checked up to depth 3)."""
    source = _source()
    rejoined: Any = source.select_data_columns(["rate"])
    joined: Any = rejoined
    for level in range(depth):
        branch = source.select_data_columns(["depth"]).map_data(
            {"depth": f"depth_{level}"}
        )
        rejoined = rejoined.rejoin(branch)
        if depth <= 3:
            joined = joined.join(branch)
    assert _system_tags(rejoined) == _system_tags(source)
    assert len(rejoined.as_table().column_names) == 2 + 1 + depth
    if depth <= 3:
        assert len(_system_tags(joined)) == 2 * (depth + 1)
        assert _user_rows(rejoined) == _user_rows(joined)


def test_a_single_input_passes_through() -> None:
    source = _source()
    assert _user_rows(Rejoin()(source)) == _user_rows(source)


# ---------------------------------------------------------------------------
# The system-tag column names never follow Join's name-extending rule (note 2)
# ---------------------------------------------------------------------------


def test_system_tag_names_are_the_inputs_on_every_path(tmp_path: Path) -> None:
    """Sync, async (barrier), zero-row typing, the predicted schema and a job node's
    stored table: the system-tag column names are the inputs'."""
    source = _source()
    expected = _system_tags(source)
    a = source.select_data_columns(["rate"])
    b = source.select_data_columns(["depth"])
    stream = Rejoin()(a, b)
    # Prediction and sync.
    predicted, _ = stream.output_schema(columns={"system_tags": True})
    assert sorted(
        n for n in predicted if n.startswith(constants.SYSTEM_TAG_PREFIX)
    ) == (expected)
    computed, _ = (
        Rejoin().static_process(a, b).output_schema(columns={"system_tags": True})
    )
    assert predicted == computed
    assert _system_tags(stream) == expected
    # Zero-row typing: the head before computing shows the same columns.
    fresh = Rejoin()(a, b)
    head = fresh.head(columns={"system_tags": True})
    assert (
        sorted(
            n
            for n in head.table.column_names
            if n.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        == expected
    )
    # Zero-row inputs.
    empty = a.polars_filter(pl.col("rate") < 0)
    zero = Rejoin()(empty, b)
    assert zero.as_table(all_info=True).num_rows == 0
    assert _system_tags(zero) == expected
    # Job nodes on both orchestrators, and their stored tables.
    for name, orchestrator in ORCHESTRATORS.items():
        job = PipelineJob(name=f"rejoin_{name}", database=InMemoryArrowDatabase())
        with job:
            _ = Rejoin()(
                source.select_data_columns(["rate"]),
                source.select_data_columns(["depth"]),
                label="rejoined",
            )
        _ = job.run(orchestrator=orchestrator())
        node = job.nodes["rejoined"]
        assert _system_tags(node) == expected, name
        assert _user_rows(node) == _user_rows(stream), name


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_a_branch_through_join_group_by_or_batch_is_refused() -> None:
    """Such a branch has other system-tag columns: refused, naming each input's, with a
    pointer to Join."""
    source = _source()
    other = _source("other").select_data_columns(["area"]).map_data({"area": "region"})
    branch = source.select_data_columns(["rate"])
    for changed in (
        source.select_data_columns(["depth"]).join(other),
        GroupBy("subject")(source.select_data_columns(["depth"])),
        source.select_data_columns(["depth"]).batch(2),
    ):
        with pytest.raises(InputValidationError) as caught:
            _ = branch.rejoin(changed)
        message = str(caught.value)
        assert "system-tag columns differ" in message
        assert "input 1:" in message and "input 2:" in message
        assert "use Join" in message


def test_inputs_without_system_tags_are_refused() -> None:
    """No system-tag column: no origin row to match on. An input whose schema states no
    system tag is checked when computed (a node of a loaded job downstream of a read-only
    function node states none either), so the refusal comes from the computation."""
    table = pa.table({"k": [1, 2], "x": [1, 2]})
    other = pa.table({"k": [1, 2], "y": [3, 4]})
    stream = Rejoin()(
        ArrowTableStream(table, tag_columns=["k"]),
        ArrowTableStream(other, tag_columns=["k"]),
    )
    with pytest.raises(InputValidationError, match="no system-tag columns"):
        _ = stream.as_table()


def test_a_shared_tag_that_differs_is_refused() -> None:
    """Two branches that each add tag ``run``, with different values: refused, naming it."""
    source = _source()
    a = source.select_data_columns(["rate"]).add_tags(run=1)
    b = source.select_data_columns(["depth"]).add_tags(run=2)
    with pytest.raises(InputValidationError, match="tag 'run' has different values"):
        _ = a.rejoin(b).as_table()
    same = source.select_data_columns(["depth"]).add_tags(run=1)
    assert {row["run"] for row in _user_rows(a.rejoin(same))} == {1}


def test_overlapping_data_columns_are_refused() -> None:
    source = _source()
    with pytest.raises(InputValidationError):
        _ = source.select_data_columns(["rate", "area"]).rejoin(
            source.select_data_columns(["depth", "area"])
        )


def test_two_rows_of_one_origin_row_in_one_input_are_refused() -> None:
    """A first rule: at most one row per origin row in each input; the error names the
    input and the system-tag values."""
    source = _source()
    table = source.as_table(columns={"system_tags": True}).select(
        ["subject", "unit", "rate"] + [name for name in _system_tags(source)]
    )
    doubled = pa.concat_tables([table, table.slice(0, 1)])
    duplicated = ArrowTableStream(doubled, tag_columns=["subject", "unit"])
    other = ArrowTableStream(
        source.as_table(columns={"system_tags": True}).select(
            ["subject", "unit", "depth"] + _system_tags(source)
        ),
        tag_columns=["subject", "unit"],
    )
    with pytest.raises(InputValidationError) as caught:
        _ = Rejoin()(duplicated, other).as_table()
    message = str(caught.value)
    assert "holds two rows of one origin row" in message
    assert "units" in message  # the source id among the system-tag values


def test_two_sources_with_one_schema_rejoin_to_nothing() -> None:
    """Same schema, different source ids: eligible (same system-tag column names), but
    no origin row is shared, so the result is empty, not an error."""
    a = _source("left").select_data_columns(["rate"])
    b = _source("right").select_data_columns(["depth"])
    result = Rejoin()(a, b)
    assert result.as_table().num_rows == 0
    assert set(result.as_table().column_names) == {"subject", "unit", "rate", "depth"}


# ---------------------------------------------------------------------------
# Provisional, save and load
# ---------------------------------------------------------------------------


def test_rejoin_is_provisional_and_warns_once() -> None:
    from orcapod.core.operators.provisional import reset_provisional_use_warnings

    reset_provisional_use_warnings()
    source = _source()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = source.select_data_columns(["rate"]).rejoin(
            source.select_data_columns(["depth"])
        )
        _ = Rejoin()
    provisional = [w for w in caught if issubclass(w.category, ProvisionalHashWarning)]
    assert len(provisional) == 1 and "Rejoin" in str(provisional[0].message)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_saved_job_with_a_rejoin_node_loads_and_reads_its_results(
    tmp_path: Path, orchestrator: str
) -> None:
    """The loaded node is a Rejoin again, with the same hashes; a load-only run reads
    the stored rows."""
    csv = tmp_path / "units.csv"
    csv.write_text(
        "subject,unit,rate,depth\n"
        + "".join(f"s{i % 3},{i},{float(i)},{10 * i}\n" for i in range(6))
    )
    from orcapod.core.sources import CSVSource

    source = CSVSource(
        str(csv),
        tag_columns=["subject", "unit"],
        tag_only_columns=True,
        source_id="units",
    )
    job = PipelineJob(name="rejoin", database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        _ = source.select_data_columns(["rate"]).rejoin(
            cast(Any, double.pod(source.select_data_columns(["rate"]))).map_data(
                {"doubled": "twice"}
            ),
            label="rejoined",
        )
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    original = job.nodes["rejoined"]
    rows = _user_rows(original)
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(str(tmp_path / "job.json"))
    node = loaded.nodes["rejoined"]
    assert type(node.producer).__name__ == "Rejoin"
    assert node.load_status.name == "FULL"
    assert node.content_hash() == original.content_hash()
    assert node.pipeline_hash() == original.pipeline_hash()
    _ = loaded.run(load_only=True, orchestrator=ORCHESTRATORS[orchestrator]())
    assert _user_rows(node) == rows


def test_the_async_barrier_gives_the_sync_rows() -> None:
    """Rejoin's async path is the barrier: the rows equal the sync path's."""
    from orcapod.channels import Channel

    source = _source(data_tags=True)
    a = source.select_data_columns(["subject", "unit", "rate"])
    b = source.select_data_columns(["subject", "unit", "depth"])
    sync = Rejoin()(a, b).as_table()

    async def run() -> list[Any]:
        inputs = []
        for stream in (a, b):
            channel: Channel[Any] = Channel()
            for row in stream.iter_data():
                await channel.writer.send(row)
            await channel.writer.close()
            inputs.append(channel.reader)
        out: Channel[Any] = Channel()
        await Rejoin().async_execute(
            inputs,
            out.writer,
            input_pipeline_hashes=[a.pipeline_hash(), b.pipeline_hash()],
        )
        return await out.reader.collect()

    rows = asyncio.run(run())
    assert len(rows) == sync.num_rows
    got = sorted((tag["unit"], data["rate"], data["depth"]) for tag, data in rows)
    want = sorted(zip(*(sync.column(n).to_pylist() for n in ("unit", "rate", "depth"))))
    assert got == want


def test_join_is_unchanged() -> None:
    """Join still extends system-tag names on a diamond (its rule, ITL-1117)."""
    source = _source()
    joined = Join()(
        source.select_data_columns(["rate"]), source.select_data_columns(["depth"])
    )
    assert len(_system_tags(joined)) == 4
    assert all(constants.BLOCK_SEPARATOR in name for name in _system_tags(joined))


# ---------------------------------------------------------------------------
# Codex review of PR 472
# ---------------------------------------------------------------------------


def test_a_null_in_a_system_tag_is_refused() -> None:
    """Provenance is never null: an input with a null in a system-tag column is refused,
    naming the column, instead of silently losing the row (the reviewer's case)."""
    source = _source()
    full = source.as_table(columns={"system_tags": True})
    names = _system_tags(source)
    source_id = next(name for name in names if "source_id" in name)
    values = full.column(source_id).to_pylist()
    nulled = full.set_column(
        full.schema.get_field_index(source_id),
        pa.field(source_id, pa.large_string(), nullable=True),
        pa.array([None, None, *values[2:]], pa.large_string()),
    )
    a = ArrowTableStream(
        nulled.select(["subject", "unit", "rate", *names]),
        tag_columns=["subject", "unit"],
    )
    b = ArrowTableStream(
        nulled.select(["subject", "unit", "depth", *names]),
        tag_columns=["subject", "unit"],
    )
    with pytest.raises(InputValidationError, match="null in system-tag column"):
        _ = Rejoin()(a, b).as_table()


def _tied(k: list[float]) -> ArrowTableStream:
    """A raw stream with data-tags ``k`` (float) and ``g``, and fixed system tags."""
    table = pa.table(
        {
            "k": pa.array(k, pa.float64()),
            "g": pa.array([1, 2], pa.int64()),
            f"{constants.SYSTEM_TAG_PREFIX}source_id::x": ["s", "s"],
            f"{constants.SYSTEM_TAG_PREFIX}record_id::x": ["r1", "r2"],
        }
    )
    return ArrowTableStream(table, tag_columns=["k", "g"], data_tags=["k", "g"])


@pytest.mark.parametrize(
    ("left", "right"),
    [([0.0, 0.0], [-0.0, -0.0]), ([float("nan")] * 2, [-float("nan")] * 2)],
    ids=["signed_zero", "nan_payload"],
)
def test_equal_values_in_different_bits_are_refused_in_both_orders(
    left: list[float], right: list[float]
) -> None:
    """Inputs with equal pipeline hashes keep the caller's order; a shared tag equal in
    value but not in bits would make the output's bits follow it. Refused in both
    orders, naming the tag (the reviewer's case)."""
    a, b = _tied(left), _tied(right)
    assert a.pipeline_hash() == b.pipeline_hash()
    for inputs in ((a, b), (b, a)):
        with pytest.raises(InputValidationError, match="tag 'k' has different values"):
            _ = Rejoin()(*inputs).as_table()
    same = Rejoin()(a, _tied(left)).as_table()
    assert same.num_rows == 2


def test_stated_system_tags_are_compared_even_beside_an_input_that_states_none() -> (
    None
):
    """Two inputs that state different system-tag columns are refused at construction,
    even when a third states none (the reviewer's case)."""
    source = _source()
    a = source.select_data_columns(["rate"])
    b = GroupBy("subject")(source.select_data_columns(["depth"]))
    c = ArrowTableStream(
        pa.table({"thirdtag": [1, 2], "thirddata": [3, 4]}), tag_columns=["thirdtag"]
    )
    with pytest.raises(InputValidationError, match="system-tag columns differ"):
        _ = Rejoin()(a, b, c)


def _float_provenance(origin: list[float], data: str) -> ArrowTableStream:
    """A raw stream whose one system tag is a float, with data column ``data``."""
    table = pa.table(
        {
            "g": pa.array([1, 2], pa.int64()),
            "origin": pa.array(origin, pa.float64()),
            data: pa.array([0.5, 0.25], pa.float64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["g"], system_tag_columns=["origin"])


def test_system_tags_matched_by_equality_must_be_the_same_bits() -> None:
    """Polars matches -0.0 with 0.0; a system tag matched that way is refused, in
    both argument orders, naming the system tag (CodeRabbit on PR 472)."""
    a = _float_provenance([0.0, 1.0], "x")
    b = _float_provenance([-0.0, 1.0], "y")
    for inputs in ((a, b), (b, a)):
        with pytest.raises(InputValidationError, match="system tag '_tag_origin'"):
            _ = Rejoin()(*inputs).as_table()
    same = Rejoin()(a, _float_provenance([0.0, 1.0], "y")).as_table()
    assert same.num_rows == 2


OTHER_CONTEXT = "std:v1:test-other"


@pytest.fixture
def other_context(monkeypatch: pytest.MonkeyPatch) -> contexts.DataContext:
    """A copy of the default data context, registered under another key (one data
    context ships, so the copy stands in for a second)."""
    other = dataclasses.replace(
        contexts.get_default_context(), context_key=OTHER_CONTEXT
    )
    registry = contexts._get_registry()  # pyright: ignore[reportPrivateUsage]  # the test registers a second context
    original = registry.get_context

    def get_context(key: Any = None) -> contexts.DataContext:  # pyright: ignore[reportExplicitAny]  # the registry's own argument
        return other if key == OTHER_CONTEXT else original(key)

    monkeypatch.setattr(registry, "get_context", get_context)
    return other


def _in_context(context: contexts.DataContext, data: str) -> ArrowTableStream:
    """A raw stream of the given data context: tags ``g`` and ``h``, one system tag,
    data column ``data``."""
    table = pa.table(
        {
            "g": pa.array([1, 2, 3], pa.int64()),
            "h": pa.array(["a", "b", "c"], pa.large_string()),
            data: pa.array([0.5, 0.25, 0.125], pa.float64()),
            "origin": pa.array(["r1", "r2", "r3"], pa.large_string()),
            constants.CONTEXT_KEY: pa.array(
                [context.context_key] * 3, pa.large_string()
            ),
        }
    )
    return ArrowTableStream(
        table,
        tag_columns=["g", "h"],
        system_tag_columns=["origin"],
        data_context=context,
    )


def test_the_result_keeps_the_inputs_data_context(
    other_context: contexts.DataContext,
) -> None:
    """The rejoined rows keep their inputs' data context on the sync and the async
    path, in either argument order (CodeRabbit on PR 472)."""
    from orcapod.channels import Channel

    a = _in_context(other_context, "x")
    b = _in_context(other_context, "y")
    for inputs in ((a, b), (b, a)):
        result = Rejoin().static_process(*inputs)
        assert result.data_context_key == OTHER_CONTEXT
        table = result.as_table(columns={"context": True})
        assert table.column(constants.CONTEXT_KEY).to_pylist() == [OTHER_CONTEXT] * 3
        assert {data.data_context_key for _, data in result.iter_data()} == {
            OTHER_CONTEXT
        }

    async def run() -> list[Any]:
        inputs = []
        for branch in (a, b):
            channel: Channel[Any] = Channel()
            for row in branch.iter_data():
                await channel.writer.send(row)
            await channel.writer.close()
            inputs.append(channel.reader)
        out: Channel[Any] = Channel()
        await Rejoin().async_execute(
            inputs,
            out.writer,
            input_pipeline_hashes=[a.pipeline_hash(), b.pipeline_hash()],
        )
        return await out.reader.collect()

    rows = asyncio.run(run())
    assert len(rows) == 3
    assert {tag.data_context_key for tag, _ in rows} == {OTHER_CONTEXT}
    assert {data.data_context_key for _, data in rows} == {OTHER_CONTEXT}


def test_inputs_of_different_data_contexts_are_refused(
    other_context: contexts.DataContext,
) -> None:
    """Rows of one origin row carry one data context: inputs that do not are refused,
    naming both keys, in either argument order."""
    a = _in_context(other_context, "x")
    b = _in_context(contexts.get_default_context(), "y")
    for inputs in ((a, b), (b, a)):
        with pytest.raises(InputValidationError, match="different data contexts"):
            _ = Rejoin()(*inputs).as_table()
