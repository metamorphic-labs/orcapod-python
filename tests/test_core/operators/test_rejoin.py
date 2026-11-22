"""Rejoin: the branches of one stream put back together, row by origin row (ITL-1336).

A diamond (one stream split into branches, then put back together) is expected to use
``Rejoin``: it matches every system-tag column and keeps those columns unchanged, so the
number of system-tag columns stays constant however many diamonds are stacked, where
``Join`` (unchanged) doubles them. The sources here have two tags and several data
columns; diamonds go through filters, renames, ``AddTags`` and function pods.

Rejoin is an inner join on every column its inputs share (ITL-1355): the system tags, a
tag several inputs have, a shared data-tag's source info and the data context, compared
as ``Join`` compares keys. It refuses only what the inputs' columns show; rows that match
nothing are left out, and a ``RejoinDifferingColumnWarning`` names the shared columns in
which rows of one origin row differ.
"""

from __future__ import annotations

import asyncio
import dataclasses
import itertools
import math
import struct
import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast
from unittest import mock

import polars as pl
import pyarrow as pa
import pytest

import orcapod.contexts as contexts
from orcapod import function_pod
from orcapod.core.operators import GroupBy, Join, Rejoin
from orcapod.core.sources import DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import (
    InputValidationError,
    ProvisionalHashWarning,
    RejoinDifferingColumnWarning,
)
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


def test_a_shared_tags_type_that_differs_is_refused() -> None:
    """A tag two inputs have in different Arrow types is refused, naming it and both
    types: the inputs' columns show it, before any row is matched."""
    origin = _origin()
    a = _branch(origin, ["rate"])
    b = _branch(origin, ["depth"], unit=pa.array(range(8), pa.int32()))
    with pytest.raises(InputValidationError, match="Arrow type of column 'unit'"):
        _ = Rejoin()(a, b).as_table()


def test_overlapping_data_columns_are_refused() -> None:
    source = _source()
    with pytest.raises(InputValidationError):
        _ = source.select_data_columns(["rate", "area"]).rejoin(
            source.select_data_columns(["depth", "area"])
        )


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


def _wide_in_context(context: contexts.DataContext) -> ArrowTableStream:
    """A raw stream of ``context``: tags ``g`` and ``h``, data ``w``, ``x``, ``y``,
    ``z``, one system tag."""
    table = pa.table(
        {
            "g": pa.array([1, 2, 3], pa.int64()),
            "h": pa.array(["a", "b", "c"], pa.large_string()),
            "w": pa.array([1, 2, 3], pa.int64()),
            "x": pa.array([0.5, 0.25, 0.125], pa.float64()),
            "y": pa.array(["p", "q", "r"], pa.large_string()),
            "z": pa.array([True, False, True], pa.bool_()),
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


def test_stacked_rejoins_in_a_non_default_context_are_accepted(
    other_context: contexts.DataContext,
) -> None:
    """A Rejoin over Rejoins and branches of one non-default data context computes, on
    the sync and the async path, whatever each input stream reports (a lazy operator
    stream reports its pod's context; the rows carry theirs): the context is read from
    the rows (CodeRabbit on PR 472)."""
    from orcapod.channels import Channel
    from orcapod.core.operators import SelectDataColumns

    source = _wide_in_context(other_context)
    lazy = {c: source.select_data_columns([c]) for c in "wxyz"}
    eager = {c: SelectDataColumns([c]).static_process(source) for c in "wxyz"}
    assert lazy["w"].data_context_key != eager["w"].data_context_key == OTHER_CONTEXT
    stacks = {
        "rejoins": (Rejoin()(lazy["w"], lazy["x"]), Rejoin()(eager["y"], lazy["z"])),
        "rejoin_and_branches": (
            Rejoin()(lazy["x"], eager["w"]),
            eager["y"],
            lazy["z"],
        ),
        "raw_input": (
            Rejoin()(lazy["w"], lazy["x"]),
            source.select_data_columns(["y", "z"]),
        ),
    }
    for name, inputs in stacks.items():
        for ordered in (inputs, tuple(reversed(inputs))):
            result = Rejoin().static_process(*ordered)
            assert result.data_context_key == OTHER_CONTEXT, name
            table = result.as_table(columns={"context": True})
            assert (
                table.column(constants.CONTEXT_KEY).to_pylist() == [OTHER_CONTEXT] * 3
            ), name
            assert sorted(table.column_names) == sorted(
                ["g", "h", "w", "x", "y", "z", constants.CONTEXT_KEY]
            ), name
            lazy_result = Rejoin()(*ordered)
            assert (
                lazy_result.as_table(columns={"context": True})
                .column(constants.CONTEXT_KEY)
                .to_pylist()
                == [OTHER_CONTEXT] * 3
            ), name

        async def run(streams: tuple[Any, ...]) -> list[Any]:
            channels = []
            for stream in streams:
                channel: Channel[Any] = Channel()
                for row in stream.iter_data():
                    await channel.writer.send(row)
                await channel.writer.close()
                channels.append(channel.reader)
            out: Channel[Any] = Channel()
            await Rejoin().async_execute(
                channels,
                out.writer,
                input_pipeline_hashes=[stream.pipeline_hash() for stream in streams],
            )
            return await out.reader.collect()

        rows = asyncio.run(run(inputs))
        assert len(rows) == 3, name
        assert {tag.data_context_key for tag, _ in rows} == {OTHER_CONTEXT}, name
        assert {data.data_context_key for _, data in rows} == {OTHER_CONTEXT}, name


# ---------------------------------------------------------------------------
# Rows that match nothing are left out (ITL-1355)
# ---------------------------------------------------------------------------


def _origin() -> pa.Table:
    """The source's rows with their system tags."""
    return _source().as_table(columns={"system_tags": True})


def _branch(
    origin: pa.Table,
    data: Sequence[str],
    *,
    tags: Sequence[str] = ("subject", "unit"),
    data_tags: Sequence[str] = (),
    **columns: Any,
) -> ArrowTableStream:
    """A branch of ``origin`` built by hand: its system tags, the named tags and data.

    ``columns`` replaces or adds columns (a tag a branch changed, a tag it added, the
    source info of a data-tag, the data context column).
    """
    table = origin
    for name, values in columns.items():
        array = values if isinstance(values, pa.Array) else pa.array(values)
        if name in table.column_names:
            table = table.set_column(table.column_names.index(name), name, array)
        else:
            table = table.append_column(name, array)
    system_tags = [
        name
        for name in origin.column_names
        if name.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    hidden = [
        name
        for name in table.column_names
        if name.startswith(constants.SOURCE_PREFIX) or name == constants.CONTEXT_KEY
    ]
    return ArrowTableStream(
        table.select([*tags, *data, *system_tags, *hidden]),
        tag_columns=list(tags),
        data_tags=list(data_tags),
    )


def _rejoined(*inputs: Any) -> tuple[Any, list[str]]:
    """The rejoin of ``inputs`` computed, and its ``RejoinDifferingColumnWarning``s."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        stream = Rejoin()(*inputs)
        _ = stream.as_table()
    return stream, [
        str(w.message)
        for w in caught
        if issubclass(w.category, RejoinDifferingColumnWarning)
    ]


def _units(stream: Any) -> list[int]:
    return sorted(stream.as_table().column("unit").to_pylist())


def test_a_shared_tag_that_differs_matches_nothing() -> None:
    """Two branches that each add tag ``run``, with different values: no row is put
    together, nothing is refused, and one warning names the tag. With the same value
    the rows are put together and nothing warns."""
    source = _source()
    a = source.select_data_columns(["rate"]).add_tags(run=1)
    b = source.select_data_columns(["depth"]).add_tags(run=2)
    for inputs in ((a, b), (b, a)):
        result, warned = _rejoined(*inputs)
        assert result.as_table().num_rows == 0
        assert set(result.as_table().column_names) == {
            "subject",
            "unit",
            "run",
            "rate",
            "depth",
        }
        assert len(warned) == 1 and "tag 'run'" in warned[0]
    same = source.select_data_columns(["depth"]).add_tags(run=1)
    result, warned = _rejoined(a, same)
    assert {row["run"] for row in _user_rows(result)} == {1}
    assert len(_user_rows(result)) == 8 and not warned


def test_several_shared_tags_several_inputs_in_every_order() -> None:
    """Three branches sharing ``subject``, ``unit`` and, two by two, the added tags
    ``run``, ``site`` and ``day``: a row is put together only when every column two
    inputs share is equal, whatever the order of the inputs."""
    origin = _origin()
    run_a = [1, 1, 1, 1, 2, 2, 2, 2]
    run_b = [1, 1, 9, 1, 2, 2, 2, 2]  # unit 2 differs from a
    site_a = ["x"] * 8
    site_c = ["x", "y", "x", "x", "x", "x", "x", "x"]  # unit 1 differs from a
    day_b = [5, 5, 5, 5, 5, 5, 7, 5]
    day_c = [5, 5, 5, 5, 5, 5, 5, 5]  # unit 6 differs from b
    a = _branch(
        origin,
        ["rate"],
        tags=["subject", "unit", "run", "site"],
        run=run_a,
        site=site_a,
    )
    b = _branch(
        origin, ["depth"], tags=["day", "run", "unit", "subject"], run=run_b, day=day_b
    )
    c = _branch(
        origin,
        ["area"],
        tags=["site", "subject", "day", "unit"],
        site=site_c,
        day=day_c,
    )
    results = []
    for order in itertools.permutations((a, b, c)):
        result, warned = _rejoined(*order)
        assert _units(result) == [0, 3, 4, 5, 7]
        # Every differing column is named, whatever the order of the inputs.
        assert len(warned) == 1
        assert all(f"tag {name!r}" in warned[0] for name in ("run", "site", "day"))
        results.append(result)
    first = results[0]
    assert all(row["site"] == "x" and row["day"] == 5 for row in _user_rows(first))
    for result in results[1:]:
        assert _user_rows(result) == _user_rows(first)
        assert result.keys() == first.keys()
        assert result.pipeline_hash() == first.pipeline_hash()
    assert _system_tags(first) == _system_tags(_source())
    # Two of them, in both orders: only the columns those two share count.
    for pair, kept in (
        ((a, b), [0, 1, 3, 4, 5, 6, 7]),
        ((b, c), [0, 1, 2, 3, 4, 5, 7]),
    ):
        for order in (pair, pair[::-1]):
            assert _units(_rejoined(*order)[0]) == kept


def test_a_null_in_a_system_tag_matches_nothing() -> None:
    """Provenance is never null: rows with a null in a system-tag column match nothing
    and are left out, in both inputs; the other rows are put together. Nothing is
    refused and nothing warns (no shared column differs)."""
    origin = _origin()
    source_id = next(name for name in _system_tags(_source()) if "source_id" in name)
    values = origin.column(source_id).to_pylist()
    nulled: dict[str, Any] = {
        source_id: pa.array([None, None, *values[2:]], pa.large_string())
    }
    a = _branch(origin, ["rate"], **nulled)
    b = _branch(origin, ["depth"], **nulled)
    whole = _branch(origin, ["area"])
    for inputs in ((a, b), (b, a), (a, whole), (whole, a), (a, whole, b)):
        result, warned = _rejoined(*inputs)
        assert _units(result) == [2, 3, 4, 5, 6, 7]
        assert not warned


def test_a_null_in_another_shared_column_equals_a_null() -> None:
    """Only a system tag's null matches nothing. Rows of one origin row that both hold
    no value in a shared tag, or in a shared data-tag's source info, are put together."""
    origin = _origin()
    holes = pa.array(
        ["s0", None, "s2", None, "s1", "s2", "s0", "s1"], pa.large_string()
    )
    a = _branch(origin, ["rate"], subject=holes)
    b = _branch(origin, ["depth"], subject=holes)
    result, warned = _rejoined(a, b)
    assert _units(result) == list(range(8)) and not warned
    assert result.as_table().column("subject").to_pylist() == holes.to_pylist()
    # A null against a value differs.
    c = _branch(origin, ["depth"])
    result, warned = _rejoined(a, c)
    assert _units(result) == [0, 2, 4, 5, 6, 7]
    assert len(warned) == 1 and "tag 'subject'" in warned[0]
    # A raw stream's data-tag has no source info at all: null on both sides.
    tied = Rejoin()(_tied([1.0, 2.0]), _tied([1.0, 2.0])).as_table()
    assert tied.num_rows == 2


def test_a_shared_data_tags_source_info_that_differs_matches_nothing() -> None:
    """A data-tag in two inputs is one column with one source info: rows of one origin
    row whose tokens differ are left out, and the warning names the data-tag."""
    origin = _origin()
    tokens = [f"units::row_{i}::unit" for i in range(8)]
    changed = [*tokens[:6], "elsewhere::row_6::unit", None]
    a = _branch(origin, ["rate"], data_tags=["unit"], _source_unit=tokens)
    b = _branch(origin, ["depth"], data_tags=["unit"], _source_unit=changed)
    for inputs in ((a, b), (b, a)):
        result, warned = _rejoined(*inputs)
        assert _units(result) == [0, 1, 2, 3, 4, 5]
        assert len(warned) == 1
        assert "the source info of data-tag 'unit'" in warned[0]
    result, warned = _rejoined(
        a, _branch(origin, ["depth"], data_tags=["unit"], _source_unit=tokens)
    )
    assert _units(result) == list(range(8)) and not warned


def test_an_input_that_repeats_an_origin_row_pairs_each_row_once() -> None:
    """System tags are assumed unique, so nothing reads the rows for it. Should an
    input repeat an origin row, each row still matches at most once: the repeats pair
    with the other input's in row order, and a row left over matches nothing."""
    origin = _origin()
    twice = pa.concat_tables([origin, origin.slice(0, 1)])
    left = _branch(
        twice, ["rate"], rate=pa.array([*origin.column("rate").to_pylist(), 100.0])
    )
    once = _branch(origin, ["depth"])
    for inputs in ((left, once), (once, left)):
        result, warned = _rejoined(*inputs)
        assert _units(result) == list(range(8)) and not warned
        rates = {row["unit"]: row["rate"] for row in result.as_table().to_pylist()}
        assert rates[0] == 0.0  # the first of the two rows; the other matches nothing
    right = _branch(
        twice, ["depth"], depth=pa.array([*origin.column("depth").to_pylist(), 1000])
    )
    result, warned = _rejoined(left, right)
    rows = [
        (row["rate"], row["depth"])
        for row in result.as_table().to_pylist()
        if row["unit"] == 0
    ]
    assert sorted(rows) == [(0.0, 0), (100.0, 1000)] and not warned
    assert result.as_table().num_rows == 9


def _repeated(n: int, data: str, k: list[int] | None = None) -> ArrowTableStream:
    """``n`` rows of ONE origin row: tag ``k``, one system tag, data ``data`` = 0..n-1."""
    table = pa.table(
        {
            "k": pa.array(k or [1] * n, pa.int64()),
            data: pa.array(range(n), pa.int64()),
            "origin": pa.array(["x"] * n, pa.large_string()),
        }
    )
    return ArrowTableStream(table, tag_columns=["k"], system_tag_columns=["origin"])


def _join_sizes(compute: Any) -> tuple[Any, list[int]]:
    """Run ``compute`` and return its result and the height of every Polars join in it."""
    sizes: list[int] = []
    join = pl.DataFrame.join

    def measured(frame: pl.DataFrame, *args: Any, **kwargs: Any) -> pl.DataFrame:
        joined = join(frame, *args, **kwargs)
        sizes.append(joined.height)
        return joined

    with mock.patch.object(pl.DataFrame, "join", measured):
        return compute(), sizes


def test_thousands_of_repeats_of_one_origin_row_are_paired_without_a_cross_product() -> (
    None
):
    """Two inputs that each hold one origin row 3,000 times give 3,000 rows, the n-th
    of one with the n-th of the other, and no join ever holds more rows than an input
    (pairing every repeat with every repeat would make 9,000,000)."""
    n = 3000
    with warnings.catch_warnings():
        warnings.simplefilter("error", RejoinDifferingColumnWarning)
        table, sizes = _join_sizes(
            lambda: Rejoin()(_repeated(n, "a"), _repeated(n, "b")).as_table()
        )
    assert table.num_rows == n
    assert table.column("a").to_pylist() == table.column("b").to_pylist()
    assert sizes and max(sizes) <= n
    # Fewer repeats in one input: the rows left over match nothing.
    table, sizes = _join_sizes(
        lambda: Rejoin()(_repeated(n, "a"), _repeated(n - 7, "b")).as_table()
    )
    assert table.num_rows == n - 7 and max(sizes) <= n
    assert table.column("a").to_pylist() == table.column("b").to_pylist()


def test_repeats_that_differ_in_a_shared_tag_pair_within_equal_values() -> None:
    """Repeats of one origin row with two values of a shared tag, in opposite orders in
    the two inputs: each row pairs with the next free row of the same value, in row
    order, still without a cross product."""
    n = 2000
    left = _repeated(n, "a", [1, 2] * (n // 2))
    right = _repeated(n, "b", [2, 1] * (n // 2))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RejoinDifferingColumnWarning)
        table, sizes = _join_sizes(lambda: Rejoin()(left, right).as_table())
    assert table.num_rows == n and max(sizes) <= n
    rows = sorted(zip(*(table.column(c).to_pylist() for c in ("a", "b", "k"))))
    # a = 0, 2, 4, ... hold k = 1, as b = 1, 3, 5, ... do; and the other way round.
    assert rows == sorted(
        [(i, i + 1, 1) for i in range(0, n, 2)]
        + [(i, i - 1, 2) for i in range(1, n, 2)]
    )


# ---------------------------------------------------------------------------
# Equality is Join's; a shared column keeps the first input's values
# ---------------------------------------------------------------------------


def _bits(value: Any) -> bytes:
    """The eight bytes of a float: its sign and its NaN payload count."""
    return struct.pack("<d", value)


def _first(*streams: Any) -> Any:
    """The first input in Rejoin's canonical order: by pipeline hash; inputs with equal
    pipeline hashes stay in the order given."""
    return sorted(streams, key=lambda stream: stream.pipeline_hash().to_hex())[0]


@pytest.mark.parametrize(
    ("left", "right"),
    [([0.0, 0.0], [-0.0, -0.0]), ([float("nan")] * 2, [-float("nan")] * 2)],
    ids=["signed_zero", "nan_payload"],
)
def test_equal_values_in_different_bits_match_and_the_first_inputs_are_kept(
    left: list[float], right: list[float]
) -> None:
    """``-0.0`` equals ``0.0`` and NaN equals NaN, as in ``Join``: the rows are put
    together and nothing warns. The shared tag keeps the first input's bits. These two
    inputs have one pipeline hash, so the first is the first argument."""
    a, b = _tied(left), _tied(right)
    assert a.pipeline_hash() == b.pipeline_hash()
    for inputs in ((a, b), (b, a)):
        result, warned = _rejoined(*inputs)
        table = result.as_table()
        assert table.num_rows == 2 and not warned
        kept = [_bits(value) for value in table.column("k").to_pylist()]
        assert kept == [
            _bits(value) for value in inputs[0].as_table().column("k").to_pylist()
        ]
    assert _bits(left[0]) != _bits(right[0])
    assert math.isnan(left[0]) or left[0] == right[0]


def test_the_first_input_is_the_first_by_pipeline_hash_whatever_the_argument_order() -> (
    None
):
    """Inputs of different pipeline hashes are taken in the order of those hashes: the
    shared column keeps the same input's bits in both argument orders. Here the shared
    column is a float system tag, ``-0.0`` in one input and ``0.0`` in the other."""
    a = _float_provenance([0.0, 1.0], "x")
    b = _float_provenance([-0.0, 1.0], "y")
    assert a.pipeline_hash() != b.pipeline_hash()
    first = _first(a, b)
    wanted = _bits(
        first.as_table(columns={"system_tags": True})
        .column("_tag_origin")
        .to_pylist()[0]
    )
    for inputs in ((a, b), (b, a)):
        result, warned = _rejoined(*inputs)
        table = result.as_table(columns={"system_tags": True})
        assert table.num_rows == 2 and not warned
        by_g = dict(
            zip(table.column("g").to_pylist(), table.column("_tag_origin").to_pylist())
        )
        assert _bits(by_g[1]) == wanted


def test_three_inputs_keep_the_bits_of_the_first_that_has_the_column() -> None:
    """A tag only the second and third inputs have keeps the second's values."""
    origin = _origin()
    zeros, minus = [0.0] * 8, [-0.0] * 8
    a = _branch(origin, ["rate"])
    b = _branch(origin, ["depth"], tags=["subject", "unit", "level"], level=zeros)
    c = _branch(origin, ["area"], tags=["subject", "unit", "level"], level=minus)
    holders = [
        stream
        for stream in sorted((a, b, c), key=lambda s: s.pipeline_hash().to_hex())
        if stream is not a
    ]
    wanted = _bits(holders[0].as_table().column("level").to_pylist()[0])
    for order in itertools.permutations((a, b, c)):
        result, warned = _rejoined(*order)
        levels = result.as_table().column("level").to_pylist()
        assert len(levels) == 8 and not warned
        assert {_bits(value) for value in levels} == {wanted}


# ---------------------------------------------------------------------------
# The warning
# ---------------------------------------------------------------------------


def test_the_warning_is_issued_once_per_operator_and_set_of_columns() -> None:
    """One operator warns once for one set of differing columns, again for another
    set, and never when nothing differs; an error filter raises it each time."""
    origin = _origin()
    plain = _branch(
        origin,
        ["rate"],
        tags=["subject", "unit", "run", "site"],
        run=[1] * 8,
        site=["x"] * 8,
    )
    same = _branch(
        origin,
        ["depth"],
        tags=["subject", "unit", "run", "site"],
        run=[1] * 8,
        site=["x"] * 8,
    )
    other_run = _branch(
        origin,
        ["depth"],
        tags=["subject", "unit", "run", "site"],
        run=[2] * 8,
        site=["x"] * 8,
    )
    both = _branch(
        origin,
        ["depth"],
        tags=["subject", "unit", "run", "site"],
        run=[2, *[1] * 7],
        site=[*["x"] * 7, "y"],
    )
    rejoin = Rejoin()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for inputs in (
            (plain, same),
            (plain, other_run),
            (other_run, plain),
            (plain, both),
            (plain, both),
            (plain, other_run),
        ):
            _ = rejoin.static_process(*inputs)
    messages = [
        str(w.message)
        for w in caught
        if issubclass(w.category, RejoinDifferingColumnWarning)
    ]
    assert len(messages) == 2
    assert "tag 'run'" in messages[0] and "site" not in messages[0]
    assert "tag 'run'" in messages[1] and "tag 'site'" in messages[1]
    assert "orcapod.errors.RejoinDifferingColumnWarning" in messages[0]

    strict = Rejoin()
    with warnings.catch_warnings():
        warnings.simplefilter("error", RejoinDifferingColumnWarning)
        _ = strict.static_process(plain, same)  # nothing differs: nothing raised
        for _ in range(2):
            with pytest.raises(RejoinDifferingColumnWarning, match="tag 'run'"):
                _ = strict.static_process(plain, other_run)


def _one_row(data: str, k: int, h: int, origin: str = "r") -> ArrowTableStream:
    """One row: tags ``k`` and ``h``, one system tag, data column ``data``."""
    table = pa.table(
        {
            "k": pa.array([k], pa.int64()),
            "h": pa.array([h], pa.int8()),
            data: pa.array([0.5], pa.float64()),
            "origin": pa.array([origin], pa.large_string()),
        }
    )
    return ArrowTableStream(
        table, tag_columns=["k", "h"], system_tag_columns=["origin"]
    )


def _in_canonical_order(*data: str) -> list[str]:
    """The data column names in the order Rejoin takes inputs holding them (their
    pipeline hashes do not depend on the values)."""
    return sorted(data, key=lambda name: _one_row(name, 0, 0).pipeline_hash().to_hex())


def _rejoined_async(*inputs: Any) -> tuple[list[Any], list[str]]:
    """The rows of the async path, and its ``RejoinDifferingColumnWarning``s."""
    from orcapod.channels import Channel

    async def run() -> list[Any]:
        readers = []
        for stream in inputs:
            channel: Channel[Any] = Channel()
            for row in stream.iter_data():
                await channel.writer.send(row)
            await channel.writer.close()
            readers.append(channel.reader)
        out: Channel[Any] = Channel()
        await Rejoin().async_execute(
            readers,
            out.writer,
            input_pipeline_hashes=[stream.pipeline_hash() for stream in inputs],
        )
        return await out.reader.collect()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rows = asyncio.run(run())
    return rows, [
        str(w.message)
        for w in caught
        if issubclass(w.category, RejoinDifferingColumnWarning)
    ]


def test_the_warning_names_every_differing_column_of_one_origin_row() -> None:
    """Three inputs hold one origin row with ``(k, h)`` = (1, 1), (2, 1), (1, 2) in the
    order Rejoin takes them: the first two differ in ``k``, so the row is gone before
    the third is met, and the third differs in ``h``. Both are named, on the sync and
    the async path, in every argument order (the warning is read from the inputs' own
    rows, not from the rows rejoined so far)."""
    first, second, third = _in_canonical_order("a", "b", "c")
    inputs = (_one_row(first, 1, 1), _one_row(second, 2, 1), _one_row(third, 1, 2))
    for order in itertools.permutations(inputs):
        result, warned = _rejoined(*order)
        assert result.as_table().num_rows == 0
        assert len(warned) == 1 and "tag 'h', tag 'k'" in warned[0]
        rows, warned = _rejoined_async(*order)
        assert rows == []
        assert len(warned) == 1 and "tag 'h', tag 'k'" in warned[0]


def test_the_warning_sees_two_inputs_that_differ_where_another_lacks_the_row() -> None:
    """The first input Rejoin takes holds another origin row only; the second and
    third hold one origin row and differ in ``k``. Nothing is put together, and the
    warning still names ``k`` (and not ``h``, which agrees), on both paths."""
    first, second, third = _in_canonical_order("a", "b", "c")
    inputs = (
        _one_row(first, 1, 1, origin="elsewhere"),
        _one_row(second, 1, 1),
        _one_row(third, 2, 1),
    )
    for order in itertools.permutations(inputs):
        result, warned = _rejoined(*order)
        assert result.as_table().num_rows == 0
        assert len(warned) == 1 and "tag 'k'" in warned[0] and "'h'" not in warned[0]
        rows, warned = _rejoined_async(*order)
        assert rows == []
        assert len(warned) == 1 and "tag 'k'" in warned[0] and "'h'" not in warned[0]
    # Nothing differs: nothing warns, although nothing is put together either.
    agreeing = (inputs[0], inputs[1], _one_row(third, 1, 1))
    for run in (_rejoined, _rejoined_async):
        assert run(*agreeing)[1] == []


def test_a_diamond_through_operators_never_warns() -> None:
    """Branches that changed no shared column: every row is put together, no warning."""
    source = _source(data_tags=True)
    a = source.select_data_columns(["subject", "unit", "rate"]).polars_filter(
        pl.col("rate") > 2
    )
    b = cast(
        Any, double.pod(source.select_data_columns(["subject", "unit", "rate"]))
    ).map_data({"doubled": "twice"})
    c = source.select_data_columns(["subject", "unit", "depth", "area"]).add_tags(
        cohort="A"
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", RejoinDifferingColumnWarning)
        for order in itertools.permutations((a, b, c)):
            assert Rejoin()(*order).as_table().num_rows == 5


# ---------------------------------------------------------------------------
# Data contexts
# ---------------------------------------------------------------------------


def test_rows_of_different_data_contexts_match_nothing(
    other_context: contexts.DataContext,
) -> None:
    """Rows of one origin row in different data contexts are not put together: the
    result is empty, nothing is refused, and the warning names the data context."""
    a = _in_context(other_context, "x")
    b = _in_context(contexts.get_default_context(), "y")
    for inputs in ((a, b), (b, a)):
        result, warned = _rejoined(*inputs)
        assert result.as_table().num_rows == 0
        assert len(warned) == 1 and "the data context" in warned[0]
    # Whatever the streams report: two lazy branches both report the default context.
    lazy_a = a.select_data_columns(["x"])
    lazy_b = b.select_data_columns(["y"])
    assert lazy_a.data_context_key == lazy_b.data_context_key
    for inputs in ((lazy_a, lazy_b), (lazy_b, lazy_a)):
        result, warned = _rejoined(*inputs)
        assert result.as_table().num_rows == 0 and len(warned) == 1


def test_rows_keep_their_own_data_context(other_context: contexts.DataContext) -> None:
    """Inputs holding rows of two data contexts: the rows whose contexts agree are put
    together, each keeping its own in the data context column."""
    default = contexts.get_default_context().context_key

    def mixed(keys: list[str], data: str) -> ArrowTableStream:
        table = pa.table(
            {
                "g": pa.array([1, 2, 3], pa.int64()),
                "h": pa.array(["a", "b", "c"], pa.large_string()),
                data: pa.array([0.5, 0.25, 0.125], pa.float64()),
                "origin": pa.array(["r1", "r2", "r3"], pa.large_string()),
                constants.CONTEXT_KEY: pa.array(keys, pa.large_string()),
            }
        )
        return ArrowTableStream(
            table, tag_columns=["g", "h"], system_tag_columns=["origin"]
        )

    a = mixed([OTHER_CONTEXT, default, OTHER_CONTEXT], "x")
    b = mixed([OTHER_CONTEXT, default, default], "y")
    for inputs in ((a, b), (b, a)):
        result, warned = _rejoined(*inputs)
        table = result.as_table(columns={"context": True})
        rows = dict(
            zip(
                table.column("g").to_pylist(),
                table.column(constants.CONTEXT_KEY).to_pylist(),
            )
        )
        assert rows == {1: OTHER_CONTEXT, 2: default}
        assert len(warned) == 1 and "the data context" in warned[0]
