"""A function pod over data-tags has one identity for every order of its input's tags (ITL-1246).

Developer, November 18, 2026: "it shouldn't depend on tag order -- generally nothing should
depend on tag order". The call ``pod(stream)`` on a stream with data-tags builds a
``FallThroughPod`` that lets them fall through. Its identity used to hold the data-tags in
the input's tag order, so two sources that differ only in the order of ``tag_columns`` (or,
after an operator, in their table's physical layout) gave function pod streams and job
nodes with the same rows but different content hashes, pipeline hashes and pipeline table
paths. The data-tags now enter the identity as a set: their sorted names.

The output still lists the data-tags first, in the input's tag order (ITL-1205): column
order is presentation and enters no hash.

The inputs are the shared several-item family (``several_items.MAIN``: three tags, three
data columns), in all six tag orders, three table layouts, all-data-tag and mixed roles.
"""

from __future__ import annotations

import asyncio
import itertools
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

import orcapod as op
from orcapod.channels import Channel
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.function_pod import FallThroughPod, FunctionPod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from ..operators.several_items import (
    DATA_TAGS,
    LAYOUTS,
    MAIN,
    MIXED,
    TAGS_FIRST,
    Arrangement,
    Row,
    make_source,
)

TAG_ORDERS = tuple(itertools.permutations(MAIN.tags))
ROLES = pytest.mark.parametrize("roles", [DATA_TAGS, MIXED])
ORCHESTRATORS = pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)


@op.function_pod(output_keys="scaled")
def scaled(d1: int) -> int:
    """Read one data column; every data-tag of the input falls through."""
    return 3 * d1


def _source(order: tuple[str, ...], roles: str, layout: str = TAGS_FIRST) -> Any:  # pyright: ignore[reportExplicitAny]  # an ArrowTableSource
    return make_source(MAIN, Arrangement(order, layout, roles))


def _data_tags(roles: str) -> tuple[str, ...]:
    """The data-tags of ``MAIN`` in ``roles``, in name order."""
    return tuple(sorted(t for t in MAIN.tags if roles == DATA_TAGS or t != "t2"))


def _plain_pod() -> FunctionPod:
    pod = scaled.pod
    assert isinstance(pod, FunctionPod)
    return pod


def _row(tag: TagProtocol, data: DataProtocol) -> tuple[str, ...]:
    """One row, free of column order: values, system tags, row hashes, source info.

    The computed column's source-info token names the result record, a random id per
    computation (UUIDv7), so only its source id (before the first ``::``) is compared.
    """
    info = {
        name: token.split("::")[0]
        if name == "scaled" and isinstance(token, str)
        else token
        for name, token in data.source_info().items()
    }
    return (
        repr(sorted(tag.as_dict().items())),
        repr(sorted(data.as_dict().items())),
        repr(sorted(tag.system_tags().items())),
        tag.content_hash().to_string(),
        data.content_hash().to_string(),
        repr(sorted(info.items())),
    )


def _rows(rows: Sequence[Row]) -> list[tuple[str, ...]]:
    return sorted(_row(tag, data) for tag, data in rows)


def _ordered_rows(rows: Sequence[Row]) -> list[tuple[str, ...]]:
    """``_rows`` plus each row's column order, for comparing two paths of one input."""
    return sorted(
        (*_row(tag, data), repr(list(tag.keys())), repr(list(data.keys())))
        for tag, data in rows
    )


def _fingerprint(stream: StreamProtocol) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]  # mixed values
    """What must not depend on the input's tag order or layout."""
    tag_keys, data_keys = stream.keys(columns={"system_tags": True})
    return {
        "rows": _rows(list(stream.iter_data())),
        "content_hash": stream.content_hash().to_string(),
        "pipeline_hash": stream.pipeline_hash().to_string(),
        "tag_names": sorted(tag_keys),
        "data_names": sorted(data_keys),
    }


def _fall_through_pod(stream: StreamProtocol) -> FallThroughPod:
    producer = stream.producer
    assert isinstance(producer, FallThroughPod)
    return producer


# ---------------------------------------------------------------------------
# The pod
# ---------------------------------------------------------------------------


def test_the_fall_through_tags_are_a_set_of_names() -> None:
    """Every order of the names gives one pod: sorted names, one identity, one config."""
    pods = [FallThroughPod(_plain_pod(), order) for order in TAG_ORDERS]
    assert {pod.fall_through_tags for pod in pods} == {("t1", "t2", "t3")}
    assert len({pod.content_hash() for pod in pods}) == 1
    assert len({pod.pipeline_hash() for pod in pods}) == 1
    assert {tuple(pod.to_config()["fall_through_tags"]) for pod in pods} == {
        ("t1", "t2", "t3")
    }
    # A set of names is accepted as it is.
    assert FallThroughPod(_plain_pod(), {"t3", "t1"}).fall_through_tags == ("t1", "t3")


def test_a_rebuilt_pod_has_the_identity_of_the_pod_it_was_saved_from() -> None:
    """``from_config`` of a config listing the names in any order: the same pod."""
    config = FallThroughPod(_plain_pod(), ("t1", "t2", "t3")).to_config()
    reference = FallThroughPod(_plain_pod(), ("t1", "t2", "t3"))
    for order in TAG_ORDERS:
        rebuilt = FallThroughPod.from_config(
            {**config, "fall_through_tags": list(order)}
        )
        assert rebuilt.fall_through_tags == ("t1", "t2", "t3")
        assert rebuilt.content_hash() == reference.content_hash()
        assert rebuilt.pipeline_hash() == reference.pipeline_hash()


def test_the_data_tags_are_part_of_the_identity() -> None:
    """A set of names is still identity: two different sets give two pods."""
    assert (
        FallThroughPod(_plain_pod(), ("t1", "t2")).content_hash()
        != FallThroughPod(_plain_pod(), ("t1", "t3")).content_hash()
    )


# ---------------------------------------------------------------------------
# Streams: every tag order, every layout, sync and async
# ---------------------------------------------------------------------------


@ROLES
def test_the_call_has_one_identity_for_every_tag_order_and_layout(roles: str) -> None:
    """``pod(source)``: rows, content and pipeline hash equal for all 18 arrangements.

    The call adds an automatic select (``d2`` and ``d3`` are not parameters).
    """
    prints = {
        (order, layout): _fingerprint(scaled.pod(_source(order, roles, layout)))
        for order in TAG_ORDERS
        for layout in LAYOUTS
    }
    first = next(iter(prints.values()))
    assert all(p == first for p in prints.values())
    for order in TAG_ORDERS:
        stream = scaled.pod(_source(order, roles))
        assert _fall_through_pod(stream).fall_through_tags == _data_tags(roles)


@ROLES
def test_after_an_explicit_select_the_identity_is_one_for_every_layout(
    roles: str,
) -> None:
    """A select's output lists its tags in the table's layout: the pod does not follow it.

    The select keeps the data-tags' columns and ``d1``, so no automatic select is added.
    """
    prints = {
        (order, layout): _fingerprint(
            scaled.pod(
                _source(order, roles, layout).select_data_columns(
                    [*_data_tags(roles), "d1"]
                )
            )
        )
        for order in TAG_ORDERS
        for layout in LAYOUTS
    }
    first = next(iter(prints.values()))
    assert all(p == first for p in prints.values())


@ROLES
def test_the_output_lists_the_data_tags_first_in_the_inputs_tag_order(
    roles: str,
) -> None:
    """Presentation follows the input (ITL-1205): data-tags first, in its tag order."""
    for order in TAG_ORDERS:
        source = _source(order, roles)
        stream = scaled.pod(source)
        tag_keys, data_keys = stream.keys()
        expected = stream_data_tags(source)
        assert tag_keys == source.keys()[0]
        assert data_keys == (*expected, "scaled")
        assert stream.output_schema()[1].keys() == set(data_keys)
        assert tuple(stream.output_schema()[1]) == data_keys
        for _, data in stream.iter_data():
            assert tuple(data.keys()) == data_keys
        assert stream.as_table().column_names[-1] == "scaled"


async def _async_rows(stream: StreamProtocol) -> list[Row]:
    """The rows of the fall-through pod's ``async_execute`` over its input's rows."""
    (upstream,) = stream.upstreams
    pod = _fall_through_pod(stream)
    channel: Channel[Row] = Channel(buffer_size=64)
    output: Channel[Row] = Channel(buffer_size=64)
    for row in upstream.iter_data():
        await channel.writer.send(row)
    await channel.writer.close()
    await pod.async_execute([channel.reader], output.writer)
    return await output.reader.collect()


@ROLES
def test_the_async_path_gives_the_sync_rows_in_every_tag_order(roles: str) -> None:
    """Async rows equal the sync rows (column order included) and equal across orders."""
    seen = []
    for order in TAG_ORDERS:
        stream = scaled.pod(_source(order, roles))
        sync = list(stream.iter_data())
        assert _ordered_rows(asyncio.run(_async_rows(stream))) == _ordered_rows(sync)
        # Across orders only the order-free part must be equal.
        seen.append(_rows(sync))
    assert all(rows == seen[0] for rows in seen)


# ---------------------------------------------------------------------------
# Jobs: the same nodes, table paths and record ids for every tag order
# ---------------------------------------------------------------------------


def _job_record(
    order: tuple[str, ...], roles: str, orchestrator: Any
) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]  # an orchestrator class
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="tag_order", database=db)
    with job:
        scaled.pod(_source(order, roles), label="scaled")
    job.run(orchestrator=orchestrator())
    node = job.nodes["scaled"]
    paths = sorted(db.list_sources())
    record: dict[str, Any] = {
        "content_hash": node.content_hash().to_string(),
        "pipeline_hash": node.pipeline_hash().to_string(),
        "table_paths": ["/".join(p) for p in paths],
        "rows": _fingerprint(node)["rows"],
    }
    for path in paths:
        name = "/".join(path)
        records = db.get_all_records(path, record_id_column="__record_id")
        assert records is not None
        if "_result" in path:
            # Result record ids are random (UUIDv7): the lookup key is compared.
            record[name] = sorted(
                str(v) for v in records.column("__input_data_hash").to_pylist()
            )
        else:
            record[name] = sorted(
                str(v) for v in records.column("__record_id").to_pylist()
            )
    return record


@ORCHESTRATORS
@ROLES
def test_a_job_has_the_same_nodes_and_table_paths_for_every_tag_order(
    roles: str, orchestrator: Any
) -> None:
    """Node hashes, every table path, stored record ids and rows: one value for 6 orders."""
    records = [_job_record(order, roles, orchestrator) for order in TAG_ORDERS]
    assert all(record == records[0] for record in records)


def test_the_sync_and_async_jobs_store_the_same_tables() -> None:
    sync = _job_record(TAG_ORDERS[-1], MIXED, SyncPipelineOrchestrator)
    async_ = _job_record(TAG_ORDERS[-1], MIXED, AsyncPipelineOrchestrator)
    assert sync == async_


@ROLES
def test_a_saved_job_states_the_data_tags_as_a_set_and_loads_with_its_hashes(
    tmp_path: Path, roles: str
) -> None:
    """The file lists the fall-through tags sorted; every order loads to one identity."""
    loaded_hashes = set()
    for i, order in enumerate(TAG_ORDERS):
        job = PipelineJob(
            name="tag_order", database=DeltaTableDatabase(tmp_path / f"s{i}")
        )
        with job:
            scaled.pod(_source(order, roles), label="scaled")
        job.run()
        path = tmp_path / f"job{i}.json"
        job.save(path)
        saved = json.loads(path.read_text())
        (descriptor,) = [
            d
            for d in saved["pipeline"]["nodes"].values()
            if d["node_type"] == "function"
        ]
        assert descriptor["fall_through_tags"] == list(_data_tags(roles))
        loaded = PipelineJob.load(path)
        node = loaded.nodes["scaled"]
        assert node.content_hash() == job.nodes["scaled"].content_hash()
        assert node.pipeline_hash() == job.nodes["scaled"].pipeline_hash()
        loaded_hashes.add((node.content_hash(), node.pipeline_hash()))
    assert len(loaded_hashes) == 1
