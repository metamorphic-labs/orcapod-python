"""``AddTags`` is the same operator for every order of its keywords (ITL-1269).

Developer, November 18, 2026: "I absolutely agree to make it entry order independent",
under the rule "it shouldn't depend on tag order -- generally nothing should depend on tag
order". The order of ``add_tags(cohort="A", run=3)`` only placed the new columns, yet it
was part of the operator's identity: two keyword orders gave two content and pipeline
hashes, two stored paths and two system-tag column names downstream. Now the new tags are
a set: the operator holds them in name order, its identity and config list them that way,
and they are placed in name order after the input's tags (as ``TagBy`` places its new
tags, ITL-1226).

Five new tags of every accepted type are given in all 120 keyword orders, on the shared
several-item inputs (three tags, three data columns; plain, data-tag and mixed roles), on
the sync and the async path and in jobs on both orchestrators.
"""

from __future__ import annotations

import asyncio
import itertools
from typing import Any

import pytest

from orcapod.core.operators import AddTags
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)

from .several_items import (
    MAIN,
    ROLES,
    TAGS_FIRST,
    Arrangement,
    async_rows,
    fingerprint,
    make_source,
    ordered_rows_fingerprint,
)

# One value of each accepted type; the names are not in name order as given.
NEW_TAGS: dict[str, Any] = {
    "run": 3,
    "cohort": "A",
    "weight": 1.5,
    "ok": True,
    "blob": b"\x01",
}
NAME_ORDER = ("blob", "cohort", "ok", "run", "weight")
KEYWORD_ORDERS = [
    {name: NEW_TAGS[name] for name in order}
    for order in itertools.permutations(NEW_TAGS)
]
# A few keyword orders for the heavier checks (streams, jobs).
SOME_ORDERS = [KEYWORD_ORDERS[0], KEYWORD_ORDERS[-1], KEYWORD_ORDERS[57]]
ORCHESTRATORS = pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)


def _source(roles: str) -> Any:  # pyright: ignore[reportExplicitAny]  # an ArrowTableSource
    return make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, roles))


def test_every_keyword_order_is_one_operator() -> None:
    """All 120 orders: one content hash, one pipeline hash, one config, name order."""
    operators = [AddTags(tags) for tags in KEYWORD_ORDERS]
    assert len({op.content_hash() for op in operators}) == 1
    assert len({op.pipeline_hash() for op in operators}) == 1
    assert {tuple(op.tags) for op in operators} == {NAME_ORDER}
    assert {
        tuple(entry["name"] for entry in op.to_config()["config"]["tags"])
        for op in operators
    } == {NAME_ORDER}


def test_a_saved_config_in_any_order_rebuilds_the_same_operator() -> None:
    reference = AddTags(NEW_TAGS)
    config = reference.to_config()
    entries = config["config"]["tags"]
    for order in itertools.permutations(entries):
        rebuilt = AddTags.from_config({**config, "config": {"tags": list(order)}})
        assert rebuilt.content_hash() == reference.content_hash()
        assert tuple(rebuilt.tags) == NAME_ORDER


def test_the_names_and_values_still_count() -> None:
    """A set of items is still identity: another name or value is another operator."""
    reference = AddTags(NEW_TAGS).content_hash()
    assert AddTags({**NEW_TAGS, "run": 4}).content_hash() != reference
    assert AddTags({**NEW_TAGS, "extra": 0}).content_hash() != reference
    assert AddTags({k: v for k, v in NEW_TAGS.items() if k != "ok"}).content_hash() != (
        reference
    )


@pytest.mark.parametrize("roles", ROLES)
def test_the_stream_is_the_same_for_every_keyword_order(roles: str) -> None:
    """Rows, hashes and column names equal; the new tags placed in name order."""
    source = _source(roles)
    prints = []
    for tags in SOME_ORDERS:
        stream = source.add_tags(**tags)
        prints.append(fingerprint(stream))
        tag_keys, _ = stream.keys()
        assert tag_keys == (*source.keys()[0], *NAME_ORDER)
        for tag, _ in stream.iter_data():
            assert tuple(tag.keys()) == tag_keys
    assert all(p == prints[0] for p in prints)


@pytest.mark.parametrize("roles", ROLES)
def test_the_async_path_gives_the_sync_rows_for_every_keyword_order(roles: str) -> None:
    source = _source(roles)
    for tags in SOME_ORDERS:
        operator = AddTags(tags)
        sync = ordered_rows_fingerprint(list(operator(source).iter_data()))
        assert ordered_rows_fingerprint(async_rows(operator, source)) == sync


def _job_record(tags: dict[str, Any], roles: str, orchestrator: Any) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]  # an orchestrator class
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="add_tags_order", database=db)
    with job:
        _source(roles).add_tags(label="tagged", **tags).drop_nulls(label="after")
    job.run(orchestrator=orchestrator())
    record: dict[str, Any] = {
        "table_paths": sorted("/".join(p) for p in db.list_sources()),
    }
    for label in sorted(job.nodes):
        node = job.nodes[label]
        record[label] = (
            node.content_hash().to_string(),
            node.pipeline_hash().to_string(),
            fingerprint(node)["rows"],
        )
    return record


@ORCHESTRATORS
@pytest.mark.parametrize("roles", ROLES)
def test_a_job_has_the_same_nodes_and_paths_for_every_keyword_order(
    roles: str, orchestrator: Any
) -> None:
    records = [_job_record(tags, roles, orchestrator) for tags in SOME_ORDERS]
    assert all(record == records[0] for record in records)


def test_the_async_runs_match_the_sync_runs() -> None:
    assert asyncio.iscoroutinefunction(AddTags.async_execute)
    tags = KEYWORD_ORDERS[-1]
    assert _job_record(tags, ROLES[2], SyncPipelineOrchestrator) == _job_record(
        tags, ROLES[2], AsyncPipelineOrchestrator
    )
