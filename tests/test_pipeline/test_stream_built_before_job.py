"""Where a stream was built does not decide a node's column names (ITL-1470).

A chain of operators built before a job is opened, with a function pod over it added
inside the job, must give the job of the same chain built inside the ``with`` block: the
same nodes, the same system-tag column names, rows, content and pipeline hashes and stored
tables, on the sync and the async path. The stream outside any job has those column names
and hashes too.

Before ITL-1387 the stream built before the job became a source slot, which named its
system-tag columns by its own declared schema (ITL-1112) and had its own pipeline hash.
"""

from __future__ import annotations

import asyncio
import re
from collections import Counter
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource
from orcapod.system_constants import constants

# Stored metadata that differs on every run.
_RUN_DEPENDENT = {
    "__pod_ts",
    "__data_id",
    "__computed",
    "__recorded_at",
    "__pipeline_run_id",
}
# A result's random record id inside a source-info token differs on every run.
_RANDOM_ID = re.compile(r"::[0-9a-f]{32}::")
_ALL_COLUMNS = {"system_tags": True, "source": True}


@function_pod(output_keys=["total", "ratio"])
def combine(x: float, y: float) -> tuple[float, float]:
    """Two outputs from the two data columns of ``units``."""
    return x + y, x / y


def _units() -> DictSource:
    """Two tags and two data columns."""
    return DictSource(
        [
            {"site": "m1", "unit": "a", "x": 1.0, "y": 2.0},
            {"site": "m1", "unit": "b", "x": 3.0, "y": 4.0},
            {"site": "m2", "unit": "a", "x": 5.0, "y": 8.0},
        ],
        tag_columns=["site", "unit"],
        tag_only_columns=True,
        source_id="units",
    )


def _sessions() -> DictSource:
    """Shares both tags with ``units``; two data columns of its own."""
    return DictSource(
        [
            {"site": "m1", "unit": "a", "depth": 10.0, "gain": 0.5},
            {"site": "m1", "unit": "b", "depth": 20.0, "gain": 1.5},
            {"site": "m2", "unit": "a", "depth": 30.0, "gain": 2.5},
        ],
        tag_columns=["site", "unit"],
        tag_only_columns=True,
        source_id="sessions",
    )


CHAINS: dict[str, Callable[[], Any]] = {
    "select": lambda: _units().select_tag_columns(["site"]),
    "map": lambda: _units().map_tags({"site": "place", "unit": "cell"}),
    "select then map": lambda: (
        _units().select_tag_columns(["unit"]).map_tags({"unit": "cell"})
    ),
    "join": lambda: _units().join(_sessions()),
    "join then select then map": lambda: (
        _units()
        .join(_sessions())
        .select_tag_columns(["site"])
        .map_tags({"site": "place"})
    ),
}


def _system_tag_columns(names: list[str]) -> list[str]:
    return sorted(n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX))


def _rows(table: pa.Table, mode: str) -> Any:
    """Rows as given on the sync path; as a multiset on the async path."""
    keep = [name for name in table.column_names if name not in _RUN_DEPENDENT]
    rows = [
        _RANDOM_ID.sub("::<id>::", repr(sorted(row.items())))
        for row in table.select(keep).to_pylist()
    ]
    return rows if mode == "sync" else Counter(rows)


def _job_over(chain: Callable[[], Any], built: str, mode: str) -> dict[str, Any]:
    """Run a job with ``combine`` over the chain and return what is compared."""
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="job", database=database)
    if built == "before":
        stream = chain()
        with job:
            combine.pod(stream, label="combine")
    else:
        with job:
            combine.pod(chain(), label="combine")
    if mode == "sync":
        job.run()
    else:
        asyncio.run(job.run_async())

    computed = {
        label: node.as_table(columns=_ALL_COLUMNS)
        for label, node in job.nodes.items()
        if node.node_type != "source"
    }
    paths = sorted(database.list_table_paths(()))
    stored = {path: database.get_all_records(path) for path in paths}
    return {
        "node names": sorted(job.nodes),
        "node types": {label: node.node_type for label, node in job.nodes.items()},
        "content hashes": {
            label: node.content_hash().to_string() for label, node in job.nodes.items()
        },
        "pipeline hashes": {
            label: node.pipeline_hash().to_string() for label, node in job.nodes.items()
        },
        "column names": {
            label: sorted(table.column_names) for label, table in computed.items()
        },
        "system-tag column names": {
            label: _system_tag_columns(table.column_names)
            for label, table in computed.items()
        },
        "rows": {label: _rows(table, mode) for label, table in computed.items()},
        "stored paths": paths,
        "stored column names": {
            path: sorted(table.column_names) for path, table in stored.items()
        },
        "stored rows": {path: _rows(table, mode) for path, table in stored.items()},
    }


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("chain", list(CHAINS))
def test_a_chain_built_before_the_job_equals_the_chain_built_inside(
    chain: str, mode: str
) -> None:
    """Names, rows, hashes and stored tables do not follow where the chain was built."""
    before = _job_over(CHAINS[chain], "before", mode)
    inside = _job_over(CHAINS[chain], "inside", mode)

    for what, expected in inside.items():
        assert before[what] == expected, f"{chain}, {mode}: {what} differ"
    # The comparison is of something: the job holds the chain's steps and sources.
    assert "units" in before["node names"]
    assert len(before["node names"]) >= 3
    assert before["system-tag column names"]["combine"]


@pytest.mark.parametrize("built", ["before", "inside"])
@pytest.mark.parametrize("chain", list(CHAINS))
def test_the_job_node_has_the_names_and_hashes_of_the_plain_stream(
    chain: str, built: str
) -> None:
    """A job node equals the same step outside any job, wherever its input was built."""
    plain = combine.pod(CHAINS[chain]())
    job = _job_over(CHAINS[chain], built, "sync")

    plain_table = plain.as_table(columns=_ALL_COLUMNS)
    assert job["system-tag column names"]["combine"] == _system_tag_columns(
        plain_table.column_names
    )
    assert job["column names"]["combine"] == sorted(plain_table.column_names)
    assert job["content hashes"]["combine"] == plain.content_hash().to_string()
    assert job["pipeline hashes"]["combine"] == plain.pipeline_hash().to_string()
    assert Counter(job["rows"]["combine"]) == _rows(plain_table, "async")
