"""A job saved before data-tags loads with its v0.3 meaning (ITL-1023).

The fixture ``fixtures/saved_job_37716c76`` was written by Orcapod at commit 37716c76
(``main`` before data-tags, after the v1 hash definition of ITL-1154; job format
``0.2.0``, the format v0.3.0 writes) by ``fixtures/make_saved_job_37716c76.py``: a
``CSVSource`` with tag ``subject`` feeding one function node, run on a Delta store. Files
at formats ``0.1.0`` and ``0.2.0`` hold v0.3 sources, whose declared tags are tag-only;
loading must rebuild them that way, so every saved hash is reproduced.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from orcapod.pipeline import PipelineJob
from orcapod.pipeline.serialization import (
    PIPELINE_FORMAT_VERSION,
    PIPELINE_JOB_FORMAT_VERSION,
    SUPPORTED_FORMAT_VERSIONS,
    SUPPORTED_JOB_FORMAT_VERSIONS,
    resolve_database_from_config,
    source_config_at_version,
)
from orcapod.sources import CSVSource

FIXTURE = Path(__file__).parent / "fixtures" / "saved_job_37716c76"


@pytest.fixture
def saved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Copy the fixture into a working directory (its paths are relative) and read it."""
    shutil.copytree(FIXTURE, tmp_path / "job")
    monkeypatch.chdir(tmp_path / "job")
    return json.loads(Path("job.json").read_text())


def test_format_versions() -> None:
    assert PIPELINE_FORMAT_VERSION == PIPELINE_JOB_FORMAT_VERSION == "0.3.0"
    assert {"0.1.0", "0.2.0", "0.3.0"} == SUPPORTED_FORMAT_VERSIONS
    assert {"0.1.0", "0.2.0", "0.3.0"} == SUPPORTED_JOB_FORMAT_VERSIONS


def test_a_v03_job_loads_with_unchanged_hashes(saved: dict[str, Any]) -> None:
    assert saved["orcapod_pipeline_job_version"] == "0.2.0"
    job = PipelineJob.load(
        "job.json",
        database=resolve_database_from_config(saved["bindings"]["database"]),
    )
    nodes = saved["pipeline"]["nodes"]
    older = next(node for node in nodes.values() if node["label"] == "older")
    # A job node's content hash is the one its saved run recorded.
    job_hashes = saved["run"]["node_job_content_hashes"]
    assert job.nodes["older"].content_hash().to_string() == job_hashes["older"]
    assert job.nodes["older"].pipeline_hash().to_string() == older["pipeline_hash"]
    # The source is rebuilt from its saved binding: its hashes are the saved ones.
    binding = saved["bindings"]["sources"]["subjects"]
    assert job.nodes["subjects"].content_hash().to_string() == binding["content_hash"]
    assert job.nodes["subjects"].pipeline_hash().to_string() == binding["pipeline_hash"]
    # The source keeps its tag plain, as v0.3 built it.
    assert job.nodes["subjects"].keys() == (("subject",), ("age",))
    records = job.nodes["older"].get_all_records()
    assert records is not None
    assert sorted(records.column("older").to_pylist()) == [4, 6]


def test_a_v03_source_config_rebuilds_the_v03_source(saved: dict[str, Any]) -> None:
    config = saved["bindings"]["sources"]["subjects"]
    assert "tag_only_columns" not in config
    # The file's version (0.1.0) states that every tag is plain (ITL-1019).
    upgraded = source_config_at_version(config, saved["orcapod_pipeline_job_version"])
    assert upgraded["tag_only_columns"] is True
    source = CSVSource.from_config(upgraded)
    assert source.keys() == (("subject",), ("age",))
    assert source.content_hash().to_string() == config["content_hash"]
    assert source.pipeline_hash().to_string() == config["pipeline_hash"]
