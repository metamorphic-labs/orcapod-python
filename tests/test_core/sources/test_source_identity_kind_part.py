"""A source's identity is its source id, schema and key columns; the id names the kind.

ITL-1425 (developer, November 23, 2026): every source id is ``<kind>_v<N>:<name>`` and the
class name is not part of a source's content hash. Two sources that present one full id over
the same rows, schema and key columns are one source, whichever class reads them; a
different kind part is a different source. Saved configs keep the name and the version
apart.
"""

from __future__ import annotations

import csv
import warnings
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceVersionWarning
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource, CSVSource

# Two tags and two data columns (several-items rule), with two layouts of the table.
SCHEMA = pa.schema(
    [
        pa.field("subject", pa.large_string(), nullable=False),
        pa.field("session", pa.int64(), nullable=False),
        pa.field("weight", pa.float64()),
        pa.field("count", pa.int64()),
    ]
)
ROWS = [
    {"subject": "s1", "session": 1, "weight": 25.5, "count": 3},
    {"subject": "s1", "session": 2, "weight": 22.0, "count": 5},
    {"subject": "s2", "session": 1, "weight": 27.25, "count": 7},
]
TAG_ORDERS = [["subject", "session"], ["session", "subject"]]
KEY = ["subject", "session"]


def _table(column_order: list[str] | None = None) -> pa.Table:
    table = pa.Table.from_pylist(ROWS, schema=SCHEMA)
    return table.select(column_order) if column_order else table


class OtherArrowTableSource(ArrowTableSource):
    """Another class reading the same kind with the same implementation version."""

    SOURCE_KIND: ClassVar[str] = "arrow_table"
    SOURCE_VERSION: ClassVar[int] = 1


class ArrowTableSourceV2(ArrowTableSource):
    """The same kind under a newer implementation version."""

    SOURCE_KIND: ClassVar[str] = "arrow_table"
    SOURCE_VERSION: ClassVar[int] = 2


def _arrow(cls: type[ArrowTableSource], tags: list[str], layout: list[str] | None):
    return cls(
        _table(layout),
        tag_columns=tags,
        record_id_columns=KEY,
        tag_only_columns=True,
        source_id="subjects",
        label="subjects",
    )


def _csv(path: Path, tags: list[str]) -> CSVSource:
    return CSVSource(
        str(path),
        tag_columns=tags,
        record_id_columns=KEY,
        tag_only_columns=True,
        label="subjects",
        source_id="subjects",
        schema=SCHEMA,
    )


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "subjects.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SCHEMA.names))
        writer.writeheader()
        writer.writerows(ROWS)
    return path


LAYOUTS = [None, ["count", "weight", "session", "subject"]]


class TestIdentityIsTheIdNotTheClass:
    @pytest.mark.parametrize("tags", TAG_ORDERS)
    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_one_full_id_is_one_source_whatever_the_class(self, tags, layout):
        base = _arrow(ArrowTableSource, TAG_ORDERS[0], None)
        other = _arrow(OtherArrowTableSource, tags, layout)
        assert other.source_id == base.source_id == "arrow_table_v1:subjects"
        assert other.content_hash() == base.content_hash()
        assert other.pipeline_hash() == base.pipeline_hash()
        assert "OtherArrowTableSource" not in other.identity_structure()

    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_another_kind_part_is_another_source(self, csv_path, tags):
        arrow = _arrow(ArrowTableSource, tags, None)
        from_csv = _csv(csv_path, tags)
        newer = _arrow(ArrowTableSourceV2, tags, None)
        assert from_csv.source_id == "csv_v1:subjects"
        assert newer.source_id == "arrow_table_v2:subjects"
        hashes = {s.content_hash() for s in (arrow, from_csv, newer)}
        assert len(hashes) == 3
        # The schema is the same: one pipeline hash, one set of system-tag columns.
        assert len({s.pipeline_hash() for s in (arrow, from_csv, newer)}) == 1

    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_record_ids_are_the_same_and_tokens_differ_by_the_kind_part(
        self, csv_path, tags
    ):
        columns = {"system_tags": True, "source": True}
        arrow = _arrow(ArrowTableSource, tags, None).as_table(columns=columns)
        from_csv = _csv(csv_path, tags).as_table(columns=columns)
        (arrow_rid,) = [c for c in arrow.column_names if c.startswith("_tag_record_id")]
        (csv_rid,) = [
            c for c in from_csv.column_names if c.startswith("_tag_record_id")
        ]
        assert arrow_rid == csv_rid
        assert (
            arrow.column(arrow_rid).to_pylist() == from_csv.column(csv_rid).to_pylist()
        )
        for column in ("weight", "count"):
            arrow_tokens = arrow.column(f"_source_{column}").to_pylist()
            csv_tokens = from_csv.column(f"_source_{column}").to_pylist()
            assert [
                str(t).replace("arrow_table_v1:", "csv_v1:", 1) for t in arrow_tokens
            ] == csv_tokens


@function_pod(output_keys="ratio")
def ratio(weight: float | None, count: int | None) -> float | None:
    if weight is None or not count:
        return None
    return weight / count


ORCHESTRATORS = [None, AsyncPipelineOrchestrator()]


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS, ids=["sync", "async"])
@pytest.mark.parametrize("tags", TAG_ORDERS)
class TestInAJob:
    def _run(self, db, source, orchestrator):
        job = PipelineJob(name="kinds", database=db)
        with job:
            ratio.pod(source, label="ratio")
        job.run(orchestrator=orchestrator)
        return job

    def test_one_full_id_shares_the_node_and_its_stored_rows(self, tags, orchestrator):
        db = InMemoryArrowDatabase()
        first = self._run(db, _arrow(ArrowTableSource, tags, None), orchestrator)
        second = self._run(
            db, _arrow(OtherArrowTableSource, tags, LAYOUTS[1]), orchestrator
        )
        assert (
            first.nodes["ratio"].content_hash() == second.nodes["ratio"].content_hash()
        )
        # All fixtures assign the same slot label across source kinds.
        assert set(first.sources) == set(second.sources) == {"subjects"}
        stored = second.nodes["ratio"].as_source().as_table()
        assert stored.num_rows == len(ROWS)

    def test_another_kind_part_is_another_node_content_hash(
        self, csv_path, tags, orchestrator
    ):
        db = InMemoryArrowDatabase()
        first = self._run(db, _arrow(ArrowTableSource, tags, None), orchestrator)
        second = self._run(db, _csv(csv_path, tags), orchestrator)
        assert (
            first.nodes["ratio"].content_hash() != second.nodes["ratio"].content_hash()
        )
        assert set(second.sources) == {"subjects"}
        # Results are memoized by input data, so each kind's rows read the same values.
        assert sorted(
            first.nodes["ratio"].as_table().column("ratio").to_pylist()
        ) == sorted(second.nodes["ratio"].as_table().column("ratio").to_pylist())


class TestSavedConfig:
    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_name_and_version_are_saved_apart_and_round_trip(self, csv_path, tags):
        source = _csv(csv_path, tags)
        config = source.to_config()
        assert config["source_type"] == "csv"
        assert config["source_name"] == "subjects"
        assert config["source_version"] == 1
        assert "source_id" not in config
        rebuilt = CSVSource.from_config(config)
        assert rebuilt.source_id == source.source_id
        assert rebuilt.content_hash() == source.content_hash()

    def test_a_default_name_round_trips(self, csv_path):
        source = CSVSource(str(csv_path), tag_columns=KEY, tag_only_columns=True)
        rebuilt = CSVSource.from_config(source.to_config())
        assert rebuilt.source_id == source.source_id == f"csv_v1:{csv_path}"

    def test_another_saved_version_loads_as_the_installed_one_and_warns(self, csv_path):
        config = _csv(csv_path, KEY).to_config()
        config["source_version"] = 2
        with pytest.warns(SourceVersionWarning, match="version 2"):
            rebuilt = CSVSource.from_config(config)
        assert rebuilt.source_id == "csv_v1:subjects"

    def test_the_same_version_does_not_warn(self, csv_path):
        config = _csv(csv_path, KEY).to_config()
        with warnings.catch_warnings():
            warnings.simplefilter("error", SourceVersionWarning)
            CSVSource.from_config(config)

    def test_a_config_saved_before_the_change_is_refused(self, csv_path):
        config = _csv(csv_path, KEY).to_config()
        del config["source_name"]
        del config["source_version"]
        config["source_id"] = "subjects"
        with pytest.raises(ValueError, match="source_name"):
            CSVSource.from_config(config)

    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_a_saved_job_keeps_ids_and_slot_names(self, tmp_path, csv_path, tags):
        job = PipelineJob(name="saved", database=InMemoryArrowDatabase())
        with job:
            ratio.pod(_csv(csv_path, tags), label="ratio")
        job.run()
        path = tmp_path / "job.json"
        job.save(path)
        loaded = PipelineJob.load(path, database=InMemoryArrowDatabase())
        assert set(loaded.sources) == {"subjects"}
        reloaded = loaded.sources["subjects"]
        assert isinstance(reloaded, CSVSource)
        assert reloaded.source_id == "csv_v1:subjects"
        assert loaded.nodes["ratio"].content_hash() == job.nodes["ratio"].content_hash()
