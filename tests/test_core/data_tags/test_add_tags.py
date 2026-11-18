"""``AddTags`` on a stream with data-tags (ITL-1196, ITL-1023).

The input's data-tags stay data-tags: one column in both roles, with its values and
source info. The added tags are plain: a constant tag is not a data value, so it never
becomes a data column. A new tag named like any column of the input (a tag, a data
column or a data-tag) is refused, as for plain tags. Sync and async agree
(``test_async_sync_equivalence.py``); the order of the data keys follows ITL-1205.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.operators import AddTags
from orcapod.errors import InputValidationError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode

ROWS = [
    {"unit": 1, "time": 10, "amp": 0.5},
    {"unit": 2, "time": 20, "amp": 0.25},
]


def _spikes(**kwargs: object) -> op.sources.DictSource:
    return op.sources.DictSource(
        ROWS,
        tag_columns=["unit", "time"],
        source_id="spikes",
        **kwargs,  # pyright: ignore[reportArgumentType]  # forwarded options
    )


def test_the_data_tags_stay_and_the_new_tags_are_plain() -> None:
    tagged = _spikes().add_tags(cohort="A", run=3)
    assert tagged.keys() == (
        ("unit", "time", "cohort", "run"),
        ("unit", "time", "amp"),
    )
    assert stream_data_tags(tagged) == ("unit", "time")
    for tag, data in tagged.iter_data():
        assert tag["unit"] == data["unit"] and tag["time"] == data["time"]
        assert tag["cohort"] == "A" and tag["run"] == 3
        assert "cohort" not in data.keys()


def test_a_mixed_stream_keeps_its_plain_tags_plain() -> None:
    tagged = _spikes(tag_only_columns=["time"]).add_tags(cohort="A")
    assert tagged.keys() == (("unit", "time", "cohort"), ("unit", "amp"))
    assert stream_data_tags(tagged) == ("unit",)


def test_values_and_source_info_are_unchanged() -> None:
    source = _spikes()
    tagged = source.add_tags(cohort="A")
    before = {(d["unit"], d["time"]): d.source_info() for _, d in source.iter_data()}
    after = {(d["unit"], d["time"]): d.source_info() for _, d in tagged.iter_data()}
    assert after == before


def test_the_prediction_is_the_result() -> None:
    source = _spikes()
    add = AddTags({"cohort": "A"})
    computed = add.static_process(source)
    assert add.output_schema(source) == computed.output_schema()
    assert list(add.output_schema(source)[1]) == list(computed.output_schema()[1])
    assert computed.keys() == source.add_tags(cohort="A").keys()


@pytest.mark.parametrize("name", ["unit", "amp"], ids=["data-tag", "data"])
def test_a_name_of_an_existing_column_is_refused(name: str) -> None:
    with pytest.raises(InputValidationError, match="existing column's name"):
        _spikes().add_tags({name: "A"})


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
@pytest.mark.parametrize("mode", [CacheMode.OFF, CacheMode.LOG], ids=["off", "log"])
def test_a_job_keeps_the_data_tags(
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
    mode: CacheMode,
) -> None:
    store = InMemoryArrowDatabase()
    job = PipelineJob(name="add", database=store)
    with job:
        _ = _spikes().add_tags(cohort="A", label="added")
    node = job.nodes["added"]
    node.attach_databases(pipeline_database=store.at("add"), cache_mode=mode)
    job.run(orchestrator=orchestrator())
    assert node.keys() == (("unit", "time", "cohort"), ("unit", "time", "amp"))
    rows = sorted(node.as_table().to_pylist(), key=lambda r: r["unit"])
    assert rows == [
        {"unit": 1, "time": 10, "cohort": "A", "amp": 0.5},
        {"unit": 2, "time": 20, "cohort": "A", "amp": 0.25},
    ]
    for tag, data in node.iter_data():
        assert tag["unit"] == data["unit"]
        assert tuple(data.keys()) == ("unit", "time", "amp")


def test_a_function_downstream_reads_the_data_tag() -> None:
    @op.function_pod(output_keys=["scaled"])
    def scale(unit: int, amp: float) -> float:
        return unit * amp

    out = scale.pod(_spikes().add_tags(cohort="A"))
    assert out.keys() == (("unit", "time", "cohort"), ("unit", "time", "scaled"))
    values = sorted(r["scaled"] for r in out.as_table().to_pylist())
    assert values == [0.5, 0.5]
    assert pa.types.is_floating(out.as_table().schema.field("scaled").type)
