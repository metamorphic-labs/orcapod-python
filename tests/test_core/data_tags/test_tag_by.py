"""``TagBy`` / ``stream.tag_by``: data columns become data-tags (ITL-1175).

A named data column becomes a data-tag: one column, a tag and the data column it
was, with its values and source info unchanged. The new tags follow the input's tags in
the order given. Every operator is driven through its public call and read through the
stream's public methods; a schema prediction is checked against ``static_process``.
"""

from __future__ import annotations

import datetime
import decimal
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.operators import TagBy
from orcapod.core.data_tags import (
    DataTagMaterializationWarning,
    stored_data_tags,
    stream_data_tags,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline.serialization import resolve_operator_from_config
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.system_constants import OPERATOR_DB_SCHEMA_VERSION, constants
from orcapod.types import CacheMode

ORCHESTRATORS = pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)

ROWS = [
    {"unit": 1, "trial": 10, "amp": 0.5, "label": "a"},
    {"unit": 2, "trial": 20, "amp": 1.5, "label": "b"},
]


def _plain() -> op.sources.DictSource:
    """``unit`` a plain tag; ``trial``, ``amp`` and ``label`` data."""
    return op.sources.DictSource(
        ROWS, tag_columns=["unit"], tag_only_columns=True, source_id="spikes"
    )


def _data_tag() -> op.sources.DictSource:
    """``unit`` a data-tag; ``trial``, ``amp`` and ``label`` data."""
    return op.sources.DictSource(ROWS, tag_columns=["unit"], source_id="spikes")


@op.function_pod(output_keys=["session"])
def session_of(trial: int) -> str:
    """A computed value later steps key on."""
    return f"s{trial // 20}"


def _sorted_rows(table: pa.Table) -> list[dict[str, Any]]:
    return sorted(table.to_pylist(), key=lambda row: repr(sorted(row.items())))


# ── what the output is ──────────────────────────────────────────────────────


def test_named_data_columns_become_data_tags_in_the_order_given() -> None:
    tagged = _plain().tag_by("label", "trial")
    assert tagged.keys() == (
        ("unit", "label", "trial"),
        ("trial", "amp", "label"),
    )
    assert stream_data_tags(tagged) == ("label", "trial")
    for tag, data in tagged.iter_data():
        assert tag["label"] == data["label"] and tag["trial"] == data["trial"]


def test_the_column_is_stored_once_among_the_tags() -> None:
    table = _plain().tag_by("trial").as_table()
    assert table.column_names == ["unit", "trial", "amp", "label"]


def test_values_system_tags_and_source_info_are_unchanged() -> None:
    source = _plain()
    tagged = source.tag_by("trial")
    before = source.as_table(all_info=True)
    after = tagged.as_table(all_info=True)
    unchanged = [
        name
        for name in before.column_names
        if name.startswith((constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX))
        or name in ("unit", "trial", "amp", "label")
    ]
    assert set(unchanged) <= set(after.column_names)
    assert _sorted_rows(after.select(unchanged)) == _sorted_rows(
        before.select(unchanged)
    )
    # The data's types and nullability are the input's.
    for name in ("trial", "amp", "label"):
        assert after.schema.field(name) == before.schema.field(name)


def test_an_existing_data_tag_is_kept() -> None:
    tagged = _data_tag().tag_by("trial")
    assert stream_data_tags(tagged) == ("unit", "trial")
    assert tagged.keys() == (("unit", "trial"), ("unit", "trial", "amp", "label"))


def test_names_as_strings_or_one_sequence_are_one_operator() -> None:
    source = _plain()
    by_strings = source.tag_by("trial", "label")
    by_list = source.tag_by(["trial", "label"])
    by_tuple = source.tag_by(("trial", "label"))
    assert (
        by_strings.content_hash() == by_list.content_hash() == by_tuple.content_hash()
    )
    assert by_strings.pipeline_hash() == by_list.pipeline_hash()


def test_the_order_of_the_names_is_part_of_the_identity() -> None:
    source = _plain()
    forward = source.tag_by("trial", "label")
    backward = source.tag_by("label", "trial")
    assert forward.keys()[0] == ("unit", "trial", "label")
    assert backward.keys()[0] == ("unit", "label", "trial")
    assert forward.content_hash() != backward.content_hash()
    assert forward.pipeline_hash() != backward.pipeline_hash()
    assert TagBy(["trial", "label"]).identity_structure() == (
        "TagBy",
        ["trial", "label"],
    )


@pytest.mark.parametrize("all_info", [False, True])
def test_the_schema_is_predicted(all_info: bool) -> None:
    source = _data_tag()
    tag_by = TagBy(["label", "trial"])
    predicted = tag_by.output_schema(source, all_info=all_info)
    assert predicted == tag_by.static_process(source).output_schema(all_info=all_info)
    tag_schema, data_schema = predicted
    assert list(tag_schema)[:3] == ["unit", "label", "trial"]
    assert data_schema == source.output_schema()[1]


def test_the_data_order_is_the_inputs_and_the_predicted_one() -> None:
    """The named columns stay where they are: tagging reorders no data column.

    The order of ``keys()`` and of the data schema of the computed stream
    (``static_process``) equals the input's and the predicted one, also when the
    names are given in another order than the data (CodeRabbit on PR #420).
    """
    source = _data_tag()
    tag_by = TagBy(["label", "trial"])
    computed = tag_by.static_process(source)
    _, input_data = source.keys()
    assert input_data == ("unit", "trial", "amp", "label")
    assert computed.keys() == (("unit", "label", "trial"), input_data)
    assert list(computed.output_schema()[1]) == list(input_data)
    assert list(tag_by.output_schema(source)[1]) == list(input_data)
    assert source.tag_by("label", "trial").keys() == computed.keys()


def test_each_row_keeps_its_data_context_key() -> None:
    """The context column passes through: no row takes the default key instead."""
    source = _plain()
    before = source.as_table(columns={"context": True})
    after = TagBy(["trial"]).static_process(source).as_table(columns={"context": True})
    assert (
        after.column(constants.CONTEXT_KEY).to_pylist()
        == before.column(constants.CONTEXT_KEY).to_pylist()
    )


def test_zero_rows_have_the_types_of_rows() -> None:
    source = _plain()
    tagged = source.tag_by("trial")
    empty = source.polars_filter(unit=99).tag_by("trial")
    assert empty.as_table().schema == tagged.as_table().schema
    assert empty.as_table().num_rows == 0


def test_the_config_round_trips_and_is_registered_for_loading() -> None:
    tag_by = TagBy(["label", "trial"])
    config = tag_by.to_config()
    assert config["class_name"] == "TagBy"
    assert config["config"] == {"columns": ["label", "trial"]}
    for loaded in (TagBy.from_config(config), resolve_operator_from_config(config)):
        assert isinstance(loaded, TagBy)
        assert loaded.columns == ("label", "trial")
        assert loaded.content_hash() == tag_by.content_hash()
        assert loaded.pipeline_hash() == tag_by.pipeline_hash()


# ── after a function pod: a computed column keys a join and a group ─────────


def test_a_computed_column_becomes_a_tag_and_keys_a_join() -> None:
    computed = TagBy("session")(session_of.pod(_data_tag()))
    assert computed.keys() == (("unit", "session"), ("unit", "session"))
    sessions = op.sources.DictSource(
        [{"session": "s0", "room": "A"}, {"session": "s1", "room": "B"}],
        tag_columns=["session"],
        source_id="sessions",
    )
    joined = computed.join(sessions)
    assert set(stream_data_tags(joined)) == {"unit", "session"}
    rows = sorted(joined.as_table().to_pylist(), key=lambda r: r["unit"])
    assert [(r["unit"], r["session"], r["room"]) for r in rows] == [
        (1, "s0", "A"),
        (2, "s1", "B"),
    ]


def test_a_computed_column_becomes_a_tag_and_keys_a_group() -> None:
    grouped = TagBy("session")(session_of.pod(_data_tag())).group_by(["session"])
    assert grouped.keys() == (("session",), ("unit", "session"))
    rows = sorted(grouped.as_table().to_pylist(), key=lambda r: r["session"])
    assert [(r["session"], r["unit"]) for r in rows] == [("s0", [1]), ("s1", [2])]


def test_a_join_with_a_data_only_column_of_that_name_is_refused() -> None:
    other = op.sources.DictSource(
        [{"k": 1, "label": "a"}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="o",
    )
    with pytest.raises(InputValidationError, match="tag_by"):
        _ = _plain().tag_by("label").join(other)


def test_removing_the_data_role_afterwards_leaves_a_plain_tag_with_the_warning() -> (
    None
):
    tagged = _plain().tag_by("label")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        plain = tagged.drop_data_columns(["label"])
    assert any(w.category is DataTagMaterializationWarning for w in caught)
    assert plain.keys() == (("unit", "label"), ("trial", "amp"))


def test_a_nullable_column_is_tagged_after_drop_nulls() -> None:
    table = pa.table(
        {"unit": pa.array([1, 2]), "label": pa.array(["a", None], pa.large_string())}
    )
    source = op.sources.ArrowTableSource(
        table, tag_columns=["unit"], tag_only_columns=True, source_id="n"
    )
    with pytest.raises(InputValidationError, match="drop_nulls"):
        _ = source.tag_by("label")
    tagged = source.drop_nulls("label").tag_by("label")
    assert tagged.as_table().column("label").to_pylist() == ["a"]


# ── refusals ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("columns", "error"),
    [
        ({"trial", "label"}, TypeError),
        (frozenset({"trial"}), TypeError),
        ([], ValueError),
        (["trial", "trial"], ValueError),
        ([1], TypeError),
        (["__meta"], InputValidationError),
        ([f"{constants.SYSTEM_TAG_PREFIX}x"], InputValidationError),
        ([f"{constants.SOURCE_PREFIX}x"], InputValidationError),
        (["a::b"], InputValidationError),
    ],
    ids=[
        "set",
        "frozenset",
        "empty",
        "repeated",
        "not_a_string",
        "meta",
        "system_tag",
        "source_info",
        "separator",
    ],
)
def test_bad_names_are_refused_at_construction(columns: Any, error: type) -> None:
    with pytest.raises(error):
        _ = TagBy(columns)


def test_the_stream_method_refuses_no_name_and_a_set() -> None:
    with pytest.raises(TypeError):
        _ = _plain().tag_by({"trial"})  # pyright: ignore[reportArgumentType]  # the refusal is the test
    with pytest.raises(ValueError, match="at least one"):
        _ = _plain().tag_by([])


@pytest.mark.parametrize(
    ("name", "match"),
    [
        ("missing", "'missing' is not a data column"),
        ("unit", "'unit' is a plain tag"),
    ],
)
def test_a_name_that_is_not_a_data_column_is_refused(name: str, match: str) -> None:
    with pytest.raises(InputValidationError, match=match):
        _ = _plain().tag_by(name)


def test_an_existing_data_tag_is_refused() -> None:
    with pytest.raises(InputValidationError, match="'unit' is already a data-tag"):
        _ = _data_tag().tag_by("unit")


def _typed_source(column: pa.Array[Any] | pa.ChunkedArray[Any]) -> Any:
    """Tag ``unit``; data ``v`` of the column's type, non-nullable."""
    schema = pa.schema(
        [
            pa.field("unit", pa.int64(), nullable=False),
            pa.field("v", column.type, False),
        ]
    )
    return op.sources.ArrowTableSource(
        pa.Table.from_arrays([pa.array([1, 2], pa.int64()), column], schema=schema),
        tag_columns=["unit"],
        tag_only_columns=True,
        source_id="typed",
    )


@pytest.mark.parametrize(
    "column",
    [
        pa.array([[1], [2]], pa.large_list(pa.int64())),
        pa.array([{"a": 1}, {"a": 2}], pa.struct([("a", pa.int64())])),
        pa.array([decimal.Decimal("1.5"), decimal.Decimal("2.5")]),
        pa.array([datetime.time(1), datetime.time(2)]),
        pa.array([datetime.timedelta(1), datetime.timedelta(2)]),
    ],
    ids=["list", "struct", "decimal", "time", "duration"],
)
def test_a_nested_or_untyped_type_is_refused(column: pa.Array[Any]) -> None:
    with pytest.raises(InputValidationError, match="'v' cannot be a tag"):
        _ = _typed_source(column).tag_by("v")


def test_a_logical_type_is_refused() -> None:
    source = op.sources.DictSource(
        [{"unit": 1, "path": Path("/a")}, {"unit": 2, "path": Path("/b")}],
        tag_columns=["unit"],
        tag_only_columns=True,
        source_id="paths",
    )
    with pytest.raises(InputValidationError, match="'path' cannot be a tag"):
        _ = source.tag_by("path")


@pytest.mark.parametrize(
    "column",
    [
        pa.array([1, 2], pa.int8()),
        pa.array([0.5, 1.5], pa.float32()),
        pa.array([True, False]),
        pa.array(["a", "b"], pa.string()),
        pa.array([b"a", b"b"], pa.binary()),
        pa.array([datetime.datetime(2026, 11, 16), datetime.datetime(2026, 11, 17)]),
        pa.array([datetime.date(2026, 11, 16), datetime.date(2026, 11, 17)]),
    ],
    ids=[
        "int8",
        "float32",
        "bool",
        "string",
        "binary",
        "timestamp",
        "date",
    ],
)
def test_a_scalar_type_is_accepted(column: pa.Array[Any]) -> None:
    source = _typed_source(column)
    tagged = source.tag_by("v")
    assert stream_data_tags(tagged) == ("v",)
    assert tagged.as_table().schema.field("v") == source.as_table().schema.field("v")


# ── in a job ────────────────────────────────────────────────────────────────


@ORCHESTRATORS
def test_in_a_job_a_function_node_stores_the_new_tag_marked(
    tmp_path: Path,
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    database = InMemoryArrowDatabase()
    job = PipelineJob(database=database)
    with job:
        computed = session_of.pod(_data_tag(), label="session")
        tagged = TagBy("session")(computed, label="tag_by")
        double.pod(tagged, label="double")
    job.run(orchestrator=orchestrator())
    assert job.nodes["tag_by"].keys() == (("unit", "session"), ("unit", "session"))
    assert job.nodes["double"].keys() == (("unit", "session"), ("unit", "session", "y"))
    rows = sorted(job.nodes["double"].as_table().to_pylist(), key=lambda r: r["unit"])
    assert [(r["unit"], r["session"], r["y"]) for r in rows] == [
        (1, "s0", "s0s0"),
        (2, "s1", "s1s1"),
    ]
    marked = {
        stored_data_tags(database.get_all_records(path).schema)  # pyright: ignore[reportOptionalMemberAccess]  # every listed table has records
        for path in database.list_table_paths()
        if "_result" not in path
    }
    assert ("unit", "session") in marked

    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json")
    assert loaded.nodes["tag_by"].keys() == job.nodes["tag_by"].keys()
    assert loaded.nodes["tag_by"].content_hash() == job.nodes["tag_by"].content_hash()


@op.function_pod(output_keys=["y"])
def double(session: str) -> str:
    """Reads the new tag's data column; ``unit`` falls through."""
    return session * 2


@ORCHESTRATORS
def test_a_logged_tag_by_node_stores_its_new_tag_marked(
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="ops", database=database)
    with job:
        _ = TagBy("trial")(_data_tag(), label="tag_by")
    job.nodes["tag_by"].attach_databases(
        pipeline_database=database.at("ops"), cache_mode=CacheMode.LOG
    )
    job.run(orchestrator=orchestrator())
    paths = [
        p for p in database.list_table_paths() if p[-1] == OPERATOR_DB_SCHEMA_VERSION
    ]
    assert len(paths) == 1
    stored = database.get_all_records(paths[0])
    assert stored is not None
    assert stored_data_tags(stored.schema) == ("unit", "trial")
