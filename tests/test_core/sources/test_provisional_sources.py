"""Provisional sources: the list, the markers and the warning (ITL-1314, ITL-1331).

Developer decisions, November 21, 2026: "make polling source provisional", and "Let's also
make DerivedSource as provisional?" (both forms). Each behaves as before (its default
source id included), but its source id and hashes are not frozen with data context v1, so
building one warns once per process; no golden case builds a ``PollingSource``, and the
golden values of a ``DerivedSource`` are kept in the provisional golden files.
"""

from __future__ import annotations

import inspect
import warnings
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

import orcapod.sources as sources
from orcapod import function_pod
from orcapod.core.sources import (
    PROVISIONAL_SOURCES,
    DerivedSource,
    PollingSource,
    RootSource,
)
from orcapod.errors import ProvisionalHashWarning
from orcapod.utils.provisional import (
    recording_provisional_uses,
    reset_provisional_use_warnings,
)


@pytest.fixture(autouse=True)
def fresh_process() -> Iterator[None]:
    """Each test starts as a new process would: no provisional source has warned."""
    reset_provisional_use_warnings()
    yield
    reset_provisional_use_warnings()


class StubImpl:
    """A polling implementation that is never polled; it round-trips through a config."""

    def __init__(self, name: str = "stub") -> None:
        self._name = name

    def identity(self) -> Any:
        return ("StubImpl", self._name)

    def to_config(self) -> dict[str, Any]:
        return {"name": self._name}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> StubImpl:
        return cls(config["name"])

    def schema(self) -> Any:
        return None

    async def poll(self, cursor: Any = None) -> bool:
        return False

    async def fetch(self, cursor: Any = None) -> Any:
        raise NotImplementedError

    async def close(self) -> None:
        return None


def _polling(name: str = "stub", **kwargs: Any) -> PollingSource[Any]:
    return PollingSource(
        StubImpl(name), tag_columns="id", record_id_columns="id", **kwargs
    )


def _provisional_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [
        str(w.message) for w in caught if issubclass(w.category, ProvisionalHashWarning)
    ]


def test_the_list_and_the_markers_agree() -> None:
    exported = {
        name: obj
        for name, obj in vars(sources).items()
        if name in sources.__all__
        and inspect.isclass(obj)
        and issubclass(obj, RootSource)
    }
    marked = {name for name, cls in exported.items() if cls.__dict__.get("PROVISIONAL")}
    assert marked == set(PROVISIONAL_SOURCES) == {"PollingSource", "DerivedSource"}
    assert dict(PROVISIONAL_SOURCES) == {
        "PollingSource": "ITL-1316",
        "DerivedSource": "ITL-1317",
    }


def test_one_warning_for_several_constructions() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for name in ("a", "b", "c"):
            _ = _polling(name)
        _ = _polling("d", source_id="named")
    messages = _provisional_warnings(caught)
    assert len(messages) == 1
    assert messages[0].startswith("PollingSource is provisional")
    assert "source id and hashes are not frozen" in messages[0]


def test_from_config_warns_too() -> None:
    config = _polling(source_id="polled").to_config()
    reset_provisional_use_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rebuilt = PollingSource.from_config(config)
    assert rebuilt.source_id == "polled"
    assert len(_provisional_warnings(caught)) == 1


def test_under_an_error_filter_every_construction_raises() -> None:
    for _ in range(2):
        with warnings.catch_warnings():
            warnings.simplefilter("error", ProvisionalHashWarning)
            with pytest.raises(ProvisionalHashWarning, match="PollingSource"):
                _ = _polling()


def test_every_use_is_recorded() -> None:
    """The golden tool refuses a case that records an undeclared provisional use."""
    with recording_provisional_uses() as used:
        _ = _polling("a")
        _ = _polling("b")
    assert used == {"PollingSource"}


def test_the_mark_enters_no_config_and_no_identity() -> None:
    plain = _polling(source_id="polled")
    assert set(plain.to_config()) == {
        "source_type",
        "impl_module",
        "impl_class",
        "tag_columns",
        "record_id_columns",
        "polling_config",
        "source_id",
        "impl_config",
    }
    assert plain.identity_structure() == (("StubImpl", "stub"), ("id",), ("id",))


def test_behavior_is_unchanged_default_source_id_included() -> None:
    """The default source id is still built from the implementation's identity."""
    first, second = _polling("a"), _polling("b")
    assert first.source_id.startswith("polling_source:")
    assert first.source_id != second.source_id
    assert _polling("a").source_id == first.source_id


def test_a_golden_case_can_declare_a_provisional_source() -> None:
    """A provisional source is declarable (ITL-1331); no golden case builds a
    PollingSource (ITL-1314), which ``test_golden_store`` checks."""
    from tests.test_hashing.golden.cases import declarable_provisional_names

    assert set(PROVISIONAL_SOURCES) <= declarable_provisional_names()


# ---------------------------------------------------------------------------
# DerivedSource, both forms (ITL-1331)
# ---------------------------------------------------------------------------


@function_pod(output_keys="total")
def _total(age: int, weight: float) -> float:
    return age + weight


def _origin(source_id: str = "people") -> Any:
    """Two tags and two data columns."""
    from orcapod.sources import ArrowTableSource

    table = pa.table(
        {
            "subject": ["s1", "s2", "s3"],
            "session": [1, 2, 1],
            "age": [2, 4, 6],
            "weight": [20.5, 22.25, 19.0],
        },
        schema=pa.schema(
            [
                pa.field("subject", pa.large_string(), nullable=False),
                pa.field("session", pa.int64(), nullable=False),
                pa.field("age", pa.int64(), nullable=False),
                pa.field("weight", pa.float64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(
        table,
        tag_columns=["subject", "session"],
        tag_only_columns=True,
        source_id=source_id,
    )


def _ran_job() -> Any:
    """A job with a function node (``total``) and an operator node (``picked``) that
    stores its rows, run once."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.operators import SelectDataColumns

    job = PipelineJob(name="derived", database=InMemoryArrowDatabase())
    with job:
        _total.pod(_origin(), label="total")
        SelectDataColumns(["age", "weight"])(_origin(), label="picked")
    job.run()
    return job


AS_DATA = {"session": "session_value", "subject": "subject_value"}

# Each way a user builds a DerivedSource: both forms, over a source and over both node
# kinds.
DERIVED_FORMS: dict[str, Any] = {
    "tags_as_data over a source": lambda job: DerivedSource(
        _origin(), tags_as_data=AS_DATA, source_id="copied"
    ),
    "tags_as_data, reversed mapping": lambda job: DerivedSource(
        _origin(),
        tags_as_data=dict(reversed(list(AS_DATA.items()))),
        source_id="copied",
    ),
    "plain over the function node": lambda job: job.nodes["total"].as_source(),
    "plain over the operator node": lambda job: job.nodes["picked"].as_source(),
    "tags_as_data over the function node": lambda job: job.nodes["total"].as_source(
        tags_as_data=AS_DATA, source_id="total_copied"
    ),
    "tags_as_data over the operator node": lambda job: job.nodes["picked"].as_source(
        tags_as_data=AS_DATA, source_id="picked_copied"
    ),
}


def test_derived_source_warns_once_for_every_form_and_node_kind() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        job = _ran_job()
    reset_provisional_use_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for build in DERIVED_FORMS.values():
            _ = build(job)
            _ = build(job)
    messages = _provisional_warnings(caught)
    assert len(messages) == 1, messages
    assert messages[0].startswith("DerivedSource is provisional")


@pytest.mark.parametrize("form", sorted(DERIVED_FORMS))
def test_each_derived_form_warns_in_a_new_process(form: str) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        job = _ran_job()
    reset_provisional_use_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with recording_provisional_uses() as used:
            _ = DERIVED_FORMS[form](job)
    assert [m.split(" ", 1)[0] for m in _provisional_warnings(caught)] == [
        "DerivedSource"
    ]
    assert used == {"DerivedSource"}


def _csv_origin(tmp_path: Any) -> Any:
    """An origin that a config can rebuild: two tags and two data columns."""
    from orcapod.sources import CSVSource

    path = tmp_path / "people.csv"
    _ = path.write_text("subject,session,age,weight\ns1,1,2,20.5\ns2,2,4,22.25\n")
    return CSVSource(
        str(path), tag_columns=["subject", "session"], tag_only_columns=True
    )


def test_derived_source_from_config_warns(tmp_path: Any) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        config = DerivedSource(
            _csv_origin(tmp_path), tags_as_data=AS_DATA, source_id="copied"
        ).to_config()
    reset_provisional_use_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rebuilt = DerivedSource.from_config(config)
    assert rebuilt.source_id == "copied"
    assert [m.split(" ", 1)[0] for m in _provisional_warnings(caught)] == [
        "DerivedSource"
    ]


def test_a_loaded_job_reading_a_derived_source_warns(tmp_path: Any) -> None:
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        job = PipelineJob(name="over_derived", database=InMemoryArrowDatabase())
        with job:
            derived = DerivedSource(
                _csv_origin(tmp_path), tags_as_data=AS_DATA, source_id="copied"
            )
            _ = derived.select_data_columns(["age", "subject_value"], label="picked")
        path = tmp_path / "job.json"
        job.save(path)
    reset_provisional_use_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        loaded = PipelineJob.load(path, database=InMemoryArrowDatabase())
    assert isinstance(loaded.sources["copied"], DerivedSource)
    assert [m.split(" ", 1)[0] for m in _provisional_warnings(caught)] == [
        "DerivedSource"
    ]


def test_no_other_source_kind_warns(tmp_path: Any) -> None:
    import polars as pl

    from orcapod.sources import (
        ArrowTableSource,
        CSVSource,
        DataFrameSource,
        DictSource,
        ListSource,
    )

    rows = [
        {"subject": "s1", "session": 1, "age": 2, "weight": 20.5},
        {"subject": "s2", "session": 2, "age": 4, "weight": 22.25},
    ]
    csv = tmp_path / "rows.csv"
    pl.DataFrame(rows).write_csv(csv)
    tags = ["subject", "session"]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        built = [
            _origin(),
            ArrowTableSource(
                pa.Table.from_pylist(rows), tag_columns=tags, tag_only_columns=True
            ),
            DictSource(data=rows, tag_columns=tags, tag_only_columns=True),
            DataFrameSource(
                pl.DataFrame(rows), tag_columns=tags, tag_only_columns=True
            ),
            CSVSource(str(csv), tag_columns=tags, tag_only_columns=True),
            ListSource("weight", [20.5, 22.25]),
        ]
        for source in built:
            _ = source.source_id, source.content_hash(), source.as_table()
    assert _provisional_warnings(caught) == []


def _derived_identity(form: str) -> tuple[Any, ...]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        derived = DERIVED_FORMS[form](_ran_job())
    config = derived.to_config()
    config.pop("origin", None)
    return (
        derived.source_id,
        derived.content_hash().to_string(),
        derived.pipeline_hash().to_string(),
        config,
    )


@pytest.mark.parametrize("form", sorted(DERIVED_FORMS))
def test_the_mark_changes_no_derived_identity(
    monkeypatch: pytest.MonkeyPatch, form: str
) -> None:
    """Source id, content hash, pipeline hash and config are those of an unmarked
    DerivedSource, in both forms (ITL-1331)."""
    marked = _derived_identity(form)
    monkeypatch.setattr(DerivedSource, "PROVISIONAL", False)
    unmarked = _derived_identity(form)
    assert marked == unmarked


@pytest.mark.parametrize("form", sorted(DERIVED_FORMS))
def test_a_derived_source_built_before_a_recording_is_noted_on_every_use(
    form: str,
) -> None:
    """Built (and read once) before the recording: reading its id, hashes or rows notes
    the use, without warning again (ITL-1331)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        derived = DERIVED_FORMS[form](_ran_job())
        _ = derived.source_id, derived.content_hash(), derived.pipeline_hash()
        _ = derived.as_table()
    uses: dict[str, Any] = {
        "source id": lambda: derived.source_id,
        "content hash": derived.content_hash,
        "pipeline hash": derived.pipeline_hash,
        "rows": derived.as_table,
        "iterated": lambda: list(derived.iter_data()),
    }
    for use, fn in uses.items():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with recording_provisional_uses() as used:
                _ = fn()
        assert used == {"DerivedSource"}, use
        assert _provisional_warnings(caught) == [], use
