"""Provisional operators: the list, the markers, the warning and the record of uses (ITL-1266)."""

from __future__ import annotations

import inspect
import warnings
from collections.abc import Iterator
from typing import Any, ClassVar

import pyarrow as pa
import pytest

import orcapod.operators as operators
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.operators.provisional import (
    PROVISIONAL_OPERATORS,
    provisional_operator_name,
    reset_provisional_operator_warnings,
    warn_provisional_operator,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.errors import ProvisionalHashWarning
from orcapod.operators import PolarsFilter
from orcapod.sources import ArrowTableSource
from orcapod.utils.provisional import recording_provisional_uses


@pytest.fixture(autouse=True)
def fresh_process() -> Iterator[None]:
    """Each test starts as a new process would: no provisional operator has warned."""
    reset_provisional_operator_warnings()
    yield
    reset_provisional_operator_warnings()


def _provisional_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [
        str(w.message) for w in caught if issubclass(w.category, ProvisionalHashWarning)
    ]


def _exported_operators() -> dict[str, type]:
    return {
        name: obj
        for name, obj in vars(operators).items()
        if name in operators.__all__
        and inspect.isclass(obj)
        and issubclass(obj, StaticOutputOperatorPod)
    }


class _ProvisionalA(UnaryOperator):
    """A provisional test operator."""

    PROVISIONAL: ClassVar[bool] = True

    def unary_static_process(self, stream: Any) -> Any:
        return stream

    def validate_unary_input(self, stream: Any) -> None:
        return None

    def unary_output_schema(self, stream: Any, **kwargs: Any) -> Any:
        return stream.output_schema(**kwargs)

    def identity_structure(self) -> Any:
        return (type(self).__name__,)


class _ProvisionalB(_ProvisionalA):
    """A second provisional test operator, marked on its own class."""

    PROVISIONAL: ClassVar[bool] = True


class _SubclassOfA(_ProvisionalA):
    """Provisional through its base only."""


def _stream() -> ArrowTableSource:
    table = pa.table(
        {
            "subject": ["s1", "s2", "s3"],
            "session": [1, 2, 1],
            "age": [2, 4, 6],
            "weight": [20.5, 22.25, 19.0],
        }
    )
    return ArrowTableSource(
        table, tag_columns=["subject", "session"], tag_only_columns=True
    )


# ---------------------------------------------------------------------------
# The list and the markers agree
# ---------------------------------------------------------------------------


def test_the_list_and_the_markers_agree() -> None:
    exported = _exported_operators()
    assert set(PROVISIONAL_OPERATORS) <= set(exported)
    marked = {
        name
        for name, cls in exported.items()
        if cls.__dict__.get("PROVISIONAL") is True
    }
    assert marked == set(PROVISIONAL_OPERATORS)
    for name, cls in exported.items():
        expected = name if name in PROVISIONAL_OPERATORS else None
        assert provisional_operator_name(cls) == expected, name


# The developer-approved list (2026-11-19, ITL-1279), each with the issue that decides
# its identity or output.
APPROVED = {
    "PolarsFilter": "ITL-1247",
    "GroupBy": "ITL-1017",
    "Batch": "ITL-1017",
    "MergeJoin": "ITL-1018",
    "MapTags": "ITL-1018",
    "MapColumns": "ITL-1022",
    "Pick": "ITL-439",
    "Index": "ITL-439",
    "TagBy": "ITL-1290",
    "AddTags": "ITL-1290",
}
# The operators frozen with data context v1 (ITL-1279).
FROZEN = {
    "Join",
    "SemiJoin",
    "MapData",
    "SelectTagColumns",
    "SelectDataColumns",
    "DropTagColumns",
    "DropDataColumns",
    "SelectColumns",
    "DropColumns",
    "DropNulls",
}


def test_the_list_is_the_approved_list_with_its_issues() -> None:
    assert dict(PROVISIONAL_OPERATORS) == APPROVED
    assert "PROVISIONAL_OPERATORS" in operators.__all__


def test_every_exported_operator_is_provisional_or_frozen() -> None:
    """No exported operator is left undecided: a new one must join a list."""
    assert set(_exported_operators()) == set(APPROVED) | FROZEN


def test_a_subclass_is_provisional_as_its_nearest_marked_base() -> None:
    assert provisional_operator_name(_ProvisionalA) == "_ProvisionalA"
    assert provisional_operator_name(_ProvisionalB) == "_ProvisionalB"
    assert provisional_operator_name(_SubclassOfA) == "_ProvisionalA"

    class _UserFilter(PolarsFilter):
        pass

    assert provisional_operator_name(_UserFilter) == "PolarsFilter"


# ---------------------------------------------------------------------------
# The warning: at construction, once per operator per process
# ---------------------------------------------------------------------------


def test_constructing_polars_filter_warns_once_per_process() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = PolarsFilter(constraints={"subject": "s1"})
        _ = PolarsFilter(constraints={"subject": "s2", "session": 1})
    messages = _provisional_warnings(caught)
    assert len(messages) == 1
    message = messages[0]
    assert message.startswith("PolarsFilter is provisional:")
    for part in (
        "not frozen with data context v1",
        "stored paths and record ids",
        "every step downstream of PolarsFilter",
        "'Provisional operators' in docs/concepts/operators.md",
        "category=orcapod.errors.ProvisionalHashWarning",
    ):
        assert part in message


def test_the_warning_points_at_the_code_that_built_the_operator() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = PolarsFilter()
    (found,) = [w for w in caught if issubclass(w.category, ProvisionalHashWarning)]
    assert found.filename == __file__


def test_the_stream_method_warns_too() -> None:
    import polars as pl

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        filtered = _stream().polars_filter(pl.col("age") > 3, session=1)
    assert len(_provisional_warnings(caught)) == 1
    assert filtered.as_table()["subject"].to_pylist() == ["s3"]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        filtered = _stream().polars_filter(pl.col("age") > 3, pl.col("weight") > 20)
        rows = filtered.as_table()
    assert _provisional_warnings(caught) == []
    assert rows["subject"].to_pylist() == ["s2"]


@pytest.mark.parametrize(
    "order", [(_ProvisionalA, _ProvisionalB), (_ProvisionalB, _ProvisionalA)]
)
def test_two_provisional_operators_warn_once_each_in_any_order(
    order: tuple[type, type],
) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for cls in (*order, *order):
            _ = cls()
    messages = _provisional_warnings(caught)
    assert [m.split(" ", 1)[0] for m in messages] == [cls.__name__ for cls in order]


def test_a_subclass_warns_under_the_name_of_its_provisional_base() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = _SubclassOfA()
        _ = _ProvisionalA()
    messages = _provisional_warnings(caught)
    assert len(messages) == 1
    assert messages[0].startswith("_ProvisionalA is provisional:")


def test_an_operator_that_is_not_provisional_does_not_warn() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = operators.SelectDataColumns(columns=["age", "weight"])
        _ = operators.Join()
    assert _provisional_warnings(caught) == []


def test_under_an_error_filter_every_construction_raises() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", ProvisionalHashWarning)
        for _ in range(2):
            with pytest.raises(ProvisionalHashWarning):
                _ = PolarsFilter()


# ---------------------------------------------------------------------------
# The record of uses
# ---------------------------------------------------------------------------


def test_construction_is_recorded_even_after_the_warning() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _ = PolarsFilter()
        with recording_provisional_uses() as used:
            _ = PolarsFilter(constraints={"subject": "s1"})
    assert used == {"PolarsFilter"}


@pytest.mark.parametrize("names", [("A", "B"), ("B", "A")])
def test_a_record_holds_every_name_used_in_any_order(names: tuple[str, str]) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with recording_provisional_uses() as used:
            for name in (*names, *names):
                warn_provisional_operator(name)
    assert used == {"A", "B"}


def test_nested_records_each_see_what_was_used_while_they_were_open() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with recording_provisional_uses() as outer:
            warn_provisional_operator("A")
            with recording_provisional_uses() as inner:
                warn_provisional_operator("B")
            # An equal set, open beside it, is a different record.
            with (
                recording_provisional_uses() as first,
                recording_provisional_uses() as second,
            ):
                pass
            warn_provisional_operator("C")
    assert outer == {"A", "B", "C"}
    assert inner == {"B"}
    assert first == second == set()


def test_a_record_closed_by_an_error_stops_recording() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with pytest.raises(RuntimeError), recording_provisional_uses() as used:
            raise RuntimeError("case failed")
        warn_provisional_operator("A")
    assert used == set()


def test_recording_changes_no_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with recording_provisional_uses():
            _ = PolarsFilter()
        with recording_provisional_uses():
            _ = PolarsFilter()
    assert len(_provisional_warnings(caught)) == 1


# ---------------------------------------------------------------------------
# Every approved operator warns once per process, through every way it is built
# (ITL-1279)
# ---------------------------------------------------------------------------


def _non_null(columns: dict[str, Any]) -> pa.Table:
    """A table of ``columns`` with every field non-nullable."""
    table = pa.table(columns)
    return table.cast(pa.schema(f.with_nullable(False) for f in table.schema))


def _lists() -> Any:
    """Two tags, a list column and a mapping column, for ``Pick`` and ``Index``."""
    from orcapod.sources import DictSource

    return DictSource(
        data=[
            {"subject": "s1", "session": 1, "values": [1, 2, 3], "meta": {"k": 10}},
            {"subject": "s2", "session": 2, "values": [4, 5], "meta": {"k": 20}},
        ],
        tag_columns=["subject", "session"],
        tag_only_columns=True,
    )


def _non_null_stream() -> ArrowTableSource:
    """``_stream``'s columns, none nullable (a tag needs a value in every row)."""
    return ArrowTableSource(
        _non_null(
            {
                "subject": ["s1", "s2", "s3"],
                "session": [1, 2, 1],
                "age": [2, 4, 6],
                "weight": [20.5, 22.25, 19.0],
            }
        ),
        tag_columns=["subject", "session"],
        tag_only_columns=True,
    )


def _other() -> ArrowTableSource:
    """A second input for ``MergeJoin``: the same tags, colliding ``age``."""
    table = pa.table({"subject": ["s1", "s2"], "session": [1, 2], "age": [7, 8]})
    return ArrowTableSource(
        table, tag_columns=["subject", "session"], tag_only_columns=True
    )


# Each approved operator built through the stream method a user calls (MergeJoin has
# none: it is called on two inputs).
BY_STREAM_METHOD: dict[str, Any] = {
    "GroupBy": lambda: _stream().group_by("subject"),
    "Batch": lambda: _stream().batch(2),
    "TagBy": lambda: _non_null_stream().tag_by("age"),
    "AddTags": lambda: _stream().add_tags(cohort="A", run=3),
    "MapTags": lambda: _stream().map_tags({"subject": "animal"}),
    "MapColumns": lambda: _stream().map_columns({"subject": "animal", "age": "years"}),
    "Pick": lambda: _lists().pick("meta", "k"),
    "Index": lambda: _lists().index("values", 0),
    "MergeJoin": lambda: operators.MergeJoin()(_stream(), _other()),
}


@pytest.mark.parametrize("name", sorted(BY_STREAM_METHOD))
def test_each_approved_operator_warns_once_through_its_stream_method(name: str) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(2):
            _ = BY_STREAM_METHOD[name]().as_table()
    messages = _provisional_warnings(caught)
    assert len(messages) == 1, messages
    assert messages[0].startswith(f"{name} is provisional:")


NOT_REBUILT_FROM_CONFIG = pytest.mark.xfail(
    strict=True,
    raises=TypeError,
    reason="Pick and Index have no to_config and are not in the operator registry of "
    + "pipeline/serialization.py: a saved job loads such a node read-only and never "
    + "rebuilds the operator (ITL-1326)",
)


@pytest.mark.parametrize(
    "name",
    [
        pytest.param(n, marks=NOT_REBUILT_FROM_CONFIG) if n in ("Index", "Pick") else n
        for n in sorted(BY_STREAM_METHOD)
    ],
)
def test_each_approved_operator_warns_when_rebuilt_from_its_config(name: str) -> None:
    """A saved and loaded job rebuilds its operators from their configs."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        stream = BY_STREAM_METHOD[name]()
    operator = stream.producer  # pyright: ignore[reportAttributeAccessIssue]
    reset_provisional_operator_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rebuilt = type(operator).from_config(operator.to_config())
    assert type(rebuilt) is type(operator)
    assert [m.split(" ", 1)[0] for m in _provisional_warnings(caught)] == [name]


@pytest.mark.parametrize(
    "order", [("GroupBy", "AddTags", "Batch"), ("AddTags", "GroupBy", "Batch")]
)
def test_a_loaded_job_warns_once_for_each_provisional_operator_it_holds(
    tmp_path: Any, order: tuple[str, ...]
) -> None:
    """Three provisional operators in a job, in two orders: loading the saved job warns
    once for each, in a new process (the bookkeeping reset), and never for a frozen one."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    steps = {
        "GroupBy": lambda s: s.group_by("subject"),
        "AddTags": lambda s: s.add_tags(cohort="A"),
        "Batch": lambda s: s.batch(2),
    }
    job = PipelineJob(name="provisional", database=InMemoryArrowDatabase())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with job:
            stream: Any = _stream().select_data_columns(["age"])
            for name in order:
                stream = steps[name](stream)
    path = tmp_path / "job.json"
    job.save(path)
    reset_provisional_operator_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = PipelineJob.load(path, database=InMemoryArrowDatabase())
    names = [m.split(" ", 1)[0] for m in _provisional_warnings(caught)]
    assert sorted(names) == sorted(order)


@pytest.mark.parametrize("name", ["Index", "Pick"])
def test_a_loaded_job_holding_an_operator_kept_read_only_warns(
    tmp_path: Any, name: str
) -> None:
    """Pick and Index are not rebuilt from a saved job (ITL-1326: the strict xfail
    above), so the loader keeps them read-only; the load still warns, once, through the
    same helper, and notes the use (Codex review of PR 469)."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    steps = {
        "Pick": lambda s: s.pick("meta", "k"),
        "Index": lambda s: s.index("values", 0),
    }
    job = PipelineJob(name="read-only", database=InMemoryArrowDatabase())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with job:
            _ = steps[name](_lists())
    path = tmp_path / "job.json"
    job.save(path)
    reset_provisional_operator_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with recording_provisional_uses() as used:
            _ = PipelineJob.load(path, database=InMemoryArrowDatabase())
    assert [m.split(" ", 1)[0] for m in _provisional_warnings(caught)] == [name]
    assert used == {name}


@pytest.mark.parametrize("name", ["Batch", "MergeJoin"])
def test_an_operator_built_before_a_recording_is_noted_on_every_use(
    name: str,
) -> None:
    """A unary and a binary provisional operator built outside the recording: reading
    its identity, applying it, and reading or hashing a stream it already computed each
    note the use, without warning again and without changing a hash (ITL-1279)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        operator: Any = operators.Batch(2) if name == "Batch" else operators.MergeJoin()

        def apply() -> Any:
            if name == "Batch":
                return operator(_stream())
            return operator(_stream(), _other())

        kept = apply()
        _ = kept.as_table()
        hashes = (
            operator.content_hash(),
            operator.pipeline_hash(),
            kept.content_hash(),
            kept.pipeline_hash(),
        )
    uses: dict[str, Any] = {
        "operator content hash": operator.content_hash,
        "operator pipeline hash": operator.pipeline_hash,
        "applied": apply,
        "applied with static_process": lambda: operator.static_process(
            *(apply().upstreams)
        ),
        "kept stream read": kept.as_table,
        "kept stream rows": lambda: list(kept.iter_data()),
        "kept stream content hash": kept.content_hash,
        "kept stream pipeline hash": kept.pipeline_hash,
        "operator identity structure": operator.identity_structure,
        "operator pipeline identity structure": operator.pipeline_identity_structure,
        "kept stream identity structure": kept.identity_structure,
        "kept stream pipeline identity structure": kept.pipeline_identity_structure,
        "kept stream head": lambda: kept.head(2),
    }
    for use, fn in uses.items():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with recording_provisional_uses() as used:
                _ = fn()
        assert used == {name}, use
        assert _provisional_warnings(caught) == [], use
    assert hashes == (
        operator.content_hash(),
        operator.pipeline_hash(),
        kept.content_hash(),
        kept.pipeline_hash(),
    )


@pytest.mark.parametrize("name", sorted(PROVISIONAL_OPERATORS))
def test_reading_a_provisional_operators_identity_structure_is_a_use(name: str) -> None:
    """Every provisional operator, built before the recording: a direct read of its
    identity structure or pipeline identity structure notes the use (Codex review 2 of
    PR 469), without warning again."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if name == "PolarsFilter":
            operator: Any = PolarsFilter(constraints={"subject": "s1", "session": 1})
        else:
            operator = BY_STREAM_METHOD[name]().producer
    for read in (operator.identity_structure, operator.pipeline_identity_structure):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with recording_provisional_uses() as used:
                _ = read()
        assert used == {name}, read
        assert _provisional_warnings(caught) == []


def _saved_job(tmp_path: Any, name: str) -> Any:
    """A saved job holding the provisional operator ``name`` (Pick and Index are kept
    read-only on load, ITL-1326; the others are rebuilt)."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    steps = {
        "GroupBy": lambda: _stream().group_by("subject"),
        "AddTags": lambda: _stream().add_tags(run=3, cohort="A"),
        "Pick": lambda: _lists().pick("meta", "k"),
        "Index": lambda: _lists().index("values", 0),
    }
    job = PipelineJob(name=f"saved_{name}", database=InMemoryArrowDatabase())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with job:
            _ = steps[name]()
    path = tmp_path / f"{name}.json"
    job.save(path)
    return path


@pytest.mark.parametrize("name", ["AddTags", "GroupBy", "Index", "Pick"])
def test_loading_a_job_under_an_error_filter_raises_the_warning(
    tmp_path: Any, name: str
) -> None:
    """A ProvisionalHashWarning turned into an error propagates from the load, whether
    the operator is rebuilt or kept read-only (Codex review 2 of PR 469); without the
    filter the job loads as before."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    path = _saved_job(tmp_path, name)
    for _ in range(2):  # every load raises: nothing is recorded as issued
        reset_provisional_operator_warnings()
        with warnings.catch_warnings():
            warnings.simplefilter("error", ProvisionalHashWarning)
            with pytest.raises(ProvisionalHashWarning, match=f"^{name} is provisional"):
                _ = PipelineJob.load(path, database=InMemoryArrowDatabase())
    reset_provisional_operator_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        loaded = PipelineJob.load(path, database=InMemoryArrowDatabase())
    assert [m.split(" ", 1)[0] for m in _provisional_warnings(caught)] == [name]
    assert loaded.nodes
