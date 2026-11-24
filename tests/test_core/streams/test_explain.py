"""``stream.explain()`` (ITL-1402): what each source applies, what is applied after reading.

The cases of section 5 of the Filter design (``docs/dev/specs/itl-1374-filter.md``):
a restricted source under a filter, an unrestricted one, a filter below an operator or
function pod, several sources through a join, a job node; and that nothing is computed,
read, polled or written, and no user code runs.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator, Mapping
import itertools
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from orcapod import PipelineJob, col, function_pod
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import (
    DropNulls,
    Pick,
    Batch,
    Filter,
    GroupBy,
    Index,
    Join,
    MergeJoin,
    PolarsFilter,
    SemiJoin,
)
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.sources import SpiralDBTableSource
from orcapod.types import Condition, Explanation, SourceReading, Step
from tests.test_databases import fake_pyspiral

pytestmark = [
    pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning"),
    pytest.mark.filterwarnings(
        "ignore::orcapod.core.data_tags.DataTagMaterializationWarning"
    ),
]

_PROJECT = "explain-sample"


def _units(source_id: str = "units") -> ArrowTableSource:
    table = pa.table(
        {
            "session": pa.array(["s1", "s1", "s2"], pa.large_string()),
            "unit": pa.array([3, 7, 3], pa.int64()),
            "t": pa.array([0.5, 1.2, 2.0], pa.float64()),
            "rate": pa.array([4.0, None, 2.5], pa.float64()),
        }
    )
    return ArrowTableSource(
        table,
        tag_columns=["session", "unit"],
        tag_only_columns=True,
        source_id=source_id,
    )


def _probes(source_id: str = "probes") -> ArrowTableSource:
    table = pa.table(
        {
            "session": pa.array(["s1", "s2"], pa.large_string()),
            "probe": pa.array(["imec0", "imec1"], pa.large_string()),
        }
    )
    return ArrowTableSource(
        table, tag_columns=["session"], tag_only_columns=True, source_id=source_id
    )


@pytest.fixture()
def spiral(tmp_path: Path):
    """A fake SpiralDB project with an ``event`` table; ``scan_log`` records reads."""
    keys = pa.schema(
        [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
    )
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        project.create_table("default.event", key_schema=keys).write(
            pa.table(
                {
                    "subject_id": ["m1", "m1", "m2"],
                    "timestamp": pa.array([1, 2, 3], pa.uint64()),
                    "amplitude": pa.array([0.5, 1.5, 2.5], pa.float64()),
                }
            )
        )
        fake_pyspiral.scan_log.clear()
        yield project
        fake_pyspiral.scan_log.clear()


def _event(**kwargs: Any) -> SpiralDBTableSource:
    return SpiralDBTableSource(_PROJECT, "event", **kwargs)


class TestSectionFiveCases:
    def test_a_restricted_source_under_a_filter(self, spiral):
        source = _event().where(subject_id="m1")
        stream = source.filter(col("amplitude") >= 1.0, label="loud")
        fake_pyspiral.scan_log.clear()
        text = stream.explain().text()
        assert fake_pyspiral.scan_log == []  # nothing read
        assert text == (
            "DynamicPodStream[loud] reads:\n"
            f"  source {source.source_id!r} (SpiralDBTableSource)\n"
            "    applied by the source: where subject_id='m1'\n"
            "    applied after reading: amplitude >= 1.0 (Filter[loud])"
        )

    def test_a_limit_is_applied_by_the_source(self, spiral):
        source = _event(limit=2)
        reading = source.filter(col("amplitude") >= 1.0).explain().sources[0]
        assert reading.limit == 2
        assert reading.lines()[1] == "  applied by the source: limit 2"

    def test_an_unrestricted_source_reads_its_whole_table(self):
        stream = _units().filter(col("t") >= 1.0, label="late")
        assert stream.explain().text() == (
            "DynamicPodStream[late] reads:\n"
            "  source 'arrow_table_v1:units' (ArrowTableSource)\n"
            "    applied by the source: nothing (the whole table is read)\n"
            "    applied after reading: t >= 1.0 (Filter[late])"
        )

    def test_a_filter_below_an_operator_and_a_function_pod(self):
        calls: list[int] = []

        @function_pod(output_keys="doubled")
        def double(t: float | None) -> float:
            calls.append(1)
            return 2 * (t or 0.0)

        filtered = _units().filter(col("t") >= 1.0, label="late")
        stream = GroupBy("session")(double.pod(filtered.select_data_columns(["t"])))
        text = stream.explain().text()
        assert calls == []  # no user code ran
        assert text.splitlines()[1:] == [
            "  source 'arrow_table_v1:units' (ArrowTableSource)",
            "    applied by the source: nothing (the whole table is read)",
            "    applied after reading: t >= 1.0 (Filter[late])",
        ]

    def test_several_sources_through_a_join(self):
        stream = Join()(_units().filter(col("t") >= 1.0, label="late"), _probes())
        assert stream.explain().text().splitlines()[1:] == [
            "  source 'arrow_table_v1:probes' (ArrowTableSource)",
            "    applied by the source: nothing (the whole table is read)",
            "    applied after reading: rows without a match in 'arrow_table_v1:units' are dropped (Join)",
            "  source 'arrow_table_v1:units' (ArrowTableSource)",
            "    applied by the source: nothing (the whole table is read)",
            "    applied after reading: t >= 1.0 (Filter[late]); rows without a match "
            "in 'arrow_table_v1:probes' are dropped (Join)",
        ]

    def test_a_job_node_walks_its_slot_to_the_bound_source(self):
        job = PipelineJob(name="explain", database=InMemoryArrowDatabase())
        with job:
            _units().filter(col("t") >= 1.0, label="late").drop_nulls(
                ["rate", "t"], label="clean"
            )
        text = job.nodes["clean"].explain().text()
        assert text.splitlines()[1:] == [
            "  source 'arrow_table_v1:units' (ArrowTableSource)",
            "    applied by the source: nothing (the whole table is read)",
            "    applied after reading: t >= 1.0 (Filter[late]); no null in rate, t "
            "(DropNulls[clean])",
        ]

    def test_an_unbound_slot(self):
        tag_schema, data_schema = _units().output_schema()
        slot = SourceNode(name="units", tag_schema=tag_schema, data_schema=data_schema)
        job = PipelineJob(name="unbound", database=InMemoryArrowDatabase())
        with job:
            Filter(col("t") >= 1.0)(slot, label="late")
        reading = job.nodes["late"].explain().sources[0]
        assert not reading.bound
        assert reading.lines() == [
            "slot 'units': no source bound",
            "  applied after reading: t >= 1.0 (Filter[late])",
        ]


class TestSourcesThatCannotSay:
    def test_a_cached_source(self):
        cached = CachedSource(_units(), cache_database=InMemoryArrowDatabase())
        reading = cached.filter(col("t") >= 1.0).explain().sources[0]
        assert not reading.known
        assert reading.lines()[1] == (
            "  applied by the source: not known for this source kind"
        )

    def test_a_raw_stream(self):
        table = pa.table({"k": [1, 2], "v": [3, 4]})
        stream = Filter(col("v") > 3)(ArrowTableStream(table, tag_columns=["k"]))
        reading = stream.explain().sources[0]
        assert reading.kind == "ArrowTableStream"
        assert not reading.known


class TestStepsThatDropRows:
    def test_drop_nulls_names_in_name_order_and_the_no_argument_form(self):
        named = _units().drop_nulls(["t", "rate"]).explain().sources[0].steps
        assert [s.text for s in named] == ["no null in rate, t"]
        every = _units().drop_nulls().explain().sources[0].steps
        assert [s.text for s in every] == ["no null in any tag or data column"]

    def test_polars_filter_in_no_given_order(self):
        a = PolarsFilter(predicates=[pl.col("t") > 1, pl.col("unit") == 3])
        b = PolarsFilter(predicates=[pl.col("unit") == 3, pl.col("t") > 1])
        assert a(_units()).explain().text() == b(_units()).explain().text()

    def test_semi_join_both_sides(self):
        stream = SemiJoin()(_units(), _probes())
        by_name = {r.name: r for r in stream.explain().sources}
        assert [s.text for s in by_name["arrow_table_v1:units"].steps] == [
            "rows without a match in 'arrow_table_v1:probes' are dropped"
        ]
        assert [s.text for s in by_name["arrow_table_v1:probes"].steps] == [
            "its rows only choose rows of 'arrow_table_v1:units' and do not flow on"
        ]

    def test_merge_join(self):
        stream = MergeJoin()(_units(), _probes())
        texts = {r.name: [s.text for s in r.steps] for r in stream.explain().sources}
        assert texts["arrow_table_v1:units"] == [
            "rows without a match in 'arrow_table_v1:probes' are dropped"
        ]

    def test_batch_drops_rows_only_when_asked(self):
        assert _units().batch(2).explain().sources[0].steps == ()
        steps = Batch(2, drop_partial_batch=True)(_units()).explain().sources[0].steps
        assert [s.text for s in steps] == ["a last partial batch is dropped"]

    def test_index_drops_rows_on_a_miss_unless_a_miss_raises(self):
        table = pa.table(
            {"k": [1, 2], "xs": [[1, 2], [3]]},
            schema=pa.schema(
                [
                    pa.field("k", pa.int64()),
                    pa.field("xs", pa.large_list(pa.int64()), nullable=False),
                ]
            ),
        )
        source = ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="lists"
        )
        steps = Index("xs", 1)(source).explain().sources[0].steps
        assert [s.text for s in steps] == [
            "rows whose 'xs' has no element 1 are dropped"
        ]
        assert (
            Index("xs", 1, fail_on_miss=True)(source).explain().sources[0].steps == ()
        )

    def test_operators_that_drop_no_rows_are_not_listed(self):
        stream = _units().select_data_columns(["t"]).map_tags({"unit": "u"})
        assert stream.explain().sources[0].steps == ()


class TestNoOrderCounts:
    def test_conditions_and_operands_in_any_order(self):
        terms = [col("t") >= 1.0, col("unit").is_in([7, 3]), col("rate").is_not_null()]
        texts = {
            _units().filter(*order, label="f").explain().text()
            for order in itertools.permutations(terms)
        }
        texts.add(
            _units()
            .filter((col("unit").is_in([3, 7])) & terms[2] & terms[0], label="f")
            .explain()
            .text()
        )
        assert len(texts) == 1

    def test_join_inputs_and_source_names_in_any_order(self):
        texts = {
            Join()(*order).explain().text().split("\n", 1)[1]
            for order in itertools.permutations((_units(), _probes()))
        }
        assert len(texts) == 1


class TestPlainData:
    def test_fields_and_text(self):
        explanation = _units().filter(col("t") >= 1.0).explain()
        assert isinstance(explanation, Explanation)
        assert str(explanation) == explanation.text()
        (reading,) = explanation.sources
        assert isinstance(reading, SourceReading)
        assert reading.condition == Condition()
        assert reading.limit is None
        assert reading.steps == (
            Step(operator="Filter", label="Filter", text="t >= 1.0"),
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            reading.name = "x"  # pyright: ignore[reportAttributeAccessIssue]

    def test_equal_pipelines_explain_equally(self):
        assert (
            _units().filter(session="s1").explain()
            == _units().filter(session="s1").explain()
        )


class TestPreview:
    def test_a_filter_over_an_unrestricted_source(self):
        header = _units().filter(col("t") >= 1.0).head().header()
        assert header.endswith(
            "applied by the source: nothing; after reading: t >= 1.0 "
            "(the whole table is read)"
        )

    def test_a_filter_over_a_restricted_source(self, spiral):
        stream = _event().where(subject_id="m1").filter(col("amplitude") >= 1.0)
        assert (
            stream.head()
            .header()
            .endswith(
                "applied by the source: where subject_id='m1'; after reading: "
                "amplitude >= 1.0"
            )
        )

    def test_once_computed_the_line_stays(self):
        stream = _units().filter(col("t") >= 1.0)
        stream.run()
        assert stream.head().header().endswith("(the whole table is read)")

    def test_no_line_elsewhere(self):
        assert "applied by the source" not in _units().drop_nulls().head().header()
        two_steps = _units().select_data_columns(["t"]).filter(col("t") >= 1.0)
        assert "applied by the source" not in two_steps.head().header()


def test_explaining_a_job_writes_and_computes_nothing():
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="quiet", database=database)
    with job:
        _units().filter(col("t") >= 1.0, label="late")
    for node in job.nodes.values():
        node.explain()
    assert list(database.list_table_paths()) == []
    assert job.nodes["late"].head().status.value == "not_computed"


class TestReviewOfPr503:
    """CodeRabbit's review of PR 503: each finding as a test."""

    def test_a_pipelines_source_node_reads_as_unbound(self):
        tag_schema, data_schema = _units().output_schema()
        slot = SourceNode(name="units", tag_schema=tag_schema, data_schema=data_schema)
        (reading,) = Filter(col("t") >= 1.0)(slot).explain().sources
        assert not reading.bound
        assert reading.lines() == [
            "slot 'units': no source bound",
            "  applied after reading: t >= 1.0 (Filter)",
        ]

    @pytest.mark.parametrize(
        ("first", "second", "expected"),
        [
            (
                {"where": {"subject_id": "m1"}},
                {"where": {"subject_id": "m2"}},
                ["where subject_id='m1'", "where subject_id='m2'"],
            ),
            (
                {"where": {"subject_id": "m1"}},
                {},
                ["nothing (the whole table is read)", "where subject_id='m1'"],
            ),
            ({"limit": 1}, {"limit": 2}, ["limit 1", "limit 2"]),
        ],
        ids=["two_conditions", "condition_and_none", "two_limits"],
    )
    def test_two_restrictions_of_one_table_are_two_readings(
        self, spiral, first, second, expected
    ):
        def build(restriction: dict[str, Any]) -> SpiralDBTableSource:
            source = (
                _event(limit=restriction["limit"])
                if "limit" in restriction
                else _event()
            )
            if "where" in restriction:
                source = source.where(**restriction["where"])
            return source

        stream = SemiJoin()(build(first), build(second))
        applied = sorted(
            r.lines()[1].removeprefix("  applied by the source: ")
            for r in stream.explain().sources
        )
        assert applied == expected

    @pytest.mark.parametrize(
        "combine",
        [
            lambda late: SemiJoin()(late, late.select_data_columns(["t"])),
            lambda late: SemiJoin()(
                late, late.select_data_columns(["t", "rate"]).select_data_columns(["t"])
            ),
            lambda late: MergeJoin()(
                late.select_data_columns(["t"]), late.select_data_columns(["rate"])
            ),
        ],
        ids=["semijoin_depths_0_1", "semijoin_depths_0_2", "merge_join_two_branches"],
    )
    def test_a_step_reached_by_two_paths_is_listed_once(self, combine):
        late = _units().filter(col("t") >= 1.0, label="late")
        (reading,) = combine(late).explain().sources
        assert [s.label for s in reading.steps].count("late") == 1

    def test_shared_inputs_cost_one_visit_per_node(self):
        stream = _units().filter(col("t") >= 1.0)
        for _ in range(40):  # 2**40 paths, 40 nodes
            stream = SemiJoin()(stream, stream)
        (reading,) = stream.explain().sources
        assert reading.steps[0].operator == "Filter"

    def test_explain_is_on_every_stream_type(self):
        # Through a parameter typed StreamProtocol: basedpyright checks the protocol
        # declares explain() (it narrows an annotated variable to the value's class).
        def explained(stream: StreamProtocol) -> Explanation:
            return stream.explain()

        assert (
            explained(_units().filter(col("t") >= 1.0)).sources[0].name
            == "arrow_table_v1:units"
        )


class _Loud(str):
    """A ``str`` subclass whose ``__str__`` and ``__repr__`` are user code."""

    def __str__(self) -> str:
        raise AssertionError("explain() ran user code")

    def __repr__(self) -> str:
        raise AssertionError("explain() ran user code")


class _LoudInt(int):
    """An ``int`` subclass whose ``__repr__`` is user code."""

    def __repr__(self) -> str:
        raise AssertionError("explain() ran user code")


class _LoudObject:
    """An object whose ``__str__`` and ``__repr__`` are user code."""

    def __str__(self) -> str:
        raise AssertionError("explain() ran user code")

    def __repr__(self) -> str:
        raise AssertionError("explain() ran user code")


class _LoudDict(dict[str, object]):
    """A ``dict`` subclass whose ``items()`` is user code."""

    def items(self):  # pyright: ignore[reportIncompatibleMethodOverride]  # raises on purpose
        raise AssertionError("explain() ran user code")


class _LoudMapping(Mapping[str, object]):
    """A mapping whose every access is user code."""

    def __getitem__(self, key: str) -> object:
        raise AssertionError("explain() ran user code")

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("explain() ran user code")

    def __len__(self) -> int:
        raise AssertionError("explain() ran user code")


class TestSecondReviewOfPr503:
    """CodeRabbit's second review of PR 503."""

    def test_polars_filter_text_runs_no_user_code(self):
        op = PolarsFilter(
            predicates=[pl.col("t") > 1, _Loud("t > 1")],
            constraints={"session": _Loud("s1"), "unit": 3},
        )
        (step,) = op(_units()).explain().sources[0].steps
        assert (
            step.text == 'session=<_Loud>, unit=3, <_Loud>, [(col("t")) > (dyn int: 1)]'
        )

    @pytest.mark.parametrize(
        ("value", "shown"),
        [
            (_Loud("s1"), "<_Loud>"),
            (_LoudInt(3), "<_LoudInt>"),
            (_LoudObject(), "<_LoudObject>"),
            ("s1", "'s1'"),
            (3, "3"),
            (2.5, "2.5"),
            (True, "True"),
            (b"x", "b'x'"),
            (None, "None"),
        ],
        ids=[
            "str_subclass",
            "int_subclass",
            "object",
            "str",
            "int",
            "float",
            "bool",
            "bytes",
            "none",
        ],
    )
    def test_a_constraint_value_is_written_only_by_a_builtin_repr(self, value, shown):
        op = PolarsFilter(constraints={"session": value})
        assert op.rows_dropped_text(0, [], []) == f"session={shown}"

    @pytest.mark.parametrize(
        ("predicate", "shown"),
        [
            (_Loud("t > 1"), "<_Loud>"),
            (_LoudObject(), "<_LoudObject>"),
            ([True, False], "<list>"),
        ],
        ids=["str_subclass", "object", "mask"],
    )
    def test_a_predicate_is_written_only_by_polars(self, predicate, shown):
        assert (
            PolarsFilter(predicates=[predicate]).rows_dropped_text(0, [], []) == shown
        )

    @pytest.mark.parametrize(
        "constraints",
        [_LoudDict(session="s1"), _LoudMapping(), {_Loud("session"): "s1"}],
        ids=["dict_subclass", "mapping", "str_subclass_name"],
    )
    def test_constraints_are_read_only_as_an_exact_dict(self, constraints):
        op = PolarsFilter(constraints=constraints)
        name = type(constraints).__name__
        assert op.rows_dropped_text(0, [], []) == f"<{name}>"

    def test_a_dict_pick_lists_its_misses(self):
        source = DictSource(
            [{"k": 1, "m": {"a": 1}}, {"k": 2, "m": {"b": 2}}],
            tag_columns=["k"],
            data_schema={"k": int, "m": dict[str, int]},
            tag_only_columns=True,
            source_id="maps",
        )
        (step,) = source.pick("m", "a").explain().sources[0].steps
        assert step.text == "rows whose 'm' has no key 'a' are dropped"

    def test_a_slot_is_named_by_its_name_not_its_label(self):
        tag_schema, data_schema = _units().output_schema()
        slot = SourceNode(
            name="units", tag_schema=tag_schema, data_schema=data_schema, label="shown"
        )
        (reading,) = Filter(col("t") >= 1.0)(slot).explain().sources
        assert reading.name == "units"


class _LoudName(str):
    """A ``str`` subclass whose ``__repr__`` is user code."""

    def __repr__(self) -> str:
        raise AssertionError("explain() ran user code")


class _LoudIndex(int):
    """An ``int`` subclass whose ``__repr__`` is user code."""

    def __repr__(self) -> str:
        raise AssertionError("explain() ran user code")


class TestReviewOfPr498:
    """CodeRabbit's review of PR 498 at cb3e3e1c, where explain() is now reviewed."""

    def test_a_join_of_one_input_drops_no_row(self):
        assert Join()(_units()).explain().sources[0].steps == ()
        two = Join()(_units(), _probes()).explain().sources
        assert all(len(r.steps) == 1 for r in two)

    @pytest.mark.parametrize(
        ("make", "text"),
        [
            (
                lambda: Pick(_LoudName("m"), _LoudName("a")),
                "rows whose 'm' has no key 'a' are dropped",
            ),
            (
                lambda: Index(_LoudName("xs"), _LoudIndex(1)),
                "rows whose 'xs' has no element 1 are dropped",
            ),
        ],
        ids=["pick_str_subclasses", "index_subclasses"],
    )
    def test_names_and_keys_are_written_without_user_code(self, make, text):
        op = make()
        op._mode = "map"  # pyright: ignore[reportAttributeAccessIssue]  # Pick's dict mode, as validation sets it
        assert op.rows_dropped_text(0, [], []) == text

    @pytest.mark.parametrize(
        ("other_tags", "left", "right"),
        [
            (
                ["session"],
                ["rows without a match in 'arrow_table_v1:probes' are dropped"],
                [
                    "its rows only choose rows of 'arrow_table_v1:units' and do not flow on"
                ],
            ),
            (
                [],
                [],
                [
                    "no tag is shared with 'arrow_table_v1:units': its rows choose nothing and do not flow on"
                ],
            ),
        ],
        ids=["shared_tag", "no_shared_tag"],
    )
    def test_a_semi_join_drops_rows_only_on_a_shared_tag(self, other_tags, left, right):
        table = pa.table(
            {
                "probe_session": pa.array(["s1", "s2"], pa.large_string()),
                "probe": pa.array(["imec0", "imec1"], pa.large_string()),
            }
        )
        if other_tags:
            table = table.rename_columns(["session", "probe"])
        probes = ArrowTableSource(
            table,
            tag_columns=other_tags or ["probe_session"],
            tag_only_columns=True,
            source_id="probes",
        )
        by_name = {r.name: r for r in SemiJoin()(_units(), probes).explain().sources}
        assert [s.text for s in by_name["arrow_table_v1:units"].steps] == left
        assert [s.text for s in by_name["arrow_table_v1:probes"].steps] == right

    def test_an_unbound_slot_keeps_its_steps_in_the_text(self):
        tag_schema, data_schema = _units().output_schema()
        slot = SourceNode(name="units", tag_schema=tag_schema, data_schema=data_schema)
        text = str(_units_filter_over(slot).explain())
        assert "slot 'units': no source bound" in text
        assert (
            "applied after reading: t >= 1.0 (Filter); no null in rate (DropNulls)"
            in text
        )


def _units_filter_over(stream):
    return DropNulls("rate")(Filter(col("t") >= 1.0)(stream))


class TestSecondReviewOfPr498:
    """CodeRabbit's review of PR 498 at ce5371ab."""

    @pytest.mark.parametrize(
        ("name", "shown"),
        [(_LoudName("units"), "'units'"), ("units", "'units'")],
        ids=["str_subclass", "str"],
    )
    def test_a_slot_name_is_written_without_user_code(self, name, shown):
        tag_schema, data_schema = _units().output_schema()
        slot = SourceNode(name=name, tag_schema=tag_schema, data_schema=data_schema)
        text = str(Filter(col("t") >= 1.0)(slot).explain())
        assert f"slot {shown}: no source bound" in text
