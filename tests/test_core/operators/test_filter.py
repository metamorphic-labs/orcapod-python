"""The ``Filter`` operator (ITL-1378): ``stream.filter(...)``, a step of the pipeline.

Through the public interface: ``stream.filter(...)`` or ``Filter(condition)(stream)``,
read back through the stream's methods. The kernel's semantics term by term are in
``test_filter_mask.py``; the arrangement, order and role matrix in
``test_several_items.py``.
"""

from __future__ import annotations

import itertools
import json
import struct
import warnings
from decimal import Decimal

import pyarrow as pa
import pytest

from orcapod import PipelineJob, col
from orcapod.core.operators import PROVISIONAL_OPERATORS, Filter, SelectDataColumns
from orcapod.core.operators.provisional import reset_provisional_operator_warnings
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError, ProvisionalHashWarning
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.serialization import _build_operator_registry  # pyright: ignore[reportPrivateUsage]  # the registry a saved job loads operators through
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.types import CacheMode, Condition

from .several_items import async_rows, rows_fingerprint

pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")

NAN = float("nan")


def _units(**kwargs: object) -> ArrowTableSource:
    """The design's example: tags ``session``, ``unit``; data ``t``, ``rate``, ``probe``."""
    table = pa.table(
        {
            "session": pa.array(
                ["s1", "s1", "s1", "s2", "s2", "s3"], pa.large_string()
            ),
            "unit": pa.array([3, 7, 9, 3, 7, 3], pa.int64()),
            "t": pa.array([0.5, 1.2, 2.0, 0.7, NAN, 3.1], pa.float64()),
            "rate": pa.array([4.0, None, 2.5, 6.1, 0.0, None], pa.float64()),
            "probe": pa.array(
                ["imec0", "imec1", "imec0", "imec1", "imec0", "imec1"],
                pa.large_string(),
            ),
        }
    )
    options: dict[str, object] = {"tag_only_columns": True, "source_id": "units"}
    options.update(kwargs)
    return ArrowTableSource(table, tag_columns=["session", "unit"], **options)  # pyright: ignore[reportArgumentType]


def _kept(stream) -> list[tuple[str, int]]:
    table = stream.as_table()
    return list(
        zip(table.column("session").to_pylist(), table.column("unit").to_pylist())
    )


S1 = [("s1", 3), ("s1", 7), ("s1", 9)]


class TestSpelling:
    """The table of section 1 of the design, through ``stream.filter``."""

    def test_keyword(self):
        assert _kept(_units().filter(session="s1")) == S1

    def test_several_keywords_and_mapping(self):
        expected = [("s1", 3), ("s1", 9)]
        assert _kept(_units().filter(session="s1", probe="imec0")) == expected
        assert _kept(_units().filter({"session": "s1", "probe": "imec0"})) == expected
        assert _kept(_units().filter({"probe": "imec0"}, session="s1")) == expected

    def test_is_in(self):
        assert _kept(_units().filter(col("unit").is_in([3, 7]))) == [
            ("s1", 3),
            ("s1", 7),
            ("s2", 3),
            ("s2", 7),
            ("s3", 3),
        ]

    def test_comparison_keeps_nan(self):
        assert _kept(_units().filter(col("t") >= 1.0)) == [
            ("s1", 7),
            ("s1", 9),
            ("s2", 7),
            ("s3", 3),
        ]

    def test_between(self):
        assert _kept(_units().filter(col("t").between(0.5, 2.0))) == [
            ("s1", 3),
            ("s1", 7),
            ("s1", 9),
            ("s2", 3),
        ]

    def test_null_tests(self):
        assert _kept(_units().filter(col("rate").is_null())) == [("s1", 7), ("s3", 3)]
        assert len(_kept(_units().filter(col("rate").is_not_null()))) == 4

    def test_several_positional_and_keyword(self):
        assert _kept(_units().filter(col("t") >= 1.0, session="s1")) == [
            ("s1", 7),
            ("s1", 9),
        ]
        assert _kept(
            _units().filter(col("t") >= 1.0, col("unit").is_in([7, 9]), session="s1")
        ) == [("s1", 7), ("s1", 9)]

    def test_and(self):
        assert _kept(_units().filter((col("t") >= 1.0) & (col("session") == "s1"))) == [
            ("s1", 7),
            ("s1", 9),
        ]

    def test_label_names_the_node_not_a_column(self):
        stream = _units().filter(session="s1", label="first_session")
        assert _kept(stream) == S1

    def test_no_condition_refused(self):
        with pytest.raises(ValueError):
            _units().filter()


class TestTreeSpelling:
    """``|``, ``~`` and column against column (after the workshop subset)."""

    def test_negation(self):
        assert _kept(_units().filter(~col("unit").is_in([3, 7]))) == [("s1", 9)]

    def test_or(self):
        stream = _units().filter((col("session") == "s3") | (col("t") < 0.6))
        assert _kept(stream) == [("s1", 3), ("s3", 3)]

    def test_column_against_column(self):
        stream = _units().filter(col("rate") > col("t"))
        assert _kept(stream) == [("s1", 3), ("s1", 9), ("s2", 3)]


class TestRoles:
    """Tag, data and data-tag columns are named alike."""

    def test_data_tag(self):
        source = _units(tag_only_columns=["session"])  # ``unit`` is a data-tag
        assert "unit" in source.keys()[1]
        assert _kept(source.filter(col("unit") > 3)) == [
            ("s1", 7),
            ("s1", 9),
            ("s2", 7),
        ]

    def test_conditions_on_every_role_together(self):
        source = _units(tag_only_columns=["session"])
        stream = source.filter(
            col("unit") >= 7, col("rate").is_not_null(), session="s1"
        )
        assert _kept(stream) == [("s1", 9)]

    def test_system_columns_are_not_named(self):
        source = _units()
        system = [
            name
            for name in source.as_table(all_info=True).column_names
            if name.startswith("_")
        ]
        assert system
        for name in system:
            with pytest.raises(InputValidationError):
                source.filter(col(name).is_null())


class TestPassThrough:
    def test_schema_is_the_input_schema(self):
        source = _units()
        stream = source.filter(col("t") >= 1.0)
        assert stream.output_schema() == source.output_schema()
        assert stream.output_schema(all_info=True) == source.output_schema(
            all_info=True
        )
        assert stream.keys() == source.keys()

    def test_prediction_equals_the_computed_schema(self):
        source = _units(tag_only_columns=["session"])
        op = Filter(col("t") >= 1.0)
        computed = op.unary_static_process(source)
        for columns in (None, {"system_tags": True}, {"source": True}):
            assert op(source).output_schema(columns=columns) == computed.output_schema(
                columns=columns
            )

    def test_is_not_null_does_not_narrow(self):
        source = _units()
        stream = source.filter(col("rate").is_not_null())
        assert stream.output_schema()[1]["rate"] == source.output_schema()[1]["rate"]
        table = stream.as_table()
        assert table.schema.field("rate").nullable

    def test_system_tags_source_info_and_order_pass_through(self):
        source = _units()
        full = source.as_table(all_info=True)
        kept = source.filter(col("unit").is_in([7, 9])).as_table(all_info=True)
        assert kept.column_names == full.column_names
        assert kept.schema == full.schema
        expected = full.filter(pa.array([False, True, True, False, True, False]))
        # ``t`` holds a NaN, which ``equals`` never matches: compared by its bits.
        assert kept.drop_columns(["t"]).equals(expected.drop_columns(["t"]))
        assert kept.column("t").combine_chunks().view(pa.int64()) == expected.column(
            "t"
        ).combine_chunks().view(pa.int64())

    def test_values_bit_for_bit(self):
        signalling = bytes.fromhex("0100807f")
        table = pa.table(
            {
                "k": pa.array([1, 2, 3], pa.int64()),
                "f32": pa.Array.from_buffers(
                    pa.float32(),
                    3,
                    [None, pa.py_buffer(signalling + struct.pack("<ff", 1.0, 2.0))],  # pyright: ignore[reportArgumentType]  # no validity buffer: no nulls
                ),
                "ns": pa.array([1, 2, 3], pa.time64("ns")),
                "dec": pa.array(
                    [Decimal("1.5"), Decimal("2.5"), Decimal("3.5")],
                    pa.decimal256(40, 1),
                ),
            }
        )
        source = ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="bits"
        )
        out = source.filter(col("k").is_in([1, 3])).as_table()
        f32 = out.column("f32").combine_chunks().buffers()[1]
        assert f32 is not None
        assert f32.to_pybytes()[:8] == signalling + struct.pack("<f", 2.0)
        assert (
            out.column("ns").to_pylist() == table.column("ns").take([0, 2]).to_pylist()
        )
        assert out.column("ns").type == pa.time64("ns")
        assert out.column("dec").type == pa.decimal256(40, 1)
        assert out.column("dec").to_pylist() == [Decimal("1.5"), Decimal("3.5")]

    def test_each_rows_data_context_passes_through(self):
        """A raw stream may hold several data contexts; the kept rows keep theirs, as
        ``SelectDataColumns`` keeps them (review of PR 497, finding 1)."""
        table = pa.table(
            {
                "k": pa.array([1, 2, 3], pa.int64()),
                "v": pa.array([10, 20, 30], pa.int64()),
                "w": pa.array([4, 5, 6], pa.int64()),
                "_context_key": pa.array(
                    ["held-a", "held-b", "held-c"], pa.large_string()
                ),
            }
        )
        stream = ArrowTableStream(table, tag_columns=["k"])
        kept = Filter(col("k") >= 2)(stream).as_table(all_info=True)
        assert kept["_context_key"].to_pylist() == ["held-b", "held-c"]
        selected = SelectDataColumns(["v", "w"])(stream).as_table(all_info=True)
        assert kept.equals(selected.filter(pa.array([False, True, True])))

    def test_no_meta_columns(self):
        table = pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "v": pa.array([10, 20], pa.int64()),
                "__note": pa.array(["a", "b"], pa.large_string()),
            }
        )
        stream = ArrowTableStream(table, tag_columns=["k"])
        out = Filter(col("v") > 10)(stream)
        assert "__note" not in out.as_table(all_info=True).column_names
        assert "__note" not in out.output_schema(all_info=True)[1]


class TestZeroRows:
    def test_no_matching_row(self):
        source = _units()
        stream = source.filter(session="none")
        table = stream.as_table(all_info=True)
        assert table.num_rows == 0
        assert table.schema == source.as_table(all_info=True).schema

    def test_zero_row_input(self):
        empty = _units().filter(session="none")
        stream = empty.filter(col("t") >= 1.0)
        assert stream.as_table().num_rows == 0
        assert stream.as_table().schema == empty.as_table().schema


class TestRefusals:
    def test_missing_column_at_build(self):
        with pytest.raises(InputValidationError, match="nope"):
            _units().filter(nope=1)

    def test_value_of_another_kind_at_build(self):
        with pytest.raises(InputValidationError):
            _units().filter(t=1)  # an int for a float column
        with pytest.raises(InputValidationError):
            _units().filter(unit=True)

    def test_out_of_range_when_applied(self):
        table = pa.table(
            {"k": pa.array([1], pa.int8()), "v": pa.array([1], pa.int64())}
        )
        source = ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="narrow"
        )
        stream = source.filter(k=300)  # an int: the Python check passes
        with pytest.raises(InputValidationError):
            stream.as_table()

    @pytest.mark.parametrize("mask", [[True, False], True, (True,)])
    def test_a_mask_by_position_is_refused(self, mask: object):
        with pytest.raises(TypeError):
            _units().filter(mask)  # pyright: ignore[reportArgumentType]


class TestIdentity:
    def test_keyword_order(self):
        a = Filter(Condition.of(session="s1", probe="imec0"))
        b = Filter(Condition.of(probe="imec0", session="s1"))
        assert a.content_hash() == b.content_hash()
        source = _units()
        assert a(source).pipeline_hash() == b(source).pipeline_hash()

    def test_argument_and_operand_order(self):
        terms = [col("t") >= 1.0, col("unit").is_in([7, 3]), col("rate").is_null()]
        hashes = {
            _units().filter(*order).content_hash().to_string()
            for order in itertools.permutations(terms)
        }
        hashes.add(
            _units()
            .filter(col("unit").is_in([3, 7]), *terms[::2])
            .content_hash()
            .to_string()
        )
        assert len(hashes) == 1

    def test_values_of_different_types_differ(self):
        one = Filter(col("x") == 1).content_hash()
        assert one != Filter(col("x") == 1.0).content_hash()
        assert one != Filter(col("x") == True).content_hash()  # noqa: E712  # a condition, not a comparison

    def test_signed_zero_agrees(self):
        assert (
            Filter(col("x") == 0.0).content_hash()
            == Filter(col("x") == -0.0).content_hash()
        )

    def test_a_condition_changes_every_hash_downstream(self):
        source = _units()
        a = source.filter(session="s1").drop_nulls("rate")
        b = source.filter(session="s2").drop_nulls("rate")
        assert a.content_hash() != b.content_hash()
        assert a.pipeline_hash() != b.pipeline_hash()

    def test_identity_structure(self):
        condition = col("t") >= 1.0
        assert Filter(condition).identity_structure() == (
            "Filter",
            condition.canonical_form(),
        )


class TestConfig:
    @pytest.mark.parametrize(
        "condition",
        [
            Condition.of(session="s1", probe="imec0"),
            Condition.of(col("t") >= 1.0, col("unit").is_in([3, 7]), session="s1"),
            Condition.of(col("rate").is_null()),
        ],
        ids=["equalities", "terms", "null_test"],
    )
    def test_round_trip(self, condition: Condition):
        op = Filter(condition)
        config = op.to_config()
        assert config["class_name"] == "Filter"
        rebuilt = _build_operator_registry()["Filter"].from_config(config)
        assert rebuilt.condition == condition
        assert rebuilt.content_hash() == op.content_hash()
        assert _kept(rebuilt(_units())) == _kept(op(_units()))

    def test_equalities_are_saved_in_canonical_order(self):
        """The saved form never follows the order given (ITL-998, ITL-1271); a config
        saved in the order given loads to the same operator."""
        forward = Filter(Condition.of(session="s1", probe="imec0"))
        backward = Filter(Condition.of(probe="imec0", session="s1"))

        assert json.dumps(forward.to_config()) == json.dumps(backward.to_config())
        assert list(forward.to_config()["config"]["condition"]) == ["probe", "session"]
        earlier = forward.to_config()
        earlier["config"]["condition"] = {"session": "s1", "probe": "imec0"}
        rebuilt = _build_operator_registry()["Filter"].from_config(earlier)
        assert rebuilt.content_hash() == forward.content_hash()


class TestProvisional:
    def test_listed_and_marked(self):
        assert "Filter" in PROVISIONAL_OPERATORS
        assert Filter.PROVISIONAL

    def test_building_one_warns_once(self):
        reset_provisional_operator_warnings()
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                Filter(Condition.of(session="s1"))
                Filter(Condition.of(session="s2"))
            provisional = [
                w for w in caught if issubclass(w.category, ProvisionalHashWarning)
            ]
            assert len(provisional) == 1
            assert "Filter" in str(provisional[0].message)
        finally:
            reset_provisional_operator_warnings()


class TestAsync:
    @pytest.mark.parametrize(
        "condition",
        [
            Condition.of(session="s1"),
            Condition.of(col("t") >= 1.0, col("unit").is_in([3, 7])),
            Condition.of(col("rate").is_null(), session="s3"),
        ],
        ids=["equality", "terms", "null_test"],
    )
    def test_async_rows_equal_sync_rows(self, condition: Condition):
        for options in ({}, {"tag_only_columns": ["session"]}):
            source = _units(**options)
            sync = rows_fingerprint(list(Filter(condition)(source).iter_data()))
            assert rows_fingerprint(async_rows(Filter(condition), source)) == sync

    def test_async_refuses_what_sync_refuses(self):
        table = pa.table(
            {"k": pa.array([1], pa.int8()), "v": pa.array([1], pa.int64())}
        )
        source = ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="narrow"
        )
        with pytest.raises(InputValidationError):
            async_rows(Filter(Condition.of(k=300)), source)


class TestInAJob:
    @pytest.mark.parametrize(
        "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
    )
    def test_a_value_its_column_cannot_hold_raises_with_no_rows(self, orchestrator):
        """The job path refuses what the sync path refuses, also with no rows to read a
        type from (review of PR 497, finding 5): the node checks the input's Arrow schema."""
        table = pa.table(
            {"k": pa.array([1], pa.int8()), "v": pa.array([1], pa.int64())}
        )
        job = PipelineJob(name="narrow", database=InMemoryArrowDatabase())
        with job:
            source = ArrowTableSource(
                table, tag_columns=["k"], tag_only_columns=True, source_id="narrow"
            )
            empty = source.filter(v=999, label="none")
            Filter(Condition.of(k=300))(empty, label="kept")
        with pytest.raises(InputValidationError, match="int8"):
            job.run(orchestrator=orchestrator())

    def test_a_job_node_reads_not_computed_before_its_first_run(self):
        """A job node shows its current run (ITL-1095): before the first run its head
        reads not computed, although the same filter outside a job shows its input's
        matching rows (decision 13 is for the plain stream; review finding 6)."""
        job = PipelineJob(name="head", database=InMemoryArrowDatabase())
        with job:
            stream = _units().filter(session="s1", label="kept")
        assert stream.head().status.value == "rows"
        assert job.nodes["kept"].head().status.value == "not_computed"
        job.run()
        assert _kept(job.nodes["kept"].head()) == S1

    @pytest.mark.parametrize(
        "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
    )
    def test_same_rows_inside_and_outside_a_job(self, orchestrator):
        outside = _kept(_units().filter(col("t") >= 1.0, session="s1"))
        database = InMemoryArrowDatabase()
        job = PipelineJob(name="filter", database=database)
        with job:
            _units().filter(col("t") >= 1.0, session="s1", label="kept")
        job.nodes["kept"].attach_databases(
            pipeline_database=database.at("filter"), cache_mode=CacheMode.LOG
        )
        job.run(orchestrator=orchestrator())
        assert _kept(job.nodes["kept"]) == outside

    def test_drop_nulls_after_filter_narrows(self):
        stream = _units().filter(col("rate").is_not_null()).drop_nulls("rate")
        assert stream.output_schema()[1]["rate"] is float


class TestHead:
    """Decision 13 of ITL-1374: a Filter's head evaluates its condition over the rows its
    input's head shows; it never computes, and the header says which rows it looked at."""

    def test_over_a_source_shows_the_matching_rows(self):
        stream = _units().filter(col("t") >= 1.0)
        view = stream.head()
        assert view.status.value == "rows"
        assert _kept(view) == [("s1", 7), ("s1", 9), ("s2", 7), ("s3", 3)]
        assert "all 4 matching rows of its input's 6" in view.header()

    def test_matches_among_a_shorter_head_say_so(self):
        view = _units().filter(col("t") >= 1.0).head(3)
        assert _kept(view) == [("s1", 7), ("s1", 9)]
        assert "2 matching rows among the 3 its input's head shows" in view.header()
        assert "not computed" in view.header()

    def test_no_match_shows_typed_columns(self):
        source = _units()
        view = source.filter(session="none").head()
        assert view.table.num_rows == 0
        assert view.table.schema == source.head().table.schema

    def test_equals_the_computed_rows_with_every_column_group(self):
        stream = _units().filter(col("unit").is_in([7, 9]))
        for columns in (
            None,
            {"system_tags": True, "source": True},
            {"sort_by_tags": True},
        ):
            head = stream.head(columns=columns).table
            # A fresh stream, so the computed head is not the one above.
            computed = _units().filter(col("unit").is_in([7, 9]))
            computed.run()
            expected = computed.head(columns=columns).table
            # ``t`` holds a NaN, which ``equals`` never matches: compared as text.
            assert head.schema == expected.schema
            assert repr(head.to_pylist()) == repr(expected.to_pylist())

    def test_does_not_compute_the_stream(self):
        stream = _units().filter(session="s1")
        _ = stream.head()
        assert (
            stream.head().header().endswith("not computed; run() computes every row)")
            is False
        )
        assert stream._cached_stream is None  # pyright: ignore[reportAttributeAccessIssue,reportPrivateUsage]  # nothing was computed

    def test_over_a_function_pod_reads_not_computed(self):
        from orcapod import function_pod

        calls: list[int] = []

        @function_pod(output_keys="doubled")
        def double(t: float | None) -> float:
            calls.append(1)
            return 2 * (t or 0.0)

        stream = Filter(col("doubled") > 1.0)(
            double.pod(_units().select_data_columns(["t"]))
        )
        view = stream.head()
        assert view.status.value == "not_computed"
        assert view.table.num_rows == 0
        assert calls == []

    def test_an_ill_typed_condition_shows_nothing_and_raises_at_the_run(self):
        table = pa.table(
            {"k": pa.array([1], pa.int8()), "v": pa.array([1], pa.int64())}
        )
        source = ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="narrow"
        )
        stream = source.filter(k=300)
        assert stream.head().status.value == "not_computed"
        with pytest.raises(InputValidationError):
            stream.run()
