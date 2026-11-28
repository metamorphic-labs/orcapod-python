"""Tests for the unified recording path in AbstractPipelineBase.

These tests verify the new _record_invocation / _invocation_lut / _source_streams
mechanics by exercising them through Pipeline (concrete subclass).
"""

from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.pipeline import Pipeline

from ..test_core.operators.several_items import (
    DATA_FIRST_REVERSED,
    MAIN,
    P,
    PLAIN,
    Arrangement,
    make_source,
)


def _src(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(["a", "b"], type=pa.large_string()),
            data_col: pa.array([1, 2], type=pa.int64()),
        }
    )
    return ArrowTableSource(
        table,
        tag_columns=[tag_col],
        tag_only_columns=True,
        infer_nullable=True,
        label=data_col,
    )


def _fn(name: str = "double") -> FunctionPod:
    def fn(value: int) -> int:
        return value * 2

    fn.__name__ = name
    return FunctionPod(PythonDataFunction(fn, output_keys="result"))


class TestRecordFunctionPodInvocation:
    def test_record_function_pod_adds_to_invocation_lut(self):
        """After recording a function pod, _invocation_lut must contain the invocation."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            pod(stream, label="out")
        # _invocation_lut is the new field; _node_lut is the legacy compiled field
        assert len(pipeline._invocation_lut) == 1

    def test_record_function_pod_captures_source_stream(self):
        """The upstream concrete stream must land in _source_streams."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            pod(stream, label="out")
        stream_hash = stream.content_hash().to_string()
        assert stream_hash in pipeline._source_streams

    def test_invocation_lut_additive_across_with_blocks(self):
        """Opening a second with-block appends to _invocation_lut, not replaces."""

        # pod1 consumes 'value' and produces 'result'; pod2 consumes 'result'
        def fn1(value: int) -> int:
            return value * 2

        fn1.__name__ = "fn1"
        pod1 = FunctionPod(PythonDataFunction(fn1, output_keys="result"))

        def fn2(result: int) -> int:
            return result * 3

        fn2.__name__ = "fn2"
        pod2 = FunctionPod(PythonDataFunction(fn2, output_keys="result2"))

        s = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            out1 = pod1(s, label="step1")
        with pipeline:
            pod2(out1, label="step2")
        assert len(pipeline._invocation_lut) == 2

    def test_compile_rebuilds_persistent_node_map_from_scratch(self):
        """compile() must produce correct _persistent_node_map from _invocation_lut."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        assert "out" in pipeline._nodes
        assert len(pipeline._persistent_node_map) == 2  # 1 source + 1 function node

    def test_compile_creates_source_node_for_unregistered_upstream(self):
        """Streams with no recorded invocation must become SourceNode at compile time."""
        from orcapod.core.nodes.source_node import SourceNode

        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        source_hash = stream.content_hash().to_string()
        assert isinstance(pipeline._persistent_node_map[source_hash], SourceNode)


class TestSourceNodeNaming:
    """Slot labels separate roles from the datasets that fill them."""

    def test_dataset_id_does_not_set_the_slot_name(self):
        source = ArrowTableSource(
            pa.table({"key": ["a", "b"], "value": [1, 2]}),
            tag_columns=["key"],
            tag_only_columns=True,
            source_id="my_dataset",
            infer_nullable=True,
        )
        pipeline = Pipeline(name="test")
        with pipeline:
            _fn()(source, label="out")
        assert set(pipeline.source_pods) == {"ArrowTableSource"}
        assert source.source_id == "arrow_table_v1:my_dataset"

    def test_slot_names_are_the_labels_given_to_the_sources(self):
        left, right = _src("key", "value"), _src("key", "score")
        pipeline = Pipeline(name="test")
        with pipeline:
            Join()(left, right, label="joined")
        assert set(pipeline.source_pods) == {"value", "score"}

    def test_explicit_slot_names_override_source_labels(self):
        left, right = _src("key", "value"), _src("key", "score")
        pipeline = Pipeline(name="test")
        with pipeline:
            Join()(
                left.as_slot("left_input"), right.as_slot("right_input"), label="joined"
            )
        assert set(pipeline.source_pods) == {"left_input", "right_input"}
        assert (left.label, right.label) == ("value", "score")

    @pytest.mark.parametrize("reverse", [False, True])
    def test_same_source_id_different_schema_raises_inconsistent_source_error(
        self, reverse: bool
    ) -> None:
        """Two sources with the same source_id but different schemas raise an error."""
        from orcapod.errors import InconsistentSourceError

        main = make_source(MAIN, source_id="shared_id")
        partner = make_source(
            P,
            Arrangement(tuple(reversed(P.tags)), DATA_FIRST_REVERSED, PLAIN),
            source_id="shared_id",
        )
        inputs = (partner, main) if reverse else (main, partner)
        join = Join()
        pipeline = Pipeline(name="test", auto_compile=False)
        with pytest.raises(InconsistentSourceError, match="shared_id"):
            with pipeline:
                join(*inputs, label="joined")


class TestRecordOperatorPodInvocation:
    def test_record_operator_pod_adds_to_invocation_lut(self):
        """After recording an operator, _invocation_lut must contain the invocation."""
        join = Join()
        s1 = _src("key", "value")
        s2 = _src("key", "score")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            join(s1, s2, label="joined")
        assert len(pipeline._invocation_lut) == 1

    def test_record_operator_captures_both_source_streams(self):
        join = Join()
        s1 = _src("key", "value")
        s2 = _src("key", "score")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            join(s1, s2, label="joined")
        h1 = s1.content_hash().to_string()
        h2 = s2.content_hash().to_string()
        assert h1 in pipeline._source_streams
        assert h2 in pipeline._source_streams
