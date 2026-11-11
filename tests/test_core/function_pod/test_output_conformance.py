"""A function pod's returned values must match the declared output types (ITL-895).

pyarrow converts a Python ``float`` to an integer field by calling ``__int__``, so a pod
declared ``-> int`` that returned ``1.5`` stored ``1`` without any error. These tests
drive pods through the public interface (standalone streams, cached pods, ``PipelineJob``
on both orchestrators and both store backends) and check that:

* a value that does not conform to its declared type is rejected with
  ``OutputTypeError`` naming the pod, the output, the declared type and the value's type;
* the rejection is a row failure in a pipeline (the run continues, the observer sees it);
* the output a downstream pod receives is exactly the stored value;
* cases the developer has not decided yet (ITL-895 D1, D2, D3, D5, D7) keep today's
  behaviour, and conforming outputs hash as before.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from orcapod import PipelineJob, errors, function_pod
from orcapod.core.datagrams import Data
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.sources import DictSource


@dataclass
class Reading:
    """A struct-valued output with an integer field."""

    count: int


def make_pod(declared: Any, returns: Any, name: str = "f") -> Any:
    """Build a function pod named ``name``, declared ``-> declared``, returning ``returns``."""

    def fn(x: int) -> Any:
        return returns

    fn.__annotations__["return"] = declared
    fn.__name__ = fn.__qualname__ = name
    return function_pod(output_keys="y")(fn)


def make_source(xs: tuple[int, ...] = (1,)) -> DictSource:
    return DictSource(
        data=[{"k": f"r{x}", "x": x} for x in xs], tag_columns=["k"], source_id="src"
    )


# (declared type, returned value, value type name expected in the message)
LOSSY = [
    pytest.param(int, 1.5, "float", id="float-for-int"),
    pytest.param(int, -1.5, "float", id="negative-float-for-int"),
    pytest.param(int, 2.0, "float", id="integral-float-for-int"),
    pytest.param(int, np.float64(1.5), "float64", id="np-float64-for-int"),
    pytest.param(int, Decimal("1.5"), "Decimal", id="decimal-for-int"),
    pytest.param(int, Fraction(3, 2), "Fraction", id="fraction-for-int"),
    pytest.param(np.int8, 1.5, "float", id="float-for-np-int8"),
    pytest.param(int | None, 1.5, "float", id="float-for-optional-int"),
    pytest.param(list[int], [1, 1.5], "float", id="float-in-list-of-int"),
    pytest.param(dict[str, int], {"a": 1.5}, "float", id="float-in-dict-of-int"),
    pytest.param(Reading, Reading(count=1.5), "float", id="float-in-dataclass-field"),  # pyright: ignore[reportArgumentType]  # the wrong field type is the point of the test
    pytest.param(dt.date, 1.5, "float", id="number-for-date"),
    pytest.param(dt.datetime, 1.5, "float", id="number-for-datetime"),
    pytest.param(
        dt.date, dt.datetime(2020, 1, 1, 12), "datetime", id="datetime-for-date"
    ),
    pytest.param(str, b"x", "bytes", id="bytes-for-str"),
    pytest.param(bytes, "x", "str", id="str-for-bytes"),
    pytest.param(int, None, "NoneType", id="none-for-int"),
    pytest.param(float, None, "NoneType", id="none-for-float"),
    pytest.param(list[int], None, "NoneType", id="none-for-list"),
]


class TestStandalonePodRejects:
    @pytest.mark.parametrize(("declared", "returned", "type_name"), LOSSY)
    def test_iter_data_raises_naming_pod_output_and_types(
        self, declared: Any, returned: Any, type_name: str
    ) -> None:
        pod = make_pod(declared, returned, name="lossy_fn")
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        message = str(info.value)
        assert "'lossy_fn'" in message
        assert "'y'" in message
        assert type_name in message

    def test_error_records_what_was_returned(self) -> None:
        pod = make_pod(int, 1.5, name="half")
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        assert info.value.function_name == "half"
        assert info.value.output_key == "y"
        assert info.value.declared_type is int
        assert info.value.value == 1.5

    def test_error_is_a_type_error(self) -> None:
        pod = make_pod(int, 1.5)
        with pytest.raises(TypeError):
            list(pod.pod(make_source()).iter_data())

    def test_as_table_names_the_output_instead_of_a_union_error(self) -> None:
        """Mixed int / float results used to fail in as_table with a "Complex unions" error."""

        @function_pod(output_keys="y")
        def sometimes_half(x: int) -> int:
            return x if x == 1 else x + 0.5  # pyright: ignore[reportReturnType]  # the wrong return type is the point of the test

        with pytest.raises(errors.OutputTypeError, match="'y'"):
            sometimes_half.pod(make_source((1, 2))).as_table()

    def test_async_function_rejects(self) -> None:
        @function_pod(output_keys="y")
        async def async_half(x: int) -> int:
            return x + 0.5  # pyright: ignore[reportReturnType]  # the wrong return type is the point of the test

        with pytest.raises(errors.OutputTypeError, match="async_half"):
            list(async_half.pod(make_source()).iter_data())


class TestCachedPodRejects:
    def test_result_database_stores_nothing(self) -> None:
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="y", result_database=db)
        def half(x: int) -> int:
            return x + 0.5  # pyright: ignore[reportReturnType]  # the wrong return type is the point of the test

        with pytest.raises(errors.OutputTypeError):
            half.pod(make_source()).as_table()
        # Nothing was stored: a second pod over the same database still calls the function
        # and still raises (a stored truncated row would be served instead).
        with pytest.raises(errors.OutputTypeError):
            half.pod(make_source()).as_table()


class _CrashRecorder(NoOpObserver):
    def __init__(self) -> None:
        self.crashes: list[BaseException] = []

    def contextualize(self, *identity_path: str) -> _CrashRecorder:
        return self

    def on_data_crash(self, node_label: Any, tag: Any, data: Any, error: Any) -> None:
        self.crashes.append(error)


def _half_on_even(x: int) -> int:
    return x + 0.5 if x % 2 == 0 else x  # pyright: ignore[reportReturnType]  # the wrong return type is the point of the test


def _store(kind: str, tmp_path: Path) -> Any:
    return (
        DeltaTableDatabase(tmp_path / "db")
        if kind == "delta"
        else InMemoryArrowDatabase()
    )


ORCHESTRATORS = [
    pytest.param(SyncPipelineOrchestrator, id="sync"),
    pytest.param(AsyncPipelineOrchestrator, id="async"),
]
STORES = [pytest.param("memory", id="memory"), pytest.param("delta", id="delta")]


class TestPipelineJobRowFailure:
    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    def test_bad_row_fails_and_good_rows_are_stored(
        self, orchestrator_cls: Any, store_kind: str, tmp_path: Path
    ) -> None:
        half = function_pod(output_keys="y")(_half_on_even)
        observer = _CrashRecorder()
        job = PipelineJob(name="itl895", store=_store(store_kind, tmp_path))
        with job:
            half.pod(make_source((1, 2, 3)), label="half")
        job.run(orchestrator=orchestrator_cls(), observer=observer)

        table = job.nodes["half"].as_table()
        assert sorted(zip(table["k"].to_pylist(), table["y"].to_pylist())) == [
            ("r1", 1),
            ("r3", 3),
        ]
        assert len(observer.crashes) == 1
        assert isinstance(observer.crashes[0], errors.OutputTypeError)

    def test_fail_fast_raises_the_output_type_error(self) -> None:
        half = function_pod(output_keys="y")(_half_on_even)
        job = PipelineJob(name="itl895", store=InMemoryArrowDatabase())
        with job:
            half.pod(make_source((1, 2, 3)), label="half")
        with pytest.raises(errors.OutputTypeError):
            job.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    def test_none_for_non_optional_output_is_a_row_failure(
        self, orchestrator_cls: Any
    ) -> None:
        """A NULL used to be stored in a not-null field, leaving the node unreadable."""
        pod = make_pod(int, None, name="nothing")
        job = PipelineJob(name="itl895", store=InMemoryArrowDatabase())
        with job:
            pod.pod(make_source((1,)), label="nothing")
        job.run(orchestrator=orchestrator_cls())
        assert job.nodes["nothing"].as_table().num_rows == 0


def _float_from_int(x: int) -> float:
    return 1  # an int for a float output: accepted until ITL-895 D1 is decided


class TestDownstreamSeesStoredValue:
    """The output Data must hold what is stored: no as_dict / as_table divergence."""

    def _chain(self, received: list[Any]) -> tuple[Any, Any]:
        up = function_pod(output_keys="y")(_float_from_int)

        @function_pod(output_keys="z")
        def down(y: float) -> float:
            received.append(y)
            return y * 10

        return up, down

    def test_standalone(self) -> None:
        received: list[Any] = []
        up, down = self._chain(received)
        list(down.pod(up.pod(make_source())).iter_data())
        assert received == [1.0]
        assert type(received[0]) is float

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    def test_pipeline_job(self, orchestrator_cls: Any) -> None:
        received: list[Any] = []
        up, down = self._chain(received)
        job = PipelineJob(name="itl895", store=InMemoryArrowDatabase())
        with job:
            down.pod(up.pod(make_source(), label="up"), label="down")
        job.run(orchestrator=orchestrator_cls())
        assert received == [1.0]
        assert type(received[0]) is float

    def test_output_as_dict_matches_as_table(self) -> None:
        up = function_pod(output_keys="y")(_float_from_int)
        for _tag, data in up.pod(make_source()).iter_data():
            assert data.as_dict() == data.as_table().to_pylist()[0]
            assert type(data.as_dict()["y"]) is float


# Cases the developer has not decided yet keep today's behaviour (ITL-895 FINDINGS sec 11).
UNDECIDED_ACCEPTED = [
    pytest.param(int, np.int64(3), 3, id="D2-np-int64-for-int"),
    pytest.param(bool, np.bool_(True), True, id="D2-np-bool-for-bool"),
    pytest.param(float, 1, 1.0, id="D1-int-for-float"),
    pytest.param(float, True, 1.0, id="D3-bool-for-float"),
    pytest.param(list[int], (1, 2), [1, 2], id="D5-tuple-for-list"),
    pytest.param(np.float32, 0.1, float(np.float32(0.1)), id="D7-float-for-float32"),
]


class TestUndecidedCasesKeepTodaysBehaviour:
    @pytest.mark.parametrize(("declared", "returned", "stored"), UNDECIDED_ACCEPTED)
    def test_accepted_and_stored_as_today(
        self, declared: Any, returned: Any, stored: Any
    ) -> None:
        pod = make_pod(declared, returned)
        table = pod.pod(make_source()).as_table()
        assert table["y"].to_pylist() == [stored]


class TestConformingOutputsUnchanged:
    def test_content_hash_matches_a_plain_data(self) -> None:
        pod = make_pod(int, 1)
        [(_tag, data)] = list(pod.pod(make_source()).iter_data())
        expected = Data({"y": 1}, python_schema={"y": int}).content_hash()
        assert data.content_hash() == expected

    @pytest.mark.parametrize(
        ("declared", "returned"),
        [
            (int, 3),
            (float, 1.5),
            (bool, True),
            (str, "x"),
            (bytes, b"x"),
            (list[int], [1, 2]),
            (dict[str, int], {"a": 1}),
            (int | None, None),
            (dt.date, dt.date(2020, 1, 1)),
            (Reading, Reading(count=2)),
        ],
    )
    def test_conforming_value_round_trips(self, declared: Any, returned: Any) -> None:
        pod = make_pod(declared, returned)
        [(_tag, data)] = list(pod.pod(make_source()).iter_data())
        assert data.as_dict()["y"] == returned


class TestConversionFailuresNameTheOutput:
    """A value of the right type that cannot be stored is reported the same way."""

    @pytest.mark.parametrize(
        ("declared", "returned"),
        [
            pytest.param(np.int8, 300, id="out-of-range-for-int8"),
            pytest.param(dt.datetime, dt.datetime(2020, 1, 1), id="naive-datetime"),
        ],
    )
    def test_raises_output_type_error_that_is_a_value_error(
        self, declared: Any, returned: Any
    ) -> None:
        pod = make_pod(declared, returned, name="unstorable")
        with pytest.raises(errors.OutputTypeError, match="'unstorable'") as info:
            list(pod.pod(make_source()).iter_data())
        assert isinstance(info.value, ValueError)
        assert info.value.output_key == "y"

    def test_error_survives_pickling(self) -> None:
        import pickle

        error = errors.OutputTypeError("f", "y", int, 1.5)
        restored = pickle.loads(pickle.dumps(error))
        assert str(restored) == str(error)
        assert restored.declared_type is int
