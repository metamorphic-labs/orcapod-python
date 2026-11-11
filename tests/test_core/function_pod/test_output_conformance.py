"""A function pod's returned values must match the declared output types (ITL-895).

pyarrow converts a Python ``float`` to an integer field by calling ``__int__``, so a pod
declared ``-> int`` that returned ``1.5`` stored ``1`` without any error. These tests
drive pods through the public interface (standalone streams, cached pods, ``PipelineJob``
on both orchestrators and both store backends) and check the developer's decisions
(2026-11-11, D1-D7 and ITL-901):

* a value that does not conform to its declared type is rejected with
  ``OutputTypeError`` naming the pod, the output, the declared type and the value's type,
  and saying what to return instead;
* the rejection is a row failure in a pipeline (the run continues, the observer sees it);
* a few near misses are converted, with an ``OutputConversionWarning`` that is shown once
  per function, output and conversion, however many rows;
* the output a downstream pod receives is exactly the stored value;
* conforming outputs and same-kind NumPy scalars are accepted silently and hash as before.
"""

from __future__ import annotations

import collections
import datetime as dt
import warnings
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
    pytest.param(float, 0, "int", id="D1-int-for-float"),
    pytest.param(float, np.int64(1), "int64", id="D1-np-int64-for-float"),
    pytest.param(np.float32, 1, "int", id="D1-int-for-np-float32"),
    pytest.param(float, True, "bool", id="D3-bool-for-float"),
    pytest.param(float, np.bool_(True), "bool", id="D3-np-bool-for-float"),
    pytest.param(list[int], {1, 2}, "set", id="ITL901-set-for-list"),
    pytest.param(
        list[int], frozenset({1}), "frozenset", id="ITL901-frozenset-for-list"
    ),
    pytest.param(list[int], iter([1, 2]), "iterator", id="Q2-iterator-for-list"),
    pytest.param(
        list[int], (i for i in range(2)), "generator", id="Q2-generator-for-list"
    ),
    pytest.param(
        list[int],
        np.array([1.5, 2.0]),
        "float64",
        id="Q2-float-ndarray-for-list-of-int",
    ),
    pytest.param(
        list[int], collections.deque([1, 1.5]), "float", id="Q2-float-in-deque"
    ),
    pytest.param(list[int], {"a": 1}, "dict", id="mapping-for-list"),
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

    @pytest.mark.parametrize(
        ("declared", "returned", "hint"),
        [
            pytest.param(
                float, 0, "Return a float instead: 0.0 or float(value)", id="D1"
            ),
            pytest.param(
                float, True, "Return a float instead: 1.0 or float(value)", id="D3"
            ),
            pytest.param(
                int, 1.5, "Return an int instead: round(value)", id="float-for-int"
            ),
            pytest.param(list[int], {2, 1}, "sorted(value)", id="ITL901"),
            pytest.param(list[int], iter([1]), "list(value)", id="iterator"),
            pytest.param(int, None, "int | None", id="D4"),
        ],
    )
    def test_message_says_what_to_return_instead(
        self, declared: Any, returned: Any, hint: str
    ) -> None:
        pod = make_pod(declared, returned)
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        assert hint in str(info.value)

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


# (declared type, returned value, stored value, returned type name, words of the message)
CONVERTED = [
    pytest.param(
        list[int],
        (1, 2),
        [1, 2],
        "tuple",
        "converted it to a list",
        id="D5-tuple-for-list",
    ),
    pytest.param(
        dt.date,
        dt.datetime(2020, 1, 1, 12),
        dt.date(2020, 1, 1),
        "datetime.datetime",
        "dropped the time of day",
        id="D6-datetime-for-date",
    ),
    pytest.param(
        np.float32,
        0.1,
        float(np.float32(0.1)),
        "float",
        "rounded it to 32 bits",
        id="D7-float-for-float32",
    ),
    pytest.param(
        list[int],
        collections.deque([1, 2]),
        [1, 2],
        "collections.deque",
        "converted it to a list",
        id="Q2-deque-for-list",
    ),
    pytest.param(
        list[int],
        np.array([1, 2]),
        [1, 2],
        "numpy.ndarray",
        "converted it to a list",
        id="Q2-int-ndarray-for-list",
    ),
    pytest.param(
        dict[str, list[int]],
        {"a": (1,)},
        {"a": [1]},
        "tuple",
        "at [*]",
        id="D5-nested-names-the-place",
    ),
]


class TestConvertedWithWarning:
    @pytest.mark.parametrize(
        ("declared", "returned", "stored", "type_name", "words"), CONVERTED
    )
    def test_warns_and_stores_the_converted_value(
        self, declared: Any, returned: Any, stored: Any, type_name: str, words: str
    ) -> None:
        pod = make_pod(declared, returned, name="near_miss")
        with pytest.warns(errors.OutputConversionWarning) as record:
            table = pod.pod(make_source()).as_table()
        message = str(record[0].message)
        assert "'near_miss'" in message
        assert "'y'" in message
        assert type_name in message
        assert words in message
        assert "to avoid this warning" in message
        stored_value = table["y"].to_pylist()[0]
        if isinstance(stored, dict):
            stored_value = dict(stored_value)
        assert stored_value == stored

    def test_warning_never_shows_the_value(self) -> None:
        """The message must be the same on every row, so it is shown once per run."""
        first = make_pod(list[int], (1, 2), name="same")
        second = make_pod(list[int], (3, 4, 5), name="same")
        with pytest.warns(errors.OutputConversionWarning) as one:
            first.pod(make_source()).as_table()
        with pytest.warns(errors.OutputConversionWarning) as two:
            second.pod(make_source()).as_table()
        assert str(one[0].message) == str(two[0].message)

    def test_warning_records_what_was_converted(self) -> None:
        pod = make_pod(dt.date, dt.datetime(2020, 1, 1, 12), name="day")
        with pytest.warns(errors.OutputConversionWarning) as record:
            pod.pod(make_source()).as_table()
        warning = record[0].message
        assert isinstance(warning, errors.OutputConversionWarning)
        assert warning.function_name == "day"
        assert warning.output_key == "y"
        assert warning.declared_type is dt.date
        assert warning.returned_type is dt.datetime

    def test_one_value_with_two_conversions_warns_twice(self) -> None:
        pod = make_pod(list[dt.date], (dt.datetime(2020, 1, 1, 3),))
        with pytest.warns(errors.OutputConversionWarning) as record:
            table = pod.pod(make_source()).as_table()
        messages = sorted(str(w.message) for w in record)
        assert len(messages) == 2
        assert any("converted it to a list" in m for m in messages)
        assert any("dropped the time of day" in m for m in messages)
        assert table["y"].to_pylist() == [[dt.date(2020, 1, 1)]]

    def test_shown_once_for_many_rows_under_the_default_filter(self) -> None:
        pod = make_pod(list[int], (1, 2), name="many")
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("default")
            table = pod.pod(make_source(tuple(range(1000)))).as_table()
        assert table.num_rows == 1000
        assert [w.category for w in record] == [errors.OutputConversionWarning]

    def test_error_filter_makes_it_a_row_failure(self) -> None:
        pod = make_pod(list[int], (1, 2), name="strict")
        observer = _CrashRecorder()
        job = PipelineJob(name="itl895", store=InMemoryArrowDatabase())
        with job:
            pod.pod(make_source((1, 2)), label="strict")
        with warnings.catch_warnings():
            warnings.simplefilter("error", errors.OutputConversionWarning)
            job.run(orchestrator=SyncPipelineOrchestrator(), observer=observer)
        assert job.nodes["strict"].as_table().num_rows == 0
        assert len(observer.crashes) == 2
        assert all(
            isinstance(c, errors.OutputConversionWarning) for c in observer.crashes
        )

    def test_warning_survives_pickling(self) -> None:
        import pickle

        warning = errors.OutputConversionWarning(
            "f", "y", "", list[int], tuple, "converted it to a list", "Return a list"
        )
        restored = pickle.loads(pickle.dumps(warning))
        assert str(restored) == str(warning)
        assert restored.returned_type is tuple


ACCEPTED_SILENTLY = [
    pytest.param(int, np.int64(3), 3, id="D2-np-int64-for-int"),
    pytest.param(bool, np.bool_(True), True, id="D2-np-bool-for-bool"),
    pytest.param(float, np.float64(1.5), 1.5, id="D2-np-float64-for-float"),
    pytest.param(float, np.float32(0.5), 0.5, id="D2-np-float32-for-float"),
    pytest.param(np.int8, np.int64(3), 3, id="D2-np-int64-for-np-int8"),
    pytest.param(np.float32, np.float32(0.5), 0.5, id="np-float32-for-np-float32"),
    pytest.param(np.float32, np.float16(0.5), 0.5, id="D2-np-float16-for-np-float32"),
    pytest.param(int, 3, 3, id="int-for-int"),
    pytest.param(list[int], [1, 2], [1, 2], id="list-for-list"),
    pytest.param(dt.date, dt.date(2020, 1, 1), dt.date(2020, 1, 1), id="date-for-date"),
]


class TestAcceptedSilently:
    @pytest.mark.parametrize(("declared", "returned", "stored"), ACCEPTED_SILENTLY)
    def test_no_warning_and_value_stored(
        self, declared: Any, returned: Any, stored: Any
    ) -> None:
        pod = make_pod(declared, returned)
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            table = pod.pod(make_source()).as_table()
        assert [w for w in record if w.category is errors.OutputConversionWarning] == []
        assert table["y"].to_pylist() == [stored]


# (declared type, returned value, value a downstream pod receives)
DOWNSTREAM = [
    pytest.param(list[int], (1, 2), [1, 2], id="D5-tuple-becomes-list"),
    pytest.param(
        dt.date,
        dt.datetime(2020, 1, 1, 12),
        dt.date(2020, 1, 1),
        id="D6-datetime-becomes-date",
    ),
    pytest.param(np.float32, 0.1, float(np.float32(0.1)), id="D7-float-rounded"),
]


class TestDownstreamSeesStoredValue:
    """The output Data must hold what is stored: no as_dict / as_table divergence."""

    def _chain(
        self, declared: Any, returned: Any, received: list[Any]
    ) -> tuple[Any, Any]:
        up = make_pod(declared, returned, name="up")

        def down(y: Any) -> int:
            received.append(y)
            return 1

        down.__annotations__["y"] = declared
        return up, function_pod(output_keys="z")(down)

    @staticmethod
    def _assert_received(received: list[Any], expected: Any) -> None:
        assert received == [expected]
        assert type(received[0]) is type(expected)

    @pytest.mark.parametrize(("declared", "returned", "expected"), DOWNSTREAM)
    def test_standalone(self, declared: Any, returned: Any, expected: Any) -> None:
        received: list[Any] = []
        up, down = self._chain(declared, returned, received)
        with pytest.warns(errors.OutputConversionWarning):
            list(down.pod(up.pod(make_source())).iter_data())
        self._assert_received(received, expected)

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    @pytest.mark.parametrize(("declared", "returned", "expected"), DOWNSTREAM)
    def test_pipeline_job(
        self,
        declared: Any,
        returned: Any,
        expected: Any,
        orchestrator_cls: Any,
        store_kind: str,
        tmp_path: Path,
    ) -> None:
        received: list[Any] = []
        up, down = self._chain(declared, returned, received)
        job = PipelineJob(name="itl895", store=_store(store_kind, tmp_path))
        with job:
            down.pod(up.pod(make_source(), label="up"), label="down")
        with pytest.warns(errors.OutputConversionWarning):
            job.run(orchestrator=orchestrator_cls())
        self._assert_received(received, expected)
        stored = job.nodes["up"].as_table()["y"].to_pylist()
        assert stored == [expected]

    @pytest.mark.parametrize(("declared", "returned", "expected"), DOWNSTREAM)
    def test_output_as_dict_matches_as_table(
        self, declared: Any, returned: Any, expected: Any
    ) -> None:
        with pytest.warns(errors.OutputConversionWarning):
            [(_tag, data)] = list(
                make_pod(declared, returned).pod(make_source()).iter_data()
            )
        assert data.as_dict() == data.as_table().to_pylist()[0]
        assert data.as_dict()["y"] == expected


class TestRejectionInPipelineJob:
    """Every decided rejection is a row failure on both orchestrators and both stores."""

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    @pytest.mark.parametrize(
        ("declared", "returned"),
        [
            pytest.param(float, 0, id="D1"),
            pytest.param(float, True, id="D3"),
            pytest.param(list[int], {1, 2}, id="ITL901"),
        ],
    )
    def test_row_failure_and_nothing_stored(
        self,
        declared: Any,
        returned: Any,
        orchestrator_cls: Any,
        store_kind: str,
        tmp_path: Path,
    ) -> None:
        pod = make_pod(declared, returned, name="rejected")
        observer = _CrashRecorder()
        job = PipelineJob(name="itl895", store=_store(store_kind, tmp_path))
        with job:
            pod.pod(make_source((1,)), label="rejected")
        job.run(orchestrator=orchestrator_cls(), observer=observer)
        assert job.nodes["rejected"].as_table().num_rows == 0
        assert len(observer.crashes) == 1
        assert isinstance(observer.crashes[0], errors.OutputTypeError)


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
