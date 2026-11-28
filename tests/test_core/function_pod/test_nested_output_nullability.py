"""Python output annotations constrain nullable Arrow children."""

import asyncio
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

import pytest
from pydantic import BaseModel

from orcapod import PipelineJob, dataclass as structural_dataclass
from orcapod.core.datagrams import Data
from orcapod.core.executors import LocalPythonFunctionExecutor
from orcapod.core.executors.capture_wrapper import make_capture_wrapper
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import OutputTypeError, RowFailuresWarning
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

from .test_output_conformance import _CrashRecorder, make_pod, make_source


@dataclass
class Required:
    count: int


@dataclass
class Optional:
    count: int | None


class RequiredModel(BaseModel):
    count: int


class OptionalModel(BaseModel):
    count: int | None


@structural_dataclass
class RequiredStructural:
    count: int


@structural_dataclass
class OptionalStructural:
    count: int | None


class _SerializedCaptureExecutor(LocalPythonFunctionExecutor):
    """Round-trip the Ray worker's capture closure and response without a cluster."""

    _capture_lock = Lock()

    def execute_callable(
        self, fn, kwargs, executor_options=None, *, logger=None
    ) -> Any:
        from ray import cloudpickle

        worker, function, arguments = cloudpickle.loads(
            cloudpickle.dumps((make_capture_wrapper(), fn, kwargs))
        )
        # The real workers capture in separate processes. Serialize capture here
        # because the wrapper redirects process-wide file descriptors.
        with self._capture_lock:
            raw, *_ = cloudpickle.loads(cloudpickle.dumps(worker(function, arguments)))
        return raw

    async def async_execute_callable(
        self, fn, kwargs, executor_options=None, *, logger=None
    ) -> Any:
        return await asyncio.to_thread(self.execute_callable, fn, kwargs)


CASES = [
    pytest.param(list[int], [None], list[int | None], [None], "[0]", id="list"),
    pytest.param(set[int], {None}, set[int | None], {None}, "[*]", id="set"),
    pytest.param(
        dict[str, int],
        {None: 1},
        dict[str | None, int],
        {None: 1},
        ".keys()",
        id="dict-key",
    ),
    pytest.param(
        dict[str, int],
        {"a": None},
        dict[str, int | None],
        {"a": None},
        "[*]",
        id="dict-value",
    ),
    pytest.param(
        Required,
        Required(None),  # pyright: ignore[reportArgumentType]  # Test the invalid return.
        Optional,
        Optional(None),
        ".count",
        id="dataclass",
    ),
    pytest.param(
        RequiredModel,
        RequiredModel.model_construct(count=None),
        OptionalModel,
        OptionalModel(count=None),
        ".count",
        id="pydantic",
    ),
    pytest.param(
        RequiredStructural,
        RequiredStructural(None),  # pyright: ignore[reportArgumentType]  # Test the invalid return.
        OptionalStructural,
        OptionalStructural(None),
        ".count",
        id="structural",
    ),
    pytest.param(
        list[Required],
        [Required(None)],  # pyright: ignore[reportArgumentType]  # Test the invalid return.
        list[Optional],
        [Optional(None)],
        "[0].count",
        id="nested",
    ),
    pytest.param(
        list[Path], [None], list[Path] | None, None, "[0]", id="logical-element"
    ),
]


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("route", ["direct", "local", "capture"])
@pytest.mark.parametrize(
    ("required", "invalid", "optional", "valid", "position"), CASES
)
def test_nested_output_nullability(
    required, invalid, optional, valid, position, route, asynchronous
):
    """Reject a nested None at the annotated position and store its supported Optional form."""
    bad = make_pod(required, invalid, name="bad", async_function=asynchronous)
    good = make_pod(optional, valid, name="good", async_function=asynchronous)
    if route != "direct":
        executor = (
            LocalPythonFunctionExecutor()
            if route == "local"
            else _SerializedCaptureExecutor()
        )
        bad.pod.executor = good.pod.executor = executor

    input_data = Data({"x": 1}, python_schema={"x": int})

    def call(pod):
        function = pod.pod.data_function
        if route == "direct":
            return (
                asyncio.run(function.direct_async_call(input_data))
                if asynchronous
                else function.direct_call(input_data)
            )
        return (
            asyncio.run(function.async_call(input_data))
            if asynchronous
            else function.call(input_data)
        )

    with pytest.raises(OutputTypeError) as info:
        call(bad)
    assert f"{position} is NoneType" in str(info.value)
    assert call(good).as_dict() == {"y": valid}

    if route != "direct":
        observer = _CrashRecorder()
        job = PipelineJob(name="nested_outputs", database=InMemoryArrowDatabase())
        with job:
            source = make_source()
            bad.pod(source, label="bad")
            good.pod(source, label="good")
        orchestrator = (
            AsyncPipelineOrchestrator() if asynchronous else SyncPipelineOrchestrator()
        )
        with pytest.warns(RowFailuresWarning):
            job.run(orchestrator=orchestrator, observer=observer)
        assert job.nodes["bad"].as_table().num_rows == 0
        assert job.nodes["good"].as_table().num_rows == 1
        [error] = observer.crashes
        assert isinstance(error, OutputTypeError)
        assert f"{position} is NoneType" in str(error)


@pytest.mark.parametrize(
    "annotation",
    [tuple[int, int], tuple[int, int | None], tuple[int, str], tuple[int | None, str]],
)
def test_tuple_is_not_a_single_pod_output_type(annotation):
    """Tuple layouts in Arrow do not imply support for a single tuple output."""
    with pytest.raises(ValueError, match="Unsupported annotation"):
        make_pod(annotation, (1, None))


def test_optional_logical_item_is_refused_at_construction():
    """The return checker must not silently bypass the ITL-1144 codec guard."""
    with pytest.raises(ValueError, match=r"Column 'y'.*ITL-1144"):
        make_pod(list[Path | None], [None])
