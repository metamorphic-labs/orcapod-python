"""A function annotated ``-> None`` declares no outputs (ITL-1257).

``-> None`` with no output keys means no outputs, exactly like a function with no return
annotation and no keys: an empty output schema. ``-> None`` with output keys is refused
at construction. An output whose value is None is something else: one output key whose
type allows null (``-> int | None``).

``typing.get_type_hints`` turns ``-> None`` into ``NoneType``; each case is tested with
live annotations (this module) and with string annotations
(``string_annotated_functions``, ``from __future__ import annotations``), for
``PythonDataFunction``, ``@function_pod`` and ``FunctionPod.from_fn``.
"""

import asyncio
import pickle
from collections.abc import Callable
from typing import Any

import pytest

from orcapod import function_pod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.function_pod import FunctionPod
from orcapod.core.sources import DictSource
from orcapod.errors import NoOutputsDeclaredError, OutputTypeError

from . import string_annotated_functions as strings

REFUSAL = "return type annotation as None, but return keys"


def log_value(x: int) -> None:
    """Declares no outputs."""
    del x


def log_value_unannotated(x: int):  # no return annotation on purpose
    """Declares no outputs by having no return annotation."""
    del x


def maybe(x: int) -> int | None:
    """One output that may be null."""
    return None if x < 0 else x


async def log_value_async(x: int) -> None:
    """Declares no outputs; async."""
    del x


NO_OUTPUTS = {
    "live": log_value,
    "string": strings.log_value,
    "async": log_value_async,
}
UNANNOTATED = {"live": log_value_unannotated, "string": strings.log_value_unannotated}
MAYBE = {"live": maybe, "string": strings.maybe}


def _build(form: str, fn: Callable[..., Any], output_keys: Any) -> Any:
    """The data function of a pod built in ``form``."""
    if form == "data_function":
        if output_keys is None:
            return PythonDataFunction(fn)
        return PythonDataFunction(fn, output_keys=output_keys)
    if form == "decorator":
        if output_keys is None:
            return function_pod()(fn).pod.data_function
        return function_pod(output_keys=output_keys)(fn).pod.data_function
    if form == "from_fn":
        return FunctionPod.from_fn(fn, output_keys=output_keys or []).data_function
    raise ValueError(form)


FORMS = ["data_function", "decorator", "from_fn"]


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("annotations", list(NO_OUTPUTS))
@pytest.mark.parametrize("output_keys", [None, []], ids=["keys_omitted", "keys_empty"])
def test_none_with_no_output_keys_declares_no_outputs(
    form: str, annotations: str, output_keys: Any
) -> None:
    data_function = _build(form, NO_OUTPUTS[annotations], output_keys)
    assert dict(data_function.output_data_schema) == {}
    assert dict(data_function.input_data_schema) == {"x": int}


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("annotations", list(UNANNOTATED))
def test_none_and_no_annotation_are_one_output_schema(
    form: str, annotations: str
) -> None:
    """``-> None`` and no annotation: the same output schema, so the same uri part."""
    annotated = _build(form, NO_OUTPUTS[annotations], None)
    unannotated = _build(form, UNANNOTATED[annotations], None)
    assert annotated.output_data_schema == unannotated.output_data_schema
    assert annotated.uri[1:] == unannotated.uri[1:]


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("annotations", list(NO_OUTPUTS))
@pytest.mark.parametrize(
    "output_keys", ["result", ["first", "second"]], ids=["one", "two"]
)
def test_none_with_output_keys_is_refused(
    form: str, annotations: str, output_keys: Any
) -> None:
    with pytest.raises(ValueError, match=REFUSAL):
        _build(form, NO_OUTPUTS[annotations], output_keys)


@pytest.mark.parametrize("annotations", list(NO_OUTPUTS))
def test_a_function_with_no_outputs_runs_and_gives_empty_data(annotations: str) -> None:
    """Called per row, sync and async, with no output columns; tags pass through."""
    data_function = PythonDataFunction(NO_OUTPUTS[annotations])
    if data_function.is_async:
        result = asyncio.run(data_function.async_call(Data({"x": 1})))
    else:
        result = data_function.call(Data({"x": 1}))
    assert result is not None
    assert result.as_dict() == {}

    source = DictSource(
        [{"k": "a", "j": 1, "x": 1}, {"k": "b", "j": 2, "x": 2}],
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="s",
    )
    stream = FunctionPod(data_function)(source)  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol; it works at runtime
    tag_keys, data_keys = stream.keys()
    assert tuple(tag_keys) == ("k", "j")
    assert tuple(data_keys) == ()
    assert sorted(str(tag.as_dict()["k"]) for tag, _ in stream.iter_data()) == [
        "a",
        "b",
    ]


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("annotations", list(MAYBE))
def test_one_output_whose_value_is_none_is_a_null_value(
    form: str, annotations: str
) -> None:
    """An output whose value is None is not "no outputs": a null of a nullable type."""
    data_function = _build(form, MAYBE[annotations], "value")
    assert dict(data_function.output_data_schema) == {"value": int | None}
    null = data_function.call(Data({"x": -1}))
    present = data_function.call(Data({"x": 3}))
    assert null is not None and present is not None
    assert null.as_dict() == {"value": None}
    assert present.as_dict() == {"value": 3}


# A function that declares no outputs but returns a value: refused (ITL-1257 decision).


def returns_status(x: int) -> None:
    """Declares no outputs, but returns a value."""
    return x  # pyright: ignore[reportReturnType]  # the contradiction under test


def returns_status_unannotated(x: int):  # no return annotation on purpose
    """Declares no outputs by having no return annotation, but returns a value."""
    return x


async def returns_status_async(x: int) -> None:
    """Declares no outputs, but returns a value; async."""
    return x  # pyright: ignore[reportReturnType]  # the contradiction under test


RETURNS_A_VALUE = {
    "annotated": returns_status,
    "unannotated": returns_status_unannotated,
    "async": returns_status_async,
}


def _call(data_function: Any, x: int) -> Any:
    if data_function.is_async:
        return asyncio.run(data_function.async_call(Data({"x": x})))
    return data_function.call(Data({"x": x}))


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("declaration", list(RETURNS_A_VALUE))
def test_a_value_returned_by_a_function_with_no_outputs_is_refused(
    form: str, declaration: str
) -> None:
    data_function = _build(form, RETURNS_A_VALUE[declaration], None)
    with pytest.raises(NoOutputsDeclaredError) as raised:
        _call(data_function, 7)
    error = raised.value
    assert isinstance(error, OutputTypeError)
    assert error.function_name == RETURNS_A_VALUE[declaration].__name__
    assert error.value == 7
    message = str(error)
    assert repr(RETURNS_A_VALUE[declaration].__name__) in message
    assert "declares no outputs" in message
    assert "Add an output key" in message and "return nothing" in message
    # Types only, never the value.
    assert "7" not in message


def explicit_none(x: int) -> None:
    del x
    return None  # explicit on purpose


def bare_return(x: int) -> None:
    del x
    return  # explicit on purpose


def falls_off_the_end(x: int):  # no return annotation on purpose
    del x


@pytest.mark.parametrize("fn", [explicit_none, bare_return, falls_off_the_end])
def test_returning_none_is_returning_nothing(fn: Callable[..., Any]) -> None:
    assert _call(PythonDataFunction(fn), 1).as_dict() == {}


def test_a_pod_over_a_stream_refuses_the_returned_value() -> None:
    source = DictSource(
        [{"k": "a", "j": 1, "x": 1}, {"k": "b", "j": 2, "x": 2}],
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="s",
    )
    stream = function_pod()(returns_status).pod(source)
    with pytest.raises(NoOutputsDeclaredError):
        list(stream.iter_data())


def test_the_error_pickles_with_its_arguments() -> None:
    error = NoOutputsDeclaredError("f", 3)
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is NoOutputsDeclaredError
    assert (restored.function_name, restored.value, str(restored)) == (
        "f",
        3,
        str(error),
    )


def test_none_is_read_when_another_annotation_cannot_be_resolved() -> None:
    """``get_type_hints`` fails on ``x: Missing``; the postponed ``-> None`` still counts.

    The input type comes from ``input_schema`` (Copilot, #447).
    """
    data_function = PythonDataFunction(
        strings.unresolvable_input, input_schema={"x": int}
    )
    assert dict(data_function.output_data_schema) == {}
    with pytest.raises(ValueError, match=REFUSAL):
        PythonDataFunction(
            strings.unresolvable_input, output_keys="out", input_schema={"x": int}
        )


def test_the_error_keeps_the_attributes_of_an_output_type_error() -> None:
    """Every attribute ``OutputTypeError`` documents is present (Copilot, #447)."""
    error = NoOutputsDeclaredError("f", 3)
    for restored in (error, pickle.loads(pickle.dumps(error))):
        assert restored.function_name == "f"
        assert restored.value == 3
        assert restored.output_key is None
        assert restored.declared_type is None
        assert restored.detail is None
        assert restored.hint == "Add an output key for the value, or return nothing"
