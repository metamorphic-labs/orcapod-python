"""A function with a positional-only parameter is refused at construction (ITL-1258).

A function pod passes its inputs by name (``self._function(**inputs)``, sync and async),
so a positional-only parameter could never be given: every call would fail. It is refused
when the pod is created, as variadic parameters are, naming every such parameter.
"""

from collections.abc import Callable
from typing import Any

import pytest

from orcapod import function_pod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.function_pod import FunctionPod


def one(x: int, /, y: int) -> int:
    return x + y


def two(x: int, w: int, /, y: int) -> int:
    return x + w + y


def mixed(x: int, /, y: int, *, z: int) -> int:
    return x + y + z


async def one_async(x: int, /, y: int) -> int:
    return x + y


def keyword_only(*, x: int, y: int) -> int:
    return x * 10 + y


def _build(form: str, fn: Callable[..., Any]) -> Any:
    if form == "data_function":
        return PythonDataFunction(fn, output_keys="out")
    if form == "decorator":
        return function_pod(output_keys="out")(fn).pod.data_function
    if form == "from_fn":
        return FunctionPod.from_fn(fn, output_keys="out").data_function
    raise ValueError(form)


FORMS = ["data_function", "decorator", "from_fn"]


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize(
    ("fn", "names"),
    [(one, ["x"]), (two, ["x", "w"]), (mixed, ["x"]), (one_async, ["x"])],
    ids=["one", "two", "mixed", "async"],
)
def test_a_positional_only_parameter_is_refused_at_construction(
    form: str, fn: Callable[..., Any], names: list[str]
) -> None:
    with pytest.raises(ValueError, match="positional-only") as raised:
        _build(form, fn)
    message = str(raised.value)
    assert f"Offending parameters: {names!r}" in message
    assert "passes its inputs by name" in message


@pytest.mark.parametrize("form", FORMS)
def test_keyword_only_parameters_keep_working(form: str) -> None:
    data_function = _build(form, keyword_only)
    # Two inputs, given in either order: matched by name.
    for data in ({"x": 1, "y": 2}, {"y": 2, "x": 1}):
        result = data_function.call(Data(data))
        assert result is not None
        assert result.as_dict() == {"out": 12}
