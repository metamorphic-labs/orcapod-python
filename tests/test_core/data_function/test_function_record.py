"""The record of ``python.function.v1`` revision 1 (ITL-1256).

Every result row records, beside the function's name and version:

* ``function_module``: the function's ``__module__``;
* ``function_signature_hash`` (``function_signature_v1``): the declared interface, one
  hash over ``{"inputs": {<name>: {"type", "default"?}}, "outputs": [{"key", "type"}]}``
  from the effective types: inputs by name in no order, outputs in declared order; no
  name, module, parameter kind or order, version or body;
* ``function_content_hash`` (``function_content_v1``): the source text from the ``def``
  line on, as written (no decorator lines; a lambda's enclosing line), absent (null)
  when the source cannot be read or does not start at a ``def`` statement.

Reuse is unchanged: on the function's identity only (ITL-1253 will make comparing an
aspect opt-in).
"""

from __future__ import annotations

import importlib.util
import itertools
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.data_function import (
    FUNCTION_CONTENT_HASH_LABEL,
    FUNCTION_SIGNATURE_HASH_LABEL,
    PythonDataFunction,
    function_source,
)
from orcapod.core.function_pod import FunctionPod
from orcapod.core.sources import DictSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.invocation import InvocationContext
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

REPO_ROOT = Path(__file__).resolve().parents[3]

TWO = ("first", "second")
THREE = ("first", "second", "third")


def same2(x: int, y: int) -> tuple[int, int]:
    return x + y, x * y


def same3(x: int, y: int) -> tuple[int, int, int]:
    return x + y, x * y, x - y


def mixed3(x: int, y: int) -> tuple[int, str, float]:
    return x + y, f"{x}-{y}", x / 4


FUNCTIONS = {"same2": (same2, TWO), "same3": (same3, THREE), "mixed3": (mixed3, THREE)}


def _build(form: str, fn: Any, keys: Any, **kwargs: Any) -> PythonDataFunction:
    """The data function of a pod built in ``form``."""
    if form == "data_function":
        return PythonDataFunction(fn, output_keys=keys, **kwargs)
    if form == "from_fn":
        return FunctionPod.from_fn(fn, output_keys=keys, **kwargs).data_function  # pyright: ignore[reportReturnType]  # a FunctionPod's data function is a PythonDataFunction here
    if form == "decorator":
        return function_pod(output_keys=keys, **kwargs)(fn).pod.data_function  # pyright: ignore[reportReturnType]  # as above
    raise ValueError(form)


FORMS = ["data_function", "from_fn", "decorator"]


def _record(data_function: PythonDataFunction) -> dict[str, Any]:
    return data_function.get_function_variation_data()


# ---------------------------------------------------------------------------
# The signature hash
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("name", list(FUNCTIONS))
def test_every_order_of_the_output_keys_has_its_own_signature_hash(
    name: str, form: str
) -> None:
    fn, keys = FUNCTIONS[name]
    records = {
        order: _record(_build(form, fn, list(order)))
        for order in itertools.permutations(keys)
    }
    signatures = {r["function_signature_hash"] for r in records.values()}
    assert len(signatures) == len(records)
    for record in records.values():
        assert record["function_signature_hash"].startswith(
            f"{FUNCTION_SIGNATURE_HASH_LABEL}:".encode()
        )
    # The order of the keys is not the function as written.
    assert len({r["function_content_hash"] for r in records.values()}) == 1


@pytest.mark.parametrize("name", list(FUNCTIONS))
def test_one_declaration_has_one_record_in_every_form(name: str) -> None:
    fn, keys = FUNCTIONS[name]
    records = [_record(_build(form, fn, list(keys))) for form in FORMS]
    for field in (
        "function_signature_hash",
        "function_content_hash",
        "function_module",
    ):
        assert len({r[field] for r in records}) == 1, field


def test_one_output_key_written_either_way_is_one_signature() -> None:
    def ratio(x: int, y: int) -> float:
        return x / y

    assert _record(PythonDataFunction(ratio, output_keys="ratio")) == _record(
        PythonDataFunction(ratio, output_keys=["ratio"])
    )


def test_the_effective_types_enter_the_signature() -> None:
    """``input_schema=`` and ``output_schema=`` change it; the annotations alone do not."""
    plain = _record(PythonDataFunction(same2, output_keys=list(TWO)))
    inputs = _record(
        PythonDataFunction(same2, output_keys=list(TWO), input_schema={"x": float})
    )
    outputs = _record(
        PythonDataFunction(
            same2, output_keys=list(TWO), output_schema={"first": float, "second": int}
        )
    )
    assert (
        len(
            {
                plain["function_signature_hash"],
                inputs["function_signature_hash"],
                outputs["function_signature_hash"],
            }
        )
        == 3
    )


def test_inputs_are_matched_by_name_so_their_order_and_kind_are_not_in_it() -> None:
    def forward(x: int, y: int) -> tuple[int, int]:
        return x + y, x * y

    def backward(y: int, x: int) -> tuple[int, int]:
        return x + y, x * y

    def keyword_only(*, x: int, y: int) -> tuple[int, int]:
        return x + y, x * y

    records = [
        _record(PythonDataFunction(fn, output_keys=list(TWO), function_name="f"))
        for fn in (forward, backward, keyword_only)
    ]
    assert len({r["function_signature_hash"] for r in records}) == 1


def test_a_default_enters_by_its_value() -> None:
    def no_default(x: int, y: int) -> int:
        return x + y

    def default_two(x: int, y: int = 2) -> int:
        return x + y

    def default_three(x: int, y: int = 3) -> int:
        return x + y

    signatures = [
        _record(PythonDataFunction(fn, output_keys="s", function_name="f"))[
            "function_signature_hash"
        ]
        for fn in (no_default, default_two, default_three)
    ]
    assert len(set(signatures)) == 3


def test_the_name_and_module_are_not_in_the_signature_but_in_their_columns() -> None:
    def alpha(x: int) -> int:
        return x

    def beta(x: int) -> int:
        return x

    a, b = (_record(PythonDataFunction(fn, output_keys="v")) for fn in (alpha, beta))
    assert a["function_signature_hash"] == b["function_signature_hash"]
    assert (a["function_name"], b["function_name"]) == ("alpha", "beta")
    assert a["function_module"] == b["function_module"] == __name__


def test_a_context_parameter_stays_as_the_data_function_has_it() -> None:
    """The data function's input schema holds a ctx parameter; so does the signature."""

    def with_ctx(x: int, ctx: InvocationContext) -> int:
        return x

    def without_ctx(x: int) -> int:
        return x

    pod = FunctionPod.from_fn(with_ctx, output_keys="v", ctx_arg_name="ctx", name="f")
    assert "ctx" in pod.data_function.input_data_schema
    with_record = _record(pod.data_function)  # pyright: ignore[reportArgumentType]  # a PythonDataFunction
    without = _record(
        PythonDataFunction(without_ctx, output_keys="v", function_name="f")
    )
    assert with_record["function_signature_hash"] != without["function_signature_hash"]


# ---------------------------------------------------------------------------
# The content hash
# ---------------------------------------------------------------------------


@function_pod(output_keys="v")
def decorated(x: int) -> int:
    return x + 1


def undecorated(x: int) -> int:
    return x + 1


def test_the_source_starts_at_the_def_line() -> None:
    # ``@function_pod`` returns the function itself, with its pod attached.
    source = function_source(decorated)
    assert source is not None
    assert source.startswith("def decorated(x: int) -> int:")
    assert "@function_pod" not in source
    record = _record(decorated.pod.data_function)  # pyright: ignore[reportArgumentType]  # a PythonDataFunction
    assert record["function_content_hash"].startswith(
        f"{FUNCTION_CONTENT_HASH_LABEL}:".encode()
    )


def test_the_decorator_line_is_not_in_the_content_hash() -> None:
    """The decorator, and its output keys, never count: one function, one content hash."""
    in_decorator = _record(decorated.pod.data_function)  # pyright: ignore[reportArgumentType]  # a PythonDataFunction
    plain = _record(PythonDataFunction(decorated, output_keys="v"))
    other_keys = _record(_build("from_fn", decorated, "w"))
    assert (
        in_decorator["function_content_hash"]
        == plain["function_content_hash"]
        == other_keys["function_content_hash"]
    )


def test_a_lambda_counts_its_enclosing_line() -> None:
    double = lambda x: x * 2  # noqa: E731  # a lambda on purpose
    source = function_source(double)
    assert source is not None and "double = lambda x: x * 2" in source
    record = _record(
        PythonDataFunction(
            double, output_keys="v", input_schema={"x": int}, output_schema={"v": int}
        )
    )
    assert record["function_content_hash"] is not None


# Where the ``def`` line is (the CodeRabbit finding on PR 449): every case is written as a
# module file, because a column-zero comment or bracket line, tabs and Windows line endings
# cannot live in this formatted file, and ``inspect`` reads a function's source from its file.

_PREAMBLE = (
    "from orcapod import function_pod\n\n\n"
    "def note(*args, **kwargs):\n"
    "    return lambda f: f\n\n\n"
)

# Each decorator variant is a list of lines, placed at the function's indentation.
DECORATORS = (
    [],
    ["@function_pod(output_keys='v')"],
    ["@function_pod(output_keys='w', version='v2.0')", "@note(k=1)"],
    ["@function_pod(", "    output_keys='v',", "    version='v1.0',", ")"],
    [
        '@note("""',
        "def g():",
        "    pass",
        '""")',
        "# a comment between",
        "@note \\",
        "(k=2)",
    ],
)

# name: (module text with {dec} where the decorators go and {n} in the body, indentation,
# line ending). Every module defines ``f(x: int) -> int``.
CUT_CASES: dict[str, tuple[str, str, str]] = {
    "top_level": ("{dec}def f(x: int) -> int:\n    return x + {n}\n", "", "\n"),
    "one_line_def": ("{dec}def f(x: int) -> int: return x + {n}\n", "", "\n"),
    "async_def": ("{dec}async def f(x: int) -> int:\n    return x + {n}\n", "", "\n"),
    "nested_with_a_column_zero_string": (
        "def outer():\n{dec}    def f(x: int) -> int:\n        text = '''\nzero\n'''\n"
        "        return x + {n} + len(text)\n    return f\n\n\nf = outer()\n",
        "    ",
        "\n",
    ),
    "nested_with_a_column_zero_comment": (
        "def outer():\n{dec}    def f(x: int) -> int:\n# column zero\n"
        "        return x + {n}\n    return f\n\n\nf = outer()\n",
        "    ",
        "\n",
    ),
    "nested_with_a_column_zero_bracket_line": (
        "def outer():\n{dec}    def f(x: int) -> int:\n        return (x +\n{n})\n"
        "    return f\n\n\nf = outer()\n",
        "    ",
        "\n",
    ),
    "nested_async_with_a_column_zero_string": (
        "def outer():\n{dec}    async def f(x: int) -> int:\n        text = '''\nzero\n'''\n"
        "        return x + {n} + len(text)\n    return f\n\n\nf = outer()\n",
        "    ",
        "\n",
    ),
    "class_body_with_a_column_zero_string": (
        "class C:\n{dec}    def f(x: int) -> int:\n        text = '''\nzero\n'''\n"
        "        return x + {n} + len(text)\n\n\nf = C.f\n",
        "    ",
        "\n",
    ),
    "tab_indented_with_a_column_zero_string": (
        "def outer():\n{dec}\tdef f(x: int) -> int:\n\t\ttext = '''\nzero\n'''\n"
        "\t\treturn x + {n} + len(text)\n\treturn f\n\n\nf = outer()\n",
        "\t",
        "\n",
    ),
    "windows_line_endings": (
        "{dec}def f(x: int) -> int:\n    return x + {n}\n",
        "",
        "\r\n",
    ),
}

_module_numbers = itertools.count()


def _load_f(directory: Path, case: str, decorator: list[str], n: int) -> Any:
    """``f`` of the case's module, written with ``decorator`` and ``n`` in the body."""
    text, indent, newline = CUT_CASES[case]
    decorators = "".join(f"{indent}{line}\n" for line in decorator)
    source = _PREAMBLE + text.format(dec=decorators, n=n)
    path = directory / f"cut_case_{next(_module_numbers)}.py"
    path.write_bytes(source.replace("\n", newline).encode())
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.f


def _content_hash(f: Any) -> Any:
    return _record(PythonDataFunction(f, output_keys="v"))["function_content_hash"]


@pytest.mark.parametrize("case", list(CUT_CASES))
def test_no_decorator_line_ever_enters_the_content_hash(
    case: str, tmp_path: Path
) -> None:
    """Every decorator variant gives one content hash; a body change gives another."""
    hashes = set()
    for decorator in DECORATORS:
        f = _load_f(tmp_path, case, decorator, 1)
        source = function_source(f)
        assert source is not None
        assert source.lstrip().startswith(("def f(", "async def f("))
        hashes.add(_content_hash(f))
    assert len(hashes) == 1 and None not in hashes
    assert _content_hash(_load_f(tmp_path, case, DECORATORS[1], 2)) not in hashes


def test_a_source_that_does_not_start_at_a_def_has_no_content_hash(
    tmp_path: Path,
) -> None:
    """A source whose first statement after the decorators is no ``def`` cannot be cut."""
    f = _load_f(tmp_path, "top_level", [], 1)
    # The code object now names line 1 of the preamble, an import, as its first line.
    f.__code__ = f.__code__.replace(co_firstlineno=1)
    assert function_source(f) is None
    assert _content_hash(f) is None


def _repl_function() -> Any:
    namespace: dict[str, Any] = {"__name__": "__repl__"}
    exec("def typed_in(x: int) -> int:\n    return x * 10\n", namespace)
    return namespace["typed_in"]


def test_a_function_whose_source_cannot_be_read_has_no_content_hash() -> None:
    fn = _repl_function()
    assert function_source(fn) is None
    record = _record(PythonDataFunction(fn, output_keys="v"))
    assert record["function_content_hash"] is None
    assert record["function_module"] == "__repl__"
    assert record["function_signature_hash"].startswith(
        f"{FUNCTION_SIGNATURE_HASH_LABEL}:".encode()
    )


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_a_function_without_source_works_in_a_job_and_stores_a_null(
    orchestrator: type,
) -> None:
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="repl", database=database)
    with job:
        FunctionPod.from_fn(_repl_function(), output_keys="v")(
            DictSource(
                [{"k": "a", "j": 1, "x": 1}, {"k": "b", "j": 2, "x": 2}],
                tag_columns=["k", "j"],
                tag_only_columns=True,
                source_id="s",
            ),
            label="typed_in",
        )
    job.run(orchestrator=orchestrator())
    assert sorted(d["v"] for _, d in job.nodes["typed_in"].iter_data()) == [10, 20]
    result_path = next(p for p in database.list_table_paths() if "_result" in p)
    stored = database.get_all_records(result_path)
    assert stored is not None
    assert stored.column("__pf_var_function_content_hash").to_pylist() == [None, None]
    assert stored.column("__pf_var_function_module").to_pylist() == ["__repl__"] * 2


# ---------------------------------------------------------------------------
# Stored in a job, and the same in every process
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "database_kind",
    [
        pytest.param(
            "in_memory",
            marks=pytest.mark.xfail(
                strict=True,
                reason="ITL-1263: InMemoryArrowDatabase refuses rows whose columns "
                + "are in another order than the stored table",
            ),
        ),
        "delta",
    ],
)
@pytest.mark.parametrize("name", ["same2", "same3"])
def test_swapped_same_typed_keys_store_different_signature_hashes(
    name: str, database_kind: str, tmp_path: Path
) -> None:
    """One uri, one result table; each order's rows record their own signature."""
    fn, keys = FUNCTIONS[name]
    database = (
        DeltaTableDatabase(tmp_path / "store")
        if database_kind == "delta"
        else InMemoryArrowDatabase()
    )
    orders = [tuple(keys), tuple(reversed(keys))]
    for index, order in enumerate(orders):
        job = PipelineJob(name="orders", database=database)
        with job:
            FunctionPod.from_fn(fn, output_keys=list(order))(
                DictSource(
                    # Another input per order, so that each order computes its own row.
                    [{"k": f"r{index}", "j": index, "x": index + 1, "y": 2}],
                    tag_columns=["k", "j"],
                    tag_only_columns=True,
                    source_id=f"s{index}",
                ),
                label="fn",
            )
        job.run()
    result_paths = [p for p in database.list_table_paths() if "_result" in p]
    assert len(result_paths) == 1
    stored = database.get_all_records(result_paths[0])
    assert stored is not None
    signatures = stored.column("__pf_var_function_signature_hash").to_pylist()
    contents = stored.column("__pf_var_function_content_hash").to_pylist()
    assert len(signatures) == 2 and len(set(signatures)) == 2
    assert len(set(contents)) == 1


_PRINT_RECORD = """
import sys
sys.path.insert(0, {root!r})
from tests.test_core.data_function.test_function_record import FUNCTIONS, _build
for name, (fn, keys) in sorted(FUNCTIONS.items()):
    for form in ("data_function", "from_fn"):
        record = _build(form, fn, list(reversed(keys)))
        print(name, form, record.get_function_variation_data()["function_signature_hash"].hex(),
              record.get_function_variation_data()["function_content_hash"].hex())
"""


def test_the_record_is_the_same_under_every_hash_seed() -> None:
    script = _PRINT_RECORD.format(root=str(REPO_ROOT))
    outputs = []
    for seed in ("0", "123"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            env=env,
            cwd=REPO_ROOT,
            check=True,
        )
        outputs.append(result.stdout)
    assert outputs[0] == outputs[1]
    assert len(outputs[0].splitlines()) == 2 * len(FUNCTIONS)
