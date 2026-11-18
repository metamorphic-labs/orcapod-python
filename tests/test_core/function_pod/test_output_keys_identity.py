"""A function's outputs are a set of named, typed values (ITL-1244).

The function's uri holds the hash of its output schema: each output key paired with its
type, in no order (the schema hash sorts field names, as for every schema). So:

* swapping two output keys of different types changes the schema: another uri, content
  hash, pipeline hash and stored tables;
* swapping two output keys of the same type leaves the schema as it is: the same uri.
  Like an edit of the body, it changes what the function computes, so it needs a
  version bump; a check of the stored function variation data on a cache hit is
  ITL-1253 (strict xfail below);
* the input parameters are in no part of the uri (names and order);
* a function with one output key has the identity the golden values pin.

``@function_pod``, ``FunctionPod.from_fn`` and ``PythonDataFunction`` give one uri, and
``to_config`` / ``from_config`` and a saved and loaded job keep the declared order.
"""

from __future__ import annotations

import itertools
import warnings
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core.data_function import CachedDataFunction, PythonDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.function_pod import FunctionPod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

from ..operators.several_items import (
    DATA_TAGS,
    MAIN,
    MIXED,
    PLAIN,
    TAGS_FIRST,
    Arrangement,
    make_source,
)

# Output names in sorted order; the tests declare them in every order.
TWO_KEYS = ("first", "second")
THREE_KEYS = ("first", "second", "third")


# Outputs of one type: every order of the keys is one schema.
def same2(d1: int, d3: str) -> tuple[int, int]:
    return d1 + len(d3), d1 * 2


def same3(d1: int, d3: str) -> tuple[int, int, int]:
    return d1 + len(d3), d1 * 2, d1 - 1


# Outputs of different types: every order of the keys is another schema.
def mixed2(d1: int, d3: str) -> tuple[int, str]:
    return d1 * 2, d3 * 2


def mixed3(d1: int, d3: str) -> tuple[int, str, float]:
    return d1 * 2, d3 * 2, d1 / 4


# Two outputs of one type and one of another.
def partly(d1: int, d3: str) -> tuple[int, int, str]:
    return d1 + len(d3), d1 * 2, d3 * 2


FUNCTIONS = {
    ("same", 2): same2,
    ("same", 3): same3,
    ("mixed", 2): mixed2,
    ("mixed", 3): mixed3,
}


def _orders(n: int) -> list[tuple[str, ...]]:
    return list(itertools.permutations(TWO_KEYS if n == 2 else THREE_KEYS))


def _expected(fn: Any, keys: tuple[str, ...], d1: int, d3: str) -> dict[str, Any]:
    """Each output name's value: the returned position its key is declared at."""
    return dict(zip(keys, fn(d1, d3)))


def _main_values() -> list[tuple[Any, Any]]:
    columns = MAIN.columns
    return list(zip(columns["d1"].to_pylist(), columns["d3"].to_pylist()))


def _identity(pod: Any) -> tuple[Any, ...]:
    return (pod.uri, pod.content_hash(), pod.pipeline_hash())


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("kind", "n"), list(FUNCTIONS), ids=lambda v: str(v))
def test_the_uri_holds_the_hash_of_the_output_schema(kind: str, n: int) -> None:
    hasher = get_default_context().semantic_hasher
    for keys in _orders(n):
        pod = FunctionPod.from_fn(FUNCTIONS[(kind, n)], output_keys=list(keys))
        schema = pod.data_function.output_data_schema
        assert tuple(schema) == keys
        assert pod.uri[1] == hasher.hash_object(schema).to_hex()


@pytest.mark.parametrize("n", [2, 3])
def test_every_order_of_same_typed_output_keys_is_one_function(n: int) -> None:
    identities = {
        _identity(FunctionPod.from_fn(FUNCTIONS[("same", n)], output_keys=list(keys)))
        for keys in _orders(n)
    }
    assert len(identities) == 1


@pytest.mark.parametrize("n", [2, 3])
def test_every_order_of_differently_typed_output_keys_is_another_function(
    n: int,
) -> None:
    orders = _orders(n)
    pods = [
        FunctionPod.from_fn(FUNCTIONS[("mixed", n)], output_keys=list(keys))
        for keys in orders
    ]
    for part in range(3):
        assert len({_identity(pod)[part] for pod in pods}) == len(orders)
    # Only the output component of the uri differs.
    assert len({(pod.uri[0], *pod.uri[2:]) for pod in pods}) == 1


def test_a_swap_changes_the_identity_exactly_when_it_changes_a_keys_type() -> None:
    """``partly`` returns (int, int, str): only where ``str`` lands matters."""
    by_str_key: dict[str, set[tuple[Any, ...]]] = {}
    for keys in _orders(3):
        pod = FunctionPod.from_fn(partly, output_keys=list(keys))
        by_str_key.setdefault(keys[2], set()).add(_identity(pod))
    assert all(len(identities) == 1 for identities in by_str_key.values())
    assert len(set().union(*by_str_key.values())) == 3


def test_renaming_an_output_key_is_another_function() -> None:
    renamed = FunctionPod.from_fn(same2, output_keys=["first", "total"])
    original = FunctionPod.from_fn(same2, output_keys=list(TWO_KEYS))
    assert renamed.uri != original.uri


def test_a_one_output_function_has_the_identity_the_golden_values_pin() -> None:
    """The uri ``pod_default_version.decorator.uri`` pins for ``golden_ratio``.

    Pinned in ``tests/test_hashing/golden/data/v1/pipeline-01.json``.
    """
    from tests.test_hashing.golden.cases import golden_ratio

    pod = golden_ratio.pod
    assert pod.uri == (
        "golden_ratio",
        "21e6485805726eb7ceed63ef6b677f0b6b11d2eaf48bd9fc44c1dccdf718aa1d",
        "v0",
        "python.function.v0",
    )


def test_the_input_parameters_are_in_no_part_of_the_uri() -> None:
    """Names and order of the parameters: one uri, content hash and pipeline hash.

    The uri holds the function's name, output schema hash, major version and kind.
    A change of signature, like one of the body, is the user's to mark with a version.
    """

    def forward(d1: int, d3: str) -> tuple[int, str]:
        return d1 * 2, d3 * 2

    def backward(d3: str, d1: int) -> tuple[int, str]:
        return d1 * 2, d3 * 2

    def renamed(a: int, b: str) -> tuple[int, str]:
        return a * 2, b * 2

    for keys in (["first", "second"], ["second", "first"]):
        identities = {
            _identity(FunctionPod.from_fn(fn, output_keys=keys, name="f"))
            for fn in (forward, backward, renamed)
        }
        assert len(identities) == 1


@pytest.mark.parametrize("keys", _orders(3), ids="-".join)
def test_every_way_of_making_a_pod_gives_one_uri(keys: tuple[str, ...]) -> None:
    decorated = function_pod(output_keys=list(keys))(mixed3).pod
    from_fn = FunctionPod.from_fn(mixed3, output_keys=list(keys))
    direct = FunctionPod(PythonDataFunction(mixed3, output_keys=list(keys)))  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol; it works at runtime
    assert _identity(decorated) == _identity(from_fn) == _identity(direct)


@pytest.mark.parametrize("keys", _orders(3), ids="-".join)
def test_the_config_keeps_the_declared_order_and_the_uri(keys: tuple[str, ...]) -> None:
    data_function = PythonDataFunction(mixed3, output_keys=list(keys))
    config = data_function.to_config()
    assert config["config"]["output_keys"] == list(keys)
    assert list(config["config"]["output_data_schema"]) == list(keys)
    assert config["uri"] == list(data_function.uri)
    restored = PythonDataFunction.from_config(config)
    assert tuple(restored.output_data_schema) == keys
    assert restored.uri == data_function.uri


# ---------------------------------------------------------------------------
# Values: in memory, a cached data function, a job
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", [PLAIN, DATA_TAGS, MIXED])
@pytest.mark.parametrize(("kind", "n"), list(FUNCTIONS), ids=lambda v: str(v))
def test_in_memory_each_order_names_its_own_values(
    kind: str, n: int, role: str
) -> None:
    fn = FUNCTIONS[(kind, n)]
    for keys in _orders(n):
        source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role))
        stream = FunctionPod.from_fn(fn, output_keys=list(keys))(source)
        _, data_keys = stream.keys()
        assert tuple(k for k in data_keys if k in keys) == keys
        rows = sorted(tuple(data[k] for k in keys) for _, data in stream.iter_data())
        expected = sorted(
            tuple(_expected(fn, keys, d1, d3)[k] for k in keys)
            for d1, d3 in _main_values()
        )
        assert rows == expected, keys


def test_a_cached_data_function_stores_differently_typed_orders_apart() -> None:
    database = InMemoryArrowDatabase()
    cached = {
        keys: CachedDataFunction(
            PythonDataFunction(mixed2, output_keys=list(keys)),  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol; it works at runtime
            result_database=database,
        )
        for keys in _orders(2)
    }
    assert len({function.record_path for function in cached.values()}) == 2
    data = Data({"d1": 10, "d3": "xy"})
    for keys, function in cached.items():
        result = function.call(data)
        assert result is not None
        assert result.as_dict() == _expected(mixed2, keys, 10, "xy")
    for keys, function in cached.items():
        stored = function.get_cached_output_for_data(data)
        assert stored is not None
        assert stored.as_dict() == _expected(mixed2, keys, 10, "xy")


def _database(kind: str, tmp_path: Path) -> Any:
    if kind == "delta":
        return DeltaTableDatabase(tmp_path / "store")
    return InMemoryArrowDatabase()


def _values(node: Any, keys: tuple[str, ...]) -> list[tuple[Any, ...]]:
    return sorted(tuple(data[k] for k in keys) for _, data in node.iter_data())


def _expected_values(fn: Any, keys: tuple[str, ...]) -> list[tuple[Any, ...]]:
    return sorted(
        tuple(_expected(fn, keys, d1, d3)[k] for k in keys) for d1, d3 in _main_values()
    )


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
@pytest.mark.parametrize("database_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("role", [PLAIN, DATA_TAGS])
@pytest.mark.parametrize("n", [2, 3])
def test_jobs_with_differently_typed_orders_on_one_database_store_their_own_rows(
    n: int, role: str, database_kind: str, orchestrator: type, tmp_path: Path
) -> None:
    """Every order in turn on one database: each computes and stores its own rows.

    A second pass over every order finds each order's rows stored, under its names.
    """
    fn = FUNCTIONS[("mixed", n)]
    database = _database(database_kind, tmp_path)
    orders = _orders(n)
    paths = set()
    for pass_number in (1, 2):
        for keys in orders:
            job = PipelineJob(name="outputs", database=database)
            with job:
                FunctionPod.from_fn(fn, output_keys=list(keys))(
                    make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role)),
                    label="outputs",
                )
            summary = job.run(orchestrator=orchestrator())
            node = job.nodes["outputs"]
            counts = summary.nodes["outputs"]
            if pass_number == 1:
                assert (counts.computed, counts.reused) == (6, 0), keys
                paths.add(node.node_identity_path)
            else:
                assert (counts.computed, counts.reused) == (0, 6), keys
            assert _values(node, keys) == _expected_values(fn, keys), keys
    assert len(paths) == len(orders)


def test_a_saved_and_loaded_job_keeps_the_declared_order(tmp_path: Path) -> None:
    keys = ("third", "first", "second")
    job = PipelineJob(name="outputs", database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        FunctionPod.from_fn(mixed3, output_keys=list(keys))(
            make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, PLAIN)),
            label="outputs",
        )
    job.run()
    uri = job.nodes["outputs"].node_uri
    path = tmp_path / "job.json"
    job.save(path)

    loaded = PipelineJob.load(path, database=DeltaTableDatabase(tmp_path / "store"))
    node = loaded.nodes["outputs"]
    assert node.node_uri == uri
    assert tuple(k for k in node.output_schema()[1] if k in keys) == keys
    assert _values(node.as_source(), keys) == _expected_values(mixed3, keys)


# ---------------------------------------------------------------------------
# Open issues
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason="ITL-1253: a cache hit whose stored function variation data differs from "
    + "the current function is served silently",
)
@pytest.mark.parametrize("n", [2, 3])
def test_a_same_typed_swap_is_never_served_the_other_orders_rows_silently(
    n: int,
) -> None:
    """Same-typed swapped keys share a uri, so the second job finds the first's rows.

    What the safety net must do: either compute the second order's own values, or
    say so (a warning or an error); never serve the first order's values under the
    second order's names without a word.
    """
    fn = FUNCTIONS[("same", n)]
    first, second = _orders(n)[0], _orders(n)[-1]
    database = InMemoryArrowDatabase()
    warned = False
    job = None
    for keys in (first, second):
        job = PipelineJob(name="outputs", database=database)
        with job:
            FunctionPod.from_fn(fn, output_keys=list(keys))(
                make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, PLAIN)),
                label="outputs",
            )
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            job.run()
        # Only the last (second) run's warnings count.
        warned = bool(recorded)
    assert job is not None
    served_its_own = _values(job.nodes["outputs"], second) == _expected_values(
        fn, second
    )
    assert served_its_own or warned


@pytest.mark.xfail(
    strict=True,
    reason="ITL-1254: a dict returned where a tuple is declared is iterated by its keys",
)
def test_a_dict_returned_for_a_tuple_annotation_is_refused() -> None:
    def first_and_second(a: str, b: str) -> tuple[str, str]:
        # The defect under test: a dict where the annotation declares a tuple.
        return {"first": a, "second": b}  # pyright: ignore[reportReturnType]

    data_function = PythonDataFunction(
        first_and_second, output_keys=["first", "second"]
    )
    with pytest.raises((TypeError, ValueError), match="first_and_second"):
        data_function.call(Data({"a": "x", "b": "y"}))
