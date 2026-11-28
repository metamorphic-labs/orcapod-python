"""A function result table's source-info token column is a declared string (ITL-1563).

The token column of every output, ``_source_<key>``, is a nullable ``large_string``,
declared once (``RESULT_TOKEN_COLUMN``), never typed from the values of a write. A
custom write through ``CachedDataFunction.record_data`` whose token has another type
(a list) is refused before anything is written.
"""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.data_function import CachedDataFunction, PythonDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.result_cache import RESULT_TOKEN_COLUMN
from orcapod.core.sources import DictSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.system_constants import constants

BACKENDS = ("memory", "delta")
KEY_ORDERS = (("total", "label"), ("label", "total"))
LIST_TYPE = "large_list<item: large_string>"

# Which outputs carry a list token, and the list each carries.
LIST_TOKENS: tuple[dict[str, Any], ...] = (
    {"total": ["a", "b"]},
    {"label": ["a", "b"]},
    {"total": ["a", "b"], "label": ["c"]},
    {"total": [None]},
    {"label": []},
)


def summarize(x: int, y: int) -> tuple[int, str]:
    return x + y, f"{x}+{y}"


def summarize_swapped(x: int, y: int) -> tuple[str, int]:
    return f"{x}+{y}", x + y


def _database(
    backend: str, tmp_path: Path
) -> InMemoryArrowDatabase | DeltaTableDatabase:
    if backend == "delta":
        return DeltaTableDatabase(tmp_path / "store")
    return InMemoryArrowDatabase()


def _function(
    database: InMemoryArrowDatabase | DeltaTableDatabase, keys: tuple[str, ...]
) -> CachedDataFunction:
    function = summarize if keys[0] == "total" else summarize_swapped
    return CachedDataFunction(
        PythonDataFunction(function, output_keys=list(keys)),  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol; it works at runtime
        result_database=database,
    )


def _output(keys: tuple[str, ...], tokens: dict[str, Any]) -> Data:
    """A hand-built output with the given keys in order and the given tokens."""
    values: dict[str, Any] = {"total": 7, "label": "3+4"}
    return Data({key: values[key] for key in keys}, source_info=tokens)


def _string_tokens(keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: f"custom::id::{key}" for key in keys}


def _token_types(function: CachedDataFunction) -> dict[str, str]:
    table = function.get_all_cached_outputs()
    assert table is not None
    return {
        field.name: str(field.type)
        for field in table.schema
        if field.name.startswith(constants.SOURCE_PREFIX)
    }


def _stored_rows(function: CachedDataFunction) -> int:
    table = function.get_all_cached_outputs()
    return 0 if table is None else table.num_rows


def _assert_names(
    error: pytest.ExceptionInfo[InputValidationError], tokens: dict[str, Any]
) -> None:
    """The error names every list-token column, the type given and the declared type."""
    message = str(error.value)
    for key in tokens:
        assert f"{constants.SOURCE_PREFIX}{key}" in message
    assert LIST_TYPE in message
    assert "large_string" in message.replace(LIST_TYPE, "")
    assert "one string" in message


def test_the_declared_token_column_is_a_nullable_large_string() -> None:
    assert RESULT_TOKEN_COLUMN.arrow_type == "large_string"
    assert RESULT_TOKEN_COLUMN.nullable is True
    assert RESULT_TOKEN_COLUMN.python_type is str


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("tokens", LIST_TOKENS, ids=repr)
def test_a_list_token_is_refused_as_the_first_write(
    backend: str, keys: tuple[str, ...], tokens: dict[str, Any], tmp_path: Path
) -> None:
    """List first: nothing is written, and a string token is then stored."""
    function = _function(_database(backend, tmp_path), keys)
    with pytest.raises(InputValidationError) as error:
        function.record_data(Data({"x": 3, "y": 4}), _output(keys, tokens))
    _assert_names(error, tokens)
    assert _stored_rows(function) == 0
    assert function.get_cached_output_for_data(Data({"x": 3, "y": 4})) is None

    # The other order of the two writes: the string token after the refused list.
    function.record_data(Data({"x": 3, "y": 4}), _output(keys, _string_tokens(keys)))
    assert _stored_rows(function) == 1
    assert set(_token_types(function).values()) == {"large_string"}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("tokens", LIST_TOKENS, ids=repr)
@pytest.mark.parametrize("first", ("call", "custom"))
def test_a_list_token_is_refused_after_a_string_token(
    backend: str,
    keys: tuple[str, ...],
    tokens: dict[str, Any],
    first: str,
    tmp_path: Path,
) -> None:
    """String first (an ordinary call or a custom write): the table keeps its rows."""
    function = _function(_database(backend, tmp_path), keys)
    stored_input = Data({"x": 1, "y": 2})
    if first == "call":
        assert function.call(stored_input) is not None
    else:
        function.record_data(stored_input, _output(keys, _string_tokens(keys)))
    before = function.get_all_cached_outputs()
    assert before is not None and before.num_rows == 1

    with pytest.raises(InputValidationError) as error:
        function.record_data(Data({"x": 3, "y": 4}), _output(keys, tokens))
    _assert_names(error, tokens)

    after = function.get_all_cached_outputs()
    assert after is not None
    assert after.equals(before)
    assert function.get_cached_output_for_data(Data({"x": 3, "y": 4})) is None
    assert function.get_cached_output_for_data(stored_input) is not None
    assert set(_token_types(function).values()) == {"large_string"}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize(
    "order", tuple(itertools.permutations(("null", "string", "call")))
)
def test_null_and_string_tokens_are_stored_as_large_string_in_every_order(
    backend: str, keys: tuple[str, ...], order: tuple[str, ...], tmp_path: Path
) -> None:
    function = _function(_database(backend, tmp_path), keys)
    inputs = {
        "null": Data({"x": 1, "y": 2}),
        "string": Data({"x": 3, "y": 4}),
        "call": Data({"x": 5, "y": 6}),
    }
    for kind in order:
        if kind == "null":
            function.record_data(inputs[kind], _output(keys, dict.fromkeys(keys, None)))
        elif kind == "string":
            function.record_data(inputs[kind], _output(keys, _string_tokens(keys)))
        else:
            assert function.call(inputs[kind]) is not None
    assert _stored_rows(function) == 3
    assert _token_types(function) == {
        f"{constants.SOURCE_PREFIX}{key}": "large_string" for key in keys
    }
    for kind, input_data in inputs.items():
        found = function.get_cached_output_for_data(input_data)
        assert found is not None
        stored_tokens = found.source_info()
        if kind == "null":
            assert stored_tokens == dict.fromkeys(keys, None)
        elif kind == "string":
            assert stored_tokens == _string_tokens(keys)


def test_an_ordinary_output_gives_large_string_tokens_also_when_null() -> None:
    """What ``ResultCache.store`` reads: a null token is a ``large_string`` null."""
    function = PythonDataFunction(summarize, output_keys=["total", "label"])
    output = function.call(Data({"x": 1, "y": 2}))
    assert output is not None
    for data in (output, _output(("total", "label"), {"total": None, "label": None})):
        table = data.as_table(columns={"source": True})
        assert [
            table.schema.field(f"{constants.SOURCE_PREFIX}{key}").type
            for key in ("total", "label")
        ] == [pa.large_string(), pa.large_string()]


@function_pod(output_keys=["total", "label"])
def summarize_pod(x: int, y: int) -> tuple[int, str]:
    return x + y, f"{x}+{y}"


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "orchestrator", (SyncPipelineOrchestrator, AsyncPipelineOrchestrator)
)
def test_an_ordinary_pod_in_a_job_stores_and_reads_back(
    backend: str, orchestrator: type, tmp_path: Path
) -> None:
    database = _database(backend, tmp_path)
    source = DictSource(
        [
            {"subject": "s1", "unit": "u1", "x": 1, "y": 2},
            {"subject": "s1", "unit": "u2", "x": 3, "y": 4},
            {"subject": "s2", "unit": "u1", "x": 5, "y": 6},
        ],
        tag_columns=["subject", "unit"],
        tag_only_columns=True,
        source_id="units",
    )
    job = PipelineJob(name="tokens", database=database)
    with job:
        _ = summarize_pod.pod(source, label="summarized")
    _ = job.run(orchestrator=orchestrator())
    node = job.nodes["summarized"]
    table = node.as_table(columns={"source": True})
    assert sorted(table.column("total").to_pylist()) == [3, 7, 11]
    for key in ("total", "label"):
        column = table.column(f"{constants.SOURCE_PREFIX}{key}")
        assert column.type == pa.large_string()
        assert all(token.endswith(f"::{key}") for token in column.to_pylist())

    results = [path for path in database.list_sources() if "rdb_v1" in path]
    assert len(results) == 1
    stored = database.get_all_records(results[0])
    assert stored is not None and stored.num_rows == 3
    for key in ("total", "label"):
        field = stored.schema.field(f"{constants.SOURCE_PREFIX}{key}")
        assert (field.type, field.nullable) == (pa.large_string(), True)

    # A second run of the same job reads the stored results back.
    again = PipelineJob(name="tokens", database=database)
    with again:
        _ = summarize_pod.pod(source, label="summarized")
    _ = again.run(orchestrator=orchestrator())
    assert sorted(again.nodes["summarized"].as_table().column("label").to_pylist()) == [
        "1+2",
        "3+4",
        "5+6",
    ]
    stored_again = database.get_all_records(results[0])
    assert stored_again is not None and stored_again.num_rows == 3


# ---------------------------------------------------------------------------
# A missing token column is stored as a large_string null (ITL-1563, round 2)
# ---------------------------------------------------------------------------

# Which outputs of a hand-built row have a token; the others have no column at all.
PARTIAL_TOKENS: tuple[tuple[str, ...], ...] = ((), ("total",), ("label",))


def _partial_output(keys: tuple[str, ...], with_token: tuple[str, ...]) -> Data:
    return _output(keys, {key: f"custom::id::{key}" for key in with_token})


def _expected_columns(keys: tuple[str, ...]) -> list[str]:
    return [*keys, *(f"{constants.SOURCE_PREFIX}{key}" for key in keys)]


def _output_and_token_columns(function: CachedDataFunction) -> list[str]:
    table = function.get_all_cached_outputs()
    assert table is not None
    return [name for name in table.column_names if not name.startswith("__")]


@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("with_token", PARTIAL_TOKENS)
def test_a_row_without_a_token_column_has_none_before_it_is_stored(
    keys: tuple[str, ...], with_token: tuple[str, ...]
) -> None:
    """The case under test: the hand-built row lacks the column, not only the value."""
    table = _partial_output(keys, with_token).as_table(columns={"source": True})
    assert [n for n in table.column_names if n not in keys] == [
        f"{constants.SOURCE_PREFIX}{key}" for key in keys if key in with_token
    ]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("with_token", PARTIAL_TOKENS)
@pytest.mark.parametrize("other", ("call", "custom"))
@pytest.mark.parametrize("missing_first", (True, False))
def test_a_missing_token_column_is_stored_as_a_null_in_either_order(
    backend: str,
    keys: tuple[str, ...],
    with_token: tuple[str, ...],
    other: str,
    missing_first: bool,
    tmp_path: Path,
) -> None:
    """Without the column then with it, and the reverse: one table, every column."""
    function = _function(_database(backend, tmp_path), keys)
    partial_input, full_input = Data({"x": 3, "y": 4}), Data({"x": 1, "y": 2})

    def write_partial() -> None:
        function.record_data(partial_input, _partial_output(keys, with_token))

    def write_full() -> None:
        if other == "call":
            assert function.call(full_input) is not None
        else:
            function.record_data(full_input, _output(keys, _string_tokens(keys)))

    for write in (
        (write_partial, write_full) if missing_first else (write_full, write_partial)
    ):
        write()
        assert _output_and_token_columns(function) == _expected_columns(keys)
        assert set(_token_types(function).values()) == {"large_string"}

    assert _stored_rows(function) == 2
    found = function.get_cached_output_for_data(partial_input)
    assert found is not None
    assert found.source_info() == {
        key: f"custom::id::{key}" if key in with_token else None for key in keys
    }
    assert function.get_cached_output_for_data(full_input) is not None


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("with_token", PARTIAL_TOKENS)
def test_a_missing_token_column_alone_gives_every_declared_column(
    backend: str, keys: tuple[str, ...], with_token: tuple[str, ...], tmp_path: Path
) -> None:
    function = _function(_database(backend, tmp_path), keys)
    function.record_data(Data({"x": 3, "y": 4}), _partial_output(keys, with_token))
    table = function.get_all_cached_outputs()
    assert table is not None and table.num_rows == 1
    assert _output_and_token_columns(function) == _expected_columns(keys)
    for key in keys:
        field = table.schema.field(f"{constants.SOURCE_PREFIX}{key}")
        assert (field.type, field.nullable) == (pa.large_string(), True)
        assert table.column(field.name).to_pylist() == [
            f"custom::id::{key}" if key in with_token else None
        ]


class _WithStrayToken(Data):
    """An output whose row carries a token column for a name that is no output."""

    def as_table(self, *args: Any, **kwargs: Any) -> pa.Table:
        table = super().as_table(*args, **kwargs)
        return table.append_column(
            f"{constants.SOURCE_PREFIX}other",
            pa.array(["custom::id::other"], type=pa.large_string()),
        )


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("stored_first", (True, False))
def test_a_token_column_for_a_name_that_is_no_output_is_refused(
    backend: str, keys: tuple[str, ...], stored_first: bool, tmp_path: Path
) -> None:
    function = _function(_database(backend, tmp_path), keys)
    if stored_first:
        assert function.call(Data({"x": 1, "y": 2})) is not None
    before = function.get_all_cached_outputs()
    values: dict[str, Any] = {"total": 7, "label": "3+4"}
    stray = _WithStrayToken(
        {key: values[key] for key in keys}, source_info=_string_tokens(keys)
    )
    with pytest.raises(InputValidationError) as error:
        function.record_data(Data({"x": 3, "y": 4}), stray)
    message = str(error.value)
    assert f"{constants.SOURCE_PREFIX}other" in message
    for key in keys:
        assert repr(key) in message
    after = function.get_all_cached_outputs()
    assert (
        (after is None)
        if before is None
        else (after is not None and after.equals(before))
    )
    assert function.get_cached_output_for_data(Data({"x": 3, "y": 4})) is None
