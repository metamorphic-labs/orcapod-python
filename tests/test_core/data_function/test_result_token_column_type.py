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


# ---------------------------------------------------------------------------
# Review of df106ed3 (ITL-1563, round 3)
# ---------------------------------------------------------------------------


class _NonNullableTokens(Data):
    """An output whose row declares the named token fields non-nullable."""

    non_nullable: tuple[str, ...] = ()

    def as_table(self, *args: Any, **kwargs: Any) -> pa.Table:
        table = super().as_table(*args, **kwargs)
        for key in self.non_nullable:
            index = table.schema.get_field_index(f"{constants.SOURCE_PREFIX}{key}")
            if index >= 0:
                table = table.set_column(
                    index,
                    table.schema.field(index).with_nullable(False),
                    table.column(index),
                )
        return table


def _non_nullable_output(
    keys: tuple[str, ...], non_nullable: tuple[str, ...]
) -> _NonNullableTokens:
    values: dict[str, Any] = {"total": 7, "label": "3+4"}
    output = _NonNullableTokens(
        {key: values[key] for key in keys}, source_info=_string_tokens(keys)
    )
    output.non_nullable = non_nullable
    return output


NON_NULLABLE: tuple[tuple[str, ...], ...] = (
    ("total",),
    ("label",),
    ("total", "label"),
    ("label", "total"),
)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("non_nullable", NON_NULLABLE)
@pytest.mark.parametrize("other", ("missing", "null", "call"))
@pytest.mark.parametrize("non_nullable_first", (True, False))
def test_a_non_nullable_token_field_is_stored_as_the_declared_nullable_field(
    backend: str,
    keys: tuple[str, ...],
    non_nullable: tuple[str, ...],
    other: str,
    non_nullable_first: bool,
    tmp_path: Path,
) -> None:
    """Review finding 1: the incoming field's flag never reaches the table."""
    function = _function(_database(backend, tmp_path), keys)
    strict_input, other_input = Data({"x": 3, "y": 4}), Data({"x": 1, "y": 2})
    strict_output = _non_nullable_output(keys, non_nullable)
    row = strict_output.as_table(columns={"source": True})
    assert [
        row.schema.field(f"{constants.SOURCE_PREFIX}{key}").nullable for key in keys
    ] == [key not in non_nullable for key in keys]

    def write_strict() -> None:
        function.record_data(strict_input, strict_output)

    def write_other() -> None:
        if other == "call":
            assert function.call(other_input) is not None
        elif other == "null":
            function.record_data(other_input, _output(keys, dict.fromkeys(keys, None)))
        else:
            function.record_data(other_input, _partial_output(keys, ()))

    for write in (
        (write_strict, write_other)
        if non_nullable_first
        else (write_other, write_strict)
    ):
        write()
        table = function.get_all_cached_outputs()
        assert table is not None
        assert _output_and_token_columns(function) == _expected_columns(keys)
        for key in keys:
            field = table.schema.field(f"{constants.SOURCE_PREFIX}{key}")
            assert (field.type, field.nullable) == (pa.large_string(), True)

    assert _stored_rows(function) == 2
    found = function.get_cached_output_for_data(strict_input)
    assert found is not None
    assert found.source_info() == _string_tokens(keys)
    assert function.get_cached_output_for_data(other_input) is not None


def _table_output(keys: tuple[str, ...], tokens: dict[str, pa.Array[Any]]) -> Data:
    """A hand-built output made from an Arrow table whose token columns are typed."""
    table = _output(keys, _string_tokens(keys)).as_table(columns={"source": True})
    for key, array in tokens.items():
        name = f"{constants.SOURCE_PREFIX}{key}"
        table = table.set_column(table.schema.get_field_index(name), name, array)
    return Data(table)


# Non-null tokens in a column whose Arrow type is not a string: the type text the
# error names, and the column.
TYPED_TOKENS: dict[str, tuple[str, pa.Array[Any]]] = {
    "dictionary": (
        "dictionary<values=large_string, indices=int32, ordered=0>",
        pa.array(["custom::id::t"], type=pa.large_string()).dictionary_encode(),
    ),
    "binary": ("large_binary", pa.array([b"custom::id::t"], type=pa.large_binary())),
    "list": (LIST_TYPE, pa.array([["a", "b"]], type=pa.large_list(pa.large_string()))),
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("kind", sorted(TYPED_TOKENS))
@pytest.mark.parametrize("typed", (("total",), ("label",), ("total", "label")))
@pytest.mark.parametrize("stored_first", (True, False))
def test_a_typed_arrow_token_column_is_refused_by_the_type_the_row_carries(
    backend: str,
    keys: tuple[str, ...],
    kind: str,
    typed: tuple[str, ...],
    stored_first: bool,
    tmp_path: Path,
) -> None:
    """Reject the Arrow type the row carries, even if a view normalizes it."""
    type_text, array = TYPED_TOKENS[kind]
    function = _function(_database(backend, tmp_path), keys)
    if stored_first:
        assert function.call(Data({"x": 1, "y": 2})) is not None
    before = function.get_all_cached_outputs()
    output = _table_output(keys, dict.fromkeys(typed, array))
    with pytest.raises(InputValidationError) as error:
        function.record_data(Data({"x": 3, "y": 4}), output)
    message = str(error.value)
    for key in typed:
        assert f"'{constants.SOURCE_PREFIX}{key}' is {type_text}" in message
    for key in set(keys) - set(typed):
        assert f"'{constants.SOURCE_PREFIX}{key}'" not in message
    assert "one string" in message
    after = function.get_all_cached_outputs()
    if before is None:
        assert after is None
    else:
        assert after is not None and after.equals(before)
    assert function.get_cached_output_for_data(Data({"x": 3, "y": 4})) is None


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("keys", KEY_ORDERS)
def test_a_small_string_arrow_token_column_is_stored_as_large_string(
    backend: str, keys: tuple[str, ...], tmp_path: Path
) -> None:
    """A ``string`` token column is the same type in its large variant: accepted."""
    function = _function(_database(backend, tmp_path), keys)
    array = pa.array(["custom::id::small"], type=pa.string())
    function.record_data(
        Data({"x": 3, "y": 4}), _table_output(keys, dict.fromkeys(keys, array))
    )
    assert function.call(Data({"x": 1, "y": 2})) is not None
    assert _token_types(function) == {
        f"{constants.SOURCE_PREFIX}{key}": "large_string" for key in keys
    }
    found = function.get_cached_output_for_data(Data({"x": 3, "y": 4}))
    assert found is not None
    assert found.source_info() == dict.fromkeys(keys, "custom::id::small")


NULL_TYPED_TOKENS: dict[str, pa.Array[Any]] = {
    "list": pa.nulls(1, pa.large_list(pa.large_string())),
    "dictionary": pa.array([None], type=pa.large_string()).dictionary_encode(),
    "binary": pa.nulls(1, pa.large_binary()),
}


@pytest.mark.parametrize("keys", KEY_ORDERS)
@pytest.mark.parametrize("kind", sorted(NULL_TYPED_TOKENS))
def test_a_null_token_in_a_typed_arrow_column_is_refused(
    keys: tuple[str, ...], kind: str
) -> None:
    """The Arrow type survives table transport even when the token is null."""
    function = _function(InMemoryArrowDatabase(), keys)
    array = NULL_TYPED_TOKENS[kind]
    output = _table_output(keys, {"total": array})
    with pytest.raises(InputValidationError) as error:
        function.record_data(Data({"x": 3, "y": 4}), output)
    assert f"'{constants.SOURCE_PREFIX}total' is {array.type}" in str(error.value)
    assert function.get_all_cached_outputs() is None


@pytest.mark.parametrize("keys", KEY_ORDERS)
def test_a_table_already_holding_a_list_token_column_is_not_rewritten(
    keys: tuple[str, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review finding 3, the changelog's caveat: a Delta result table written before
    this fix with a list token keeps its column type; an ordinary write to it still
    fails as a database write error, and its rows stay."""
    from orcapod.core import result_cache
    from orcapod.errors import OrcapodDatabaseWriteError

    database = DeltaTableDatabase(tmp_path / "store")
    function = _function(database, keys)
    with monkeypatch.context() as before_the_fix:
        before_the_fix.setattr(
            result_cache, "_with_declared_token_columns", lambda table, *_: table
        )
        function.record_data(
            Data({"x": 3, "y": 4}), _output(keys, dict.fromkeys(keys, ["a", "b"]))
        )
    old = function.get_all_cached_outputs()
    assert old is not None and old.num_rows == 1
    assert set(_token_types(function).values()) == {LIST_TYPE}

    with pytest.raises(OrcapodDatabaseWriteError):
        _ = function.call(Data({"x": 1, "y": 2}))
    again = _function(
        DeltaTableDatabase(tmp_path / "store"), keys
    ).get_all_cached_outputs()
    assert again is not None and again.equals(old)
