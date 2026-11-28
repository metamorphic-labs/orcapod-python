"""F, the row-built form of a table, equals the table built from its rows (ITL-1347, L0).

``StaticOutputOperatorPod._row_built_form`` (F in the spec,
``docs/dev/specs/itl-1347-job-table-transport.md``) gives a node's output table as
``_rows_to_table`` (R) builds it from the node's rows, without making the rows. Every
operator, column role and arrangement of the async-sync matrix is checked: the column
names and their order, every field (type, nullability, field metadata), the schema
metadata, and every value (floats by their bits).

Two properties each case covers by construction:

- A data-tag's source info comes first among the source info, in tag order, whatever
  its column's place in the table (``source_mid``, ``tag_by``): a row lists its source
  info in the order of its data keys.
- A system-tag field is nullable only when a value is null: a join over inputs tied on
  pipeline hash (``ref_join_3_tied``) declares its system tags nullable, a table built
  from rows does not.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob
from orcapod.core.operators import Batch, DropNulls, SelectDataColumns
from orcapod.core.operators.static_output_pod import (
    StaticOutputOperatorPod,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.observer import NoOpObserver
from orcapod.types import CacheMode
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS, row_built_columns
from orcapod.errors import InputValidationError
from orcapod.system_constants import constants
from tests.token_shapes import data_type_for, nested_value, with_token_column
from tests.test_core.operators.test_async_sync_equivalence import (
    AFTER_OPERATOR_CASES,
    CASES,
    FUNCTION_POD_CASES,
    Case,
)


_FLOAT_BITS = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}


def _as_bits(array: Any) -> Any:  # pyright: ignore[reportExplicitAny]  # any Arrow array kind
    """``array``'s values, with every float, at any depth, as its bit pattern.

    Arrow's ``equals`` holds NaN unequal to itself, so floats are compared as unsigned
    integers of their width. Lists and structs are taken apart into their validity, their
    lengths and their children's values, so the physical layout (a list's first offset,
    the bytes behind a null) does not count.
    """
    kind = array.type
    if isinstance(array, pa.ExtensionArray):
        return ("extension", _as_bits(array.storage))
    if pa.types.is_floating(kind):
        filled = pc.if_else(pc.is_null(array), pa.scalar(0, kind), array)
        return ("float", array.is_null(), filled.view(_FLOAT_BITS[kind.bit_width]))
    if pa.types.is_map(kind):
        return (
            "map",
            array.is_null(),
            pc.list_value_length(array),
            _as_bits(array.keys),
            _as_bits(array.items),
        )
    if (
        pa.types.is_list(kind)
        or pa.types.is_large_list(kind)
        or pa.types.is_fixed_size_list(kind)
    ):
        return (
            "list",
            array.is_null(),
            pc.list_value_length(array),
            _as_bits(array.flatten()),
        )
    if pa.types.is_struct(kind):
        return (
            "struct",
            array.is_null(),
            tuple(_as_bits(child) for child in array.flatten()),
        )
    return ("value", array)


def assert_same_values(formed: pa.Table, built: pa.Table) -> None:
    """Every column of ``formed`` holds the values of ``built``, floats by their bits.

    The physical layout (a list's first offset, the bytes behind a null) is not a
    value and may differ.
    """
    for name in built.column_names:
        expected, got = built.column(name), formed.column(name)
        assert got.type == expected.type, name
        assert _as_bits(got.combine_chunks()) == _as_bits(expected.combine_chunks()), (
            name
        )


def _all_cases() -> list[Any]:
    cases: list[Any] = [pytest.param(c, id=c.label) for c in CASES]
    cases += list(AFTER_OPERATOR_CASES)
    cases += [pytest.param(c, id=f"fp-{c.label}") for c in FUNCTION_POD_CASES]
    return cases


@pytest.mark.parametrize("case", _all_cases())
def test_row_built_form_equals_the_table_built_from_rows(case: Case) -> None:
    """F of an operator's result table equals R of its rows, field and bit for bit."""
    result = case.make_op()(*case.make_sources())
    rows = list(result.iter_data())
    if not rows:
        pytest.skip("no rows: F is defined on a table with rows")
    built, _, _ = StaticOutputOperatorPod._rows_to_table(rows)  # pyright: ignore[reportPrivateUsage]  # R, the oracle
    formed = StaticOutputOperatorPod._row_built_form(  # pyright: ignore[reportPrivateUsage]  # F, under test
        result.as_table(columns=ZERO_ROW_INPUT_COLUMNS), result.keys()
    )
    assert formed.column_names == built.column_names
    assert formed.schema.equals(built.schema, check_metadata=True)
    assert_same_values(formed, built)


def test_row_built_columns_is_the_same_on_every_slice() -> None:
    """The row-by-row steps of F give the same columns on a slice as on the whole."""
    case = next(c for c in CASES if c.label == "ref_join_3_tied")
    result = case.make_op()(*case.make_sources())
    table = result.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    keys = result.keys()
    whole = row_built_columns(table, keys)
    for start in range(table.num_rows):
        part = row_built_columns(table.slice(start, 1), keys)
        assert part.schema.equals(whole.schema, check_metadata=True)
        assert part.equals(whole.slice(start, 1))


def _raw_stream() -> ArrowTableStream:
    """A raw stream with provenance fields no source writes (Codex on PR #483).

    A system-tag field with field metadata, a list-valued source-info column whose item
    field is named ``member`` and non-nullable, and a meta (``__``) data column with
    its own source info.
    """
    system_tag = f"{constants.SYSTEM_TAG_PREFIX}source_id::abc"
    record_id = f"{constants.SYSTEM_TAG_PREFIX}record_id::abc"
    member = pa.field("member", pa.large_string(), nullable=False)
    table = pa.table(
        {
            "k": pa.array([1, 2], pa.int64()),
            "j": pa.array(["a", "b"], pa.large_string()),
            system_tag: pa.array(["s1", "s2"], pa.large_string()),
            record_id: pa.array(["r1", "r2"], pa.large_string()),
            # A list token needs list data (the token-shape rule, ITL-1491).
            "x": pa.array([[1.0, float("nan")], [2.0]], pa.large_list(pa.float64())),
            "z": pa.array([3, None], pa.int8()),
            "__note": pa.array(["a", "b"]),
            "_source_x": pa.array([["a", "b"], ["c"]], pa.large_list(member)),
            "_source_z": pa.array(["t1", "t2"], pa.large_string()),
            "_source___note": pa.array(["n1", "n2"], pa.large_string()),
        }
    )
    position = table.schema.get_field_index(system_tag)
    schema = table.schema.set(
        position, table.schema.field(position).with_metadata({b"m": b"v"})
    )
    return ArrowTableStream(
        pa.Table.from_arrays(table.columns, schema=schema), tag_columns=["k", "j"]
    )


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(_raw_stream, id="raw"),
        pytest.param(
            lambda: SelectDataColumns(["z", "x"])(_raw_stream()), id="select-z-x"
        ),
        pytest.param(lambda: DropNulls(["j", "k"])(_raw_stream()), id="drop-nulls"),
    ],
)
def test_row_built_form_of_a_raw_stream(make: Any) -> None:
    """F equals R on a raw stream: provenance fields as rows build them, no meta.

    R takes a system tag and a source-info token from its values, so the system-tag
    field has no metadata and a list token's item field is nullable ``item``; a meta
    column is not data, so neither it nor its source info is passed on.
    """
    stream = make()
    built, _, _ = StaticOutputOperatorPod._rows_to_table(list(stream.iter_data()))  # pyright: ignore[reportPrivateUsage]  # R, the oracle
    formed = StaticOutputOperatorPod._row_built_form(  # pyright: ignore[reportPrivateUsage]  # F, under test
        stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS), stream.keys()
    )
    assert "__note" not in formed.column_names
    assert "_source___note" not in formed.column_names
    assert formed.column_names == built.column_names
    assert formed.schema.equals(built.schema, check_metadata=True)
    assert_same_values(formed, built)


def _source(kind: pa.DataType | None = None) -> Any:
    """A source with one tag and one data column, ``x``.

    Args:
        kind: A token type whose list layers ``x`` is given (the token-shape rule,
            ITL-1491: a list token needs list data); ``None`` for a plain integer.
    """
    from orcapod.sources import ArrowTableSource

    data_type = data_type_for(kind or pa.large_string())
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("x", data_type, nullable=False),
        ]
    )
    return ArrowTableSource(
        pa.Table.from_arrays(
            [
                pa.array([1, 2], pa.int64()),
                pa.array([nested_value(v, data_type) for v in (1, 2)], data_type),
            ],
            schema=schema,
        ),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
    )


def _source_table() -> pa.Table:
    """A source's table with its system tags and source info (one tag, one data)."""
    return _source().as_table(columns={"source": True, "system_tags": True})


def _with_source_x(values: list[Any], kind: pa.DataType) -> ArrowTableStream:
    """A source's rows as a raw stream, ``_source_x`` replaced by ``values`` of ``kind``.

    ``x`` gets the list layers of ``kind``, each row's value at every token position
    (the token-shape rule, ITL-1491). The stream keeps the source's system-tag pair,
    so a job slot accepts it (Codex on PR #485).
    """
    return ArrowTableStream(
        with_token_column(_source_table(), "x", values, kind), tag_columns=["k"]
    )


_STRINGS = pa.large_list(pa.large_string())
_NESTED = pa.large_list(_STRINGS)


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda: _with_source_x([[], []], _NESTED), id="empty-lists"),
        pytest.param(lambda: _with_source_x([[], []], _STRINGS), id="empty-strings"),
        pytest.param(
            lambda: _with_source_x([[[None]], [[None]]], _NESTED), id="lists-of-null"
        ),
        pytest.param(
            lambda: Batch(batch_size=1)(
                _with_source_x([None, None], pa.large_string())
            ),
            id="batch-1-of-null-tokens",
        ),
        pytest.param(
            lambda: Batch(batch_size=2)(
                _with_source_x([None, None], pa.large_string())
            ),
            id="batch-2-of-null-tokens",
        ),
        pytest.param(lambda: _with_source_x([None, None], pa.null()), id="null-type"),
    ],
)
def test_source_info_types_are_the_streams(make: Any) -> None:
    """F keeps each source-info column's type, as the rows do (ITL-1491).

    A list of ``None`` or of empty lists shows fewer levels than its column declares.
    The pipeline determines the column's type; neither the rows nor F take it from
    the tokens: both give the stream's type (a ``null`` column as ``large_string``,
    as every datagram stores it).
    """
    stream = make()
    built, _, _ = StaticOutputOperatorPod._rows_to_table(list(stream.iter_data()))  # pyright: ignore[reportPrivateUsage]  # R, the oracle
    table = stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    formed = StaticOutputOperatorPod._row_built_form(  # pyright: ignore[reportPrivateUsage]  # F, under test
        table, stream.keys()
    )
    assert formed.column_names == built.column_names
    assert formed.schema.equals(built.schema, check_metadata=True)
    assert_same_values(formed, built)
    declared = table.schema.field("_source_x").type
    assert formed.schema.field("_source_x").type == (
        pa.large_string() if pa.types.is_null(declared) else declared
    )


@pytest.mark.parametrize(
    ("values", "kind"),
    [
        pytest.param([["s"], []], _STRINGS, id="list-beside-empty"),
        pytest.param([[[None]], [["s"]]], _NESTED, id="null-inside-beside-nested"),
        pytest.param([[], [["s"]]], _NESTED, id="empty-beside-nested"),
        pytest.param(
            [[[], [[]]], [[["t"]]]], pa.large_list(_NESTED), id="three-levels"
        ),
        pytest.param(
            [["a", "b"], ["c", "d"]], pa.list_(pa.large_string(), 2), id="fixed-size"
        ),
        pytest.param(
            [["a"], ["b"]],
            pa.large_list(pa.field("member", pa.large_string(), nullable=False)),
            id="named-non-null-item",
        ),
    ],
)
def test_tokens_no_value_could_type_alike_make_one_table(
    values: list[Any], kind: pa.DataType
) -> None:
    """Rows whose tokens show two shapes, or a type no value gives, make one table.

    Before ITL-1491 each row typed its token from its value, so a token that shows
    fewer levels than its column (an empty list, a null inside a list) could not be
    concatenated with a full one (``ArrowInvalid``), and F said so
    (``SourceInfoTypesDisagree``). The rows and F now keep the stream's type: a
    fixed-size list stays one, and a list's item field keeps its name and
    nullability. (A null list token is not a valid token: the test below.)
    """
    stream = _with_source_x(values, kind)
    built, _, _ = StaticOutputOperatorPod._rows_to_table(list(stream.iter_data()))  # pyright: ignore[reportPrivateUsage]  # R, the oracle
    formed = StaticOutputOperatorPod._row_built_form(  # pyright: ignore[reportPrivateUsage]  # F, under test
        stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS), stream.keys()
    )
    for table in (built, formed):
        field = table.schema.field("_source_x")
        assert field.type.equals(kind, check_metadata=True)
        assert field.nullable
        assert table.column("_source_x").to_pylist() == values
    assert formed.schema.equals(built.schema, check_metadata=True)


class _NodeEvents(NoOpObserver):
    """Records node starts and ends."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def contextualize(self, *identity_path: str) -> _NodeEvents:
        return self

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: Any = None
    ) -> None:
        self.events.append(("start", node_label))

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        self.events.append(("end", node_label))


def _batched_job(
    database: InMemoryArrowDatabase, kind: pa.DataType | None = None
) -> PipelineJob:
    """``source -> select x -> Batch(2)``, the batch node in ``LOG`` mode.

    ``x`` has the list layers of the token type ``kind`` its slot is to be bound with.
    """
    source = _source(kind)
    job = PipelineJob(name="source_info_types", database=database)
    with job:
        Batch(batch_size=2)(source.select_data_columns(["x"]), label="batched")
    job.nodes["batched"].attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    return job


@pytest.mark.parametrize(
    ("values", "kind"),
    [
        pytest.param([None, None], _STRINGS, id="null-lists"),
        pytest.param([["s"], None], _STRINGS, id="list-beside-null"),
        pytest.param([None, [["s"]]], _NESTED, id="null-beside-nested"),
        pytest.param(
            [None, ["a", "b"]], pa.list_(pa.large_string(), 2), id="null-fixed"
        ),
    ],
)
def test_a_null_list_token_is_refused_when_the_stream_is_built(
    values: list[Any], kind: pa.DataType
) -> None:
    """A token that has a list layer is never null (the token-shape rule, ITL-1491).

    Replaces the cases that built streams with null list tokens ("null-lists",
    "batch-2-of-null-lists", "list-beside-null", and the job over null list tokens
    through ``Batch(2)``): such a stream is refused where it is built, naming the
    token column, its data column and both types, so neither F, the rows nor a job
    ever sees one. It still protects what those cases did: no table is built whose
    rows disagree on a token column's type.
    """
    with pytest.raises(InputValidationError) as refused:
        _ = _with_source_x(values, kind)
    message = str(refused.value)
    assert "'_source_x'" in message and "'x'" in message
    assert str(kind) in message and str(data_type_for(kind)) in message
    assert "null" in message


def test_a_job_keeps_and_stores_the_token_type_of_a_batch_of_null_tokens() -> None:
    """A bound raw source with null string tokens, through ``Batch(2)`` in ``LOG`` mode.

    A plain string token may be null. ``Batch`` puts the two null tokens into one
    list, an element of which may be null: kept and stored as
    ``large_list<large_string>``, the type ``Batch`` declares over string tokens.
    """
    database = InMemoryArrowDatabase()
    job = _batched_job(database)
    job.bind(sources={"s": _with_source_x([None, None], pa.large_string())})
    job.run()
    node = job.nodes["batched"]
    kept = node.as_table(columns={"source": True})
    assert kept.schema.field("_source_x").type == _STRINGS
    assert kept.column("_source_x").to_pylist() == [[None, None]]
    stored = node.get_all_records(columns={"source": True})
    assert stored is not None
    assert stored.schema.field("_source_x").type == _STRINGS


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("values", "kind"),
    [
        pytest.param([["s"], []], _STRINGS, id="list-beside-empty"),
        pytest.param([[[], [[]]], [[], [[]]]], pa.large_list(_NESTED), id="deep"),
    ],
)
def test_a_job_over_tokens_no_value_could_type_alike_completes(
    values: list[Any], kind: pa.DataType, asynchronous: bool
) -> None:
    """Rows that used to raise where they are built into one table now pass.

    A list token beside an empty list and a token whose value shows fewer levels
    than its column (``ArrowTypeError``) raised at the successor's edge, as on
    ``main``. The rows keep the stream's token type
    (ITL-1491), so the run completes on both orchestrators, with the same events
    and the same batch: one more list level of the declared type.
    """
    from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

    job = _batched_job(InMemoryArrowDatabase(), kind)
    job.bind(sources={"s": _with_source_x(values, kind)})
    observer = _NodeEvents()
    orchestrator = (
        AsyncPipelineOrchestrator() if asynchronous else SyncPipelineOrchestrator()
    )
    job.run(observer=observer, orchestrator=orchestrator)
    assert sorted(observer.events) == sorted(
        (event, label) for label in job.nodes for event in ("start", "end")
    )
    kept = job.nodes["batched"].as_table(columns={"source": True})
    assert kept.schema.field("_source_x").type == pa.large_list(kind)
    assert kept.column("_source_x").to_pylist() == [values]


def test_a_source_with_a_nullable_system_tag_is_not_bound() -> None:
    """A slot rejects a nullable ID declaration even when all values are present."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.errors import SourceSpecMismatchError
    from orcapod.sources import DictSource

    source = DictSource(
        [{"k": 1, "x": 1}, {"k": 2, "x": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
    )
    job = PipelineJob(name="nullable_system_tag", database=InMemoryArrowDatabase())
    with job:
        source.select_data_columns(["x"], label="selected")
    table = _source_table()
    record_id = next(
        name
        for name in table.column_names
        if name.startswith(constants.SYSTEM_TAG_PREFIX) and "record_id" in name
    )
    position = table.schema.get_field_index(record_id)
    with_null = table.set_column(
        position,
        pa.field(record_id, pa.large_string(), nullable=True),
        pa.array(["row_0", "row_1"], pa.large_string()),
    )
    slot = next(label for label in job.nodes if label != "selected")
    with pytest.raises(SourceSpecMismatchError, match="record_id"):
        job.bind(sources={slot: ArrowTableStream(with_null, tag_columns=["k"])})
