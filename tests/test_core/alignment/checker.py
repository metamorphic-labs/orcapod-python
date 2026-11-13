"""Check that every per-row column of a produced table belongs to its own row (ITL-1034).

``assert_rows_aligned`` walks every row and recomputes each derived column from that row
alone, never by comparing two runs (two wrong runs can agree):

* data values: each value must be the value its origin row has (``Oracle``), and every
  column of one row must come from the same origin row of each source;
* ``_source_<c>``: the token names the origin row whose value the row holds (element by
  element for list columns);
* system tags: a record id is the documented UUID v5 of the origin row the data came from,
  and the source id its source's id; a folded (aggregated) pair is the documented fold of
  the row's own members;
* user tags: the origin row's tag values (element by element for list-valued tags);
* ``_content_hash``: the data context's Arrow hash of the row's data columns;
* ``_context_key``: the stream's data context key.

A function's output token embeds the output datagram's uuid, made at run time: it cannot be
recomputed from the row, so it is compared, keyed on the row's record id, with the stored
record's (``function_tokens``). A column no rule covers fails the check, so a new kind of
per-row column cannot slip by unchecked.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from orcapod.system_constants import constants

from .oracle import Oracle, Origin, fold_record_ids, fold_values

MAX_REPORTED = 12
DEFAULT_CONTENT_HASH_COLUMN = "_content_hash"


@dataclass(frozen=True)
class Layout:
    """How the columns of a produced table relate to its origin rows.

    Attributes:
        tag_keys: The user tag columns.
        data_keys: The data columns.
        members: Columns holding one element per member row (``Batch``, ``GroupBy``): the
            row's members are the elements, the same members in every such column.
        merged: Columns holding one element per input (``MergeJoin``), each element from
            another input of the same row.
        content_hash_column: The content hash column's name when it is not the default.
        null_tokens: Whether a source-info cell may be null beside a value. Only for a
            plain ``DerivedSource`` (``node.as_source()``), whose source info is null in
            every row by the current design (ITL-1059); every other view fails on it.
    """

    tag_keys: tuple[str, ...]
    data_keys: tuple[str, ...]
    members: frozenset[str] = frozenset()
    merged: frozenset[str] = frozenset()
    content_hash_column: str = DEFAULT_CONTENT_HASH_COLUMN
    null_tokens: bool = False


@dataclass
class _Row:
    """The origin rows one produced row was found to come from, per member."""

    members: list[dict[str, Origin]] = field(default_factory=lambda: [{}])


def split_token(token: str) -> tuple[str, str, str]:
    """Split a source-info token into source id (or function uri), row token and column."""
    prefix, column = token.rsplit(constants.BLOCK_SEPARATOR, 1)
    source, row_token = prefix.rsplit(constants.BLOCK_SEPARATOR, 1)
    return source, row_token, column


class _RowChecker:
    """Check one row; collect every failure."""

    def __init__(
        self,
        index: int,
        row: dict[str, Any],
        layout: Layout,
        oracle: Oracle,
        function_tokens: Mapping[tuple[str, bytes], str] | None,
    ) -> None:
        self.index = index
        self.row = row
        self.layout = layout
        self.oracle = oracle
        self.function_tokens = function_tokens
        self.failures: list[str] = []
        self.state = _Row()
        self.checked: set[str] = set()

    def fail(self, column: str, message: str) -> None:
        """Record a failure of ``column`` in this row."""
        self.failures.append(f"row {self.index}, column {column!r}: {message}")

    # -- origins -----------------------------------------------------------

    def _elements(self, column: str) -> list[Any]:
        """The elements of ``column``: the list for a member or merged column."""
        value = self.row[column]
        if column in self.layout.members or column in self.layout.merged:
            return list(value) if value is not None else []
        return [value]

    def _member_count(self) -> int:
        """How many members the row has: the length of its member columns."""
        lengths = {len(self._elements(c)) for c in self.layout.members}
        if len(lengths) > 1:
            self.fail("members", f"member columns have different lengths {lengths}")
        return lengths.pop() if lengths else 1

    def _assign(self, member: int, column: str, origin: Origin) -> None:
        """Record that ``column`` of ``member`` comes from ``origin``; it must agree."""
        known = self.state.members[member].get(origin.source_id)
        if known is None:
            self.state.members[member][origin.source_id] = origin
        elif known is not origin:
            self.fail(
                column,
                f"comes from row {origin.token!r} of {origin.source_id!r}, but another "
                f"column of the same row comes from row {known.token!r}",
            )

    def _resolve(self, column: str, value: Any, token: str | None) -> Origin | None:
        """The origin of one element of a data column, from its token and its value."""
        from_token: Origin | None = None
        token_name = f"{constants.SOURCE_PREFIX}{column}"
        if token is not None:
            self.checked.add(token_name)
            source, row_token, token_column = split_token(token)
            if column in self.oracle.derivations:
                # A function output's token names the column itself; an operator's
                # derived token (``Pick``, ``Index``) is checked whole, by its recipe.
                if self.oracle.derivations[column].token is None and (
                    token_column != column
                ):
                    self.fail(
                        token_name, f"token {token!r} names column {token_column!r}"
                    )
            else:
                if token_column != self.oracle.base_name(column):
                    self.fail(
                        token_name, f"token {token!r} names column {token_column!r}"
                    )
                from_token = self.oracle.by_token.get((source, row_token))
                if from_token is None:
                    self.fail(token_name, f"token {token!r} names no source row")
        if from_token is not None:
            expected = self.oracle.expected_value(column, from_token)
            if expected != value:
                self.fail(
                    token_name,
                    f"value {value!r} but its token {token!r} names a row whose value "
                    f"is {expected!r}",
                )
            return from_token
        if value is None:
            return None
        candidates = self.oracle.origins_for_value(column, value)
        if not candidates:
            self.fail(column, f"value {value!r} is the value of no origin row")
            return None
        return candidates[0] if len(candidates) == 1 else None

    def _tokens(self, column: str, count: int) -> list[str | None]:
        """The source-info tokens of ``column``, one per element, or ``None`` each."""
        name = f"{constants.SOURCE_PREFIX}{column}"
        if name not in self.row:
            return [None] * count
        value = self.row[name]
        if column in self.layout.members or column in self.layout.merged:
            tokens = list(value) if value is not None else []
            if len(tokens) != count:
                self.fail(name, f"{len(tokens)} tokens for {count} values")
                return [None] * count
            return tokens
        return [value]

    def _check_present_token(self, column: str, value: Any, token: str | None) -> None:
        """A source-info cell that is present must not be null beside a value."""
        name = f"{constants.SOURCE_PREFIX}{column}"
        if name not in self.row or token is not None:
            return
        if self.layout.null_tokens:
            # ITL-1059: a plain DerivedSource has null source info by design (for now).
            self.checked.add(name)
        elif value is not None:
            self.fail(name, f"null token beside the value {value!r}")

    def resolve_data(self) -> None:
        """Find each data element's origin; all columns of a member must agree."""
        count = self._member_count()
        self.state.members = [{} for _ in range(count)]
        for column in self.layout.data_keys:
            elements = self._elements(column)
            tokens = self._tokens(column, len(elements))
            for position, (value, token) in enumerate(
                zip(elements, tokens, strict=True)
            ):
                self._check_present_token(column, value, token)
                origin = self._resolve(column, value, token)
                if origin is None:
                    continue
                member = position if column in self.layout.members else 0
                if column in self.layout.merged:
                    self._assign_merged(column, origin)
                else:
                    self._assign(member, column, origin)
                    self.checked.add(column)
                if token is not None and column in self.oracle.derivations:
                    self._check_function_token(column, origin, token)

    def _assign_merged(self, column: str, origin: Origin) -> None:
        """Record the origin of one element of a merged column (one element per input)."""
        self._assign(0, column, origin)
        self.checked.add(column)

    def _check_function_token(self, column: str, origin: Origin, token: str) -> None:
        """A derived column's token: recomputed, or the one stored for this row's record."""
        name = f"{constants.SOURCE_PREFIX}{column}"
        recompute = self.oracle.derivations[column].token
        if recompute is not None:
            expected = recompute(origin)
            if token != expected:
                self.fail(name, f"token {token!r}, its row's token is {expected!r}")
            return
        if self.function_tokens is None:
            _, row_token, _ = split_token(token)
            if len(row_token) != 32:
                self.fail(name, f"token {token!r} does not name an output datagram")
            return
        expected = self.function_tokens.get((column, origin.record_id))
        if expected != token:
            self.fail(
                name,
                f"token {token!r}, but the record of row {origin.token!r} stores "
                f"{expected!r}",
            )

    # -- system tags ---------------------------------------------------------

    def check_system_tags(self) -> None:
        """Every system tag pair belongs to this row's origin rows (or folds its members)."""
        source_prefix = constants.SYSTEM_TAG_SOURCE_ID_PREFIX
        record_prefix = constants.SYSTEM_TAG_RECORD_ID_PREFIX
        folded = bool(self.layout.members)
        for name in self.row:
            if not name.startswith(source_prefix):
                continue
            suffix = name[len(source_prefix) :]
            record_name = f"{record_prefix}{suffix}"
            if record_name not in self.row:
                self.fail(name, f"no matching record id column {record_name!r}")
                continue
            self.checked.update((name, record_name))
            source_value = self.row[name]
            record_value = self.row[record_name]
            if folded:
                self._check_folded(name, source_value, record_value)
            else:
                self._check_scalar(record_name, source_value, record_value)

    def _check_scalar(self, name: str, source_value: Any, record_value: Any) -> None:
        """A scalar record id is the record id of this row's origin row of its source."""
        origin = self.oracle.by_record_id.get(record_value)
        if origin is None:
            self.fail(name, f"record id {record_value!r} is no source row's")
            return
        if origin.source_id != source_value:
            self.fail(
                name,
                f"record id of a row of {origin.source_id!r} beside source id "
                f"{source_value!r}",
            )
            return
        self._assign(0, name, origin)

    def _check_folded(self, name: str, source_value: Any, record_value: Any) -> None:
        """A folded pair is the documented fold of this row's members of one source."""
        sources = {s for member in self.state.members for s in member}
        for source in sorted(sources):
            origins = [member.get(source) for member in self.state.members]
            if fold_values([source] * len(origins)) != source_value:
                continue
            if any(o is None for o in origins):
                self.fail(name, f"members of {source!r} not all known; cannot fold")
                return
            expected = fold_record_ids([o.record_id for o in origins if o is not None])
            if expected != record_value:
                self.fail(
                    name,
                    "folded record id is not the fold of the row's own members "
                    f"{[o.token for o in origins if o is not None]!r}",
                )
            return
        self.fail(name, f"folded source id {source_value!r} folds no member list")

    # -- values --------------------------------------------------------------

    def check_values(self) -> None:
        """Every tag and data value is its origin row's value."""
        for column in (*self.layout.tag_keys, *self.layout.data_keys):
            if column in self.layout.merged:
                self._check_merged_values(column)
                continue
            elements = self._elements(column)
            if column in self.layout.members:
                if len(elements) != len(self.state.members):
                    self.fail(column, "length differs from the row's member count")
                    continue
                pairs = list(zip(elements, self.state.members, strict=True))
            else:
                pairs = [(elements[0], m) for m in self.state.members]
            for value, member in pairs:
                for origin in member.values():
                    if not self.oracle.holds(column, origin):
                        continue
                    self.checked.add(column)
                    expected = self.oracle.expected_value(column, origin)
                    if expected != value:
                        self.fail(
                            column,
                            f"value {value!r}, but its row {origin.token!r} of "
                            f"{origin.source_id!r} has {expected!r}",
                        )

    def _check_merged_values(self, column: str) -> None:
        """A merged column holds exactly the values of the row's inputs that have it."""
        expected = [
            self.oracle.expected_value(column, o)
            for o in self.state.members[0].values()
            if self.oracle.holds(column, o)
        ]
        actual = self._elements(column)
        if sorted(map(repr, expected)) != sorted(map(repr, actual)):
            self.fail(column, f"holds {actual!r}, its inputs have {expected!r}")
        self.checked.add(column)


def assert_rows_aligned(
    table: pa.Table,
    layout: Layout,
    oracle: Oracle,
    data_context: Any,
    *,
    function_tokens: Mapping[tuple[str, bytes], str] | None = None,
    require_rows: bool = True,
) -> set[str]:
    """Assert that every per-row column of every row of ``table`` belongs to that row.

    Args:
        table: The produced table.
        layout: How its columns relate to the origin rows.
        oracle: The origin rows, from the tables the test wrote.
        data_context: The stream's data context: its key and Arrow hasher.
        function_tokens: The stored source-info token of each function output, keyed by
            ``(column, origin record id)``; without it a function token's form is checked.
        require_rows: Fail on a table with no rows (a check of nothing).

    Returns:
        The names of the columns at least one rule compared.

    Raises:
        AssertionError: Naming every misaligned row and column (the first few).
    """
    if require_rows:
        assert table.num_rows > 0, (
            "the table has no rows: the check would check nothing"
        )
    hasher = data_context.arrow_hasher
    context_key = data_context.context_key
    hash_column = layout.content_hash_column
    known = set(layout.tag_keys) | set(layout.data_keys)
    unknown = [
        c
        for c in table.column_names
        if c not in known
        and c != hash_column
        and c != constants.CONTEXT_KEY
        # Only source-id / record-id pairs: any other system tag has no rule.
        and not c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        and not c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
        and not (
            c.startswith(constants.SOURCE_PREFIX)
            and c[len(constants.SOURCE_PREFIX) :] in layout.data_keys
        )
    ]
    assert not unknown, f"no alignment rule covers the columns {unknown!r}"

    failures: list[str] = []
    checked: set[str] = set()
    rows = table.to_pylist()
    for index, row in enumerate(rows):
        checker = _RowChecker(index, row, layout, oracle, function_tokens)
        checker.resolve_data()
        checker.check_system_tags()
        checker.check_values()
        if hash_column in row:
            data_row = table.slice(index, 1).select(list(layout.data_keys))
            expected = hasher.hash_table(data_row).to_string()
            if row[hash_column] != expected:
                checker.fail(
                    hash_column,
                    f"{row[hash_column]!r} is not the hash of the row's data {expected!r}",
                )
            checker.checked.add(hash_column)
        if constants.CONTEXT_KEY in row:
            if row[constants.CONTEXT_KEY] != context_key:
                checker.fail(constants.CONTEXT_KEY, f"{row[constants.CONTEXT_KEY]!r}")
            checker.checked.add(constants.CONTEXT_KEY)
        failures.extend(checker.failures)
        checked |= checker.checked
    if failures:
        shown = "\n  ".join(failures[:MAX_REPORTED])
        raise AssertionError(
            f"{len(failures)} misaligned cells in {table.num_rows} rows:\n  {shown}"
        )
    # Every column must have been compared in some row: a column no rule reached is a
    # column the check said nothing about.
    if table.num_rows:
        assert_all_columns_checked(table, checked)
    return checked


def assert_requested_columns(
    table: pa.Table, config: Mapping[str, Any], hash_column: str
) -> None:
    """Assert ``table`` has the per-row columns ``config`` asks for.

    A table that silently left out a requested column would be checked on fewer
    columns than asked: the content hash column and the context key are required
    whenever the configuration (or ``all_info``) asks for them.

    Args:
        table: The produced table.
        config: The keyword arguments the table was asked with (``columns``,
            ``all_info``).
        hash_column: The name the content hash column must have.
    """
    columns = config.get("columns", {})
    all_info = bool(config.get("all_info"))
    if all_info or columns.get("content_hash"):
        assert hash_column in table.column_names, f"no {hash_column!r} column"
    if all_info or columns.get("context"):
        assert constants.CONTEXT_KEY in table.column_names, "no context key column"


def assert_all_columns_checked(table: pa.Table, checked: Collection[str]) -> None:
    """Assert that the checker compared every column of ``table`` at least once."""
    missed = [c for c in table.column_names if c not in checked]
    assert not missed, f"the check compared nothing in the columns {missed!r}"


def rows_by_identity(
    table: pa.Table, ignore: Collection[str] = ()
) -> dict[tuple[Any, ...], dict[str, Any]]:
    """The rows of ``table`` keyed by their record ids (every system tag record id column).

    Raises:
        AssertionError: If the table has no record id column or two rows share a key.
    """
    keys = sorted(
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    )
    assert keys, "the table has no record id column to key rows by"
    keyed: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in table.to_pylist():
        key = tuple(row[k] for k in keys)
        assert key not in keyed, f"two rows share the record ids {key!r}"
        keyed[key] = {c: v for c, v in row.items() if c not in ignore}
    return keyed


def assert_same_rows_by_identity(
    left: pa.Table, right: pa.Table, ignore: Collection[str] = ()
) -> None:
    """Assert two tables hold the same rows, matched by record ids, never by position.

    Args:
        left: One table.
        right: The other; same columns in the same order.
        ignore: Columns that differ between runs by design (a per-run uuid, a time).
    """
    assert left.column_names == right.column_names, (
        f"columns differ: {left.column_names} vs {right.column_names}"
    )
    left_rows = rows_by_identity(left, ignore)
    right_rows = rows_by_identity(right, ignore)
    assert left_rows.keys() == right_rows.keys(), "the tables hold different records"
    for key, row in left_rows.items():
        assert row == right_rows[key], f"record {key!r}: {row} != {right_rows[key]}"


def rows_table(pairs: Iterable[tuple[Any, Any]]) -> pa.Table:
    """The ``(tag, data)`` rows an iteration yielded, as one table with every group.

    Each row's own Arrow tables (tag with system tags; data with source info and context)
    side by side, so the rows ``iter_data`` yields are checked like a produced table.
    """
    from orcapod.utils import arrow_utils

    tables = [
        arrow_utils.hstack_tables(
            tag.as_table(columns={"system_tags": True}),
            data.as_table(columns={"source": True, "context": True}),
        )
        for tag, data in pairs
    ]
    return pa.concat_tables(tables, promote_options="permissive")
