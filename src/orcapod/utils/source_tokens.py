"""Source-info tokens that are carried as a recipe, not as text (ITL-1464).

A source row's token is ``<source id>::<record id>::<column>``: the values of the row's
two system tags and the name the column had at the source. As text it repeats them for
every row and data column, about 35 times the data of a typical table. Inside the library
a token column can instead be **derived**: a column of ``DerivedTokenType`` holds no
bytes, and its type holds the recipe that gives the text from the table's own system-tag
columns. Filtering, taking, slicing, concatenating, selecting and renaming carry such a
column as any other; nothing is computed until the text is needed (``materialize``).

A derived column is an internal form of a table that operators pass on
(``StreamProtocol.as_carried_table``). It never leaves the library: ``as_table``, rows,
stored tables and everything hashed hold the text. A place that forgets fails loudly: no
value of a derived column can be read, cast to text, hashed, pickled, or written to
Parquet (its storage is a struct with no field, which Parquet refuses).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, NoReturn, TypeAlias, cast

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from orcapod.system_constants import constants

_TEXT = pa.large_string()

ROWS_PER_CHUNK = 65_536
"""The most rows whose token text is built in one step."""


_folds: dict[str, Callable[[Sequence[str | None]], str]] = {}


def register_fold(name: str, fold: Callable[[Sequence[str | None]], str]) -> None:
    """Say how a ``FoldRecipe`` named ``name`` makes one token of several.

    Args:
        name: The fold's name; for a fold with an algorithm label, the label.
        fold: One token from the members' tokens. Called once per row, with the members
            in the recipe's order.
    """
    _folds[name] = fold


@dataclass(frozen=True)
class TokenRecipe:
    """How the token of each row is made from the row's system tags.

    Attributes:
        source_id: The system-tag column holding the row's source id.
        record_id: The system-tag column holding the row's record id.
        column: The column name the token ends with: the column's name at the source,
            whatever it is called now.
    """

    source_id: str
    record_id: str
    column: str

    def to_json(self) -> Any:  # pyright: ignore[reportExplicitAny]  # JSON
        """The recipe as a JSON value: what two equal recipes share."""
        return [self.source_id, self.record_id, self.column]

    @property
    def read_columns(self) -> frozenset[str]:
        """The system-tag columns the recipe reads."""
        return frozenset((self.source_id, self.record_id))

    def renamed(self, names: Mapping[str, str]) -> TokenRecipe:
        """The recipe over system-tag columns renamed by ``names``."""
        return TokenRecipe(
            names.get(self.source_id, self.source_id),
            names.get(self.record_id, self.record_id),
            self.column,
        )

    def text(self, batch: pa.RecordBatch) -> pa.Array[Any]:  # pyright: ignore[reportExplicitAny]  # an Arrow array
        """The tokens of ``batch``'s rows, as ``large_string``."""
        return pc.binary_join_element_wise(  # pyright: ignore[reportCallIssue, reportUnknownVariableType]  # the stubs leave out scalar arguments
            batch.column(self.source_id).cast(_TEXT),
            batch.column(self.record_id).cast(_TEXT),
            pa.scalar(self.column, _TEXT),  # pyright: ignore[reportArgumentType]  # as above
            pa.scalar(constants.BLOCK_SEPARATOR, _TEXT),  # pyright: ignore[reportArgumentType]  # as above
        )


@dataclass(frozen=True)
class FoldRecipe:
    """How one token is made of the tokens of several merged columns, row by row.

    Attributes:
        fold: The registered fold (``register_fold``).
        members: The merged columns' recipes, in the order they were met.
    """

    fold: str
    members: tuple[Recipe, ...]

    def to_json(self) -> Any:  # pyright: ignore[reportExplicitAny]  # JSON
        """The recipe as a JSON value: what two equal recipes share."""
        return {"fold": self.fold, "members": [m.to_json() for m in self.members]}

    @property
    def read_columns(self) -> frozenset[str]:
        """The system-tag columns the members read."""
        return frozenset[str]().union(*(m.read_columns for m in self.members))

    def renamed(self, names: Mapping[str, str]) -> FoldRecipe:
        """The recipe over system-tag columns renamed by ``names``."""
        return FoldRecipe(self.fold, tuple(m.renamed(names) for m in self.members))

    def text(self, batch: pa.RecordBatch) -> pa.Array[Any]:  # pyright: ignore[reportExplicitAny]  # an Arrow array
        """The tokens of ``batch``'s rows, as ``large_string``."""
        fold = _folds[self.fold]
        members = [member.text(batch).to_pylist() for member in self.members]
        return pa.array(
            [fold(tokens) for tokens in zip(*members, strict=True)], type=_TEXT
        )


Recipe: TypeAlias = TokenRecipe | FoldRecipe


def recipe_from_json(value: Any) -> Recipe:  # pyright: ignore[reportExplicitAny, reportAny]  # JSON
    """The recipe whose ``to_json`` is ``value``."""
    if isinstance(value, dict):
        return FoldRecipe(
            value["fold"], tuple(recipe_from_json(m) for m in value["members"])
        )
    return TokenRecipe(*value)


class _DerivedTokenScalar(pa.ExtensionScalar):
    """A value of a derived column: it has none to read."""

    def as_py(self, *args: Any, **kwargs: Any) -> NoReturn:  # pyright: ignore[reportExplicitAny, reportIncompatibleMethodOverride]  # pyarrow's own signature varies by version
        """Refuse to give a value.

        Raises:
            TypeError: Always; the text must be built first (``materialize``).
        """
        raise TypeError(
            "a derived source-info token has no value: build its text first "
            "(source_tokens.materialize)"
        )


class _DerivedType(pa.ExtensionType):
    """What both kinds of derived column type share: their values refuse to be read.

    Internal: in no stored table and no schema Orcapod presents or hashes. Not
    registered with Arrow: a spilled table is written as its storage and wrapped in its
    type again. Two types are equal only with one recipe (``_parameters``).
    """

    def __init__(
        self, recipe: Recipe, storage_type: pa.DataType, extension_name: str
    ) -> None:
        self.recipe: Recipe = recipe
        super().__init__(storage_type, extension_name)

    def _parameters(self) -> Any:  # pyright: ignore[reportExplicitAny]  # JSON
        return self.recipe.to_json()

    def __arrow_ext_serialize__(self) -> bytes:
        return json.dumps(self._parameters()).encode()

    def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
        return _DerivedTokenScalar

    def __reduce__(self) -> NoReturn:
        """Refuse to be pickled: a table another process reads holds the text.

        Raises:
            TypeError: Always.
        """
        raise TypeError("a derived source-info token column is not pickled")

    # pyarrow compares extension types by name and storage only; Arrow asks this.
    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, _DerivedType)
            and type(self) is type(other)
            and self.storage_type == other.storage_type
            and self._parameters() == other._parameters()
        )

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash(self.recipe)


class DerivedTokenType(_DerivedType):
    """The Arrow type of a derived token column: no bytes, and the recipe."""

    def __init__(self, recipe: Recipe) -> None:
        super().__init__(recipe, pa.struct([]), "orcapod.internal.derived_token")

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> DerivedTokenType:
        return cls(recipe_from_json(json.loads(serialized)))


class MemberTokenType(_DerivedType):
    """The Arrow type of a derived list of tokens: one per member of a row's group.

    An aggregation folds its members' system tags into one digest, so a list column
    holds the members' own ids: its storage is a list of structs of the system-tag
    values the recipe reads (dictionaries over the input's columns, so a member is a few
    bytes and its id's text is held once), and an empty struct, which Parquet refuses.

    Attributes:
        recipe: How a member's token is made from its ids.
        list_type: The type of the text, a list of ``large_string``.
    """

    def __init__(
        self, recipe: Recipe, list_type: pa.DataType, storage_type: pa.DataType
    ) -> None:
        self.list_type: pa.DataType = list_type
        super().__init__(recipe, storage_type, "orcapod.internal.derived_member_tokens")

    def _parameters(self) -> Any:  # pyright: ignore[reportExplicitAny]  # JSON
        item: pa.Field[Any] = self.list_type.field(0)
        return [self.recipe.to_json(), item.name, item.nullable]

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> MemberTokenType:
        recipe, name, nullable = json.loads(serialized)
        item = pa.field(name, _TEXT, nullable=nullable)
        return cls(recipe_from_json(recipe), pa.large_list(item), storage_type)

    def with_list_type(self, list_type: pa.DataType) -> MemberTokenType:
        """The same tokens, whose text is of type ``list_type``."""
        return MemberTokenType(self.recipe, list_type, self.storage_type)

    def text(self, storage: Any) -> pa.Array[Any]:  # pyright: ignore[reportExplicitAny, reportAny]  # a large list of structs
        """The lists of tokens of one chunk's storage."""
        ids = storage.flatten()
        pieces = [
            self.recipe.text(
                pa.RecordBatch.from_struct_array(ids.slice(start, ROWS_PER_CHUNK))
            )
            for start in range(0, len(ids), ROWS_PER_CHUNK)
        ]
        tokens = pa.concat_arrays(pieces) if pieces else pa.array([], _TEXT)
        offsets = pc.subtract(storage.offsets, storage.offsets[0])
        return pa.LargeListArray.from_arrays(
            cast("pa.Int64Array", offsets), tokens, type=self.list_type
        )


class MemberTokens:
    """Builds the derived list columns of one aggregation; they share the members' ids.

    Args:
        table: The aggregation's input, with its system-tag columns.
        order: The input's rows in member order, group after group.
        offsets: ``int64`` offsets of each group in ``order``.
    """

    def __init__(
        self,
        table: pa.Table,
        order: pa.Array[Any],
        offsets: pa.Int64Array,  # pyright: ignore[reportExplicitAny]  # integers
    ) -> None:
        self._table = table
        self._order = order
        self._offsets = offsets
        self._lists: dict[frozenset[str], Any] = {}

    def column(self, recipe: Recipe, list_type: pa.DataType) -> pa.Array[Any]:  # pyright: ignore[reportExplicitAny]  # an Arrow array
        """The lists of the members' tokens, derived by ``recipe``.

        Args:
            recipe: The recipe of the aggregated token column.
            list_type: The type the lists have as text.
        """
        read = recipe.read_columns
        if read not in self._lists:
            names = sorted(read)
            ids = [self._ids(name) for name in names]
            guard = pa.Array.from_buffers(
                pa.struct([]), len(self._order), cast(Any, [None]), 0, 0
            )
            members = pa.StructArray.from_arrays([*ids, guard], names=[*names, ""])
            self._lists[read] = pa.LargeListArray.from_arrays(self._offsets, members)
        storage = self._lists[read]
        return pa.ExtensionArray.from_storage(
            MemberTokenType(recipe, list_type, storage.type), storage
        )

    def _ids(self, name: str) -> pa.Array[Any]:  # pyright: ignore[reportExplicitAny]  # an Arrow array
        """A system-tag column's values in member order, as a dictionary over them."""
        column = self._table.column(name)
        if name.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX):
            # A few values repeated over every row.
            encoded: Any = column.combine_chunks().dictionary_encode()
            return encoded.take(self._order)
        return pa.DictionaryArray.from_arrays(
            self._order.cast(pa.int64()), column.combine_chunks()
        )


def derived_array(recipe: Recipe, rows: int) -> pa.Array[Any]:  # pyright: ignore[reportExplicitAny]  # an Arrow array
    """A derived token column of ``rows`` rows: no row has a byte."""
    # No validity bitmap: no row is null. (The stubs do not allow the absent buffer.)
    storage = pa.Array.from_buffers(pa.struct([]), rows, cast(Any, [None]), 0, 0)
    return pa.ExtensionArray.from_storage(DerivedTokenType(recipe), storage)


def is_derived(kind: pa.DataType) -> bool:
    """Whether a column of type ``kind`` is derived: one token a row, or a list."""
    return isinstance(kind, _DerivedType)


def derived_recipe(kind: pa.DataType) -> Recipe | None:
    """The recipe of a column of one token a row; ``None`` for any other column."""
    return kind.recipe if isinstance(kind, DerivedTokenType) else None


def derived_columns(schema: pa.Schema) -> tuple[str, ...]:
    """The names of the derived token columns of ``schema``, lists included."""
    return tuple(f.name for f in schema if is_derived(f.type))


def recipe_columns(schema: pa.Schema) -> dict[str, Recipe]:
    """The columns of one derived token a row, which read the table's system tags."""
    return {
        f.name: recipe for f in schema if (recipe := derived_recipe(f.type)) is not None
    }


def members_per_row(table: pa.Table) -> Any:  # pyright: ignore[reportExplicitAny]  # a numpy array, or None
    """How many tokens each row's longest derived list has; ``None`` with no such list."""
    lengths: Any = None
    for column in table.columns:
        if not isinstance(column.type, MemberTokenType):
            continue
        chunks: list[Any] = [cast(Any, chunk).storage for chunk in column.chunks]
        storage = cast(Any, pa.chunked_array)(chunks, type=column.type.storage_type)
        sizes = pc.list_value_length(storage).to_numpy()
        lengths = sizes if lengths is None else np.maximum(lengths, sizes)
    return lengths


def member_texts(
    table: pa.Table,
    name: str,
    order: pa.Array[Any],  # pyright: ignore[reportExplicitAny]  # integers
    offsets: pa.Int64Array,
) -> Iterator[Iterable[str | None]]:
    """The text of column ``name`` of each group's members, a part at a time.

    Args:
        table: The aggregation's input, with the system tags a derived ``name`` reads.
        name: A text column, or one of derived tokens.
        order: The input's rows in member order, group after group.
        offsets: The offsets of each group in ``order``.

    Yields:
        One group's values, in member order; read each before the next.
    """
    recipe = derived_recipe(table.schema.field(name).type)
    read = {name} | (recipe.read_columns if recipe is not None else set[str]())
    table = table.select([c for c in table.column_names if c in read])

    def texts(start: int, stop: int) -> list[str | None]:
        rows = table.take(order.slice(start, stop - start))
        return cast("list[str | None]", materialize(rows).column(name).to_pylist())

    def large(start: int, stop: int) -> Iterator[str | None]:
        for part in range(start, stop, ROWS_PER_CHUNK):
            yield from texts(part, min(part + ROWS_PER_CHUNK, stop))

    bounds = cast("list[int]", offsets.to_pylist())
    group = 0
    while group + 1 < len(bounds):
        first = bounds[group]
        if bounds[group + 1] - first > ROWS_PER_CHUNK:
            yield large(first, bounds[group + 1])
            group += 1
            continue
        end = group + 1
        while end + 1 < len(bounds) and bounds[end + 1] - first <= ROWS_PER_CHUNK:
            end += 1
        values = texts(first, bounds[end])
        for start, stop in zip(bounds[group:end], bounds[group + 1 : end + 1]):
            yield values[start - first : stop - first]
        group = end


def retyped(
    column: pa.ChunkedArray[Any],  # pyright: ignore[reportExplicitAny]  # an Arrow column
    kind: MemberTokenType,
) -> pa.ChunkedArray[Any]:  # pyright: ignore[reportExplicitAny]  # an Arrow column
    """A derived list column under another type over the same storage."""
    chunks: list[Any] = [
        pa.ExtensionArray.from_storage(kind, cast(Any, chunk).storage)
        for chunk in column.chunks
    ]
    return cast(Any, pa.chunked_array)(chunks, type=kind)


def with_recipe(table: pa.Table, name: str, recipe: Recipe) -> pa.Table:
    """``table`` with its token column ``name`` derived by ``recipe``, its field kept."""
    tokens = derived_array(recipe, table.num_rows)
    field = table.schema.field(name).with_type(tokens.type)
    return table.set_column(table.schema.get_field_index(name), field, tokens)


def materialize(table: pa.Table, names: Collection[str] | None = None) -> pa.Table:
    """Return ``table`` with its derived token columns as text.

    Args:
        table: A table holding the system-tag columns its derived columns read.
        names: The derived columns to build; ``None`` for all of them.

    Returns:
        The table, each such column ``large_string`` with the field's own name and
        nullability; ``table`` itself when there is none.
    """
    recipes = {
        name: recipe
        for name, recipe in recipe_columns(table.schema).items()
        if names is None or name in names
    }
    built: dict[str, list[pa.Array[Any]]] = {name: [] for name in recipes}  # pyright: ignore[reportExplicitAny]  # Arrow arrays
    if recipes:
        read = {c for recipe in recipes.values() for c in recipe.read_columns}
        tags = table.select([c for c in table.column_names if c in read])
        for batch in tags.to_batches(max_chunksize=ROWS_PER_CHUNK):
            for name, recipe in recipes.items():
                built[name].append(recipe.text(batch))
    types: dict[str, pa.DataType] = dict.fromkeys(recipes, _TEXT)
    for field in table.schema:
        kind = field.type
        if isinstance(kind, MemberTokenType) and (names is None or field.name in names):
            types[field.name] = kind.list_type
            built[field.name] = [
                kind.text(cast(Any, chunk).storage)
                for chunk in table.column(field.name).chunks
            ]
    for name, kind in types.items():
        table = table.set_column(
            table.schema.get_field_index(name),
            table.schema.field(name).with_type(kind),
            cast(Any, pa.chunked_array)(built[name], type=kind),
        )
    return table


def materialize_reading(table: pa.Table, columns: Collection[str]) -> pa.Table:
    """Build the text of the derived columns that read one of ``columns``.

    Called before the values of system-tag columns change, so every token keeps naming
    the row it named.
    """
    return materialize(
        table,
        [
            name
            for name, recipe in recipe_columns(table.schema).items()
            if not recipe.read_columns.isdisjoint(columns)
        ],
    )


def rename_read_columns(table: pa.Table, names: Mapping[str, str]) -> pa.Table:
    """Point the derived columns of ``table`` at system-tag columns renamed by ``names``.

    Args:
        table: The table, its system-tag columns already renamed.
        names: Old to new column name.

    Returns:
        The table with each derived column's recipe reading the new names.
    """
    for name, recipe in recipe_columns(table.schema).items():
        renamed = recipe.renamed(names)
        if renamed != recipe:
            table = with_recipe(table, name, renamed)
    return table
