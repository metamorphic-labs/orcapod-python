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
value of a derived column can be read, cast to text, hashed, or written to Parquet (its
storage is a struct with no field, which Parquet refuses).
"""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any, NoReturn

import pyarrow as pa
import pyarrow.compute as pc

from orcapod.system_constants import constants

_TEXT = pa.large_string()

ROWS_PER_CHUNK = 65_536
"""The most rows whose token text is built in one step."""


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

    def to_json(self) -> str:
        """The recipe as text: what two equal recipes share."""
        return json.dumps([self.source_id, self.record_id, self.column])

    @classmethod
    def from_json(cls, text: str) -> TokenRecipe:
        """The recipe ``to_json`` wrote."""
        return cls(*json.loads(text))

    def renamed(self, names: Mapping[str, str]) -> TokenRecipe:
        """The recipe over system-tag columns renamed by ``names``."""
        return TokenRecipe(
            names.get(self.source_id, self.source_id),
            names.get(self.record_id, self.record_id),
            self.column,
        )

    def reads(self, columns: Collection[str]) -> bool:
        """Whether the recipe reads one of ``columns``."""
        return self.source_id in columns or self.record_id in columns

    def text(self, batch: pa.RecordBatch) -> pa.Array[Any]:  # pyright: ignore[reportExplicitAny]  # an Arrow array
        """The tokens of ``batch``'s rows, as ``large_string``."""
        return pc.binary_join_element_wise(
            batch.column(self.source_id).cast(_TEXT),
            batch.column(self.record_id).cast(_TEXT),
            pa.scalar(self.column, _TEXT),
            pa.scalar(constants.BLOCK_SEPARATOR, _TEXT),
        )


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


class DerivedTokenType(pa.ExtensionType):
    """The Arrow type of a derived token column: no bytes, and the recipe.

    Internal and never registered, so it is in no stored table and no schema Orcapod
    presents or hashes. Two derived columns have one type only with one recipe.
    """

    def __init__(self, recipe: TokenRecipe) -> None:
        self.recipe = recipe
        super().__init__(pa.struct([]), "orcapod.internal.derived_token")

    def __arrow_ext_serialize__(self) -> bytes:
        return self.recipe.to_json().encode()

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> DerivedTokenType:
        return cls(TokenRecipe.from_json(serialized.decode()))

    def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
        return _DerivedTokenScalar

    # pyarrow compares extension types by name and storage only; Arrow asks this.
    def __eq__(self, other: object) -> bool:
        return isinstance(other, DerivedTokenType) and self.recipe == other.recipe

    def __hash__(self) -> int:
        return hash(self.recipe)


def derived_array(recipe: TokenRecipe, rows: int) -> pa.Array[Any]:  # pyright: ignore[reportExplicitAny]  # an Arrow array
    """A derived token column of ``rows`` rows: no row has a byte."""
    storage = pa.Array.from_buffers(pa.struct([]), rows, [None], 0, 0)
    return pa.ExtensionArray.from_storage(DerivedTokenType(recipe), storage)


def derived_recipe(kind: pa.DataType) -> TokenRecipe | None:
    """The recipe of a column of type ``kind``; ``None`` when it is not derived."""
    return kind.recipe if isinstance(kind, DerivedTokenType) else None


def derived_columns(schema: pa.Schema) -> tuple[str, ...]:
    """The names of the derived token columns of ``schema``."""
    return tuple(f.name for f in schema if derived_recipe(f.type) is not None)


def materialize(table: pa.Table, names: Collection[str] | None = None) -> pa.Table:
    """Return ``table`` with its derived token columns as text.

    Args:
        table: A table holding the system-tag columns its derived columns read.
        names: The derived columns to build; ``None`` for all of them.

    Returns:
        The table, each such column ``large_string`` with the field's own name and
        nullability; ``table`` itself when there is none.
    """
    wanted = [
        name for name in derived_columns(table.schema) if names is None or name in names
    ]
    if not wanted:
        return table
    schema = table.schema
    recipes: dict[str, TokenRecipe] = {}
    for name in wanted:
        recipe = derived_recipe(schema.field(name).type)
        assert recipe is not None
        recipes[name] = recipe
    read = {c for r in recipes.values() for c in (r.source_id, r.record_id)}
    tags = table.select([c for c in table.column_names if c in read])
    chunks: dict[str, list[pa.Array[Any]]] = {name: [] for name in wanted}  # pyright: ignore[reportExplicitAny]  # Arrow arrays
    for batch in tags.to_batches(max_chunksize=ROWS_PER_CHUNK):
        for name, recipe in recipes.items():
            chunks[name].append(recipe.text(batch))
    for name in wanted:
        index = schema.get_field_index(name)
        table = table.set_column(
            index,
            schema.field(name).with_type(_TEXT),
            pa.chunked_array(chunks[name], type=_TEXT),
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
            for name in derived_columns(table.schema)
            if (recipe := derived_recipe(table.schema.field(name).type)) is not None
            and recipe.reads(columns)
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
    for name in derived_columns(table.schema):
        field = table.schema.field(name)
        recipe = derived_recipe(field.type)
        assert recipe is not None
        renamed = recipe.renamed(names)
        if renamed == recipe:
            continue
        table = table.set_column(
            table.schema.get_field_index(name),
            field.with_type(DerivedTokenType(renamed)),
            derived_array(renamed, table.num_rows),
        )
    return table


def text_schema(schema: pa.Schema) -> pa.Schema:
    """``schema`` with each derived token column as the text it stands for."""
    if not derived_columns(schema):
        return schema
    return pa.schema(
        [
            field.with_type(_TEXT) if derived_recipe(field.type) else field
            for field in schema
        ],
        metadata=schema.metadata,
    )
