from __future__ import annotations

import functools
import math
import os
import re
from collections.abc import Collection
from typing import Any, ClassVar

import pyarrow as pa
from typing_extensions import override

from orcapod.core.sources.base import (
    RootSource,
    saved_tag_only_columns,
    source_path_text,
)
from orcapod.core.data_tags import TagOnlyColumns, source_data_tags
from orcapod.core.sources.source_ids import location_source_name
from orcapod.core.sources.reload import keep_nullable_columns, same_rows
from orcapod.core.sources.stream_builder import (
    normalize_record_id_columns,
    SourceStreamBuilder,
    SourceStreamResult,
)
from orcapod.utils import arrow_utils
from orcapod.utils.schema_utils import _normalize_column_list

_INFINITY = re.compile(r"\s*[+-]?inf(inity)?\s*", re.IGNORECASE)
"""The texts pyarrow's CSV reader reads as an infinity: ``inf`` or ``infinity``, any
case, with an optional sign and surrounding spaces."""


def _storage_type(declared: pa.DataType) -> pa.DataType:
    """Return a type's storage type: itself, or a logical type's storage."""
    while isinstance(declared, pa.ExtensionType):
        declared = declared.storage_type
    return declared


def _is_text_like(declared: pa.DataType) -> bool:
    """Return whether a column of ``declared`` type follows the text null rule (option C).

    The one place that decides the class: a column stored as a string or binary type
    (``string``, ``large_string``, ``binary``, ``large_binary``,
    ``fixed_size_binary``, or a logical type stored as one of them) holds a cell's
    text as written, so only an unquoted empty field is null. Every other column
    reads pyarrow's null markers as null.
    """
    storage = _storage_type(declared)
    return (
        pa.types.is_string(storage)
        or pa.types.is_large_string(storage)
        or pa.types.is_binary(storage)
        or pa.types.is_large_binary(storage)
        or pa.types.is_fixed_size_binary(storage)
    )


def _read_as_text(declared: pa.DataType) -> bool:
    """Return whether a declared type is read as text and cast (the reader cannot parse it)."""
    storage = _storage_type(declared)
    return pa.types.is_float16(storage) or pa.types.is_decimal256(storage)


@functools.cache
def _reader_parses(arrow_type: pa.DataType) -> bool:
    """Return whether pyarrow's CSV reader parses ``arrow_type`` (asked of the reader)."""
    import pyarrow.csv as pa_csv

    try:
        pa_csv.read_csv(
            pa.BufferReader(b"v\n"),  # pyright: ignore[reportArgumentType]  # pyarrow reads a NativeFile; its stubs name only paths and IO
            convert_options=pa_csv.ConvertOptions(column_types={"v": arrow_type}),
        )
    except pa.ArrowNotImplementedError:
        return False
    return True


def _reader_type(declared: pa.DataType) -> pa.DataType:
    """Return the type the CSV reader parses a declared column's text as (ITL-1322).

    The declared type itself wherever pyarrow's CSV reader parses it from text; a
    logical (extension) type's storage type, by the same rule (the final cast wraps
    it); ``large_string`` for a type the reader cannot parse but a checked cast from
    text can (``float16``, ``decimal256``).

    Raises:
        ValueError: If no CSV cell can hold a value of the type (a list, struct, map,
            union or other nested type), or the reader cannot parse it (for example
            ``decimal32``, ``string_view``, an interval).
    """
    storage = _storage_type(declared)
    if _read_as_text(storage):
        return pa.large_string()
    if pa.types.is_nested(storage) or pa.types.is_run_end_encoded(storage):
        raise ValueError(f"a CSV cell is text, and {declared} cannot be read from it")
    if not _reader_parses(storage):
        raise ValueError(f"pyarrow's CSV reader cannot read {declared} from text")
    return storage


def _refuse_overflow(values: pa.ChunkedArray[Any], texts: pa.ChunkedArray[Any]) -> None:
    """Raise if a finite text was read as an infinity (it does not fit the float type).

    An infinity is kept only where the text spells one (``_INFINITY``); ordinary
    rounding of a finite value is not checked.

    Raises:
        ValueError: Naming the first text that overflows.
    """
    import pyarrow.compute as pc

    if not pc.any(pc.fill_null(pc.is_inf(values), pa.scalar(False))).as_py():
        return
    for value, text in zip(values.to_pylist(), texts.to_pylist()):
        if value is not None and math.isinf(value):
            if text is None or not _INFINITY.fullmatch(text):
                raise ValueError(
                    f"the text {text!r} does not fit {values.type}: a finite value "
                    "read as an infinity is refused"
                )


def _read_declared(path: str, schema: pa.Schema) -> pa.Table:
    """Read a CSV file with every column parsed as ``schema`` declares it (ITL-1322).

    The text of each cell is parsed as its column's declared type; nothing is
    inferred and then cast. Nulls (developer decision, option C; the class is decided
    by ``_is_text_like``):

    - A text-like column (string or binary, of any width, or a logical type stored
      as one): only an unquoted empty field is null. A quoted empty field is empty,
      and every other text (``NA``, ``null``, spaces) is kept as written.
    - Any other column: pyarrow's null markers (an empty field, ``NA``, ``null``,
      ``N/A``, ``NaN`` and the rest of ``ConvertOptions().null_values``) are null.

    The file is read once into memory (decompressed as the reader would from its
    name) and every parse reads that one snapshot, so a file replaced meanwhile
    cannot mix two versions. pyarrow's null list applies to a whole parse, so a file
    with both classes of columns is parsed twice with one set of read and parse
    options, the text-like columns the second time, and the row counts are compared.
    A float column holding an infinity is parsed as text too, and a finite text read
    as an infinity (a value too large for the type) raises. A type the reader cannot
    parse (``float16``, ``decimal256``) is read as text and cast with a checked
    cast, its null markers null. The table is then given ``schema`` exactly (field
    flags and metadata; a null in a non-nullable field raises).

    Raises:
        ValueError: If the file's columns are not the schema's fields in order, a
            declared type cannot be read from text, a value overflows its float
            type (the note names the column), the parses disagree on the number of
            rows, a cell is not a value of its column's type (pyarrow names the
            column by its position in the file), or a non-nullable field holds a
            null.
    """
    import pyarrow.compute as pc
    import pyarrow.csv as pa_csv

    reader_types: dict[str, pa.DataType] = {}
    for field in schema:
        try:
            reader_types[field.name] = _reader_type(field.type)
        except ValueError as exc:
            exc.add_note(f"column {field.name!r}")
            raise
    text_like = [f.name for f in schema if _is_text_like(f.type)]
    # One snapshot of the file, and one set of read and parse options, for every parse.
    with pa.input_stream(path) as stream:
        data = stream.read()
    read_options = pa_csv.ReadOptions()
    parse_options = pa_csv.ParseOptions()
    # pyarrow's default null markers, which the reader applies to other columns.
    markers: list[str] = list(pa_csv.ConvertOptions().null_values or ())

    def parse(convert_options: pa_csv.ConvertOptions) -> pa.Table:
        return pa_csv.read_csv(
            pa.BufferReader(data),  # pyright: ignore[reportArgumentType]  # pyarrow reads a NativeFile; its stubs name only paths and IO
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

    def as_text(columns: list[str], include: list[str]) -> pa_csv.ConvertOptions:
        return pa_csv.ConvertOptions(
            column_types={
                name: reader_types[name] if name in text_like else pa.large_string()
                for name in columns
            },
            include_columns=include,
            null_values=[""],
            strings_can_be_null=True,
            quoted_strings_can_be_null=False,
        )

    if len(text_like) == len(reader_types):
        table = parse(as_text(text_like, []))  # text-like columns only: one parse
    else:
        table = parse(pa_csv.ConvertOptions(column_types=reader_types))
    if table.column_names != schema.names:
        raise ValueError(
            f"the file's columns {table.column_names} are not the schema's fields "
            f"{schema.names}: a schema names every column of the file, in order"
        )
    # Floats the reader parsed, holding an infinity: their text tells an infinity
    # spelled in the file from a finite value too large for the type.
    infinite = [
        f.name
        for f in schema
        if pa.types.is_floating(reader_types[f.name])
        and pc.any(
            pc.fill_null(pc.is_inf(table.column(f.name)), pa.scalar(False))
        ).as_py()
    ]
    second = (text_like if len(text_like) < len(reader_types) else []) + infinite
    if second:
        texts = parse(as_text(second, second))
        if texts.num_rows != table.num_rows:
            raise ValueError(
                f"the second parse of the file read {texts.num_rows} rows and the "
                f"first {table.num_rows}: the two parses disagree"
            )
        for name in second:
            if name in text_like:
                table = table.set_column(
                    table.column_names.index(name), name, texts.column(name)
                )
            else:
                try:
                    _refuse_overflow(table.column(name), texts.column(name))
                except ValueError as exc:
                    exc.add_note(f"column {name!r}")
                    raise
    for field in schema:
        if not _read_as_text(field.type):
            continue
        # A type the reader cannot parse (float16, decimal256), read as text: its
        # markers are null, and surrounding spaces go as the reader drops them.
        text = pc.utf8_trim_whitespace(table.column(field.name))
        is_marker = pc.is_in(text, value_set=pa.array(markers, pa.large_string()))
        text = pc.if_else(is_marker, pa.scalar(None, text.type), text)
        try:
            value = pc.cast(text, _storage_type(field.type), safe=True)
            if pa.types.is_floating(value.type):
                _refuse_overflow(value, text)
        except (pa.ArrowInvalid, ValueError) as exc:
            exc.add_note(f"column {field.name!r}")
            raise
        table = table.set_column(
            table.column_names.index(field.name), field.name, value
        )
    # A null in a non-nullable field raises here, naming the field.
    return table.cast(schema)


def _file_state(path: str) -> tuple[int, int]:
    """Return a file's modification time (nanoseconds) and size: what tells a change."""
    stat = os.stat(path)
    return (stat.st_mtime_ns, stat.st_size)


class CSVSource(RootSource):
    """A source backed by a CSV file.

    The file is read once at construction time using PyArrow's CSV reader,
    converted to an Arrow table, and enriched by ``SourceStreamBuilder``
    (source-info, schema-hash, system tags).

    At the start of every run (``check_for_new_data``, ITL-1102) the source looks at
    the file's modification time and size. When either changed, it reads the file
    again and, if the rows changed (in order without a ``record_id_columns``: row
    positions are record ids; in any order with one), holds the new rows and
    moves its modified time. Record ids keep their rule: the ``record_id_columns``
    value, or the row's position.

    Data-tags (ITL-1023): every declared tag column is a data-tag by default,
    also a data column of the same name. ``tag_only_columns`` (keyword-only) names the
    tags kept plain (tag only, not data, as in v0.3); ``True`` keeps all of them plain.

    A path-like ``file_path`` is stored as its filesystem text, which also supplies
    the default source name.
    """

    SOURCE_KIND: ClassVar[str] = "csv"
    """Config type key and the kind in the source id's kind part (one vocabulary)."""
    SOURCE_VERSION: ClassVar[int] = 1
    """The version of this kind's implementation, in the kind part (ITL-1425)."""
    TABLE_IS_ROWS: ClassVar[bool] = True
    TAKES_TAG_ONLY_COLUMNS: ClassVar[bool] = True

    def __init__(
        self,
        file_path: str | os.PathLike[str],
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_columns: str | Collection[str] | None = None,
        source_id: str | None = None,
        schema: pa.Schema | None = None,
        *,
        tag_only_columns: TagOnlyColumns = (),
        **kwargs: Any,
    ) -> None:
        self._file_path: str = source_path_text(file_path)
        super().__init__(source_id=source_id, **kwargs)
        if self._source_name is None:
            # A default name is where the data lives, not a name the user gave: it is
            # not checked for a start shaped like a source id (ITL-1425).
            self._source_name = location_source_name(self._file_path)

        # Every later read (a check, ITL-1102) opens the file found now, whatever the
        # working directory is then.
        self._access_path = os.path.abspath(self._file_path)
        self._schema = schema
        self._tag_columns: Collection[str] = _normalize_column_list(tag_columns)
        self._system_tag_columns: Collection[str] = system_tag_columns
        self._record_id_columns = normalize_record_id_columns(record_id_columns)
        self._key_columns = self._record_id_columns or ()
        self._file_state = _file_state(self._access_path)
        table = self._read_table()
        # The rows as read, columns and types included: a check compares a new read
        # with them, and refuses a table whose columns changed.
        self._table: pa.Table = table
        # Every declared tag is a data-tag unless kept tag-only (ITL-1023); a
        # rebuild after a check (ITL-1102) applies the same choice.
        data_tags = source_data_tags(self._tag_columns, tag_only_columns)
        self._tag_only_columns: tuple[str, ...] = tuple(
            c for c in self._tag_columns if c not in data_tags
        )
        result = self._build(table)
        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        if self._source_name is None:
            self._source_name = result.source_name

    def _read_table(self) -> pa.Table:
        """Read the file into an Arrow table, typed as at construction.

        With ``schema=`` every column is read as declared (ITL-1322): the text is
        parsed as the declared type, never inferred first and cast afterwards, so
        ``007`` under a string type stays ``"007"``. Without it the types are
        inferred, nullability from the rows.

        Raises:
            ValueError: If a cell is not a value of its declared type, or a declared
                type cannot be read from text; the note names the file and column.
        """
        import pyarrow.csv as pa_csv

        if self._schema is None:
            table: pa.Table = pa_csv.read_csv(self._access_path)
            return table.cast(arrow_utils.infer_schema_nullable(table))
        try:
            return _read_declared(self._access_path, self._schema)
        except ValueError as exc:  # pyarrow's ArrowInvalid is a ValueError
            # The reader names a column by its position in the file, which is the
            # schema's field order (a schema names every column, in order).
            match = re.search(r"CSV column #(\d+)", str(exc))
            position = int(match.group(1)) if match else None
            if position is not None and position < len(self._schema):
                exc.add_note(f"column {self._schema.names[position]!r}")
            exc.add_note(f"CSV file {self._file_path!r} read with its declared schema")
            raise

    def _build(self, table: pa.Table) -> SourceStreamResult:
        """Enrich ``table`` as this source's stream (source info, system tags)."""
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        return builder.build(
            table,
            tag_columns=self._tag_columns,
            data_tags=source_data_tags(self._tag_columns, self._tag_only_columns),
            source_name=self._source_name,
            kind_part=self.kind_part(),
            record_id_columns=self._record_id_columns or (),
            system_tag_columns=self._system_tag_columns,
        )

    @override
    def check_for_new_data(self) -> bool:
        """Take in the file's rows if the file changed and its rows changed.

        Only the file's modification time and size are read when neither changed. A
        file written again with the same rows (in another order too, given a
        ``record_id_columns``) leaves the held rows and
        the modified time as they are. The rows are compared as read from the file,
        every column included (a ``system_tag_columns`` column too), in order unless a
        ``record_id_columns`` gives the record ids
        (``same_rows``).

        Returns:
            ``True`` if the source now holds new rows.

        Raises:
            ValueError: If the file's columns or their types changed: the source's
                schema is part of its identity; build a new source to read it.
        """
        state = _file_state(self._access_path)
        if state == self._file_state:
            return False
        table = self._read_table()
        if self._schema is None:
            # Nullability is inferred from the rows: a column that lost its nulls is
            # still the nullable column the source was built with.
            table = keep_nullable_columns(table, self._table.schema)
        if not table.schema.equals(self._table.schema, check_metadata=True):
            raise ValueError(
                f"CSV file {self._file_path!r} changed its columns: the source was "
                f"built with {self._table.schema}, the file now has {table.schema}. "
                "Build a new CSVSource to read it"
            )
        # Row positions are record ids without a record_id_column: order counts.
        if same_rows(self._table, table, ordered=not self._record_id_columns):
            self._file_state = state
            return False
        stream = self._build(table).stream
        self._file_state = state
        self._stream = stream
        self._table = table
        self._update_modified_time()
        return True

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize this source's configuration to a JSON-compatible dict.

        A source built with ``schema=`` saves it under ``"schema"`` (ITL-1315): every
        field's Arrow type and nullability, as given, so ``from_config`` reads the
        file with the same types and the reloaded source has the same schema and
        hashes. A source built without one saves no ``"schema"`` entry.

        Raises:
            ValueError: If the explicit schema cannot be saved exactly (field or
                schema metadata, a type the saved type text cannot restore).
        """
        schema_config: dict[str, Any] = {}
        if self._schema is not None:
            from orcapod.pipeline.serialization import serialize_arrow_schema

            try:
                schema_config["schema"] = serialize_arrow_schema(
                    self._schema, self.data_context.type_converter
                )
            except ValueError as exc:
                exc.add_note(f"CSVSource over {self._file_path!r}")
                raise
        return {
            "source_type": self.SOURCE_KIND,
            "file_path": self._file_path,
            "tag_columns": list(self._tag_columns),
            **self._tag_only_config(),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_columns": None
            if self._record_id_columns is None
            else list(self._record_id_columns),
            **self._source_id_config(),
            **schema_config,
            **self._identity_config(),
        }

    @classmethod
    def saved_schema(cls, config: dict[str, Any]) -> pa.Schema | None:
        """Return the explicit schema a saved config holds, or ``None`` if it holds none.

        Only an absent ``"schema"`` key means no explicit schema (a source built
        without ``schema=``, or a config written before ITL-1315). A present entry
        must be what ``to_config`` writes; anything else (``null``, a string where a
        list belongs, an unknown key) is refused, never interpreted.

        Args:
            config: A saved ``CSVSource`` config.

        Returns:
            The schema, or ``None``.

        Raises:
            ValueError: If the ``"schema"`` entry is malformed, naming what is wrong
                and the file the config reads.
        """
        if "schema" not in config:
            return None
        from orcapod.pipeline.serialization import deserialize_arrow_schema

        try:
            return deserialize_arrow_schema(config["schema"])
        except ValueError as exc:
            exc.add_note(f"saved CSVSource config over {config.get('file_path')!r}")
            raise

    @classmethod
    def check_saved_config(cls, config: dict[str, Any]) -> None:
        """Refuse a saved config whose ``"schema"`` entry is malformed (``saved_schema``).

        Raises:
            ValueError: If the ``"schema"`` entry is malformed.
        """
        cls.saved_schema(config)

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> CSVSource:
        """Reconstruct a CSVSource from a config dict.

        The saved ``"schema"`` entry, when there is one, is given to the constructor
        as ``schema=``; a config without it (a source built without ``schema=``, or a
        config written before ITL-1315) infers the types from the file.

        Raises:
            ValueError: If a ``"schema"`` entry is present but malformed.
        """
        return cls._from_saved_name(
            config,
            lambda name: cls(
                file_path=config["file_path"],
                tag_columns=config.get("tag_columns", ()),
                tag_only_columns=saved_tag_only_columns(config),
                system_tag_columns=config.get("system_tag_columns", ()),
                record_id_columns=config.get("record_id_columns"),
                source_id=name,
                schema=cls.saved_schema(config),
            ),
        )
