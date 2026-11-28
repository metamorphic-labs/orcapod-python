from __future__ import annotations

import dataclasses
from collections.abc import Collection, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, cast

import pyarrow as pa

from orcapod.core.data_tags import (
    stored_data_tags,
    stream_data_tags,
    without_tag_kind_marks,
)
from typing_extensions import override

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.provisional import warn_provisional_source
from orcapod.core.sources.reload import same_rows
from orcapod.core.sources.source_ids import derived_source_name
from orcapod.core.sources.stream_builder import (
    require_supported_nullable_types,
    SourceStreamBuilder,
    normalize_record_id_columns,
    row_identifiers,
)
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.view import (
    ViewStatus,
    StreamView,
)
from orcapod.core.streams.base import (
    check_head_size,
    schema_requires_io,
    typed_arrow_schemas,
    typed_zero_row_table,
)
from orcapod.errors import InputValidationError, RowIdentifierError
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.provisional import provisional_class_name

if TYPE_CHECKING:
    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.core.nodes.operator_node import OperatorJobNode


DERIVED_ROW_FOLD_LABEL = "derived_row_v1"
"""The fold of a derived row's upstream source and record pairs by column name."""


def _most_recent_rows(
    lineages: Sequence[tuple[str, ...]],
    recorded_at: Sequence[datetime],
    result_ids: Sequence[bytes] | None,
) -> list[int]:
    """Select the latest stored row per lineage, keeping unresolved ties for validation.

    Function rows use their result UUID bytes as the second sort key. Operator
    rows have a recording time but no result UUID.
    """
    winners: dict[tuple[str, ...], int] = {}
    ties: dict[tuple[str, ...], int] = {}

    def rank(index: int) -> tuple[datetime, bytes]:
        return recorded_at[index], b"" if result_ids is None else result_ids[index]

    for index, lineage in enumerate(lineages):
        prior = winners.get(lineage)
        if prior is None:
            winners[lineage] = index
            continue
        current_rank, prior_rank = rank(index), rank(prior)
        if current_rank > prior_rank:
            winners[lineage] = index
            ties.pop(lineage, None)
        elif current_rank == prior_rank:
            ties[lineage] = index
    return [*winners.values(), *ties.values()]


def validate_tags_as_data(
    tags_as_data: Mapping[str, str],
    tag_keys: tuple[str, ...],
    data_keys: tuple[str, ...],
) -> None:
    """Check a ``tags_as_data`` mapping against the columns it copies from.

    The one rule for ``DerivedSource`` and ``SpiralDBTableSource``.

    Args:
        tags_as_data: Maps a tag column to the name of its data copy.
        tag_keys: The user tag columns of the stream copied from.
        data_keys: The data columns of the stream copied from.

    Raises:
        InputValidationError: If a key is not a tag column, or a new
            name is already a column, is used twice, or starts with the prefix
            reserved for system columns.
    """
    seen: set[str] = set()
    for tag, name in tags_as_data.items():
        if tag not in tag_keys:
            kind = "a data column" if tag in data_keys else "not a column"
            raise InputValidationError(
                f"tags_as_data: {tag!r} is {kind}, not a tag column. "
                f"Tag columns: {list(tag_keys)}."
            )
        if name in tag_keys or name in data_keys:
            raise InputValidationError(
                f"tags_as_data: cannot copy tag {tag!r} to {name!r}, which is "
                "already a column. Choose a new name."
            )
        if name in seen:
            raise InputValidationError(
                f"tags_as_data: the name {name!r} is given to more than one tag. "
                "Each copy needs its own name."
            )
        if name.startswith(constants.DATAGRAM_PREFIX):
            raise InputValidationError(
                f"tags_as_data: cannot copy tag {tag!r} to {name!r}: names starting "
                f"with {constants.DATAGRAM_PREFIX!r} are reserved for system columns."
            )
        seen.add(name)


def _field_text(field: "pa.Field[Any]") -> str:
    """The Arrow type of ``field`` with its top-level nullability, for a message."""
    return f"{field.type}{'' if field.nullable else ' not null'}"


class DerivedSource(RootSource):
    """A source whose rows are derived from another source or from a job node.

    Provisional in data context v1, in both forms (ITL-1331, ``PROVISIONAL_SOURCES``):
    its source id and hashes are not frozen and may change in a later release (ITL-1317),
    and so are those of every step downstream of it. Building one, also through
    ``node.as_source()``, ``from_config`` or a loaded job, issues
    ``ProvisionalHashWarning`` once per process.

    There are two forms.

    **Plain** (``tags_as_data`` not given), created by ``FunctionNode.as_source()``
    or ``OperatorNode.as_source()``: a static stream backed by the computed records
    of a DB-backed stream node. It presents the node's own tag and data columns.
    For each input lineage it reads the most recent readable stored record.
    Function ties in recording time use the greater result UUIDv7. If operator
    rows still tie, the source refuses the duplicate lineage. Other selection
    rules are planned.

    **With ``tags_as_data``**: a new root source over any source or job node that
    exposes chosen tag columns of the origin *also* as data columns, under the
    given names. A function pod never sees tags, and an operator never puts new
    content into the data group; a source defines its own tag and data columns, so
    this is where a key (for example a session start time) can also become an
    input to computation. The tags are the origin's tags. The data columns are the
    origin's data columns followed by the copies, in the order of their new names
    (``{"t1": "z1", "t3": "x3"}`` appends ``x3`` before ``z1``), whatever the
    mapping's order (ITL-1271). Each copy has
    the tag column's exact Arrow type, nullability and field metadata. The copy
    equals the tag when the source is read; downstream it is plain data.

    Both forms stamp one new system-tag pair and a fresh source-info token
    (``<source id>::<record id>::<column>``) for every data column, including copies.
    By default the record id folds the upstream source/record pairs in column-name
    order. Every key policy requires complete upstream pairs that uniquely identify
    the selected rows. The origin's stored tables keep their own provenance.

    Identity (``tags_as_data`` form):
        - ``source_id`` is required: there is no default (developer, November 21,
          2026, ITL-1313). The default this form had, a hash of the derivation,
          was of questionable stability (``record_id_columns=None`` and ``()``,
          one key policy, gave two ids).
        - ``content_hash`` follows the rule of every root source (``source_id``,
          ``derived_v1:<name>``, output schema and key columns; no class name,
          ITL-1425) plus the sorted ``tags_as_data`` items,
          since two mappings can give one schema and one explicit ``source_id``.
          Explicit ``record_id_columns=()`` appends the marker
          ``("record_id_policy", "position")`` to distinguish positional IDs from
          the default lineage fold.
        - ``pipeline_hash`` is schema-only, as for every root source.

    Identity (plain form):
        - ``content_hash`` follows the root source rule: the full source id,
          the Arrow schema identity and the empty key tuple. The default source
          id includes the job path and the origin's content hash (ITL-1564).
        - ``pipeline_hash`` is schema-only, so plain derived sources with
          identical schemas share the same pipeline DB table.

    If the origin is a job node that has not run yet, the source presents zero
    rows with the correct columns. Once records are available, the source reads them.

    Data-tags (ITL-1019): the source carries the bindings of what it reads,
    in both forms. Over a job node they are the ones the node's stored table states
    (the marker of ``data_tags``), which must be the node's own (a name in
    both halves of its keys); a table that disagrees is refused. Over a source they
    are the source's own. A ``tags_as_data`` copy of a data-tag is a new,
    plain data column. Both forms hash the output schema, in which a data-tag
    appears in both the tag and data halves.

    Args:
        origin: The source or DB-backed job node to derive from.
        source_id: The source's name; its id is ``derived_v1:<name>`` (ITL-1425).
            Required with ``tags_as_data`` or when the origin has no usable job
            name. The plain form defaults to
            ``derived_v1:<job path>:<algorithm label>:<hex>``, using the origin's
            job name and content hash.
        tags_as_data: Maps a tag column of the origin to the name of the data
            column that receives a copy of it.
        record_id_columns: With ``tags_as_data``, name key columns or pass ``()``
            to use the row's position. The default uses the upstream lineage fold.
        **kwargs: ``label``, ``data_context`` (defaults to the origin's) and
            ``config``, passed to ``RootSource``.

    Raises:
        InputValidationError: If ``tags_as_data`` names a column that is not a tag
            of the origin, or a new name that is already a column, is repeated,
            or is reserved.
        ValueError: If neither an explicit name nor a default is available, or
            ``record_id_columns`` is used without ``tags_as_data``.
        TypeError: If the origin is a source and ``tags_as_data`` is not given.

    Example::

        experiment = DerivedSource(
            SpiralDBTableSource(project, "experiment"),
            tags_as_data={"session_start_datetime": "session_start"},
            source_id="experiment_with_start",
        )
    """

    # The config type key (names the class a saved config is rebuilt with) and
    # the kind in the source id's kind part, for both forms.
    SOURCE_KIND: ClassVar[str] = "derived"
    # The version of this kind's implementation, in the kind part (ITL-1425).
    SOURCE_VERSION: ClassVar[int] = 1
    TABLE_IS_ROWS: ClassVar[bool] = True
    PROVISIONAL = True
    """Provisional in data context v1, both forms (ITL-1331, ``PROVISIONAL_SOURCES``)."""

    REPORTS_WHAT_IT_READS = False
    """It reads the rows stored for its origin (``explain()``, ITL-1402)."""

    @staticmethod
    def _missing_source_id() -> ValueError:
        """The error for a ``tags_as_data`` source built or loaded without a ``source_id``."""
        return ValueError(
            "DerivedSource with tags_as_data needs a source_id: it has no default, "
            "because a default built from a hash of the derivation was of "
            "questionable stability (ITL-1313). Pass source_id='<a name for this "
            "source>'."
        )

    @classmethod
    def require_saved_source_id(cls, config: Mapping[str, Any]) -> None:
        """Refuse a saved ``tags_as_data`` config without a saved name (ITL-1313).

        Checks the config and every source config nested in it (under the keys of
        ``serialization.NESTED_SOURCE_CONFIG_KEYS``: a derived source's ``origin``, a
        cached source's ``inner_source``), without rebuilding anything. A missing id is
        an invalid config, not an unavailable source, so ``PipelineJob.load`` calls this
        before it rebuilds the sources best-effort, and ``from_config`` before it
        rebuilds the origin.

        Args:
            config: A source config, as ``to_config`` writes it.

        Raises:
            ValueError: If the config, or a config nested in it, is a derived source
                with ``tags_as_data`` but no saved name (``source_name`` absent or
                null), or one saved before ITL-1425 (a ``source_id`` without a
                ``source_name``, refused by ``require_saved_name_fields`` with its
                own message).
        """
        from orcapod.pipeline.serialization import NESTED_SOURCE_CONFIG_KEYS

        if (
            config.get("source_type") == cls.SOURCE_KIND
            and config.get("tags_as_data") is not None
            and config.get("source_name") is None
        ):
            # A config saved before ITL-1425 holds a source_id: say so, not that one
            # is missing.
            cls.require_saved_name_fields(dict(config))
            raise cls._missing_source_id()
        for key in NESTED_SOURCE_CONFIG_KEYS:
            nested = config.get(key)
            if isinstance(nested, Mapping):
                cls.require_saved_source_id(nested)

    def __init__(
        self,
        origin: "FunctionJobNode | OperatorJobNode | RootSource",
        source_id: str | None = None,
        tags_as_data: Mapping[str, str] | None = None,
        record_id_columns: str | Collection[str] | None = None,
        **kwargs: Any,
    ) -> None:
        key_columns = normalize_record_id_columns(record_id_columns)
        if tags_as_data is None:
            if isinstance(origin, RootSource):
                raise TypeError(
                    "A DerivedSource over a source needs tags_as_data: the plain "
                    "form reads the stored records of a job node."
                )
            if key_columns is not None:
                raise ValueError(
                    "record_id_columns is only used together with tags_as_data."
                )
            self._tags_as_data: dict[str, str] | None = None
        else:
            # No default source id (ITL-1313): refused before the origin is read.
            if source_id is None:
                raise self._missing_source_id()
            # The mapping names a set of copies: they are placed in the order of their
            # new names, so the order it is given in changes nothing (ITL-1271).
            self._tags_as_data = dict(
                sorted(tags_as_data.items(), key=lambda item: item[1])
            )
            tag_keys, data_keys = origin.keys()
            validate_tags_as_data(self._tags_as_data, tag_keys, data_keys)
            kwargs.setdefault("data_context", origin.data_context_key)

        self._explicit_source_id = source_id
        super().__init__(source_id=source_id, **kwargs)
        if source_id is None and not isinstance(origin, RootSource):
            self._source_name = derived_source_name(
                origin.job_name, origin.content_hash()
            )
        self._origin = origin
        self._record_id_columns = key_columns
        self._key_columns = key_columns or ()
        self._cached_stream: ArrowTableStream | None = None
        # The source's columns without reading the origin.
        self._zero_row_stream: ArrowTableStream | None = None
        # The origin job node's modified time when its rows were cached: a check reads
        # its stored rows again only after it ran again (ITL-1102).
        self._origin_seen: datetime | None = None

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Return what the content hash is built from.

        Both forms start with the root source rule: the full source id, Arrow
        schema identity and key columns. The ``tags_as_data`` form appends the
        sorted mapping items so two mappings with the same schema and source id
        can still name different copies (ITL-1000). Explicit positional ids add
        a marker to distinguish them from the default fold.

        Returns:
            The identity structure.
        """
        identity = super().identity_structure()
        if self._tags_as_data is None:
            return identity
        identity = (
            *identity,
            ("tags_as_data", tuple(sorted(self._tags_as_data.items()))),
        )
        if self._record_id_columns == ():
            return (*identity, ("record_id_policy", "position"))
        return identity

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize this source to a JSON-compatible dict.

        The origin's own config is embedded when the origin is a source, which
        makes the derived source reloadable whenever its origin is. The
        ``source_id`` is the one given (always, with ``tags_as_data``); the plain
        form's default is not written, since it is recomputed from the origin.

        Args:
            db_registry: Optional database registry, forwarded to the origin.

        Returns:
            The config dict, with ``source_type`` ``"derived"``.
        """
        config: dict[str, Any] = {
            "source_type": self.SOURCE_KIND,
            # A copy: a caller that edits the exported config never edits this source.
            "tags_as_data": None
            if self._tags_as_data is None
            else dict(self._tags_as_data),
            "record_id_columns": None
            if self._record_id_columns is None
            else list(self._record_id_columns),
            # The plain form's default name is not written: it is recomputed from
            # the origin. ``source_name_given`` says the same, as for every source.
            "source_name": self._explicit_source_id,
            "source_name_given": self._explicit_source_id is not None,
            "source_version": self.SOURCE_VERSION,
        }
        # Every concrete source has to_config; the abstract RootSource does not
        # declare it, so look it up as PipelineJob.save does.
        origin_to_config = getattr(self._origin, "to_config", None)
        if (
            self._tags_as_data is not None
            and isinstance(self._origin, RootSource)
            and origin_to_config is not None
        ):
            config["origin"] = origin_to_config(db_registry=db_registry)
        config.update(self._identity_config())
        return config

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry: Any = None
    ) -> "DerivedSource":
        """Rebuild a derived source by first rebuilding its origin.

        Args:
            config: Dict as produced by ``to_config()``.
            db_registry: Optional database registry, forwarded to the origin.

        Returns:
            A new ``DerivedSource`` over the rebuilt origin.

        Raises:
            NotImplementedError: If the config holds no origin: a derived source
                over a job node cannot be rebuilt from a config.
            ValueError: If the config has ``tags_as_data`` but no ``source_id``,
                before the origin is rebuilt.
        """
        from orcapod.pipeline.serialization import resolve_source_from_config

        # Reading a saved DerivedSource is a use of a provisional source, also when its
        # origin (a job node) cannot be rebuilt and the constructor is never reached: a
        # loaded job warns for it as for a rebuilt one (ITL-1331).
        provisional = provisional_class_name(cls)
        if provisional is not None:
            warn_provisional_source(provisional)
        cls.require_saved_source_id(config)

        if config.get("origin") is None:
            raise NotImplementedError(
                "A DerivedSource over a job node cannot be reconstructed from "
                "config: its rows live in the node's pipeline database."
            )
        origin = resolve_source_from_config(config["origin"], db_registry=db_registry)
        return cls(
            origin,
            source_id=cls.saved_source_name(config),
            tags_as_data=config["tags_as_data"],
            record_id_columns=config.get("record_id_columns"),
        )

    # -------------------------------------------------------------------------
    # Stream interface
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._columns_stream().output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._columns_stream().keys(columns=columns, all_info=all_info)

    def _columns_stream(self) -> ArrowTableStream:
        """Return the held stream or predict its columns without reading rows."""
        if self._cached_stream is not None:
            return self._cached_stream
        if self._zero_row_stream is None:
            self._zero_row_stream = self._derive_from(self._empty_origin_table())
        return self._zero_row_stream

    def _data_tags(self) -> tuple[str, ...]:
        """Return the data-tags this source presents: its origin's.

        A derived source carries the bindings of what it reads (gap audit Q7). Over a
        job node they are the ones its stored table states, which ``_origin_records``
        checks against the node's keys on every read; over a source, the source's own.
        """
        return stream_data_tags(self._origin)

    def _origin_records(self) -> "pa.Table | None":
        """Return the origin's rows, or ``None`` when a job node has none stored.

        A job node's stored rows state their data-tags with a marker on the
        column (ITL-1019). The derived source presents exactly those: the marker must
        name the tags the node's keys share between tag and data, and is then
        dropped, since in a stream the binding is the shared name.

        Raises:
            InputValidationError: If a job node's stored rows mark other data-tags than the node's keys declare, or carry an invalid marker.
        """
        from orcapod.core.nodes.operator_node import OperatorJobNode

        origin = self._origin
        if isinstance(origin, RootSource):
            return origin.as_table(columns={"system_tags": True})
        if isinstance(origin, OperatorJobNode):
            records = origin._stored_rows_for_source()  # pyright: ignore[reportPrivateUsage]  # internal stored read for derivation
        else:
            records = origin.get_all_records(all_info=True)
        if records is None:
            return None
        require_supported_nullable_types(records.schema)
        try:
            stored = stored_data_tags(records.schema)
        except ValueError as exc:
            raise InputValidationError(
                f"DerivedSource over {self._origin.label!r}: {exc}"
            ) from exc
        declared = self._data_tags()
        if set(stored) != set(declared):
            raise InputValidationError(
                f"DerivedSource over {self._origin.label!r}: the stored records mark "
                f"the data-tags {sorted(stored)}, but the node declares "
                f"{sorted(declared)}; nothing was read."
            )
        records = without_tag_kind_marks(records)
        self._require_origin_types(records)
        return records

    def _require_origin_types(self, records: "pa.Table") -> None:
        """Refuse stored rows whose Arrow types are not the origin's (ITL-640).

        This source's identity is built from the origin's Arrow types; a store that
        gave a column back as another type (a fixed-size list read back as a list)
        would present rows of another schema under that identity, so nothing is read.
        A column's top-level nullability is not compared: the stored rows say it.

        Raises:
            InputValidationError: Naming the column and both types.
        """
        # The column's own nullability is the stored rows' (a job saved without its
        # nullability record reads its columns as non-null); every type, and
        # nullability below the top level, must be the origin's.
        mismatch = schema_utils.stored_type_mismatch(
            self._empty_origin_table().schema,
            records.schema,
            self.data_context.arrow_hasher,
        )
        if mismatch is not None:
            field, stored = mismatch
            raise InputValidationError(
                f"DerivedSource over {self._origin.label!r}: the stored column "
                f"{field.name!r} reads back as {_field_text(stored)}, but the "
                f"origin's rows have {_field_text(field)}; nothing was read."
            )

    def _empty_origin_table(self) -> "pa.Table":
        """Return a zero-row table with the origin's tag and data columns.

        The columns have the Arrow types the origin's rows have (its typed zero-row
        table, ITL-912), so this source presents the same types and has the same
        identity before and after rows are stored (ITL-640). Only an origin that
        cannot type itself without rows gives its declared Python schema, converted.
        """
        tag_keys, data_keys = self._origin.keys()
        names = list(dict.fromkeys([*tag_keys, *data_keys]))
        typed = typed_zero_row_table(self._origin)
        if isinstance(typed, pa.Table):
            require_supported_nullable_types(typed.schema)
            return typed.select(names)
        # An origin that cannot type its zero-row table states its Arrow types (a
        # read-only loaded operator, from its saved text), or gives its declared
        # Python schema, converted (``typed_arrow_schemas``).
        tag_arrow, data_arrow = typed_arrow_schemas(self._origin)
        fields = {field.name: field for field in (*tag_arrow, *data_arrow)}
        schema = pa.schema([fields[name] for name in names])
        require_supported_nullable_types(schema)
        return schema.empty_table()

    def _compute_arrow_schemas(self) -> "tuple[pa.Schema, pa.Schema]":
        """Return the declared Arrow schemas, including any tag copies as data."""
        schema = self._empty_origin_table().schema
        tags, data = self._origin.keys()
        copies = [
            schema.field(tag).with_name(name)
            for tag, name in (self._tags_as_data or {}).items()
        ]
        return (
            pa.schema([schema.field(name) for name in tags]),
            pa.schema([*(schema.field(name) for name in data), *copies]),
        )

    def _read_lineage(self, table: pa.Table) -> list[tuple[str, ...]]:
        """Read the complete source and record pairs of each origin row."""
        if table.num_rows == 0:
            return []
        source_prefix = (
            constants.SYSTEM_TAG_SOURCE_ID_PREFIX + constants.BLOCK_SEPARATOR
        )
        record_prefix = (
            constants.SYSTEM_TAG_RECORD_ID_PREFIX + constants.BLOCK_SEPARATOR
        )
        source_names = {
            name.removeprefix(source_prefix)
            for name in table.column_names
            if name.startswith(source_prefix)
        }
        record_names = {
            name.removeprefix(record_prefix)
            for name in table.column_names
            if name.startswith(record_prefix)
        }
        if not source_names or source_names != record_names:
            raise RowIdentifierError(
                f"DerivedSource over {self._origin.label!r}: no complete system-tag "
                "pair or unmatched source-id and record-id columns"
            )
        columns = [
            (
                record_prefix + name,
                table[source_prefix + name].to_pylist(),
                table[record_prefix + name].to_pylist(),
            )
            for name in sorted(record_names)
        ]
        result: list[tuple[str, ...]] = []
        for i in range(table.num_rows):
            lineage: list[str] = []
            for column, source_ids, record_ids in columns:
                source_id, record_id = source_ids[i], record_ids[i]
                if not isinstance(source_id, str) or not isinstance(record_id, str):
                    raise RowIdentifierError(
                        f"DerivedSource over {self._origin.label!r}: null or non-text "
                        f"system-tag pair for {column!r} in origin row {i}"
                    )
                lineage.extend((column, source_id, record_id))
            result.append(tuple(lineage))
        return result

    def _select_origin_rows(
        self, table: pa.Table
    ) -> tuple[pa.Table, list[tuple[str, ...]]]:
        """Apply the most-recent policy to stored rows, then validate uniqueness."""
        from orcapod.core.nodes.function_node import PIPELINE_RECORDED_AT_COL

        node_origin = not isinstance(self._origin, RootSource)
        lineages = self._read_lineage(table)
        operator_ties = False
        if node_origin and len(set(lineages)) < len(lineages):
            recorded_at = cast(
                "list[datetime]", table[PIPELINE_RECORDED_AT_COL].to_pylist()
            )
            result_ids = (
                cast("list[bytes]", table[constants.DATA_RECORD_ID].to_pylist())
                if constants.DATA_RECORD_ID in table.column_names
                else None
            )
            operator_ties = result_ids is None
            indices = _most_recent_rows(lineages, recorded_at, result_ids)
            table = table.take(indices)
            lineages = [lineages[i] for i in indices]
        seen: set[tuple[str, ...]] = set()
        for lineage in lineages:
            if lineage in seen:
                detail = (
                    " An operator cache can hold tied rows when one batch "
                    "already repeated the same system tags."
                    if operator_ties
                    else ""
                )
                raise RowIdentifierError(
                    f"DerivedSource over {self._origin.label!r}: the origin's rows "
                    "are not uniquely identified by their system tags. "
                    f"Repeated pair {lineage[:3]!r} in lineage {lineage!r}.{detail}"
                )
            seen.add(lineage)
        return table, lineages

    def _build_derived_stream(self) -> ArrowTableStream:
        """Read the origin and enrich its selected rows without computing it."""
        table = self._origin_records()
        return self._derive_from(self._empty_origin_table() if table is None else table)

    def _derive_from(self, table: pa.Table) -> ArrowTableStream:
        """Give the selected rows this source's system tags and data tokens."""
        table, lineages = self._select_origin_rows(table)
        tags, data = self._origin.keys()
        table = table.select(list(dict.fromkeys((*tags, *data))))
        for tag, name in (self._tags_as_data or {}).items():
            table = table.append_column(
                table.schema.field(tag).with_name(name), table.column(tag)
            )
        if self._record_id_columns is None:
            identifiers = [
                arrow_utils.fold_system_tag_values(
                    lineage, label=DERIVED_ROW_FOLD_LABEL
                )
                for lineage in lineages
            ]
        else:
            identifiers = row_identifiers(table, self._record_id_columns)
        schema_hash = schema_utils.compute_source_schema_hash(
            *self.arrow_schemas, self.data_context, self.orcapod_config
        )
        return SourceStreamBuilder(
            self.data_context, self.orcapod_config
        ).attach_provenance(
            table,
            tag_columns=tags,
            system_tag_columns=(),
            schema_hash=schema_hash,
            source_id=self.source_id,
            row_identifiers=identifiers,
            data_tags=self._data_tags(),
        )

    def _get_stream(self) -> ArrowTableStream:
        """Return the enriched rows, retaining a snapshot from a nonempty read."""
        self._note_provisional_identity_use()
        if self._cached_stream is not None:
            return self._cached_stream
        seen = self._origin.last_modified
        stream = self._build_derived_stream()
        if stream.as_table().num_rows:
            self._cached_stream = stream
        self._origin_seen = seen
        return stream

    @override
    def check_for_new_data(self) -> bool:
        """Take in the origin's rows if they changed since this source read them.

        Developer, November 14, 2026 (ITL-1102). Over a source, the origin is checked
        first (``check_for_new_data``). Then the origin's rows are read again only when
        its modified time moved since this source last read them: a source origin that
        took in new data, by this check or by a check made through another source over
        it, or a job node that ran again. The rows read are compared with the rows
        held (order matters only when the source uses positional record ids). When
        they differ the source holds the new ones and moves its
        modified time. A read or comparison that raises leaves everything as it was, so
        the next check reads again. A source that holds nothing (it has not read its
        origin yet, or read none of its rows: a job node that had not run) reads the
        origin at its next read; when the origin's time moved, its own time moves too,
        so a node that read no rows from it runs again.

        Returns:
            ``True`` if the source's modified time moved: it holds new rows, or it holds
            none and its origin changed.
        """
        if isinstance(self._origin, RootSource):
            _ = self._origin.check_for_new_data()
        seen = self._origin.last_modified
        if seen == self._origin_seen:
            return False
        if self._cached_stream is None:
            self._origin_seen = seen
            self._update_modified_time()
            return True
        stream = self._build_derived_stream()
        columns = {"system_tags": True, "source": True}
        unchanged = same_rows(
            self._cached_stream.as_table(columns=columns),
            stream.as_table(columns=columns),
            ordered=self._record_id_columns == (),
        )
        self._origin_seen = seen
        if unchanged:
            return False
        self._cached_stream = stream if stream.as_table().num_rows else None
        self._update_modified_time()
        return True

    def _schema_requires_io(self, config: ColumnConfig) -> bool:
        """Whether reading the schema with ``config`` would poll: as for the origin."""
        return schema_requires_io(self._origin, config)

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows of the origin's records, without computing.

        The records are the ones this source already read. Otherwise, an origin source
        gives a preview, without polling or writing a cache, and an origin job node
        gives its stored rows, read now and not kept. An origin job node that has not
        run gives no rows. When the origin holds all its rows, validate them together
        before slicing so a duplicate lineage outside the preview still raises. A
        bounded origin read validates only the rows it read. With ``sort_by_tags`` (or
        ``all_info``), the rows are sorted by tag before slicing (see ``StreamBase.head``).
        """
        check_head_size(n)
        # A preview is a read of rows: it notes the provisional use (ITL-1331).
        self._note_provisional_identity_use()
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        origin_kind = "held" if isinstance(self._origin, RootSource) else "stored"
        if self._cached_stream is not None:
            return self._cached_stream.head(n, columns=config).for_stream(
                self, origin=origin_kind
            )
        if isinstance(self._origin, RootSource):
            origin_columns = {"system_tags": True}
            origin_head = self._origin.head(n, columns=origin_columns)
            if (
                origin_head.status is ViewStatus.ROWS
                and not origin_head.total_is_estimate
                and origin_head.total_rows is not None
                and origin_head.total_rows > origin_head.table.num_rows
            ):
                # Validate all held lineages even for a short, unsorted head. Keep
                # the origin's order for an explicitly positional record-id policy.
                origin_head = self._origin.head(
                    origin_head.total_rows, columns=origin_columns
                )
            if origin_head.status is not ViewStatus.ROWS:
                if origin_head.table.num_columns == 0:
                    # The origin's columns are not known without polling it.
                    return origin_head.for_stream(self)
                # No rows: this source's own columns (tag copies included), with the
                # origin's status and explanation.
                empty = self._derive_from(origin_head.table.slice(0, 0))
                return dataclasses.replace(
                    StreamView.without_rows(
                        self,
                        origin_head.status,
                        empty.as_table(columns=config),
                        tag_columns=empty.keys(columns=config)[0],
                        note=origin_head.note,
                    ),
                    total_rows=origin_head.total_rows,
                    total_is_estimate=origin_head.total_is_estimate,
                    description=origin_head.description,
                )
            stream = self._derive_from(origin_head.table)
            return dataclasses.replace(
                stream.head(n, columns=config).for_stream(self),
                total_rows=origin_head.total_rows,
                total_is_estimate=origin_head.total_is_estimate,
                origin=origin_head.origin,
                note=origin_head.note,
            )
        stream = self._build_derived_stream()
        if not stream.as_table().num_rows:
            return StreamView.without_rows(
                self,
                ViewStatus.NOTHING_STORED,
                stream.as_table(columns=config),
                tag_columns=stream.keys(columns=config)[0],
                note="nothing stored: its origin has not run",
            )
        return stream.head(n, columns=config).for_stream(self, origin="stored")

    def iter_data(self):
        return self._get_stream().iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._get_stream().as_table(columns=columns, all_info=all_info)
