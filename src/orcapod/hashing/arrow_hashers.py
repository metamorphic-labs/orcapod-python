from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from orcapod.hashing.starfix_digest import (
    digest_library_value,
    digest_orcapod_table,
    digest_schema,
)
from orcapod.hashing.schema_cleaner import (
    clean_schema_for_hashing,
    has_extension_metadata,
    has_extension_type,
)
from orcapod.errors import MissingOptionalDependencyError
from orcapod.logical_types.identifier import LogicalTypeIdentifierError
from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.logical_types.canonical_form import storage_form_schema
from orcapod.utils.arrow_utils import normalize_extension_columns
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol


ARROW_HASH_LABEL = "arrow_v1"
"""``ContentHash.method`` of an Arrow table hash (``hash_table``): the single definition.

It names the whole procedure of ``StarfixArrowHasher``: the replacement of
extension-typed cells by their value digests, then the starfix digest (hash format 2:
starfix 0.5 and later; Orcapod uses starfix 0.6, the Rust implementation, ITL-1179). A change that gives an existing table a different digest takes a new
label (``arrow_v2``). The number is not kept equal to the starfix hash format number.
The data context file names the hasher class and does not repeat the label.
"""

ARROW_STRICT_HASH_LABEL = "arrow_strict_v1"
"""``ContentHash.method`` of ``StarfixArrowHasher.hash_table_strict``: the single definition.

The hash of a pyarrow table or record batch given as a VALUE (not a table Orcapod built):
the procedure of ``hash_table`` without Orcapod's float rule, so every bit counts, as for
every library value (ITL-1179). One label, one procedure: it differs from ``arrow_v1``
only for ``-0.0`` and for NaNs other than the ordinary one.
"""

ARROW_ORDERED_HASH_LABEL = "arrow_ordered_v1"
"""``ContentHash.method`` of ``StarfixArrowHasher.hash_table_ordered``: the single definition.

The order-including table hash is a procedure of its own (column order and, by default,
struct field order are part of the digest; the options are an input of the digest), so it
has its own label: one label, one procedure (ITL-1154). It is fed, with this label, into the
value hashes built on it (pandas ``DataFrame`` and ``Series``). It is a library value's
digest, so Orcapod's float rule does not apply: every bit counts (ITL-1179).
"""


ARROW_SCHEMA_HASH_LABEL = "arrow_schema_v1"
"""``ContentHash.method`` of ``StarfixArrowHasher.hash_schema``: the single definition.

A schema digest is a procedure of its own, not a table digest: the declared schema with
every logical type in the storage form (``storage_form_schema``: its storage type with
its identifier as field metadata, at any depth), then the starfix schema digest, which
ignores column order and the large and non-large variants and tells apart every type,
width, unit, zone, nullability at every depth and logical type identifier (ITL-640).
Every schema that enters an identity is hashed by this procedure, so it has its own
label (ITL-1154).
"""


class StarfixArrowHasher:
    """Arrow table hasher backed by starfix (the Rust implementation), via ``starfix_digest``.

    Pipeline
    --------
    1. **Semantic pre-processing** — the ``SemanticHashingVisitor`` traverses
       every column. Extension-typed columns whose Python type has a registered
       semantic hasher are replaced with ``pa.large_binary()`` hash tokens
       (e.g. ``Path`` columns are replaced by their file-content hash).
       Extension-typed columns without a registered hasher pass through with
       their full extension metadata intact.
    2. **Starfix hashing** — ``ArrowDigester.hash_table`` produces a 35-byte
       digest: the starfix hash format number ``00 00 02`` (hash format 2,
       starfix 0.5 and later) followed by a SHA-256 digest. starfix 0.6 computes
       it in Rust, byte-for-byte as the pure-Python starfix-python 0.5.0 did. In ``hash_table`` the order of columns
       and of struct fields is not part of the hash (starfix's order options stay
       off); ``hash_table_ordered`` turns both options on. Row order and list
       element order are always part of it. Extension identity
       (``ARROW:extension:name`` and ``ARROW:extension:metadata``) is hashed
       through starfix's ``include_metadata`` whenever the table has an
       extension column, including pyarrow's built-in canonical extension types
       such as ``arrow.uuid``.
    3. **Floats.** ``hash_table`` hashes a table Orcapod built, under Orcapod's
       float rule (starfix's ``normalize_negative_zero`` and ``normalize_nan``,
       explicitly on): ``-0.0`` as ``0.0`` and every NaN as the ordinary NaN.
       ``hash_table_ordered`` and ``hash_table_strict`` hash a library value, with
       both options explicitly off: every bit counts. ``hashing/starfix_digest.py``
       is the one module that calls starfix and states which use gets which.

    starfix raises ``ValueError`` for inputs it cannot hash: data of ``null``,
    ``map``, interval and view types (whatever the row count); union and
    run-end-encoded types; two schema nodes sharing a path (duplicate column
    names, or a ``/`` in a name aliasing a struct child); and a non-nullable
    field holding a null. Those errors propagate unchanged; they name the
    column path and type.

    Parameters
    ----------
    type_converter:
        ``UniversalTypeConverter`` used to resolve extension types to Python
        types and convert storage values back to Python objects.
    semantic_hasher:
        ``SemanticHasherProtocol`` used to hash Python objects extracted
        from extension-typed columns.
    hasher_id:
        The label (``ContentHash.method``) of every table hash produced. Defaults to
        ``ARROW_HASH_LABEL``, the label of this algorithm.
    """

    def __init__(
        self,
        type_converter: "UniversalTypeConverter",
        semantic_hasher: "SemanticHasherProtocol",
        hasher_id: str = ARROW_HASH_LABEL,
    ) -> None:
        self._type_converter = type_converter
        self._semantic_hasher = semantic_hasher
        self._hasher_id = hasher_id

    @property
    def hasher_id(self) -> str:
        return self._hasher_id

    def _process_table_columns(self, table: "pa.Table | pa.RecordBatch") -> "pa.Table":
        """Replace semantic-typed columns with content-hash bytes; normalize extension columns.

        For columns whose Python type has a registered semantic handler (e.g. ``Path``),
        the extension-typed column is replaced by a ``pa.large_binary()`` column of
        content-hash tokens.  For all other extension-typed columns (visitor passthrough),
        the column is normalized to IPC storage representation via
        ``normalize_extension_columns`` — storage type for the data, extension identity
        in field metadata — so that ``ArrowDigester`` can hash them without encountering
        a live ``pa.ExtensionType``, which is unhashable.
        """
        new_columns: list[pa.Array | pa.ChunkedArray] = []
        new_fields: list[pa.Field] = []

        for i, field in enumerate(table.schema):
            # Short-circuit: columns that cannot contain semantic types skip
            # the costly Python round-trip. Extension types must pass through
            # so visit_extension can process them.
            if not (
                isinstance(field.type, pa.ExtensionType)
                or pa.types.is_struct(field.type)
                or pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_fixed_size_list(field.type)
                or pa.types.is_map(field.type)
            ):
                new_columns.append(table.column(i))
                new_fields.append(field)
                continue

            column_data = table.column(i).to_pylist()
            visitor = SemanticHashingVisitor(
                self._type_converter, self._semantic_hasher
            )

            try:
                # The hashed type is derived from the declared type alone, never
                # from a row: nested nullability, field metadata and list item
                # fields are kept, and a column hashes the same way whether it
                # holds data, nulls or no rows (ITL-942 I).
                # A value-hashed logical type keeps its identifier on the field, so
                # it is part of the hash also when the column holds no value
                # (ITL-967).
                new_field = visitor.hashed_field(field)
                processed_data = [
                    visitor.visit(field.type, value)[1] for value in column_data
                ]
                new_columns.append(pa.array(processed_data, type=new_field.type))
                new_fields.append(new_field)

            except MissingOptionalDependencyError:
                # A logical type whose library is not installed: raised as itself,
                # naming the type and the package (the schema hash looks nothing up
                # since ITL-640, so this is where such a column is first met).
                raise
            except LogicalTypeIdentifierError as exc:
                # An identifier this installation cannot read: raised as itself,
                # naming the column (met here first, as above).
                raise exc.for_column(field.name) from exc
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to process column '{field.name}': {exc}"
                ) from exc

        intermediate = pa.table(
            new_columns,
            schema=pa.schema(new_fields, metadata=table.schema.metadata),
        )
        # Normalize any remaining extension-typed columns to their IPC storage
        # representation (storage type + ARROW:extension:* field metadata).
        # This handles the visitor passthrough case — extension types with no
        # registered semantic handler — so that ArrowDigester never receives a
        # live pa.ExtensionType, which is unhashable and would crash starfix.
        return normalize_extension_columns(intermediate)

    def hash_schema(self, schema: "pa.Schema") -> ContentHash:
        """Hash an Arrow schema using the starfix canonical algorithm.

        This is the hash of every schema that enters an identity (ITL-640): a source's
        pipeline and content identity, the system-tag column names, the output schema
        in a function's ``uri``. The ``ContentHash`` returned is labelled
        ``ARROW_SCHEMA_HASH_LABEL``. Schema metadata and field metadata other than a
        logical type's ``ARROW:extension:*`` keys are not part of the hash.

        It hashes the DECLARED schema (developer, November 21, 2026, ITL-640, option
        (a)): every extension type, in whatever form it arrives (a live
        ``pa.ExtensionType``, the metadata or IPC form of a table read back from a
        store, a type this process has not registered), is brought to one form, its
        storage type with its identifier (``ARROW:extension:name`` and
        ``ARROW:extension:metadata``) as field metadata, recursively inside lists,
        large lists, fixed-size lists, structs and maps
        (``logical_types.canonical_form.storage_form_schema``), and then hashed. There
        is no value-handler rewrite and no registry lookup: the identifier pins how
        the type's values are hashed, so the storage type plus the identifier is
        enough. ``hash_table`` is a separate procedure (``arrow_v1``): there a
        value-hashed type's cells become their value hashes in a ``large_binary``
        column that keeps the identifier, so ``hash_schema(table.schema)`` is not the
        schema inside ``hash_table(table)``, and nothing may rely on it.
        """
        schema = storage_form_schema(schema)
        include_meta = has_extension_metadata(schema) or has_extension_type(schema)
        if include_meta:
            schema = clean_schema_for_hashing(schema)
        digest = digest_schema(schema, include_metadata=include_meta)
        return ContentHash(method=ARROW_SCHEMA_HASH_LABEL, digest=digest)

    def hash_table(self, table: "pa.Table | pa.RecordBatch") -> ContentHash:
        """Hash an Arrow table (or ``RecordBatch``) that Orcapod built, using starfix.

        Column order and struct field order are not part of the hash: this is the hash of
        Orcapod's own tables (streams, datagrams, rows, preimages), which ignore order on
        purpose. Floats are hashed under Orcapod's float rule: ``-0.0`` as ``0.0`` and
        every NaN as the ordinary NaN (``starfix_digest.digest_orcapod_table``). A
        library value inside a cell keeps its own strict value digest, which replaces
        the cell before the table is hashed.
        """
        return ContentHash(
            method=self._hasher_id,
            digest=self._digest(
                table,
                ordered_columns=False,
                ordered_struct_fields=False,
                orcapod_table=True,
            ),
        )

    def hash_table_strict(self, table: "pa.Table | pa.RecordBatch") -> ContentHash:
        """Hash a pyarrow table (or ``RecordBatch``) given as a value: every bit counts.

        The procedure of ``hash_table`` (the same preprocessing, column and struct field
        order not part of it) without Orcapod's float rule, as for every library value:
        ``-0.0`` and ``0.0``, and NaNs that differ in bits, hash apart. Labelled
        ``ARROW_STRICT_HASH_LABEL``.
        """
        return ContentHash(
            method=ARROW_STRICT_HASH_LABEL,
            digest=self._digest(
                table,
                ordered_columns=False,
                ordered_struct_fields=False,
                orcapod_table=False,
            ),
        )

    def hash_table_ordered(
        self, table: "pa.Table | pa.RecordBatch", *, struct_field_order: bool = True
    ) -> ContentHash:
        """Hash an Arrow table (or ``RecordBatch``) as a value, order included.

        The same as ``hash_table`` (the same semantic preprocessing, so a ``File`` column is
        hashed by its content), except that the order of the columns and, by default, of the
        fields of every struct at every depth is part of the hash (starfix's
        ``ordered_columns`` and ``ordered_struct_fields`` options). The options are an input
        of the digest, so the hashes of one table under different options never coincide,
        even for a one-column table. Value types whose own equality depends on order build
        their hash on this digest (pandas ``DataFrame`` and ``Series``), which they take with
        its label. The ``ContentHash`` returned here is labelled
        ``ARROW_ORDERED_HASH_LABEL``: it is another procedure than ``hash_table``. It is a
        library value's digest, so Orcapod's float rule does not apply: every bit counts.

        Args:
            table: The table or record batch to hash.
            struct_field_order: Whether the order of struct fields is part of the hash.
                ``False`` is for a value type configured to ignore struct field order;
                column order stays part of the hash.
        """
        return ContentHash(
            method=ARROW_ORDERED_HASH_LABEL,
            digest=self._digest(
                table,
                ordered_columns=True,
                ordered_struct_fields=struct_field_order,
                orcapod_table=False,
            ),
        )

    def _digest(
        self,
        table: "pa.Table | pa.RecordBatch",
        *,
        ordered_columns: bool,
        ordered_struct_fields: bool,
        orcapod_table: bool,
    ) -> bytes:
        """Preprocess ``table`` and return its starfix digest.

        Args:
            table: The table or record batch to hash.
            ordered_columns: Whether column order is part of the digest.
            ordered_struct_fields: Whether struct field order is part of the digest.
            orcapod_table: Whether Orcapod built the table (its floats are hashed under
                Orcapod's float rule) rather than it being a library value's Arrow form
                (every bit counts).

        Returns:
            The starfix digest (version prefix included).
        """
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])

        processed_table = self._process_table_columns(table)
        include_meta = has_extension_metadata(
            processed_table.schema
        ) or has_extension_type(processed_table.schema)
        if include_meta:
            clean_schema = clean_schema_for_hashing(processed_table.schema)
            clean_table = pa.Table.from_arrays(
                processed_table.columns, schema=clean_schema
            )
        else:
            clean_table = processed_table
        digest = digest_orcapod_table if orcapod_table else digest_library_value
        return digest(
            clean_table,
            include_metadata=include_meta,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
        )
