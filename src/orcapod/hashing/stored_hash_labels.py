"""Tell stored rows hashed under one Arrow hash label from another (ITL-910).

Orcapod keys stored rows by Arrow-derived hashes, and a change of the Arrow hash label
(``arrow_v0.1`` up to Orcapod v0.2, ``arrow_v0.2`` since v0.3) changes those values but not
the table they are stored in. A table can therefore hold rows of several labels side by
side. Orcapod writes and uses rows of its current label only; rows of another label stay
where they are, unchanged, until they are migrated.

Two stored forms carry the label:

- prefixed digests, ``b"<label>:<digest>"`` (for example ``b"arrow_v0.2:\\x00\\x00\\x02..."``),
  in function-node pipeline and result tables and side-effect invocation logs;
- hex digests without a label (``b"000002..."``), the record ids of operator-node and
  cached-source tables. Every starfix digest starts with its 3-byte hash format number, so
  the first six hex characters name the starfix hash format of the row. A later Arrow
  label that keeps the starfix format could not be told apart in these tables.

To list the labels a stored table holds::

    from orcapod.hashing.stored_hash_labels import stored_hash_labels

    stored_hash_labels(store, record_path)  # {"arrow_v0.1": 3, "arrow_v0.2": 3}
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol, cast

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

logger = logging.getLogger(__name__)

# ``arrow_v<version>:`` at the start of a prefixed digest.
_ARROW_LABEL_PATTERN = r"^arrow_v[0-9A-Za-z._-]*:"

# A full starfix digest in hex: 35 bytes, 70 lowercase hex characters.
_HEX_DIGEST_PATTERN = "^[0-9a-f]{70}$"

# The Arrow hash label that used each starfix hash format. Labels move more often than
# formats (any change to a table hash moves the label), so this maps only the formats
# whose label is known.
_LABEL_OF_FORMAT = {"000001": "arrow_v0.1", "000002": "arrow_v0.2"}

# Columns holding an Arrow-derived key, in the order they are looked for: the base entry
# id of function-node pipeline tables, the input data hash of function result tables and
# the record id hash of side-effect invocation logs. A table with none of them
# (operator-node and cached-source tables) keeps its hash as the record id.
_KEY_COLUMNS = ("__pipeline_base_entry_id", "__input_data_hash", "record_id_hash")

_RECORD_ID_COLUMN = "__stored_hash_labels_record_id"


def hash_labels_of(
    values: "pa.Array[Any] | pa.ChunkedArray[Any]",
) -> "pa.Array[Any]":
    """Return what each stored key says about the Arrow hasher that produced it.

    Args:
        values: A binary or string column of stored keys.

    Returns:
        A ``large_string`` array as long as ``values``: the Arrow hash label of a
        prefixed digest (``b"arrow_v0.2:..."`` gives ``"arrow_v0.2"``), the starfix hash
        format of a full hex digest (``b"000002..."``, 70 lowercase hex characters,
        gives ``"000002"``), and null for every other value (nulls, labels of other
        hashers, hand-made ids).
    """
    binary = values.cast(pa.large_binary())
    if isinstance(binary, pa.ChunkedArray):
        binary = binary.combine_chunks()
    if len(binary) == 0:
        return pa.array([], pa.large_string())
    # The pyarrow stubs type these kernels for Expression inputs only; arrays are
    # accepted at runtime.
    is_prefixed = pc.match_substring_regex(binary, _ARROW_LABEL_PATTERN)
    is_hex_digest = pc.match_substring_regex(binary, _HEX_DIGEST_PATTERN)
    label = pc.list_element(
        pc.split_pattern(binary, pattern=":", max_splits=1),  # pyright: ignore[reportCallIssue, reportArgumentType]
        0,
    )
    hash_format = pc.binary_slice(binary, 0, 6)
    picked = pc.if_else(
        is_prefixed,
        label,
        pc.if_else(is_hex_digest, hash_format, pa.scalar(None, pa.large_binary())),
    )
    return picked.cast(pa.large_string())


def keep_rows_hashed_under(table: "pa.Table", column: str, current: str) -> "pa.Table":
    """Drop the rows of ``table`` whose key was hashed under another Arrow hasher.

    Every read of a whole stored table goes through this function, so rows of another
    Arrow hash label are never returned as results.

    Args:
        table: Rows read from a stored table.
        column: The column of ``table`` holding the Arrow-derived key.
        current: What ``hash_labels_of`` gives for a key written by the current Arrow
            hasher: its label (``"arrow_v0.2"``) for prefixed digests, its starfix hash
            format (``current_hash_format``) for hex digests.

    Returns:
        ``table`` itself when no row is dropped, otherwise ``table`` without the rows
        whose key names another label or format. Rows whose key names none are kept.
    """
    labels = hash_labels_of(table.column(column))
    # The pyarrow stubs give comparison kernels a generic array type; the result is
    # a Boolean array.
    other = cast(
        "pa.BooleanArray",
        pc.not_equal(labels, pa.scalar(current, pa.large_string())).fill_null(False),
    )
    if not pc.any(other).as_py():
        return table
    # The single place where stored rows of another label are left out of a read.
    return table.filter(cast("pa.BooleanArray", pc.invert(other)))  # pyright: ignore[reportCallIssue, reportArgumentType]  # the stubs type invert for Expression inputs only


def report_skipped_rows(record_path: tuple[str, ...], skipped: int) -> None:
    """Warn that a read left out stored rows written under another hash label.

    Every reader that filters with ``keep_rows_hashed_under`` reports through this
    function, once per table, with the number of rows it left out.

    Args:
        record_path: Path of the stored table from the root of the store.
        skipped: Number of rows of the table left out of the read.
    """
    logger.warning(
        "Skipped %d %s of stored table %r: written under another hash label than the "
        "current one. The rows are left in place unchanged and can be migrated later.",
        skipped,
        "row" if skipped == 1 else "rows",
        "/".join(record_path),
    )


def other_arrow_label(
    values: "pa.Array[Any] | pa.ChunkedArray[Any]", current: str
) -> str | None:
    """Return a label or format in ``values`` other than ``current``, if there is one.

    Args:
        values: A binary or string column of stored keys.
        current: What ``hash_labels_of`` gives for a key written by the current Arrow
            hasher.

    Returns:
        The smallest label or starfix hash format found that is not ``current``, or
        ``None`` when every key that names one names ``current``.
    """
    found = cast("list[str | None]", pc.unique(hash_labels_of(values)).to_pylist())
    others = sorted(label for label in found if label is not None and label != current)
    return others[0] if others else None


class _Digest(Protocol):
    @property
    def digest(self) -> bytes: ...


class _TableHasher(Protocol):
    """The part of ``ArrowHasherProtocol`` this module needs (no import cycle)."""

    def hash_table(self, table: "pa.Table") -> _Digest: ...


def current_hash_format(hasher: _TableHasher) -> str:
    """Return the starfix hash format ``hasher`` writes, as six hex characters.

    Args:
        hasher: The Arrow hasher whose digests are stored.

    Returns:
        The first three bytes of a digest it produces, in hex (``"000002"`` for starfix
        hash format 2).
    """
    probe = pa.table({"x": pa.array([0], pa.int64())})
    return hasher.hash_table(probe).digest[:3].hex()


class _RecordReader(Protocol):
    """The part of ``ArrowDatabaseProtocol`` this module needs (no import cycle)."""

    def get_all_records(
        self, record_path: tuple[str, ...], record_id_column: str | None = None
    ) -> "pa.Table | None": ...


def stored_hash_labels(
    database: _RecordReader, record_path: tuple[str, ...]
) -> dict[str | None, int]:
    """Count the rows of a stored table by the Arrow hash label they were hashed under.

    Reads the whole table. The label is taken from the table's Arrow-derived key: the
    base entry id of a function-node pipeline table, the input data hash of a function
    result table, the record id hash of a side-effect invocation log, and otherwise the
    record id (operator-node and cached-source tables, whose hex digests name only a
    starfix hash format).

    Args:
        database: The store, positioned so that ``record_path`` names the table.
        record_path: Path of the stored table.

    Returns:
        A mapping from label to number of rows, for example
        ``{"arrow_v0.1": 3, "arrow_v0.2": 3}``. A starfix hash format whose label is
        not known appears as ``"starfix hash format <hex>"``, rows whose key names no
        label appear under ``None``, and a missing or empty table gives ``{}``.
    """
    table = database.get_all_records(record_path, record_id_column=_RECORD_ID_COLUMN)
    if table is None or table.num_rows == 0:
        return {}
    column = next(
        (name for name in _KEY_COLUMNS if name in table.column_names),
        _RECORD_ID_COLUMN,
    )
    per_value = pc.value_counts(hash_labels_of(table.column(column)))
    found_values: list[str | None] = per_value.field(0).to_pylist()
    row_counts = cast("list[int]", per_value.field(1).to_pylist())
    counts: dict[str | None, int] = {}
    for found, rows in zip(found_values, row_counts):
        if found is not None and not found.startswith("arrow_v"):
            found = _LABEL_OF_FORMAT.get(found, f"starfix hash format {found}")
        counts[found] = counts.get(found, 0) + rows
    return counts
