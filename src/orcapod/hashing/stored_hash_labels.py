"""Find stored rows that were hashed under another Arrow hash label (ITL-910).

Orcapod keys stored rows by Arrow-derived hashes. Two stored forms exist:

- prefixed digests, ``b"<label>:<digest>"`` (for example ``b"arrow_v0.2:\\x00\\x00\\x02..."``),
  in function-node pipeline and result tables and side-effect invocation logs;
- hex digests without a label (``b"000002..."``), in operator-node and cached-source
  tables. Every starfix digest starts with its 3-byte hash format number, so the first six
  hex characters name the starfix hash format of the row.

The functions here only report what they find; the caller, which knows the table and the
node, raises ``StoreHashLabelError``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc

    from orcapod.protocols.hashing_protocols import ArrowHasherProtocol
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

_ARROW_LABEL_PREFIX = "arrow_v"

# The Arrow hash label that used each starfix hash format. Labels move more often than
# formats (any change to a table hash moves the label), so this maps only the formats
# whose label is known.
_LABEL_OF_FORMAT = {"000001": "arrow_v0.1", "000002": "arrow_v0.2"}

_HEX_DIGEST_PATTERN = "^[0-9a-f]{70}$"


def foreign_arrow_label(
    values: "pa.Array | pa.ChunkedArray", current_label: str
) -> str | None:
    """Return an Arrow hash label other than ``current_label`` found in ``values``.

    Args:
        values: A binary or string column of prefixed digests ``b"<label>:<digest>"``.
            Nulls and values whose label is not an Arrow hash label (``arrow_v...``) are
            not considered.
        current_label: The Arrow hash label the current hasher writes.

    Returns:
        The smallest such label, or ``None`` when every Arrow-labelled value carries
        ``current_label``.
    """
    if len(values) == 0:
        return None
    binary = values.cast(pa.large_binary())
    labels = pc.list_element(pc.split_pattern(binary, pattern=b":", max_splits=1), 0)
    found = sorted(
        label.decode("utf-8", errors="replace")
        for label in pc.unique(labels).to_pylist()
        if label is not None
    )
    for label in found:
        if label.startswith(_ARROW_LABEL_PREFIX) and label != current_label:
            return label
    return None


def current_hash_format(hasher: "ArrowHasherProtocol") -> str:
    """Return the starfix hash format ``hasher`` writes, as six hex characters.

    Args:
        hasher: The Arrow hasher whose digests are stored.

    Returns:
        The first three bytes of a digest it produces, in hex (``"000002"`` for starfix
        hash format 2).
    """
    probe = pa.table({"x": pa.array([0], pa.int64())})
    return hasher.hash_table(probe).digest[:3].hex()


def foreign_hash_format(
    values: "pa.Array | pa.ChunkedArray", current_format: str
) -> str | None:
    """Return a description of a starfix hash format other than ``current_format``.

    Args:
        values: A binary or string column of hex digests without a label. Values
            that are not a full starfix digest in hex (70 lowercase hex characters)
            are not considered.
        current_format: The format the current hasher writes, as returned by
            ``current_hash_format``.

    Returns:
        The Arrow hash label that used the first other format found (for example
        ``"arrow_v0.1"``), or ``"starfix hash format <hex>"`` when that format's label is
        not known; ``None`` when every non-null value has ``current_format``.
    """
    if len(values) == 0:
        return None
    binary = values.cast(pa.large_binary())
    # Only full starfix digests count: 35 bytes, 70 lowercase hex characters.
    binary = pc.filter(binary, pc.match_substring_regex(binary, _HEX_DIGEST_PATTERN))
    formats = sorted(
        prefix.decode("ascii", errors="replace")
        for prefix in pc.unique(pc.binary_slice(binary, 0, 6)).to_pylist()
        if prefix is not None
    )
    for fmt in formats:
        if fmt != current_format:
            return _LABEL_OF_FORMAT.get(fmt, f"starfix hash format {fmt}")
    return None
