"""Labelled per-type value hashes, and the order-dependent Arrow digest they build on.

A value type whose own equality depends on order (a pandas DataFrame's columns, a struct's fields)
hashes as::

    sha256(canonical_json(descriptor) + b"\\x00" + values_digest)

under a ``ContentHash`` method label ``<type>_v<N>`` (for example ``pandas_dataframe_v1``). The
descriptor carries what the Arrow values cannot express (label types, library dtypes); the
values digest is ``ordered_arrow_digest`` of the value's Arrow form. The label names the whole
definition and is its only version marker: the bytes produced under a label never change, and a
new definition gets a new label.

``ordered_arrow_digest`` is the one place Orcapod asks starfix for an order-dependent digest. It
is for values only. Orcapod streams, datagrams and rows ignore column order on purpose and keep
using the data context's Arrow hasher.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from starfix import ArrowDigester

from orcapod.types import ContentHash
from orcapod.utils.arrow_utils import normalize_extension_columns

if TYPE_CHECKING:
    import pyarrow as pa


def canonical_json(descriptor: Any) -> bytes:
    """Serialise a descriptor as canonical JSON.

    Keys are sorted, separators are ``","`` and ``":"``, and non-ASCII characters are kept as
    UTF-8, so equal descriptors always give equal bytes.

    Args:
        descriptor: A JSON-serialisable structure.

    Returns:
        The UTF-8 bytes of the canonical JSON text.
    """
    return json.dumps(
        descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def labelled_content_hash(
    method: str, descriptor: Any, values_digest: bytes
) -> ContentHash:
    """Combine a descriptor and a values digest into a labelled ``ContentHash``.

    Args:
        method: The versioned label of the definition, for example ``"pandas_dataframe_v1"``.
        descriptor: The JSON-serialisable descriptor of what the values digest does not see.
        values_digest: The digest of the value's contents.

    Returns:
        ``ContentHash(method, sha256(canonical_json(descriptor) + b"\\x00" + values_digest))``.
    """
    payload = canonical_json(descriptor) + b"\x00" + values_digest
    return ContentHash(method=method, digest=hashlib.sha256(payload).digest())


def ordered_arrow_digest(table: pa.Table) -> bytes:
    """Return starfix's digest of ``table`` with column order and struct field order included.

    Uses starfix's per-call options ``ordered_columns`` and ``ordered_struct_fields`` (hash
    format 2), so permuting the columns, or the fields of a struct at any depth, changes the
    digest, while chunking does not. Extension-typed columns (for example pyarrow's pandas
    ``Period`` type) are hashed by their storage; the caller's descriptor carries their type.

    Args:
        table: The Arrow form of a value.

    Returns:
        The starfix digest (format-2 version prefix included).

    Raises:
        ValueError: If starfix rejects the schema, for example two columns or two fields of
            one struct with the same name.
    """
    return ArrowDigester.hash_table(
        normalize_extension_columns(table),
        ordered_columns=True,
        ordered_struct_fields=True,
    )
