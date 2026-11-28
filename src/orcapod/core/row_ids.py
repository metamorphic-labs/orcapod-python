# pyright: reportImportCycles=false
# The existing cycle core_protocols -> side_effect_pod -> side_effects, which this module's
# type-only import of the protocols brings into pyright's view because side_effects imports
# it (a file-level diagnostic: no line-level ignore reaches it; as for core/data_tags.py).
# Breaking it is ITL-1206.
"""The three nested ids of a row that passes through a node (ITL-1466, ITL-1471).

Each id is the hash of a one-row Arrow table, its preimage, made by the data
context's Arrow hasher, and each is built from the one before:

- The **entry id** names an entry of the pipeline table: the row's user tags,
  its system tags and the hash of its data. Side effects and functions that take
  the invocation context are keyed by it, and the operator cache stores it.
- The **instance id** names one arrival of an entry, with its provenance: the
  entry id and the source-info tokens of the row's data. A function node looks a
  row up by it.
- The **record id** names one stored row of a function node's pipeline table:
  the content hash of the node that records it, the instance id and the id of
  the result the row points at. It is the table's key.

Every preimage is built here and nowhere else. The column names inside a
preimage are part of its recipe: changing one moves every id made from it.

A nested id enters the next preimage as its labelled digest
(``ContentHash.to_prefixed_digest()``): a hash fed into another hash carries the
name of the algorithm that produced it.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pyarrow as pa

from orcapod.system_constants import constants

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
    from orcapod.protocols.hashing_protocols import ArrowHasherProtocol
    from orcapod.types import ContentHash

# The entry id's column: in the instance id's preimage, and in the operator cache.
ENTRY_ID_COL = "__pipeline_entry_id"

# The instance id's column: the pipeline table's lookup column, and in the record
# id's preimage.
INSTANCE_ID_COL = "__pipeline_instance_id"

# The record id's column in the pipeline table.
RECORD_ID_COL = "__pipeline_record_id"

# What stands in a source-info token where the result id would be, when the token
# belongs to the placeholder of an ephemeral result that is gone (``EmptyData``).
# A result id is 32 hexadecimal characters, so this text names no row of any
# result table; it holds no ``::``, so the token still splits into its three parts.
EPHEMERAL_RESULT_GONE = "EPHEMERAL_RESULT_GONE"


def entry_id_preimage(tag: "TagProtocol", data: "DataProtocol") -> "pa.Table":
    """Build the preimage of a row's entry id.

    Args:
        tag: The row's tag.
        data: The row's data. Only its content hash is read, so a placeholder
            (``EmptyData``) is accepted.

    Returns:
        A one-row table: the user tag and system tag columns, then
        ``__input_data_hash`` (``large_binary``), the labelled digest of the
        data's content hash.
    """
    return entry_id_preimage_from_parts(
        tag.as_table(columns={"system_tags": True}), data.content_hash()
    )


def entry_id_preimage_from_parts(tags: pa.Table, data_hash: ContentHash) -> pa.Table:
    """Build the entry preimage from the row's tag table and data hash.

    Args:
        tags: The one-row tag table with its system tags.
        data_hash: The row's input data content hash.

    Returns:
        The tag table with the labelled data digest appended.
    """
    return tags.append_column(
        constants.INPUT_DATA_HASH_COL,
        pa.array([data_hash.to_prefixed_digest()], type=pa.large_binary()),
    )


def entry_id(
    hasher: "ArrowHasherProtocol", tag: "TagProtocol", data: "DataProtocol"
) -> "ContentHash":
    """Return a row's entry id: the hash of its tags, system tags and data hash.

    Args:
        hasher: The Arrow hasher of the row's data context.
        tag: The row's tag.
        data: The row's data.

    Returns:
        The entry id.
    """
    return hasher.hash_table(entry_id_preimage(tag, data))


def instance_id_preimage(entry: "ContentHash", data: "DataProtocol") -> "pa.Table":
    """Build the preimage of a row's instance id.

    Args:
        entry: The row's entry id.
        data: The row's data, whose source-info tokens are read.

    Returns:
        A one-row table: ``__pipeline_entry_id`` (``large_binary``, the labelled
        digest of the entry id), then the source-info columns of the data in name
        order. A placeholder that carries no token gives the entry id alone.
    """
    # A placeholder (``EmptyData``) gives the tokens it carries here, and no column
    # when it carries none.
    return instance_id_preimage_from_tokens(entry, data.source_info_table())


def instance_id_preimage_from_tokens(entry: ContentHash, tokens: pa.Table) -> pa.Table:
    """Build the instance preimage from an entry id and the row's token table.

    Args:
        entry: The row's entry id.
        tokens: The row's source-info table, in the form its datagram gives.

    Returns:
        The labelled entry digest followed by token columns in name order.
    """
    preimage = pa.table(
        {ENTRY_ID_COL: pa.array([entry.to_prefixed_digest()], type=pa.large_binary())}
    )
    for name in sorted(tokens.column_names):
        preimage = preimage.append_column(
            tokens.schema.field(name), tokens.column(name)
        )
    return preimage


def instance_id(
    hasher: "ArrowHasherProtocol", tag: "TagProtocol", data: "DataProtocol"
) -> "ContentHash":
    """Return a row's instance id: the hash of its entry id and its tokens.

    Args:
        hasher: The Arrow hasher of the row's data context.
        tag: The row's tag.
        data: The row's data.

    Returns:
        The instance id.
    """
    return hasher.hash_table(instance_id_preimage(entry_id(hasher, tag, data), data))


def record_id_preimage(
    node_content_hash: "ContentHash", instance: "ContentHash", result_id: uuid.UUID
) -> "pa.Table":
    """Build the preimage of a stored row's record id.

    Args:
        node_content_hash: The content hash of the node that records the row.
        instance: The row's instance id.
        result_id: The id of the result the row points at.

    Returns:
        A one-row table: ``__node_content_hash`` (``large_binary``, the labelled
        digest of the node's content hash), then ``__pipeline_instance_id``
        (``large_binary``, the labelled digest of the instance id), then
        ``__data_id`` (``large_binary``, the bytes of the result id). A pipeline
        row stores the three columns as they are here, so it gives its record id
        again from its own columns.
    """
    return pa.table(
        {
            constants.NODE_CONTENT_HASH_COL: pa.array(
                [node_content_hash.to_prefixed_digest()], type=pa.large_binary()
            ),
            INSTANCE_ID_COL: pa.array(
                [instance.to_prefixed_digest()], type=pa.large_binary()
            ),
            constants.DATA_RECORD_ID: pa.array(
                [result_id.bytes], type=pa.large_binary()
            ),
        }
    )


def record_id(
    hasher: "ArrowHasherProtocol",
    node_content_hash: "ContentHash",
    instance: "ContentHash",
    result_id: uuid.UUID,
) -> "ContentHash":
    """Return a stored row's record id.

    It is the hash of the recording node's content hash, the row's instance id
    and its result id. One pipeline table holds the rows of every node with one
    pipeline hash (the default table scope): two such nodes that record the same
    instance with the same stored result have two record ids, so each keeps its
    own row, and the record id alone is the table's key (developer, November 26,
    2026).

    Args:
        hasher: The Arrow hasher of the row's data context.
        node_content_hash: The content hash of the node that records the row.
        instance: The row's instance id.
        result_id: The id of the result the row points at.

    Returns:
        The record id.
    """
    return hasher.hash_table(record_id_preimage(node_content_hash, instance, result_id))
