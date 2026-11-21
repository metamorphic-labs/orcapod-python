"""The one place Orcapod calls starfix, and which float options each use gets.

Orcapod hashes with starfix 0.6.0 or newer (developer, November 19, 2026; ITL-1179,
ITL-1251). starfix's ``normalize_negative_zero`` and ``normalize_nan`` options are
explicitly ON for the Arrow tables Orcapod itself builds (streams, rows, tag and data
datagrams, record-id and entry-id preimages, operator and cached-source record hashes,
the tables behind default source ids): there ``-0.0`` hashes as ``0.0`` and every NaN
(any sign or payload, quiet or signalling) as the ordinary NaN (``0x7e00``,
``0x7fc00000``, ``0x7ff8000000000000``), in ``float16``, ``float32`` and ``float64``
leaves at every depth, as Polars matching treats them. They are explicitly OFF for a
library value's digest (a pandas or Polars value, a pyarrow table given as a value):
library values keep the strict rule, every bit counts. Stored values are never
rewritten in either case.

starfix's own default for both options is off, and starfix does not record them in the
digest, so a call that left them out would change some digests with nothing to tell it
apart. So every starfix call goes through one of the three functions below, each of which
passes both options on every call, and no other module imports starfix
(``tests/test_hashing/test_starfix_calls.py`` checks both directions).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pyarrow as pa
from starfix import ArrowDigester


@dataclass(frozen=True)
class FloatOptions:
    """starfix's two float options, as passed on every call.

    Attributes:
        normalize_negative_zero: Hash ``-0.0`` as ``0.0``.
        normalize_nan: Hash every NaN as the ordinary NaN.
    """

    normalize_negative_zero: bool
    normalize_nan: bool


ORCAPOD_TABLE_FLOAT_OPTIONS: Final = FloatOptions(
    normalize_negative_zero=True, normalize_nan=True
)
"""The options of every Arrow table Orcapod builds: Orcapod's float rule, explicitly on."""

LIBRARY_VALUE_FLOAT_OPTIONS: Final = FloatOptions(
    normalize_negative_zero=False, normalize_nan=False
)
"""The options of a library value's digest: explicitly off, every bit counts."""


def _digest_table(
    table: pa.Table,
    options: FloatOptions,
    *,
    include_metadata: bool,
    ordered_columns: bool,
    ordered_struct_fields: bool,
) -> bytes:
    return ArrowDigester.hash_table(
        table,
        include_metadata=include_metadata,
        ordered_columns=ordered_columns,
        ordered_struct_fields=ordered_struct_fields,
        normalize_negative_zero=options.normalize_negative_zero,
        normalize_nan=options.normalize_nan,
    )


def digest_orcapod_table(
    table: pa.Table,
    *,
    include_metadata: bool,
    ordered_columns: bool,
    ordered_struct_fields: bool,
) -> bytes:
    """Return starfix's digest of an Arrow table Orcapod built, under the float rule.

    Args:
        table: The table, as Orcapod prepared it for hashing.
        include_metadata: Whether field and schema metadata are part of the digest.
        ordered_columns: Whether column order is part of the digest.
        ordered_struct_fields: Whether struct field order, at every depth, is part of the
            digest.

    Returns:
        The 35-byte starfix digest (format number included).

    Raises:
        ValueError: For an input starfix does not hash; the message is starfix's.
    """
    return _digest_table(
        table,
        ORCAPOD_TABLE_FLOAT_OPTIONS,
        include_metadata=include_metadata,
        ordered_columns=ordered_columns,
        ordered_struct_fields=ordered_struct_fields,
    )


def digest_library_value(
    table: pa.Table,
    *,
    include_metadata: bool,
    ordered_columns: bool,
    ordered_struct_fields: bool,
) -> bytes:
    """Return starfix's strict digest of a library value's Arrow form (every bit counts).

    Args:
        table: The value's Arrow form, as the value's hash prepared it.
        include_metadata: Whether field and schema metadata are part of the digest.
        ordered_columns: Whether column order is part of the digest.
        ordered_struct_fields: Whether struct field order, at every depth, is part of the
            digest.

    Returns:
        The 35-byte starfix digest (format number included).

    Raises:
        ValueError: For an input starfix does not hash; the message is starfix's.
    """
    return _digest_table(
        table,
        LIBRARY_VALUE_FLOAT_OPTIONS,
        include_metadata=include_metadata,
        ordered_columns=ordered_columns,
        ordered_struct_fields=ordered_struct_fields,
    )


def digest_schema(schema: pa.Schema, *, include_metadata: bool) -> bytes:
    """Return starfix's digest of a schema.

    A schema holds no values, so the float options do not change its digest; the options
    of Orcapod's tables are passed anyway, so that no starfix call is made without them.

    Args:
        schema: The schema, as Orcapod prepared it for hashing.
        include_metadata: Whether field and schema metadata are part of the digest.

    Returns:
        The 35-byte starfix digest (format number included).

    Raises:
        ValueError: For a schema starfix does not hash; the message is starfix's.
    """
    return ArrowDigester.hash_schema(
        schema,
        include_metadata=include_metadata,
        normalize_negative_zero=ORCAPOD_TABLE_FLOAT_OPTIONS.normalize_negative_zero,
        normalize_nan=ORCAPOD_TABLE_FLOAT_OPTIONS.normalize_nan,
    )
