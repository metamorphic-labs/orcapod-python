"""The forms of a default source id: the one place that builds them (ITL-957, ITL-1154).

A source id is the source's name. A source given a ``source_id`` uses it exactly as given.
With none, every source builds a default that names its kind first (the class's
``SOURCE_KIND``, which is also its config type key):

* ``<kind>:<label>:<hex>`` for a default built from a hash (``hash_source_id``);
* ``<kind>:<location>`` for a default built from where the data lives: a file path, a
  table name, a qualified table (``location_source_id``).

A default never contains ``::``, the separator of the parts of a source-info value.
"""

from __future__ import annotations

from orcapod.types import ContentHash


def hash_source_id(
    kind: str, content_hash: ContentHash, hexdigits: int | None = None
) -> str:
    """Return ``<kind>:<algorithm label>:<hex>``, a default source id built from a hash.

    A source id is the source's name. The kind says what sort of source it is, and
    the label and hex are the hash in its labelled form (``ContentHash.to_string``).
    The parts are joined by a single ``:`` and none contains one, so the id never
    contains the ``::`` that separates the parts of a source-info value.

    Args:
        kind: The source kind, for example ``dict`` or ``derived``.
        content_hash: The hash the id is built from.
        hexdigits: If given, keep only this many hex characters of the digest.

    Returns:
        The source id.

    Raises:
        ValueError: If the kind or the hash's label is empty or contains ``:``.
    """
    if not kind or ":" in kind:
        raise ValueError(f"source kind must be non-empty without ':', got {kind!r}")
    if not content_hash.method or ":" in content_hash.method:
        raise ValueError(
            f"hash label must be non-empty without ':', got {content_hash.method!r}"
        )
    return f"{kind}:{content_hash.to_string(hexdigits=hexdigits)}"


def location_source_id(kind: str, location: str) -> str:
    """Return ``<kind>:<location>``, a default source id built from where the data lives.

    Args:
        kind: The source kind, the class's ``SOURCE_KIND`` (for example ``csv``).
        location: The file path, table name or qualified table, as the source names it.

    Returns:
        The source id.

    Raises:
        ValueError: If the kind is empty or contains ``:``, or the location is empty or
            contains ``::``, the separator of a source-info value: such a location has
            no default id, so give the source a ``source_id``.
    """
    if not kind or ":" in kind:
        raise ValueError(f"source kind must be non-empty without ':', got {kind!r}")
    if not location:
        raise ValueError(
            f"a {kind} source has an empty location, so it has no default source id; "
            "give it a source_id"
        )
    if "::" in location:
        raise ValueError(
            f"the location {location!r} of a {kind} source contains '::', the separator "
            "of a source-info value, so it cannot be the default source id; give the "
            "source a source_id"
        )
    return f"{kind}:{location}"
