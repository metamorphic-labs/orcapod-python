"""Source ids: the one place that builds and parses them (ITL-957, ITL-1154, ITL-1425).

Every source id is ``<kind>_v<N>:<name>`` (developer, November 23, 2026, ITL-1425):

* the **kind part**, ``<kind>_v<N>``, names the kind of source that read the data (the
  class's ``SOURCE_KIND``, also its config type key) and the version of that kind's
  implementation (the class's ``SOURCE_VERSION``). Orcapod always adds it, to a given
  name as to a default. It never holds ``:``;
* the **name**, the user's given ``source_id`` used exactly as given, or the default:
  ``<label>:<hex>`` of a hash (``hash_source_name``) or where the data lives, a file path,
  a table name or a qualified table (``location_source_name``). A job node's
  derived source combines the job path and labelled node hash. A name may hold ``:``.

A source id is split at its first ``:`` (``parse_source_id``). It never holds ``::``, the
separator of the parts of a source-info token (``<source id>::<record id>::<column>``),
and never ends with ``:``. A record id never holds the source id.
"""

from __future__ import annotations

import re
import warnings
from typing import NamedTuple

from orcapod.errors import SourceNameWarning
from orcapod.system_constants import constants
from orcapod.types import ContentHash

_KIND_PATTERN = re.compile(r"[a-z][a-z0-9_]*")
_KIND_PART_PATTERN = re.compile(r"([a-z][a-z0-9_]*)_v([1-9][0-9]*)")

_registered_kinds: set[str] = set()
"""The ``SOURCE_KIND`` of every source class defined so far (``register_kind``)."""


class ParsedSourceId(NamedTuple):
    """A source id taken apart: ``<kind>_v<version>:<name>``."""

    kind: str
    version: int
    name: str


def register_kind(kind: str) -> None:
    """Record a source kind, so that a given name that looks like an id of it warns.

    Called once per source class that declares its own ``SOURCE_KIND``.

    Raises:
        ValueError: If the kind is not lower-case letters, digits and ``_``, starting
            with a letter.
    """
    if not _KIND_PATTERN.fullmatch(kind):
        raise ValueError(
            f"a source kind is lower-case letters, digits and '_', starting with a "
            f"letter; got {kind!r}"
        )
    _registered_kinds.add(kind)


def kind_part(kind: str, version: int) -> str:
    """Return ``<kind>_v<version>``, the part of a source id that names its reader.

    Raises:
        ValueError: If the kind is not a valid kind or the version is not a positive
            ``int``.
    """
    if not _KIND_PATTERN.fullmatch(kind):
        raise ValueError(
            f"a source kind is lower-case letters, digits and '_', starting with a "
            f"letter; got {kind!r}"
        )
    if type(version) is not int or version < 1:
        raise ValueError(f"a source version is a positive int, got {version!r}")
    return f"{kind}_v{version}"


def require_source_name(name: str) -> str:
    """Check the name part of a source id, and return it.

    A name is any non-empty text that does not hold ``::``, start with ``:`` or end
    with ``:``: each would blur the separator of a source-info token once the kind
    part is in front.

    Raises:
        ValueError: If the name breaks one of these rules.
    """
    if not name:
        raise ValueError(
            "the source name is empty: a source id is the source's name after its "
            "kind part, and an empty one would end the id with ':'; give the source "
            "a name, or leave source_id unset for the default"
        )
    separator = constants.BLOCK_SEPARATOR
    if separator in name or name.startswith(":") or name.endswith(":"):
        raise ValueError(
            f"the source name {name!r} contains {separator!r}, or starts or ends "
            "with ':', which would blur the separator of the parts of a source-info "
            f"token (<source id>{separator}<record id>{separator}<column>); give the "
            "source another name"
        )
    return name


def warn_if_name_looks_like_an_id(name: str) -> None:
    """Warn when a given name starts like the id of a registered kind.

    The name is accepted as it is, nested after the source's own kind part
    (``csv_v1:delta_table_v1:units``): the kind part is always the reader's, so the
    warning says that the name's own prefix is part of the name, not a kind part.
    """
    head, colon, _ = name.partition(":")
    if not colon:
        return
    match = _KIND_PART_PATTERN.fullmatch(head)
    if match is None or match.group(1) not in _registered_kinds:
        return
    warnings.warn(
        f"the source name {name!r} starts like a source id of kind "
        f"{match.group(1)!r}; it is used as the name, after this source's own kind "
        "part. Give the name alone (without a '<kind>_v<N>:' part): Orcapod adds the "
        "kind part of the source that reads the data.",
        SourceNameWarning,
        stacklevel=3,
    )


def build_source_id(kind: str, version: int, name: str) -> str:
    """Return the source id ``<kind>_v<version>:<name>``: the one place that builds it.

    Raises:
        ValueError: If the kind, the version or the name is not valid.
    """
    return f"{kind_part(kind, version)}:{require_source_name(name)}"


def parse_kind_part(text: str) -> tuple[str, int]:
    """Take a kind part ``<kind>_v<N>`` apart into its kind and version.

    For a caller handed a kind part already built (``SourceStreamBuilder.build``), which
    then builds the id with ``build_source_id``.

    Raises:
        ValueError: If ``text`` is not a kind part.
    """
    match = _KIND_PART_PATTERN.fullmatch(text)
    if match is None:
        raise ValueError(
            f"{text!r} is not the kind part of a source id: a kind part is "
            "'<kind>_v<N>', for example 'csv_v1'"
        )
    return match.group(1), int(match.group(2))


def parse_source_id(source_id: str) -> ParsedSourceId:
    """Take a source id apart at its first ``:``.

    Raises:
        ValueError: If the text before the first ``:`` is not a kind part
            (``<kind>_v<N>``) or the name is not valid.
    """
    head, colon, name = source_id.partition(":")
    match = _KIND_PART_PATTERN.fullmatch(head)
    if not colon or match is None:
        raise ValueError(
            f"{source_id!r} is not a source id: a source id is "
            "'<kind>_v<N>:<name>', for example 'csv_v1:units'"
        )
    return ParsedSourceId(
        match.group(1), int(match.group(2)), require_source_name(name)
    )


def hash_source_name(content_hash: ContentHash, hexdigits: int | None = None) -> str:
    """Return ``<algorithm label>:<hex>``, a default name built from a hash.

    The label and hex are the hash in its labelled form (``ContentHash.to_string``); the
    source's kind part goes in front of it (``build_source_id``).

    Args:
        content_hash: The hash the name is built from.
        hexdigits: If given, keep only this many hex characters of the digest.

    Raises:
        ValueError: If the hash's label is empty or contains ``:``.
    """
    if not content_hash.method or ":" in content_hash.method:
        raise ValueError(
            f"hash label must be non-empty without ':', got {content_hash.method!r}"
        )
    return content_hash.to_string(hexdigits=hexdigits)


def location_source_name(location: str) -> str:
    """Return ``location``, a default name built from where the data lives.

    Args:
        location: The file path, table name or qualified table, as the source names it.

    Raises:
        ValueError: If the location is empty, holds ``::`` or starts or ends with ``:``:
            such a location gives no default name, so give the source a
            ``source_id``.
    """
    try:
        return require_source_name(location)
    except ValueError as exc:
        raise ValueError(
            f"the location {location!r} gives no default source name: it is empty, "
            "contains '::', or starts or ends with ':'; give the source a source_id"
        ) from exc


def joined_location(separator: str, *parts: str) -> str:
    """Join the parts of a location with ``separator``, refusing a part that holds it.

    A default name must name one location only: ``a.b`` + ``c`` and ``a`` + ``b.c``
    would both be ``a.b.c``. A part that contains the separator (or is empty) leaves
    the parts indistinguishable, so such a location has no default name (ITL-1154,
    ITL-1157).

    Args:
        separator: The text between the parts, for example ``.``.
        parts: The parts in order, for example a database, a schema and a table.

    Returns:
        The parts joined by ``separator``.

    Raises:
        ValueError: If a part is empty or contains ``separator``; give the source a
            ``source_id``.
    """
    for part in parts:
        if not part or separator in part:
            raise ValueError(
                f"{part!r} is empty or contains {separator!r}, so the default source "
                f"id cannot tell the parts of {separator.join(parts)!r} apart; give "
                "the source a source_id"
            )
    return separator.join(parts)


def derived_source_name(
    job_name: tuple[str, ...] | None, node_hash: ContentHash
) -> str:
    """Return the default name of a source over a job node's stored rows.

    Args:
        job_name: The job name components used to scope the stored rows.
        node_hash: The origin node's content hash.

    Raises:
        ValueError: If the job name is missing or a component is empty or contains
            a separator. Give the source a ``source_id`` in these cases.
    """
    if not job_name:
        raise ValueError(
            "the node has no job name for the default derived source id; "
            "give the source a source_id"
        )
    job_path = joined_location("/", *job_name)
    # A colon separates the job path from the labelled hash.
    if ":" in job_path:
        raise ValueError(
            f"the job path {job_path!r} contains ':', the separator of the "
            "default derived source id; give the source a source_id"
        )
    return location_source_name(f"{job_path}:{hash_source_name(node_hash)}")
