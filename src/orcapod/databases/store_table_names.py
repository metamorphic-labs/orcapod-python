"""Path-based table names shared by connector stores."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence

SPIRAL_TABLE_NAME_LABEL = "spiral_table_name_v1"
"""The digest algorithm shared with SpiralDB. The name's grammar carries its version."""

STORE_TABLE_NAME_PREFIX = "o1_"


def store_table_path_json(path: Sequence[str]) -> bytes:
    """Encode the full store path as a compact UTF-8 JSON array.

    Args:
        path: The store's base path followed by its record path.

    Raises:
        ValueError: If the path is empty or contains a component that cannot be
            represented by a UTF-8 string.
    """
    components = list(path)
    if not components:
        raise ValueError("a store table path has at least one component")
    if not all(type(component) is str for component in components):
        raise ValueError(f"a store table path holds strings only: {components!r}")
    try:
        return json.dumps(
            components, ensure_ascii=False, separators=(",", ":")
        ).encode()
    except UnicodeEncodeError as exc:
        raise ValueError(f"a store table path must be valid UTF-8: {exc}") from exc


def store_table_digest(path: Sequence[str]) -> str:
    """Return the full, bare SHA-256 hex digest of the canonical JSON path.

    Args:
        path: The store's base path followed by its record path.
    """
    return hashlib.sha256(store_table_path_json(path)).hexdigest()


def table_name_hint(path: Sequence[str], *, hint_length: int) -> str:
    """Return a readable ASCII hint from the leading path characters.

    Args:
        path: Full path components or leading components when filtering by name.
        hint_length: The maximum number of characters to keep.
    """
    return "".join(
        char.lower() if char.isascii() and char.isalnum() else "_"
        for char in "/".join(path)[:hint_length]
    )


def store_table_name(
    path: Sequence[str], *, hint_length: int, digest_length: int
) -> str:
    """Return a table name using the backend's identifier budget.

    Args:
        path: The store's base path followed by its record path.
        hint_length: The maximum number of characters from the readable hint.
        digest_length: The number of hex digits to keep from the start of the digest.
    """
    digest = store_table_digest(path)[:digest_length]
    hint = table_name_hint(path, hint_length=hint_length)
    return f"{STORE_TABLE_NAME_PREFIX}{hint}_{digest}"
