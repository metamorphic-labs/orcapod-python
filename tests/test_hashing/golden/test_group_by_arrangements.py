"""The golden ``GroupBy`` arrangements pin one set of values (ITL-1226).

``pipeline_group_by_arrangement`` in ``cases.py`` pins ``GroupBy`` over one input in
several arrangements of its tag and data columns and of ``by``. GroupBy does not depend on
the arrangement, so every arrangement's entries must be equal; an explicit
``member_order`` gives other values.
"""

from __future__ import annotations

from typing import Any

from tests.test_hashing.golden.cases import (
    GROUP_MEMBER_ORDER_ARRANGEMENTS,
    pipeline_group_by_arrangement,
)


def _by_arrangement(entries: dict[str, Any], kind: str) -> dict[str, dict[str, Any]]:
    """The entries of ``kind``, per arrangement, with the arrangement name removed."""
    out: dict[str, dict[str, Any]] = {}
    for name in GROUP_MEMBER_ORDER_ARRANGEMENTS:
        prefix = f"{kind}.{name}."
        out[name] = {
            key.removeprefix(prefix): value
            for key, value in entries.items()
            if key.startswith(prefix)
        }
    return out


def test_every_arrangement_pins_the_same_values() -> None:
    entries = pipeline_group_by_arrangement()
    for kind in ("default", "member_order_y", "grouped_again"):
        per_arrangement = _by_arrangement(entries, kind)
        first, *others = per_arrangement.values()
        assert first
        for other in others:
            assert other == first, kind


def test_an_explicit_member_order_pins_other_values() -> None:
    entries = pipeline_group_by_arrangement()
    default = _by_arrangement(entries, "default")["tags_first"]
    by_y = _by_arrangement(entries, "member_order_y")["tags_first"]
    assert default.keys() == by_y.keys()
    assert default["content_hash"] != by_y["content_hash"]
    data_hashes = [k for k in default if k.endswith(".data_hash")]
    assert data_hashes
    assert all(default[k] != by_y[k] for k in data_hashes)
