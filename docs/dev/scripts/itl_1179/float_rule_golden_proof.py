"""ITL-1179 stage 2: what the float rule changes in the golden values, and nothing else.

Computes every golden group twice in one process: with Orcapod's own-table float options
as shipped (on), and with them switched off (starfix's default, which is what the code on
main computes, since the routing of datagram and stream tables to ``hash_table`` gives the
same digests and labels as the semantic ``ArrowTableHandler`` did). Prints every value
that differs outside the ``arrow.float_rule`` case (which pins the rule itself, so its
-0.0 and NaN entries differ by design). Expected: the two approved entries of
``pipeline:group_by_nan_keys``.

Usage::

    uv run python docs/dev/scripts/itl_1179/float_rule_golden_proof.py
"""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
warnings.filterwarnings("ignore")

from orcapod.hashing import starfix_digest  # noqa: E402
from tests.test_hashing.golden import cases  # noqa: E402


def _flatten(value: Any, prefix: str) -> dict[str, str]:
    if isinstance(value, dict):
        out: dict[str, str] = {}
        for key, item in value.items():
            out.update(_flatten(item, f"{prefix}.{key}" if prefix else str(key)))
        return out
    return {prefix: json.dumps(value, sort_keys=True)}


def main() -> int:
    on = _flatten(cases.compute_all(), "")
    shipped = starfix_digest.ORCAPOD_TABLE_FLOAT_OPTIONS
    starfix_digest.ORCAPOD_TABLE_FLOAT_OPTIONS = starfix_digest.FloatOptions(
        normalize_negative_zero=False, normalize_nan=False
    )
    try:
        off = _flatten(cases.compute_all(), "")
    finally:
        starfix_digest.ORCAPOD_TABLE_FLOAT_OPTIONS = shipped
    assert set(on) == set(off)
    differ = sorted(k for k in on if on[k] != off[k])
    rule_pins = [k for k in differ if k.startswith("arrow.float_rule.")]
    differ = [k for k in differ if not k.startswith("arrow.float_rule.")]
    print(
        f"{len(on)} computed golden values; the arrow.float_rule case (pins of the rule "
        f"itself) differs in {len(rule_pins)}; outside it, {len(differ)} differ with the "
        "float rule on:"
    )
    for key in differ:
        print(f"  {key}\n    off: {off[key]}\n    on:  {on[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
