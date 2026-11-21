"""ITL-1179 stage 2: what the float rule changes in the golden values, and nothing else.

Computes every golden group twice in one process: with Orcapod's own-table float options
as shipped (on), and with them switched off (starfix's default, which is what the code on
main computes, since the routing of datagram and stream tables to ``hash_table`` gives the
same digests and labels as the semantic ``ArrowTableHandler`` did). Prints every value
that differs outside the ``arrow.float_rule`` case (which pins the rule itself, so its
-0.0 and NaN entries differ by design), each marked with the kind of golden file its case
pins it in: frozen, or provisional (a case that depends on a provisional operator, source or
library value type, ITL-1266; ``cases.compute_group_kinds`` says which). Expected: the two
approved entries of ``pipeline:group_by_nan_keys``, provisional since GroupBy is.

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


def _compute() -> tuple[dict[str, str], set[str]]:
    """Every computed golden value, flattened, and the names of the provisional ones."""
    values: dict[str, Any] = {}
    provisional: set[str] = set()
    for group in cases.available_groups():
        live, kinds = cases.compute_group_kinds(group)
        values[group] = live
        provisional |= {f"{group}.{entry}" for entry in kinds}
    return _flatten(values, ""), provisional


def _is_provisional(key: str, provisional: set[str]) -> bool:
    return any(key == name or key.startswith(f"{name}.") for name in provisional)


def main() -> int:
    on, provisional = _compute()
    shipped = starfix_digest.ORCAPOD_TABLE_FLOAT_OPTIONS
    starfix_digest.ORCAPOD_TABLE_FLOAT_OPTIONS = starfix_digest.FloatOptions(
        normalize_negative_zero=False, normalize_nan=False
    )
    try:
        off, _ = _compute()
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
        kind = "provisional" if _is_provisional(key, provisional) else "frozen"
        print(f"  {key} [{kind}]\n    off: {off[key]}\n    on:  {on[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
