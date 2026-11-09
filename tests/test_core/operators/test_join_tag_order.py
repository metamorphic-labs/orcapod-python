"""Tag column order produced by Join and MergeJoin must not depend on PYTHONHASHSEED.

Both operators used to build ``tag_columns`` from a ``set``, so the order of the
user tag columns on every emitted ``Tag`` changed from process to process. Rows
loaded from a ``FunctionJobNode`` cache use the compile-time order, so a buffer
mixing cached and freshly computed rows could not be concatenated (ITL-817).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

_SCRIPT = """
import json
import pyarrow as pa
from orcapod.core.operators import Join, MergeJoin
from orcapod.core.sources.arrow_table_source import ArrowTableSource

def source(columns, tags, source_id):
    return ArrowTableSource(pa.table(columns), tag_columns=tags, source_id=source_id)

operator = {operator}
left = source({{"probe": [0, 1], "x": [1, 2]}}, ["probe"], "left")
right = source({{"method": [0, 1], "{right_data}": [3, 4]}}, ["method"], "right")
stream = operator(left, right)
tag, _ = next(iter(stream.iter_data()))
print(json.dumps({{"tag": list(tag.keys()), "stream": list(stream.keys()[0])}}))
"""


def _tag_orders(operator: str, right_data: str, seed: int) -> dict[str, list[str]]:
    env = {**os.environ, "PYTHONHASHSEED": str(seed)}
    result = subprocess.run(
        [sys.executable, "-c", _SCRIPT.format(operator=operator, right_data=right_data)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize(
    ("operator", "right_data"),
    [("Join()", "w"), ("MergeJoin()", "x")],
    ids=["join", "merge_join"],
)
def test_tag_order_is_independent_of_hash_seed(operator: str, right_data: str) -> None:
    """Every process emits tags in the order the operator's schema predicts."""
    orders = [_tag_orders(operator, right_data, seed) for seed in range(1, 7)]
    for seed, order in enumerate(orders, start=1):
        assert order["tag"] == ["probe", "method"], f"PYTHONHASHSEED={seed}: {order}"
        assert order["stream"] == ["probe", "method"], f"PYTHONHASHSEED={seed}: {order}"
