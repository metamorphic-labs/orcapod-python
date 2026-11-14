"""Comment 6: a SideEffectJobNode over a standalone SideEffectPodStream, run with
load_only=True, delivers the upstream stream's side effects while reading it.

Compared with the general case: a FunctionJobNode over a FunctionPodStream (a stream
outside a job), run with load_only=True, computes the upstream function while reading it.
"""

from typing import Any

import pyarrow as pa

from orcapod import function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.side_effects import SideEffectJobNode, tap_pod

UP: list[int] = []
OWN: list[int] = []
CALLS: list[int] = []


def deliver_up(x: int, ctx: Any) -> None:
    UP.append(x)


def deliver_own(x: int, ctx: Any) -> None:
    OWN.append(x)


@function_pod(output_keys="y")
def inc(x: int) -> int:
    CALLS.append(x)
    return x + 1


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    return 2 * y


schema = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ]
)
base = ArrowTableStream(
    pa.table({"k": [1, 2, 3], "x": [1, 2, 3]}, schema=schema), tag_columns=["k"]
)

upstream: Any = tap_pod(deliver_up).process(
    base
)  # a SideEffectPodStream, outside a job
print("input type:", type(upstream).__name__)
node = SideEffectJobNode(tap_pod(deliver_own), upstream)
view = node.run(load_only=True)
print(
    f"side-effect node, run(load_only=True): upstream delivered {len(UP)}, own delivered {len(OWN)}, rows {view.as_table().num_rows}"
)
node.clear_cache()
node.run(load_only=True)
print(f"  again after clear_cache(): upstream delivered {len(UP)} in total")

fn_upstream: Any = inc.pod(base)  # a FunctionPodStream, outside a job
fnode = FunctionJobNode(dbl.pod, fn_upstream, pipeline_database=InMemoryArrowDatabase())
fview = fnode.run(load_only=True)
print(
    f"function node over a pod stream, run(load_only=True): upstream function called {len(CALLS)} times; status {fview.status}"
)
