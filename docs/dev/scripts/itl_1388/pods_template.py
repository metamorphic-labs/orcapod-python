"""Template of the user module ``itl1388_pods`` the audit's jobs are built from (ITL-1388).

``common.write_pods_module(directory, variant)`` writes this text, with the variant's
substitutions, as ``<directory>/itl1388_pods.py``. The audit then runs a reader with that
directory on ``sys.path`` (situation a: unchanged, or c: changed) or without it
(situation b: not importable). Synthetic data only.
"""

TEMPLATE = '''
"""Synthetic pods of the ITL-1388 save/load audit (variant: {variant})."""

from dataclasses import dataclass

import pyarrow as pa

import orcapod
from orcapod import function_pod, tap_pod
from orcapod.types import Cursor, Schema


@dataclass
class Summary:
    """An exact user class (module path + qualified name identity)."""

    value: float
    doubled: float


@orcapod.dataclass
class Pair:
    """A structural class (name + fields identity; rebuildable from metadata)."""

    value: float
    doubled: float


class TwoBatch:
    """A PollingSource implementation: two batches, then nothing new."""

    _BATCHES = [
        {{"id": [1, 2], "val": [10, 20]}},
        {{"id": [3], "val": [30]}},
    ]

    def identity(self):
        return "TwoBatch"

    def to_config(self):
        return None

    @classmethod
    def from_config(cls, config):
        return cls()

    def schema(self):
        return Schema({{"id": int, "val": int}})

    async def poll(self, cursor=None):
        idx = cursor.value if cursor is not None else 0
        return idx < len(self._BATCHES)

    async def fetch(self, cursor=None):
        idx = cursor.value if cursor is not None else 0
        if idx >= len(self._BATCHES):
            return Cursor(value=idx), {{}}
        batch = self._BATCHES[idx]
        return Cursor(value=idx + 1), {{
            k: pa.array(v, type=pa.int64()) for k, v in batch.items()
        }}

    async def close(self):
        pass


@function_pod(output_keys="pair")
def pair(value: float) -> Pair:
    return Pair(value=value, doubled=value * 2)


@function_pod(output_keys="doubled")
def double_val(val: int) -> int:
    return val * 2


@function_pod(output_keys="score")
def score(value: float) -> float:
    return value * {score_factor}


@function_pod(output_keys="maybe")
def maybe({maybe_param}: float) -> float | None:
    return None if {maybe_param} < 3 else {maybe_param}


@function_pod(output_keys="label")
def label(score: float) -> {label_type}:
    return {label_body}


@function_pod(output_keys="kept")
def kept(maybe: float) -> float:
    return maybe + 0.5


@function_pod(output_keys="joined_label")
def joined_label(score: float, group: str) -> str:
    return f"{{group}}:{{score}}"


@function_pod(output_keys="mean_score", version="{mean_version}")
def mean_score(score: list[float]) -> float:
    return sum(score) / len(score)


@function_pod(output_keys="batch_sum")
def batch_sum(score: list[float]) -> float:
    return float(sum(score))


@function_pod(output_keys="summary")
def summarize(value: float) -> Summary:
    return Summary(value=value, doubled=value * 2)


@tap_pod
def tap(score: float, ctx) -> None:
    return None
'''

VARIANTS = {
    "v1": dict(
        score_factor="2.0",
        maybe_param="value",
        label_type="str",
        label_body='"hi" if score > 6 else "lo"',
        mean_version="v0.0",
    ),
    # Situation (c): every kind of change at once: body (score), signature (maybe's
    # parameter), output type (label), version (mean_score major).
    "changed": dict(
        score_factor="3.0",
        maybe_param="reading",
        label_type="int",
        label_body="1 if score > 6 else 0",
        mean_version="v1.0",
    ),
}


def render(variant: str) -> str:
    """The module text of one variant."""
    return TEMPLATE.format(variant=variant, **VARIANTS[variant])
