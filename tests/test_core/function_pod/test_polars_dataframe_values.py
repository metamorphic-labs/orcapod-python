"""``pl.DataFrame`` values through the public interface (ITL-578).

Function pods take and return ``pl.DataFrame``; the frames survive operators, orchestrators
and every database backend unchanged, dtype for dtype.
"""

from __future__ import annotations

import polars as pl

from orcapod import function_pod


def test_pod_can_return_a_polars_dataframe() -> None:
    """Declaring a pod with a ``pl.DataFrame`` return type no longer raises."""

    @function_pod(output_keys="df")
    def make_frame(n: int) -> pl.DataFrame:
        return pl.DataFrame({"i": list(range(n))})

    assert make_frame is not None
