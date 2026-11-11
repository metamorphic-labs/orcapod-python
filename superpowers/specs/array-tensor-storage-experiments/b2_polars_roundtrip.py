"""B2: does each candidate survive the Polars round trips Orcapod operators perform?

Run:
    uv run python superpowers/specs/array-tensor-storage-experiments/b2_polars_roundtrip.py

Claims tested, per candidate:

* ``pl.from_arrow(table).to_arrow()``: what Polars dtype the column becomes and
  what Arrow type comes back (extension kept, storage only, or changed layout).
* The ``Join`` path (``src/orcapod/core/operators/join.py``): ``pl.DataFrame(a)
  .join(pl.DataFrame(b), on=...).to_arrow()`` followed by
  ``arrow_utils.restore_schema_nullability`` with the input schemas. Is the
  column's type restored and are the arrays equal?
* A Polars filter (as ``PolarsFilter``) followed by the same restore.

Orcapod's default context is loaded first, so its Polars extension type for
``numpy.ndarray`` is registered exactly as in a real pipeline.
"""

from __future__ import annotations

from typing import cast

import warnings

import numpy as np
import polars as pl
import pyarrow as pa
from b0_common import (  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory
    ENCODERS,
    arrays_equal,
    checking,
    decode,
    report,
    sample_arrays,
    sample_table,
)

from orcapod.contexts import get_default_context
from orcapod.utils.arrow_utils import restore_schema_nullability

get_default_context()  # registers orcapod's Arrow and Polars extension types


def roundtrip(name: str) -> None:
    table = sample_table(name)
    original_type = table.schema.field("x").type
    like = sample_arrays()[0]
    with checking(f"{name}: from_arrow/to_arrow"):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            df = cast(pl.DataFrame, pl.from_arrow(table))
            back = df.to_arrow()
        back_type = back.schema.field("x").type
        report(
            "INFO",
            f"{name}: polars dtype",
            f"{df.schema['x']}; warnings={len(caught)}",
        )
        report(
            "OK" if back_type == original_type else "INFO",
            f"{name}: type after to_arrow",
            f"{back_type} (same as input: {back_type == original_type})",
        )

    with checking(f"{name}: join + restore_schema_nullability"):
        other = pa.table(
            {"id": pa.array([1, 0], pa.int64()), "y": pa.array(["b", "a"])}
        )
        ref_schema = table.schema
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            joined = (
                pl.DataFrame(table)
                .join(pl.DataFrame(other), on=["id"], how="inner")
                .to_arrow()
            )
        restored = restore_schema_nullability(joined, ref_schema, other.schema).sort_by(
            "id"
        )
        rtype = restored.schema.field("x").type
        got = decode(name, restored.column("x"), like)
        ok = rtype == original_type and arrays_equal(got, sample_arrays())
        report(
            "OK" if ok else "FAIL",
            f"{name}: join + restore",
            f"type restored={rtype == original_type}; arrays equal={arrays_equal(got, sample_arrays())}",
        )

    with checking(f"{name}: filter + restore_schema_nullability"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            filtered = pl.DataFrame(table).filter(pl.col("id") == 1).to_arrow()
        restored = restore_schema_nullability(filtered, table.schema)
        got = decode(name, restored.column("x"), like)
        ok = restored.schema.field("x").type == original_type and arrays_equal(
            got, sample_arrays()[1:]
        )
        report("OK" if ok else "FAIL", f"{name}: filter + restore", f"equal={ok}")


def fst_with_polars_extension_loading() -> None:
    """Polars can keep unknown extensions when told to (load_as_extension)."""
    import os

    with checking("fst: POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR=load_as_extension"):
        os.environ["POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"] = "load_as_extension"
        try:
            df = cast(pl.DataFrame, pl.from_arrow(sample_table("fst")))
            back = df.to_arrow().schema.field("x").type
        finally:
            del os.environ["POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"]
        report(
            "INFO",
            "fst: with load_as_extension",
            f"polars={df.schema['x']}; arrow back={back}",
        )


def polars_array_ops() -> None:
    """A fixed_size_list column becomes a Polars Array: is reshape native?"""
    with checking("fst storage: polars Array reshape"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = cast(pl.DataFrame, pl.from_arrow(sample_table("fst")))
        out = df.select(pl.col("x").reshape((-1, 2, 3)))
        report("INFO", "fst storage: polars reshape to (2,3)", out.schema["x"])
    _ = np  # keep numpy import used in all branches


if __name__ == "__main__":
    for cand in ENCODERS:
        roundtrip(cand)
    fst_with_polars_extension_loading()
    polars_array_ops()
