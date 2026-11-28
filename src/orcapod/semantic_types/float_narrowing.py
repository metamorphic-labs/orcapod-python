"""Warn when Python inputs will be stored in a declared float32 column."""

import warnings
from collections.abc import Mapping, Sequence

import numpy as np

from orcapod.errors import ValueConversionWarning


def warn_float32_narrowing(
    rows: Sequence[Mapping[str, object]], schema: Mapping[str, object] | None
) -> None:
    """Warn when supplied Python values meet a top-level float32 declaration."""
    if schema is not None and any(
        any(issubclass(type(row.get(name)), float) for row in rows)
        for name, annotation in schema.items()
        if annotation is np.float32
    ):
        warnings.warn(
            "A column declared numpy.float32 was given Python floats. Values "
            "that numpy.float32 cannot hold exactly are stored changed. Pass "
            "numpy.float32 values to control the conversion, or declare the "
            "column float to keep Python float precision.",
            ValueConversionWarning,
            stacklevel=3,
        )
