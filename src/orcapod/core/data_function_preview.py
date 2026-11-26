"""Which data functions a preview may call, read-only, at every layer (ITL-1464).

A preview calls a data function through ``require_preview`` and ``preview_call``. Those
are the library's own only for an instance of exactly one of the library's classes
that have a read-only call (developer decision N55), so every layer of a function is
checked to be one, before any of its methods is called.
"""

from __future__ import annotations

from orcapod.core.data_function import (
    CachedDataFunction,
    DataFunctionWrapper,
    PythonDataFunction,
)
from orcapod.core.data_function_proxy import DataFunctionProxy
from orcapod.core.streams.preview import is_exact_library_instance
from orcapod.errors import PreviewRefusedError

PREVIEWABLE_DATA_FUNCTIONS: frozenset[type] = frozenset(
    {PythonDataFunction, DataFunctionWrapper, CachedDataFunction, DataFunctionProxy}
)
"""The library's data function classes with a read-only call: a ``CachedDataFunction``
reads its store and writes none; the others call what they wrap or bind."""


def require_previewable_function(function: object) -> None:
    """Refuse a data function any layer of which is not exactly one of
    ``PREVIEWABLE_DATA_FUNCTIONS``.

    The layers are walked from the outermost in: what a wrapper wraps, what a proxy has
    bound (an unbound proxy raises when it is called, as ``call`` does). No method of a
    layer is called before the layer is checked, so a subclass's own ``require_preview``
    or ``preview_call`` never runs, and a function that has none (written to an older
    protocol) is refused, not called.

    Args:
        function: The pod's data function.

    Raises:
        PreviewRefusedError: If a layer is not one of the library's previewable classes.
    """
    layer: object | None = function
    while layer is not None:
        if not is_exact_library_instance(layer, PREVIEWABLE_DATA_FUNCTIONS):
            raise PreviewRefusedError(
                f"its data function {type(layer).__name__} has no read-only call"
            )
        if isinstance(layer, DataFunctionProxy):
            layer = layer.bound_function
        elif isinstance(layer, DataFunctionWrapper):
            layer = layer.wrapped
        else:
            layer = None
