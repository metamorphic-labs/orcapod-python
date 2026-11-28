"""Shared container checks for positional function outputs."""

from collections.abc import Callable, Mapping


EXCLUDED_OUTPUT_CONTAINERS = (Mapping, str, bytes, bytearray)


def _make_output_container_check() -> Callable[[object, type], str | None]:
    """Build a check that cloudpickle can send to a worker without imports."""
    excluded = EXCLUDED_OUTPUT_CONTAINERS

    def check(value: object, container: type) -> str | None:
        if isinstance(value, container) and not isinstance(value, excluded):
            return None
        returned = type(value)
        if returned.__module__ in ("builtins", "__main__"):
            return returned.__qualname__
        return f"{returned.__module__}.{returned.__qualname__}"

    return check


output_container_mismatch = _make_output_container_check()
"""Return the type name of an invalid output container, or None on success."""


class UnexpectedOutputContainer(ValueError):
    """Report the returned type without retaining an invalid value."""

    def __init__(self, returned_type: str) -> None:
        """Record the name of the returned container type.

        Args:
            returned_type: Display name of the value's type.
        """
        self.returned_type = returned_type
        super().__init__(f"Unexpected output container type {returned_type}")
