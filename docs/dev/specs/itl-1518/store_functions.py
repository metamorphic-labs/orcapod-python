"""Functions of the store measurement (ITL-1518).

Each call appends a line to the file ``CALLS_FILE`` names, so calls are counted across
processes.
"""

from __future__ import annotations

import os

from orcapod import function_pod


def _note(name: str) -> None:
    with open(os.environ["CALLS_FILE"], "a") as f:
        f.write(name + "\n")


@function_pod(output_keys="doubled")
def double(amplitude: float) -> float:
    """Return twice ``amplitude``."""
    _note("double")
    return 2 * amplitude


@function_pod(output_keys="tripled")
def triple(doubled: float) -> float:
    """Return three times ``doubled``."""
    _note("triple")
    return 3 * doubled


@function_pod(output_keys="halved")
def halve(amplitude: float) -> float:
    """Return half of ``amplitude``."""
    _note("halve")
    return amplitude / 2


@function_pod(output_keys="total")
def add(x: float, y: float) -> float:
    """Return ``x + y``."""
    _note("add")
    return x + y


@function_pod(output_keys="scaled")
def scale(amplitude: float) -> float:
    """Return ten times ``amplitude``."""
    _note("scale")
    return 10 * amplitude
