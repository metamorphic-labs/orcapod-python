"""The row identifiers a source has read, to find one it reads twice (ITL-1464)."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any

import numpy as np
import numpy.typing as npt

_DIGEST_BYTES = 16


class SeenKeys:
    """The identifiers read so far, held as 16-byte digests in sorted arrays.

    A set of the identifiers themselves is a few hundred bytes a row; this is 16. The
    digest is BLAKE2b with 128 bits, so that two identifiers with one digest are not
    expected in any table (for a billion rows, about one chance in 10^21): an identifier
    whose digest was seen is taken as seen.
    """

    def __init__(self) -> None:
        # Each array is sorted, and at least twice as long as the next, so a lookup
        # reads few of them and an identifier is merged few times.
        self._runs: list[npt.NDArray[Any]] = []  # pyright: ignore[reportExplicitAny]  # fixed-size bytes

    def add(self, identifiers: Sequence[str]) -> list[str]:
        """Take ``identifiers``, all different, as seen.

        Args:
            identifiers: The identifiers of the next rows.

        Returns:
            Those that were seen before, sorted; none of ``identifiers`` is then added.
        """
        if not identifiers:
            return []
        digests = np.frombuffer(
            b"".join(
                hashlib.blake2b(
                    text.encode("utf-8", "surrogatepass"), digest_size=_DIGEST_BYTES
                ).digest()
                for text in identifiers
            ),
            dtype=f"S{_DIGEST_BYTES}",
        )
        seen = np.zeros(len(digests), dtype=bool)
        for run in self._runs:
            at = np.searchsorted(run, digests)
            seen |= run[np.minimum(at, len(run) - 1)] == digests
        if seen.any():
            return sorted(identifiers[int(i)] for i in np.flatnonzero(seen))
        added = np.sort(digests)
        while self._runs and len(self._runs[-1]) < 2 * len(added):
            added = np.sort(np.concatenate([self._runs.pop(), added]), kind="stable")
        self._runs.append(added)
        return []

    @property
    def nbytes(self) -> int:
        """The bytes held."""
        return sum(run.nbytes for run in self._runs)
