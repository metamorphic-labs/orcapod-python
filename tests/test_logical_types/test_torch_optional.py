"""PyTorch stays optional: ``import orcapod`` never imports torch (ITL-871).

These tests run in the default suite, where torch is usually not installed.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.errors import MissingOptionalDependencyError
from orcapod.logical_types.array_format import ARRAY_FORMAT_METADATA, ARRAY_STORAGE_TYPE
from orcapod.sources import ArrowTableSource

TORCH_INSTALLED = importlib.util.find_spec("torch") is not None

SCRIPT = textwrap.dedent(
    """
    import sys

    import numpy as np

    import orcapod
    from orcapod import function_pod
    from orcapod.contexts import get_default_context
    from orcapod.sources import DictSource

    @function_pod(output_keys="y")
    def double(x: np.ndarray) -> np.ndarray:
        return x * 2

    source = DictSource(data=[{"k": "a", "x": np.arange(3.0)}], tag_columns=["k"])
    for _, data in double.pod(source).iter_data():
        data.content_hash()
    get_default_context().semantic_hasher.hash_object([1, "a", np.arange(2)])
    print("torch" in sys.modules)
    """
)


def test_orcapod_never_imports_torch() -> None:
    """Importing Orcapod and running a pipeline leaves torch unimported."""
    repo_root = Path(__file__).resolve().parents[2]
    proc = subprocess.run(
        [sys.executable, "-c", SCRIPT],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(repo_root)},
        cwd=repo_root,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    assert proc.stdout.strip().splitlines()[-1] == "False"


class _StoredTensorType(pa.ExtensionType):
    """A stand-in for a column written by Orcapod with PyTorch installed."""

    def __init__(self) -> None:
        """Build the stand-in over the array struct."""
        super().__init__(ARRAY_STORAGE_TYPE, "orcapod.torch_tensor")

    def __arrow_ext_serialize__(self) -> bytes:
        """Return the array format's extension metadata."""
        return ARRAY_FORMAT_METADATA

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "_StoredTensorType":
        """Rebuild the stand-in type."""
        return cls()


@pytest.mark.skipif(TORCH_INSTALLED, reason="needs an environment without torch")
def test_reading_a_tensor_column_without_torch_names_the_missing_dependency() -> None:
    """Reading a tensor column without torch raises naming the type and the package."""
    storage = pa.array(
        [{"dtype": "float32", "shape": [1], "data": b"\x00\x00\x80?"}],
        type=ARRAY_STORAGE_TYPE,
    )
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "x": pa.ExtensionArray.from_storage(_StoredTensorType(), storage),
        }
    )
    with pytest.raises(
        MissingOptionalDependencyError, match="orcapod.torch_tensor.*torch"
    ):
        source = ArrowTableSource(table, tag_columns=["k"])
        for _, data in source.iter_data():
            data.as_dict()
