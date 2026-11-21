"""Every starfix call passes both float options, on for Orcapod's tables, off for values.

starfix's ``normalize_negative_zero`` and ``normalize_nan`` default to off and are not
recorded in the digest (ITL-1179). Orcapod turns them on, explicitly, for the Arrow tables
it builds, and off, explicitly, for a library value's digest. These tests fail when a call
site bypasses ``hashing/starfix_digest.py``, when a call there leaves an option out, when
an own-table path hashes without the options, and when a value path hashes with them.
"""

from __future__ import annotations

import ast
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import orcapod
from orcapod.contexts import get_default_context
from orcapod.hashing import starfix_digest
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.sources import ArrowTableSource

SOURCE_ROOT = Path(orcapod.__file__).parent
STARFIX_MODULE = "hashing/starfix_digest.py"
FLOAT_KEYWORDS = {"normalize_negative_zero", "normalize_nan"}


def _modules() -> list[tuple[str, ast.Module]]:
    return [
        (path.relative_to(SOURCE_ROOT).as_posix(), ast.parse(path.read_text("utf-8")))
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
    ]


class TestOnePlaceCallsStarfix:
    def test_only_starfix_digest_imports_starfix(self) -> None:
        """No module but ``hashing/starfix_digest.py`` imports starfix, in any form."""
        importers = set()
        for name, tree in _modules():
            for node in ast.walk(tree):
                if isinstance(node, ast.Import) and any(
                    alias.name.split(".")[0] == "starfix" for alias in node.names
                ):
                    importers.add(name)
                if (
                    isinstance(node, ast.ImportFrom)
                    and (node.module or "").split(".")[0] == "starfix"
                ):
                    importers.add(name)
        assert importers == {STARFIX_MODULE}

    def test_no_module_reaches_starfix_dynamically(self) -> None:
        """No ``importlib`` or ``__import__`` of starfix anywhere in the package."""
        for name, tree in _modules():
            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and node.value == "starfix":
                    raise AssertionError(f"{name} names the starfix module as a string")

    def test_every_call_in_the_module_passes_both_options(self) -> None:
        """Each ``ArrowDigester`` call in ``starfix_digest`` passes both float keywords."""
        tree = ast.parse((SOURCE_ROOT / STARFIX_MODULE).read_text("utf-8"))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "ArrowDigester"
        ]
        assert calls, "starfix_digest makes no ArrowDigester call"
        for call in calls:
            assert FLOAT_KEYWORDS <= {k.arg for k in call.keywords}, ast.unparse(call)

    def test_the_options_of_each_use(self) -> None:
        on = starfix_digest.ORCAPOD_TABLE_FLOAT_OPTIONS
        off = starfix_digest.LIBRARY_VALUE_FLOAT_OPTIONS
        assert (on.normalize_negative_zero, on.normalize_nan) == (True, True)
        assert (off.normalize_negative_zero, off.normalize_nan) == (False, False)


class _Spy:
    """Stands in for ``ArrowDigester`` and records the float options of every call."""

    def __init__(self, real: Any) -> None:
        self.real = real
        self.calls: list[tuple[str, bool | None, bool | None]] = []

    def _record(self, entry: str, kwargs: dict[str, Any]) -> None:
        self.calls.append(
            (entry, kwargs.get("normalize_negative_zero"), kwargs.get("normalize_nan"))
        )

    def hash_table(self, table: Any, **kwargs: Any) -> bytes:
        self._record("hash_table", kwargs)
        return self.real.hash_table(table, **kwargs)

    def hash_schema(self, schema: Any, **kwargs: Any) -> bytes:
        self._record("hash_schema", kwargs)
        return self.real.hash_schema(schema, **kwargs)

    def table_calls(self) -> set[tuple[bool | None, bool | None]]:
        return {(z, n) for entry, z, n in self.calls if entry == "hash_table"}


@pytest.fixture
def spy(monkeypatch: pytest.MonkeyPatch) -> _Spy:
    recorder = _Spy(starfix_digest.ArrowDigester)
    monkeypatch.setattr(starfix_digest, "ArrowDigester", recorder)
    return recorder


def _orcapod_table() -> pa.Table:
    return pa.table(
        {
            "k": pa.array(["a", "b"], pa.large_string()),
            "j": pa.array([1, 2], pa.int64()),
            "x": pa.array([-0.0, math.nan], pa.float64()),
            "y": pa.array([1.5, 2.5], pa.float32()),
        }
    )


OWN_TABLE_PATHS = {
    "hash_table": lambda: get_default_context().arrow_hasher.hash_table(
        _orcapod_table()
    ),
    "source with a default source id": lambda: (
        ArrowTableSource(
            _orcapod_table(), tag_columns=["k", "j"], tag_only_columns=True
        ).source_id
    ),
    "row hashes of a source": lambda: [
        (tag.content_hash(), data.content_hash())
        for tag, data in ArrowTableSource(
            _orcapod_table(),
            tag_columns=["k", "j"],
            source_id="s",
            tag_only_columns=True,
        ).iter_data()
    ],
    "raw stream content hash": lambda: orcapod.streams.ArrowTableStream(
        _orcapod_table(), tag_columns=["k", "j"]
    ).content_hash(),
    "a stream's as_table content hash column": lambda: ArrowTableSource(
        _orcapod_table(), tag_columns=["k", "j"], source_id="s", tag_only_columns=True
    ).as_table(columns={"content_hash": True}),
}

VALUE_PATHS = {
    "pandas DataFrame": lambda: get_default_context().semantic_hasher.hash_object(
        pd.DataFrame({"x": [-0.0, math.nan], "y": [1, 2]})
    ),
    "pandas Series": lambda: get_default_context().semantic_hasher.hash_object(
        pd.Series([-0.0, 1.0], name="s")
    ),
    "polars DataFrame": lambda: get_default_context().semantic_hasher.hash_object(
        pl.DataFrame({"x": [-0.0, math.nan], "y": [1, 2]})
    ),
    "polars Series": lambda: get_default_context().semantic_hasher.hash_object(
        pl.Series("s", [-0.0, 1.0])
    ),
    "pyarrow Table as a value": lambda: (
        get_default_context().semantic_hasher.hash_object(
            pa.table({"x": [-0.0, math.nan]})
        )
    ),
    "pyarrow RecordBatch as a value": lambda: (
        get_default_context().semantic_hasher.hash_object(
            pa.table({"x": [-0.0, math.nan]}).to_batches()[0]
        )
    ),
    "hash_table_ordered": lambda: get_default_context().arrow_hasher.hash_table_ordered(
        pa.table({"x": [-0.0]})
    ),
    "hash_table_strict": lambda: get_default_context().arrow_hasher.hash_table_strict(
        pa.table({"x": [-0.0]})
    ),
}


class TestEachUseGetsItsOptions:
    @pytest.mark.parametrize("path", list(OWN_TABLE_PATHS))
    def test_orcapod_tables_hash_with_both_options_on(
        self, spy: _Spy, path: str
    ) -> None:
        OWN_TABLE_PATHS[path]()
        assert spy.table_calls() == {(True, True)}, spy.calls

    @pytest.mark.parametrize("path", list(VALUE_PATHS))
    def test_library_values_hash_with_both_options_off(
        self, spy: _Spy, path: str
    ) -> None:
        VALUE_PATHS[path]()
        assert spy.table_calls() == {(False, False)}, spy.calls

    def test_a_row_with_value_cells_uses_each_options_for_its_own_part(
        self, spy: _Spy
    ) -> None:
        """A pandas cell's digest is strict; the row around it follows the float rule."""
        source = ArrowTableSource(
            get_default_context().type_converter.python_dicts_to_arrow_table(
                [{"k": "a", "x": -0.0, "frame": pd.DataFrame({"v": [-0.0]})}],
                python_schema=orcapod.types.Schema(
                    {"k": str, "x": float, "frame": pd.DataFrame}
                ),
            ),
            tag_columns=["k"],
            source_id="s",
            tag_only_columns=True,
        )
        spy.calls.clear()
        for _, data in source.iter_data():
            data.content_hash()
        assert spy.table_calls() == {(True, True), (False, False)}

    def test_schemas_pass_both_options(self, spy: _Spy) -> None:
        hasher = get_default_context().arrow_hasher
        assert isinstance(hasher, StarfixArrowHasher)
        hasher.hash_schema(_orcapod_table().schema)
        assert {(z, n) for e, z, n in spy.calls if e == "hash_schema"} == {(True, True)}


def test_numpy_values_do_not_call_starfix(spy: _Spy) -> None:
    """A numpy array's value hash is its own canonical bytes, never a starfix digest."""
    get_default_context().semantic_hasher.hash_object(np.array([-0.0, np.nan]))
    assert spy.calls == []
