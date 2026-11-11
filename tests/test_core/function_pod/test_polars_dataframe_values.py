"""``pl.DataFrame`` values through the public interface (ITL-578).

Function pods take and return ``pl.DataFrame``; the frames survive operators, orchestrators
and database backends unchanged, dtype for dtype. Every comparison checks the Polars schema
as well as the values.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from orcapod import PipelineJob, function_pod
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from orcapod.operators import Batch, GroupBy, Join
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import DictSource


def _frame(n: int) -> pl.DataFrame:
    """A frame of height ``n`` whose dtypes would all be lost by careless conversion."""
    return pl.DataFrame(
        {
            "i": pl.Series(list(range(n)), dtype=pl.Int8),
            "e": pl.Series(["a"] * n, dtype=pl.Enum(["b", "a"])),
            "t": pl.Series(
                [dt.datetime(2020, 1, 1)] * n, dtype=pl.Datetime("ms", "UTC")
            ),
            "s": pl.Series(
                [{"u": k, "l": [k]} for k in range(n)],
                dtype=pl.Struct({"u": pl.UInt16, "l": pl.List(pl.Float32)}),
            ),
        }
    )


def _assert_exact(got: object, expected: pl.DataFrame) -> None:
    assert isinstance(got, pl.DataFrame)
    assert got.schema == expected.schema
    assert_frame_equal(got, expected)


def _frame_list(value: object) -> list[pl.DataFrame]:
    """Narrow a list-valued data column to a list of frames."""
    assert isinstance(value, list)
    assert all(isinstance(item, pl.DataFrame) for item in value)
    return cast(list[pl.DataFrame], value)


@function_pod(output_keys="df")
def make_frame(n: int) -> pl.DataFrame:
    return _frame(n)


@function_pod(output_keys="df")
def add_row_index(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_row_index("row", offset=0)


@function_pod(output_keys="height")
def frame_height(df: pl.DataFrame) -> int:
    return df.height


def _int_source() -> DictSource:
    return DictSource(
        [{"k": "a", "n": 1}, {"k": "b", "n": 3}],
        tag_columns=["k"],
    )


def _frame_source() -> DictSource:
    """Three frame-valued rows: two in group ``a``, one in group ``b``."""
    rows: list[dict[str, Any]] = [
        {"g": "a", "k": "1", "df": _frame(1)},
        {"g": "a", "k": "2", "df": _frame(2)},
        {"g": "b", "k": "3", "df": _frame(3)},
    ]
    return DictSource(
        rows,
        tag_columns=["g", "k"],
        data_schema={"g": str, "k": str, "df": pl.DataFrame},
    )


def test_pod_can_return_a_polars_dataframe() -> None:
    """Declaring a pod with a ``pl.DataFrame`` return type no longer raises."""

    @function_pod(output_keys="df")
    def make(n: int) -> pl.DataFrame:
        return pl.DataFrame({"i": list(range(n))})

    assert make.pod is not None


class TestPodValues:
    def test_pod_output(self) -> None:
        stream = make_frame.pod(_int_source())
        got = {tag["k"]: data["df"] for tag, data in stream.iter_data()}
        _assert_exact(got["a"], _frame(1))
        _assert_exact(got["b"], _frame(3))

    def test_output_schema_names_the_python_type(self) -> None:
        _, data_schema = make_frame.pod(_int_source()).output_schema()
        assert data_schema["df"] is pl.DataFrame

    def test_pod_input(self) -> None:
        stream = frame_height.pod(_frame_source())
        got = {tag["k"]: data["height"] for tag, data in stream.iter_data()}
        assert got == {"1": 1, "2": 2, "3": 3}

    def test_chained_pods(self) -> None:
        stream = frame_height.pod(add_row_index.pod(make_frame.pod(_int_source())))
        got = {tag["k"]: data["height"] for tag, data in stream.iter_data()}
        assert got == {"a": 1, "b": 3}

    def test_chained_frame_is_transformed_exactly(self) -> None:
        stream = add_row_index.pod(make_frame.pod(_int_source()))
        got = {tag["k"]: data["df"] for tag, data in stream.iter_data()}
        _assert_exact(got["b"], _frame(3).with_row_index("row", offset=0))


class TestOperators:
    def test_batch_over_frame_column(self) -> None:
        stream = Batch(batch_size=2)(_frame_source())
        rows = [(tag["k"], data["df"]) for tag, data in stream.iter_data()]
        assert [keys for keys, _ in rows] == [["1", "2"], ["3"]]
        members = _frame_list(rows[0][1]) + _frame_list(rows[1][1])
        for got, n in zip(members, [1, 2, 3], strict=True):
            _assert_exact(got, _frame(n))

    def test_batch_output_type_is_a_list_of_frames(self) -> None:
        _, data_schema = Batch(batch_size=2)(_frame_source()).output_schema()
        assert data_schema["df"] == list[pl.DataFrame]

    def test_group_by_over_frame_column(self) -> None:
        stream = GroupBy(by=["g"])(_frame_source())
        got = {
            str(tag["g"]): _frame_list(data["df"]) for tag, data in stream.iter_data()
        }
        assert sorted(got) == ["a", "b"]
        for frame, n in zip(got["a"], [1, 2], strict=True):
            _assert_exact(frame, _frame(n))
        (only_b,) = got["b"]
        _assert_exact(only_b, _frame(3))

    def test_join_passes_frames_through(self) -> None:
        weights = DictSource(
            [{"g": "a", "k": "1", "w": 10}, {"g": "b", "k": "3", "w": 30}],
            tag_columns=["g", "k"],
        )
        stream = Join()(_frame_source(), weights)
        got = {
            str(tag["k"]): (data["df"], data["w"]) for tag, data in stream.iter_data()
        }
        assert sorted(got) == ["1", "3"]
        _assert_exact(got["1"][0], _frame(1))
        _assert_exact(got["3"][0], _frame(3))
        assert (got["1"][1], got["3"][1]) == (10, 30)


_STORES: dict[str, Callable[[Path], ArrowDatabaseProtocol]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda tmp: DeltaTableDatabase(tmp / "delta"),
}
_ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


class TestPipelineStores:
    @pytest.mark.parametrize("store_name", list(_STORES))
    @pytest.mark.parametrize("orchestrator_name", list(_ORCHESTRATORS))
    def test_frames_stored_and_read_back(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        job = PipelineJob(name="frames", store=_STORES[store_name](tmp_path))
        with job:
            frames = make_frame.pod(_int_source(), label="make")
            frame_height.pod(frames, label="height")
        job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())

        got = {tag["k"]: data["df"] for tag, data in job.nodes["make"].iter_data()}
        _assert_exact(got["a"], _frame(1))
        _assert_exact(got["b"], _frame(3))
        heights = {
            tag["k"]: data["height"] for tag, data in job.nodes["height"].iter_data()
        }
        assert heights == {"a": 1, "b": 3}

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "ITL-877: SQLiteConnector rejects every logical-typed column at write time "
            "('does not preserve Arrow metadata'), for pandas, numpy and Path too; "
            "not specific to pl.DataFrame."
        ),
    )
    def test_sqlite_store(self, tmp_path: Path) -> None:
        connector = SQLiteConnector(str(tmp_path / "frames.sqlite"))
        job = PipelineJob(name="frames", store=ConnectorArrowDatabase(connector))
        with job:
            make_frame.pod(_int_source(), label="make")
        job.run()

        got = {tag["k"]: data["df"] for tag, data in job.nodes["make"].iter_data()}
        _assert_exact(got["a"], _frame(1))
        _assert_exact(got["b"], _frame(3))


# Runs one pipeline over a Delta store and prints how many times each pod body ran, plus
# the frames read back. The pod bodies append to a marker file so a cache hit (body not
# run) is observable from the parent process.
_CROSS_PROCESS_SCRIPT = textwrap.dedent(
    """
    import json, sys
    from pathlib import Path
    import polars as pl
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource

    store_path, marker = Path(sys.argv[1]), Path(sys.argv[2])

    @function_pod(output_keys="df")
    def make_frame(n: int) -> pl.DataFrame:
        with marker.open("a") as fh:
            fh.write("make\\n")
        return pl.DataFrame({
            "i": pl.Series(list(range(n)), dtype=pl.Int8),
            "e": pl.Series(["a"] * n, dtype=pl.Enum(["b", "a"])),
        })

    @function_pod(output_keys="height")
    def frame_height(df: pl.DataFrame) -> int:
        with marker.open("a") as fh:
            fh.write("height\\n")
        return df.height

    source = DictSource([{"k": "a", "n": 1}, {"k": "b", "n": 3}], tag_columns=["k"])
    job = PipelineJob(name="frames", store=DeltaTableDatabase(store_path))
    with job:
        frames = make_frame.pod(source, label="make")
        frame_height.pod(frames, label="height")
    job.run()
    frames = {t["k"]: d["df"] for t, d in job.nodes["make"].iter_data()}
    print(json.dumps({
        "frames": {k: f.write_json() for k, f in frames.items()},
        "schemas": {k: str(f.schema) for k, f in frames.items()},
        "heights": {t["k"]: d["height"] for t, d in job.nodes["height"].iter_data()},
    }))
    """
)


def _run_pipeline_process(store_path: Path, marker: Path) -> dict[str, Any]:
    result = subprocess.run(
        [sys.executable, "-c", _CROSS_PROCESS_SCRIPT, str(store_path), str(marker)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_cached_rerun_in_new_process_hits_the_cache(tmp_path: Path) -> None:
    """A second process over the same Delta store recomputes nothing and reads equal frames."""
    store_path, marker = tmp_path / "store", tmp_path / "calls.txt"

    first = _run_pipeline_process(store_path, marker)
    calls_after_first = marker.read_text().splitlines()
    assert sorted(calls_after_first) == ["height", "height", "make", "make"]

    second = _run_pipeline_process(store_path, marker)
    assert marker.read_text().splitlines() == calls_after_first  # nothing re-ran
    assert second == first
    assert second["heights"] == {"a": 1, "b": 3}
    assert second["schemas"]["b"] == str(
        pl.Schema({"i": pl.Int8, "e": pl.Enum(["b", "a"])})
    )


@function_pod(output_keys="column")
def first_column(df: pl.DataFrame) -> pl.Series:
    return df.to_series(0)


@function_pod(output_keys="total")
def series_sum(column: pl.Series) -> int:
    return int(column.sum())


class TestSeriesValues:
    def test_pod_output_and_input(self, tmp_path: Path) -> None:
        job = PipelineJob(name="series", store=DeltaTableDatabase(tmp_path / "s"))
        with job:
            columns = first_column.pod(make_frame.pod(_int_source()), label="first")
            series_sum.pod(columns, label="sum")
        job.run()

        got = {tag["k"]: data["column"] for tag, data in job.nodes["first"].iter_data()}
        for key, n in (("a", 1), ("b", 3)):
            series = got[key]
            assert isinstance(series, pl.Series)
            assert series.name == "i"
            assert series.dtype == pl.Int8
            assert series.to_list() == list(range(n))
        totals = {tag["k"]: data["total"] for tag, data in job.nodes["sum"].iter_data()}
        assert totals == {"a": 0, "b": 3}
