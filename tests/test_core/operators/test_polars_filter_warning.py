"""``PolarsFilter`` warns every time one is built (ITL-1296).

``PolarsFilter`` passes every column through Polars, which changes values of types Polars
does not hold exactly (a ``date64`` off midnight comes back at midnight). Until that is
fixed, building one issues ``PolarsFilterWarning``, at every construction and at every load
of a saved one (rebuilt or kept read-only, once per node), in addition to
the once-per-process ``ProvisionalHashWarning``; ``Filter`` is the operator to use.
"""

from __future__ import annotations

import os
import subprocess
import sys
import warnings
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

import orcapod
from orcapod import PipelineJob
from orcapod.core.operators import Filter, PolarsFilter
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import PolarsFilterWarning, ProvisionalHashWarning
from orcapod.pipeline.serialization import resolve_operator_from_config
from orcapod.types import col
from orcapod.utils.provisional import reset_provisional_use_warnings


def _source() -> ArrowTableSource:
    table = pa.table(
        {
            "session": pa.array(["s1", "s1", "s2"], pa.large_string()),
            "unit": pa.array([3, 7, 3], pa.int64()),
            "rate": pa.array([4.0, 2.5, 6.1], pa.float64()),
            "depth": pa.array([10, 20, 30], pa.int32()),
        }
    )
    return ArrowTableSource(
        table,
        tag_columns=["session", "unit"],
        tag_only_columns=True,
        source_id="units",
    )


def _polars_filter_warnings(
    caught: list[warnings.WarningMessage],
) -> list[warnings.WarningMessage]:
    return [w for w in caught if issubclass(w.category, PolarsFilterWarning)]


def test_the_warning_is_a_user_warning() -> None:
    assert issubclass(PolarsFilterWarning, UserWarning)
    assert not issubclass(PolarsFilterWarning, ProvisionalHashWarning)


def test_every_construction_warns() -> None:
    """Three filters built on one line give three warnings: the default filter, which
    shows a warning once per line, does not hide the second."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        for threshold in (1.0, 2.0, 3.0):
            _ = PolarsFilter(
                predicates=[pl.col("rate") > threshold, pl.col("depth") < 100],
                constraints={"session": "s1", "unit": 3},
            )
    assert len(_polars_filter_warnings(caught)) == 3


def test_the_message_names_the_fault_and_the_operator_to_use() -> None:
    with pytest.warns(PolarsFilterWarning) as caught:
        _ = PolarsFilter(constraints={"session": "s1", "unit": 3})
    (warning,) = _polars_filter_warnings(list(caught))
    text = str(warning.message)
    assert "PolarsFilter" in text
    assert "date64" in text and "midnight" in text
    assert "not recommended" in text
    assert "Filter" in text.replace("PolarsFilter", "")
    # Shown at the line that built it.
    assert warning.filename == __file__


def test_a_filter_with_no_argument_warns_too() -> None:
    with pytest.warns(PolarsFilterWarning):
        _ = PolarsFilter()


def test_the_stream_method_warns_at_every_call() -> None:
    source = _source()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        for session in ("s1", "s2"):
            _ = source.polars_filter(pl.col("rate") > 1.0, session=session, unit=3)
    found = _polars_filter_warnings(caught)
    assert len(found) == 2
    assert all(w.filename == __file__ for w in found)


def test_an_error_filter_refuses_the_construction() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", PolarsFilterWarning)
        with pytest.raises(PolarsFilterWarning):
            _ = PolarsFilter(constraints={"session": "s1", "unit": 3})


def test_an_ignore_filter_silences_it() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        warnings.simplefilter("ignore", PolarsFilterWarning)
        _ = PolarsFilter(constraints={"session": "s1", "unit": 3})
    assert not _polars_filter_warnings(caught)


def test_the_provisional_warning_stays_once_per_process() -> None:
    reset_provisional_use_warnings()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _ = PolarsFilter(constraints={"session": "s1", "unit": 3})
            _ = PolarsFilter(constraints={"unit": 3, "session": "s1"})
        provisional = [
            w
            for w in caught
            if issubclass(w.category, ProvisionalHashWarning)
            and "PolarsFilter" in str(w.message)
        ]
        assert len(provisional) == 1
        assert len(_polars_filter_warnings(caught)) == 2
    finally:
        reset_provisional_use_warnings()


def test_filter_does_not_warn() -> None:
    source = _source()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = Filter(col("rate") > 1.0)
        _ = source.filter(col("rate") > 1.0, col("depth") < 100, session="s1", unit=3)
    assert not _polars_filter_warnings(caught)


def test_the_warning_changes_no_identity() -> None:
    """Two filters of one predicate and constraints share an identity, warned or
    silenced."""
    source = _source()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PolarsFilterWarning)
        silenced = source.polars_filter(session="s1", unit=3)
    with pytest.warns(PolarsFilterWarning):
        warned = source.polars_filter(session="s1", unit=3)
    assert warned.content_hash() == silenced.content_hash()
    assert warned.pipeline_hash() == silenced.pipeline_hash()


# ── load routes ─────────────────────────────────────────────────────────────


def _count(caught: list[warnings.WarningMessage]) -> int:
    return len(_polars_filter_warnings(caught))


def test_from_config_warns_at_every_call() -> None:
    with pytest.warns(PolarsFilterWarning):
        config = PolarsFilter(
            predicates=[pl.col("rate") > 1.0, pl.col("depth") < 100],
            constraints={"session": "s1", "unit": 3},
        ).to_config()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        rebuilt = [PolarsFilter.from_config(config) for _ in range(2)]
    assert all(isinstance(op, PolarsFilter) for op in rebuilt)
    assert _count(caught) == 2


def test_the_operator_resolver_warns_at_every_call() -> None:
    with pytest.warns(PolarsFilterWarning):
        config = PolarsFilter(constraints={"session": "s1", "unit": 3}).to_config()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        for _ in range(2):
            _ = resolve_operator_from_config(config)
    assert _count(caught) == 2


def _saved_job(tmp_path: Path, *, rebuildable: bool) -> Path:
    """Save a job holding two ``PolarsFilter`` nodes; return the saved file.

    With ``rebuildable`` their predicates are expressions, which a load rebuilds;
    otherwise boolean masks, which no config holds: a load keeps such a node read-only.
    """
    first: list[object] = (
        [pl.col("rate") > 1.0, pl.col("depth") < 100]
        if rebuildable
        else [[True, True, False]]
    )
    second: list[object] = (
        [pl.col("depth") >= 10] if rebuildable else [[True, False, True]]
    )
    job = PipelineJob(name="filtered", database=DeltaTableDatabase(tmp_path / "store"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PolarsFilterWarning)
        with job:
            _ = _source().polars_filter(*first, session="s1", label="first")
            _ = _source().polars_filter(*second, unit=3, label="second")
    path = tmp_path / "job.json"
    job.save(path)
    return path


@pytest.mark.parametrize("rebuildable", [True, False], ids=["rebuilt", "read_only"])
def test_every_load_warns_once_per_polars_filter_node(
    tmp_path: Path, rebuildable: bool
) -> None:
    """A load warns for each saved ``PolarsFilter``, rebuilt or kept read-only, and the
    next load warns again."""
    path = _saved_job(tmp_path, rebuildable=rebuildable)
    for _ in range(2):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("default")
            loaded = PipelineJob.load(
                path, database=DeltaTableDatabase(tmp_path / "store")
            )
        assert _count(caught) == 2
        assert all(w.filename == __file__ for w in _polars_filter_warnings(caught))
        assert sorted(loaded.nodes) == ["first", "second", "units"]


def test_saving_and_running_do_not_warn(tmp_path: Path) -> None:
    job = PipelineJob(name="filtered", database=DeltaTableDatabase(tmp_path / "store"))
    with pytest.warns(PolarsFilterWarning):
        with job:
            _ = _source().polars_filter(pl.col("rate") > 1.0, unit=3, label="kept")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        _ = job.run()
        job.save(tmp_path / "job.json")
    assert _count(caught) == 0


@pytest.mark.parametrize("rebuildable", [True, False], ids=["rebuilt", "read_only"])
def test_an_error_filter_refuses_the_load(tmp_path: Path, rebuildable: bool) -> None:
    path = _saved_job(tmp_path, rebuildable=rebuildable)
    with warnings.catch_warnings():
        warnings.simplefilter("error", PolarsFilterWarning)
        with pytest.raises(PolarsFilterWarning):
            _ = PipelineJob.load(path, database=DeltaTableDatabase(tmp_path / "store"))


# ── the decimal256 fault, as it is described ────────────────────────────────

_DECIMAL256_SCRIPT = """
import decimal, warnings
import polars as pl
import pyarrow as pa
from orcapod.core.sources import ArrowTableSource

warnings.simplefilter("ignore")
table = pa.table(
    {
        "session": pa.array(["s1", "s2"], pa.large_string()),
        "unit": pa.array([1, 2], pa.int64()),
        "rate": pa.array([1.0, 2.0], pa.float64()),
        "wide": pa.array(
            [decimal.Decimal("1.1"), decimal.Decimal("2.2")], pa.decimal256(76, 40)
        ),
    }
)
source = ArrowTableSource(
    table, tag_columns=["session", "unit"], tag_only_columns=True, source_id="wide"
)
try:
    try:
        source.polars_filter(pl.col("rate") > 0.0, unit=1).as_table()
    except Exception:
        print("caught as Exception")
    else:
        print("no error")
except BaseException as error:
    print("caught as BaseException:", type(error).__module__, type(error).__name__)
print("the process goes on")
"""


def test_a_decimal256_column_raises_a_panic_exception() -> None:
    """What the warning and the documentation say of ``decimal256`` (review of PR 625):
    Polars panics, the filter raises ``pyo3_runtime.PanicException``, which ``except
    Exception`` does not catch and ``except BaseException`` does, and the process goes
    on. Run in a subprocess: a panic is not something to provoke in the test process."""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(orcapod.__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-c", _DECIMAL256_SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "caught as BaseException: pyo3_runtime PanicException",
        "the process goes on",
    ]
    assert "Int256" in result.stderr
