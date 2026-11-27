"""Regression tests for the independent review of PR 525 (ITL-1425).

One class per finding; each test fails without its fix:

1. a ``CachedSource`` reloaded under a bumped reader version refuses a cache of the
   old id instead of adding the new rows beside the old ones;
2. two different sources whose names coincide are refused one job slot;
3. a ``CachedSource`` over a default ``node.as_source()`` reloads with the saved id;
4. a job file with a source config saved before ITL-1425 is refused at load, also
   nested in a ``CachedSource`` config;
5. the stream builder refuses a malformed kind part, and ``SourceProxy.source_name``
   reads the saved id with the one parser;
6. the warning for a name shaped like a source id is for a given name only, not a
   default built from where the data lives.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import warnings
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.sources.source_proxy import SourceProxy
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import (
    CachedSourceIdMismatchError,
    ProvisionalHashWarning,
    SourceNameWarning,
    SourceSlotNameCollisionError,
    SourceVersionWarning,
)
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    SQLiteTableSource,
)
from orcapod.types import Schema

# Two tags and two data columns (several-items rule).
SCHEMA = pa.schema(
    [
        pa.field("subject", pa.large_string(), nullable=False),
        pa.field("session", pa.int64(), nullable=False),
        pa.field("weight", pa.float64()),
        pa.field("count", pa.int64()),
    ]
)
KEY = ["subject", "session"]
ROWS = [
    {"subject": "s1", "session": 1, "weight": 25.5, "count": 3},
    {"subject": "s1", "session": 2, "weight": 22.0, "count": 5},
    {"subject": "s2", "session": 1, "weight": 27.25, "count": 7},
]
OTHER_ROWS = [
    {"subject": "s7", "session": 4, "weight": 40.0, "count": 8},
    {"subject": "s8", "session": 5, "weight": 9.0, "count": 3},
]
TAG_ORDERS = [["subject", "session"], ["session", "subject"]]
ORCHESTRATORS = [None, AsyncPipelineOrchestrator()]


def _sorted_weights(table: pa.Table) -> list[float]:
    """Return the fixture's non-null weights in a stable order."""
    return sorted(cast("list[float]", table.column("weight").to_pylist()))


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> Path:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SCHEMA.names))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _csv(
    path: str | os.PathLike[str], tags: list[str] | None = None, **kwargs
) -> CSVSource:
    return CSVSource(
        path,
        tag_columns=tags or KEY,
        record_id_columns=KEY,
        tag_only_columns=True,
        schema=SCHEMA,
        **kwargs,
    )


@function_pod(output_keys="ratio")
def ratio(weight: float | None, count: int | None) -> float | None:
    if weight is None or not count:
        return None
    return weight / count


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    return _write_csv(tmp_path / "subjects.csv", ROWS)


def _source_ids(table: pa.Table) -> set[str]:
    return {
        str(value)
        for name in table.column_names
        if name.startswith("_tag_source_id")
        for value in table.column(name).to_pylist()
    }


class TestFinding1CacheUnderAnotherVersion:
    """A cache of ``csv_v1:subjects`` is not read or written as ``csv_v2:subjects``."""

    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_reload_under_a_bumped_version_is_refused_and_the_cache_kept(
        self, tmp_path, csv_path, tags, monkeypatch
    ):
        cache_dir = tmp_path / "cache"
        cached = CachedSource(
            _csv(csv_path, tags, source_id="subjects"),
            cache_database=DeltaTableDatabase(cache_dir),
        )
        assert cached.as_table().num_rows == len(ROWS)
        config = cached.to_config()
        path = cached.cache_path

        def stored() -> pa.Table:
            table = DeltaTableDatabase(cache_dir).get_all_records(
                path, record_id_column="_record_hash"
            )
            assert table is not None
            return table

        before = stored()
        assert _source_ids(before) == {"csv_v1:subjects"}

        monkeypatch.setattr(CSVSource, "SOURCE_VERSION", 2)
        with pytest.warns(SourceVersionWarning):
            reloaded = CachedSource.from_config(config)
        assert reloaded.source_id == "csv_v2:subjects"
        with pytest.raises(CachedSourceIdMismatchError) as caught:
            reloaded.as_table()
        assert "csv_v1:subjects" in str(caught.value)
        assert "csv_v2:subjects" in str(caught.value)
        assert "rebuilt" in str(caught.value)
        # A second read is refused too: nothing was taken for this source's rows.
        with pytest.raises(CachedSourceIdMismatchError):
            reloaded.as_table()

        after = stored()
        assert after.num_rows == before.num_rows == len(ROWS)
        assert _source_ids(after) == {"csv_v1:subjects"}

    def test_the_same_version_reads_its_cache(self, tmp_path, csv_path):
        cache_dir = tmp_path / "cache"
        cached = CachedSource(
            _csv(csv_path, source_id="subjects"),
            cache_database=DeltaTableDatabase(cache_dir),
        )
        cached.as_table()
        reloaded = CachedSource.from_config(cached.to_config())
        table = reloaded.as_table(columns={"system_tags": True})
        assert table.num_rows == len(ROWS)
        assert _source_ids(table) == {"csv_v1:subjects"}


# A data frame source reads a nullable column back as declared by the frame; over a
# table without nullable columns both sources hash one table and so get one name.
NON_NULL_SCHEMA = pa.schema([field.with_nullable(False) for field in SCHEMA])


def _arrow_and_data_frame_defaults() -> tuple[ArrowTableSource, DataFrameSource]:
    table = pa.Table.from_pylist(ROWS, schema=NON_NULL_SCHEMA)
    arrow = ArrowTableSource(
        table, tag_columns=KEY, record_id_columns=KEY, tag_only_columns=True
    )
    frame = DataFrameSource(
        table, tag_columns=KEY, record_id_columns=KEY, tag_only_columns=True
    )
    return arrow, frame


def _sqlite_and_csv_one_name(tmp_path: Path) -> tuple[SQLiteTableSource, CSVSource]:
    db_path = tmp_path / "data.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE records (subject TEXT NOT NULL, session INTEGER NOT NULL, "
            "weight REAL, count INTEGER, PRIMARY KEY (subject, session))"
        )
        connection.executemany(
            "INSERT INTO records VALUES (:subject, :session, :weight, :count)", ROWS
        )
    sqlite_source = SQLiteTableSource(
        str(db_path), "records", tag_columns=KEY, tag_only_columns=True
    )
    csv_source = _csv(_write_csv(tmp_path / "data.db:main.records", OTHER_ROWS))
    return sqlite_source, csv_source


class TestFinding2SourcesSharingASlotName:
    def test_the_names_coincide(self, tmp_path):
        arrow, frame = _arrow_and_data_frame_defaults()
        assert arrow.source_name == frame.source_name
        assert arrow.source_id != frame.source_id
        sqlite_source, csv_source = _sqlite_and_csv_one_name(tmp_path)
        assert sqlite_source.source_name == csv_source.source_name
        assert sqlite_source.source_id != csv_source.source_id

    @pytest.mark.parametrize("pair", ["arrow_and_frame", "sqlite_and_csv"])
    @pytest.mark.parametrize("order", ["given", "reversed"])
    def test_two_sources_with_one_name_are_refused(self, tmp_path, pair, order):
        sources = (
            _arrow_and_data_frame_defaults()
            if pair == "arrow_and_frame"
            else _sqlite_and_csv_one_name(tmp_path)
        )
        if order == "reversed":
            sources = sources[::-1]
        job = PipelineJob(name="collide", database=InMemoryArrowDatabase())
        with pytest.raises(SourceSlotNameCollisionError) as caught:
            with job:
                ratio.pod(sources[0], label="first")
                ratio.pod(sources[1], label="second")
        for source in sources:
            assert repr(source.source_id) in str(caught.value)
        assert "source_id=" in str(caught.value)

    @pytest.mark.parametrize("orchestrator", ORCHESTRATORS, ids=["sync", "async"])
    def test_given_names_keep_both_sources_and_their_rows(self, tmp_path, orchestrator):
        db_path = tmp_path / "data.db"
        _sqlite_and_csv_one_name(tmp_path)  # writes both files
        sqlite_source = SQLiteTableSource(
            str(db_path),
            "records",
            tag_columns=KEY,
            tag_only_columns=True,
            source_id="from_sqlite",
        )
        csv_source = _csv(tmp_path / "data.db:main.records", source_id="from_csv")
        job = PipelineJob(name="named", database=DeltaTableDatabase(tmp_path / "db"))
        with job:
            ratio.pod(sqlite_source, label="first")
            ratio.pod(csv_source, label="second")
        job.run(orchestrator=orchestrator)
        assert set(job.sources) == {"from_sqlite", "from_csv"}
        first = job.nodes["first"].as_table().column("subject").to_pylist()
        second = job.nodes["second"].as_table().column("subject").to_pylist()
        assert sorted(first) == ["s1", "s1", "s2"]
        assert sorted(second) == ["s7", "s8"]

        saved = tmp_path / "job.json"
        job.save(saved)
        loaded = PipelineJob.load(saved, database=DeltaTableDatabase(tmp_path / "db"))
        assert set(loaded.sources) == {"from_sqlite", "from_csv"}
        from_sqlite, from_csv = (
            loaded.sources["from_sqlite"],
            loaded.sources["from_csv"],
        )
        assert isinstance(from_sqlite, SQLiteTableSource)
        assert isinstance(from_csv, CSVSource)
        assert from_sqlite.source_id == "sqlite_table_v1:from_sqlite"
        assert from_csv.source_id == "csv_v1:from_csv"


class TestFinding3CachedDefaultDerivedSource:
    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_reload_keeps_the_saved_id_name_and_slot(self, tmp_path, csv_path, tags):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ProvisionalHashWarning)
            job = PipelineJob(name="calc", database=DeltaTableDatabase(tmp_path / "db"))
            with job:
                ratio.pod(_csv(csv_path, tags, source_id="subjects"), label="calc")
            job.run()
            derived = job.nodes["calc"].as_source()
            cached = CachedSource(
                derived, cache_database=DeltaTableDatabase(tmp_path / "cache")
            )
            rows = cached.as_table().num_rows
            config = cached.to_config()
            assert config["inner_source"]["source_name"] is None
            reloaded = CachedSource.from_config(config)
            assert isinstance(reloaded.cached_source, SourceProxy)
            assert reloaded.source_id == cached.source_id
            assert reloaded.source_id.startswith("derived_v1:semantic_v1:")
            # A slot over a source is named by its source_name (SourceJobNode.from_stream);
            # binding a cache over a proxy is refused for another reason, on main too
            # (its zero-row table has no system-tag columns), so the name is compared.
            assert reloaded.source_name == cached.source_name
            assert SourceJobNode.from_stream(cached).name == reloaded.source_name
            assert reloaded.as_table().num_rows == rows == len(ROWS)


def _legacy(config: dict[str, object]) -> None:
    """Turn a saved source config into its form before ITL-1425, in place."""
    config["source_id"] = config.pop("source_name")
    del config["source_version"]


class TestFinding4LegacyConfigInAJobFile:
    def _saved_job(self, tmp_path: Path, source) -> Path:
        job = PipelineJob(name="legacy", database=DeltaTableDatabase(tmp_path / "db"))
        with job:
            ratio.pod(source, label="ratio")
        job.run()
        path = tmp_path / "job.json"
        job.save(path)
        return path

    def _edit(self, path: Path, edit) -> None:
        data = json.loads(path.read_text())
        (config,) = data["bindings"]["sources"].values()
        edit(config)
        path.write_text(json.dumps(data))

    def test_the_unedited_file_loads(self, tmp_path, csv_path):
        path = self._saved_job(tmp_path, _csv(csv_path, source_id="subjects"))
        loaded = PipelineJob.load(path, database=DeltaTableDatabase(tmp_path / "db"))
        reloaded = loaded.sources["subjects"]
        assert isinstance(reloaded, CSVSource)
        assert reloaded.source_id == "csv_v1:subjects"

    def test_a_legacy_source_config_is_refused(self, tmp_path, csv_path):
        path = self._saved_job(tmp_path, _csv(csv_path, source_id="subjects"))
        self._edit(path, _legacy)
        with pytest.raises(ValueError, match="source_name"):
            PipelineJob.load(path, database=DeltaTableDatabase(tmp_path / "db"))

    def test_a_legacy_config_nested_in_a_cached_source_is_refused(
        self, tmp_path, csv_path
    ):
        cached = CachedSource(
            _csv(csv_path, source_id="subjects"),
            cache_database=DeltaTableDatabase(tmp_path / "cache"),
        )
        path = self._saved_job(tmp_path, cached)
        self._edit(path, lambda config: _legacy(config["inner_source"]))
        with pytest.raises(ValueError, match="source_name"):
            PipelineJob.load(path, database=DeltaTableDatabase(tmp_path / "db"))


class TestFinding5OneBuilderOneParser:
    @pytest.mark.parametrize("bad", ["wrong", "csv", "csv_v0", "Csv_v1", "csv_v1:x"])
    def test_the_stream_builder_refuses_a_malformed_kind_part(self, bad):
        from orcapod.config import DEFAULT_CONFIG
        from orcapod.contexts import resolve_context

        builder = SourceStreamBuilder(
            data_context=resolve_context(None), config=DEFAULT_CONFIG
        )
        table = pa.Table.from_pylist(ROWS, schema=SCHEMA)
        with pytest.raises(ValueError, match="kind part"):
            builder.build(table, tag_columns=KEY, source_name="x", kind_part=bad)

    def test_the_stream_builder_builds_a_valid_kind_part(self):
        from orcapod.config import DEFAULT_CONFIG
        from orcapod.contexts import resolve_context

        builder = SourceStreamBuilder(
            data_context=resolve_context(None), config=DEFAULT_CONFIG
        )
        table = pa.Table.from_pylist(ROWS, schema=SCHEMA)
        result = builder.build(
            table, tag_columns=KEY, source_name="a:b", kind_part="csv_v1"
        )
        assert result.source_id == "csv_v1:a:b"

    def _proxy(self, source_id: str) -> SourceProxy:
        return SourceProxy(
            source_id=source_id,
            content_hash_str="semantic_v1:" + "0" * 64,
            pipeline_hash_str="semantic_v1:" + "1" * 64,
            tag_schema=Schema({"subject": str, "session": int}),
            data_schema=Schema({"weight": float, "count": int}),
        )

    def test_the_proxy_reads_its_name_with_the_parser(self):
        assert self._proxy("csv_v1:a:b").source_name == "a:b"

    @pytest.mark.parametrize("bad", ["csv:units", "unknown", "csv_v1:"])
    def test_the_proxy_refuses_a_saved_id_that_is_not_one(self, bad):
        with pytest.raises(ValueError):
            _ = self._proxy(bad).source_name


class TestFinding6WarningForAGivenNameOnly:
    @pytest.fixture
    def kind_looking_file(self, tmp_path, monkeypatch) -> str:
        monkeypatch.chdir(tmp_path)
        _write_csv(tmp_path / "csv_v1:input.csv", ROWS)
        return "csv_v1:input.csv"

    def test_a_default_location_does_not_warn(self, kind_looking_file):
        with warnings.catch_warnings():
            warnings.simplefilter("error", SourceNameWarning)
            source = _csv(Path(kind_looking_file))
            reloaded = CSVSource.from_config(source.to_config())
        assert source.source_id == reloaded.source_id == "csv_v1:csv_v1:input.csv"

    def test_the_same_text_given_as_the_name_warns(self, kind_looking_file):
        with pytest.warns(SourceNameWarning):
            source = _csv(Path(kind_looking_file), source_id="csv_v1:input.csv")
        assert source.source_id == "csv_v1:csv_v1:input.csv"


# ---------------------------------------------------------------------------
# Second review of PR 525 (head 0af907a3)
# ---------------------------------------------------------------------------


def _write_sqlite(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE records (subject TEXT NOT NULL, session INTEGER NOT NULL, "
            "weight REAL, count INTEGER, PRIMARY KEY (subject, session))"
        )
        connection.executemany(
            "INSERT INTO records VALUES (:subject, :session, :weight, :count)", ROWS
        )


def _write_delta(path: Path) -> None:
    from deltalake import write_deltalake

    write_deltalake(str(path), pa.Table.from_pylist(ROWS, schema=SCHEMA))


def _reader(
    kind: str, location: str | os.PathLike[str], **kwargs
) -> CSVSource | DeltaTableSource | SQLiteTableSource:
    if kind == "csv":
        return _csv(location, **kwargs)
    if kind == "delta":
        return DeltaTableSource(
            location,
            tag_columns=KEY,
            record_id_columns=KEY,
            tag_only_columns=True,
            **kwargs,
        )
    return SQLiteTableSource(
        location, "records", tag_columns=KEY, tag_only_columns=True, **kwargs
    )


def _write(kind: str, path: Path) -> None:
    if kind == "csv":
        _write_csv(path, ROWS)
    elif kind == "delta":
        _write_delta(path)
    else:
        _write_sqlite(path)


def _rebuild(source):
    return type(source).from_config(source.to_config())


READERS = ["csv", "delta", "sqlite"]


def test_a_job_over_a_csv_path_saves_and_loads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = _write_csv(Path("units.csv"), ROWS)
    source = CSVSource(path, tag_columns=KEY, tag_only_columns=True, source_id="units")
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "db"))
    with job:
        _ = ratio.pod(source, label="ratio")

    saved = tmp_path / "job.json"
    job.save(saved)
    reloaded = PipelineJob.load(saved).sources["units"]
    assert isinstance(reloaded, CSVSource)
    assert reloaded.source_id == source.source_id == "csv_v1:units"
    assert reloaded.to_config()["file_path"] == "units.csv"
    assert reloaded.as_table().num_rows == len(ROWS)


class _CountingPath:
    def __init__(self, text: str) -> None:
        self.text: str = text
        self.calls: int = 0

    def __fspath__(self) -> str:
        self.calls += 1
        return self.text


@pytest.mark.parametrize(
    "kind, path_key, kind_part, name_suffix",
    [
        ("csv", "file_path", "csv_v1", ""),
        ("delta", "delta_table_path", "delta_table_v1", ""),
        ("sqlite", "db_path", "sqlite_table_v1", ":main.records"),
    ],
)
def test_a_path_like_source_uses_its_text_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    path_key: str,
    kind_part: str,
    name_suffix: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    filename = f"units.{kind}"
    _write(kind, tmp_path / filename)
    path = _CountingPath(f"./{filename}")
    source = _reader(kind, path)
    config = source.to_config()
    _ = json.dumps(config)
    assert config[path_key] == path.text
    reloaded = type(source).from_config(config)
    assert (
        reloaded.source_id
        == source.source_id
        == f"{kind_part}:{path.text}{name_suffix}"
    )
    assert reloaded.as_table().num_rows == len(ROWS)
    assert path.calls == 1


@pytest.mark.parametrize(
    "kind,path_key,kind_part",
    [("csv", "file_path", "csv_v1"), ("delta", "delta_table_path", "delta_table_v1")],
)
@pytest.mark.parametrize(
    "spelling", ["relative", "absolute", "pathlib", "parent", "symlink"]
)
def test_file_source_job_saves_given_path_and_pins_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    path_key: str,
    kind_part: str,
    spelling: str,
) -> None:
    original = tmp_path / "original"
    (original / "data" / "nested").mkdir(parents=True)
    elsewhere = tmp_path / "elsewhere"
    (elsewhere / "data" / "nested").mkdir(parents=True)
    monkeypatch.chdir(original)
    path = Path("data/units")
    _write(kind, original / path)
    if spelling == "symlink":
        (original / "link").symlink_to(original / "data", target_is_directory=True)
        (elsewhere / "link").symlink_to(elsewhere / "data", target_is_directory=True)
    given = {
        "relative": "./data//units",
        "absolute": str(original / path),
        "pathlib": path,
        "parent": "data/nested/../units",
        "symlink": "link/units",
    }[spelling]
    expected = os.fspath(given)
    source = _reader(kind, given)
    assert source.source_id == f"{kind_part}:{expected}"
    assert source.to_config()[path_key] == expected

    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "db"))
    with job:
        _ = ratio.pod(source, label="ratio")
    saved = tmp_path / "job.json"
    job.save(saved)

    if kind == "csv":
        _ = _write_csv(elsewhere / path, ROWS)
        _ = _write_csv(original / path, OTHER_ROWS)
    else:
        _write_delta(elsewhere / path)
        from deltalake import write_deltalake

        write_deltalake(
            str(original / path),
            pa.Table.from_pylist(OTHER_ROWS, schema=SCHEMA),
            mode="overwrite",
        )

    monkeypatch.chdir(elsewhere)
    assert source.check_for_new_data()
    assert source.as_table().num_rows == len(OTHER_ROWS)
    loaded = PipelineJob.load(saved)
    restored = loaded.sources[expected]
    assert isinstance(restored, (CSVSource, DeltaTableSource))
    assert restored.source_id == source.source_id
    assert restored.as_table().num_rows == (
        len(OTHER_ROWS) if spelling == "absolute" else len(ROWS)
    )
    saved_again = tmp_path / "job-again.json"
    loaded.save(saved_again)
    assert saved_again.read_bytes() == saved.read_bytes()


def test_a_job_with_a_previously_resolved_delta_path_still_loads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "units"
    _write_delta(path)
    source = _reader("delta", "units")
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "db"))
    with job:
        _ = ratio.pod(source, label="ratio")
    saved = tmp_path / "job.json"
    job.save(saved)
    # Older jobs saved the resolved location alongside the original, relative name.
    payload = json.loads(saved.read_text())
    config = payload["bindings"]["sources"]["units"]
    config["delta_table_path"] = str(path.resolve())
    _ = saved.write_text(json.dumps(payload))

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    restored = PipelineJob.load(saved).sources["units"]
    assert isinstance(restored, DeltaTableSource)
    assert restored.source_id == "delta_table_v1:units"
    assert restored.to_config() == config
    assert restored.as_table().num_rows == len(ROWS)


@pytest.mark.parametrize(
    "filename",
    [
        "alias/../units.sqlite",
        pytest.param(
            "file:units.sqlite",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="Colon in a literal filename"
            ),
        ),
    ],
)
def test_sqlite_refresh_opens_the_same_literal_filename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, filename: str
) -> None:
    original = tmp_path / "original"
    elsewhere = tmp_path / "elsewhere"
    (original / "physical" / "child").mkdir(parents=True)
    elsewhere.mkdir()
    if filename.startswith("alias"):
        (original / "alias").symlink_to("physical/child", target_is_directory=True)
        original_file = original / "physical" / "units.sqlite"
        other_file = original / "units.sqlite"
    else:
        original_file = original / filename
        other_file = elsewhere / filename
    for path in (original_file, other_file):
        _write_sqlite(path)
    monkeypatch.chdir(original)
    source = _reader("sqlite", filename)
    assert _sorted_weights(source.as_table()) == [22.0, 25.5, 27.25]
    for path, offset in ((original_file, 100), (other_file, 200)):
        with sqlite3.connect(path) as connection:
            _ = connection.execute("UPDATE records SET weight = weight + ?", (offset,))
    monkeypatch.chdir(elsewhere)
    assert source.check_for_new_data()
    assert _sorted_weights(source.as_table()) == [122.0, 125.5, 127.25]


def test_sqlite_source_rejects_bytes_paths() -> None:
    with pytest.raises(
        TypeError, match=r"Source paths must be str or os.PathLike\[str\]"
    ):
        _ = SQLiteTableSource(b"units.sqlite", "records")  # pyright: ignore[reportArgumentType]  # exercise runtime rejection


def test_resaving_a_job_keeps_the_unavailable_relative_delta_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = tmp_path / "original"
    elsewhere = tmp_path / "elsewhere"
    original.mkdir()
    elsewhere.mkdir()
    _write_delta(original / "units")
    monkeypatch.chdir(original)
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "db"))
    with job:
        _ = ratio.pod(_reader("delta", "./units"), label="ratio")
    saved = tmp_path / "job.json"
    job.save(saved)

    monkeypatch.chdir(elsewhere)
    loaded = PipelineJob.load(saved)
    assert loaded.unbound_sources == ["./units"]
    saved_again = tmp_path / "job-again.json"
    loaded.save(saved_again)
    config = json.loads(saved.read_text())["bindings"]["sources"]
    assert config["./units"]["delta_table_path"] == "./units"
    assert json.loads(saved_again.read_text())["bindings"]["sources"] == config


@pytest.mark.parametrize("kind", ["csv", "delta"])
def test_parent_after_symlink_follows_the_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    from deltalake import write_deltalake

    (tmp_path / "physical" / "child").mkdir(parents=True)
    (tmp_path / "alias").symlink_to("physical/child", target_is_directory=True)
    _write(kind, tmp_path / "units")
    physical = tmp_path / "physical" / "units"
    if kind == "csv":
        _ = _write_csv(physical, OTHER_ROWS)
    else:
        write_deltalake(str(physical), pa.Table.from_pylist(OTHER_ROWS, schema=SCHEMA))
    monkeypatch.chdir(tmp_path)
    source = _reader(kind, "alias/../units")
    assert _sorted_weights(source.as_table()) == [9.0, 40.0]
    if kind == "csv":
        _ = _write_csv(physical, ROWS)
    else:
        write_deltalake(
            str(physical), pa.Table.from_pylist(ROWS, schema=SCHEMA), mode="overwrite"
        )
    monkeypatch.chdir(tmp_path / "physical")
    assert source.check_for_new_data()
    assert _sorted_weights(source.as_table()) == [22.0, 25.5, 27.25]


@pytest.mark.parametrize("kind", ["csv", "delta"])
def test_file_source_refresh_schema_error_names_the_supplied_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    from deltalake import write_deltalake

    monkeypatch.chdir(tmp_path)
    path = tmp_path / "units"
    source: CSVSource | DeltaTableSource
    if kind == "csv":
        _ = path.write_text("weight\n25\n")
        source = CSVSource("./units")
        _ = path.write_text("other\n250\n")
    else:
        write_deltalake(str(path), pa.table({"weight": [25]}))
        source = DeltaTableSource("./units")
        write_deltalake(
            str(path),
            pa.table({"other": [250]}),
            mode="overwrite",
            schema_mode="overwrite",
        )
    with pytest.raises(ValueError, match=r" '\./units' changed its columns"):
        source.check_for_new_data()


@pytest.mark.parametrize("kind", ["csv", "delta"])
def test_relative_source_cache_rebinding_pending_developer_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    from deltalake import write_deltalake

    original = tmp_path / "original"
    elsewhere = tmp_path / "elsewhere"
    original.mkdir()
    elsewhere.mkdir()
    _write(kind, original / "units")
    if kind == "csv":
        _ = _write_csv(elsewhere / "units", OTHER_ROWS)
    else:
        write_deltalake(
            str(elsewhere / "units"), pa.Table.from_pylist(OTHER_ROWS, schema=SCHEMA)
        )
    monkeypatch.chdir(original)
    cache = CachedSource(
        _reader(kind, "units"), cache_database=DeltaTableDatabase(tmp_path / "cache")
    )
    assert cache.as_table().num_rows == len(ROWS)
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "db"))
    with job:
        _ = ratio.pod(cache, label="ratio")
    saved = tmp_path / "job.json"
    job.save(saved)

    def restored_weights() -> list[float]:
        restored = PipelineJob.load(saved).sources["units"]
        assert isinstance(restored, CachedSource)
        assert restored.cache_path == cache.cache_path
        return _sorted_weights(restored.as_table())

    # Record current behavior pending a decision about relative source paths in caches.
    monkeypatch.chdir(elsewhere)
    assert restored_weights() == [9.0, 22.0, 25.5, 27.25, 40.0]
    monkeypatch.chdir(original)
    assert restored_weights() == [9.0, 22.0, 25.5, 27.25, 40.0]


def test_delta_source_passes_remote_uri_to_deltalake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import deltalake

    path = tmp_path / "units"
    _write_delta(path)
    table = deltalake.DeltaTable(str(path))
    uri = "s3://bucket/units"

    def open_table(location: str) -> deltalake.DeltaTable:
        assert location == uri
        return table

    monkeypatch.setattr(deltalake, "DeltaTable", open_table)
    source = DeltaTableSource(uri)
    assert source.source_id == f"delta_table_v1:{uri}"
    assert source.to_config()["delta_table_path"] == uri
    assert source.as_table().num_rows == len(ROWS)


class TestSecondReview1ExplicitNameOverAnUnusableLocation:
    """A given name never needs a default from the location (``::`` cannot be one)."""

    @pytest.mark.parametrize("kind", READERS)
    def test_a_named_source_at_a_location_with_the_separator_round_trips(
        self, tmp_path, kind
    ):
        location = tmp_path / f"bad::{kind}"
        _write(kind, location)
        source = _reader(kind, str(location), source_id="units")
        rebuilt = _rebuild(source)
        assert (
            rebuilt.source_id
            == source.source_id
            == f"{type(source).SOURCE_KIND}_v1:units"
        )
        assert rebuilt.content_hash() == source.content_hash()
        assert rebuilt.as_table().num_rows == len(ROWS)

    @pytest.mark.parametrize("kind", READERS)
    def test_without_a_name_the_location_is_refused(self, tmp_path, kind):
        location = tmp_path / f"bad::{kind}"
        _write(kind, location)
        with pytest.raises(ValueError, match="::"):
            _reader(kind, str(location))

    def test_a_saved_job_loads_the_named_source(self, tmp_path):
        location = _write_csv(tmp_path / "bad::location.csv", ROWS)
        job = PipelineJob(name="sep", database=DeltaTableDatabase(tmp_path / "db"))
        with job:
            ratio.pod(_csv(location, source_id="units"), label="ratio")
        job.run()
        saved = tmp_path / "job.json"
        job.save(saved)
        loaded = PipelineJob.load(saved, database=DeltaTableDatabase(tmp_path / "db"))
        source = loaded.sources["units"]
        assert isinstance(source, CSVSource)
        assert source.source_id == "csv_v1:units"


class TestSecondReview2GivenOrDefaultIsSaved:
    """Whether the name was given is saved and read back, never guessed."""

    @pytest.mark.parametrize(
        "kind, file_name",
        [
            ("csv", "csv_v1:input.csv"),
            ("delta", "delta_table_v1:input"),
            ("sqlite", "csv_v1:default.db"),
        ],
    )
    def test_a_kind_looking_default_is_quiet_on_construction_and_reload(
        self, tmp_path, monkeypatch, kind, file_name
    ):
        monkeypatch.chdir(tmp_path)
        _write(kind, tmp_path / file_name)
        with warnings.catch_warnings():
            warnings.simplefilter("error", SourceNameWarning)
            source = _reader(kind, file_name)  # a relative location
            config = source.to_config()
            rebuilt = type(source).from_config(config)
            again = type(source).from_config(rebuilt.to_config())
        assert config["source_name_given"] is False
        assert rebuilt.source_id == again.source_id == source.source_id
        assert again.to_config()["source_name_given"] is False

    @pytest.mark.parametrize("kind", READERS)
    def test_a_given_name_equal_to_the_default_stays_given(
        self, tmp_path, monkeypatch, kind
    ):
        monkeypatch.chdir(tmp_path)
        file_name = {"csv": "csv_v1:input.csv", "delta": "delta_table_v1:input"}.get(
            kind, "csv_v1:default.db"
        )
        _write(kind, tmp_path / file_name)
        default = _reader(kind, file_name).source_name
        with pytest.warns(SourceNameWarning):
            source = _reader(kind, file_name, source_id=default)
        config = source.to_config()
        assert config["source_name_given"] is True
        with pytest.warns(SourceNameWarning):
            rebuilt = type(source).from_config(config)
        assert rebuilt.source_id == source.source_id
        assert rebuilt.to_config()["source_name_given"] is True


class TestSecondReview3Claims:
    def test_one_name_with_two_schemas_names_both_full_ids(self):
        from orcapod.errors import InconsistentSourceError
        from orcapod.sources import DictSource

        arrow = ArrowTableSource(
            pa.Table.from_pylist(ROWS, schema=SCHEMA),
            tag_columns=KEY,
            record_id_columns=KEY,
            tag_only_columns=True,
            source_id="units",
        )
        other = DictSource(
            [{"subject": "s1", "session": 1, "weight": 1.5, "label": "a"}],
            tag_columns=KEY,
            record_id_columns=KEY,
            tag_only_columns=True,
            source_id="units",
        )

        @function_pod(output_keys="tagged")
        def tagged(weight: float | None) -> float | None:
            return weight

        job = PipelineJob(name="schemas", database=InMemoryArrowDatabase())
        with pytest.raises(InconsistentSourceError) as caught:
            with job:
                tagged.pod(arrow, label="first")
                tagged.pod(other, label="second")
        assert "'arrow_table_v1:units'" in str(caught.value)
        assert "'dict_v1:units'" in str(caught.value)

    def test_a_cache_of_another_id_reads_no_row_of_the_source(
        self, tmp_path, csv_path, monkeypatch
    ):
        cached = CachedSource(
            _csv(csv_path, source_id="subjects"),
            cache_database=DeltaTableDatabase(tmp_path / "cache"),
        )
        cached.as_table()
        config = cached.to_config()
        monkeypatch.setattr(CSVSource, "SOURCE_VERSION", 2)
        with pytest.warns(SourceVersionWarning):
            reloaded = CachedSource.from_config(config)

        def no_read(*args, **kwargs):
            raise AssertionError("the wrapped source's rows were read")

        monkeypatch.setattr(reloaded.cached_source, "as_table", no_read)
        with pytest.raises(CachedSourceIdMismatchError):
            reloaded.as_table()


# ---------------------------------------------------------------------------
# Third review of PR 525 (head b0f00891)
# ---------------------------------------------------------------------------


class TestThirdReview:
    def test_a_check_refuses_a_cache_of_another_id_before_checking_the_source(
        self, tmp_path, csv_path, monkeypatch
    ):
        cached = CachedSource(
            _csv(csv_path, source_id="subjects"),
            cache_database=DeltaTableDatabase(tmp_path / "cache"),
        )
        cached.as_table()
        config = cached.to_config()
        monkeypatch.setattr(CSVSource, "SOURCE_VERSION", 2)
        with pytest.warns(SourceVersionWarning):
            reloaded = CachedSource.from_config(config)
        # The file changes, so a check of the wrapped source would read it.
        _write_csv(csv_path, OTHER_ROWS)

        def no_refresh(*args, **kwargs):
            raise AssertionError("the wrapped source was checked")

        monkeypatch.setattr(reloaded.cached_source, "check_for_new_data", no_refresh)
        monkeypatch.setattr(reloaded.cached_source, "as_table", no_refresh)
        with pytest.raises(CachedSourceIdMismatchError):
            reloaded.check_for_new_data()

    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_derived_source_configs_say_whether_the_name_was_given(
        self, tmp_path, csv_path, tags
    ):
        from orcapod.sources import DerivedSource

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ProvisionalHashWarning)
            job = PipelineJob(name="calc", database=DeltaTableDatabase(tmp_path / "db"))
            with job:
                ratio.pod(_csv(csv_path, tags, source_id="subjects"), label="calc")
            job.run()
            plain = job.nodes["calc"].as_source().to_config()
            assert plain["source_name"] is None
            assert plain["source_name_given"] is False

            named = DerivedSource(
                _csv(csv_path, tags, source_id="subjects"),
                tags_as_data={"session": "session_copy", "subject": "subject_copy"},
                source_id="copied",
            )
            config = named.to_config()
            assert config["source_name"] == "copied"
            assert config["source_name_given"] is True
            rebuilt = DerivedSource.from_config(config)
            assert rebuilt.source_id == named.source_id == "derived_v1:copied"
            assert rebuilt.to_config()["source_name_given"] is True


# ---------------------------------------------------------------------------
# CodeRabbit review of PR 525 (head 5e6ad527)
# ---------------------------------------------------------------------------


class TestCodeRabbitReview:
    def _legacy_tags_as_data_config(
        self, csv_path: Path, tags: list[str]
    ) -> dict[str, object]:
        from orcapod.sources import DerivedSource

        config = DerivedSource(
            _csv(csv_path, tags, source_id="subjects"),
            tags_as_data={"session": "session_copy", "subject": "subject_copy"},
            source_id="copied",
        ).to_config()
        config["source_id"] = config.pop("source_name")
        del config["source_name_given"]
        del config["source_version"]
        return config

    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_a_legacy_tags_as_data_config_is_refused_as_legacy(self, csv_path, tags):
        from orcapod.sources import DerivedSource

        config = self._legacy_tags_as_data_config(csv_path, tags)
        with pytest.raises(ValueError, match="saved before") as caught:
            DerivedSource.require_saved_source_id(config)
        assert "needs a source_id" not in str(caught.value)
        with pytest.raises(ValueError, match="saved before"):
            DerivedSource.from_config(config)
        nested = {"source_type": "cached", "inner_source": config}
        with pytest.raises(ValueError, match="saved before"):
            DerivedSource.require_saved_source_id(nested)

    def test_a_tags_as_data_config_without_a_name_still_needs_one(self, csv_path):
        from orcapod.sources import DerivedSource

        config = self._legacy_tags_as_data_config(csv_path, KEY)
        del config["source_id"]
        with pytest.raises(ValueError, match="needs a source_id"):
            DerivedSource.require_saved_source_id(config)

    @pytest.mark.parametrize("tags", TAG_ORDERS)
    def test_a_nested_cache_that_cannot_be_rebuilt_keeps_its_source_id(
        self, tmp_path, csv_path, tags
    ):
        inner = CachedSource(
            _csv(csv_path, tags, source_id="subjects"),
            cache_database=DeltaTableDatabase(tmp_path / "inner"),
        )
        outer = CachedSource(
            inner, cache_database=DeltaTableDatabase(tmp_path / "outer")
        )
        assert outer.as_table().num_rows == len(ROWS)
        config = outer.to_config()
        # The cache configs hold no identity (ENG-320); the wrapped source's does.
        assert "full_source_id" not in config["inner_source"]
        assert config["inner_source"]["inner_source"]["full_source_id"] == (
            "csv_v1:subjects"
        )
        # The inner cache cannot be rebuilt: a proxy stands in for it.
        config["inner_source"]["cache_database"] = {"type": "not_a_database"}
        reloaded = CachedSource.from_config(config)
        assert isinstance(reloaded.cached_source, SourceProxy)
        assert reloaded.source_id == outer.source_id == "csv_v1:subjects"
        table = reloaded.as_table(columns={"system_tags": True})
        assert table.num_rows == len(ROWS)
        assert _source_ids(table) == {"csv_v1:subjects"}
