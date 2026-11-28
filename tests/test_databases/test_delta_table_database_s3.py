"""
S3 integration tests for DeltaTableDatabase.

The S3 endpoint comes from the ``s3_server`` fixture in ``conftest.py``: a moto
server started as a subprocess on a free localhost port (no Docker, nothing to
install beyond the dev group). Setting ``MINIO_ENDPOINT``, ``MINIO_ACCESS_KEY`` and
``MINIO_SECRET_KEY`` replaces it with a real S3-compatible endpoint (for example a
MinIO instance you run yourself); the fixture then creates the test bucket there.

These tests never skip for a missing server: if the moto server cannot start, the
tests ERROR with the reason.

Run with:  uv run pytest tests/test_databases/test_delta_table_database_s3.py -v
"""

import json

import deltalake
import pyarrow as pa
import pytest
import s3fs
from upath import UPath

from orcapod.databases import DeltaTableDatabase
from orcapod.databases import delta_lake_databases
from orcapod.errors import OrcapodDatabaseReadError

pytestmark = pytest.mark.s3


# ---------------------------------------------------------------------------
# Core S3 operations
# ---------------------------------------------------------------------------


def test_s3_add_record_and_retrieve(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    record = pa.table({"x": [1], "y": ["hello"]})
    db.add_record(("run1",), b"rec-001", record, flush=True)

    result = db.get_record_by_id(("run1",), b"rec-001")
    assert result is not None
    assert result["x"][0].as_py() == 1


def test_s3_add_records_batch(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    table = pa.table({"id": ["a", "b", "c"], "score": [1.0, 2.0, 3.0]})
    db.add_records(("exp",), table, record_id_column="id", flush=True)

    result = db.get_all_records(("exp",))
    assert result is not None
    assert result.num_rows == 3


def test_s3_flush_and_read(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    for i in range(3):
        db.add_record(("src",), f"r{i}".encode(), pa.table({"n": [i]}))
    db.flush()

    result = db.get_all_records(("src",), retrieve_pending=False)
    assert result is not None
    assert result.num_rows == 3


@pytest.mark.parametrize(
    "path", [("table",), ("group", "table")], ids=["top-level", "nested"]
)
@pytest.mark.parametrize(
    "partitions",
    [None, ["tag1", "tag2"], ["tag2", "tag1"]],
    ids=["flat", "partitioned", "reordered-partitions"],
)
def test_s3_table_with_missing_log_is_refused(
    s3_base_uri: str,
    s3_storage_options: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    path: tuple[str, ...],
    partitions: list[str] | None,
) -> None:
    """Cloud reads, metadata and flushes refuse a missing log and preserve the objects."""
    monkeypatch.setattr(delta_lake_databases, "sleep", lambda _delay: None)
    uri = "/".join((s3_base_uri,) + path)
    data = pa.table(
        {
            "y": [2],
            "tag1": ["a"],
            DeltaTableDatabase.RECORD_ID_COLUMN: [b"a"],
            "tag2": ["b"],
            "x": [1],
        }
    )
    if partitions == ["tag2", "tag1"]:
        data = data.select(
            ["x", "tag2", DeltaTableDatabase.RECORD_ID_COLUMN, "tag1", "y"]
        )
    deltalake.write_deltalake(
        uri, data, partition_by=partitions, storage_options=s3_storage_options
    )

    fs = s3fs.S3FileSystem(
        key=s3_storage_options["AWS_ACCESS_KEY_ID"],
        secret=s3_storage_options["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"endpoint_url": s3_storage_options["AWS_ENDPOINT_URL"]},
        use_listings_cache=False,
    )
    fs.rm(f"{uri}/_delta_log", recursive=True)
    before = {name: fs.cat_file(name) for name in fs.find(uri)}
    assert len(before) == 1 and next(iter(before)).endswith(".parquet")

    fresh = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    for operation in (
        lambda: fresh.get_all_records(path),
        lambda: fresh.get_table_schema(path),
        lambda: fresh.table_exists(path),
        lambda: fresh.add_record(path, b"b", pa.table({"v": [2]}), flush=True),
    ):
        with pytest.raises(OrcapodDatabaseReadError, match="_delta_log") as info:
            _ = operation()
        assert info.value.table == uri
        assert info.value.record_path == path
    assert {name: fs.cat_file(name) for name in fs.find(uri)} == before


def test_s3_non_hive_orphan_is_refused_on_first_read_and_flush(
    s3_base_uri: str,
    s3_storage_options: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Delta storage backend finds data at arbitrary depth after a miss."""
    monkeypatch.setattr(delta_lake_databases, "sleep", lambda _delay: None)
    path = ("table",)
    uri = f"{s3_base_uri}/table"
    writer_with_cached_miss = DeltaTableDatabase(
        s3_base_uri, storage_options=s3_storage_options
    )
    assert writer_with_cached_miss.get_all_records(path) is None
    writer = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    writer.add_record(path, b"a", pa.table({"x": [1]}), flush=True)

    fs = s3fs.S3FileSystem(
        key=s3_storage_options["AWS_ACCESS_KEY_ID"],
        secret=s3_storage_options["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"endpoint_url": s3_storage_options["AWS_ENDPOINT_URL"]},
        use_listings_cache=False,
    )
    log = f"{uri}/_delta_log/00000000000000000000.json"
    actions = [json.loads(line) for line in fs.cat_file(log).splitlines()]
    for action in actions:
        if "add" in action:
            original = action["add"]["path"]
            moved = f"files/bucket/{original}"
            fs.mv(f"{uri}/{original}", f"{uri}/{moved}")
            action["add"]["path"] = moved
    fs.pipe_file(log, "".join(json.dumps(action) + "\n" for action in actions).encode())
    fresh = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    assert fresh.list_table_paths() == [path]
    assert fresh.at(*path, "files").list_table_paths() == []
    fs.rm(f"{uri}/_delta_log", recursive=True)
    before = {name: fs.cat_file(name) for name in fs.find(uri)}
    assert len(before) == 1

    for operation in (
        lambda: fresh.get_all_records(path),
        lambda: writer_with_cached_miss.add_record(
            path, b"b", pa.table({"x": [2]}), flush=True
        ),
    ):
        with pytest.raises(OrcapodDatabaseReadError, match="_delta_log") as info:
            operation()
        assert info.value.record_path == path
    assert {name: fs.cat_file(name) for name in fs.find(uri)} == before


def test_s3_partition_path_accepts_independent_child(
    s3_base_uri: str,
    s3_storage_options: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Objects registered in a parent's log are not orphaned at its partition."""
    monkeypatch.setattr(delta_lake_databases, "sleep", lambda _delay: None)
    path = ("parent",)
    partition = (*path, "tag1=a", "tag2=b")
    data = pa.table(
        {
            DeltaTableDatabase.RECORD_ID_COLUMN: [b"a"],
            "tag1": ["a"],
            "tag2": ["b"],
            "x": [1],
        }
    )
    deltalake.write_deltalake(
        f"{s3_base_uri}/parent",
        data,
        partition_by=["tag1", "tag2"],
        storage_options=s3_storage_options,
    )
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    assert not db.table_exists(partition)
    assert db.get_all_records(partition) is None
    assert db.get_table_schema(partition) is None
    db.add_record(partition, b"b", pa.table({"x": [2]}), flush=True)
    fresh = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    child_rows = fresh.get_all_records(partition)
    assert child_rows is not None and child_rows.column("x").to_pylist() == [2]
    parent_rows = fresh.get_all_records(path)
    assert parent_rows is not None and parent_rows.column("x").to_pylist() == [1]


def test_s3_skip_duplicates(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    record = pa.table({"v": [1]})
    db.add_record(("src",), b"dup", record, flush=True)
    db.add_record(("src",), b"dup", record, skip_duplicates=True, flush=True)

    result = db.get_all_records(("src",))
    assert result is not None
    assert result.num_rows == 1


# ---------------------------------------------------------------------------
# UPath credential passthrough
# ---------------------------------------------------------------------------


def test_s3_upath_credentials(s3_base_uri, s3_server):
    """
    DeltaTableDatabase accepts a UPath with embedded S3 credentials (key/secret/endpoint_url).
    These are auto-translated from fsspec format to AWS_* keys.

    AWS_REGION, AWS_ALLOW_HTTP, and AWS_S3_ALLOW_UNSAFE_RENAME cannot be embedded
    in UPath kwargs (they have no fsspec equivalent), so they are passed via the
    explicit storage_options argument. Explicit options are merged on top of
    UPath-derived ones — neither alone is sufficient for S3 emulators.
    """
    bucket = s3_server["bucket"]
    prefix = s3_base_uri.split(f"s3://{bucket}/")[1]

    upath = UPath(
        f"s3://{bucket}/{prefix}",
        key=s3_server["access_key"],
        secret=s3_server["secret_key"],
        endpoint_url=s3_server["endpoint"],
    )
    db = DeltaTableDatabase(
        upath,
        storage_options={
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        },
    )
    record = pa.table({"k": ["hello"]})
    db.add_record(("t",), b"r1", record, flush=True)
    result = db.get_record_by_id(("t",), b"r1")
    assert result is not None
    assert result["k"][0].as_py() == "hello"


# ---------------------------------------------------------------------------
# Cloud-specific behaviour
# ---------------------------------------------------------------------------


def test_s3_list_sources_raises(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    with pytest.raises(NotImplementedError, match="not supported for cloud"):
        db.list_sources()


def test_s3_create_base_path_ignored(s3_base_uri, s3_storage_options):
    """create_base_path=False must not raise for cloud URIs."""
    db = DeltaTableDatabase(
        s3_base_uri,
        storage_options=s3_storage_options,
        create_base_path=False,
    )
    assert db is not None
