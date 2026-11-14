"""conftest.py for test_databases.

Installs a lightweight mock psycopg module into sys.modules so that:
  - ``patch("psycopg.connect")`` works even when libpq is not installed.
  - The LazyModule("psycopg") in postgresql_connector.py resolves without
    trying to load the real psycopg (which requires libpq).

Also provides the S3 fixture for the S3 integration tests: a moto server in a
subprocess, or a real endpoint from MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import tempfile
import time
import types
import uuid
from unittest.mock import MagicMock

import pytest

# Only install the stub if psycopg is not already importable.
try:
    import psycopg  # noqa: F401
except ImportError:
    _psycopg_stub = types.ModuleType("psycopg")
    _psycopg_stub.connect = MagicMock()  # type: ignore[attr-defined]
    sys.modules["psycopg"] = _psycopg_stub


# ---------------------------------------------------------------------------
# S3 fixtures
# ---------------------------------------------------------------------------

_S3_ACCESS_KEY = "testing"
_S3_SECRET_KEY = "testing"
_S3_BUCKET = "test-bucket"
_MOTO_START_TIMEOUT_SECONDS = 60.0


def _free_localhost_port() -> int:
    """Return a TCP port on 127.0.0.1 that is free right now."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _make_bucket(endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
    """Create ``bucket`` at an S3 endpoint if it does not exist (through s3fs)."""
    import s3fs

    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint, "region_name": "us-east-1"},
    )
    if not fs.exists(bucket):
        fs.call_s3("create_bucket", Bucket=bucket)
        fs.invalidate_cache()


@contextlib.contextmanager
def _moto_server():
    """Run a moto S3 server in a subprocess and yield its endpoint URL.

    A subprocess, not ``ThreadedMotoServer``: deltalake holds the GIL while it works
    on a table, so a server thread in the test process cannot answer it and an append
    hangs. Raises ``RuntimeError`` with the server's output if it does not come up.
    """
    port = _free_localhost_port()
    endpoint = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": _S3_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": _S3_SECRET_KEY,
    }
    with tempfile.TemporaryFile() as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        try:
            deadline = time.monotonic() + _MOTO_START_TIMEOUT_SECONDS
            while True:
                if proc.poll() is not None:
                    break
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=1):
                        pass
                except OSError:
                    if time.monotonic() > deadline:
                        break
                    time.sleep(0.1)
                    continue
                yield endpoint
                return
            log.seek(0)
            output = log.read().decode(errors="replace")
            raise RuntimeError(
                f"The moto S3 server did not start on {endpoint}:\n{output}"
            )
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()


@pytest.fixture(scope="session")
def s3_server():
    """
    Provide S3 connection info as a dict with keys:
        endpoint, access_key, secret_key, bucket

    Default: a moto server started in a subprocess on a free localhost port (nothing
    to install beyond the dev group, no Docker). Override: a real S3-compatible
    endpoint from MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY.
    A server that cannot start makes the test ERROR with the reason; never a skip.
    """
    endpoint = os.environ.get("MINIO_ENDPOINT")
    if endpoint:
        access_key = os.environ["MINIO_ACCESS_KEY"]
        secret_key = os.environ["MINIO_SECRET_KEY"]
        _make_bucket(endpoint, access_key, secret_key, _S3_BUCKET)
        yield {
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket": _S3_BUCKET,
        }
        return
    with _moto_server() as moto_endpoint:
        _make_bucket(moto_endpoint, _S3_ACCESS_KEY, _S3_SECRET_KEY, _S3_BUCKET)
        yield {
            "endpoint": moto_endpoint,
            "access_key": _S3_ACCESS_KEY,
            "secret_key": _S3_SECRET_KEY,
            "bucket": _S3_BUCKET,
        }


@pytest.fixture
def s3_base_uri(s3_server):
    """Unique S3 URI prefix per test — prevents cross-test table pollution."""
    prefix = f"tables-{uuid.uuid4().hex[:8]}"
    return f"s3://{s3_server['bucket']}/{prefix}"


@pytest.fixture
def s3_storage_options(s3_server):
    """Full set of storage_options required for an S3 emulator via deltalake."""
    return {
        "AWS_ENDPOINT_URL": s3_server["endpoint"],
        "AWS_ACCESS_KEY_ID": s3_server["access_key"],
        "AWS_SECRET_ACCESS_KEY": s3_server["secret_key"],
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
