"""Arrow's worker threads never destroy a logical type's extension type (ITL-1422).

The C++ destructor of an extension type written in Python takes the GIL. A worker that runs
it while the interpreter is finalizing aborts the process on Linux (Python 3.11 and 3.12)
or leaves it hanging. A type that has one instance, which is never destroyed, cannot be
destroyed by a worker. The race itself is rare, so these tests check the property.
"""

from __future__ import annotations

import gc
import subprocess
import sys
import textwrap
import weakref
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import pytest

import orcapod.contexts as contexts
from orcapod.logical_types import make_arrow_extension_type
from orcapod.logical_types.ignore_list import _ExtensionFamily
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


def _registered_types() -> dict[str, pa.ExtensionType]:
    """The extension type of every logical type of the default data context."""
    converter = contexts.get_default_context().type_converter
    assert isinstance(converter, UniversalTypeConverter)
    registry = converter._logical_type_registry
    assert registry is not None
    # The container types are registered when first used.
    for hint in (set[bytes], set[int], list[Path], set[Path]):
        converter.python_type_to_arrow_type(hint)
    return {
        name: logical_type.get_arrow_extension_type()
        for name, logical_type in registry._by_logical_name.items()
    }


def test_a_type_is_one_instance_however_it_is_made() -> None:
    """Calling and deserializing give the one instance, with its own storage."""
    extension_class = make_arrow_extension_type(
        "test.lifetime.one", pa.large_string(), b"1", also_reads=(b"0",)
    )
    one = extension_class()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert extension_class() is one  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    for stored, metadata in [
        (pa.large_string(), b"1"),
        (pa.string(), b"1"),
        (pa.large_string(), b"0"),
    ]:
        assert extension_class.__arrow_ext_deserialize__(stored, metadata) is one
    assert one.storage_type == pa.large_string()
    assert one.extension_name == "test.lifetime.one"
    assert one.__arrow_ext_serialize__() == b"1"


def test_two_classes_do_not_share_an_instance() -> None:
    """Each made class has its own instance, also under one extension name."""
    first = make_arrow_extension_type("test.lifetime.two", pa.int8(), b"1")()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    second = make_arrow_extension_type("test.lifetime.two", pa.int64(), b"2")()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert first is not second
    assert (first.storage_type, second.storage_type) == (pa.int8(), pa.int64())
    assert (first.__arrow_ext_serialize__(), second.__arrow_ext_serialize__()) == (
        b"1",
        b"2",
    )


@pytest.mark.parametrize("kind", ["made", "ignore_list"])
def test_a_type_outlives_every_python_reference(kind: str) -> None:
    """Dropping every reference Python holds does not destroy the type."""
    if kind == "made":
        extension = make_arrow_extension_type(
            "test.lifetime.kept", pa.large_string(), b"1"
        )()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    else:
        # The family is made apart from the process-wide one, so it can be dropped.
        family = _ExtensionFamily(
            "test.lifetime.family", pa.large_string(), 1, frozenset({"label"})
        )
        extension = family.arrow_type(("label",))
        del family
    alive = weakref.ref(extension)
    del extension
    gc.collect()
    assert alive() is not None


def test_a_scan_reads_the_shared_instances(tmp_path: Path) -> None:
    """A threaded scan of every registered type gives the instances already held."""
    types = {
        name: t
        for name, t in _registered_types().items()
        # Parquet has no form for a struct without fields, a null or a dictionary.
        if not pa.types.is_null(t.storage_type)
    }
    arrays = [
        pa.ExtensionArray.from_storage(t, pa.array([None, None], t.storage_type))
        for t in types.values()
    ]
    table = pa.table(arrays, names=list(types))
    pq.write_table(table, tmp_path / "all.parquet")
    read = pa_ds.dataset(tmp_path / "all.parquet").to_table(use_threads=True)
    for name, extension in types.items():
        assert read.schema.field(name).type is extension, name
    assert read.num_rows == 2


_CHILD = """
    import sys
    from pathlib import Path

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource

    @function_pod(output_keys="out")
    def produce(i: int) -> set[bytes]:
        return {b"y", b"x"}

    work, mode = sys.argv[1:]
    database = DeltaTableDatabase(work + "/store")
    if mode == "save":
        source = DictSource(
            [{"k": 0, "i": 0}], tag_columns=["k"], tag_only_columns=True, source_id="one"
        )
        job = PipelineJob(name="exit", database=database)
        with job:
            produce.pod(source, label="produce")
        job.run()
        job.save(work + "/job.json")
    else:
        job = PipelineJob.load(work + "/job.json", database=database)
    _, data = next(iter(job.nodes["produce"].as_source().iter_data()))
    assert data.as_dict() == {"out": {b"y", b"x"}}
    print("read", flush=True)
"""


def test_a_process_that_read_logical_types_exits_cleanly(tmp_path: Path) -> None:
    """The saved and loaded job's processes read their rows and exit with code 0."""
    for mode in ["save", *["load"] * 5]:
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(_CHILD), str(tmp_path), mode],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, result.stderr[-3000:]
        assert result.stdout.splitlines()[-1] == "read"
