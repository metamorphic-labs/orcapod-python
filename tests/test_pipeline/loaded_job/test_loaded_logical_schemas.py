"""Loaded operators validate the complete logical identifiers in saved schema text."""

from __future__ import annotations

import dataclasses
import json
import logging
from pathlib import Path
import subprocess
import sys
import uuid
from typing import Any

import numpy as np
import orcapod as op
import pytest

from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline.serialization import LoadStatus
from orcapod.sources import DictSource
from orcapod.logical_types.identifier import (
    LogicalTypeIdentifierError,
    logical_type_identifier,
)
from orcapod.contexts import get_default_context


@dataclasses.dataclass
class SavedValue:
    """Nominal class registered by constructing its function node."""

    value: int


@op.dataclass
class StructuralValue:
    """Structural class registered by constructing its function node."""

    value: int


def path_value(x: str) -> Path:
    """Return a path."""
    return Path(x)


def uuid_value(x: str) -> uuid.UUID:
    """Return a deterministic UUID."""
    return uuid.uuid5(uuid.NAMESPACE_URL, x)


def file_value(x: str) -> op.File:
    """Return an existing file."""
    return op.File(x)


def directory_value(x: str) -> op.Directory:
    """Return the containing directory."""
    return op.Directory(Path(x).parent)


# The schema registers bare ndarray, independently of any shape or dtype annotation.
def array_value(x: str) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]
    """Return a typed array."""
    return np.array([len(x)], dtype=np.int32)


def nominal_value(x: str) -> SavedValue:
    """Return a nominal user value."""
    return SavedValue(len(x))


def structural_value(x: str) -> StructuralValue:
    """Return a structural user value."""
    return StructuralValue(len(x))


def _save_logical_job(tmp_path: Path, function: Any, *, with_join: bool = True) -> Path:
    """Run and save a logical-output function followed by a Join."""
    source_file = tmp_path / "input"
    source_file.write_text("data")
    job = op.PipelineJob(
        name="logical", database=DeltaTableDatabase(tmp_path / "store")
    )
    with job:
        source = DictSource(
            [{"id": 1, "x": str(source_file)}],
            label="data_input",
            tag_columns=["id"],
            tag_only_columns=True,
        )
        result = op.function_pod(output_keys="value")(function).pod(
            source, label="value"
        )
        if with_join:
            other = DictSource(
                [{"id": 1, "other": 2}],
                tag_columns=["id"],
                tag_only_columns=True,
                label="join_metadata",
            )
            Join()(result, other, label="joined")
    job.run()
    path = tmp_path / "job.json"
    job.save(path)

    return path


@pytest.mark.parametrize(
    "function, expected_type",
    [
        (path_value, Path),
        (uuid_value, uuid.UUID),
        (file_value, op.File),
        (directory_value, op.Directory),
        (array_value, np.ndarray),
        (nominal_value, SavedValue),
        (structural_value, StructuralValue),
    ],
)
def test_registered_logical_output_keeps_join_available(
    tmp_path: Path,
    function: Any,
    expected_type: type,
) -> None:
    """A saved logical output can reconstruct its downstream Join normally."""
    path = _save_logical_job(tmp_path, function)
    descriptors = json.loads(path.read_text())["pipeline"]["nodes"].values()
    descriptor = next(d for d in descriptors if d["label"] == "value")
    logical = get_default_context().type_converter.get_logical_type(expected_type)
    assert logical is not None
    assert descriptor["output_schema"]["data"]["value"] == (
        f"extension<{logical_type_identifier(logical).text}>"
    )

    loaded = op.PipelineJob.load(path)
    assert loaded.nodes["value"].output_schema()[1]["value"] is expected_type
    # A loaded node is empty until it runs (ITL-1095): read its stored view.
    assert loaded.nodes["value"].as_source().as_table().num_rows == 1
    assert loaded.nodes["joined"].load_status == LoadStatus.FULL
    assert loaded.nodes["joined"].output_schema()[1]["value"] is expected_type


@pytest.mark.parametrize(
    "saved_type",
    [
        "extension<orcapod.path;7b2276657273696f6e223a3939397d>",
        "extension<orcapod.path;not-hex>",
        "extension<orcapod.path;7b2276657273696f6e223a317d",
        "extension<;7b2276657273696f6e223a317d>",
    ],
)
@pytest.mark.parametrize("with_join", [True, False])
def test_unreadable_saved_identifier_names_node_and_column(
    tmp_path: Path,
    saved_type: str,
    with_join: bool,
) -> None:
    """Corrupt or incompatible identifiers cannot produce an unavailable-operator fallback."""
    path = _save_logical_job(tmp_path, path_value, with_join=with_join)
    saved = json.loads(path.read_text())
    descriptor = next(
        d for d in saved["pipeline"]["nodes"].values() if d["label"] == "value"
    )
    descriptor["output_schema"]["data"]["value"] = saved_type
    path.write_text(json.dumps(saved))
    with pytest.raises(
        LogicalTypeIdentifierError, match="node 'value'.*Column 'value'"
    ):
        op.PipelineJob.load(path)


def test_name_only_saved_type_is_rejected(tmp_path: Path) -> None:
    """Name-only schema text cannot silently assume the current type version."""
    path = _save_logical_job(tmp_path, path_value)
    saved = json.loads(path.read_text())
    descriptor = next(
        d for d in saved["pipeline"]["nodes"].values() if d["label"] == "value"
    )
    descriptor["output_schema"]["data"]["value"] = (
        "extension<orcapod.path<_ArrowExt_orcapod_path>>"
    )
    path.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match="earlier version"):
        op.PipelineJob.load(path)


def test_unregistered_user_type_reports_node_and_column(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A class registered only in the writer leaves a reported operator stub on load."""
    script = tmp_path / "save_unknown.py"
    script.write_text("""from dataclasses import dataclass
from pathlib import Path
import sys
from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource
@dataclass
class WriterOnly:
    value: int
@function_pod(output_keys="payload")
def make_payload(x: int) -> WriterOnly:
    return WriterOnly(x)
root = Path(sys.argv[1])
job = PipelineJob(name="unknown", database=DeltaTableDatabase(root / "store"))
with job:
    source = DictSource([{"id": 1, "x": 2}], tag_columns=["id"], tag_only_columns=True, label="payload_input")
    result = make_payload.pod(source, label="producer")
    other = DictSource([{"id": 1, "other": 3}], tag_columns=["id"], tag_only_columns=True, label="join_metadata")
    Join()(result, other, label="joined")
job.run()
job.save(root / "job.json")
""")
    subprocess.run([sys.executable, str(script), str(tmp_path)], check=True)
    with caplog.at_level(logging.WARNING):
        loaded = op.PipelineJob.load(tmp_path / "job.json")
    assert loaded.nodes["joined"].load_status == LoadStatus.UNAVAILABLE
    assert "joined" in caplog.text
    assert "producer" in caplog.text
    assert "payload" in caplog.text
    with pytest.raises(ValueError, match="producer.*payload"):
        loaded.nodes["producer"].output_schema()


# A writer process whose function returns a class only it registers, beside narrow
# integer tags and data: the reader sees an unregistered logical type on a function node
# ("producer", read-only) and on the operator node below it ("joined", read-only).
_UNREGISTERED_WRITER = """from dataclasses import dataclass
from pathlib import Path
import sys
import pyarrow as pa
from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import ArrowTableSource


@dataclass
class WriterOnly:
    value: int


@function_pod(output_keys=("payload", "doubled"))
def make_payload(x: int) -> tuple[WriterOnly, int]:
    return WriterOnly(x), 2 * x


root = Path(sys.argv[1])
left = ArrowTableSource(
    pa.table({"id": pa.array([1, 2], pa.int32()), "k": pa.array([3, 4], pa.int16()),
              "x": pa.array([5, 6], pa.int64())}, schema=pa.schema([pa.field("id", pa.int32(), nullable=False), pa.field("k", pa.int16(), nullable=False), pa.field("x", pa.int64(), nullable=False)])),
    tag_columns=["id", "k"], tag_only_columns=True, source_id="left", label="left")
right = ArrowTableSource(
    pa.table({"id": pa.array([1, 2], pa.int32()), "k": pa.array([3, 4], pa.int16()),
              "other": pa.array([7, None], pa.int16())}),
    tag_columns=["id", "k"], tag_only_columns=True, source_id="right", label="right")
job = PipelineJob(name="unknown", database=DeltaTableDatabase(root / "store"))
with job:
    produced = make_payload.pod(left, label="producer")
    Join()(produced, right, label="joined")
job.run()
job.save(root / "job.json")
job.pipeline.save(root / "pipeline.json")
"""


@pytest.mark.parametrize("save", ["job", "pipeline"])
def test_a_loaded_file_with_an_unregistered_type_saves_again_unchanged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, save: str
) -> None:
    """Load accepts the file with a warning; saving it again writes every node's entry
    as it was (its retained type texts and its saved config), and the reloaded file
    reads the same. The type is still refused where it is used (CodeRabbit on PR
    454)."""
    script = tmp_path / "writer.py"
    script.write_text(_UNREGISTERED_WRITER)
    subprocess.run([sys.executable, str(script), str(tmp_path)], check=True)
    original = tmp_path / f"{save}.json"
    again = tmp_path / f"{save}_again.json"
    with caplog.at_level(logging.WARNING):
        if save == "job":
            loaded = op.PipelineJob.load(original)
            loaded.save(again)
        else:
            loaded = op.Pipeline.load(original)
            loaded.save(again)
    assert "payload" in caplog.text  # load warned about the unregistered type

    def entries(path: Path) -> tuple[Any, Any]:
        data = json.loads(path.read_text())
        pipeline = data["pipeline"] if save == "job" else data
        return pipeline["nodes"], pipeline["edges"]

    assert entries(again) == entries(original)
    labels = {d["label"]: d for d in entries(original)[0].values()}
    assert labels["joined"]["output_schema"]["tag"] == {"id": "int32", "k": "int16"}
    assert labels["joined"]["output_schema"]["data"]["other"] == "int16"
    assert (
        "extension<__main__.WriterOnly@"
        in labels["producer"]["output_schema"]["data"]["payload"]
    )
    # The file saved again loads as the original did, and saves the same again.
    reloaded = op.PipelineJob.load(again) if save == "job" else op.Pipeline.load(again)
    third = tmp_path / f"{save}_third.json"
    reloaded.save(third)
    assert entries(third) == entries(original)

    def statuses(pipeline: Any) -> dict[str, Any]:
        return {
            label: node.load_status
            for label, node in pipeline.nodes.items()
            if hasattr(node, "load_status")
        }

    assert statuses(reloaded) == statuses(loaded)
    assert statuses(loaded)["joined"] == LoadStatus.UNAVAILABLE
    assert statuses(loaded)["producer"] != LoadStatus.FULL
    # Where the type is used, it is refused, as before.
    with pytest.raises(ValueError, match="producer.*payload"):
        reloaded.nodes["producer"].output_schema()


@pytest.mark.parametrize("fallback_schema", [False, True])
def test_source_schema_uses_saved_context_converter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fallback_schema: bool
) -> None:
    """Source schema restoration uses the resolved context, including fallback schemas."""
    from orcapod.contexts import create_registry
    from orcapod.core.nodes import SourceNode
    from orcapod.pipeline import Pipeline
    from orcapod.types import Schema

    @op.dataclass
    class ContextOnly:
        """A type registered only in this isolated data context."""

        value: int

    context = create_registry().get_context()
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    assert isinstance(context.type_converter, UniversalTypeConverter)
    context.type_converter.register_python_class(ContextOnly)
    left = SourceNode(
        "left",
        tag_schema=Schema({"key": ContextOnly}),
        data_schema=Schema({"payload": ContextOnly}),
        data_context=context,
    )
    right = SourceNode(
        "right",
        tag_schema=Schema({"key": ContextOnly}),
        data_schema=Schema({"other": int}),
        data_context=context,
    )
    pipeline = Pipeline(name="context_source")
    with pipeline:
        Join()(left, right, label="joined")
    path = tmp_path / "pipeline.json"
    pipeline.save(path)
    if fallback_schema:
        saved = json.loads(path.read_text())
        for descriptor in saved["nodes"].values():
            if descriptor["node_type"] == "source":
                descriptor["source_config"].pop("tag_schema", None)
                descriptor["source_config"].pop("data_schema", None)
        path.write_text(json.dumps(saved))
    # This test targets routing from the saved-context resolver into schema restoration.
    monkeypatch.setattr(
        "orcapod.pipeline.graph.resolve_saved_context", lambda key: context
    )
    loaded = Pipeline.load(path)
    tag_schema, data_schema = loaded.nodes["left"].output_schema()
    assert tag_schema["key"] is ContextOnly
    assert data_schema["payload"] is ContextOnly
    assert loaded.nodes["joined"].load_status == LoadStatus.FULL
