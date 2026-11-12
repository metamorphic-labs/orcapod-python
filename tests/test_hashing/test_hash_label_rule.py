"""The hash label rule (ITL-945; ITL-938 design, section 5.6).

1. A label names the algorithm that produced a digest; its format is ``<algorithm>_v<N>``.
   The file content hash keeps ``sha256``.
2. A label never enters a hash computation: renaming a label changes no digest.
3. A hash inside a name (path component, function uri, source id, slot name) is the bare
   hex digest.
4. A hash stored as a value carries its label, including the row keys of operator-cache
   and cached-source tables.
"""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pyarrow as pa
import pytest

import orcapod
import orcapod as op
from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.sources import CachedSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hashing import directory_hashers
from orcapod.hashing.file_hashers import FileHasher
from orcapod.hashing.semantic_hashing import polars_expr
from orcapod.hashing.semantic_hashing.builtin_handlers import NumpyArrayHandler
from orcapod.operators import Join
from orcapod.sources import DictSource
from orcapod.types import CacheMode, ContentHash
from tests.test_hashing.golden import cases

LABEL_FORMAT = re.compile(r"[a-z][a-z0-9_]*_v[1-9][0-9]*")
BARE_SHA256 = re.compile(r"[0-9a-f]{64}")
# A starfix digest: 3 bytes of hash format number, then SHA-256.
ARROW_DIGEST_BYTES = 35


@function_pod(output_keys="doubled")
def label_rule_double(x: int, y: int) -> int:
    return (x + y) * 2


def _source() -> DictSource:
    return DictSource(
        data=[{"id": "a", "x": 1}, {"id": "b", "x": 2}],
        tag_columns=["id"],
        source_id="label_rule_source",
    )


def _other_source() -> DictSource:
    return DictSource(
        data=[{"id": "a", "y": 10}, {"id": "b", "y": 20}],
        tag_columns=["id"],
        source_id="label_rule_other",
    )


# ---------------------------------------------------------------------------
# Rule 2: the format
# ---------------------------------------------------------------------------


class TestLabelFormat:
    def test_structure_hash_label(self) -> None:
        hasher = get_default_context().semantic_hasher
        assert hasher.hasher_id == "semantic_v1"
        assert hasher.hash_object({"a": 1}).method == "semantic_v1"

    def test_arrow_hash_label(self) -> None:
        hasher = get_default_context().arrow_hasher
        assert hasher.hasher_id == "arrow_v1"
        assert hasher.hash_table(pa.table({"x": [1]})).method == "arrow_v1"

    def test_value_hash_labels(self, tmp_path: Path) -> None:
        hasher = get_default_context().semantic_hasher
        (tmp_path / "f.txt").write_text("hello")
        assert hasher.hash_object(np.arange(3)).method == "ndarray_v1"
        assert hasher.hash_object(pl.col("x") > 1).method == "polars_expr_v1"
        assert hasher.hash_object(op.Directory(tmp_path)).method == "directory_v1"

    def test_file_hash_keeps_the_digest_function_name(self, tmp_path: Path) -> None:
        # The one stated exception to the format.
        path = tmp_path / "f.txt"
        path.write_text("hello")
        hasher = get_default_context().semantic_hasher
        assert hasher.hash_object(op.File(path)).method == "sha256"

    def test_every_label_but_the_file_hash_follows_the_format(
        self, tmp_path: Path
    ) -> None:
        context = get_default_context()
        (tmp_path / "f.txt").write_text("hello")
        labels = {
            context.semantic_hasher.hasher_id,
            context.arrow_hasher.hasher_id,
            context.semantic_hasher.hash_object(np.arange(3)).method,
            context.semantic_hasher.hash_object(pl.col("x") > 1).method,
            context.semantic_hasher.hash_object(op.Directory(tmp_path)).method,
        }
        assert all(LABEL_FORMAT.fullmatch(label) for label in labels), labels

    def test_directory_label_names_the_sha256_procedure_only(self) -> None:
        with pytest.raises(ValueError, match="sha256"):
            directory_hashers.BasicDirectoryHasher(
                file_hasher=FileHasher(), algorithm="md5"
            )


# ---------------------------------------------------------------------------
# Rule 6: a label never enters a hash computation
# ---------------------------------------------------------------------------


class TestNestedHashForm:
    def test_nested_hash_contributes_its_digest_only(self) -> None:
        hasher = get_default_context().semantic_hasher
        digest = bytes(range(32))
        one = hasher.hash_object(["x", ContentHash("one_v1", digest)])
        other = hasher.hash_object(["x", ContentHash("other_v7", digest)])
        assert one == other

    def test_nested_hash_differs_from_its_hex_string(self) -> None:
        hasher = get_default_context().semantic_hasher
        digest = bytes(range(32))
        as_hash = hasher.hash_object(["x", ContentHash("one_v1", digest)])
        as_text = hasher.hash_object(["x", digest.hex()])
        assert as_hash != as_text

    def test_nested_hashes_with_different_digests_differ(self) -> None:
        hasher = get_default_context().semantic_hasher
        one = hasher.hash_object([ContentHash("one_v1", bytes(32))])
        other = hasher.hash_object([ContentHash("one_v1", bytes([1] * 32))])
        assert one != other


RENAMED = {
    "semantic_v1": "renamedsemantic_v9",
    "arrow_v1": "renamedarrow_v9",
    "ndarray_v1": "renamedndarray_v9",
    "polars_expr_v1": "renamedpolarsexpr_v9",
    "directory_v1": "renameddirectory_v9",
}


@pytest.fixture
def renamed_labels(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, str]]:
    """Rename every label in place, for the duration of one test.

    There is no public way to rename a label (each is defined once, in code), so this
    fixture sets the non-public definitions directly.
    """
    context = get_default_context()
    # Non-public: the label of the default context's two hashers.
    monkeypatch.setattr(
        context.semantic_hasher, "_hasher_id", RENAMED["semantic_v1"], raising=True
    )
    monkeypatch.setattr(
        context.arrow_hasher, "_hasher_id", RENAMED["arrow_v1"], raising=True
    )
    monkeypatch.setattr(NumpyArrayHandler, "HASH_LABEL", RENAMED["ndarray_v1"])
    monkeypatch.setattr(
        polars_expr, "POLARS_EXPR_HASH_LABEL", RENAMED["polars_expr_v1"]
    )
    monkeypatch.setattr(
        directory_hashers, "DIRECTORY_HASH_LABEL", RENAMED["directory_v1"]
    )
    yield RENAMED


def _fresh_golden_values() -> str:
    """Every pinned hash, path and column name, computed now, as one JSON text.

    Hash caches of the golden cases' module-level function pods are keyed by the label of
    the hasher that filled them, so a renamed label is computed afresh.
    """
    return json.dumps(cases.compute_all(), sort_keys=True, indent=0)


class TestRenamingALabelChangesNoDigest:
    def test_every_pinned_value_keeps_its_digest(
        self, request: pytest.FixtureRequest
    ) -> None:
        before = _fresh_golden_values()
        renamed: dict[str, str] = request.getfixturevalue("renamed_labels")
        after = _fresh_golden_values()

        # The labels did change ...
        assert RENAMED["semantic_v1"] in after
        assert RENAMED["arrow_v1"] in after
        # ... and nothing else: putting the old labels back gives the old values.
        restored = after
        for old, new in renamed.items():
            restored = restored.replace(new, old)
        before_values: dict[str, dict[str, Any]] = json.loads(before)
        restored_values: dict[str, dict[str, Any]] = json.loads(restored)
        moved = [
            f"{group}:{entry}"
            for group, entries in before_values.items()
            for entry, value in entries.items()
            if restored_values[group].get(entry) != value
        ]
        assert not moved, f"{len(moved)} values depend on a label: {moved[:10]}"

    def test_operator_cache_and_cached_source_keys_keep_their_digest(
        self, request: pytest.FixtureRequest
    ) -> None:
        before = _stored_row_keys()
        renamed: dict[str, str] = request.getfixturevalue("renamed_labels")
        after = _stored_row_keys()
        prefix_old, prefix_new = b"arrow_v1:", renamed["arrow_v1"].encode() + b":"
        assert [key.removeprefix(prefix_old) for key in before] == [
            key.removeprefix(prefix_new) for key in after
        ]
        assert all(key.startswith(prefix_new) for key in after)


# ---------------------------------------------------------------------------
# Rule 5: bare digests inside names
# ---------------------------------------------------------------------------


def _run_cached_join() -> tuple[OperatorJobNode, InMemoryArrowDatabase]:
    """Run a join whose rows are written to an operator-cache table."""
    store = InMemoryArrowDatabase()
    node = OperatorJobNode(
        operator=Join(),
        input_streams=(_source(), _other_source()),
        pipeline_database=store,
        cache_mode=CacheMode.LOG,
    )
    node.run()
    return node, store


def _run_job() -> tuple[PipelineJob, InMemoryArrowDatabase]:
    store = InMemoryArrowDatabase()
    job = PipelineJob(name="label_rule_job", store=store)
    with job:
        joined = Join(label="joined")(_source(), _other_source())
        label_rule_double.pod(joined, label="double")
    job.run()
    return job, store


class TestNamesHoldBareDigests:
    def test_function_uri_holds_the_bare_output_schema_digest(self) -> None:
        uri = label_rule_double.pod.uri
        assert uri[0] == "label_rule_double"
        assert BARE_SHA256.fullmatch(uri[1]), uri

    def test_function_table_paths_hold_bare_digests(self) -> None:
        _, store = _run_job()
        paths = ["/".join(path) for path in store.list_sources()]
        function_paths = [p for p in paths if "label_rule_double" in p]
        assert function_paths
        for path in function_paths:
            assert ":" not in path.replace("schema:", ""), path
        pipeline_table = next(p for p in function_paths if "/_result/" not in p)
        components = pipeline_table.split("/")
        assert BARE_SHA256.fullmatch(components[2]), pipeline_table
        schema_component = next(c for c in components if c.startswith("schema:"))
        assert BARE_SHA256.fullmatch(schema_component.removeprefix("schema:"))

    def test_operator_table_path_holds_bare_digests(self) -> None:
        node, store = _run_cached_join()
        (path,) = store.list_sources()
        assert path == node.node_identity_path
        assert path[0] == "Join"
        assert BARE_SHA256.fullmatch(path[1]), path
        assert path[2].startswith("schema:")
        assert BARE_SHA256.fullmatch(path[2].removeprefix("schema:")), path

    def test_no_stored_table_path_holds_a_label(self) -> None:
        _, store = _run_job()
        for path in store.list_sources():
            for component in path:
                assert "semantic_v" not in component, path
                assert "arrow_v" not in component, path

    def test_cached_source_path_holds_the_bare_content_digest(self) -> None:
        cached = CachedSource(_source(), cache_database=InMemoryArrowDatabase())
        component = cached.cache_path[-1]
        assert component.startswith("node:")
        assert BARE_SHA256.fullmatch(component.removeprefix("node:")), component
        assert component.removeprefix("node:") == cached.content_hash().to_hex()

    def test_derived_source_id_holds_the_bare_content_digest(self) -> None:
        job, _ = _run_job()
        node = job.nodes["double"]
        derived = node.as_source()
        assert "semantic_v" not in derived.source_id
        assert derived.source_id.endswith(":" + node.content_hash().to_hex())

    def test_source_info_token_of_a_function_output_holds_no_label(self) -> None:
        stream = label_rule_double.pod(Join()(_source(), _other_source()))
        table = stream.as_table(columns={"source": True})
        tokens = table.column("_source_doubled").to_pylist()
        assert tokens
        for token in tokens:
            assert "semantic_v" not in token, token
            assert token.startswith(
                "label_rule_double:" + label_rule_double.pod.uri[1] + ":"
            )


# ---------------------------------------------------------------------------
# Rule 4: stored hash values carry their label
# ---------------------------------------------------------------------------


def _stored_row_keys() -> list[bytes]:
    """Row keys of an operator-cache table and of a cached-source table."""
    _, store = _run_cached_join()
    (operator_path,) = store.list_sources()
    records = store.get_all_records(operator_path, record_id_column="__row_key")
    assert records is not None
    keys = sorted(records.column("__row_key").to_pylist())

    cache = InMemoryArrowDatabase()
    cached = CachedSource(_source(), cache_database=cache)
    assert cached.as_table().num_rows == 2
    cached_records = cache.get_all_records(
        cached.cache_path, record_id_column="__row_key"
    )
    assert cached_records is not None
    return keys + sorted(cached_records.column("__row_key").to_pylist())


class TestStoredHashValuesCarryTheirLabel:
    def test_operator_cache_and_cached_source_row_keys_are_labelled(self) -> None:
        keys = _stored_row_keys()
        assert len(keys) == 4
        for key in keys:
            assert key.startswith(b"arrow_v1:"), key
            parsed = ContentHash.from_prefixed_digest(key)
            assert len(parsed.digest) == ARROW_DIGEST_BYTES

    def test_function_tables_keep_labelled_hash_columns(self) -> None:
        _, store = _run_job()
        path = next(
            p
            for p in store.list_sources()
            if "label_rule_double" in p and "_result" not in p
        )
        records = store.get_all_records(path)
        assert records is not None
        for value in records.column("__input_data_hash").to_pylist():
            assert value.startswith(b"arrow_v1:")
        for value in records.column("__node_content_hash").to_pylist():
            assert value.startswith(b"semantic_v1:")


# ---------------------------------------------------------------------------
# Rule 6 at a stream with no producer: its identity covers every column group but the
# derived per-row content hash (whose values carry the Arrow label)
# ---------------------------------------------------------------------------


def _bare_stream(
    *,
    tag: str = "a",
    data: int = 1,
    system_tag: str = "s",
    source_info: str = "origin",
    meta: str = "m",
    context_key: str | None = None,
) -> ArrowTableStream:
    columns: dict[str, pa.Array[Any]] = {
        "id": pa.array([tag], pa.large_string()),
        "x": pa.array([data], pa.int64()),
        "__note": pa.array([meta], pa.large_string()),
        "_tag::origin": pa.array([system_tag], pa.large_string()),
    }
    if context_key is not None:
        columns["_context_key"] = pa.array([context_key], pa.large_string())
    return ArrowTableStream(
        pa.table(columns), tag_columns=["id"], source_info={"x": source_info}
    )


class TestProducerlessStreamIdentity:
    def test_equal_streams_have_equal_content_hashes(self) -> None:
        assert _bare_stream().content_hash() == _bare_stream().content_hash()

    @pytest.mark.parametrize(
        "changed",
        [
            {"tag": "b"},
            {"data": 2},
            {"system_tag": "t"},
            {"source_info": "elsewhere"},
            {"meta": "n"},
            {"context_key": "std:v9:other"},
        ],
        ids=["tag", "data", "system_tag", "source_info", "meta", "context"],
    )
    def test_any_changed_value_changes_the_content_hash(
        self, changed: dict[str, Any]
    ) -> None:
        assert _bare_stream().content_hash() != _bare_stream(**changed).content_hash()

    def test_identity_does_not_cover_the_derived_content_hash_column(self) -> None:
        stream = _bare_stream()
        with_hashes = stream.as_table(all_info=True)
        assert (
            with_hashes.column("_content_hash").to_pylist()[0].startswith("arrow_v1:")
        )
        # The public identity_structure() is what content_hash() hashes.
        _, identity_table, _ = stream.identity_structure()
        assert "_content_hash" not in identity_table.column_names
        assert set(with_hashes.column_names) - set(identity_table.column_names) == {
            "_content_hash"
        }


# ---------------------------------------------------------------------------
# Rule 9: each label is defined in a single place, in code beside its algorithm
# ---------------------------------------------------------------------------

SOURCE_ROOT = Path(orcapod.__file__).parent
LABEL_DEFINITIONS = {
    "semantic_v1": "hashing/semantic_hashing/semantic_hasher.py",
    "arrow_v1": "hashing/arrow_hashers.py",
    "directory_v1": "hashing/directory_hashers.py",
    "ndarray_v1": "hashing/semantic_hashing/builtin_handlers.py",
    "polars_expr_v1": "hashing/semantic_hashing/polars_expr.py",
}


def _string_literals_equal_to(label: str) -> list[str]:
    """Return the source files, relative to the package, that spell ``label`` in code.

    Only string constants equal to the label count: docstrings and comments that
    mention a label are prose, not definitions.
    """
    found: list[str] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and node.value == label:
                found.append(path.relative_to(SOURCE_ROOT).as_posix())
    return found


class TestEachLabelIsDefinedOnce:
    @pytest.mark.parametrize("label", sorted(LABEL_DEFINITIONS))
    def test_label_is_spelled_once_in_code(self, label: str) -> None:
        assert _string_literals_equal_to(label) == [LABEL_DEFINITIONS[label]]

    def test_no_data_context_file_spells_a_hasher_label(self) -> None:
        offenders: list[str] = []
        for path in sorted((SOURCE_ROOT / "contexts").rglob("*.json")):
            text = path.read_text(encoding="utf-8")
            if '"hasher_id"' in text:
                offenders.append(path.relative_to(SOURCE_ROOT).as_posix())
        assert not offenders, (
            "A data context file names the hasher class and must not repeat its "
            f"label: {offenders}"
        )

    def test_hashers_take_their_label_from_the_definition(self) -> None:
        from orcapod.hashing.arrow_hashers import ARROW_HASH_LABEL
        from orcapod.hashing.semantic_hashing.semantic_hasher import (
            SEMANTIC_HASH_LABEL,
            SemanticAwarePythonHasher,
        )
        from orcapod.hashing.versioned_hashers import (
            get_versioned_semantic_arrow_hasher,
            get_versioned_semantic_hasher,
        )

        context = get_default_context()
        assert context.semantic_hasher.hasher_id == SEMANTIC_HASH_LABEL
        assert context.arrow_hasher.hasher_id == ARROW_HASH_LABEL
        assert SemanticAwarePythonHasher().hasher_id == SEMANTIC_HASH_LABEL
        assert get_versioned_semantic_hasher().hasher_id == SEMANTIC_HASH_LABEL
        assert get_versioned_semantic_arrow_hasher().hasher_id == ARROW_HASH_LABEL
