"""SpiralDB store table names and their metadata record (ITL-1495; spec ITL-1446 round 2)."""

from __future__ import annotations

import hashlib
import json
import random
import re

import pytest

from orcapod.databases import spiral_store_names as names
from orcapod.errors import SpiralStoreTableCollisionError

H = "0123456789abcdef" * 4
RESULT = ("my_fn", H, "v0", "python.function.v1", "rev1", "rdb_v1")
PIPELINE = ("my_fn", H, "v0", "python.function.v1", f"schema:{H}", "pdb_v1")
NAME = re.compile(r"^o1_[a-z0-9_]{1,96}_[0-9a-f]{64}$")


def test_the_digest_is_sha256_of_the_canonical_json_of_the_components() -> None:
    """The digest is SHA-256 of the canonical JSON array of the components, ending the name."""
    expected = hashlib.sha256(
        json.dumps(list(RESULT), ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    assert names.store_table_digest(RESULT) == expected
    assert names.store_table_name(RESULT).endswith("_" + expected)


def test_the_label_names_the_algorithm() -> None:
    # The label is defined once, in the module (the hash label rule): not repeated here.
    """The label constant has the label form and is not written into the name."""
    assert re.fullmatch(r"[a-z_]+_v[0-9]+", names.SPIRAL_TABLE_NAME_LABEL)
    # A hash in a name is bare hex: the label is not written into the name.
    assert names.SPIRAL_TABLE_NAME_LABEL not in names.store_table_name(RESULT)


@pytest.mark.parametrize("path", [RESULT, PIPELINE, ("a",), ("É" * 300, "x" * 500)])
def test_a_name_is_short_and_plain(path: tuple[str, ...]) -> None:
    """A name is ``o1_<hint>_<64 hex>``, within 164 characters, for short and long paths."""
    name = names.store_table_name(path)
    assert NAME.match(name), name
    assert len(name) <= 3 + 96 + 1 + 64 <= 255


def test_the_hint_keeps_lower_case_letters_and_digits() -> None:
    """The hint maps one character to one and is cut to 96 characters."""
    assert names.table_name_hint(("My-Fn", "v0", "schema:AB")) == "my_fn_v0_schema_ab"
    # one character to one: each other character, the separator included, is one '_'
    assert names.table_name_hint(("é", "a/b")) == "__a_b"
    assert names.table_name_hint(("é", "z")) == "__z"
    assert len(names.table_name_hint(("x" * 200,))) == 96


def test_the_name_is_deterministic_and_depends_on_every_component_and_its_order() -> (
    None
):
    """A name follows every component and their order, and nothing else."""
    assert names.store_table_name(RESULT) == names.store_table_name(list(RESULT))
    assert names.store_table_name(RESULT) != names.store_table_name(RESULT[::-1])
    for i in range(len(RESULT)):
        changed = RESULT[:i] + (RESULT[i] + "x",) + RESULT[i + 1 :]
        assert names.store_table_name(changed) != names.store_table_name(RESULT)


def test_components_are_kept_apart() -> None:
    # Paths whose joined text or hint agree still get different names.
    """Paths whose joined text or hint agree still get different names."""
    pairs = [
        (("a", "b"), ("a/b",)),
        (("a_b",), ("a-b",)),
        (("ab", "c"), ("a", "bc")),
        (("A",), ("a",)),
        (("x",), ("x", "")),
    ]
    for left, right in pairs:
        assert names.store_table_name(left) != names.store_table_name(right)


def test_no_two_of_many_adversarial_paths_share_a_name() -> None:
    """No two of 20,000 adversarial paths share a name."""
    rng = random.Random(1495)
    alphabet = "aA0_-./:é \x00 "
    seen: dict[str, tuple[str, ...]] = {}
    for _ in range(20_000):
        path = tuple(
            "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 4)))
            for _ in range(rng.randint(1, 3))
        )
        name = names.store_table_name(path)
        assert seen.setdefault(name, path) == path


@pytest.mark.parametrize(
    ("path", "prefix"),
    [
        (RESULT, ()),
        (RESULT, ("my_fn",)),
        (RESULT, RESULT[:3]),
        (RESULT, RESULT),
        (("side_effects", "My Pod", H, "tdb_v1"), ("side_effects", "My Pod")),
        (("f" * 120, H, "rdb_v1"), ("f" * 120,)),
        (("f" * 60, "g" * 60, "rdb_v1"), ("f" * 60,)),
    ],
)
def test_a_table_under_a_prefix_has_the_prefix_s_name_prefix(
    path: tuple[str, ...], prefix: tuple[str, ...]
) -> None:
    """Every table under a path prefix has the prefix's name prefix, also past 96 characters."""
    assert names.store_table_name(path).startswith(
        names.store_table_name_prefix(prefix)
    )


def test_the_name_prefix_of_nothing_is_every_store_table() -> None:
    """The name prefix of the empty path prefix is the one every store table has."""
    assert names.store_table_name_prefix(()) == "o1_"


@pytest.mark.parametrize("path", [(), ("a", 1), ("\ud800",)])
def test_a_path_that_cannot_be_named_is_refused(path: tuple[object, ...]) -> None:
    """An empty path, a component that is not text and one that is not UTF-8 are refused."""
    with pytest.raises(ValueError, match="path"):
        _ = names.store_table_name(path)  # pyright: ignore[reportArgumentType]  # refused on purpose


def test_the_metadata_record() -> None:
    """A table's record holds its format, path, layout and data context, and reads back."""
    record = names.store_table_metadata(PIPELINE, "std:v1:default")
    assert record == {
        "orcapod.store": b"orcapod.spiral_store.v1",
        "orcapod.path": json.dumps(
            list(PIPELINE), ensure_ascii=False, separators=(",", ":")
        ).encode(),
        "orcapod.layout": b"pdb_v1",
        "orcapod.data_context": b"std:v1:default",
    }
    found = names.read_store_table_metadata(record)
    assert found == names.StoreTableRecord(PIPELINE, "pdb_v1", "std:v1:default")
    assert found is not None
    names.require_store_table_path(found, PIPELINE)


def test_every_entry_fits_the_service_s_entry_limit() -> None:
    """Every entry of a record fits SpiralDB's entry limit; a longer path is refused."""
    longest = ("x" * (names.MAX_PATH_JSON_BYTES - 4),)
    record = names.store_table_metadata(longest, "std:v1:default")
    assert all(len(k.encode()) + len(v) <= 8192 for k, v in record.items())
    with pytest.raises(ValueError, match="path"):
        _ = names.store_table_metadata(
            ("x" * names.MAX_PATH_JSON_BYTES,), "std:v1:default"
        )


def test_a_table_without_orcapod_metadata_is_unmarked() -> None:
    """A table with none of the record's entries is unmarked."""
    assert names.read_store_table_metadata({}) is None
    assert names.read_store_table_metadata({"someone.else": b"1"}) is None


@pytest.mark.parametrize(
    "change",
    [
        {"orcapod.store": b"orcapod.spiral_store.v2"},
        {"orcapod.path": b"not json"},
        {"orcapod.path": b'{"a": 1}'},
        {"orcapod.path": b"[]"},
        {"orcapod.path": b'["a", 1]'},
        {"orcapod.layout": b"odb_v1"},
        {"orcapod.data_context": b""},
        {"orcapod.path": b"\xff"},
    ],
)
def test_a_record_that_is_not_this_store_s_is_refused(change: dict[str, bytes]) -> None:
    """A record of another format, or with an unreadable or inconsistent entry, is refused."""
    record = {**names.store_table_metadata(PIPELINE, "std:v1:default"), **change}
    with pytest.raises(SpiralStoreTableCollisionError):
        _ = names.read_store_table_metadata(record)


@pytest.mark.parametrize(
    "missing",
    ["orcapod.store", "orcapod.path", "orcapod.layout", "orcapod.data_context"],
)
def test_a_partial_record_is_refused(missing: str) -> None:
    """A record lacking one of its entries is refused."""
    record = names.store_table_metadata(PIPELINE, "std:v1:default")
    del record[missing]
    with pytest.raises(SpiralStoreTableCollisionError):
        _ = names.read_store_table_metadata(record)


def test_a_table_of_another_path_under_the_name_is_refused() -> None:
    """A record of another path than the one looked up is refused, naming it."""
    found = names.read_store_table_metadata(
        names.store_table_metadata(RESULT, "std:v1:default")
    )
    assert found is not None
    with pytest.raises(SpiralStoreTableCollisionError, match="rdb_v1"):
        names.require_store_table_path(found, PIPELINE)


def test_a_record_with_an_entry_over_the_service_s_limit_is_refused() -> None:
    # Review of PR 589, finding 2: the data context key is the caller's text too.
    """A record entry over SpiralDB's limit is refused before any table is created."""
    with pytest.raises(ValueError, match="8192"):
        _ = names.store_table_metadata(PIPELINE, "x" * 8192)


def test_a_recorded_path_that_writers_refuse_is_refused_on_read() -> None:
    # Review of PR 589, finding 3: JSON can spell a lone surrogate, which no writer writes.
    """A recorded path no writer writes (a lone surrogate) is refused on read."""
    record = {
        **names.store_table_metadata(RESULT, "std:v1:default"),
        "orcapod.path": b'["\\ud800","rdb_v1"]',
    }
    with pytest.raises(SpiralStoreTableCollisionError, match="UTF-8"):
        _ = names.read_store_table_metadata(record)


def test_the_collision_message_keeps_components_apart() -> None:
    # Review of PR 589, finding 4: ("a/b", ...) and ("a", "b", ...) must read differently.
    """The collision message shows component lists, so different paths read differently."""
    found = names.read_store_table_metadata(
        names.store_table_metadata(("a/b", "rdb_v1"), "std:v1:default")
    )
    assert found is not None
    with pytest.raises(SpiralStoreTableCollisionError) as raised:
        names.require_store_table_path(found, ("a", "b", "rdb_v1"))
    assert "['a/b', 'rdb_v1']" in str(raised.value)
    assert "['a', 'b', 'rdb_v1']" in str(raised.value)


@pytest.mark.parametrize(
    "path_json",
    [b"[" + b"1" * 5000 + b"]", b"[" * 2000 + b"0" + b"]" * 2000],
    ids=["a 5,000-digit integer", "2,000 nested arrays"],
)
def test_a_path_the_json_parser_refuses_is_a_collision(path_json: bytes) -> None:
    """CodeRabbit on PR 589: every unreadable record raises the collision error."""
    record = {
        **names.store_table_metadata(RESULT, "std:v1:default"),
        "orcapod.path": path_json,
    }
    with pytest.raises(SpiralStoreTableCollisionError):
        _ = names.read_store_table_metadata(record)
