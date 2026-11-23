"""The property of a ``DeltaTableDatabase`` append: Orcapod refuses nothing Delta stores.

The developer's decision for ITL-1403 (2026-11-22): the flush is the point of truth.
When rows are added, only what Orcapod can tell for certain is refused; everything else
is left to the commit, whose refusal the flush raises once, without keeping the rows
queued. This file is the review of PR 504's sweep, as a test: every stored type (same
schema, as written), every record change, struct evolution, null and layout case and
logical-type case, with schema evolution on and off, and with the commit an append
(every id new) or a MERGE (a stored id among the rows). For each:

- what Orcapod refuses when the rows are added, Delta refuses too: the same rows given
  to the database with Orcapod's add-time checks switched off are not stored (the one
  exception is the logical-type rule, an Orcapod rule by design: another logical type's
  values must not be read as the stored one's);
- nothing fails forever: after the flush, no row is still queued.
"""

from __future__ import annotations

from collections.abc import Iterator
from itertools import product
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.databases import delta_lake_databases as dld
from tests.test_databases import test_delta_append_types as append_types
from tests.test_databases import test_delta_refused_before_queueing as refused
from tests.test_databases import test_delta_struct_evolution as evolution

PATH = ("t",)


def _variants() -> Iterator[tuple[str, Any, Any, Any, Any]]:
    for param in append_types.all_types():
        name, arrow_type, nullable = param.values
        if name in append_types.NOT_STORABLE:
            continue
        column = append_types.column(arrow_type)
        field = pa.field("x", arrow_type, nullable=nullable)
        yield f"same {param.id}", column, column, field, field
    for label, (a, b) in refused.RECORD_CHANGES.items():
        yield f"record {label}", a, b, None, None
    for label, (at, av, bt, bv, _) in evolution.CASES.items():
        yield f"evolution {label}", pa.array(av, at), pa.array(bv, bt), None, None
    for label, (a, b) in {
        "simple null": (pa.array([1]), pa.nulls(1)),
        "null struct field": (
            pa.array([{"a": 1}]),
            pa.array([{"a": None}], pa.struct([("a", pa.null())])),
        ),
        "null list item": (
            pa.array([[1]], pa.large_list(pa.int64())),
            pa.array([[None]], pa.large_list(pa.null())),
        ),
        "null into recorded unsigned": (pa.array([1], pa.uint8()), pa.nulls(1)),
        "null into recorded list": (pa.array([[1]], pa.list_(pa.int64())), pa.nulls(1)),
        "plain field becomes nullable": (
            pa.array(
                [{"a": 1}], pa.struct([pa.field("a", pa.int64(), nullable=False)])
            ),
            pa.array([{"a": None}], pa.struct([("a", pa.int64())])),
        ),
        "small list then large": (
            pa.array([[1]], pa.list_(pa.int64())),
            pa.array([[2]], pa.large_list(pa.int64())),
        ),
    }.items():
        yield f"null/layout {label}", a, b, None, None
    field, column, _ = refused._path_column()  # pyright: ignore[reportPrivateUsage]
    stored = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    for label, ext in {
        "bare": None,
        "current": field.type.__arrow_ext_serialize__(),
        "readable old": b"",
        "unknown": b"bogus",
    }.items():
        metadata = (
            None
            if ext is None
            else {
                b"ARROW:extension:name": field.type.extension_name.encode(),
                b"ARROW:extension:metadata": ext,
            }
        )
        yield (
            f"path null {label}",
            stored,
            pa.nulls(1),
            field,
            pa.field("x", pa.null(), metadata=metadata),
        )
    for label, a, b in [
        ("same", b"x", b"x"),
        ("changed", b"x", b"y"),
        ("empty to value", b"", b"y"),
        ("value to empty", b"x", b""),
    ]:
        units = [
            pa.ExtensionArray.from_storage(refused._Unit(v), pa.array([1]))  # pyright: ignore[reportPrivateUsage]
            for v in (a, b)
        ]
        yield f"logical {label}", units[0], units[1], None, None


VARIANTS = {label: rest for label, *rest in _variants()}
LOGICAL_RULE = {"logical changed", "logical empty to value", "logical value to empty"}


def _rows(column: Any, ids: list[bytes], field: Any = None) -> pa.Table:
    values = pa.concat_arrays([column] * len(ids)) if len(ids) > 1 else column
    return pa.table(
        [pa.array(ids, pa.large_binary()), values],
        schema=pa.schema(
            [
                pa.field("rid", pa.large_binary(), nullable=False),
                field or pa.field("x", column.type),
            ]
        ),
    )


def _run(
    root: Path, initial: pa.Table, incoming: pa.Table, evolve: bool
) -> tuple[str, bool]:
    """``(outcome, rows still queued)``: ``stored``, ``refused when added``, or
    ``refused at flush``."""
    db = DeltaTableDatabase(root, allow_schema_evolution=evolve)
    db.add_records(PATH, initial, record_id_column="rid", flush=True)
    try:
        db.add_records(PATH, incoming, record_id_column="rid")
    except Exception:  # noqa: BLE001  # the outcome is the subject
        return "refused when added", bool(db._pending_batches)  # pyright: ignore[reportPrivateUsage]
    try:
        db.flush()
    except Exception:  # noqa: BLE001
        return "refused at flush", bool(db._pending_batches)  # pyright: ignore[reportPrivateUsage]
    return "stored", False


@pytest.fixture(autouse=True)
def _unit_registered(unit_type: Any) -> None:
    """The logical cases use a registered extension type."""


unit_type = refused.unit_type


@pytest.mark.parametrize(
    "seen", [False, True], ids=["append (every id new)", "MERGE (a stored id)"]
)
@pytest.mark.parametrize("evolve", [False, True], ids=["evolution off", "evolution on"])
@pytest.mark.parametrize("label", sorted(VARIANTS))
def test_orcapod_refuses_nothing_delta_stores(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    label: str,
    evolve: bool,
    seen: bool,
) -> None:
    a, b, field_a, field_b = VARIANTS[label]
    initial = _rows(a, [b"1"], field_a)
    incoming = _rows(b, [b"1", b"2"] if seen else [b"2"], field_b)

    live, queued = _run(tmp_path / "live", initial, incoming, evolve)
    assert not queued, f"{label}: rows still queued after the flush ({live})"

    if live == "refused when added" and label not in LOGICAL_RULE:
        # Delta's own verdict: the same rows with Orcapod's add-time checks off.
        monkeypatch.setattr(dld, "_append_refusal", lambda *args: None)
        monkeypatch.setattr(
            DeltaTableDatabase, "_refuse_case_clash_with_pending", lambda *args: None
        )
        raw, _ = _run(tmp_path / "raw", initial, incoming, evolve)
        assert raw != "stored", f"{label}: Orcapod refused rows Delta stores"


def test_the_sweep_covers_every_kind() -> None:
    kinds = {label.split(" ")[0] for label in VARIANTS}
    assert kinds == {"same", "record", "evolution", "null/layout", "path", "logical"}
    assert len(VARIANTS) > 100
    _ = product  # the sweep is the product of the three parameters above
