"""A stored hit takes its stored datagram id by a shallow copy (ITL-1361, ITL-1060).

``_with_datagram_id`` used to build each stored result's ``Data`` again from its table
to give it the id it was stored under. It now copies the datagram the stored stream
built (``Datagram.with_datagram_uuid``). The copy must be the rebuilt datagram in every
view: columns, types, meta, source info, content hash and id.
"""

from __future__ import annotations

import pathlib
import uuid
from typing import Any

import pytest

from orcapod.core.datagrams import Data
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from tests.test_core.function_pod.test_function_node_arrow_types import (
    TAG_COLUMNS,
    _build,  # pyright: ignore[reportPrivateUsage]  # the job these tests share
    _source_table,  # pyright: ignore[reportPrivateUsage]  # the job these tests share
)


def _rebuilt(data: Data, datagram_id: uuid.UUID) -> Data:
    """The datagram as ``_with_datagram_id`` built it before."""
    return Data(
        data.as_table(columns={"source": True}),
        record_uuid=datagram_id,
        data_context=data.data_context_key,
    )


def _views(data: Data) -> list[Any]:
    return [
        data.as_table(all_info=True),
        data.as_table(columns={"source": True}),
        data.keys(all_info=True),
        data.source_info(),
        data.content_hash(),
        data.datagram_uuid,
        data.arrow_schema(all_info=True),
        data.data_context_key,
    ]


@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("name", list(TAG_COLUMNS))
def test_a_stored_hit_is_the_rebuilt_datagram(
    name: str, store: str, tmp_path: pathlib.Path
) -> None:
    """Every stored hit equals the datagram rebuilt with its stored id, in every view."""
    database: Any = (
        InMemoryArrowDatabase() if store == "memory" else DeltaTableDatabase(tmp_path)
    )
    table = _source_table(name)
    _build(database, table).run()
    node = _build(database, table).nodes["double"]
    loaded = node._load_cached_entries()  # pyright: ignore[reportPrivateUsage]  # the stored hits under test
    assert loaded
    for _, data in loaded.values():
        assert isinstance(data, Data)
        rebuilt = _rebuilt(data, data.datagram_uuid)
        for got, expected in zip(_views(data), _views(rebuilt), strict=True):
            if hasattr(got, "equals"):
                assert got.equals(expected, check_metadata=True)
            else:
                assert got == expected
