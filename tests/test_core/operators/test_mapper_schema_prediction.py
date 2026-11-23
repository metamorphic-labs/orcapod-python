"""A mapper's predicted schema keeps every column the result keeps (ITL-1363).

``MapTags(drop_unmapped=True)`` used to filter its whole tag schema by the names of
``name_map``, so the system-tag columns, which no ``name_map`` names and the table
keeps, were missing from the prediction under ``system_tags=True`` or
``all_info=True``. Every mapper now renames and drops only user columns
(``mappers._plan_group``). The prediction is checked against
``static_process(...).output_schema(...)``, as CLAUDE.md asks, for every column
configuration, every arrangement of a source with three tags and three data
columns, both orders of a two-entry ``name_map``, with and without
``drop_unmapped``; the async rows carry the same system tags as the sync rows.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import Any

import pytest

from orcapod import operators
from orcapod.core.operators import MapColumns, MapData, MapTags
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.errors import ProvisionalHashWarning
from orcapod.system_constants import constants

from .several_items import (
    MAIN,
    PLAIN,
    Arrangement,
    arrangements,
    async_rows,
    make_source,
    rows_fingerprint,
)

pytestmark = pytest.mark.filterwarnings(
    f"ignore::{ProvisionalHashWarning.__module__}.{ProvisionalHashWarning.__name__}"
)

# Every column configuration a schema can be asked for.
CONFIGS: tuple[dict[str, Any], ...] = (
    {},
    {"columns": {"system_tags": True}},
    {"columns": {"source": True}},
    {"columns": {"meta": True}},
    {"columns": {"context": True}},
    {"columns": {"content_hash": True}},
    {"columns": {"system_tags": True, "source": True, "meta": True}},
    {"all_info": True},
)


def _orders(entries: tuple[tuple[str, str], ...]) -> list[dict[str, str]]:
    return [dict(entries), dict(reversed(entries))]


TAG_MAP = (("t1", "u1"), ("t3", "u3"))
DATA_MAP = (("d1", "e1"), ("d3", "e3"))
COLUMN_MAP = (("t1", "u1"), ("d2", "e2"))


def _cases() -> list[Any]:
    cases: list[Any] = []
    for drop in (False, True):
        for i, name_map in enumerate(_orders(TAG_MAP)):
            for tag_only in (False, True):
                cases.append(
                    pytest.param(
                        (
                            lambda m, d, t: (
                                lambda: MapTags(m, drop_unmapped=d, tag_only=t)
                            )
                        )(name_map, drop, tag_only),
                        None,
                        id=f"map_tags-order{i}-drop{drop}-tag_only{tag_only}",
                    )
                )
        for i, name_map in enumerate(_orders(DATA_MAP)):
            cases.append(
                pytest.param(
                    (lambda m, d: lambda: MapData(m, drop_unmapped=d))(name_map, drop),
                    None,
                    id=f"map_data-order{i}-drop{drop}",
                )
            )
        for i, name_map in enumerate(_orders(COLUMN_MAP)):
            # MapColumns is refused with data-tags (ITL-1022).
            cases.append(
                pytest.param(
                    (lambda m, d: lambda: MapColumns(m, drop_unmapped=d))(
                        name_map, drop
                    ),
                    (PLAIN,),
                    id=f"map_columns-order{i}-drop{drop}",
                )
            )
    return cases


def _arrangement_params(roles: tuple[str, ...] | None) -> list[Arrangement]:
    return arrangements(MAIN) if roles is None else arrangements(MAIN, roles)


def _system_tags(names: Any) -> list[str]:
    return sorted(n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX))


@pytest.mark.parametrize(("make", "roles"), _cases())
def test_the_prediction_equals_the_result_schema_in_every_configuration(
    make: Callable[[], StaticOutputOperatorPod], roles: tuple[str, ...] | None
) -> None:
    for arrangement in _arrangement_params(roles):
        source = make_source(MAIN, arrangement)
        op = make()
        predicted_stream = op(source)
        computed = op.static_process(source)
        for config in CONFIGS:
            predicted = predicted_stream.output_schema(**config)
            actual = computed.output_schema(**config)
            label = f"{arrangement.id} {config}"
            # Names and types; column order is presentation, and a source's own
            # schema follows its table layout while its keys follow the tag order.
            for p, a in zip(predicted, actual):
                assert dict(p) == dict(a), label
        # The table holds exactly the system tags the prediction lists.
        predicted_tags = predicted_stream.output_schema(all_info=True)[0]
        table = predicted_stream.as_table(all_info=True)
        assert _system_tags(predicted_tags) == _system_tags(table.column_names)
        assert len(_system_tags(predicted_tags)) == 2, arrangement.id


@pytest.mark.parametrize(("make", "roles"), _cases())
def test_the_async_rows_keep_the_system_tags_of_the_sync_rows(
    make: Callable[[], StaticOutputOperatorPod], roles: tuple[str, ...] | None
) -> None:
    for arrangement in _arrangement_params(roles):
        source = make_source(MAIN, arrangement)
        op = make()
        result = op(source)
        sync = list(result.iter_data())
        rows = async_rows(op, source)
        # Order-free: the data-column order of rows may differ between the paths.
        assert rows_fingerprint(rows) == rows_fingerprint(sync)
        predicted = _system_tags(result.output_schema(all_info=True)[0])
        for tag, _ in rows:
            assert sorted(tag.system_tags()) == predicted, arrangement.id


def test_drop_unmapped_keeps_the_system_tags_in_the_prediction() -> None:
    """The reported case: two tags, one renamed, the other dropped."""
    source = make_source(MAIN)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        stream = MapTags({"t2": "u2", "t1": "u1"}, drop_unmapped=True)(source)
    tag_schema, _ = stream.output_schema(all_info=True)
    system_tags = _system_tags(source.output_schema(all_info=True)[0])
    assert len(system_tags) == 2
    assert [n for n in tag_schema if n not in system_tags] == ["u1", "u2"]
    assert _system_tags(tag_schema) == system_tags
    assert _system_tags(stream.as_table(all_info=True).column_names) == system_tags


# ── every exported operator: predicted columns are the result's columns ──────


def _operator_streams() -> list[Any]:
    """One case per exported operator, from the order-invariance cases."""
    from ..test_order_invariance import OPERATOR_CASES

    names = {
        name
        for name in operators.__all__
        if isinstance(obj := getattr(operators, name), type)
        and issubclass(obj, StaticOutputOperatorPod)
    }
    assert names == set(OPERATOR_CASES)
    return [
        pytest.param(case, id=case.label)
        for cases in OPERATOR_CASES.values()
        for case in cases
    ]


@pytest.mark.parametrize("case", _operator_streams())
def test_every_operator_predicts_the_columns_of_its_result(case: Any) -> None:
    """Under every configuration the predicted column names (in order) are the
    result's. Column TYPES are not compared here."""
    for role in case.roles:
        sources = case.sources(role, 0, "tags_first")
        op = case.build()
        if not isinstance(op, StaticOutputOperatorPod):
            # Rejoin's case is a function of the source: its branches are the inputs.
            stream = op(*sources)
            op, sources = stream.producer, list(stream.upstreams)
        computed = op.static_process(*sources)
        for config in CONFIGS:
            predicted = op.output_schema(*sources, **config)
            actual = computed.output_schema(**config)
            for p, a in zip(predicted, actual):
                assert list(p) == list(a), f"{case.label} {role} {config}"
