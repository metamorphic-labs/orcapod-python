"""The golden cases split their items by the provisional library value types (ITL-1324).

A golden case's frozen part and provisional part are chosen by
``cases.provisional_types_of``, which reads a type's fields: the tool's record of uses
cannot see a dependence hidden behind an identity built earlier and cached (a registered
user class with a field of a provisional type). These tests check that predicate against
what the library itself stores: every fixture user class's identity record. A fixture
whose identity embeds a provisional type's identifier is provisional for the predicate,
and no other is, so a new fixture cannot be forgotten.
"""

from __future__ import annotations

import dataclasses
import inspect
import warnings
from typing import Any

import pydantic
import pytest

from orcapod.logical_types.user_class_identity import known_record
from tests.test_hashing.golden import cases


def _logical_names(token: Any) -> set[str]:
    """The extension names of every logical-type token in an identity record."""
    names: set[str] = set()
    if isinstance(token, dict):
        logical = token.get("logical")
        if isinstance(logical, str):
            names.add(logical.split(";", 1)[0])
        for value in token.values():
            names |= _logical_names(value)
    elif isinstance(token, list):
        for value in token:
            names |= _logical_names(value)
    return names


def _fixture_classes() -> dict[str, type]:
    return {
        name: obj
        for name, obj in vars(cases).items()
        if inspect.isclass(obj)
        and obj.__module__ == cases.__name__
        and (dataclasses.is_dataclass(obj) or issubclass(obj, pydantic.BaseModel))
    }


@pytest.fixture(scope="module", autouse=True)
def registered() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cases.register_logical_classes()


def test_there_are_fixture_classes_and_one_embeds_a_provisional_type() -> None:
    classes = _fixture_classes()
    assert len(classes) >= 2
    assert cases.provisional_types_of(cases.GoldenWithArray) == {cases.NUMPY_NDARRAY}


@pytest.mark.parametrize("name", sorted(_fixture_classes()))
def test_the_predicate_agrees_with_each_fixture_class_identity_record(
    name: str,
) -> None:
    """The provisional types a fixture class embeds, by the predicate, are exactly the
    provisional identifiers in its identity record (nested classes included)."""
    cls = _fixture_classes()[name]
    record = known_record(cls)
    if record is None:
        pytest.skip(f"{name} is not registered as a user class")
    provisional = cases.declarable_provisional_names()
    assert set(cases.provisional_types_of(cls)) == _logical_names(record) & provisional


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        (1, set()),
        ("a", set()),
        ([1, (2, "x")], set()),
        ({"k": [1, 2]}, set()),
    ],
)
def test_a_plain_value_embeds_no_provisional_type(
    item: Any, expected: set[str]
) -> None:
    assert set(cases.provisional_types_of(item)) == expected


def test_containers_and_generic_types_are_searched() -> None:
    import numpy as np
    import pandas as pd

    assert cases.provisional_types_of([1, {"a": np.arange(2)}]) == {cases.NUMPY_NDARRAY}
    assert cases.provisional_types_of(list[pd.DataFrame]) == {cases.PANDAS_DATAFRAME}
    assert cases.provisional_types_of(
        dict[str, cases.GoldenWithArray]  # pyright: ignore[reportUnknownArgumentType]
    ) == {cases.NUMPY_NDARRAY}
