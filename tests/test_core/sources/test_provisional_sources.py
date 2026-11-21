"""Provisional sources: the list, the markers and the warning (ITL-1314).

Developer decision, November 21, 2026: "make polling source provisional". A
``PollingSource`` behaves as before (its default source id included), but its source id
and hashes are not frozen with data context v1, so building one warns once per process
and no golden value depends on one.
"""

from __future__ import annotations

import inspect
import warnings
from collections.abc import Iterator
from typing import Any

import pytest

import orcapod.sources as sources
from orcapod.core.sources import PROVISIONAL_SOURCES, PollingSource, RootSource
from orcapod.errors import ProvisionalHashWarning
from orcapod.utils.provisional import (
    recording_provisional_uses,
    reset_provisional_use_warnings,
)


@pytest.fixture(autouse=True)
def fresh_process() -> Iterator[None]:
    """Each test starts as a new process would: no provisional source has warned."""
    reset_provisional_use_warnings()
    yield
    reset_provisional_use_warnings()


class StubImpl:
    """A polling implementation that is never polled; it round-trips through a config."""

    def __init__(self, name: str = "stub") -> None:
        self._name = name

    def identity(self) -> Any:
        return ("StubImpl", self._name)

    def to_config(self) -> dict[str, Any]:
        return {"name": self._name}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> StubImpl:
        return cls(config["name"])

    def schema(self) -> Any:
        return None

    async def poll(self, cursor: Any = None) -> bool:
        return False

    async def fetch(self, cursor: Any = None) -> Any:
        raise NotImplementedError

    async def close(self) -> None:
        return None


def _polling(name: str = "stub", **kwargs: Any) -> PollingSource[Any]:
    return PollingSource(
        StubImpl(name), tag_columns="id", record_id_columns="id", **kwargs
    )


def _provisional_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [
        str(w.message) for w in caught if issubclass(w.category, ProvisionalHashWarning)
    ]


def test_the_list_and_the_markers_agree() -> None:
    exported = {
        name: obj
        for name, obj in vars(sources).items()
        if name in sources.__all__
        and inspect.isclass(obj)
        and issubclass(obj, RootSource)
    }
    marked = {name for name, cls in exported.items() if cls.__dict__.get("PROVISIONAL")}
    assert marked == set(PROVISIONAL_SOURCES) == {"PollingSource"}


def test_one_warning_for_several_constructions() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for name in ("a", "b", "c"):
            _ = _polling(name)
        _ = _polling("d", source_id="named")
    messages = _provisional_warnings(caught)
    assert len(messages) == 1
    assert messages[0].startswith("PollingSource is provisional")
    assert "source id and hashes are not frozen" in messages[0]


def test_from_config_warns_too() -> None:
    config = _polling(source_id="polled").to_config()
    reset_provisional_use_warnings()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rebuilt = PollingSource.from_config(config)
    assert rebuilt.source_id == "polled"
    assert len(_provisional_warnings(caught)) == 1


def test_under_an_error_filter_every_construction_raises() -> None:
    for _ in range(2):
        with warnings.catch_warnings():
            warnings.simplefilter("error", ProvisionalHashWarning)
            with pytest.raises(ProvisionalHashWarning, match="PollingSource"):
                _ = _polling()


def test_every_use_is_recorded() -> None:
    """The golden tool refuses a case that records an undeclared provisional use."""
    with recording_provisional_uses() as used:
        _ = _polling("a")
        _ = _polling("b")
    assert used == {"PollingSource"}


def test_the_mark_enters_no_config_and_no_identity() -> None:
    plain = _polling(source_id="polled")
    assert set(plain.to_config()) == {
        "source_type",
        "impl_module",
        "impl_class",
        "tag_columns",
        "record_id_columns",
        "polling_config",
        "source_id",
        "impl_config",
    }
    assert plain.identity_structure() == (("StubImpl", "stub"), ("id",), ("id",))


def test_behavior_is_unchanged_default_source_id_included() -> None:
    """The default source id is still built from the implementation's identity."""
    first, second = _polling("a"), _polling("b")
    assert first.source_id.startswith("polling_source:")
    assert first.source_id != second.source_id
    assert _polling("a").source_id == first.source_id


def test_a_golden_case_cannot_declare_a_provisional_source() -> None:
    from tests.test_hashing.golden.cases import declarable_provisional_names

    assert not set(PROVISIONAL_SOURCES) & declarable_provisional_names()
