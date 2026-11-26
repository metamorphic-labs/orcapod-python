"""The identifiers a source has read, held as digests (ITL-1464)."""

from __future__ import annotations

import random

import pytest

from orcapod.core.sources.seen_keys import SeenKeys


@pytest.mark.parametrize("seed", range(20))
def test_it_finds_what_a_set_of_the_identifiers_finds(seed: int) -> None:
    rng = random.Random(seed)
    seen, expected = SeenKeys(), set[str]()
    for _ in range(40):
        size = rng.choice([0, 1, 2, 3, 50, 400])
        batch = list({f'{{"k":{rng.randrange(60_000)}}}' for _ in range(size)})
        repeated = sorted(expected.intersection(batch))
        assert seen.add(batch) == repeated
        if not repeated:
            expected.update(batch)


def test_any_text_is_an_identifier() -> None:
    texts = ["", "a", "a\x00", "\x00", "é", "\ud800", "b" * 10_000]
    seen = SeenKeys()
    assert seen.add(texts) == []
    for text in texts:
        assert seen.add([text]) == [text]


def test_a_batch_with_a_repeated_identifier_adds_nothing() -> None:
    seen = SeenKeys()
    assert seen.add(["a"]) == []
    assert seen.add(["b", "a"]) == ["a"]
    assert seen.add(["b"]) == []


def test_it_holds_sixteen_bytes_an_identifier() -> None:
    seen = SeenKeys()
    for start in range(0, 100_000, 1_000):
        assert seen.add([f"key {i}" for i in range(start, start + 1_000)]) == []
    assert seen.nbytes == 1_600_000
