"""Compare two runs of ``starfix_differential_corpus.py`` (ITL-1179 stage 1, proof B).

Prints every entry whose outcome differs and classifies it:

* ``documented``: a call of a ``known_defect.*`` case that makes exactly the transition
  starfix 0.6.0 documents against 0.5.0 (``EXPECTED_TRANSITIONS``): for ITL-1223, 0.5.0
  gives another digest and 0.6.0 gives the digest of the same values built fresh (the
  case's twin); for the bufferless dictionary, 0.5.0 raises ``TypeError`` and 0.6.0 gives
  the digest of the same dictionary with its buffer allocated (the twin);
* ``message``: both runs raise the same exception class and only the message differs;
* ``UNEXPECTED``: anything else, including any other outcome of a known-defect case.

Exits non-zero when an unexpected difference is found.

Usage::

    uv run python docs/dev/scripts/itl_1179/compare_corpus_runs.py OLD.json NEW.json
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Transition:
    """What a known-defect case does between 0.5.0 (old) and 0.6.0 (new).

    Attributes:
        old_raises: The exception class the old run raises, or ``None`` when it gives a
            digest (which must then differ from the new one).
        twin: The case holding the same values built fresh: the new digest must equal
            the twin's new digest for the same call.
    """

    old_raises: str | None
    twin: str


EXPECTED_TRANSITIONS = {
    "known_defect.itl1223_sliced_nested_fsl_dictionary": Transition(
        old_raises=None, twin="twin.itl1223_fresh_values"
    ),
    "known_defect.bufferless_empty_dictionary": Transition(
        old_raises="TypeError", twin="twin.allocated_empty_dictionary"
    ),
}


def _is_raise(outcome: str) -> bool:
    return outcome.startswith("raise:")


def _documented(name: str, old: dict[str, str], new: dict[str, str]) -> bool:
    """Whether ``name`` (``<case>|<call>``) makes its case's expected transition."""
    case, call = name.split("|", 1)
    transition = EXPECTED_TRANSITIONS.get(case)
    if transition is None:
        return False
    twin = new.get(f"{transition.twin}|{call}")
    if twin is None or _is_raise(new[name]) or new[name] != twin:
        return False
    if transition.old_raises is None:
        return not _is_raise(old[name]) and old[name] != new[name]
    return old[name].split(":", 2)[:2] == ["raise", transition.old_raises]


def _classify(name: str, old: dict[str, str], new: dict[str, str]) -> str:
    if _documented(name, old, new):
        return "documented"
    if _is_raise(old[name]) and _is_raise(new[name]):
        if old[name].split(":", 2)[1] == new[name].split(":", 2)[1]:
            return "message"
    return "UNEXPECTED"


def main() -> int:
    old_run = json.loads(Path(sys.argv[1]).read_text())
    new_run = json.loads(Path(sys.argv[2]).read_text())
    old, new = old_run["entries"], new_run["entries"]
    print(
        f"old: starfix {old_run['starfix_version']}, new: starfix {new_run['starfix_version']}, "
        f"pyarrow {old_run['pyarrow_version']} / {new_run['pyarrow_version']}"
    )
    if set(old) != set(new):
        print(f"UNEXPECTED: entry sets differ: {sorted(set(old) ^ set(new))}")
        return 1
    counts = {"same": 0, "documented": 0, "message": 0, "UNEXPECTED": 0}
    for name in sorted(old):
        if old[name] == new[name]:
            counts["same"] += 1
            continue
        kind = _classify(name, old, new)
        counts[kind] += 1
        print(f"[{kind}] {name}\n    old: {old[name]}\n    new: {new[name]}")
    print(f"{len(old)} entries: {counts}")
    return 1 if counts["UNEXPECTED"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
