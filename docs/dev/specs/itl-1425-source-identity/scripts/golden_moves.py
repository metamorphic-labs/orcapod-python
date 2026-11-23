"""Which golden entries an identity change would move (ITL-1425, section 3).

Each variant patches the library in this process only (nothing under ``src/`` changes),
computes every golden group with the golden cases (``tests/test_hashing/golden/cases.py``)
and compares the values with the same computation unpatched. Each variant runs in its own
process, so no hash cached by one variant is seen by another.

Variants:

* ``C``: the class name leaves ``RootSource.identity_structure``
  (identity = Arrow schema identity, source id, key columns).
* ``B``: a declared implementation version enters it beside the class name
  (identity = class name, version 1, Arrow schema identity, source id, key columns).
* ``B_no_class``: C plus a version (``SOURCE_KIND`` and version replace the class name).
* ``A``: the default source ids carry a version (``<kind>.v1:...``); identity unchanged.
* ``K_defaults``: the explicit-kind-in-id design for defaults only: C, and every default
  id starts with ``<SOURCE_KIND>_v1:`` (``csv_v1:<path>``, ``arrow_table_v1:arrow_v1:<hex>``).
* ``K_always``: ``K_defaults``, and a given id is prefixed too (``subjects`` given to a
  CSV source becomes ``csv_v1:subjects``); wrappers (``CachedSource``, ``SourceProxy``)
  keep the id of what they wrap.

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.golden_moves`` (all variants) or ``... golden_moves C``.
"""

from __future__ import annotations

import json
import subprocess
import sys
import warnings
from collections import Counter
from pathlib import Path

warnings.simplefilter("ignore")

REPO = Path(__file__).resolve().parents[5]
VARIANTS = ("C", "B", "B_no_class", "A", "K_defaults", "K_always")


def patch(variant: str) -> None:
    from orcapod.core.sources import base, source_ids

    original = base.RootSource.identity_structure

    if variant in ("K_defaults", "K_always"):
        patch("C")
        _patch_default_ids("_v1")
        if variant == "K_always":
            _prefix_given_ids()
        return
    if variant in ("C", "B", "B_no_class"):

        def identity_structure(self):  # type: ignore[no-untyped-def]
            full = original(self)
            if not (
                isinstance(full, tuple) and full and full[0] == type(self).__name__
            ):
                return full  # a subclass's own form (DerivedSource plain form)
            rest = full[1:]
            if variant == "C":
                return rest
            if variant == "B":
                return (full[0], ("version", 1), *rest)
            return (self.SOURCE_KIND, ("version", 1), *rest)

        base.RootSource.identity_structure = identity_structure  # type: ignore[method-assign]
    elif variant == "A":
        _patch_default_ids(".v1")
    _ = source_ids


def _patch_default_ids(suffix: str) -> None:
    """Make every default id start with ``<kind><suffix>:``."""
    from orcapod.core.sources import source_ids

    orig_hash, orig_loc = source_ids.hash_source_id, source_ids.location_source_id

    def hash_source_id(kind, content_hash, hexdigits=None):  # type: ignore[no-untyped-def]
        return orig_hash(f"{kind}{suffix}", content_hash, hexdigits)

    def location_source_id(kind, location):  # type: ignore[no-untyped-def]
        return orig_loc(f"{kind}{suffix}", location)

    for name, module in list(sys.modules.items()):
        if not name.startswith("orcapod."):
            continue
        if getattr(module, "hash_source_id", None) is orig_hash:
            setattr(module, "hash_source_id", hash_source_id)
        if getattr(module, "location_source_id", None) is orig_loc:
            setattr(module, "location_source_id", location_source_id)


def _prefix_given_ids() -> None:
    """Prefix a given source id with ``<SOURCE_KIND>_v1:`` (wrappers excepted)."""
    from orcapod.core.sources import base
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.source_proxy import SourceProxy

    original_init = base.RootSource.__init__

    def __init__(self, source_id=None, *args, **kwargs):  # type: ignore[no-untyped-def]
        tag = f"{self.SOURCE_KIND}_v1:"
        if (
            source_id is not None
            and not isinstance(self, (CachedSource, SourceProxy))
            and not source_id.startswith(tag)
        ):
            source_id = tag + source_id
        original_init(self, source_id, *args, **kwargs)

    base.RootSource.__init__ = __init__  # type: ignore[method-assign]


def compute(variant: str | None) -> dict[str, dict[str, object]]:
    sys.path.insert(0, str(REPO))
    import orcapod.core.sources  # noqa: F401 - load the modules before patching
    import orcapod.pipeline  # noqa: F401
    from tests.test_hashing.golden import cases

    if variant:
        patch(variant)
    out: dict[str, dict[str, object]] = {}
    for group in cases.GROUPS:
        try:
            live, _ = cases.compute_group_kinds(group, allow_errors=True)
        except ModuleNotFoundError:
            continue
        out[group] = {k: repr(v) for k, v in live.items()}
    return out


def run_child(variant: str) -> dict[str, dict[str, object]]:
    proc = subprocess.run(
        [sys.executable, "-m", "scripts.golden_moves", "--child", variant],
        capture_output=True,
        text=True,
        check=True,
        cwd=Path(__file__).resolve().parents[1],
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def provisional_entries() -> set[tuple[str, str]]:
    """The entries pinned in the provisional files (outside the freeze, ITL-1266)."""
    sys.path.insert(0, str(REPO))
    from orcapod.contexts import get_default_context
    from tests.test_hashing.golden import store

    version = get_default_context().version
    manifest = store.read_json(store.manifest_path(REPO, version))
    folder = store.golden_folder(REPO, manifest)
    return {
        (g, e) for g, entries in store.load_provisional(folder).items() for e in entries
    }


def main() -> None:
    if len(sys.argv) > 2 and sys.argv[1] == "--child":
        variant = None if sys.argv[2] == "none" else sys.argv[2]
        print(json.dumps(compute(variant)))
        return
    chosen = sys.argv[1:] or list(VARIANTS)
    baseline = run_child("none")
    total = sum(len(v) for v in baseline.values())
    provisional = provisional_entries()
    print(f"golden entries computed: {total}")
    for variant in chosen:
        live = run_child(variant)
        moved = [
            (g, e)
            for g, entries in baseline.items()
            for e, v in entries.items()
            if live.get(g, {}).get(e) != v
        ]
        per_group = Counter(g for g, _ in moved)
        in_provisional = sum(1 for m in moved if m in provisional)
        print(
            f"\n== {variant}: {len(moved)} of {total} entries move "
            f"({len(moved) - in_provisional} frozen-file, {in_provisional} provisional) =="
        )
        for group in baseline:
            print(
                f"   {group:9s} {per_group.get(group, 0):4d} of {len(baseline[group])}"
            )
        prefixes = Counter(
            e.split(":")[0].split("/")[0].split(".")[0] for _, e in moved
        )
        print("   moved entries by name prefix:", dict(sorted(prefixes.items())))
        listing = Path(__file__).resolve().parents[1] / f"golden-moves-{variant}.txt"
        listing.write_text("".join(f"{g}:{e}\n" for g, e in sorted(moved)))
        print(f"   full list: {listing.name}")


if __name__ == "__main__":
    main()
