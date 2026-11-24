"""A record-keeping ledger, prototyped outside the library (ITL-1425, section 4).

The ledger of a source is one row per record: ``source_id``, ``record_id``, ``row_hash``
and implementation columns (``kind``, ``class``). The row hash here is
``arrow_hasher.hash_table`` (``arrow_v1``) of a one-row table of the record's user
columns (tags and data), with their Arrow types, and nothing else: no system tag,
source-info token, context or meta column (the source id and record id are the ledger's
key, not part of what is compared). A later read is compared with the ledger, per record
id: same, changed, new, missing.

Cases:

1. Every other kind holding the same rows, against a ledger written by the
   ``ArrowTableSource``.
2. A CSV read with inferred types (non-null columns) against the same ledger: hashed as
   read, then hashed in the slot's declared form (the declared nullable schema, as
   ``bind`` presents a narrower source).
3. A reader change: one value read differently under the same source id and key.
4. Positional record ids, the rows listed in another order.

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.ledger_prototype``.
"""

from __future__ import annotations

import warnings

import pyarrow as pa

from orcapod.sources import ArrowTableSource, CSVSource

from ._fixtures import (
    KEYED,
    POSITIONAL,
    ROWS,
    SCHEMA,
    arrow_table,
    same_rows_sources,
    scratch,
)

warnings.simplefilter("ignore")


def ledger(source, declared: pa.Schema | None = None) -> dict[str, bytes]:
    """Map each record id to the hash of its user columns (optionally as declared)."""
    full = source.as_table(columns={"system_tags": True})
    rid = next(c for c in full.column_names if c.startswith("_tag_record_id"))
    tag_keys, data_keys = source.keys()
    user = full.select([*tag_keys, *data_keys])
    if declared is not None:
        user = pa.Table.from_arrays(user.columns, schema=declared)
    hasher = source.data_context.arrow_hasher
    out = {}
    for i, record_id in enumerate(full.column(rid).to_pylist()):
        out[record_id] = hasher.hash_table(user.slice(i, 1)).to_prefixed_digest()
    return out


def compare(old: dict[str, bytes], new: dict[str, bytes]) -> dict[str, int]:
    return {
        "same": sum(1 for k in new if k in old and old[k] == new[k]),
        "changed": sum(1 for k in new if k in old and old[k] != new[k]),
        "new": sum(1 for k in new if k not in old),
        "missing": sum(1 for k in old if k not in new),
    }


def main() -> None:
    folder = scratch()
    sources = same_rows_sources(folder)
    base = ledger(sources["arrow_table"])
    print("row hash label:", next(iter(base.values())).split(b":")[0].decode())

    print("\n== 1. Other kinds, same rows ==")
    for kind, src in sources.items():
        print(f"   {kind:13s} {compare(base, ledger(src))}")

    print("\n== 2. CSV with inferred (non-null) types ==")
    inferred = CSVSource(str(folder / "subjects.csv"), source_id="subjects", **KEYED)
    print("   hashed as read:          ", compare(base, ledger(inferred)))
    print(
        "   hashed in declared form: ", compare(base, ledger(inferred, declared=SCHEMA))
    )

    print("\n== 3. A reader change: one value read differently ==")
    rows = [dict(r) for r in ROWS]
    rows[1]["weight"] = 22.0001
    changed = ArrowTableSource(arrow_table(rows), source_id="subjects", **KEYED)
    print("   ", compare(base, ledger(changed)))

    print("\n== 4. Positional record ids, rows in another order ==")
    a = ArrowTableSource(arrow_table(), **POSITIONAL)
    b = ArrowTableSource(arrow_table(list(reversed(ROWS))), **POSITIONAL)
    print(
        "   ",
        compare(ledger(a), ledger(b)),
        "(same rows; the middle row keeps its position)",
    )


if __name__ == "__main__":
    main()
